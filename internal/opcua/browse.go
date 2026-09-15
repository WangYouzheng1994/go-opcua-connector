package opcua

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	goopcua "github.com/gopcua/opcua"
	"github.com/gopcua/opcua/id"
	"github.com/gopcua/opcua/ua"
)

const maxBrowsePathDepth = 128

// SourceRef 是 OPC UA Adapter 内部使用的协议侧点位引用。
// 它保存规范化后的完整 NodeID，不得传入 StateStore。
type SourceRef struct {
	nodeID string
}

func newSourceRef(nodeID *ua.NodeID) (SourceRef, error) {
	if nodeID == nil {
		return SourceRef{}, errors.New("node ID is nil")
	}
	return SourceRef{nodeID: nodeID.String()}, nil
}

func parseSourceRef(value string) (SourceRef, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return SourceRef{}, errors.New("node ID is empty")
	}
	nodeID, err := ua.ParseNodeID(value)
	if err != nil {
		return SourceRef{}, fmt.Errorf("invalid node ID %q: %w", value, err)
	}
	return newSourceRef(nodeID)
}

func (r SourceRef) String() string {
	return r.nodeID
}

func (r SourceRef) node() (*ua.NodeID, error) {
	if r.nodeID == "" {
		return nil, errors.New("source reference is empty")
	}
	return ua.ParseNodeID(r.nodeID)
}

// DiscoveredPoint 是 OPC UA Adapter 对外提供的协议无关发现结果。
// SourceRef 和 BrowsePath 仍保留在 Adapter 私有映射中。
type DiscoveredPoint struct {
	PointID string
}

// DiscoveryIssue 记录单个配置项或源节点的发现失败。
type DiscoveryIssue struct {
	Source string
	Reason string
}

// PointDiscoveryResult 支持部分成功；Points 与 Issues 均按稳定顺序返回。
type PointDiscoveryResult struct {
	Points []DiscoveredPoint
	Issues []DiscoveryIssue
}

type pointIdentity struct {
	pointID    string
	sourceRef  SourceRef
	browsePath []string
}

type pointRegistry struct {
	sourceByPointID     map[string]SourceRef
	pointIDBySource     map[string]string
	browsePathByPointID map[string][]string
}

type nodeMetadata struct {
	sourceRef  SourceRef
	browseName string
	nodeClass  ua.NodeClass
}

type browseReference struct {
	sourceRef  SourceRef
	browseName string
	nodeClass  ua.NodeClass
	visitKey   string
}

type browseService interface {
	metadata(ctx context.Context, sourceRef SourceRef) (nodeMetadata, error)
	references(
		ctx context.Context,
		sourceRef SourceRef,
		referenceType uint32,
		direction ua.BrowseDirection,
		nodeClass ua.NodeClass,
	) ([]browseReference, error)
}

type opcuaBrowseService struct {
	client *goopcua.Client
}

func (b *opcuaBrowseService) metadata(ctx context.Context, sourceRef SourceRef) (nodeMetadata, error) {
	nodeID, err := sourceRef.node()
	if err != nil {
		return nodeMetadata{}, err
	}
	node := b.client.Node(nodeID)
	nodeClass, err := node.NodeClass(ctx)
	if err != nil {
		return nodeMetadata{}, fmt.Errorf("read node class for %s: %w", sourceRef, err)
	}
	browseName, err := node.BrowseName(ctx)
	if err != nil {
		return nodeMetadata{}, fmt.Errorf("read browse name for %s: %w", sourceRef, err)
	}
	if browseName == nil || browseName.Name == "" {
		return nodeMetadata{}, fmt.Errorf("node %s has empty browse name", sourceRef)
	}
	return nodeMetadata{sourceRef: sourceRef, browseName: browseName.Name, nodeClass: nodeClass}, nil
}

func (b *opcuaBrowseService) references(
	ctx context.Context,
	sourceRef SourceRef,
	referenceType uint32,
	direction ua.BrowseDirection,
	nodeClass ua.NodeClass,
) ([]browseReference, error) {
	nodeID, err := sourceRef.node()
	if err != nil {
		return nil, err
	}
	refs, err := b.client.Node(nodeID).References(ctx, referenceType, direction, nodeClass, true)
	if err != nil {
		return nil, err
	}

	result := make([]browseReference, 0, len(refs))
	for _, ref := range refs {
		if ref == nil || ref.NodeID == nil || ref.NodeID.NodeID == nil || ref.BrowseName == nil {
			return nil, fmt.Errorf("browse response for %s contains an incomplete reference", sourceRef)
		}
		if ref.NodeID.ServerIndex != 0 {
			return nil, fmt.Errorf("browse response for %s references remote server index %d", sourceRef, ref.NodeID.ServerIndex)
		}
		resolvedNode := b.client.NodeFromExpandedNodeID(ref.NodeID)
		if resolvedNode == nil || resolvedNode.ID == nil {
			return nil, fmt.Errorf("cannot resolve child NodeID from browse response for %s", sourceRef)
		}
		resolvedRef, err := newSourceRef(resolvedNode.ID)
		if err != nil {
			return nil, fmt.Errorf("resolve child SourceRef from %s: %w", sourceRef, err)
		}
		result = append(result, browseReference{
			sourceRef:  resolvedRef,
			browseName: ref.BrowseName.Name,
			nodeClass:  ref.NodeClass,
			visitKey:   expandedNodeKey(ref.NodeID),
		})
	}
	return result, nil
}

func expandedNodeKey(nodeID *ua.ExpandedNodeID) string {
	if nodeID == nil || nodeID.NodeID == nil {
		return ""
	}
	return fmt.Sprintf("%d|%s|%s", nodeID.ServerIndex, nodeID.NamespaceURI, nodeID.NodeID.String())
}

type pointDiscoverer struct {
	browser   browseService
	overrides map[string]string
}

type pointIDOverride struct {
	sourceRef string
	pointID   string
}

func newPointDiscoverer(browser browseService, overrides []pointIDOverride) (*pointDiscoverer, error) {
	normalized := make(map[string]string, len(overrides))
	for _, override := range overrides {
		sourceRef, err := parseSourceRef(override.sourceRef)
		if err != nil {
			return nil, fmt.Errorf("invalid point ID override source: %w", err)
		}
		pointID := strings.TrimSpace(override.pointID)
		if pointID == "" {
			return nil, fmt.Errorf("point ID override for %s is empty", sourceRef)
		}
		if existing, exists := normalized[sourceRef.String()]; exists && existing != pointID {
			return nil, fmt.Errorf("conflicting point ID overrides for %s", sourceRef)
		}
		normalized[sourceRef.String()] = pointID
	}
	return &pointDiscoverer{browser: browser, overrides: normalized}, nil
}

func (d *pointDiscoverer) discover(ctx context.Context, configuredNodes []string) (PointDiscoveryResult, pointRegistry) {
	candidates := make([]pointIdentity, 0, len(configuredNodes))
	issues := make([]DiscoveryIssue, 0)

	for _, configuredNode := range configuredNodes {
		if ctx.Err() != nil {
			break
		}
		points, err := d.discoverConfiguredNode(ctx, configuredNode)
		if err != nil {
			issues = append(issues, DiscoveryIssue{Source: configuredNode, Reason: err.Error()})
			continue
		}
		candidates = append(candidates, points...)
	}

	result, registry := d.buildRegistry(candidates)
	result.Issues = append(result.Issues, issues...)
	sort.Slice(result.Issues, func(i, j int) bool {
		if result.Issues[i].Source == result.Issues[j].Source {
			return result.Issues[i].Reason < result.Issues[j].Reason
		}
		return result.Issues[i].Source < result.Issues[j].Source
	})
	return result, registry
}

func (d *pointDiscoverer) discoverConfiguredNode(ctx context.Context, configuredNode string) ([]pointIdentity, error) {
	configuredNode = strings.TrimSpace(configuredNode)
	recursive := strings.HasSuffix(configuredNode, ".*")
	rawNodeID := strings.TrimSpace(strings.TrimSuffix(configuredNode, ".*"))
	rootRef, err := parseSourceRef(rawNodeID)
	if err != nil {
		return nil, err
	}
	root, err := d.browser.metadata(ctx, rootRef)
	if err != nil {
		return nil, err
	}
	rootPath, err := canonicalBrowsePath(ctx, d.browser, rootRef)
	if err != nil {
		return nil, err
	}

	switch root.nodeClass {
	case ua.NodeClassVariable:
		return []pointIdentity{d.identity(root.sourceRef, rootPath)}, nil
	case ua.NodeClassObject:
		var points []pointIdentity
		if recursive {
			points, err = d.browseRecursive(ctx, root.sourceRef, rootPath, "local||"+root.sourceRef.String(), make(map[string]struct{}))
		} else {
			points, err = d.browseDirectVariables(ctx, root.sourceRef, rootPath)
		}
		if err != nil {
			return nil, err
		}
		if len(points) == 0 {
			return nil, fmt.Errorf("no variable nodes discovered under %s", rootRef)
		}
		return points, nil
	default:
		return nil, fmt.Errorf("configured node %s has unsupported class %s", rootRef, root.nodeClass)
	}
}

func (d *pointDiscoverer) browseDirectVariables(ctx context.Context, root SourceRef, rootPath []string) ([]pointIdentity, error) {
	refs, err := d.browser.references(ctx, root, id.HasComponent, ua.BrowseDirectionForward, ua.NodeClassVariable)
	if err != nil {
		return nil, fmt.Errorf("browse direct variables from %s: %w", root, err)
	}
	sortBrowseReferences(refs)
	points := make([]pointIdentity, 0, len(refs))
	for _, ref := range refs {
		if ref.browseName == "" {
			return nil, fmt.Errorf("variable %s has empty browse name", ref.sourceRef)
		}
		path := appendBrowsePath(rootPath, ref.browseName)
		points = append(points, d.identity(ref.sourceRef, path))
	}
	return points, nil
}

func (d *pointDiscoverer) browseRecursive(
	ctx context.Context,
	root SourceRef,
	rootPath []string,
	visitKey string,
	visited map[string]struct{},
) ([]pointIdentity, error) {
	if visitKey == "" {
		visitKey = "local||" + root.String()
	}
	sourceKey := "source||" + root.String()
	if _, exists := visited[visitKey]; exists {
		return nil, nil
	}
	if _, exists := visited[sourceKey]; exists {
		return nil, nil
	}
	visited[visitKey] = struct{}{}
	visited[sourceKey] = struct{}{}

	childClasses := ua.NodeClassObject | ua.NodeClassVariable
	componentRefs, err := d.browser.references(ctx, root, id.HasComponent, ua.BrowseDirectionForward, childClasses)
	if err != nil {
		return nil, fmt.Errorf("browse HasComponent from %s: %w", root, err)
	}
	organizesRefs, err := d.browser.references(ctx, root, id.Organizes, ua.BrowseDirectionForward, childClasses)
	if err != nil {
		return nil, fmt.Errorf("browse Organizes from %s: %w", root, err)
	}

	refs := deduplicateBrowseReferences(append(componentRefs, organizesRefs...))
	sortBrowseReferences(refs)
	points := make([]pointIdentity, 0)
	for _, ref := range refs {
		if ref.browseName == "" {
			return nil, fmt.Errorf("child %s has empty browse name", ref.sourceRef)
		}
		path := appendBrowsePath(rootPath, ref.browseName)
		switch ref.nodeClass {
		case ua.NodeClassVariable:
			points = append(points, d.identity(ref.sourceRef, path))
		case ua.NodeClassObject:
			children, err := d.browseRecursive(ctx, ref.sourceRef, path, ref.visitKey, visited)
			if err != nil {
				return nil, err
			}
			points = append(points, children...)
		}
	}
	return points, nil
}

func (d *pointDiscoverer) identity(sourceRef SourceRef, browsePath []string) pointIdentity {
	return pointIdentity{sourceRef: sourceRef, browsePath: browsePath}
}

func (d *pointDiscoverer) buildRegistry(candidates []pointIdentity) (PointDiscoveryResult, pointRegistry) {
	candidatesBySource := make(map[string][]pointIdentity, len(candidates))
	for _, candidate := range candidates {
		candidate.pointID = d.pointID(candidate)
		source := candidate.sourceRef.String()
		candidatesBySource[source] = append(candidatesBySource[source], candidate)
	}

	result := PointDiscoveryResult{}
	uniqueSources := make(map[string]pointIdentity, len(candidatesBySource))
	sources := make([]string, 0, len(candidatesBySource))
	for source := range candidatesBySource {
		sources = append(sources, source)
	}
	sort.Strings(sources)
	for _, source := range sources {
		group := candidatesBySource[source]
		identities := uniquePointIdentities(group)
		if _, overridden := d.overrides[source]; overridden {
			// 显式 PointID 已裁决协议无关身份；仅稳定选择一条私有 BrowsePath。
			uniqueSources[source] = stablePointIdentity(identities)
			continue
		}
		if len(identities) > 1 {
			descriptions := make([]string, 0, len(identities))
			for _, identity := range identities {
				descriptions = append(descriptions, fmt.Sprintf(
					"BrowsePath=%q, PointID=%q",
					strings.Join(identity.browsePath, "/"),
					identity.pointID,
				))
			}
			sort.Strings(descriptions)
			result.Issues = append(result.Issues, DiscoveryIssue{
				Source: source,
				Reason: fmt.Sprintf(
					"SourceRef has ambiguous identities: %s",
					strings.Join(descriptions, "; "),
				),
			})
			continue
		}
		uniqueSources[source] = identities[0]
	}

	byPointID := make(map[string][]pointIdentity, len(uniqueSources))
	for _, candidate := range uniqueSources {
		byPointID[candidate.pointID] = append(byPointID[candidate.pointID], candidate)
	}

	registry := pointRegistry{
		sourceByPointID:     make(map[string]SourceRef),
		pointIDBySource:     make(map[string]string),
		browsePathByPointID: make(map[string][]string),
	}
	for pointID, group := range byPointID {
		if pointID == "" {
			for _, candidate := range group {
				result.Issues = append(result.Issues, DiscoveryIssue{
					Source: candidate.sourceRef.String(),
					Reason: "resolved BrowsePath produces an empty PointID",
				})
			}
			continue
		}
		if len(group) > 1 {
			sources := make([]string, 0, len(group))
			for _, candidate := range group {
				sources = append(sources, candidate.sourceRef.String())
			}
			sort.Strings(sources)
			reason := fmt.Sprintf("PointID %q conflicts between SourceRefs: %s", pointID, strings.Join(sources, ", "))
			for _, source := range sources {
				result.Issues = append(result.Issues, DiscoveryIssue{Source: source, Reason: reason})
			}
			continue
		}

		candidate := group[0]
		result.Points = append(result.Points, DiscoveredPoint{PointID: pointID})
		registry.sourceByPointID[pointID] = candidate.sourceRef
		registry.pointIDBySource[candidate.sourceRef.String()] = pointID
		registry.browsePathByPointID[pointID] = append([]string(nil), candidate.browsePath...)
	}
	sort.Slice(result.Points, func(i, j int) bool {
		return result.Points[i].PointID < result.Points[j].PointID
	})
	return result, registry
}

func uniquePointIdentities(candidates []pointIdentity) []pointIdentity {
	identities := make([]pointIdentity, 0, len(candidates))
	for _, candidate := range candidates {
		duplicate := false
		for _, identity := range identities {
			if identity.pointID == candidate.pointID && sameBrowsePath(identity.browsePath, candidate.browsePath) {
				duplicate = true
				break
			}
		}
		if !duplicate {
			identities = append(identities, candidate)
		}
	}
	return identities
}

func stablePointIdentity(identities []pointIdentity) pointIdentity {
	selected := identities[0]
	for _, identity := range identities[1:] {
		if browsePathLess(identity.browsePath, selected.browsePath) {
			selected = identity
		}
	}
	return selected
}

func browsePathLess(left, right []string) bool {
	limit := len(left)
	if len(right) < limit {
		limit = len(right)
	}
	for i := 0; i < limit; i++ {
		if left[i] != right[i] {
			return left[i] < right[i]
		}
	}
	return len(left) < len(right)
}

func sameBrowsePath(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func (d *pointDiscoverer) pointID(candidate pointIdentity) string {
	if pointID, exists := d.overrides[candidate.sourceRef.String()]; exists {
		return pointID
	}
	return pointIDFromBrowsePath(candidate.browsePath)
}

func pointIDFromBrowsePath(path []string) string {
	// Objects 是 Adapter 内部保存的规范路径根段，不属于业务 PointID。
	if len(path) > 0 && path[0] == "Objects" {
		path = path[1:]
	}
	encoded := make([]string, 0, len(path))
	for _, segment := range path {
		if segment == "" {
			return ""
		}
		segment = strings.ReplaceAll(segment, "%", "%25")
		segment = strings.ReplaceAll(segment, ".", "%2E")
		encoded = append(encoded, segment)
	}
	return strings.Join(encoded, ".")
}

// canonicalBrowsePath 返回从 Objects 根段开始的完整规范路径。
func canonicalBrowsePath(ctx context.Context, browser browseService, sourceRef SourceRef) ([]string, error) {
	objectsRef, _ := newSourceRef(ua.NewNumericNodeID(0, id.ObjectsFolder))
	path, found, err := findBrowsePath(ctx, browser, sourceRef, objectsRef, make(map[string]struct{}), 0)
	if err != nil {
		return nil, err
	}
	if !found || len(path) == 0 {
		// 部分服务端不返回业务节点的反向引用，改由正向引用验证真实路径。
		return forwardBrowsePath(ctx, browser, objectsRef, sourceRef)
	}
	return path, nil
}

func forwardBrowsePath(ctx context.Context, browser browseService, objects, target SourceRef) ([]string, error) {
	type entry struct {
		source SourceRef
		path   []string
	}
	queue := []entry{{source: objects, path: []string{"Objects"}}}
	visited := map[SourceRef]bool{objects: true}
	var issues []error
	for head := 0; head < len(queue); head++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		current := queue[head]
		if len(current.path) >= maxBrowsePathDepth {
			continue
		}
		refs, err := browser.references(ctx, current.source, id.HierarchicalReferences, ua.BrowseDirectionForward, ua.NodeClassObject|ua.NodeClassVariable)
		if err != nil {
			issues = append(issues, fmt.Errorf("browse children of %s: %w", current.source, err))
			continue
		}
		sortBrowseReferences(refs)
		for _, ref := range refs {
			if ref.browseName == "" || visited[ref.sourceRef] {
				continue
			}
			path := appendBrowsePath(current.path, ref.browseName)
			if ref.sourceRef == target {
				return path, nil
			}
			visited[ref.sourceRef] = true
			if ref.nodeClass == ua.NodeClassObject {
				queue = append(queue, entry{source: ref.sourceRef, path: path})
			}
		}
	}
	return nil, errors.Join(append([]error{fmt.Errorf("cannot resolve BrowsePath from Objects to %s using inverse or forward references", target)}, issues...)...)
}

func findBrowsePath(
	ctx context.Context,
	browser browseService,
	current SourceRef,
	objects SourceRef,
	visiting map[string]struct{},
	depth int,
) ([]string, bool, error) {
	if current == objects {
		return []string{"Objects"}, true, nil
	}
	if depth >= maxBrowsePathDepth {
		return nil, false, fmt.Errorf("BrowsePath exceeds %d levels at %s", maxBrowsePathDepth, current)
	}
	if _, exists := visiting[current.String()]; exists {
		return nil, false, nil
	}
	visiting[current.String()] = struct{}{}
	defer delete(visiting, current.String())

	metadata, err := browser.metadata(ctx, current)
	if err != nil {
		return nil, false, err
	}
	parents, err := browser.references(ctx, current, id.HierarchicalReferences, ua.BrowseDirectionInverse, ua.NodeClassAll)
	if err != nil {
		return nil, false, fmt.Errorf("browse parents of %s: %w", current, err)
	}
	sortBrowseReferences(parents)
	for _, parent := range parents {
		path, found, err := findBrowsePath(ctx, browser, parent.sourceRef, objects, visiting, depth+1)
		if err != nil {
			return nil, false, err
		}
		if found {
			return appendBrowsePath(path, metadata.browseName), true, nil
		}
	}
	return nil, false, nil
}

func appendBrowsePath(path []string, segment string) []string {
	result := make([]string, len(path), len(path)+1)
	copy(result, path)
	return append(result, segment)
}

func deduplicateBrowseReferences(refs []browseReference) []browseReference {
	seen := make(map[string]struct{}, len(refs))
	result := make([]browseReference, 0, len(refs))
	for _, ref := range refs {
		key := ref.visitKey
		if key == "" {
			key = ref.sourceRef.String()
		}
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		result = append(result, ref)
	}
	return result
}

func sortBrowseReferences(refs []browseReference) {
	sort.Slice(refs, func(i, j int) bool {
		if refs[i].browseName == refs[j].browseName {
			return refs[i].sourceRef.String() < refs[j].sourceRef.String()
		}
		return refs[i].browseName < refs[j].browseName
	})
}
