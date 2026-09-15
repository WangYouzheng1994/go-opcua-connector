package opcua

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/gopcua/opcua/id"
	"github.com/gopcua/opcua/ua"
)

func TestPointIDFromBrowsePathEscapesSeparators(t *testing.T) {
	t.Parallel()

	got := pointIDFromBrowsePath([]string{"Objects", "Channel.A", "50%", "Tag"})
	want := "Channel%2EA.50%25.Tag"
	if got != want {
		t.Fatalf("pointIDFromBrowsePath() = %q, want %q", got, want)
	}
}

func TestCanonicalBrowsePathIncludesObjectsWhilePointIDOmitIt(t *testing.T) {
	t.Parallel()

	browser := basicVariableTree(t, "ns=2;s=temperature")
	path, err := canonicalBrowsePath(context.Background(), browser, mustSourceRef(t, "ns=2;s=temperature"))
	if err != nil {
		t.Fatalf("canonicalBrowsePath: %v", err)
	}
	wantPath := []string{"Objects", "Channel", "Device", "Temperature"}
	if !reflect.DeepEqual(path, wantPath) {
		t.Fatalf("canonical BrowsePath = %#v, want %#v", path, wantPath)
	}
	if pointID := pointIDFromBrowsePath(path); pointID != "Channel.Device.Temperature" {
		t.Fatalf("PointID = %q, want %q", pointID, "Channel.Device.Temperature")
	}
}

func TestPointDiscoverySupportsExactDirectAndRecursiveModes(t *testing.T) {
	t.Parallel()

	browser := newFakeBrowseService(t)
	browser.addNode("i=85", "Objects", ua.NodeClassObject)
	browser.addNode("ns=2;s=channel", "Channel", ua.NodeClassObject)
	browser.addNode("ns=2;s=device", "Device", ua.NodeClassObject)
	browser.addNode("ns=2;s=temperature", "Temperature", ua.NodeClassVariable)
	browser.addNode("ns=2;s=group", "Group", ua.NodeClassObject)
	browser.addNode("ns=2;s=pressure", "Pressure", ua.NodeClassVariable)
	browser.addRelation("i=85", "ns=2;s=channel", id.Organizes)
	browser.addRelation("ns=2;s=channel", "ns=2;s=device", id.Organizes)
	browser.addRelation("ns=2;s=device", "ns=2;s=temperature", id.HasComponent)
	browser.addRelation("ns=2;s=device", "ns=2;s=group", id.HasComponent)
	browser.addRelation("ns=2;s=group", "ns=2;s=pressure", id.Organizes)

	discoverer := mustPointDiscoverer(t, browser, nil)

	exact, _ := discoverer.discover(context.Background(), []string{"ns=2;s=temperature"})
	assertPointIDs(t, exact, "Channel.Device.Temperature")

	direct, _ := discoverer.discover(context.Background(), []string{"ns=2;s=device"})
	assertPointIDs(t, direct, "Channel.Device.Temperature")

	recursive, _ := discoverer.discover(context.Background(), []string{"ns=2;s=device.*"})
	assertPointIDs(t, recursive, "Channel.Device.Group.Pressure", "Channel.Device.Temperature")
}

func TestPointDiscoveryWithoutInverseReferences(t *testing.T) {
	t.Parallel()

	browser := basicVariableTree(t, "ns=2;s=temperature")
	// 模拟服务端只返回正向层级引用，NodeID 与 BrowseName 无字符串对应关系。
	for call, refs := range browser.referencesByCall {
		if call.direction == ua.BrowseDirectionInverse {
			delete(browser.referencesByCall, call)
		} else {
			hierarchical := call
			hierarchical.referenceType = id.HierarchicalReferences
			browser.referencesByCall[hierarchical] = refs
		}
	}
	for _, configured := range []string{"ns=2;s=temperature", "ns=2;s=device", "ns=2;s=device.*"} {
		result, registry := mustPointDiscoverer(t, browser, nil).discover(context.Background(), []string{configured})
		assertPointIDs(t, result, "Channel.Device.Temperature")
		if got := registry.browsePathByPointID["Channel.Device.Temperature"]; !reflect.DeepEqual(got, []string{"Objects", "Channel", "Device", "Temperature"}) {
			t.Fatalf("lost full BrowsePath: %#v", got)
		}
	}
}

func TestForwardBrowsePathHandlesCyclesAndMissingTarget(t *testing.T) {
	t.Parallel()

	browser := newFakeBrowseService(t)
	browser.addNode("i=85", "Objects", ua.NodeClassObject)
	browser.addNode("ns=2;i=1", "Folder", ua.NodeClassObject)
	browser.addRelation("i=85", "ns=2;i=1", id.HierarchicalReferences)
	browser.addRelation("ns=2;i=1", "i=85", id.HierarchicalReferences)
	_, err := forwardBrowsePath(context.Background(), browser, mustSourceRef(t, "i=85"), mustSourceRef(t, "ns=2;i=999"))
	if err == nil || !strings.Contains(err.Error(), "cannot resolve BrowsePath") {
		t.Fatalf("missing target should fail after traversing cycle: %v", err)
	}
}

func TestPointDiscoveryDeduplicatesOverlappingConfiguration(t *testing.T) {
	t.Parallel()

	browser := basicVariableTree(t, "ns=2;s=temperature")
	discoverer := mustPointDiscoverer(t, browser, nil)
	result, _ := discoverer.discover(context.Background(), []string{
		"ns=2;s=temperature",
		"ns=2;s=device",
		"ns=2;s=device.*",
	})

	assertPointIDs(t, result, "Channel.Device.Temperature")
}

func TestPointDiscoveryRejectsAmbiguousSourceRefRegardlessOfConfigurationOrder(t *testing.T) {
	t.Parallel()

	browser := ambiguousSourceTree(t)

	discover := func(configuredNodes []string) (PointDiscoveryResult, pointRegistry) {
		discoverer := mustPointDiscoverer(t, browser, nil)
		return discoverer.discover(context.Background(), configuredNodes)
	}
	forwardResult, forwardRegistry := discover([]string{"ns=2;s=branch-a", "ns=2;s=branch-b"})
	reverseResult, reverseRegistry := discover([]string{"ns=2;s=branch-b", "ns=2;s=branch-a"})

	if !reflect.DeepEqual(forwardResult, reverseResult) {
		t.Fatalf("configuration order changed ambiguity result: forward=%#v reverse=%#v", forwardResult, reverseResult)
	}
	if len(forwardResult.Points) != 0 || len(forwardResult.Issues) != 1 {
		t.Fatalf("ambiguous SourceRef must produce one issue and no point: %#v", forwardResult)
	}
	issue := forwardResult.Issues[0]
	if issue.Source != "ns=2;s=shared-tag" ||
		!strings.Contains(issue.Reason, `BrowsePath="Objects/BranchA/Tag", PointID="BranchA.Tag"`) ||
		!strings.Contains(issue.Reason, `BrowsePath="Objects/BranchB/Tag", PointID="BranchB.Tag"`) {
		t.Fatalf("ambiguity issue lacks both identities: %#v", issue)
	}
	for _, registry := range []pointRegistry{forwardRegistry, reverseRegistry} {
		if len(registry.sourceByPointID) != 0 || len(registry.pointIDBySource) != 0 || len(registry.browsePathByPointID) != 0 {
			t.Fatalf("ambiguous SourceRef entered registry: %#v", registry)
		}
	}
}

func TestPointDiscoveryOverrideResolvesAmbiguousSourceRefRegardlessOfConfigurationOrder(t *testing.T) {
	t.Parallel()

	browser := ambiguousSourceTree(t)
	discover := func(configuredNodes []string) (PointDiscoveryResult, pointRegistry) {
		discoverer := mustPointDiscoverer(t, browser, map[string]string{
			"ns=2;s=shared-tag": "plant.shared-tag",
		})
		return discoverer.discover(context.Background(), configuredNodes)
	}
	forwardResult, forwardRegistry := discover([]string{"ns=2;s=branch-a", "ns=2;s=branch-b"})
	reverseResult, reverseRegistry := discover([]string{"ns=2;s=branch-b", "ns=2;s=branch-a"})

	if !reflect.DeepEqual(forwardResult, reverseResult) || !reflect.DeepEqual(forwardRegistry, reverseRegistry) {
		t.Fatalf(
			"configuration order changed overridden identity: forward=%#v/%#v reverse=%#v/%#v",
			forwardResult,
			forwardRegistry,
			reverseResult,
			reverseRegistry,
		)
	}
	assertPointIDs(t, forwardResult, "plant.shared-tag")
	if len(forwardRegistry.sourceByPointID) != 1 || len(forwardRegistry.pointIDBySource) != 1 {
		t.Fatalf("overridden SourceRef did not enter registry exactly once: %#v", forwardRegistry)
	}
	if sourceRef := forwardRegistry.sourceByPointID["plant.shared-tag"]; sourceRef.String() != "ns=2;s=shared-tag" {
		t.Fatalf("unexpected overridden PointID mapping: %#v", forwardRegistry.sourceByPointID)
	}
	if path := forwardRegistry.browsePathByPointID["plant.shared-tag"]; !reflect.DeepEqual(path, []string{"Objects", "BranchA", "Tag"}) {
		t.Fatalf("override did not retain the stable first BrowsePath: %#v", path)
	}
}

func TestPointDiscoveryDoesNotFilterVariableByLeadingUnderscore(t *testing.T) {
	t.Parallel()

	browser := newFakeBrowseService(t)
	browser.addNode("i=85", "Objects", ua.NodeClassObject)
	browser.addNode("ns=2;s=device", "Device", ua.NodeClassObject)
	browser.addNode("ns=2;s=diagnostic", "_Diagnostic", ua.NodeClassVariable)
	browser.addRelation("i=85", "ns=2;s=device", id.Organizes)
	browser.addRelation("ns=2;s=device", "ns=2;s=diagnostic", id.HasComponent)

	discoverer := mustPointDiscoverer(t, browser, nil)
	result, _ := discoverer.discover(context.Background(), []string{"ns=2;s=device"})
	assertPointIDs(t, result, "Device._Diagnostic")
}

func TestPointDiscoveryUsesExplicitOverrideAndBuildsBidirectionalRegistry(t *testing.T) {
	t.Parallel()

	browser := basicVariableTree(t, "ns=2;s=temperature")
	discoverer := mustPointDiscoverer(t, browser, map[string]string{
		" ns=2;s=temperature ": " boiler.temperature ",
	})
	result, registry := discoverer.discover(context.Background(), []string{"ns=2;s=temperature"})

	assertPointIDs(t, result, "boiler.temperature")
	sourceRef, exists := registry.sourceByPointID["boiler.temperature"]
	if !exists || sourceRef.String() != "ns=2;s=temperature" {
		t.Fatalf("unexpected PointID to SourceRef mapping: %#v", registry.sourceByPointID)
	}
	if pointID := registry.pointIDBySource[sourceRef.String()]; pointID != "boiler.temperature" {
		t.Fatalf("unexpected SourceRef to PointID mapping: %#v", registry.pointIDBySource)
	}
	if path := registry.browsePathByPointID["boiler.temperature"]; !reflect.DeepEqual(path, []string{"Objects", "Channel", "Device", "Temperature"}) {
		t.Fatalf("unexpected private BrowsePath: %#v", path)
	}
}

func TestPointDiscoveryRejectsPointIDCollisionWithoutOverwritingMapping(t *testing.T) {
	t.Parallel()

	browser := newFakeBrowseService(t)
	browser.addNode("i=85", "Objects", ua.NodeClassObject)
	browser.addNode("ns=2;s=device", "Device", ua.NodeClassObject)
	browser.addNode("ns=2;s=first", "Tag", ua.NodeClassVariable)
	browser.addNode("ns=2;s=second", "Tag", ua.NodeClassVariable)
	browser.addRelation("i=85", "ns=2;s=device", id.Organizes)
	browser.addRelation("ns=2;s=device", "ns=2;s=first", id.HasComponent)
	browser.addRelation("ns=2;s=device", "ns=2;s=second", id.HasComponent)

	discoverer := mustPointDiscoverer(t, browser, nil)
	result, registry := discoverer.discover(context.Background(), []string{"ns=2;s=device"})

	if len(result.Points) != 0 {
		t.Fatalf("conflicting points must be removed, got %#v", result.Points)
	}
	if len(result.Issues) != 2 {
		t.Fatalf("expected both conflicting SourceRefs in issues, got %#v", result.Issues)
	}
	for _, issue := range result.Issues {
		if !strings.Contains(issue.Reason, `PointID "Device.Tag" conflicts`) ||
			!strings.Contains(issue.Reason, "ns=2;s=first") ||
			!strings.Contains(issue.Reason, "ns=2;s=second") {
			t.Fatalf("collision issue lacks both SourceRefs: %#v", issue)
		}
	}
	if len(registry.sourceByPointID) != 0 || len(registry.pointIDBySource) != 0 {
		t.Fatalf("collision overwrote registry: %#v", registry)
	}
}

func TestPointDiscoveryChecksExplicitAndGeneratedPointIDsTogether(t *testing.T) {
	t.Parallel()

	browser := newFakeBrowseService(t)
	browser.addNode("i=85", "Objects", ua.NodeClassObject)
	browser.addNode("ns=2;s=device", "Device", ua.NodeClassObject)
	browser.addNode("ns=2;s=temperature", "Temperature", ua.NodeClassVariable)
	browser.addNode("ns=2;s=pressure", "Pressure", ua.NodeClassVariable)
	browser.addRelation("i=85", "ns=2;s=device", id.Organizes)
	browser.addRelation("ns=2;s=device", "ns=2;s=temperature", id.HasComponent)
	browser.addRelation("ns=2;s=device", "ns=2;s=pressure", id.HasComponent)

	discoverer := mustPointDiscoverer(t, browser, map[string]string{
		"ns=2;s=pressure": "Device.Temperature",
	})
	result, registry := discoverer.discover(context.Background(), []string{"ns=2;s=device"})
	if len(result.Points) != 0 || len(result.Issues) != 2 {
		t.Fatalf("generated and explicit collision was not rejected: %#v", result)
	}
	if len(registry.sourceByPointID) != 0 {
		t.Fatalf("collision entered registry: %#v", registry.sourceByPointID)
	}
}

func TestPointDiscoveryBrowseFailureDoesNotFallbackToConfiguredObject(t *testing.T) {
	t.Parallel()

	browser := newFakeBrowseService(t)
	browser.addNode("i=85", "Objects", ua.NodeClassObject)
	browser.addNode("ns=2;s=device", "Device", ua.NodeClassObject)
	browser.addRelation("i=85", "ns=2;s=device", id.Organizes)
	browser.referenceErrors[browseCall{
		source: "ns=2;s=device", referenceType: id.HasComponent, direction: ua.BrowseDirectionForward,
	}] = errors.New("browse unavailable")

	discoverer := mustPointDiscoverer(t, browser, nil)
	result, registry := discoverer.discover(context.Background(), []string{"ns=2;s=device"})
	if len(result.Points) != 0 || len(registry.sourceByPointID) != 0 {
		t.Fatalf("failed object browse must not fall back to the object: %#v", result)
	}
	if len(result.Issues) != 1 || !strings.Contains(result.Issues[0].Reason, "browse unavailable") {
		t.Fatalf("unexpected discovery issues: %#v", result.Issues)
	}
}

func TestPointDiscoveryKeepsSuccessfulConfigurationWhenAnotherFails(t *testing.T) {
	t.Parallel()

	browser := basicVariableTree(t, "ns=2;s=temperature")
	discoverer := mustPointDiscoverer(t, browser, nil)
	result, _ := discoverer.discover(context.Background(), []string{
		"invalid;node=id",
		"ns=2;s=temperature",
	})

	if len(result.Points) != 1 || result.Points[0].PointID != "Channel.Device.Temperature" {
		t.Fatalf("successful configuration was lost: %#v", result.Points)
	}
	if len(result.Issues) != 1 || result.Issues[0].Source != "invalid;node=id" {
		t.Fatalf("unexpected partial failure diagnostics: %#v", result.Issues)
	}
}

func TestPointDiscoveryRecursiveBrowseStopsAtCycle(t *testing.T) {
	t.Parallel()

	browser := newFakeBrowseService(t)
	browser.addNode("i=85", "Objects", ua.NodeClassObject)
	browser.addNode("ns=2;s=device", "Device", ua.NodeClassObject)
	browser.addNode("ns=2;s=group", "Group", ua.NodeClassObject)
	browser.addNode("ns=2;s=tag", "Tag", ua.NodeClassVariable)
	browser.addRelation("i=85", "ns=2;s=device", id.Organizes)
	browser.addRelation("ns=2;s=device", "ns=2;s=group", id.Organizes)
	browser.addRelation("ns=2;s=group", "ns=2;s=device", id.Organizes)
	browser.addRelation("ns=2;s=group", "ns=2;s=tag", id.HasComponent)

	discoverer := mustPointDiscoverer(t, browser, nil)
	result, _ := discoverer.discover(context.Background(), []string{"ns=2;s=device.*"})
	assertPointIDs(t, result, "Device.Group.Tag")
}

func TestPointIDRemainsStableWhenNodeIDChangesButBrowsePathDoesNot(t *testing.T) {
	t.Parallel()

	discover := func(nodeID string) PointDiscoveryResult {
		browser := basicVariableTree(t, nodeID)
		discoverer := mustPointDiscoverer(t, browser, nil)
		result, _ := discoverer.discover(context.Background(), []string{nodeID})
		return result
	}

	before := discover("ns=2;s=temperature-v1")
	after := discover("ns=2;s=temperature-v2")
	if !reflect.DeepEqual(before.Points, after.Points) {
		t.Fatalf("PointID changed with SourceRef: before=%#v after=%#v", before.Points, after.Points)
	}
}

func TestPointDiscovererRejectsInvalidOverrides(t *testing.T) {
	t.Parallel()

	browser := newFakeBrowseService(t)
	tests := []struct {
		name      string
		overrides []pointIDOverride
	}{
		{name: "invalid source", overrides: []pointIDOverride{{sourceRef: "not;a;node", pointID: "point"}}},
		{name: "empty source", overrides: []pointIDOverride{{sourceRef: " ", pointID: "point"}}},
		{name: "empty point ID", overrides: []pointIDOverride{{sourceRef: "ns=2;s=tag", pointID: "  "}}},
		{
			name: "duplicate normalized source",
			overrides: []pointIDOverride{
				{sourceRef: "ns=0;i=85", pointID: "first"},
				{sourceRef: "i=85", pointID: "second"},
			},
		},
	}
	for _, tt := range tests {
		testCase := tt
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			if _, err := newPointDiscoverer(browser, testCase.overrides); err == nil {
				t.Fatal("expected override validation error")
			}
		})
	}
}

func TestClientInstallsAndReplacesPrivatePointRegistry(t *testing.T) {
	t.Parallel()

	client := &Client{}
	firstRef := mustSourceRef(t, "ns=2;s=first")
	client.installPointRegistry(pointRegistry{
		sourceByPointID:     map[string]SourceRef{"first.point": firstRef},
		pointIDBySource:     map[string]string{firstRef.String(): "first.point"},
		browsePathByPointID: map[string][]string{"first.point": {"First", "Point"}},
	})

	resolvedRef, exists := client.resolveSourceRef("first.point")
	if !exists || resolvedRef != firstRef {
		t.Fatalf("unexpected resolved SourceRef: %#v, %v", resolvedRef, exists)
	}
	resolvedPointID, exists := client.resolvePointID(firstRef)
	if !exists || resolvedPointID != "first.point" {
		t.Fatalf("unexpected resolved PointID: %q, %v", resolvedPointID, exists)
	}
	path, exists := client.resolveBrowsePath("first.point")
	if !exists || !reflect.DeepEqual(path, []string{"First", "Point"}) {
		t.Fatalf("unexpected resolved BrowsePath: %#v, %v", path, exists)
	}
	path[0] = "changed"
	pathAgain, _ := client.resolveBrowsePath("first.point")
	if pathAgain[0] != "First" {
		t.Fatalf("BrowsePath lookup leaked mutable mapping: %#v", pathAgain)
	}

	secondRef := mustSourceRef(t, "ns=2;s=second")
	client.installPointRegistry(pointRegistry{
		sourceByPointID:     map[string]SourceRef{"second.point": secondRef},
		pointIDBySource:     map[string]string{secondRef.String(): "second.point"},
		browsePathByPointID: map[string][]string{"second.point": {"Second", "Point"}},
	})
	if _, exists := client.resolveSourceRef("first.point"); exists {
		t.Fatal("registry replacement retained an old PointID")
	}
	if resolvedRef, exists := client.resolveSourceRef("second.point"); !exists || resolvedRef != secondRef {
		t.Fatalf("new registry was not installed: %#v, %v", resolvedRef, exists)
	}
}

func basicVariableTree(t *testing.T, variableNodeID string) *fakeBrowseService {
	t.Helper()
	browser := newFakeBrowseService(t)
	browser.addNode("i=85", "Objects", ua.NodeClassObject)
	browser.addNode("ns=2;s=channel", "Channel", ua.NodeClassObject)
	browser.addNode("ns=2;s=device", "Device", ua.NodeClassObject)
	browser.addNode(variableNodeID, "Temperature", ua.NodeClassVariable)
	browser.addRelation("i=85", "ns=2;s=channel", id.Organizes)
	browser.addRelation("ns=2;s=channel", "ns=2;s=device", id.Organizes)
	browser.addRelation("ns=2;s=device", variableNodeID, id.HasComponent)
	return browser
}

func ambiguousSourceTree(t *testing.T) *fakeBrowseService {
	t.Helper()
	browser := newFakeBrowseService(t)
	browser.addNode("i=85", "Objects", ua.NodeClassObject)
	browser.addNode("ns=2;s=branch-a", "BranchA", ua.NodeClassObject)
	browser.addNode("ns=2;s=branch-b", "BranchB", ua.NodeClassObject)
	browser.addNode("ns=2;s=shared-tag", "Tag", ua.NodeClassVariable)
	browser.addRelation("i=85", "ns=2;s=branch-a", id.Organizes)
	browser.addRelation("i=85", "ns=2;s=branch-b", id.Organizes)
	browser.addRelation("ns=2;s=branch-a", "ns=2;s=shared-tag", id.HasComponent)
	browser.addRelation("ns=2;s=branch-b", "ns=2;s=shared-tag", id.HasComponent)
	return browser
}

func mustPointDiscoverer(t *testing.T, browser browseService, overrides map[string]string) *pointDiscoverer {
	t.Helper()
	items := make([]pointIDOverride, 0, len(overrides))
	for sourceRef, pointID := range overrides {
		items = append(items, pointIDOverride{sourceRef: sourceRef, pointID: pointID})
	}
	discoverer, err := newPointDiscoverer(browser, items)
	if err != nil {
		t.Fatalf("newPointDiscoverer: %v", err)
	}
	return discoverer
}

func assertPointIDs(t *testing.T, result PointDiscoveryResult, want ...string) {
	t.Helper()
	if len(result.Issues) != 0 {
		t.Fatalf("unexpected discovery issues: %#v", result.Issues)
	}
	got := make([]string, len(result.Points))
	for i, point := range result.Points {
		got[i] = point.PointID
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("PointIDs = %#v, want %#v", got, want)
	}
}

type browseCall struct {
	source        string
	referenceType uint32
	direction     ua.BrowseDirection
}

type fakeBrowseService struct {
	t                *testing.T
	metadataBySource map[string]nodeMetadata
	referencesByCall map[browseCall][]browseReference
	referenceErrors  map[browseCall]error
}

func newFakeBrowseService(t *testing.T) *fakeBrowseService {
	t.Helper()
	return &fakeBrowseService{
		t:                t,
		metadataBySource: make(map[string]nodeMetadata),
		referencesByCall: make(map[browseCall][]browseReference),
		referenceErrors:  make(map[browseCall]error),
	}
}

func (f *fakeBrowseService) addNode(nodeID, browseName string, nodeClass ua.NodeClass) {
	f.t.Helper()
	sourceRef := mustSourceRef(f.t, nodeID)
	f.metadataBySource[sourceRef.String()] = nodeMetadata{
		sourceRef:  sourceRef,
		browseName: browseName,
		nodeClass:  nodeClass,
	}
}

func (f *fakeBrowseService) addRelation(parentNodeID, childNodeID string, referenceType uint32) {
	f.t.Helper()
	parent := f.metadataBySource[mustSourceRef(f.t, parentNodeID).String()]
	child := f.metadataBySource[mustSourceRef(f.t, childNodeID).String()]
	f.referencesByCall[browseCall{
		source: parent.sourceRef.String(), referenceType: referenceType, direction: ua.BrowseDirectionForward,
	}] = append(f.referencesByCall[browseCall{
		source: parent.sourceRef.String(), referenceType: referenceType, direction: ua.BrowseDirectionForward,
	}], browseReference{
		sourceRef: child.sourceRef, browseName: child.browseName, nodeClass: child.nodeClass,
		visitKey: child.sourceRef.String(),
	})
	f.referencesByCall[browseCall{
		source: child.sourceRef.String(), referenceType: id.HierarchicalReferences, direction: ua.BrowseDirectionInverse,
	}] = append(f.referencesByCall[browseCall{
		source: child.sourceRef.String(), referenceType: id.HierarchicalReferences, direction: ua.BrowseDirectionInverse,
	}], browseReference{
		sourceRef: parent.sourceRef, browseName: parent.browseName, nodeClass: parent.nodeClass,
		visitKey: parent.sourceRef.String(),
	})
}

func (f *fakeBrowseService) metadata(_ context.Context, sourceRef SourceRef) (nodeMetadata, error) {
	metadata, exists := f.metadataBySource[sourceRef.String()]
	if !exists {
		return nodeMetadata{}, errors.New("metadata not found")
	}
	return metadata, nil
}

func (f *fakeBrowseService) references(
	_ context.Context,
	sourceRef SourceRef,
	referenceType uint32,
	direction ua.BrowseDirection,
	nodeClass ua.NodeClass,
) ([]browseReference, error) {
	call := browseCall{source: sourceRef.String(), referenceType: referenceType, direction: direction}
	if err := f.referenceErrors[call]; err != nil {
		return nil, err
	}
	refs := f.referencesByCall[call]
	result := make([]browseReference, 0, len(refs))
	for _, ref := range refs {
		if nodeClass != ua.NodeClassAll && ref.nodeClass&nodeClass == 0 {
			continue
		}
		result = append(result, ref)
	}
	return result, nil
}

func mustSourceRef(t *testing.T, value string) SourceRef {
	t.Helper()
	sourceRef, err := parseSourceRef(value)
	if err != nil {
		t.Fatalf("parseSourceRef(%q): %v", value, err)
	}
	return sourceRef
}

func TestMergePointRegistryPreservesExistingPoints(t *testing.T) {
	t.Parallel()

	oldRef := mustSourceRef(t, "ns=2;s=old")
	newRef := mustSourceRef(t, "ns=2;s=new")
	client := &Client{
		sourceByPointID:     map[string]SourceRef{"old.point": oldRef},
		pointIDBySource:     map[string]string{oldRef.String(): "old.point"},
		browsePathByPointID: map[string][]string{"old.point": {"Objects", "Old"}},
	}
	addition := pointRegistry{
		sourceByPointID:     map[string]SourceRef{"new.point": newRef},
		pointIDBySource:     map[string]string{newRef.String(): "new.point"},
		browsePathByPointID: map[string][]string{"new.point": {"Objects", "New"}},
	}
	merged, result := client.mergePointRegistry(addition, PointDiscoveryResult{Points: []DiscoveredPoint{{PointID: "new.point"}}})
	if len(result.Issues) != 0 || len(result.Points) != 1 {
		t.Fatalf("merge result = %#v", result)
	}
	if merged.sourceByPointID["old.point"].String() != oldRef.String() || merged.sourceByPointID["new.point"].String() != newRef.String() {
		t.Fatalf("merged registry = %#v", merged)
	}
}

func TestFailedConfiguredNodesDoesNotRetrySuccessfulEntries(t *testing.T) {
	t.Parallel()

	configured := []string{"ns=2;s=good", "ns=2;s=failed", "ns=2;s=other"}
	issues := []DiscoveryIssue{
		{Source: "ns=2;s=failed", Reason: "browse failed"},
		{Source: "ns=2;s=ambiguous-source", Reason: "identity ambiguous"},
	}
	got := failedConfiguredNodes(configured, issues)
	want := []string{"ns=2;s=failed"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("failed configured nodes = %#v, want %#v", got, want)
	}
}
