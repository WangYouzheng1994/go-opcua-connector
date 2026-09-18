package opcua

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"go-opcua-connector/internal/collector"

	goopcua "github.com/gopcua/opcua"
	"github.com/gopcua/opcua/ua"
)

// ErrPointNotFound 表示当前 Point Registry 中不存在请求的 PointID。
// 调用方应通过 errors.Is 判断，不应依赖错误文本。
var ErrPointNotFound = errors.New("point ID not found")

type writeStatusError struct {
	status ua.StatusCode
}

func (e *writeStatusError) Error() string {
	return fmt.Sprintf("OPC UA write rejected: %s", e.status)
}

// WriteStatusOf 返回 Write 失败携带的 OPC UA Status 名称。
// Status 仅用于诊断，不参与上层稳定错误码分类。
func WriteStatusOf(err error) (string, bool) {
	var statusErr *writeStatusError
	if errors.As(err, &statusErr) {
		return writeStatusName(statusErr.status), true
	}
	var status ua.StatusCode
	if errors.As(err, &status) {
		return writeStatusName(status), true
	}
	return "", false
}

func writeStatusName(status ua.StatusCode) string {
	description, ok := ua.StatusCodes[status]
	if !ok {
		return fmt.Sprintf("0x%X", uint32(status))
	}
	return strings.TrimPrefix(description.Name, "Status")
}

// WriteTarget 是一次 PointID 解析得到的稳定回写目标。
// 它只暴露业务身份和回写操作，不暴露 SourceRef 或任何 OPC UA 节点身份。
type WriteTarget interface {
	PointID() collector.PointID
	ReadDataType(ctx context.Context) (string, error)
	Write(ctx context.Context, value any) error
}

type resolvedWriteTarget struct {
	client    *Client
	pointID   collector.PointID
	sourceRef SourceRef
}

func (t *resolvedWriteTarget) PointID() collector.PointID {
	return t.pointID
}

// String 和 GoString 确保日志格式化目标对象时只显示 PointID。
func (t *resolvedWriteTarget) String() string {
	return fmt.Sprintf("WriteTarget(%s)", t.pointID)
}

func (t *resolvedWriteTarget) GoString() string {
	return t.String()
}

func (t *resolvedWriteTarget) ReadDataType(ctx context.Context) (string, error) {
	return t.client.readWriteTargetDataType(ctx, t.sourceRef)
}

func (t *resolvedWriteTarget) Write(ctx context.Context, value any) error {
	return t.client.writeResolvedTarget(ctx, t.sourceRef, value)
}

// ResolveWriteTarget 使用当前 Point Registry 将 PointID 解析为稳定回写目标。
// 返回的目标保存本次解析得到的 SourceRef，后续操作不会再次查询 Point Registry。
func (c *Client) ResolveWriteTarget(pointID collector.PointID) (WriteTarget, error) {
	sourceRef, ok := c.resolveSourceRef(string(pointID))
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrPointNotFound, pointID)
	}
	return &resolvedWriteTarget{
		client:    c,
		pointID:   pointID,
		sourceRef: sourceRef,
	}, nil
}

type writeTargetBackend interface {
	Read(ctx context.Context, request *ua.ReadRequest) (*ua.ReadResponse, error)
	Write(ctx context.Context, request *ua.WriteRequest) (*ua.WriteResponse, error)
}

func (c *Client) currentWriteTargetBackend() (writeTargetBackend, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.writeTargetBackend != nil {
		return c.writeTargetBackend, nil
	}
	if c.client == nil {
		return nil, errors.New("client not connected")
	}
	return c.client, nil
}

func (c *Client) readWriteTargetDataType(ctx context.Context, sourceRef SourceRef) (string, error) {
	backend, err := c.currentWriteTargetBackend()
	if err != nil {
		return "", err
	}
	nodeID, err := sourceRef.node()
	if err != nil {
		return "", errors.New("write target contains an invalid internal source reference")
	}
	response, err := backend.Read(ctx, &ua.ReadRequest{
		NodesToRead: []*ua.ReadValueID{{
			NodeID:      nodeID,
			AttributeID: ua.AttributeIDDataType,
		}},
		TimestampsToReturn: ua.TimestampsToReturnNeither,
	})
	if err != nil {
		return "", fmt.Errorf("read data type failed: %w", err)
	}
	if response == nil || len(response.Results) == 0 || response.Results[0] == nil {
		return "", errors.New("read data type returned no result")
	}
	result := response.Results[0]
	if result.Status != ua.StatusOK {
		return "", fmt.Errorf("read data type rejected: %s", result.Status)
	}
	if result.Value == nil || result.Value.NodeID() == nil {
		return "", errors.New("read data type returned no type ID")
	}
	return writeTargetDataTypeName(result.Value.NodeID())
}

func (c *Client) writeResolvedTarget(ctx context.Context, sourceRef SourceRef, value any) error {
	backend, err := c.currentWriteTargetBackend()
	if err != nil {
		return err
	}
	nodeID, err := sourceRef.node()
	if err != nil {
		return errors.New("write target contains an invalid internal source reference")
	}
	variant, err := ua.NewVariant(value)
	if err != nil {
		return fmt.Errorf("failed to create variant: %w", err)
	}
	response, err := backend.Write(ctx, &ua.WriteRequest{
		NodesToWrite: []*ua.WriteValue{{
			NodeID:      nodeID,
			AttributeID: ua.AttributeIDValue,
			Value: &ua.DataValue{
				EncodingMask: ua.DataValueValue,
				Value:        variant,
			},
		}},
	})
	if err != nil {
		var status ua.StatusCode
		if errors.As(err, &status) {
			return &writeStatusError{status: status}
		}
		return fmt.Errorf("write failed: %w", err)
	}
	if response == nil || len(response.Results) == 0 {
		return errors.New("write returned no result")
	}
	if response.Results[0] != ua.StatusOK {
		return &writeStatusError{status: response.Results[0]}
	}
	return nil
}

func writeTargetDataTypeName(typeID *ua.NodeID) (string, error) {
	if typeID == nil || typeID.Namespace() != 0 {
		return "", errors.New("read data type returned an unsupported type")
	}
	switch typeID.Type() {
	case ua.NodeIDTypeTwoByte, ua.NodeIDTypeFourByte, ua.NodeIDTypeNumeric:
	default:
		return "", errors.New("read data type returned an unsupported type")
	}
	if typeID.IntID() > 24 {
		return "", errors.New("read data type returned an unsupported type")
	}
	return typeIDToString(typeID), nil
}

var _ writeTargetBackend = (*goopcua.Client)(nil)
