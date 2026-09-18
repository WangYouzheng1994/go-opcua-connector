package writeback

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"go-opcua-connector/internal/collector"
)

const MaxRequestItems = 100

// ErrorCode 是回写协议对外稳定的错误分类。
type ErrorCode string

const (
	ErrorCodeInvalidRequest        ErrorCode = "INVALID_REQUEST"
	ErrorCodeWriteQueueFull        ErrorCode = "WRITE_QUEUE_FULL"
	ErrorCodePointNotFound         ErrorCode = "POINT_NOT_FOUND"
	ErrorCodeDataTypeReadFailed    ErrorCode = "DATATYPE_READ_FAILED"
	ErrorCodeValueConversionFailed ErrorCode = "VALUE_CONVERSION_FAILED"
	ErrorCodeValueOutOfRange       ErrorCode = "VALUE_OUT_OF_RANGE"
	ErrorCodeWriteTimeout          ErrorCode = "WRITE_TIMEOUT"
	ErrorCodeWriteFailed           ErrorCode = "WRITE_FAILED"
)

// Request 是单点和批量回写共用的请求信封。
// RequestID 只用于结果关联和日志追踪，不提供幂等或去重语义。
type Request struct {
	RequestID string
	Items     []Item
}

// Item 是一项 PointID 回写请求。Value 保留 JSON 原始数值精度，供后续按服务端 DataType 转换。
type Item struct {
	PointID collector.PointID
	Value   any
}

// Result 是一个请求唯一对应的汇总结果。
type Result struct {
	RequestID string       `json:"request_id"`
	Success   bool         `json:"success"`
	Code      ErrorCode    `json:"code,omitempty"`
	Message   string       `json:"message,omitempty"`
	Results   []ItemResult `json:"results"`
}

// ItemResult 是单个回写项的执行结果。
type ItemResult struct {
	PointID     collector.PointID `json:"point_id"`
	Success     bool              `json:"success"`
	Code        ErrorCode         `json:"code,omitempty"`
	Message     string            `json:"message,omitempty"`
	OPCUAStatus string            `json:"opcua_status,omitempty"`
}

// InvalidRequestError 表示消息无法通过统一回写协议校验。
type InvalidRequestError struct {
	Message string
}

func (e *InvalidRequestError) Error() string {
	return e.Message
}

// Code 返回统一协议对应的稳定错误码。
func (e *InvalidRequestError) Code() ErrorCode {
	return ErrorCodeInvalidRequest
}

type requestEnvelope struct {
	RequestID string        `json:"request_id"`
	Items     []requestItem `json:"items"`
}

type requestItem struct {
	PointID string          `json:"point_id"`
	Value   json.RawMessage `json:"value"`
}

// DecodeRequest 严格解码并校验统一回写请求。未知字段和尾随 JSON 均视为非法请求。
func DecodeRequest(data []byte) (Request, error) {
	var envelope requestEnvelope
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&envelope); err != nil {
		return Request{}, invalidRequest("invalid JSON: %v", err)
	}
	if err := ensureJSONEnd(decoder); err != nil {
		return Request{}, err
	}
	if strings.TrimSpace(envelope.RequestID) == "" {
		return Request{}, invalidRequest("request_id is required")
	}
	if len(envelope.Items) == 0 {
		return Request{}, invalidRequest("items must contain at least one item")
	}
	if len(envelope.Items) > MaxRequestItems {
		return Request{}, invalidRequest("items exceeds maximum limit of %d", MaxRequestItems)
	}

	request := Request{
		RequestID: envelope.RequestID,
		Items:     make([]Item, len(envelope.Items)),
	}
	for i, item := range envelope.Items {
		if strings.TrimSpace(item.PointID) == "" {
			return Request{}, invalidRequest("items[%d].point_id is required", i)
		}
		if len(item.Value) == 0 {
			return Request{}, invalidRequest("items[%d].value is required", i)
		}
		if bytes.Equal(bytes.TrimSpace(item.Value), []byte("null")) {
			return Request{}, invalidRequest("items[%d].value must not be null", i)
		}
		value, err := decodeValue(item.Value)
		if err != nil {
			return Request{}, invalidRequest("items[%d].value is invalid: %v", i, err)
		}
		request.Items[i] = Item{
			PointID: collector.PointID(item.PointID),
			Value:   value,
		}
	}
	return request, nil
}

// NewRejectedResult 创建未进入执行队列的请求级失败结果。
func NewRejectedResult(requestID string, code ErrorCode, message string) Result {
	return Result{
		RequestID: requestID,
		Success:   false,
		Code:      code,
		Message:   message,
		Results:   make([]ItemResult, 0),
	}
}

// requestIDFromPayload 在完整 JSON 对象中提取可用的 request_id，供协议拒绝结果关联。
// 无法完整解析、字段缺失或字段类型错误时返回空字符串。
func requestIDFromPayload(data []byte) string {
	var object map[string]json.RawMessage
	if err := json.Unmarshal(data, &object); err != nil {
		return ""
	}
	raw, ok := object["request_id"]
	if !ok {
		return ""
	}
	var requestID string
	if err := json.Unmarshal(raw, &requestID); err != nil || strings.TrimSpace(requestID) == "" {
		return ""
	}
	return requestID
}

func decodeValue(raw json.RawMessage) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	if err := ensureJSONEnd(decoder); err != nil {
		return nil, err
	}
	return value, nil
}

func ensureJSONEnd(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return invalidRequest("message contains multiple JSON values")
		}
		return invalidRequest("invalid trailing JSON: %v", err)
	}
	return nil
}

func invalidRequest(format string, args ...any) error {
	return &InvalidRequestError{Message: fmt.Sprintf(format, args...)}
}
