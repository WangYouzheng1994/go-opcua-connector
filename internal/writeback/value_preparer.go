package writeback

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"strconv"

	"go-opcua-connector/internal/collector"
	"go-opcua-connector/internal/opcua"
)

// DataTypeCache 保存当前 Connector 进程内 PointID 对应的服务端 DataType。
// 最终执行模型为单 Worker，因此这里使用普通 map，不增加并发控制和失效机制。
type DataTypeCache struct {
	values map[collector.PointID]string
}

func NewDataTypeCache() *DataTypeCache {
	return &DataTypeCache{values: make(map[collector.PointID]string)}
}

// GetOrRead 返回 PointID 对应的权威服务端 DataType。
// 仅成功读取的非空类型会进入缓存。
func (c *DataTypeCache) GetOrRead(ctx context.Context, target opcua.WriteTarget) (string, error) {
	if target == nil {
		return "", newCodedError(ErrorCodeDataTypeReadFailed, "write target is required", nil)
	}
	if c.values == nil {
		c.values = make(map[collector.PointID]string)
	}
	pointID := target.PointID()
	if dataType, ok := c.values[pointID]; ok {
		return dataType, nil
	}
	dataType, err := target.ReadDataType(ctx)
	if err != nil {
		return "", newCodedError(ErrorCodeDataTypeReadFailed, "failed to read server data type", err)
	}
	if dataType == "" {
		return "", newCodedError(ErrorCodeDataTypeReadFailed, "server returned an empty data type", nil)
	}
	c.values[pointID] = dataType
	return dataType, nil
}

// ValuePreparer 按服务端 DataType 将请求原始值转换为 OPC UA Write 可接收的 Go 值。
// Prepare 不执行 Write。
type ValuePreparer struct {
	dataTypes DataTypeCache
}

func NewValuePreparer() *ValuePreparer {
	return &ValuePreparer{dataTypes: DataTypeCache{values: make(map[collector.PointID]string)}}
}

func (p *ValuePreparer) Prepare(ctx context.Context, target opcua.WriteTarget, raw any) (any, error) {
	dataType, err := p.dataTypes.GetOrRead(ctx, target)
	if err != nil {
		return nil, err
	}
	return ConvertValue(raw, dataType)
}

// ConvertValue 按服务端 DataType 执行受控转换，不推断目标类型，也不回退到 String。
func ConvertValue(raw any, dataType string) (any, error) {
	switch dataType {
	case "Boolean":
		return convertBoolean(raw)
	case "SByte":
		return convertSignedInteger(raw, 8)
	case "Byte":
		return convertUnsignedInteger(raw, 8)
	case "Int16":
		return convertSignedInteger(raw, 16)
	case "UInt16":
		return convertUnsignedInteger(raw, 16)
	case "Int32":
		return convertSignedInteger(raw, 32)
	case "UInt32":
		return convertUnsignedInteger(raw, 32)
	case "Int64":
		return convertSignedInteger(raw, 64)
	case "UInt64":
		return convertUnsignedInteger(raw, 64)
	case "Float":
		return convertFloat(raw, 32)
	case "Double":
		return convertFloat(raw, 64)
	case "String":
		value, ok := raw.(string)
		if !ok {
			return nil, conversionError("String accepts only a JSON string")
		}
		return value, nil
	default:
		return nil, conversionError("unsupported server data type %q", dataType)
	}
}

// ErrorCodeOf 提取回写协议稳定错误码，不要求调用方解析错误文本。
func ErrorCodeOf(err error) (ErrorCode, bool) {
	var coded interface{ Code() ErrorCode }
	if !errors.As(err, &coded) {
		return "", false
	}
	return coded.Code(), true
}

type codedError struct {
	code    ErrorCode
	message string
	cause   error
}

func (e *codedError) Error() string {
	if e.cause == nil {
		return e.message
	}
	return fmt.Sprintf("%s: %v", e.message, e.cause)
}

func (e *codedError) Unwrap() error {
	return e.cause
}

func (e *codedError) Code() ErrorCode {
	return e.code
}

func newCodedError(code ErrorCode, message string, cause error) error {
	return &codedError{code: code, message: message, cause: cause}
}

func conversionError(format string, args ...any) error {
	return newCodedError(ErrorCodeValueConversionFailed, fmt.Sprintf(format, args...), nil)
}

func rangeError(format string, args ...any) error {
	return newCodedError(ErrorCodeValueOutOfRange, fmt.Sprintf(format, args...), nil)
}

func convertBoolean(raw any) (any, error) {
	switch value := raw.(type) {
	case bool:
		return value, nil
	case string:
		switch value {
		case "true", "1":
			return true, nil
		case "false", "0":
			return false, nil
		default:
			return nil, conversionError("value is not an allowed Boolean string")
		}
	case json.Number:
		switch value.String() {
		case "1":
			return true, nil
		case "0":
			return false, nil
		default:
			return nil, conversionError("Boolean number must be 0 or 1")
		}
	default:
		return nil, conversionError("value cannot be converted to Boolean")
	}
}

func convertSignedInteger(raw any, bitSize int) (any, error) {
	integer, err := exactInteger(raw)
	if err != nil {
		return nil, err
	}
	min := new(big.Int).Neg(new(big.Int).Lsh(big.NewInt(1), uint(bitSize-1)))
	max := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), uint(bitSize-1)), big.NewInt(1))
	if integer.Cmp(min) < 0 || integer.Cmp(max) > 0 {
		return nil, rangeError("integer is outside Int%d range", bitSize)
	}
	value := integer.Int64()
	switch bitSize {
	case 8:
		return int8(value), nil
	case 16:
		return int16(value), nil
	case 32:
		return int32(value), nil
	case 64:
		return value, nil
	default:
		return nil, conversionError("unsupported signed integer width %d", bitSize)
	}
}

func convertUnsignedInteger(raw any, bitSize int) (any, error) {
	integer, err := exactInteger(raw)
	if err != nil {
		return nil, err
	}
	if integer.Sign() < 0 {
		return nil, rangeError("negative value cannot be written to UInt%d", bitSize)
	}
	max := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), uint(bitSize)), big.NewInt(1))
	if integer.Cmp(max) > 0 {
		return nil, rangeError("integer is outside UInt%d range", bitSize)
	}
	value := integer.Uint64()
	switch bitSize {
	case 8:
		return uint8(value), nil
	case 16:
		return uint16(value), nil
	case 32:
		return uint32(value), nil
	case 64:
		return value, nil
	default:
		return nil, conversionError("unsupported unsigned integer width %d", bitSize)
	}
}

func exactInteger(raw any) (*big.Int, error) {
	switch value := raw.(type) {
	case json.Number:
		rational, ok := new(big.Rat).SetString(value.String())
		if !ok {
			return nil, conversionError("value is not a valid JSON number")
		}
		if !rational.IsInt() {
			return nil, conversionError("fractional value cannot be written to an integer type")
		}
		return new(big.Int).Set(rational.Num()), nil
	case string:
		integer, ok := new(big.Int).SetString(value, 10)
		if !ok {
			return nil, conversionError("value is not a valid integer string")
		}
		return integer, nil
	default:
		return nil, conversionError("integer type accepts only a JSON number or integer string")
	}
}

func convertFloat(raw any, bitSize int) (any, error) {
	var text string
	switch value := raw.(type) {
	case json.Number:
		text = value.String()
	case string:
		text = value
	default:
		return nil, conversionError("floating-point type accepts only a JSON number or numeric string")
	}
	value, err := strconv.ParseFloat(text, bitSize)
	if err != nil {
		if errors.Is(err, strconv.ErrRange) {
			return nil, rangeError("floating-point value is outside Float%d range", bitSize)
		}
		return nil, conversionError("value is not a valid floating-point number")
	}
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return nil, rangeError("NaN and Inf are not allowed")
	}
	if bitSize == 32 {
		converted := float32(value)
		if math.IsInf(float64(converted), 0) {
			return nil, rangeError("floating-point value is outside Float32 range")
		}
		return converted, nil
	}
	return value, nil
}
