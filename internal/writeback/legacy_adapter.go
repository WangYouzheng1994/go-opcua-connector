package writeback

import (
	"bytes"
	"encoding/json"
	"strconv"
	"strings"
	"sync/atomic"

	"go-opcua-connector/internal/collector"
)

var legacyRequestSequence atomic.Uint64

type legacyRequestItem struct {
	ID    string          `json:"id"`
	Value json.RawMessage `json:"v"`
}

// decodeIncomingRequest 在消息入口兼容临时旧数组协议。
// 对象请求仍由统一协议严格解码；数组请求只转换身份字段，后续共用同一执行链路。
func decodeIncomingRequest(data []byte) (Request, error) {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 || trimmed[0] != '[' {
		return DecodeRequest(data)
	}
	return decodeLegacyRequest(trimmed)
}

func decodeLegacyRequest(data []byte) (Request, error) {
	var legacyItems []legacyRequestItem
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&legacyItems); err != nil {
		return Request{}, invalidRequest("invalid legacy JSON: %v", err)
	}
	if err := ensureJSONEnd(decoder); err != nil {
		return Request{}, err
	}
	if len(legacyItems) == 0 {
		return Request{}, invalidRequest("legacy items must contain at least one item")
	}
	if len(legacyItems) > MaxRequestItems {
		return Request{}, invalidRequest("legacy items exceeds maximum limit of %d", MaxRequestItems)
	}

	request := Request{
		Items: make([]Item, len(legacyItems)),
	}
	for index, legacyItem := range legacyItems {
		if strings.TrimSpace(legacyItem.ID) == "" {
			return Request{}, invalidRequest("legacy items[%d].id is required", index)
		}
		if len(legacyItem.Value) == 0 {
			return Request{}, invalidRequest("legacy items[%d].v is required", index)
		}
		if bytes.Equal(bytes.TrimSpace(legacyItem.Value), []byte("null")) {
			return Request{}, invalidRequest("legacy items[%d].v must not be null", index)
		}
		value, err := decodeValue(legacyItem.Value)
		if err != nil {
			return Request{}, invalidRequest("legacy items[%d].v is invalid: %v", index, err)
		}
		request.Items[index] = Item{PointID: collector.PointID(legacyItem.ID), Value: value}
	}
	request.RequestID = nextLegacyRequestID()
	return request, nil
}

func nextLegacyRequestID() string {
	return "legacy-" + strconv.FormatUint(legacyRequestSequence.Add(1), 10)
}
