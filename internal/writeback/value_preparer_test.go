package writeback

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"go-opcua-connector/internal/collector"
)

func TestDataTypeCacheMissThenHitByPointID(t *testing.T) {
	t.Parallel()

	cache := NewDataTypeCache()
	first := &fakePreparationTarget{pointID: "motor.speed", dataType: "Int32"}
	second := &fakePreparationTarget{pointID: "motor.speed", dataType: "Boolean"}
	ctx := context.WithValue(context.Background(), preparationContextKey{}, "cache")

	dataType, err := cache.GetOrRead(ctx, first)
	if err != nil || dataType != "Int32" {
		t.Fatalf("first GetOrRead() = %q, %v", dataType, err)
	}
	dataType, err = cache.GetOrRead(ctx, second)
	if err != nil || dataType != "Int32" {
		t.Fatalf("second GetOrRead() = %q, %v", dataType, err)
	}
	if first.readCount != 1 || second.readCount != 0 {
		t.Fatalf("read counts = %d, %d; want 1, 0", first.readCount, second.readCount)
	}
	if first.readContext != ctx {
		t.Fatal("cache miss did not pass the caller context to ReadDataType")
	}
}

func TestDataTypeCacheDoesNotCacheReadFailureOrEmptyType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		dataType string
		readErr  error
	}{
		{name: "read error", readErr: errors.New("read unavailable")},
		{name: "empty type"},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			cache := NewDataTypeCache()
			target := &fakePreparationTarget{pointID: "motor.speed", dataType: test.dataType, readErr: test.readErr}
			for attempt := 0; attempt < 2; attempt++ {
				_, err := cache.GetOrRead(context.Background(), target)
				assertErrorCode(t, err, ErrorCodeDataTypeReadFailed)
			}
			if target.readCount != 2 {
				t.Fatalf("ReadDataType() calls = %d, want 2", target.readCount)
			}
		})
	}
}

func TestNewDataTypeCacheStartsEmpty(t *testing.T) {
	t.Parallel()

	firstCache := NewDataTypeCache()
	firstTarget := &fakePreparationTarget{pointID: "motor.speed", dataType: "Int32"}
	if _, err := firstCache.GetOrRead(context.Background(), firstTarget); err != nil {
		t.Fatalf("first cache GetOrRead() error = %v", err)
	}

	secondCache := NewDataTypeCache()
	secondTarget := &fakePreparationTarget{pointID: "motor.speed", dataType: "Int32"}
	if _, err := secondCache.GetOrRead(context.Background(), secondTarget); err != nil {
		t.Fatalf("second cache GetOrRead() error = %v", err)
	}
	if firstTarget.readCount != 1 || secondTarget.readCount != 1 {
		t.Fatalf("read counts = %d, %d; want 1, 1", firstTarget.readCount, secondTarget.readCount)
	}
}

func TestValuePreparerUsesServerDataTypeAndNeverWrites(t *testing.T) {
	t.Parallel()

	preparer := NewValuePreparer()
	target := &fakePreparationTarget{pointID: "motor.speed", dataType: "Int32"}
	value, err := preparer.Prepare(context.Background(), target, json.Number("1500"))
	if err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	if value != int32(1500) {
		t.Fatalf("prepared value = %#v (%T)", value, value)
	}
	_, err = preparer.Prepare(context.Background(), target, json.Number("1500.5"))
	assertErrorCode(t, err, ErrorCodeValueConversionFailed)
	if target.writeCount != 0 {
		t.Fatalf("Write() calls = %d, want 0", target.writeCount)
	}
}

func TestConvertBooleanControlledInputs(t *testing.T) {
	t.Parallel()

	valid := []struct {
		raw  any
		want bool
	}{
		{raw: true, want: true},
		{raw: false, want: false},
		{raw: "true", want: true},
		{raw: "false", want: false},
		{raw: "1", want: true},
		{raw: "0", want: false},
		{raw: json.Number("1"), want: true},
		{raw: json.Number("0"), want: false},
	}
	for _, test := range valid {
		value, err := ConvertValue(test.raw, "Boolean")
		if err != nil || value != test.want {
			t.Errorf("ConvertValue(%#v, Boolean) = %#v, %v; want %v", test.raw, value, err, test.want)
		}
	}

	for _, raw := range []any{json.Number("2"), json.Number("-1"), json.Number("1.0"), "yes", "no", "on", "off", "TRUE", 1} {
		_, err := ConvertValue(raw, "Boolean")
		assertErrorCode(t, err, ErrorCodeValueConversionFailed)
	}
}

func TestConvertSignedIntegerBoundaries(t *testing.T) {
	t.Parallel()

	valid := []struct {
		dataType string
		raw      any
		want     any
	}{
		{dataType: "SByte", raw: json.Number("-128"), want: int8(-128)},
		{dataType: "SByte", raw: json.Number("127"), want: int8(127)},
		{dataType: "Int16", raw: json.Number("-32768"), want: int16(-32768)},
		{dataType: "Int16", raw: json.Number("32767"), want: int16(32767)},
		{dataType: "Int32", raw: json.Number("-2147483648"), want: int32(-2147483648)},
		{dataType: "Int32", raw: json.Number("2147483647"), want: int32(2147483647)},
		{dataType: "Int64", raw: json.Number("-9223372036854775808"), want: int64(-9223372036854775808)},
		{dataType: "Int64", raw: json.Number("9223372036854775807"), want: int64(9223372036854775807)},
		{dataType: "Int32", raw: json.Number("1500.0"), want: int32(1500)},
		{dataType: "Int32", raw: "1500", want: int32(1500)},
	}
	for _, test := range valid {
		value, err := ConvertValue(test.raw, test.dataType)
		if err != nil || !reflect.DeepEqual(value, test.want) {
			t.Errorf("ConvertValue(%#v, %s) = %#v (%T), %v; want %#v (%T)", test.raw, test.dataType, value, value, err, test.want, test.want)
		}
	}

	outOfRange := []struct {
		dataType string
		raw      json.Number
	}{
		{dataType: "SByte", raw: "-129"},
		{dataType: "SByte", raw: "128"},
		{dataType: "Int16", raw: "-32769"},
		{dataType: "Int16", raw: "32768"},
		{dataType: "Int32", raw: "-2147483649"},
		{dataType: "Int32", raw: "2147483648"},
		{dataType: "Int64", raw: "-9223372036854775809"},
		{dataType: "Int64", raw: "9223372036854775808"},
	}
	for _, test := range outOfRange {
		_, err := ConvertValue(test.raw, test.dataType)
		assertErrorCode(t, err, ErrorCodeValueOutOfRange)
	}
	_, err := ConvertValue(json.Number("1500.5"), "Int32")
	assertErrorCode(t, err, ErrorCodeValueConversionFailed)
}

func TestConvertUnsignedIntegerBoundaries(t *testing.T) {
	t.Parallel()

	valid := []struct {
		dataType string
		raw      any
		want     any
	}{
		{dataType: "Byte", raw: json.Number("255"), want: uint8(255)},
		{dataType: "UInt16", raw: json.Number("65535"), want: uint16(65535)},
		{dataType: "UInt32", raw: json.Number("4294967295"), want: uint32(4294967295)},
		{dataType: "UInt64", raw: json.Number("18446744073709551615"), want: uint64(18446744073709551615)},
		{dataType: "UInt64", raw: "18446744073709551615", want: uint64(18446744073709551615)},
	}
	for _, test := range valid {
		value, err := ConvertValue(test.raw, test.dataType)
		if err != nil || !reflect.DeepEqual(value, test.want) {
			t.Errorf("ConvertValue(%#v, %s) = %#v (%T), %v; want %#v (%T)", test.raw, test.dataType, value, value, err, test.want, test.want)
		}
	}

	outOfRange := []struct {
		dataType string
		raw      json.Number
	}{
		{dataType: "Byte", raw: "-1"},
		{dataType: "Byte", raw: "256"},
		{dataType: "UInt16", raw: "65536"},
		{dataType: "UInt32", raw: "4294967296"},
		{dataType: "UInt64", raw: "18446744073709551616"},
	}
	for _, test := range outOfRange {
		_, err := ConvertValue(test.raw, test.dataType)
		assertErrorCode(t, err, ErrorCodeValueOutOfRange)
	}
	_, err := ConvertValue(json.Number("1.5"), "UInt32")
	assertErrorCode(t, err, ErrorCodeValueConversionFailed)
}

func TestConvertIntegerRejectsInvalidInputs(t *testing.T) {
	t.Parallel()

	for _, raw := range []any{"1500.0", "not-a-number", true, nil} {
		_, err := ConvertValue(raw, "Int32")
		assertErrorCode(t, err, ErrorCodeValueConversionFailed)
	}
}

func TestDecodeAndConvertPreservesLargeIntegerPrecision(t *testing.T) {
	t.Parallel()

	request, err := DecodeRequest([]byte(`{"request_id":"large","items":[{"point_id":"counter","value":18446744073709551615}]}`))
	if err != nil {
		t.Fatalf("DecodeRequest() error = %v", err)
	}
	value, err := ConvertValue(request.Items[0].Value, "UInt64")
	if err != nil {
		t.Fatalf("ConvertValue() error = %v", err)
	}
	if value != uint64(18446744073709551615) {
		t.Fatalf("value = %#v (%T)", value, value)
	}
}

func TestConvertFloatAndDouble(t *testing.T) {
	t.Parallel()

	valid := []struct {
		dataType string
		raw      any
		want     any
	}{
		{dataType: "Float", raw: json.Number("25.5"), want: float32(25.5)},
		{dataType: "Float", raw: "1500", want: float32(1500)},
		{dataType: "Double", raw: json.Number("25.5"), want: float64(25.5)},
		{dataType: "Double", raw: "25.5", want: float64(25.5)},
	}
	for _, test := range valid {
		value, err := ConvertValue(test.raw, test.dataType)
		if err != nil || !reflect.DeepEqual(value, test.want) {
			t.Errorf("ConvertValue(%#v, %s) = %#v (%T), %v; want %#v (%T)", test.raw, test.dataType, value, value, err, test.want, test.want)
		}
	}

	for _, test := range []struct {
		dataType string
		raw      any
		code     ErrorCode
	}{
		{dataType: "Double", raw: "not-a-number", code: ErrorCodeValueConversionFailed},
		{dataType: "Double", raw: "NaN", code: ErrorCodeValueOutOfRange},
		{dataType: "Double", raw: "Inf", code: ErrorCodeValueOutOfRange},
		{dataType: "Double", raw: "-Inf", code: ErrorCodeValueOutOfRange},
		{dataType: "Double", raw: "1e10000", code: ErrorCodeValueOutOfRange},
		{dataType: "Float", raw: "3.5e38", code: ErrorCodeValueOutOfRange},
		{dataType: "Float", raw: true, code: ErrorCodeValueConversionFailed},
	} {
		_, err := ConvertValue(test.raw, test.dataType)
		assertErrorCode(t, err, test.code)
	}
}

func TestConvertStringDoesNotCoerceOtherTypes(t *testing.T) {
	t.Parallel()

	value, err := ConvertValue("1500", "String")
	if err != nil || value != "1500" {
		t.Fatalf("ConvertValue(String) = %#v, %v", value, err)
	}
	for _, raw := range []any{json.Number("1500"), true, false} {
		_, err := ConvertValue(raw, "String")
		assertErrorCode(t, err, ErrorCodeValueConversionFailed)
	}
}

func TestConvertRejectsUnsupportedDataType(t *testing.T) {
	t.Parallel()

	_, err := ConvertValue("2026-09-17T00:00:00Z", "DateTime")
	assertErrorCode(t, err, ErrorCodeValueConversionFailed)
}

func assertErrorCode(t *testing.T, err error, want ErrorCode) {
	t.Helper()
	if err == nil {
		t.Fatalf("error = nil, want code %s", want)
	}
	got, ok := ErrorCodeOf(err)
	if !ok || got != want {
		t.Fatalf("error = %v, code = %q, want %q", err, got, want)
	}
}

type preparationContextKey struct{}

type fakePreparationTarget struct {
	pointID     collector.PointID
	dataType    string
	readErr     error
	readCount   int
	writeCount  int
	readContext context.Context
}

func (t *fakePreparationTarget) PointID() collector.PointID {
	return t.pointID
}

func (t *fakePreparationTarget) ReadDataType(ctx context.Context) (string, error) {
	t.readCount++
	t.readContext = ctx
	return t.dataType, t.readErr
}

func (t *fakePreparationTarget) Write(context.Context, any) error {
	t.writeCount++
	return fmt.Errorf("Write must not be called by Task 3")
}
