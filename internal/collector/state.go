package collector

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"
)

// PointID 是数据源 Adapter 生成的协议无关点位标识。
type PointID string

// SampleValidity 表示 Adapter 标准化后的数据有效性。
type SampleValidity string

const (
	SampleValid   SampleValidity = "Valid"
	SampleInvalid SampleValidity = "Invalid"
)

// PointHealth 表示点位当前的结构化健康状态。
type PointHealth string

const (
	PointInitializing         PointHealth = "Initializing"
	PointHealthy              PointHealth = "Healthy"
	PointSampleInvalid        PointHealth = "SampleInvalid"
	PointSubscribeFailed      PointHealth = "SubscribeFailed"
	PointVerificationFailed   PointHealth = "VerificationFailed"
	PointSubscriptionMismatch PointHealth = "SubscriptionMismatch"
	PointSourceDisconnected   PointHealth = "SourceDisconnected"
	PointStale                PointHealth = "Stale"
)

// GenerationToken 同时标识连接生命周期和单点监控生命周期。
type GenerationToken struct {
	Connection uint64
	Monitor    uint64
}

// PointSample 是由订阅产生的标准化样本。
type PointSample struct {
	PointID       PointID
	Value         any
	HasValue      bool
	Validity      SampleValidity
	ObservedAt    time.Time
	ReceivedAt    time.Time
	FailureReason string
	Generation    GenerationToken
}

// Verification 是主动读取产生的标准化验证结果。
type Verification struct {
	PointID       PointID
	Value         any
	HasValue      bool
	Validity      SampleValidity
	ObservedAt    time.Time
	CheckedAt     time.Time
	FailureReason string
	Generation    GenerationToken
}

// PointSnapshot 是 StateStore 对外提供的不可变点位快照。
type PointSnapshot struct {
	PointID              PointID
	Value                any
	HasValue             bool
	ObservedAt           time.Time
	Validity             SampleValidity
	Health               PointHealth
	FailureReason        string
	LastSubscriptionAt   time.Time
	LastVerificationAt   time.Time
	LastConfirmedAt      time.Time
	InitializedAt        time.Time
	LastStateChangedAt   time.Time
	Version              uint64
	ConnectionGeneration uint64
	MonitorGeneration    uint64
}

// StateStore 保存采集点的最新可信状态。
type StateStore interface {
	Initialize(points []PointID, tokenByPoint map[PointID]GenerationToken) error
	SeedInitialValue(sample Verification, expectedVersion uint64) bool
	ApplySubscription(sample PointSample) bool
	ApplyVerification(result Verification, expectedVersion uint64) bool
	AdvanceMonitor(pointID PointID, expected GenerationToken, health PointHealth) (GenerationToken, bool)
	MarkPointFailed(pointID PointID, token GenerationToken, health PointHealth, reason string) bool
	MarkPointFailedIfVersion(pointID PointID, token GenerationToken, expectedVersion uint64, health PointHealth, reason string) bool
	MarkPointMonitoring(pointID PointID, token GenerationToken) bool
	AdvanceConnection(expectedConnection uint64, reason string) (uint64, bool)
	Remove(pointID PointID, expected GenerationToken) bool
	Snapshot() []PointSnapshot
	Get(pointID PointID) (PointSnapshot, bool)
	SubscribeChanges() <-chan struct{}
}

type pointState struct {
	PointSnapshot
	subscriptionOwnsValue bool
}

type memoryStateStore struct {
	mu                   sync.RWMutex
	states               map[PointID]*pointState
	connectionGeneration uint64
	changes              chan struct{}
	now                  func() time.Time
}

// NewStateStore 创建一个进程内最新状态池。
func NewStateStore() StateStore {
	return newStateStore(time.Now)
}

func newStateStore(now func() time.Time) *memoryStateStore {
	return &memoryStateStore{
		states:  make(map[PointID]*pointState),
		changes: make(chan struct{}, 1),
		now:     now,
	}
}

func (s *memoryStateStore) Initialize(points []PointID, tokenByPoint map[PointID]GenerationToken) error {
	connection, err := validateInitialization(points, tokenByPoint)
	if err != nil {
		return err
	}
	if len(points) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.connectionGeneration == 0 {
		s.connectionGeneration = connection
	}
	if connection != s.connectionGeneration {
		return fmt.Errorf("connection generation %d does not match current generation %d", connection, s.connectionGeneration)
	}

	now := s.now()
	for _, pointID := range points {
		token := tokenByPoint[pointID]
		state, exists := s.states[pointID]
		if !exists {
			s.states[pointID] = &pointState{PointSnapshot: PointSnapshot{
				PointID:              pointID,
				Validity:             SampleInvalid,
				Health:               PointInitializing,
				InitializedAt:        now,
				LastStateChangedAt:   now,
				Version:              1,
				ConnectionGeneration: token.Connection,
				MonitorGeneration:    token.Monitor,
			}}
			continue
		}

		state.ConnectionGeneration = token.Connection
		state.MonitorGeneration = token.Monitor
		state.Validity = SampleInvalid
		state.Health = PointInitializing
		state.FailureReason = ""
		state.LastStateChangedAt = now
		state.Version++
	}

	s.notifyLocked()
	return nil
}

func validateInitialization(points []PointID, tokenByPoint map[PointID]GenerationToken) (uint64, error) {
	if len(points) != len(tokenByPoint) {
		return 0, errors.New("points and generation tokens must have the same size")
	}

	seen := make(map[PointID]struct{}, len(points))
	var connection uint64
	for _, pointID := range points {
		if strings.TrimSpace(string(pointID)) == "" {
			return 0, errors.New("point ID cannot be empty")
		}
		if _, exists := seen[pointID]; exists {
			return 0, fmt.Errorf("duplicate point ID %q", pointID)
		}
		seen[pointID] = struct{}{}

		token, exists := tokenByPoint[pointID]
		if !exists {
			return 0, fmt.Errorf("missing generation token for point %q", pointID)
		}
		if token.Connection == 0 || token.Monitor == 0 {
			return 0, fmt.Errorf("invalid generation token for point %q", pointID)
		}
		if connection == 0 {
			connection = token.Connection
		} else if connection != token.Connection {
			return 0, errors.New("all initialized points must use the same connection generation")
		}
	}

	return connection, nil
}

func (s *memoryStateStore) SeedInitialValue(sample Verification, expectedVersion uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	state, ok := s.currentStateLocked(sample.PointID, sample.Generation)
	if !ok || state.Version != expectedVersion || state.HasValue || state.subscriptionOwnsValue {
		return false
	}
	if sample.Validity != SampleValid || !sample.HasValue {
		return false
	}

	checkedAt := fallbackTime(sample.CheckedAt, s.now)
	state.Value = cloneValue(sample.Value)
	state.HasValue = true
	state.ObservedAt = fallbackTime(sample.ObservedAt, func() time.Time { return checkedAt })
	state.Validity = SampleValid
	if verificationCanChangeHealth(state.Health) {
		state.Health = PointHealthy
		state.FailureReason = ""
	}
	state.LastVerificationAt = checkedAt
	state.LastConfirmedAt = checkedAt
	state.LastStateChangedAt = checkedAt
	state.Version++
	s.notifyLocked()
	return true
}

func (s *memoryStateStore) ApplySubscription(sample PointSample) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	state, ok := s.currentStateLocked(sample.PointID, sample.Generation)
	if !ok {
		return false
	}

	// 迟到通知不属于当前业务状态事件；接收和诊断统计应在 StateStore 外完成。
	if state.HasValue && !sample.ObservedAt.IsZero() && sample.ObservedAt.Before(state.ObservedAt) {
		return true
	}

	receivedAt := fallbackTime(sample.ReceivedAt, s.now)
	state.LastSubscriptionAt = receivedAt
	state.LastStateChangedAt = receivedAt
	state.Version++

	if sample.Validity != SampleValid || !sample.HasValue {
		state.Validity = SampleInvalid
		state.Health = PointSampleInvalid
		state.FailureReason = sample.FailureReason
		s.notifyLocked()
		return true
	}

	state.Value = cloneValue(sample.Value)
	state.HasValue = true
	state.ObservedAt = fallbackTime(sample.ObservedAt, func() time.Time { return receivedAt })
	state.Validity = SampleValid
	state.Health = PointHealthy
	state.FailureReason = ""
	state.LastConfirmedAt = receivedAt
	state.subscriptionOwnsValue = true
	s.notifyLocked()
	return true
}

func (s *memoryStateStore) ApplyVerification(result Verification, expectedVersion uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	state, ok := s.currentStateLocked(result.PointID, result.Generation)
	if !ok || state.Version != expectedVersion {
		return false
	}

	checkedAt := fallbackTime(result.CheckedAt, s.now)
	state.LastVerificationAt = checkedAt
	state.LastStateChangedAt = checkedAt
	state.Version++

	switch {
	case result.Validity != SampleValid || !result.HasValue:
		if verificationCanChangeHealth(state.Health) {
			state.Validity = SampleInvalid
			state.Health = PointVerificationFailed
			state.FailureReason = result.FailureReason
		}
	case !state.HasValue:
		if verificationCanChangeHealth(state.Health) {
			state.Validity = SampleInvalid
			state.Health = PointVerificationFailed
			state.FailureReason = result.FailureReason
		}
	case reflect.DeepEqual(state.Value, result.Value):
		state.LastConfirmedAt = checkedAt
		if verificationCanChangeHealth(state.Health) {
			state.Validity = SampleValid
			state.Health = PointHealthy
			state.FailureReason = ""
		}
	default:
		if verificationCanChangeHealth(state.Health) {
			state.Validity = SampleInvalid
			state.Health = PointSubscriptionMismatch
			state.FailureReason = result.FailureReason
		}
	}

	s.notifyLocked()
	return true
}

func (s *memoryStateStore) AdvanceMonitor(pointID PointID, expected GenerationToken, health PointHealth) (GenerationToken, bool) {
	if !knownPointHealth(health) || health == PointHealthy {
		return GenerationToken{}, false
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	state, ok := s.currentStateLocked(pointID, expected)
	if !ok || expected.Monitor == math.MaxUint64 {
		return GenerationToken{}, false
	}

	state.MonitorGeneration++
	state.Validity = SampleInvalid
	state.Health = health
	state.LastStateChangedAt = s.now()
	state.Version++
	result := GenerationToken{Connection: state.ConnectionGeneration, Monitor: state.MonitorGeneration}
	s.notifyLocked()
	return result, true
}

func (s *memoryStateStore) MarkPointFailed(pointID PointID, token GenerationToken, health PointHealth, reason string) bool {
	return s.markPointFailed(pointID, token, 0, false, health, reason)
}

func (s *memoryStateStore) MarkPointFailedIfVersion(pointID PointID, token GenerationToken, expectedVersion uint64, health PointHealth, reason string) bool {
	return s.markPointFailed(pointID, token, expectedVersion, true, health, reason)
}

func (s *memoryStateStore) markPointFailed(pointID PointID, token GenerationToken, expectedVersion uint64, checkVersion bool, health PointHealth, reason string) bool {
	if !knownPointHealth(health) || health == PointHealthy {
		return false
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	state, ok := s.currentStateLocked(pointID, token)
	if !ok || checkVersion && state.Version != expectedVersion {
		return false
	}

	state.Validity = SampleInvalid
	state.Health = health
	state.FailureReason = reason
	state.LastStateChangedAt = s.now()
	state.Version++
	s.notifyLocked()
	return true
}

func (s *memoryStateStore) MarkPointMonitoring(pointID PointID, token GenerationToken) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	state, ok := s.currentStateLocked(pointID, token)
	if !ok {
		return false
	}
	state.Validity = SampleInvalid
	state.Health = PointInitializing
	state.FailureReason = ""
	state.LastStateChangedAt = s.now()
	state.Version++
	s.notifyLocked()
	return true
}

func (s *memoryStateStore) AdvanceConnection(expectedConnection uint64, reason string) (uint64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if expectedConnection == 0 || s.connectionGeneration != expectedConnection || expectedConnection == math.MaxUint64 {
		return 0, false
	}

	next := expectedConnection + 1
	s.connectionGeneration = next
	now := s.now()
	for _, state := range s.states {
		state.ConnectionGeneration = next
		state.MonitorGeneration = 0
		state.Validity = SampleInvalid
		state.Health = PointSourceDisconnected
		state.FailureReason = reason
		state.LastStateChangedAt = now
		state.Version++
	}
	s.notifyLocked()
	return next, true
}

func (s *memoryStateStore) Remove(pointID PointID, expected GenerationToken) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.currentStateLocked(pointID, expected); !ok {
		return false
	}
	delete(s.states, pointID)
	s.notifyLocked()
	return true
}

func (s *memoryStateStore) Snapshot() []PointSnapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]PointSnapshot, 0, len(s.states))
	for _, state := range s.states {
		result = append(result, cloneSnapshot(state.PointSnapshot))
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].PointID < result[j].PointID
	})
	return result
}

func (s *memoryStateStore) Get(pointID PointID) (PointSnapshot, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	state, ok := s.states[pointID]
	if !ok {
		return PointSnapshot{}, false
	}
	return cloneSnapshot(state.PointSnapshot), true
}

func (s *memoryStateStore) SubscribeChanges() <-chan struct{} {
	return s.changes
}

func (s *memoryStateStore) currentStateLocked(pointID PointID, token GenerationToken) (*pointState, bool) {
	state, ok := s.states[pointID]
	if !ok {
		return nil, false
	}
	if state.ConnectionGeneration != token.Connection || state.MonitorGeneration != token.Monitor {
		return nil, false
	}
	return state, true
}

func (s *memoryStateStore) notifyLocked() {
	select {
	case s.changes <- struct{}{}:
	default:
	}
}

func fallbackTime(value time.Time, fallback func() time.Time) time.Time {
	if !value.IsZero() {
		return value
	}
	return fallback()
}

func verificationCanChangeHealth(health PointHealth) bool {
	switch health {
	case PointInitializing, PointHealthy, PointVerificationFailed, PointSubscriptionMismatch, PointStale:
		return true
	default:
		return false
	}
}

func knownPointHealth(health PointHealth) bool {
	switch health {
	case PointInitializing,
		PointHealthy,
		PointSampleInvalid,
		PointSubscribeFailed,
		PointVerificationFailed,
		PointSubscriptionMismatch,
		PointSourceDisconnected,
		PointStale:
		return true
	default:
		return false
	}
}

func cloneSnapshot(snapshot PointSnapshot) PointSnapshot {
	snapshot.Value = cloneValue(snapshot.Value)
	return snapshot
}

func cloneValue(value any) any {
	if value == nil {
		return nil
	}
	return cloneReflectValue(reflect.ValueOf(value)).Interface()
}

func cloneReflectValue(value reflect.Value) reflect.Value {
	if !value.IsValid() {
		return value
	}

	switch value.Kind() {
	case reflect.Interface:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		cloned := cloneReflectValue(value.Elem())
		result := reflect.New(value.Type()).Elem()
		result.Set(cloned)
		return result
	case reflect.Pointer:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		result := reflect.New(value.Type().Elem())
		result.Elem().Set(cloneReflectValue(value.Elem()))
		return result
	case reflect.Slice:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		result := reflect.MakeSlice(value.Type(), value.Len(), value.Len())
		for i := 0; i < value.Len(); i++ {
			result.Index(i).Set(cloneReflectValue(value.Index(i)))
		}
		return result
	case reflect.Map:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		result := reflect.MakeMapWithSize(value.Type(), value.Len())
		iterator := value.MapRange()
		for iterator.Next() {
			result.SetMapIndex(iterator.Key(), cloneReflectValue(iterator.Value()))
		}
		return result
	case reflect.Array:
		result := reflect.New(value.Type()).Elem()
		for i := 0; i < value.Len(); i++ {
			result.Index(i).Set(cloneReflectValue(value.Index(i)))
		}
		return result
	case reflect.Struct:
		result := reflect.New(value.Type()).Elem()
		result.Set(value)
		for i := 0; i < value.NumField(); i++ {
			if result.Field(i).CanSet() && value.Field(i).CanInterface() {
				result.Field(i).Set(cloneReflectValue(value.Field(i)))
			}
		}
		return result
	default:
		return value
	}
}
