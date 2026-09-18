package writeback

import (
	"context"
	"errors"
	"sync"
	"time"

	"go-opcua-connector/internal/collector"
	"go-opcua-connector/internal/opcua"
)

const (
	requestQueueCapacity = 500
	itemExecutionTimeout = 5 * time.Second
)

var errExecutorStopped = errors.New("writeback executor is shutting down")

type executorLifecycle uint8

const (
	executorNotStarted executorLifecycle = iota
	executorRunning
	executorStopped
)

// TargetResolver 是回写执行器使用的 PointID 目标解析边界。
type TargetResolver interface {
	ResolveWriteTarget(pointID collector.PointID) (opcua.WriteTarget, error)
}

// CompletionHandler 接收一个 Request 对应的唯一聚合结果。
type CompletionHandler func(Result)

// QueueExecutor 使用固定有界队列和单 Worker 串行执行回写请求。
type QueueExecutor struct {
	resolver    TargetResolver
	preparer    *ValuePreparer
	onComplete  CompletionHandler
	queue       chan Request
	itemTimeout time.Duration
	done        chan struct{}
	stopping    chan struct{}
	stopOnce    sync.Once
	submitMu    sync.Mutex
	accepting   bool
	lifecycleMu sync.Mutex
	lifecycle   executorLifecycle
	cancelRun   context.CancelFunc
}

func NewQueueExecutor(resolver TargetResolver, onComplete CompletionHandler) *QueueExecutor {
	return newQueueExecutor(resolver, NewValuePreparer(), onComplete, itemExecutionTimeout)
}

func newQueueExecutor(
	resolver TargetResolver,
	preparer *ValuePreparer,
	onComplete CompletionHandler,
	itemTimeout time.Duration,
) *QueueExecutor {
	executor := &QueueExecutor{
		resolver:    resolver,
		preparer:    preparer,
		onComplete:  onComplete,
		queue:       make(chan Request, requestQueueCapacity),
		itemTimeout: itemTimeout,
		done:        make(chan struct{}),
		stopping:    make(chan struct{}),
		accepting:   true,
	}
	return executor
}

// TrySubmit 非阻塞提交完整 Request。队列满时立即返回 WRITE_QUEUE_FULL。
func (e *QueueExecutor) TrySubmit(request Request) error {
	e.submitMu.Lock()
	defer e.submitMu.Unlock()
	if !e.accepting {
		return errExecutorStopped
	}
	request.Items = append([]Item(nil), request.Items...)
	select {
	case e.queue <- request:
		return nil
	default:
		return newCodedError(ErrorCodeWriteQueueFull, "write request queue is full", nil)
	}
}

// Run 在当前 goroutine 中运行唯一 Worker，直到 ctx 取消。
// 调用方如需异步运行，只应启动这一个 Run goroutine。
func (e *QueueExecutor) Run(ctx context.Context) {
	e.lifecycleMu.Lock()
	if e.lifecycle != executorNotStarted {
		e.lifecycleMu.Unlock()
		return
	}
	select {
	case <-e.stopping:
		e.lifecycle = executorStopped
		close(e.done)
		e.lifecycleMu.Unlock()
		return
	default:
	}
	runCtx, cancel := context.WithCancel(ctx)
	e.lifecycle = executorRunning
	e.cancelRun = cancel
	e.lifecycleMu.Unlock()

	defer func() {
		cancel()
		e.lifecycleMu.Lock()
		e.cancelRun = nil
		e.lifecycle = executorStopped
		close(e.done)
		e.lifecycleMu.Unlock()
	}()
	e.run(runCtx)
}

func (e *QueueExecutor) run(ctx context.Context) {
	for {
		select {
		case <-e.stopping:
			return
		default:
		}
		select {
		case <-ctx.Done():
			return
		case <-e.stopping:
			return
		case request := <-e.queue:
			select {
			case <-e.stopping:
				return
			default:
			}
			result := e.executeRequest(ctx, request)
			if e.onComplete != nil {
				e.onComplete(result)
			}
		}
	}
}

// StopAccepting 阻止新请求进入队列。已入队但尚未执行的请求不会被继续消费。
func (e *QueueExecutor) StopAccepting() {
	e.submitMu.Lock()
	e.accepting = false
	e.stopOnce.Do(func() {
		close(e.stopping)
	})
	e.submitMu.Unlock()
}

// Shutdown 停止接收新请求，并等待当前正在执行的请求结束。
// ctx 到期后会取消当前执行；受底层 OPC UA I/O 限制，Worker 不保证立即退出。
func (e *QueueExecutor) Shutdown(ctx context.Context) error {
	e.StopAccepting()

	e.lifecycleMu.Lock()
	switch e.lifecycle {
	case executorNotStarted:
		e.lifecycle = executorStopped
		close(e.done)
		e.lifecycleMu.Unlock()
		return nil
	case executorStopped:
		e.lifecycleMu.Unlock()
		return nil
	case executorRunning:
	}
	e.lifecycleMu.Unlock()

	select {
	case <-e.done:
		return nil
	case <-ctx.Done():
		e.lifecycleMu.Lock()
		cancel := e.cancelRun
		e.lifecycleMu.Unlock()
		if cancel != nil {
			cancel()
		}
		return ctx.Err()
	}
}

func (e *QueueExecutor) Done() <-chan struct{} {
	return e.done
}

func (e *QueueExecutor) executeRequest(ctx context.Context, request Request) Result {
	result := Result{
		RequestID: request.RequestID,
		Success:   true,
		Results:   make([]ItemResult, 0, len(request.Items)),
	}
	for _, item := range request.Items {
		itemResult := e.executeItem(ctx, item)
		result.Results = append(result.Results, itemResult)
		if !itemResult.Success {
			result.Success = false
		}
	}
	return result
}

func (e *QueueExecutor) executeItem(parent context.Context, item Item) ItemResult {
	ctx, cancel := context.WithTimeout(parent, e.itemTimeout)
	defer cancel()

	target, err := e.resolver.ResolveWriteTarget(item.PointID)
	if timeoutErr := ctx.Err(); timeoutErr != nil {
		return failedItem(item.PointID, ErrorCodeWriteTimeout, "write item timed out")
	}
	if err != nil {
		if errors.Is(err, opcua.ErrPointNotFound) {
			return failedItem(item.PointID, ErrorCodePointNotFound, "point_id was not found")
		}
		return failedItem(item.PointID, ErrorCodeWriteFailed, "failed to resolve write target")
	}

	value, err := e.preparer.Prepare(ctx, target, item.Value)
	if timeoutErr := ctx.Err(); timeoutErr != nil {
		return failedItem(item.PointID, ErrorCodeWriteTimeout, "write item timed out")
	}
	if err != nil {
		if code, ok := ErrorCodeOf(err); ok {
			return failedItem(item.PointID, code, err.Error())
		}
		return failedItem(item.PointID, ErrorCodeValueConversionFailed, "failed to prepare write value")
	}

	err = target.Write(ctx, value)
	if timeoutErr := ctx.Err(); timeoutErr != nil {
		return failedItem(item.PointID, ErrorCodeWriteTimeout, "write item timed out")
	}
	if err != nil {
		if status, ok := opcua.WriteStatusOf(err); ok {
			result := failedItem(item.PointID, ErrorCodeWriteFailed, "OPC UA server rejected write")
			result.OPCUAStatus = status
			return result
		}
		return failedItem(item.PointID, ErrorCodeWriteFailed, "OPC UA write failed")
	}
	return ItemResult{PointID: item.PointID, Success: true}
}

func failedItem(pointID collector.PointID, code ErrorCode, message string) ItemResult {
	return ItemResult{
		PointID: pointID,
		Success: false,
		Code:    code,
		Message: message,
	}
}
