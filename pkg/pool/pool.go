// Package pool provides generic object pool implementations to reduce GC pressure.
package pool

import (
	"sync"
)

/**
 * 通用对象池接口
 *
 * @author 王有政
 */
type Pool[T any] interface {
	Get() T
	Put(T)
}

/**
 * 通用对象池实现
 *
 * @author 王有政
 */
type genericPool[T any] struct {
	// 底层sync.Pool
	pool sync.Pool
	// 创建新对象的工厂函数
	newFn func() T
}

/**
 * 创建一个新的对象池
 *
 */
func New[T any](newFn func() T) Pool[T] {
	return &genericPool[T]{
		pool: sync.Pool{
			New: func() interface{} {
				return newFn()
			},
		},
		newFn: newFn,
	}
}

/**
 * 获取一个对象
 *
 */
func (p *genericPool[T]) Get() T {
	return p.pool.Get().(T)
}

/**
 * 归还一个对象
 *
 */
func (p *genericPool[T]) Put(obj T) {
	p.pool.Put(obj)
}

/**
 * 字节缓冲区池，用于减少GC压力
 *
 * @author 王有政
 */
type BytePool struct {
	// 底层sync.Pool
	pool sync.Pool
	// 缓冲区大小
	size int
}

/**
 * 创建字节缓冲区池
 *
 */
func NewBytePool(size int) *BytePool {
	return &BytePool{
		size: size,
		pool: sync.Pool{
			New: func() interface{} {
				buf := make([]byte, size)
				return &buf
			},
		},
	}
}

/**
 * 获取缓冲区
 *
 */
func (p *BytePool) Get() *[]byte {
	buf := p.pool.Get().(*[]byte)
	return buf
}

/**
 * 归还缓冲区
 *
 */
func (p *BytePool) Put(buf *[]byte) {
	if cap(*buf) == p.size {
		p.pool.Put(buf)
	}
}

/**
 * 数据点切片池
 *
 * @author 王有政
 */
type SlicePool[T any] struct {
	// 底层sync.Pool
	pool sync.Pool
	// 切片初始容量
	size int
}

/**
 * 创建切片池
 *
 */
func NewSlicePool[T any](size int) *SlicePool[T] {
	return &SlicePool[T]{
		size: size,
		pool: sync.Pool{
			New: func() interface{} {
				s := make([]T, 0, size)
				return &s
			},
		},
	}
}

/**
 * 获取切片
 *
 */
func (p *SlicePool[T]) Get() *[]T {
	s := p.pool.Get().(*[]T)
	*s = (*s)[:0]
	return s
}

/**
 * 归还切片
 *
 */
func (p *SlicePool[T]) Put(s *[]T) {
	if cap(*s) == p.size {
		p.pool.Put(s)
	}
}
