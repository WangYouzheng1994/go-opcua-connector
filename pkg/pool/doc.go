/*
Package pool 提供通用对象池实现，在高吞吐场景下降低 GC 压力。

类型：
  Pool[T]:       通用对象池接口（Get/Put）
  genericPool:   基于 sync.Pool 的实现
  BytePool:      字节切片专用池
  SlicePool[T]:  切片通用池，带容量控制

使用示例：
  p := pool.New(func() MyType { return MyType{} })
  obj := p.Get()
  // 使用 obj
  p.Put(obj)

说明：此包当前未被主程序使用，作为未来优化预留的公共工具。
*/
package pool