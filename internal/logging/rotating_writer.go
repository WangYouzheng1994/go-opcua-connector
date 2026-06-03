package logging

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// rotatingWriter 实现按大小轮转、按天数清理的日志写入器。
// 写操作是线程安全的。
type rotatingWriter struct {
	mu        sync.Mutex
	file      *os.File
	filePath  string
	dir       string
	baseName  string
	maxSize   int64 // 字节
	maxAgeDay int
	written   int64 // 当前文件已写字节数
}

// newRotatingWriter 创建日志轮转写入器。
// filePath 为主日志文件路径，maxSizeMB 为单个文件最大 MB，maxAgeDay 为保留天数。
func newRotatingWriter(filePath string, maxSizeMB, maxAgeDay int) (*rotatingWriter, error) {
	dir := filepath.Dir(filePath)
	baseName := filepath.Base(filePath)

	rw := &rotatingWriter{
		filePath:  filePath,
		dir:       dir,
		baseName:  baseName,
		maxSize:   int64(maxSizeMB) * 1024 * 1024,
		maxAgeDay: maxAgeDay,
	}

	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("logging: open log file %s: %w", filePath, err)
	}

	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("logging: stat log file %s: %w", filePath, err)
	}

	rw.file = file
	rw.written = info.Size()

	// 启动时清理过期文件
	go rw.cleanup()

	return rw, nil
}

// Write 实现 io.Writer 接口，线程安全地写入日志。
// 超过大小限制时自动轮转到带时间戳的备份文件。
func (rw *rotatingWriter) Write(p []byte) (int, error) {
	rw.mu.Lock()
	defer rw.mu.Unlock()

	if rw.written >= rw.maxSize {
		if err := rw.rotate(); err != nil {
			return 0, err
		}
	}

	n, err := rw.file.Write(p)
	if n > 0 {
		rw.written += int64(n)
	}
	return n, err
}

// Close 关闭日志文件。
func (rw *rotatingWriter) Close() error {
	rw.mu.Lock()
	defer rw.mu.Unlock()
	return rw.file.Close()
}

// rotate 轮转当前日志文件（调用者需持有锁）。
func (rw *rotatingWriter) rotate() error {
	if err := rw.file.Close(); err != nil {
		return err
	}

	ts := time.Now().Format("2006-01-02T15-04-05.000")
	backupName := strings.TrimSuffix(rw.baseName, ".log") + "-" + ts + ".log"
	backupPath := filepath.Join(rw.dir, backupName)

	if err := os.Rename(rw.filePath, backupPath); err != nil {
		return fmt.Errorf("logging: rotate %s -> %s: %w", rw.filePath, backupPath, err)
	}

	file, err := os.OpenFile(rw.filePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("logging: create new log file %s: %w", rw.filePath, err)
	}

	rw.file = file
	rw.written = 0
	return nil
}

// cleanup 清理超过保留天数的轮转日志。
func (rw *rotatingWriter) cleanup() {
	entries, err := os.ReadDir(rw.dir)
	if err != nil {
		return
	}

	prefix := strings.TrimSuffix(rw.baseName, ".log") + "-"
	cutoff := time.Now().AddDate(0, 0, -rw.maxAgeDay)

	var oldFiles []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, ".log") {
			continue
		}

		info, err := entry.Info()
		if err != nil {
			continue
		}
		if info.ModTime().Before(cutoff) {
			oldFiles = append(oldFiles, filepath.Join(rw.dir, name))
		}
	}

	sort.Strings(oldFiles)
	for _, p := range oldFiles {
		os.Remove(p)
	}
}
