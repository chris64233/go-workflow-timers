package workflowtimers

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// Store 是持久化后端。Service 在内存中持有状态并在加锁事务内
// 调用 Save 写穿透，因此 Save 的调用方已保证串行。
type Store interface {
	Load() (*snapshot, error)
	Save(s *snapshot) error
}

// MemoryStore 不持久化，仅用于测试或纯内存部署。
type MemoryStore struct {
	saved *snapshot
}

func (m *MemoryStore) Load() (*snapshot, error) {
	if m.saved == nil {
		return newSnapshot(), nil
	}
	return m.saved, nil
}

func (m *MemoryStore) Save(s *snapshot) error {
	m.saved = s
	return nil
}

// FileStore 以 JSON 快照写穿透持久化（临时文件 + rename 保证原子替换）。
type FileStore struct {
	path string
}

func NewFileStore(path string) *FileStore {
	return &FileStore{path: path}
}

func (f *FileStore) Load() (*snapshot, error) {
	data, err := os.ReadFile(f.path)
	if errors.Is(err, fs.ErrNotExist) {
		return newSnapshot(), nil
	}
	if err != nil {
		return nil, fmt.Errorf("load snapshot: %w", err)
	}
	s := newSnapshot()
	if err := json.Unmarshal(data, s); err != nil {
		return nil, fmt.Errorf("decode snapshot: %w", err)
	}
	return s, nil
}

func (f *FileStore) Save(s *snapshot) error {
	data, err := json.Marshal(s)
	if err != nil {
		return fmt.Errorf("encode snapshot: %w", err)
	}
	tmp := f.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("write snapshot: %w", err)
	}
	if err := os.Rename(tmp, f.path); err != nil {
		return fmt.Errorf("commit snapshot: %w", err)
	}
	// 尽量让目录项落盘，失败不影响本次提交。
	if dir, err := os.Open(filepath.Dir(f.path)); err == nil {
		_ = dir.Sync()
		_ = dir.Close()
	}
	return nil
}
