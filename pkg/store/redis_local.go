//go:build !production

package store

import (
	"context"
	"time"
)

// RedisStore is a dependency-free in-memory fallback for tests and local builds
// without Redis libraries. Production Docker builds use redis.go.
type RedisStore struct {
	*MemoryStore
}

func NewRedisStore(_ string, _ string, _ int, _ time.Duration) *RedisStore {
	return &RedisStore{MemoryStore: NewMemoryStore()}
}

func (s *RedisStore) Close() error                 { return nil }
func (s *RedisStore) Ping(_ context.Context) error { return nil }
