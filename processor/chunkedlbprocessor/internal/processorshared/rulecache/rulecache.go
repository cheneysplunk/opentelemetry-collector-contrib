// Copyright Splunk, Inc.
// SPDX-License-Identifier: Apache-2.0

// Package rulecache provides a thread-safe, capacity-bounded cache mapping
// sourcetype keys to compiled per-sourcetype rule entries.  Phase 2 ships a
// sync.Map-backed implementation; Phase 3 will add LRU eviction.
package rulecache

import (
	"errors"
	"sync"
	"sync/atomic"
)

// ErrCapacityExceeded is returned by Get when the cache is full and the
// requested key is not already present.
var ErrCapacityExceeded = errors.New("rulecache: capacity exceeded")

// ErrClosed is returned by Get after Close has been called.
var ErrClosed = errors.New("rulecache: closed")

// CompileContext carries per-compilation metadata passed to the compiler
// function.  Phase 2: empty struct; Phase 3 will carry PCRE2 arena handles.
type CompileContext struct{}

// Cache is a thread-safe map from K to V, bounded by maxSize entries.
// The zero value is not usable; create instances with New.
type Cache[K comparable, V any] struct {
	maxSize  int
	closeVal func(V)
	m        sync.Map
	size     atomic.Int64
	closed   atomic.Bool
}

// New constructs a Cache capped at maxSize entries.  closeVal, if non-nil, is
// called on every evicted or closed value to release native resources (e.g.,
// PCRE2 compiled patterns in Phase 3).  Pass nil when V has no native
// resources.
func New[K comparable, V any](maxSize int, closeVal func(V)) *Cache[K, V] {
	if maxSize <= 0 {
		maxSize = 10000
	}
	return &Cache[K, V]{maxSize: maxSize, closeVal: closeVal}
}

// Get looks up key and returns the cached value.  On a miss the supplied
// compiler is called to produce a new value which is stored and returned.
// Returns ErrCapacityExceeded if the cache is full and key is absent.
// Returns ErrClosed after Close has been called.
func (c *Cache[K, V]) Get(key K, compiler func(CompileContext, K) (V, error)) (V, error) {
	if c.closed.Load() {
		var zero V
		return zero, ErrClosed
	}
	if v, ok := c.m.Load(key); ok {
		return v.(V), nil
	}
	if int(c.size.Load()) >= c.maxSize {
		var zero V
		return zero, ErrCapacityExceeded
	}
	v, err := compiler(CompileContext{}, key)
	if err != nil {
		return v, err
	}
	if _, loaded := c.m.LoadOrStore(key, v); !loaded {
		c.size.Add(1)
	}
	return v, nil
}

// Close marks the cache as closed and, if closeVal was provided, calls it on
// every stored value.  Subsequent calls to Get return ErrClosed.
func (c *Cache[K, V]) Close() error {
	c.closed.Store(true)
	if c.closeVal != nil {
		c.m.Range(func(_, v any) bool {
			c.closeVal(v.(V))
			return true
		})
	}
	return nil
}
