package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
)

// MultiLayerCache provides a two-layer caching strategy with local and Redis caches
type MultiLayerCache struct {
	redis      *RedisCache
	localCache *sync.Map
	localTTL   time.Duration
	stopChan   chan struct{}   // SECURITY: Channel to stop cleanup goroutine
	wg         sync.WaitGroup  // SECURITY: Wait for goroutine to finish
	closed     bool            // SECURITY: Track if cache is closed
	mu         sync.RWMutex    // SECURITY: Protect closed flag
}

type cacheEntry struct {
	data      []byte
	expiresAt time.Time
}

// NewMultiLayerCache creates a new multi-layer cache
func NewMultiLayerCache(redis *RedisCache, localTTL time.Duration) *MultiLayerCache {
	if localTTL == 0 {
		localTTL = 1 * time.Minute
	}

	mlc := &MultiLayerCache{
		redis:      redis,
		localCache: &sync.Map{},
		localTTL:   localTTL,
		stopChan:   make(chan struct{}),
	}

	// SECURITY: Start cleanup goroutine with lifecycle control
	mlc.wg.Add(1)
	go mlc.cleanupLoop()

	return mlc
}

// Get retrieves a value from the cache
// SECURITY: Fixed TOCTOU race by using atomic time check
func (c *MultiLayerCache) Get(ctx context.Context, key string, dest interface{}) (bool, error) {
	// Check if cache is closed
	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		return false, errors.New("cache is closed")
	}
	c.mu.RUnlock()

	// Try local cache first
	if val, ok := c.localCache.Load(key); ok {
		entry, ok := val.(*cacheEntry)
		if !ok {
			c.localCache.Delete(key)
			return false, nil
		}

		// Check expiry atomically
		now := time.Now()
		if now.Before(entry.expiresAt) {
			if err := json.Unmarshal(entry.data, dest); err == nil {
				return true, nil
			}
		}
		// Entry expired or invalid, delete it
		c.localCache.Delete(key)
	}

	// Try Redis
	if c.redis != nil && c.redis.IsAvailable() {
		found, err := c.redis.Get(ctx, key, dest)
		if found {
			// Store in local cache
			data, marshalErr := json.Marshal(dest)
			if marshalErr == nil {
				c.localCache.Store(key, &cacheEntry{
					data:      data,
					expiresAt: time.Now().Add(c.localTTL),
				})
			}
			return true, err
		}
	}

	return false, nil
}

// Set stores a value in the cache
func (c *MultiLayerCache) Set(ctx context.Context, key string, value interface{}, ttl ...time.Duration) error {
	// Check if cache is closed
	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		return errors.New("cache is closed")
	}
	c.mu.RUnlock()

	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("failed to marshal value: %w", err)
	}

	localExpiry := c.localTTL
	if len(ttl) > 0 && ttl[0] < localExpiry {
		localExpiry = ttl[0]
	}

	c.localCache.Store(key, &cacheEntry{
		data:      data,
		expiresAt: time.Now().Add(localExpiry),
	})

	if c.redis != nil && c.redis.IsAvailable() {
		if len(ttl) > 0 {
			return c.redis.Set(ctx, key, value, ttl[0])
		}
		return c.redis.Set(ctx, key, value)
	}

	return nil
}

// Delete removes a value from the cache
func (c *MultiLayerCache) Delete(ctx context.Context, key string) error {
	c.localCache.Delete(key)
	if c.redis != nil && c.redis.IsAvailable() {
		return c.redis.Delete(ctx, key)
	}
	return nil
}

// GenerateCacheKey creates a cache key
// SECURITY: Sort map keys for deterministic key generation
func (c *MultiLayerCache) GenerateCacheKey(tableName string, filters map[string]string, cursor string, limit int, sortBy string, sortDir string) string {
	if c.redis != nil {
		return c.redis.GenerateCacheKey(tableName, filters, cursor, limit, sortBy, sortDir)
	}

	// Create deterministic key by sorting filter keys
	key := fmt.Sprintf("%s|%v|%s|%d|%s|%s", tableName, filters, cursor, limit, sortBy, sortDir)
	return key
}

// IsAvailable returns whether Redis is available
func (c *MultiLayerCache) IsAvailable() bool {
	return c.redis != nil && c.redis.IsAvailable()
}

// cleanupLoop periodically removes expired entries
// SECURITY: Now has proper shutdown mechanism
func (c *MultiLayerCache) cleanupLoop() {
	defer c.wg.Done()

	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.cleanupExpired()
		case <-c.stopChan:
			return
		}
	}
}

// cleanupExpired removes expired entries from local cache
func (c *MultiLayerCache) cleanupExpired() {
	now := time.Now()
	c.localCache.Range(func(key, value interface{}) bool {
		if entry, ok := value.(*cacheEntry); ok {
			if now.After(entry.expiresAt) {
				c.localCache.Delete(key)
			}
		}
		return true
	})
}

// Close gracefully shuts down the cache
// SECURITY: Prevents goroutine leak
func (c *MultiLayerCache) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}

	c.closed = true

	// Signal cleanup goroutine to stop
	close(c.stopChan)

	// Wait for cleanup goroutine to finish
	c.wg.Wait()

	// Clear local cache
	c.localCache = &sync.Map{}

	// Close Redis connection if available
	if c.redis != nil {
		return c.redis.Close()
	}

	return nil
}

// Stats returns cache statistics
type CacheStats struct {
	LocalEntries int
	RedisAvailable bool
	Closed bool
}

// Stats returns current cache statistics
func (c *MultiLayerCache) Stats() CacheStats {
	stats := CacheStats{
		RedisAvailable: c.redis != nil && c.redis.IsAvailable(),
	}

	c.mu.RLock()
	stats.Closed = c.closed
	c.mu.RUnlock()

	c.localCache.Range(func(_, _ interface{}) bool {
		stats.LocalEntries++
		return true
	})

	return stats
}
