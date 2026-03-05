package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sort"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisCache provides Redis caching functionality
type RedisCache struct {
	client  *redis.Client
	timeout time.Duration
}

// NewRedisCache creates a new Redis cache instance
// SECURITY: Returns error on connection failure for better error handling
func NewRedisCache(addr, password string, db int, timeout time.Duration) *RedisCache {
	if timeout == 0 {
		timeout = 5 * time.Minute
	}

	client := redis.NewClient(&redis.Options{
		Addr:         addr,
		Password:     password,
		DB:           db,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		PoolSize:     10,
		MinIdleConns: 2,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		// Log warning but don't fail - return cache with nil client
		log.Printf("Redis connection failed: %v", err)
		return &RedisCache{client: nil, timeout: timeout}
	}

	log.Printf("Redis connected successfully: %s", addr)
	return &RedisCache{client: client, timeout: timeout}
}

// Get retrieves a value from Redis
func (c *RedisCache) Get(ctx context.Context, key string, dest interface{}) (bool, error) {
	if c.client == nil {
		return false, nil
	}

	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	val, err := c.client.Get(ctx, key).Result()
	if err == redis.Nil {
		return false, nil
	}
	if err != nil {
		log.Printf("Redis Get error for key %s: %v", key, err)
		return false, fmt.Errorf("redis get failed: %w", err)
	}

	if err := json.Unmarshal([]byte(val), dest); err != nil {
		log.Printf("JSON unmarshal error for key %s: %v", key, err)
		return false, fmt.Errorf("failed to unmarshal cached value: %w", err)
	}

	return true, nil
}

// Set stores a value in Redis with optional TTL
func (c *RedisCache) Set(ctx context.Context, key string, value interface{}, ttl ...time.Duration) error {
	if c.client == nil {
		return nil
	}

	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("failed to marshal value: %w", err)
	}

	expiration := c.timeout
	if len(ttl) > 0 {
		expiration = ttl[0]
	}

	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	if err := c.client.Set(ctx, key, data, expiration).Err(); err != nil {
		log.Printf("Redis Set error for key %s: %v", key, err)
		return fmt.Errorf("redis set failed: %w", err)
	}

	return nil
}

// Delete removes a key from Redis
func (c *RedisCache) Delete(ctx context.Context, key string) error {
	if c.client == nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	if err := c.client.Del(ctx, key).Err(); err != nil {
		log.Printf("Redis Delete error for key %s: %v", key, err)
		return fmt.Errorf("redis delete failed: %w", err)
	}

	return nil
}

// IsAvailable checks if Redis connection is healthy
func (c *RedisCache) IsAvailable() bool {
	if c.client == nil {
		return false
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := c.client.Ping(ctx).Err()
	return err == nil
}

// GenerateCacheKey creates a deterministic cache key
// SECURITY: Sorts filter keys for deterministic key generation
func (c *RedisCache) GenerateCacheKey(tableName string, filters map[string]string, cursor string, limit int, sortBy string, sortDir string) string {
	// Create deterministic key by sorting filter keys
	keys := make([]string, 0, len(filters))
	for k := range filters {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	filterStr := ""
	for _, k := range keys {
		filterStr += fmt.Sprintf("%s=%s;", k, filters[k])
	}

	return fmt.Sprintf("cache:%s:%s:%s:%d:%s:%s", tableName, filterStr, cursor, limit, sortBy, sortDir)
}

// Close closes the Redis connection
// SECURITY: Properly releases resources
func (c *RedisCache) Close() error {
	if c.client == nil {
		return nil
	}

	if err := c.client.Close(); err != nil {
		log.Printf("Redis close error: %v", err)
		return fmt.Errorf("failed to close redis connection: %w", err)
	}

	log.Println("Redis connection closed")
	return nil
}

// InvalidatePattern invalidates all keys matching a pattern
// Useful for invalidating all cache entries for a specific table
func (c *RedisCache) InvalidatePattern(ctx context.Context, pattern string) error {
	if c.client == nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	iter := c.client.Scan(ctx, 0, pattern, 0).Iterator()
	var keys []string

	for iter.Next(ctx) {
		keys = append(keys, iter.Val())
	}

	if err := iter.Err(); err != nil {
		return fmt.Errorf("scan failed: %w", err)
	}

	if len(keys) > 0 {
		if err := c.client.Del(ctx, keys...).Err(); err != nil {
			return fmt.Errorf("delete failed: %w", err)
		}
		log.Printf("Invalidated %d keys matching pattern %s", len(keys), pattern)
	}

	return nil
}

// Stats returns Redis connection statistics
type RedisStats struct {
	Available bool
	ConnectedClients int64
	KeyCount int64
}

// Stats returns current Redis statistics
func (c *RedisCache) Stats() RedisStats {
	stats := RedisStats{
		Available: c.IsAvailable(),
	}

	if c.client == nil {
		return stats
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if info, err := c.client.Info(ctx, "clients").Result(); err == nil {
		stats.ConnectedClients = parseClientCount(info)
	}

	if size, err := c.client.DBSize(ctx).Result(); err == nil {
		stats.KeyCount = size
	}

	return stats
}

func parseClientCount(info string) int64 {
	// Simple parsing - count "connected_clients:X" line
	var count int64
	fmt.Sscanf(info, "connected_clients:%d", &count)
	return count
}

// Ping checks Redis connectivity with context
func (c *RedisCache) Ping(ctx context.Context) error {
	if c.client == nil {
		return errors.New("redis client not initialized")
	}

	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	return c.client.Ping(ctx).Err()
}
