package clickhouse

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// ConnectionPool manages a pool of ClickHouse connections
type ConnectionPool struct {
	connections []*Connection
	counter     uint64
	available   atomic.Bool // SECURITY: Use atomic for thread-safe reads
	mu          sync.RWMutex
	closed      atomic.Bool
}

// NewConnectionPool creates a new connection pool
func NewConnectionPool(cfg Config, poolSize int) (*ConnectionPool, error) {
	if poolSize <= 0 {
		poolSize = 3
	}

	pool := &ConnectionPool{
		connections: make([]*Connection, 0, poolSize),
	}
	pool.available.Store(false)
	pool.closed.Store(false)

	var connectionErrors []error
	for i := 0; i < poolSize; i++ {
		conn, err := NewConnection(cfg)
		if err != nil {
			connectionErrors = append(connectionErrors, err)
			continue
		}
		if !conn.IsAvailable() {
			connectionErrors = append(connectionErrors, errors.New("connection not available"))
			continue
		}
		pool.connections = append(pool.connections, conn)
	}

	if len(pool.connections) > 0 {
		pool.available.Store(true)
	} else if len(connectionErrors) > 0 {
		// Return the first error if no connections were established
		return nil, connectionErrors[0]
	}

	return pool, nil
}

// GetConnection returns a connection from the pool using round-robin
// SECURITY: Now properly synchronized with Close()
func (p *ConnectionPool) GetConnection() *Connection {
	// Fast path: check availability atomically
	if !p.available.Load() {
		return &Connection{available: false}
	}

	// Acquire read lock to protect slice access
	p.mu.RLock()
	defer p.mu.RUnlock()

	// Double-check after acquiring lock
	if !p.available.Load() || len(p.connections) == 0 {
		return &Connection{available: false}
	}

	// Round-robin selection using atomic counter
	idx := atomic.AddUint64(&p.counter, 1) - 1 // Subtract 1 because AddUint64 returns old value + n
	idx = idx % uint64(len(p.connections))

	return p.connections[idx]
}

// IsAvailable returns whether the pool has available connections
func (p *ConnectionPool) IsAvailable() bool {
	return p.available.Load() && !p.closed.Load()
}

// Query executes a query using a connection from the pool
func (p *ConnectionPool) Query(ctx context.Context, query string, args ...interface{}) (driver.Rows, error) {
	conn := p.GetConnection()
	if !conn.IsAvailable() {
		return nil, errors.New("no available connections in pool")
	}
	return conn.Query(ctx, query, args...)
}

// QueryRow executes a query that returns a single row
func (p *ConnectionPool) QueryRow(ctx context.Context, query string, args ...interface{}) driver.Row {
	return p.GetConnection().QueryRow(ctx, query, args...)
}

// Exec executes a query without returning rows
func (p *ConnectionPool) Exec(ctx context.Context, query string, args ...interface{}) error {
	conn := p.GetConnection()
	if !conn.IsAvailable() {
		return errors.New("no available connections in pool")
	}
	return conn.Exec(ctx, query, args...)
}

// PrepareBatch prepares a batch insert statement
func (p *ConnectionPool) PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error) {
	conn := p.GetConnection()
	if !conn.IsAvailable() {
		return nil, errors.New("no available connections in pool")
	}
	return conn.PrepareBatch(ctx, query, opts...)
}

// Conn returns the underlying driver connection
func (p *ConnectionPool) Conn() driver.Conn {
	return p.GetConnection().Conn()
}

// Close closes all connections in the pool
// SECURITY: Now properly synchronized with GetConnection()
func (p *ConnectionPool) Close() error {
	// Mark as closed first to prevent new connections
	p.closed.Store(true)
	p.available.Store(false)

	// Acquire write lock to protect connections slice
	p.mu.Lock()
	defer p.mu.Unlock()

	var lastErr error
	for _, conn := range p.connections {
		if err := conn.Close(); err != nil {
			lastErr = err
		}
	}

	// Clear the slice to help GC
	p.connections = nil

	return lastErr
}

// PoolStats returns statistics about the connection pool
type PoolStats struct {
	TotalConnections int
	Available        bool
	Closed           bool
}

// Stats returns current pool statistics
func (p *ConnectionPool) Stats() PoolStats {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return PoolStats{
		TotalConnections: len(p.connections),
		Available:        p.available.Load(),
		Closed:           p.closed.Load(),
	}
}

// HealthCheck verifies all connections are healthy
func (p *ConnectionPool) HealthCheck(ctx context.Context) error {
	if p.closed.Load() {
		return errors.New("pool is closed")
	}

	p.mu.RLock()
	defer p.mu.RUnlock()

	for i, conn := range p.connections {
		if !conn.IsAvailable() {
			return errors.New("connection %d is not available")
		}
		// Try a simple ping
		if err := conn.Ping(ctx); err != nil {
			return errors.New("connection %d ping failed: %w")
		}
		_ = i // suppress unused variable warning
	}

	return nil
}
