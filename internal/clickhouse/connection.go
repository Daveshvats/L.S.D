package clickhouse

import (
        "context"
        "fmt"
        "time"

        "github.com/ClickHouse/clickhouse-go/v2"
        "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type Connection struct {
        conn      driver.Conn
        available bool
}

type Config struct {
        Addr     string
        Database string
        Username string
        Password string
}
type nullRow struct {
        err error
}

func (r *nullRow) Scan(dest ...interface{}) error {
        if r.err != nil {
                return r.err
        }
        return fmt.Errorf("no row available")
}

func (r *nullRow) ScanStruct(dest interface{}) error {
        if r.err != nil {
                return r.err
        }
        return fmt.Errorf("no row available")
}

func (r *nullRow) Err() error {
        return r.err
}

func NewConnection(cfg Config) (*Connection, error) {
        if cfg.Addr == "" {
                return &Connection{available: false}, nil
        }

        conn, err := clickhouse.Open(&clickhouse.Options{
                Addr: []string{cfg.Addr}, // Keeps your old way of handling address
                Auth: clickhouse.Auth{
                        Database: cfg.Database,
                        Username: cfg.Username,
                        Password: cfg.Password,
                },
                Settings: clickhouse.Settings{
                        "max_execution_time":     60,
                        "max_memory_usage":       34359738368, // Updated to 32GB for 2TB scale
                        "max_threads":            0,           // Auto detect (0)
                        "use_uncompressed_cache": 1,

                        // ⭐ NEW: Async Insert Performance Settings
                        "async_insert":                 1,       // Enable Async Inserts
                        "wait_for_async_insert":        0,       // Fire and Forget (0ms wait)
                        "async_insert_max_data_size":   1048576, // 1MB Buffer
                        "async_insert_busy_timeout_ms": 1000,    // Flush after 1s max

                        "allow_experimental_projection_optimization": 1, // Enable Projection Speedup
                },
                DialTimeout:     5 * time.Second,
                MaxOpenConns:    20, // Connection Pool
                MaxIdleConns:    10,
                ConnMaxLifetime: time.Hour,
                Compression: &clickhouse.Compression{
                        Method: clickhouse.CompressionLZ4,
                },
        })
        if err != nil {
                return &Connection{available: false}, nil
        }

        ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
        defer cancel()
        if err := conn.Ping(ctx); err != nil {
                return &Connection{available: false}, nil
        }

        return &Connection{conn: conn, available: true}, nil
}

func (c *Connection) IsAvailable() bool {
        return c.available
}

func (c *Connection) Query(ctx context.Context, query string, args ...interface{}) (driver.Rows, error) {
        if !c.available {
                return nil, fmt.Errorf("clickhouse not available")
        }
        return c.conn.Query(ctx, query, args...)
}

func (c *Connection) QueryRow(ctx context.Context, query string, args ...interface{}) driver.Row {
        if !c.available {
                return &nullRow{err: fmt.Errorf("clickhouse not available")}
        }
        return c.conn.QueryRow(ctx, query, args...)
}

func (c *Connection) PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error) {
        if !c.available {
                return nil, fmt.Errorf("clickhouse not available")
        }
        return c.conn.PrepareBatch(ctx, query, opts...)
}

func (c *Connection) Exec(ctx context.Context, query string, args ...interface{}) error {
        if !c.available {
                return fmt.Errorf("clickhouse not available")
        }
        return c.conn.Exec(ctx, query, args...)
}

func (c *Connection) Close() error {
        if c.conn != nil {
                return c.conn.Close()
        }
        return nil
}

// Ping checks if the connection is still alive
func (c *Connection) Ping(ctx context.Context) error {
        if !c.available || c.conn == nil {
                return fmt.Errorf("clickhouse not available")
        }
        return c.conn.Ping(ctx)
}

func (c *Connection) Conn() driver.Conn {
        return c.conn
}
