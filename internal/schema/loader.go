package schema

import (
        "context"
        "fmt"
        "strings"
        "sync"

        "github.com/jackc/pgx/v5/pgxpool"
)

type ColumnInfo struct {
        Name       string `json:"name"`
        DataType   string `json:"data_type"`
        IsNullable bool   `json:"is_nullable"`
        IsPrimary  bool   `json:"is_primary"`
        IsIndexed  bool   `json:"is_indexed"`
        OrdinalPos int    `json:"ordinal_position"`
}

type TableInfo struct {
        Name           string       `json:"name"`
        Schema         string       `json:"schema"`
        Columns        []ColumnInfo `json:"columns"`
        PrimaryKey     []string     `json:"primary_key"`
        Indexes        []string     `json:"indexed_columns"`
        LeadingIndexes []string     `json:"leading_indexed_columns"`
}

type SchemaRegistry struct {
        tables map[string]*TableInfo
        mu     sync.RWMutex
        pool   *pgxpool.Pool
}

func NewSchemaRegistry(pool *pgxpool.Pool) *SchemaRegistry {
        return &SchemaRegistry{
                tables: make(map[string]*TableInfo),
                pool:   pool,
        }
}

func (r *SchemaRegistry) LoadSchema(ctx context.Context) error {
        r.mu.Lock()
        defer r.mu.Unlock()

        tables, err := r.discoverTables(ctx)
        if err != nil {
                return fmt.Errorf("failed to discover tables: %w", err)
        }

        for _, table := range tables {
                columns, err := r.discoverColumns(ctx, table.Schema, table.Name)
                if err != nil {
                        return fmt.Errorf("failed to discover columns for %s: %w", table.Name, err)
                }
                table.Columns = columns

                primaryKey, err := r.discoverPrimaryKey(ctx, table.Schema, table.Name)
                if err != nil {
                        return fmt.Errorf("failed to discover primary key for %s: %w", table.Name, err)
                }
                table.PrimaryKey = primaryKey

                indexes, err := r.discoverIndexedColumns(ctx, table.Schema, table.Name)
                if err != nil {
                        return fmt.Errorf("failed to discover indexes for %s: %w", table.Name, err)
                }
                table.Indexes = indexes

                leadingIndexes, err := r.discoverLeadingIndexedColumns(ctx, table.Schema, table.Name)
                if err != nil {
                        return fmt.Errorf("failed to discover leading indexes for %s: %w", table.Name, err)
                }
                table.LeadingIndexes = leadingIndexes

                for i := range table.Columns {
                        for _, pk := range table.PrimaryKey {
                                if table.Columns[i].Name == pk {
                                        table.Columns[i].IsPrimary = true
                                }
                        }
                        for _, idx := range table.Indexes {
                                if table.Columns[i].Name == idx {
                                        table.Columns[i].IsIndexed = true
                                }
                        }
                }

                r.tables[table.Name] = table
        }

        return nil
}

func (r *SchemaRegistry) discoverTables(ctx context.Context) ([]*TableInfo, error) {
        query := `
                SELECT table_name, table_schema
                FROM information_schema.tables
                WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
                AND table_type = 'BASE TABLE'
                ORDER BY table_name
        `

        rows, err := r.pool.Query(ctx, query)
        if err != nil {
                return nil, err
        }
        defer rows.Close()

        var tables []*TableInfo
        for rows.Next() {
                var table TableInfo
                if err := rows.Scan(&table.Name, &table.Schema); err != nil {
                        return nil, err
                }
                tables = append(tables, &table)
        }

        return tables, rows.Err()
}

func (r *SchemaRegistry) discoverColumns(ctx context.Context, schema, tableName string) ([]ColumnInfo, error) {
        query := `
                SELECT 
                        column_name,
                        data_type,
                        is_nullable = 'YES' as is_nullable,
                        ordinal_position
                FROM information_schema.columns
                WHERE table_schema = $1 AND table_name = $2
                ORDER BY ordinal_position
        `

        rows, err := r.pool.Query(ctx, query, schema, tableName)
        if err != nil {
                return nil, err
        }
        defer rows.Close()

        var columns []ColumnInfo
        for rows.Next() {
                var col ColumnInfo
                if err := rows.Scan(&col.Name, &col.DataType, &col.IsNullable, &col.OrdinalPos); err != nil {
                        return nil, err
                }
                columns = append(columns, col)
        }

        return columns, rows.Err()
}

func (r *SchemaRegistry) discoverPrimaryKey(ctx context.Context, schema, tableName string) ([]string, error) {
        query := `
                SELECT a.attname
                FROM pg_index i
                JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                JOIN pg_class c ON c.oid = i.indrelid
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE i.indisprimary
                AND n.nspname = $1
                AND c.relname = $2
                ORDER BY array_position(i.indkey, a.attnum)
        `

        rows, err := r.pool.Query(ctx, query, schema, tableName)
        if err != nil {
                return nil, err
        }
        defer rows.Close()

        var pkColumns []string
        for rows.Next() {
                var colName string
                if err := rows.Scan(&colName); err != nil {
                        return nil, err
                }
                pkColumns = append(pkColumns, colName)
        }

        return pkColumns, rows.Err()
}

func (r *SchemaRegistry) discoverIndexedColumns(ctx context.Context, schema, tableName string) ([]string, error) {
        query := `
                SELECT DISTINCT a.attname
                FROM pg_index i
                JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                JOIN pg_class c ON c.oid = i.indrelid
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = $1
                AND c.relname = $2
                ORDER BY a.attname
        `

        rows, err := r.pool.Query(ctx, query, schema, tableName)
        if err != nil {
                return nil, err
        }
        defer rows.Close()

        var indexedCols []string
        for rows.Next() {
                var colName string
                if err := rows.Scan(&colName); err != nil {
                        return nil, err
                }
                indexedCols = append(indexedCols, colName)
        }

        return indexedCols, rows.Err()
}

func (r *SchemaRegistry) discoverLeadingIndexedColumns(ctx context.Context, schema, tableName string) ([]string, error) {
        query := `
                SELECT DISTINCT a.attname
                FROM pg_index i
                JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = i.indkey[0]
                JOIN pg_class c ON c.oid = i.indrelid
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = $1
                AND c.relname = $2
                ORDER BY a.attname
        `

        rows, err := r.pool.Query(ctx, query, schema, tableName)
        if err != nil {
                return nil, err
        }
        defer rows.Close()

        var leadingCols []string
        for rows.Next() {
                var colName string
                if err := rows.Scan(&colName); err != nil {
                        return nil, err
                }
                leadingCols = append(leadingCols, colName)
        }

        return leadingCols, rows.Err()
}

func (r *SchemaRegistry) GetTable(name string) *TableInfo {
        r.mu.RLock()
        defer r.mu.RUnlock()
        return r.tables[name]
}

func (r *SchemaRegistry) GetAllTables() []*TableInfo {
        r.mu.RLock()
        defer r.mu.RUnlock()

        tables := make([]*TableInfo, 0, len(r.tables))
        for _, t := range r.tables {
                tables = append(tables, t)
        }
        return tables
}

func (r *SchemaRegistry) TableExists(name string) bool {
        r.mu.RLock()
        defer r.mu.RUnlock()
        _, exists := r.tables[name]
        return exists
}

func (r *SchemaRegistry) GetSortableColumns(tableName string) []string {
        table := r.GetTable(tableName)
        if table == nil {
                return nil
        }

        seen := make(map[string]bool)
        var sortable []string
        for _, col := range table.LeadingIndexes {
                if !seen[col] {
                        sortable = append(sortable, col)
                        seen[col] = true
                }
        }
        for _, col := range table.PrimaryKey {
                if !seen[col] {
                        sortable = append(sortable, col)
                        seen[col] = true
                }
        }
        return sortable
}

func (r *SchemaRegistry) GetFilterableColumns(tableName string) []string {
        return r.GetSortableColumns(tableName)
}

func (r *SchemaRegistry) IsColumnSortable(tableName, columnName string) bool {
        for _, col := range r.GetSortableColumns(tableName) {
                if col == columnName {
                        return true
                }
        }
        return false
}

// ValidateTableName checks if a table name is safe and exists in the registry
// This prevents SQL injection through table name parameters
func (r *SchemaRegistry) ValidateTableName(name string) error {
        if name == "" {
                return fmt.Errorf("table name cannot be empty")
        }
        // Check for safe identifier pattern (alphanumeric and underscore only)
        for i, ch := range name {
                if i == 0 {
                        // First character must be letter or underscore
                        if !((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_') {
                                return fmt.Errorf("invalid table name: must start with letter or underscore")
                        }
                } else {
                        // Subsequent characters can be alphanumeric or underscore
                        if !((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '_') {
                                return fmt.Errorf("invalid table name: contains invalid character '%c'", ch)
                        }
                }
        }
        // Check max length (PostgreSQL limit is 63)
        if len(name) > 63 {
                return fmt.Errorf("table name too long: maximum 63 characters")
        }
        // Verify table exists in registry
        if !r.TableExists(name) {
                return fmt.Errorf("table not found: %s", name)
        }
        return nil
}

// ValidateColumn checks if a column exists in a table
// This prevents SQL injection through column name parameters
func (r *SchemaRegistry) ValidateColumn(tableName, columnName string) error {
        if columnName == "" {
                return fmt.Errorf("column name cannot be empty")
        }
        // Check for safe identifier pattern
        for i, ch := range columnName {
                if i == 0 {
                        if !((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_') {
                                return fmt.Errorf("invalid column name: must start with letter or underscore")
                        }
                } else {
                        if !((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '_') {
                                return fmt.Errorf("invalid column name: contains invalid character '%c'", ch)
                        }
                }
        }
        if len(columnName) > 63 {
                return fmt.Errorf("column name too long: maximum 63 characters")
        }
        // Verify column exists in table
        table := r.GetTable(tableName)
        if table == nil {
                return fmt.Errorf("table not found: %s", tableName)
        }
        for _, col := range table.Columns {
                if col.Name == columnName {
                        return nil
                }
        }
        return fmt.Errorf("column '%s' not found in table '%s'", columnName, tableName)
}

// ValidateColumns validates multiple column names against a table
func (r *SchemaRegistry) ValidateColumns(tableName string, columnNames []string) error {
        for _, col := range columnNames {
                if err := r.ValidateColumn(tableName, col); err != nil {
                        return err
                }
        }
        return nil
}

// QuoteIdent safely quotes a SQL identifier to prevent injection
// Uses PostgreSQL-style double quoting
func QuoteIdent(name string) string {
        // Double any double quotes and wrap in double quotes
        return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// GetColumnNames returns all column names for a table
func (r *SchemaRegistry) GetColumnNames(tableName string) []string {
        table := r.GetTable(tableName)
        if table == nil {
                return nil
        }
        names := make([]string, len(table.Columns))
        for i, col := range table.Columns {
                names[i] = col.Name
        }
        return names
}

func (r *SchemaRegistry) GetColumnType(tableName, columnName string) string {
        table := r.GetTable(tableName)
        if table == nil {
                return ""
        }

        for _, col := range table.Columns {
                if col.Name == columnName {
                        return col.DataType
                }
        }
        return ""
}

// RefreshSchema reloads the schema (alias for LoadSchema for compatibility)
func (r *SchemaRegistry) RefreshSchema() error {
        return r.LoadSchema(context.Background())
}

// AddTable manually adds/refreshes a single table to the registry
func (r *SchemaRegistry) AddTable(tableName string) error {
        ctx := context.Background()

        r.mu.Lock()
        defer r.mu.Unlock()

        // Get table schema
        var schema string
        schemaQuery := `
                SELECT table_schema 
                FROM information_schema.tables 
                WHERE table_name = $1 
                AND table_schema NOT IN ('pg_catalog', 'information_schema')
                LIMIT 1
        `
        if err := r.pool.QueryRow(ctx, schemaQuery, tableName).Scan(&schema); err != nil {
                return fmt.Errorf("table not found: %w", err)
        }

        table := &TableInfo{
                Name:   tableName,
                Schema: schema,
        }

        // Load columns
        columns, err := r.discoverColumns(ctx, schema, tableName)
        if err != nil {
                return fmt.Errorf("failed to discover columns: %w", err)
        }
        table.Columns = columns

        // Load primary key
        primaryKey, err := r.discoverPrimaryKey(ctx, schema, tableName)
        if err != nil {
                return fmt.Errorf("failed to discover primary key: %w", err)
        }
        table.PrimaryKey = primaryKey

        // Load indexes
        indexes, err := r.discoverIndexedColumns(ctx, schema, tableName)
        if err != nil {
                return fmt.Errorf("failed to discover indexes: %w", err)
        }
        table.Indexes = indexes

        // Load leading indexes
        leadingIndexes, err := r.discoverLeadingIndexedColumns(ctx, schema, tableName)
        if err != nil {
                return fmt.Errorf("failed to discover leading indexes: %w", err)
        }
        table.LeadingIndexes = leadingIndexes

        // Mark primary and indexed columns
        for i := range table.Columns {
                for _, pk := range table.PrimaryKey {
                        if table.Columns[i].Name == pk {
                                table.Columns[i].IsPrimary = true
                        }
                }
                for _, idx := range table.Indexes {
                        if table.Columns[i].Name == idx {
                                table.Columns[i].IsIndexed = true
                        }
                }
        }

        r.tables[tableName] = table
        return nil
}
