package clickhouse

import (
        "context"
        "encoding/json"
        "fmt"
        "highperf-api/internal/schema"
        "log"
        "math"
        "math/rand"
        "regexp"
        "strings"
        "sync"
        "time"

        "github.com/jackc/pgx/v5"
        "github.com/jackc/pgx/v5/pgxpool"
)

type CDCConfig struct {
        BatchSize       int
        SyncInterval    time.Duration
        ParallelWorkers int
        ChunkSize       int64
        MaxRetries      int
}

type CDCManager struct {
        pgPool       *pgxpool.Pool
        chRepo       *SearchRepository
        registry     *schema.SchemaRegistry
        config       CDCConfig
        stopChan     chan struct{}
        wg           sync.WaitGroup
        mu           sync.RWMutex
        tableStatus  map[string]*TableSyncStatus
        lastSyncedID map[string]int64
        lastSyncTime map[string]time.Time
        globalStatus *CDCStatus
        tables       []string
}

type TableSyncStatus struct {
        TableName       string    `json:"table_name"`
        TotalRows       int64     `json:"total_rows"`
        RecordsIndexed  int64     `json:"records_indexed"`
        LastSyncAt      time.Time `json:"last_sync_at"`
        LastSyncRecords int       `json:"last_sync_records"`
        LastSyncedID    int64     `json:"last_synced_id"`
        IsRunning       bool      `json:"is_running"`
        LastError       string    `json:"last_error,omitempty"`
}

type CDCStatus struct {
        IsRunning     bool                        `json:"is_running"`
        StartedAt     time.Time                   `json:"started_at"`
        TotalTables   int                         `json:"total_tables"`
        SyncInterval  string                      `json:"sync_interval"`
        TableStatuses map[string]*TableSyncStatus `json:"table_statuses"`
}

type CDCEvent struct {
        Table     string                 `json:"table"`
        Operation string                 `json:"operation"`
        Data      map[string]interface{} `json:"data"`
        Timestamp time.Time              `json:"timestamp"`
}

var validCHIdent = regexp.MustCompile(`^[a-zA-Z0-9_]+$`)

// Tables to exclude from CDC sync (auth/system tables)
var cdcExcludedTables = map[string]bool{
        "users":               true,
        "sessions":            true,
        "api_keys":            true,
        "audit_logs":          true,
        "rate_limit_entries":  true,
        "pipeline_configs":    true,
}

func NewCDCManager(pgPool *pgxpool.Pool, chRepo *SearchRepository, registry *schema.SchemaRegistry, cfg CDCConfig) *CDCManager {
        if cfg.BatchSize <= 0 {
                cfg.BatchSize = 100000
        }
        if cfg.SyncInterval <= 0 {
                cfg.SyncInterval = 30 * time.Second
        }
        if cfg.ParallelWorkers == 0 {
                cfg.ParallelWorkers = 5
        }
        if cfg.ChunkSize == 0 {
                cfg.ChunkSize = 10000
        }
        if cfg.MaxRetries == 0 {
                cfg.MaxRetries = 3
        }

        allTables := registry.GetAllTables()
        var tableNames []string
        for _, table := range allTables {
                // Skip excluded tables
                if cdcExcludedTables[table.Name] {
                        log.Printf("[%s] ⏭️ Excluded from CDC sync (auth/system table)", table.Name)
                        continue
                }
                tableNames = append(tableNames, table.Name)
        }

        log.Printf("Auto-discovered %d tables for CDC sync: %v", len(tableNames), tableNames)

        tableStatus := make(map[string]*TableSyncStatus)
        for _, t := range tableNames {
                tableStatus[t] = &TableSyncStatus{TableName: t}
        }

        return &CDCManager{
                pgPool:       pgPool,
                chRepo:       chRepo,
                registry:     registry,
                config:       cfg,
                stopChan:     make(chan struct{}),
                tableStatus:  tableStatus,
                lastSyncedID: make(map[string]int64),
                lastSyncTime: make(map[string]time.Time),
                tables:       tableNames,
                globalStatus: &CDCStatus{
                        TotalTables:   len(tableNames),
                        SyncInterval:  cfg.SyncInterval.String(),
                        TableStatuses: tableStatus,
                },
        }
}

func (m *CDCManager) Start() {
        if !m.chRepo.IsAvailable() {
                log.Println("ClickHouse not available, CDC sync disabled")
                return
        }

        if len(m.tables) == 0 {
                log.Println("No tables found for CDC sync")
                return
        }

        ctx := context.Background()

        if err := m.ensureGlobalTables(ctx); err != nil {
                log.Printf("FATAL: Failed to create global tables: %v", err)
                return
        }

        m.mu.Lock()
        m.globalStatus.IsRunning = true
        m.globalStatus.StartedAt = time.Now()
        m.mu.Unlock()

        log.Printf("CDC sync started for %d tables with batch size %d and chunk size %d", len(m.tables), m.config.BatchSize, m.config.ChunkSize)
        m.loadCheckpoints()

        m.wg.Add(1)
        go m.syncLoop()
}

func (m *CDCManager) Stop() {
        log.Println("Stopping CDC sync...")
        m.mu.Lock()
        m.globalStatus.IsRunning = false
        m.mu.Unlock()
        close(m.stopChan)
        m.wg.Wait()
        log.Println("CDC sync stopped")
}

func (m *CDCManager) GetStatus() *CDCStatus {
        m.mu.RLock()
        defer m.mu.RUnlock()

        tableStatuses := make(map[string]*TableSyncStatus)
        for name, status := range m.tableStatus {
                copied := &TableSyncStatus{
                        TableName:       status.TableName,
                        TotalRows:       status.TotalRows,
                        RecordsIndexed:  status.RecordsIndexed,
                        LastSyncAt:      status.LastSyncAt,
                        LastSyncRecords: status.LastSyncRecords,
                        LastSyncedID:    status.LastSyncedID,
                        IsRunning:       status.IsRunning,
                        LastError:       status.LastError,
                }
                tableStatuses[name] = copied
        }

        return &CDCStatus{
                IsRunning:     m.globalStatus.IsRunning,
                StartedAt:     m.globalStatus.StartedAt,
                TotalTables:   m.globalStatus.TotalTables,
                SyncInterval:  m.globalStatus.SyncInterval,
                TableStatuses: tableStatuses,
        }
}

func (m *CDCManager) syncLoop() {
        defer m.wg.Done()

        log.Printf("Starting parallel initial sync with %d workers...", m.config.ParallelWorkers)
        m.parallelInitialSync()
        log.Println("All initial syncs completed. Switching to incremental mode.")

        ticker := time.NewTicker(m.config.SyncInterval)
        defer ticker.Stop()

        for {
                select {
                case <-m.stopChan:
                        return
                case <-ticker.C:
                        for _, tableName := range m.tables {
                                m.incrementalSync(tableName)
                        }
                }
        }
}

func (m *CDCManager) parallelInitialSync() {
        var wg sync.WaitGroup
        semaphore := make(chan struct{}, m.config.ParallelWorkers)

        for _, tableName := range m.tables {
                wg.Add(1)
                go func(tName string) {
                        defer wg.Done()
                        semaphore <- struct{}{}
                        defer func() { <-semaphore }()

                        if err := m.syncTableChunked(tName); err != nil {
                                log.Printf("[%s] Initial sync failed: %v", tName, err)
                        }
                }(tableName)
        }

        wg.Wait()
}

// ⭐ UPDATED SCHEMA MANAGER
func (m *CDCManager) ensureGlobalTables(ctx context.Context) error {
        log.Println("🛠️ Ensuring global tables (Smart Indexing Schema)...")

        // // 1. Drop existing tables to force schema rebuild (Removing s_indx)
        // dropStatements := []string{
        //      "DROP TABLE IF EXISTS mv_token_bitmap",
        //      "DROP TABLE IF EXISTS search_token_bitmap",
        //      "DROP TABLE IF EXISTS search_token_stats",
        //      "DROP TABLE IF EXISTS search_token_entity",
        // }

        // for _, stmt := range dropStatements {
        //      if err := m.chRepo.conn.Exec(ctx, stmt); err != nil {
        //              log.Printf("Warn: cleanup error: %v", err)
        //      }
        // }

        // 2. Create Global Bitmap Table
        // Removed 'token' string to save space. Only hash is needed.
        bitmapTableSQL := `
        CREATE TABLE IF NOT EXISTS search_token_bitmap (
            token_hash UInt64,
            ids_bitmap AggregateFunction(groupBitmap, UInt64),
            updated_at DateTime DEFAULT now()
        ) ENGINE = AggregatingMergeTree()
        ORDER BY (token_hash)
        SETTINGS index_granularity = 8192
    `
        if err := m.chRepo.conn.Exec(ctx, bitmapTableSQL); err != nil {
                return fmt.Errorf("failed to create search_token_bitmap: %w", err)
        }
        log.Println("✅ search_token_bitmap created")

        // 3. Create Stats Table
        // Keeps token string for UI/Debug
        statsTableSQL := `
        CREATE TABLE IF NOT EXISTS search_token_stats (
            token_hash UInt64,
            token LowCardinality(String),
            table_name LowCardinality(String),
            count UInt64,
            updated_at DateTime DEFAULT now()
        ) ENGINE = SummingMergeTree()
        ORDER BY (token_hash, table_name)
        SETTINGS index_granularity = 8192
    `
        if err := m.chRepo.conn.Exec(ctx, statsTableSQL); err != nil {
                return fmt.Errorf("failed to create search_token_stats: %w", err)
        }
        log.Println("✅ search_token_stats created")

        // 4. Create Token Stream Table
        streamTableSQL := `
        CREATE TABLE IF NOT EXISTS search_token_entity (
            token_hash UInt64,
            token LowCardinality(String),
            global_id UInt64,
            table_name LowCardinality(String),
            updated_at DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        ORDER BY (token_hash, global_id)
        TTL updated_at + INTERVAL 3 DAY DELETE
        SETTINGS index_granularity = 8192
    `
        if err := m.chRepo.conn.Exec(ctx, streamTableSQL); err != nil {
                return fmt.Errorf("failed to create search_token_entity: %w", err)
        }

        // 5. Create Materialized View
        mvSQL := `
        CREATE MATERIALIZED VIEW IF NOT EXISTS mv_token_bitmap
        TO search_token_bitmap
        AS
        SELECT
            token_hash,
            groupBitmapState(global_id) as ids_bitmap,
            max(updated_at) as updated_at
        FROM search_token_entity
        GROUP BY token_hash
    `
        if err := m.chRepo.conn.Exec(ctx, mvSQL); err != nil {
                return fmt.Errorf("failed to create mv_token_bitmap: %w", err)
        }

        // 6. Dead Letter Table
        deadLetterSQL := `
    CREATE TABLE IF NOT EXISTS search_index_errors (
        table_name String, start_id UInt64, end_id UInt64, attempts UInt8,
        last_error String, sample_data String, created_at DateTime DEFAULT now()
    ) ENGINE = MergeTree() ORDER BY created_at`
        if err := m.chRepo.conn.Exec(ctx, deadLetterSQL); err != nil {
                return fmt.Errorf("failed to create search_index_errors: %w", err)
        }

        log.Println("✅ Global tables initialized")
        return nil
}

func (m *CDCManager) syncTableChunked(tableName string) error {
        if !validCHIdent.MatchString(tableName) {
                err := fmt.Errorf("invalid table name: %s", tableName)
                m.updateTableStatus(tableName, func(s *TableSyncStatus) { s.LastError = err.Error() })
                return err
        }

        ctx, cancel := context.WithTimeout(context.Background(), 72*time.Hour)
        defer cancel()

        m.updateTableStatus(tableName, func(s *TableSyncStatus) {
                s.IsRunning = true
                s.LastError = ""
        })
        defer m.updateTableStatus(tableName, func(s *TableSyncStatus) {
                s.IsRunning = false
        })

        table := m.registry.GetTable(tableName)
        if table == nil {
                err := fmt.Errorf("table not found: %s", tableName)
                m.updateTableStatus(tableName, func(s *TableSyncStatus) { s.LastError = err.Error() })
                return err
        }

        pkCol, err := m.getPrimaryKeyColumn(tableName)
        if err != nil {
                log.Printf("[%s] %v - skipping sync", tableName, err)
                m.updateTableStatus(tableName, func(s *TableSyncStatus) { s.LastError = err.Error() })
                return err
        }

        if err := m.chRepo.EnsureSearchIndex(ctx, tableName); err != nil {
                log.Printf("[%s] Failed to create search index: %v", tableName, err)
                m.updateTableStatus(tableName, func(s *TableSyncStatus) { s.LastError = err.Error() })
                return err
        }

        var totalRows int64
        countQuery := fmt.Sprintf("SELECT COALESCE(MAX(%s), 0) FROM %s", pgx.Identifier{pkCol}.Sanitize(), pgx.Identifier{tableName}.Sanitize())
        if err := m.pgPool.QueryRow(ctx, countQuery).Scan(&totalRows); err != nil {
                log.Printf("[%s] Failed to get max ID: %v", tableName, err)
        }

        m.updateTableStatus(tableName, func(s *TableSyncStatus) { s.TotalRows = totalRows })

        m.mu.RLock()
        startID := m.lastSyncedID[tableName]
        m.mu.RUnlock()

        if startID >= totalRows {
                log.Printf("[%s] ✅ Already up to date (max %s: %d)", tableName, pkCol, totalRows)
                return nil
        }

        log.Printf("[%s] Syncing from %s %d to %d (total: %d records)",
                tableName, pkCol, startID, totalRows, totalRows-startID)

        var columns []string
        for _, col := range table.Columns {
                columns = append(columns, col.Name)
        }

        columnList := joinColumns(columns)
        totalSynced := int64(0)
        startTime := time.Now()
        lastLogTime := time.Now()

        for currentID := startID; currentID < totalRows; currentID += m.config.ChunkSize {

                select {
                case <-m.stopChan:
                        log.Println("🛑 Stop signal received! Aborting sync for table:", tableName)
                        return fmt.Errorf("sync stopped by user")
                default:
                }

                endID := currentID + m.config.ChunkSize
                if endID > totalRows {
                        endID = totalRows
                }

                query := fmt.Sprintf(`
            SELECT %s
            FROM %s
            WHERE %s > $1 AND %s <= $2
            ORDER BY %s
        `, columnList, pgx.Identifier{tableName}.Sanitize(),
                        pgx.Identifier{pkCol}.Sanitize(),
                        pgx.Identifier{pkCol}.Sanitize(),
                        pgx.Identifier{pkCol}.Sanitize())

                rows, err := m.pgPool.Query(ctx, query, currentID, endID)
                if err != nil {
                        log.Printf("[%s] Failed to query chunk %d-%d: %v", tableName, currentID, endID, err)
                        backoffWithJitter(0)
                        continue
                }

                var records []map[string]interface{}
                for rows.Next() {
                        values, err := rows.Values()
                        if err != nil {
                                continue
                        }

                        record := make(map[string]interface{})
                        for i, col := range table.Columns {
                                if i < len(values) {
                                        record[col.Name] = values[i]
                                }
                        }

                        records = append(records, record)
                }
                rows.Close()

                if len(records) > 0 {
                        var ingestErr error
                        for attempt := 0; attempt < m.config.MaxRetries; attempt++ {
                                ingestErr = m.chRepo.BulkIndex(ctx, tableName, records)
                                if ingestErr == nil {
                                        break
                                }

                                m.chRepo.IncRetryCount(1)
                                log.Printf("[%s] ⚠️ Chunk ingest failed (Attempt %d/%d): %v", tableName, attempt+1, m.config.MaxRetries, ingestErr)

                                if attempt < m.config.MaxRetries-1 {
                                        backoffWithJitter(attempt)
                                }
                        }

                        if ingestErr != nil {
                                log.Printf("[%s] 🚨 Failed to index chunk after %d retries. Inserting into dead-letter.", tableName, m.config.MaxRetries)
                                m.updateTableStatus(tableName, func(s *TableSyncStatus) { s.LastError = ingestErr.Error() })

                                sample := ""
                                if len(records) > 0 {
                                        if b, err := json.Marshal(records[0]); err == nil {
                                                sample = string(b)
                                        }
                                }

                                if err := m.chRepo.InsertDeadLetter(ctx, tableName, uint64(currentID), uint64(endID), uint8(m.config.MaxRetries), ingestErr.Error(), sample); err != nil {
                                        log.Printf("[%s] Failed to insert dead-letter: %v", tableName, err)
                                }

                                m.chRepo.IncDeadLetterCount(1)
                                return fmt.Errorf("chunk ingestion failed permanently: %w", ingestErr)
                        }

                        totalSynced += int64(len(records))

                        m.mu.Lock()
                        m.lastSyncedID[tableName] = endID
                        m.mu.Unlock()

                        m.updateTableStatus(tableName, func(s *TableSyncStatus) {
                                s.RecordsIndexed += int64(len(records))
                                s.LastSyncedID = endID
                        })
                }

                if time.Since(lastLogTime) > 10*time.Second {
                        elapsed := time.Since(startTime)
                        rate := float64(totalSynced) / elapsed.Seconds()
                        progress := float64(endID-startID) / float64(totalRows-startID) * 100
                        log.Printf("[%s] Progress: %.1f%% (%d/%d records, %.0f rec/sec)",
                                tableName, progress, totalSynced, totalRows-startID, rate)
                        lastLogTime = time.Now()
                }
        }

        elapsed := time.Since(startTime)
        if totalSynced > 0 {
                log.Printf("[%s] ✅ Initial sync completed: %d records in %v (%.0f rec/sec)",
                        tableName, totalSynced, elapsed, float64(totalSynced)/elapsed.Seconds())
        }

        m.updateTableStatus(tableName, func(s *TableSyncStatus) {
                s.LastSyncAt = time.Now()
                s.LastSyncRecords = int(totalSynced)
        })

        return nil
}

func (m *CDCManager) incrementalSync(tableName string) error {
        ctx := context.Background()

        table := m.registry.GetTable(tableName)
        if table == nil {
                return fmt.Errorf("table not found: %s", tableName)
        }

        pkCol, err := m.getPrimaryKeyColumn(tableName)
        if err != nil {
                return err
        }

        m.updateTableStatus(tableName, func(s *TableSyncStatus) { s.IsRunning = true })
        defer m.updateTableStatus(tableName, func(s *TableSyncStatus) { s.IsRunning = false })

        m.mu.RLock()
        lastID := m.lastSyncedID[tableName]
        m.mu.RUnlock()

        var columns []string
        for _, col := range table.Columns {
                columns = append(columns, col.Name)
        }
        columnList := joinColumns(columns)

        query := fmt.Sprintf(`
        SELECT %s
        FROM %s
        WHERE %s > $1
        ORDER BY %s
        LIMIT $2
    `, columnList, pgx.Identifier{tableName}.Sanitize(), pgx.Identifier{pkCol}.Sanitize(), pgx.Identifier{pkCol}.Sanitize())

        rows, err := m.pgPool.Query(ctx, query, lastID, m.config.BatchSize)
        if err != nil {
                m.updateTableStatus(tableName, func(s *TableSyncStatus) { s.LastError = err.Error() })
                return err
        }

        var records []map[string]interface{}
        var newMaxID int64

        for rows.Next() {
                values, err := rows.Values()
                if err != nil {
                        continue
                }
                record := make(map[string]interface{})
                for i, col := range table.Columns {
                        if i < len(values) {
                                record[col.Name] = values[i]
                        }
                }

                if idVal, ok := record[pkCol]; ok {
                        if id, ok := idVal.(int64); ok && id > newMaxID {
                                newMaxID = id
                        }
                }
                records = append(records, record)
        }
        rows.Close()

        if len(records) > 0 {
                var ingestErr error
                for attempt := 0; attempt < m.config.MaxRetries; attempt++ {
                        ingestErr = m.chRepo.BulkIndex(ctx, tableName, records)
                        if ingestErr == nil {
                                break
                        }
                        m.chRepo.IncRetryCount(1)
                        log.Printf("[%s] Incremental ingest failed (Attempt %d/%d): %v", tableName, attempt+1, m.config.MaxRetries, ingestErr)
                        if attempt < m.config.MaxRetries-1 {
                                backoffWithJitter(attempt)
                        }
                }
                if ingestErr != nil {
                        sample := ""
                        if len(records) > 0 {
                                if b, err := json.Marshal(records[0]); err == nil {
                                        sample = string(b)
                                }
                        }
                        if err := m.chRepo.InsertDeadLetter(ctx, tableName, uint64(lastID), uint64(newMaxID), uint8(m.config.MaxRetries), ingestErr.Error(), sample); err != nil {
                                log.Printf("[%s] Failed to insert dead-letter (incremental): %v", tableName, err)
                        }
                        m.chRepo.IncDeadLetterCount(1)
                        m.updateTableStatus(tableName, func(s *TableSyncStatus) { s.LastError = ingestErr.Error() })
                        return ingestErr
                }

                m.mu.Lock()
                m.lastSyncedID[tableName] = newMaxID
                m.mu.Unlock()

                m.updateTableStatus(tableName, func(s *TableSyncStatus) {
                        s.RecordsIndexed += int64(len(records))
                        s.LastSyncAt = time.Now()
                        s.LastSyncRecords = len(records)
                        s.LastSyncedID = newMaxID
                })
        }

        return nil
}

func (m *CDCManager) getPrimaryKeyColumn(tableName string) (string, error) {
        table := m.registry.GetTable(tableName)
        if table == nil {
                return "", fmt.Errorf("table not found")
        }

        if len(table.PrimaryKey) == 0 {
                return "", fmt.Errorf("no primary key defined")
        }

        return table.PrimaryKey[0], nil
}

func (m *CDCManager) loadCheckpoints() {
        ctx := context.Background()

        for _, tableName := range m.tables {

                if tableName == "" || !validCHIdent.MatchString(tableName) {
                        continue
                }

                searchTable := fmt.Sprintf("search_%s", tableName)

                checkQuery := `
            SELECT count()
            FROM system.tables
            WHERE database = currentDatabase()
            AND name = ?`
                var exists uint64
                row := m.chRepo.conn.QueryRow(ctx, checkQuery, searchTable)
                if err := row.Scan(&exists); err != nil || exists == 0 {
                        continue
                }

                // ⭐ UPDATED: Extract local ID from max global_id
                query := fmt.Sprintf(
                        "SELECT coalesce(max(global_id), 0) FROM %s WHERE is_deleted = 0",
                        searchTable,
                )

                var maxGlobalID uint64
                row = m.chRepo.conn.QueryRow(ctx, query)

                if err := row.Scan(&maxGlobalID); err == nil && maxGlobalID > 0 {
                        localID := extractLocalID(maxGlobalID)
                        m.mu.Lock()
                        m.lastSyncedID[tableName] = int64(localID)
                        m.mu.Unlock()
                        log.Printf("[%s] Resuming from ID: %d (extracted from global_id)", tableName, localID)
                }
        }
}

func extractLocalID(globalID uint64) uint64 {
        return globalID & 0xFFFFFFFFFFFF
}

func (m *CDCManager) updateTableStatus(tableName string, update func(*TableSyncStatus)) {
        m.mu.Lock()
        defer m.mu.Unlock()
        if status, ok := m.tableStatus[tableName]; ok {
                update(status)
        }
}

func (m *CDCManager) TriggerSync(tableName string) error {
        return m.syncTableChunked(tableName)
}

func (m *CDCManager) ProcessEvent(ctx context.Context, event CDCEvent) error {
        if !m.chRepo.IsAvailable() {
                return nil
        }

        table := m.registry.GetTable(event.Table)
        if table == nil {
                return fmt.Errorf("table not found: %s", event.Table)
        }

        pkColumn := "id"
        if len(table.PrimaryKey) > 0 {
                pkColumn = table.PrimaryKey[0]
        }

        id := fmt.Sprintf("%v", event.Data[pkColumn])

        switch event.Operation {
        case "INSERT", "UPDATE":
                return m.chRepo.IndexRecord(ctx, event.Table, id, event.Data)
        case "DELETE":
                return m.chRepo.DeleteRecord(ctx, event.Table, id)
        }

        return nil
}

func ParseCDCPayload(payload []byte) (*CDCEvent, error) {
        var event CDCEvent
        if err := json.Unmarshal(payload, &event); err != nil {
                return nil, err
        }
        event.Timestamp = time.Now()
        return &event, nil
}

func joinColumns(columns []string) string {
        var quoted []string
        for _, c := range columns {
                // Wrap every column name in double quotes to handle reserved keywords like "on", "group", "user"
                quoted = append(quoted, pgx.Identifier{c}.Sanitize())
        }
        return strings.Join(quoted, ", ")
}

func (m *CDCManager) Restart() error {
        log.Println("Restarting CDC sync...")

        if m.globalStatus.IsRunning {
                m.Stop()
                time.Sleep(1 * time.Second)
        }

        allTables := m.registry.GetAllTables()
        var tableNames []string
        for _, table := range allTables {
                tableNames = append(tableNames, table.Name)
        }

        m.mu.Lock()
        m.tables = tableNames
        for _, t := range tableNames {
                if _, exists := m.tableStatus[t]; !exists {
                        m.tableStatus[t] = &TableSyncStatus{TableName: t}
                        log.Printf("Added new table to CDC: %s", t)
                }
        }
        m.globalStatus.TotalTables = len(tableNames)
        m.globalStatus.TableStatuses = m.tableStatus
        m.mu.Unlock()

        log.Printf("Rediscovered %d tables for CDC sync: %v", len(tableNames), tableNames)
        m.Start()
        return nil
}

func (m *CDCManager) TriggerTableSync(tableName string) error {
        if !m.chRepo.IsAvailable() {
                return fmt.Errorf("ClickHouse not available")
        }

        table := m.registry.GetTable(tableName)
        if table == nil {
                log.Printf("[%s] Not in registry, adding table...", tableName)
                if err := m.registry.AddTable(tableName); err != nil {
                        return fmt.Errorf("failed to add table to registry: %w", err)
                }

                table = m.registry.GetTable(tableName)
                if table == nil {
                        return fmt.Errorf("table %s not found even after adding", tableName)
                }
        }

        m.mu.Lock()
        if _, exists := m.tableStatus[tableName]; !exists {
                m.tableStatus[tableName] = &TableSyncStatus{TableName: tableName}
                m.tables = append(m.tables, tableName)
                m.globalStatus.TotalTables = len(m.tables)
                log.Printf("Added %s to CDC tracking", tableName)
        }
        m.mu.Unlock()

        log.Printf("Starting immediate sync for table: %s", tableName)
        if err := m.syncTableChunked(tableName); err != nil {
                return fmt.Errorf("sync failed: %w", err)
        }

        log.Printf("✅ Successfully synced %s to ClickHouse", tableName)
        return nil
}

func (m *CDCManager) GetEntityRepository() interface{} {
        return nil
}

func backoffWithJitter(attempt int) {
        baseMs := 500.0
        maxMs := 30000.0
        exp := math.Min(maxMs, baseMs*math.Pow(2, float64(attempt)))
        jitter := rand.Float64() * exp * 0.2
        sleep := time.Duration(exp+jitter) * time.Millisecond
        time.Sleep(sleep)
}

// lol
