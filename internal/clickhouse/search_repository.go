package clickhouse

import (
        "context"
        "encoding/base64"
        "encoding/json"
        "fmt"
        "hash/fnv"
        "log"
        "regexp"
        "sort"
        "strconv"
        "strings"
        "sync"
        "sync/atomic"
        "time"

        "highperf-api/internal/schema"

        "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type ClickHouseConnection interface {
        Query(ctx context.Context, query string, args ...interface{}) (driver.Rows, error)
        QueryRow(ctx context.Context, query string, args ...interface{}) driver.Row
        Exec(ctx context.Context, query string, args ...interface{}) error
        PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error)
        IsAvailable() bool
        Conn() driver.Conn
}

type SearchRepository struct {
        conn       ClickHouseConnection
        registry   *schema.SchemaRegistry
        entityRepo *EntityRepository
        mu         sync.RWMutex

        retryCount       uint64
        deadLetterCount  uint64
        collisionCount   uint64
        indexLagGauge    uint64
        tableIDValidated bool
}

func NewSearchRepository(conn ClickHouseConnection, registry *schema.SchemaRegistry) *SearchRepository {
        r := &SearchRepository{
                conn:       conn,
                registry:   registry,
                entityRepo: NewEntityRepository(conn),
        }

        if err := r.validateTableIDs(); err != nil {
                log.Printf("⚠️ Table ID validation: %v", err)
                atomic.AddUint64(&r.collisionCount, 1)
        } else {
                r.tableIDValidated = true
        }

        return r
}

type SearchResult struct {
        Data          []map[string]interface{} `json:"data"`
        NextCursor    string                   `json:"next_cursor,omitempty"`
        HasMore       bool                     `json:"has_more"`
        Count         int                      `json:"count"`
        SearchColumns []string                 `json:"search_columns,omitempty"`
        Aggregations  map[string]int           `json:"aggregations"`
}

type SearchParams struct {
        TableName     string
        SearchTerm    string
        SearchColumns []string
        Cursor        string
        Limit         int
        Filters       map[string]string
        DateFrom      *time.Time
}

type SyncStats struct {
        TableName    string
        RecordCount  int
        SyncDuration time.Duration
        LastSyncAt   time.Time
}

type SearchCursor struct {
        LastGlobalID string `json:"last_global_id"`
}

func marshalCursor(c SearchCursor) (string, error) {
        cursorJSON, err := json.Marshal(c)
        if err == nil {
                return base64.URLEncoding.EncodeToString(cursorJSON), nil
        }
        return "", err
}

func unmarshalCursor(cursorStr string) (*SearchCursor, error) {
        if cursorStr == "" {
                return &SearchCursor{LastGlobalID: ""}, nil
        }
        decoded, err := base64.URLEncoding.DecodeString(cursorStr)
        if err != nil {
                return &SearchCursor{LastGlobalID: ""}, nil
        }
        var c SearchCursor
        if err := json.Unmarshal(decoded, &c); err != nil {
                return &SearchCursor{LastGlobalID: ""}, nil
        }
        return &c, nil
}

func uint64SliceToStrings(ids []uint64) []string {
        strs := make([]string, len(ids))
        for i, id := range ids {
                strs[i] = fmt.Sprintf("%d", id)
        }
        return strs
}

func (r *SearchRepository) getGlobalStatsFromHashes(hashes []uint64) map[string]uint64 {
        if len(hashes) == 0 {
                return make(map[string]uint64)
        }

        // SECURITY: Use parameterized query to prevent SQL injection
        // ClickHouse uses ? for placeholders
        placeholders := make([]string, len(hashes))
        args := make([]interface{}, len(hashes))
        for i, h := range hashes {
                placeholders[i] = "?"
                args[i] = h
        }

        query := fmt.Sprintf(`
        SELECT table_name, sum(count) as count
        FROM search_token_stats
        WHERE token_hash IN (%s)
        GROUP BY table_name
    `, strings.Join(placeholders, ","))

        rows, err := r.conn.Query(context.Background(), query, args...)
        if err != nil {
                return make(map[string]uint64)
        }
        defer rows.Close()

        stats := make(map[string]uint64)
        for rows.Next() {
                var t string
                var count uint64
                if rows.Scan(&t, &count) == nil {
                        stats[t] = count
                }
        }
        return stats
}

func (r *SearchRepository) IsAvailable() bool {
        return r.conn != nil && r.conn.IsAvailable()
}

var validTableRegex = func() *regexp.Regexp {
        return regexp.MustCompile(`^[a-zA-Z0-9_]+$`)
}()

func validateTableName(name string) error {
        if name == "" {
                return fmt.Errorf("empty table name")
        }
        if !validTableRegex.MatchString(name) {
                return fmt.Errorf("invalid table name: %s", name)
        }
        return nil
}

// ⭐ UPDATED: Removed s_indx
func (r *SearchRepository) EnsureSearchIndex(ctx context.Context, tableName string) error {
        if !r.IsAvailable() {
                return fmt.Errorf("clickhouse not available")
        }

        if err := validateTableName(tableName); err != nil {
                return err
        }

        table := r.registry.GetTable(tableName)
        if table == nil {
                return fmt.Errorf("table not found: %s", tableName)
        }

        indexTableName := fmt.Sprintf("search_%s", tableName)

        createTableSQL := fmt.Sprintf(`
    CREATE TABLE IF NOT EXISTS %s (
        global_id UInt64 CODEC(ZSTD(3)),
        original_data String CODEC(ZSTD(3)),
        is_deleted UInt8 DEFAULT 0,
        synced_at DateTime CODEC(DoubleDelta, ZSTD(3)),
        updated_at DateTime CODEC(DoubleDelta, ZSTD(3))
    ) ENGINE = ReplacingMergeTree(updated_at)
    PARTITION BY toYYYYMM(updated_at)
    ORDER BY (global_id)
    SETTINGS index_granularity = 16384
`, indexTableName)

        return r.conn.Exec(ctx, createTableSQL)
}

// ⭐ Smart Tokenizer
func (r *SearchRepository) extractSearchableTokens(record map[string]interface{}, table *schema.TableInfo, pkCol string) []string {
        var tokens []string

        // Columns to strictly ignore (system/junk)
        // We ignore UUIDs by type later, but names help for timestamp columns
        ignoredCols := map[string]bool{
                "created_at": true,
                "updated_at": true,
                "deleted_at": true,
        }

        for _, col := range table.Columns {
                // 1. Skip System Columns
                if ignoredCols[col.Name] {
                        continue
                }

                // 2. Skip Primary Key (Junk token for search)
                if col.Name == pkCol {
                        continue
                }

                val, ok := record[col.Name]
                if !ok || val == nil {
                        continue
                }

                // 3. Process by Data Type
                switch strings.ToLower(col.DataType) {
                case "text", "character varying", "varchar", "char":
                        // Index Text
                        if str, ok := val.(string); ok {
                                tokens = append(tokens, strings.Fields(strings.ToLower(str))...)
                        }

                case "integer", "bigint", "smallint", "int", "int2", "int4", "int8":
                        // Index Integers (Statuses, FKs)
                        tokens = append(tokens, fmt.Sprintf("%d", val))

                case "numeric", "decimal", "double precision", "real", "float":
                        // Index Floats (2 decimal precision)
                        if f, ok := val.(float64); ok {
                                tokens = append(tokens, fmt.Sprintf("%.2f", f))
                        } else if f, ok := val.(float32); ok {
                                tokens = append(tokens, fmt.Sprintf("%.2f", f))
                        }

                case "date", "timestamp", "timestamptz":
                        // Index Dates (Searchable strings)
                        if t, ok := val.(time.Time); ok {
                                tokens = append(tokens, t.Format("2006"), t.Format("2006-01"), t.Format("2006-01-02"))
                        }

                case "boolean":
                        if b, ok := val.(bool); ok {
                                tokens = append(tokens, strconv.FormatBool(b))
                        }
                }
        }
        return tokens
}

func (r *SearchRepository) BulkIndex(ctx context.Context, tableName string, records []map[string]interface{}) error {
        if !r.IsAvailable() || len(records) == 0 {
                return nil
        }

        if err := validateTableName(tableName); err != nil {
                return fmt.Errorf("invalid table name: %w", err)
        }

        if err := r.EnsureSearchIndex(ctx, tableName); err != nil {
                return fmt.Errorf("failed to ensure search index: %w", err)
        }

        table := r.registry.GetTable(tableName)
        if table == nil {
                return fmt.Errorf("table not found: %s", tableName)
        }

        pkCol := "id"
        if len(table.PrimaryKey) > 0 {
                pkCol = table.PrimaryKey[0]
        }

        indexTableName := fmt.Sprintf("search_%s", tableName)
        startTime := time.Now()

        // 1. Main Data Batch (No s_indx)
        batch, err := r.conn.PrepareBatch(ctx, fmt.Sprintf(`
        INSERT INTO %s (global_id, original_data, is_deleted, synced_at, updated_at)
    `, indexTableName))
        if err != nil {
                return fmt.Errorf("failed to prepare batch: %w", err)
        }
        defer func() { _ = batch.Abort() }()

        // 2. Token Stream Batch
        tokenBatch, err := r.conn.PrepareBatch(ctx, `
        INSERT INTO search_token_entity (token_hash, token, global_id, table_name, updated_at)
    `)
        if err != nil {
                return fmt.Errorf("failed to prepare token batch: %w", err)
        }
        defer func() { _ = tokenBatch.Abort() }()

        // 3. Stats Batch
        statsBatch, err := r.conn.PrepareBatch(ctx, `
        INSERT INTO search_token_stats (token_hash, token, table_name, count, updated_at)
    `)
        if err != nil {
                return fmt.Errorf("failed to prepare stats batch: %w", err)
        }
        defer func() { _ = statsBatch.Abort() }()

        tableID := r.getTableIDDeterministic(tableName)
        now := time.Now()

        for _, record := range records {
                pkValueRaw, ok := record[pkCol]
                if !ok {
                        continue
                }

                pkVal := getUint64FromInterface(pkValueRaw)
                if pkVal == 0 {
                        continue
                }

                global_id := generateCompactGlobalID(tableID, pkVal)

                // ⭐ Use Smart Tokenizer
                tokens := r.extractSearchableTokens(record, table, pkCol)

                var updatedAt time.Time
                if val, ok := record["updated_at"].(time.Time); ok {
                        updatedAt = val
                }
                if updatedAt.IsZero() {
                        updatedAt = now
                }

                isDeleted := uint8(0)
                if val, ok := record["is_deleted"]; ok {
                        if deleted, ok := val.(bool); ok && deleted {
                                isDeleted = 1
                        }
                }

                originalData, _ := json.Marshal(record)

                // Append Main (No s_indx)
                if err := batch.Append(global_id, string(originalData), isDeleted, now, updatedAt); err != nil {
                        return fmt.Errorf("failed to append main batch: %w", err)
                }

                // Process Tokens
                seenTokens := make(map[uint64]bool)

                for _, token := range tokens {
                        if len(token) < 2 { // Skip single char noise
                                continue
                        }

                        tokenLower := strings.ToLower(token)
                        tokenHash := hashToken(tokenLower)

                        // 1. Token Stream
                        if err := tokenBatch.Append(tokenHash, tokenLower, global_id, tableName, now); err != nil {
                                log.Printf("warn: failed token append: %v", err)
                        }

                        // 2. Stats (Deduplicated per record)
                        if !seenTokens[tokenHash] {
                                seenTokens[tokenHash] = true
                                if err := statsBatch.Append(tokenHash, tokenLower, tableName, 1, now); err != nil {
                                        log.Printf("warn: failed stats append: %v", err)
                                }
                        }
                }
        }

        if err := batch.Send(); err != nil {
                return fmt.Errorf("main batch send failed: %w", err)
        }
        if err := tokenBatch.Send(); err != nil {
                return fmt.Errorf("token batch send failed: %w", err)
        }
        if err := statsBatch.Send(); err != nil {
                return fmt.Errorf("stats batch send failed: %w", err)
        }

        duration := time.Since(startTime)
        if len(records) > 100 {
                log.Printf("[BulkIndex] %s: %d records in %v", tableName, len(records), duration)
        }

        return nil
}

func getUint64FromInterface(val interface{}) uint64 {
        switch v := val.(type) {
        case int:
                return uint64(v)
        case int8:
                return uint64(v)
        case int16:
                return uint64(v)
        case int32:
                return uint64(v)
        case int64:
                return uint64(v)
        case uint:
                return uint64(v)
        case uint8:
                return uint64(v)
        case uint16:
                return uint64(v)
        case uint32:
                return uint64(v)
        case uint64:
                return v
        case float32:
                return uint64(v)
        case float64:
                return uint64(v)
        case string:
                if v == "" {
                        return 0
                }
                if x, err := strconv.ParseUint(v, 10, 64); err == nil {
                        return x
                }
        case []byte:
                if x, err := strconv.ParseUint(string(v), 10, 64); err == nil {
                        return x
                }
        }
        return 0
}

func (r *SearchRepository) getTableIDDeterministic(tableName string) uint16 {
        h := fnv.New64a()
        h.Write([]byte(tableName))
        hash := h.Sum64()
        return uint16(hash >> 48)
}

func generateCompactGlobalID(tableID uint16, localID uint64) uint64 {
        return (uint64(tableID) << 48) | (localID & 0xFFFFFFFFFFFF)
}

func hashToken(token string) uint64 {
        h := fnv.New64a()
        h.Write([]byte(strings.ToLower(token)))
        return h.Sum64()
}

// Legacy function kept for compatibility if needed elsewhere
func (r *SearchRepository) extractSearchableText(record map[string]interface{}, table *schema.TableInfo) string {
        return ""
}

func (r *SearchRepository) BulkIndexTokens(ctx context.Context, tableName string, records []map[string]interface{}) error {
        // Deprecated
        return nil
}

// ⭐ SEARCH FUNCTION
func (r *SearchRepository) SearchFullHistoryBitmap(ctx context.Context, searchTerm string, limit int, cursor string) (*SearchResult, error) {
        searchTerm = strings.ToLower(strings.TrimSpace(searchTerm))
        tokens := strings.Fields(searchTerm)

        if len(tokens) == 0 {
                return &SearchResult{Data: []map[string]interface{}{}}, nil
        }

        var tokenHashes []uint64
        for _, t := range tokens {
                tokenHashes = append(tokenHashes, hashToken(t))
        }

        c, _ := unmarshalCursor(cursor)

        var bitmapQuery string

        if len(tokenHashes) == 1 {
                bitmapQuery = fmt.Sprintf(`
        SELECT groupBitmapMergeState(ids_bitmap)
        FROM search_token_bitmap 
        WHERE token_hash = %d 
    `, tokenHashes[0])
        } else {
                var views []string
                for i, hash := range tokenHashes {
                        views = append(views, fmt.Sprintf(
                                "(SELECT groupBitmapMergeState(ids_bitmap) FROM search_token_bitmap WHERE token_hash = %d) as b%d",
                                hash, i,
                        ))
                }

                intersection := "b0"
                for i := 1; i < len(views); i++ {
                        intersection = fmt.Sprintf("bitmapAnd(%s, b%d)", intersection, i)
                }

                bitmapQuery = fmt.Sprintf("SELECT %s FROM (SELECT %s)", intersection, strings.Join(views, ", "))
        }

        finalQuery := fmt.Sprintf(`
    SELECT arrayJoin(bitmapToArray(ifNull(
        (%s),
        bitmapBuild(emptyArrayUInt64())
    ))) as global_id
`, bitmapQuery)

        var whereClause string
        if c.LastGlobalID != "" {
                parsedID, err := strconv.ParseUint(c.LastGlobalID, 10, 64)
                if err == nil {
                        whereClause = fmt.Sprintf(" WHERE global_id < %d", parsedID)
                }
        }

        query := fmt.Sprintf(`
    SELECT global_id FROM (
        %s
    ) 
    %s
    ORDER BY global_id DESC
    LIMIT %d
`, finalQuery, whereClause, limit+1)

        rows, err := r.conn.Query(ctx, query)
        if err != nil {
                return nil, fmt.Errorf("bitmap query failed: %w", err)
        }
        defer rows.Close()

        var globalIDs []uint64
        for rows.Next() {
                var id uint64
                if err := rows.Scan(&id); err != nil {
                        continue
                }
                globalIDs = append(globalIDs, id)
        }

        if len(globalIDs) == 0 {
                return &SearchResult{
                        Data:         []map[string]interface{}{},
                        Count:        0,
                        HasMore:      false,
                        Aggregations: make(map[string]int),
                }, nil
        }

        idsByTable := make(map[string][]uint64)
        for _, gid := range globalIDs {
                tid := extractTableID(gid)
                tableName := r.getTableName(tid)

                if tableName == "unknown" {
                        continue
                }
                idsByTable[tableName] = append(idsByTable[tableName], gid)
        }

        var allRecords []map[string]interface{}
        var wgData sync.WaitGroup
        var muData sync.Mutex

        for tableName, gIDs := range idsByTable {
                wgData.Add(1)
                go func(tName string, tIds []uint64) {
                        defer wgData.Done()

                        // SECURITY: Validate table name before query
                        if err := validateTableName(tName); err != nil {
                                log.Printf("Warning: invalid table name in search: %v", err)
                                return
                        }

                        // SECURITY: Use parameterized query for global IDs
                        placeholders := make([]string, len(tIds))
                        args := make([]interface{}, len(tIds))
                        for i, id := range tIds {
                                placeholders[i] = "?"
                                args[i] = id
                        }

                        // Table name is validated above, so it's safe to use in query
                        lookupQuery := fmt.Sprintf(`
            SELECT original_data, updated_at, global_id
            FROM search_%s
            WHERE global_id IN (%s) AND is_deleted = 0
            ORDER BY global_id DESC
        `, tName, strings.Join(placeholders, ","))

                        dataRows, err := r.conn.Query(ctx, lookupQuery, args...)
                        if err != nil {
                                return
                        }
                        defer dataRows.Close()

                        for dataRows.Next() {
                                var data string
                                var t time.Time
                                var globalID uint64

                                if err := dataRows.Scan(&data, &t, &globalID); err != nil {
                                        continue
                                }

                                var record map[string]interface{}
                                if err := json.Unmarshal([]byte(data), &record); err != nil {
                                        continue
                                }

                                // ⭐ Reconstruct ID for UI
                                localID := extractLocalID(globalID)

                                record["_source_table"] = tName
                                record["updated_at"] = t
                                record["id"] = localID // Return standard ID
                                record["global_id"] = strconv.FormatUint(globalID, 10)

                                muData.Lock()
                                allRecords = append(allRecords, record)
                                muData.Unlock()
                        }
                }(tableName, gIDs)
        }
        wgData.Wait()

        // ... (inside SearchFullHistoryBitmap, after wgData.Wait()) ...

        // 8. Final Sort by Global ID DESC
        // FIX: Parse strings back to uint64 for correct numeric sorting
        sort.Slice(allRecords, func(i, j int) bool {
                var idI, idJ uint64
                // Parse string to uint64
                if s, ok := allRecords[i]["global_id"].(string); ok {
                        idI, _ = strconv.ParseUint(s, 10, 64)
                }
                if s, ok := allRecords[j]["global_id"].(string); ok {
                        idJ, _ = strconv.ParseUint(s, 10, 64)
                }
                return idI > idJ
        })

        // 9. Pagination Logic
        hasMore := len(allRecords) > limit
        if hasMore {
                allRecords = allRecords[:limit]
        }

        // 10. Generate Next Cursor
        var nextCursor string
        if len(allRecords) > 0 {
                lastRecord := allRecords[len(allRecords)-1]

                // FIX: global_id is now a string, extract it
                var lastGlobalID uint64
                if idStr, ok := lastRecord["global_id"].(string); ok {
                        lastGlobalID, _ = strconv.ParseUint(idStr, 10, 64)
                }

                if hasMore {
                        nextCursor, _ = marshalCursor(SearchCursor{LastGlobalID: fmt.Sprintf("%d", lastGlobalID)})
                }
        }

        // 11. Stats
        pageAggregations := make(map[string]int)
        for _, r := range allRecords {
                if t, ok := r["_source_table"].(string); ok {
                        pageAggregations[t]++
                }
        }

        return &SearchResult{
                Data:         allRecords,
                Count:        len(allRecords),
                HasMore:      hasMore,
                NextCursor:   nextCursor,
                Aggregations: pageAggregations,
        }, nil
}

func (r *SearchRepository) IndexRecord(ctx context.Context, tableName string, id string, record map[string]interface{}) error {
        if !r.IsAvailable() {
                return nil
        }
        return r.BulkIndex(ctx, tableName, []map[string]interface{}{record})
}

func (r *SearchRepository) DeleteRecord(ctx context.Context, tableName string, id string) error {
        if !r.IsAvailable() {
                return nil
        }

        if err := r.EnsureSearchIndex(ctx, tableName); err != nil {
                return fmt.Errorf("failed to ensure search index: %w", err)
        }

        tableID := r.getTableIDDeterministic(tableName)
        pkVal := getUint64FromInterface(id)
        if pkVal == 0 {
                return fmt.Errorf("invalid id format: %s", id)
        }
        globalID := generateCompactGlobalID(tableID, pkVal)

        indexTableName := fmt.Sprintf("search_%s", tableName)

        // ⭐ UPDATED: No s_indx
        insertSQL := fmt.Sprintf(`
        INSERT INTO %s (global_id, original_data, is_deleted, updated_at)
        VALUES (?, '', 1, now())
    `, indexTableName)

        return r.conn.Exec(ctx, insertSQL, globalID)
}

func (r *SearchRepository) GetEntityRepository() *EntityRepository {
        return r.entityRepo
}

func (r *SearchRepository) InitializeEntitySearch(ctx context.Context) error {
        if r.entityRepo == nil {
                return fmt.Errorf("entity repository not initialized")
        }
        return r.entityRepo.Initialize(ctx)
}

func (r *SearchRepository) SearchWithCursor(
        ctx context.Context,
        tableName, searchTerm string,
        searchColumns []string,
        limit int,
        cursorStr string,
) ([]map[string]interface{}, string, bool, error) {
        res, err := r.SearchFullHistoryBitmap(ctx, searchTerm, limit, cursorStr)
        if err != nil {
                return nil, "", false, err
        }
        return res.Data, res.NextCursor, res.HasMore, nil
}

func (r *SearchRepository) GetSyncStats(ctx context.Context, tableName string) (*SyncStats, error) {
        if !r.IsAvailable() {
                return nil, fmt.Errorf("clickhouse not available")
        }

        indexTableName := fmt.Sprintf("search_%s", tableName)
        query := fmt.Sprintf(`
        SELECT
            COUNT(*) as record_count,
            MAX(synced_at) as last_sync
        FROM %s
        WHERE is_deleted = 0
    `, indexTableName)

        var recordCount int
        var lastSync time.Time
        row := r.conn.QueryRow(ctx, query)
        if err := row.Scan(&recordCount, &lastSync); err != nil {
                return nil, fmt.Errorf("failed to get sync stats: %w", err)
        }

        return &SyncStats{
                TableName:   tableName,
                RecordCount: recordCount,
                LastSyncAt:  lastSync,
        }, nil
}

func (r *SearchRepository) getTableName(id uint16) string {
        allTables := r.registry.GetAllTables()
        for _, table := range allTables {
                if r.getTableIDDeterministic(table.Name) == id {
                        return table.Name
                }
        }
        return "unknown"
}

func extractTableID(globalID uint64) uint16 {
        return uint16(globalID >> 48)
}

func (r *SearchRepository) validateTableIDs() error {
        seen := map[uint16]string{}
        for _, t := range r.registry.GetAllTables() {
                id := r.getTableIDDeterministic(t.Name)
                if prev, ok := seen[id]; ok && prev != t.Name {
                        return fmt.Errorf("table id collision detected: %d -> %s and %s", id, prev, t.Name)
                }
                seen[id] = t.Name
        }
        return nil
}

func (r *SearchRepository) InsertDeadLetter(ctx context.Context, tableName string, startID, endID uint64, attempts uint8, lastError, sampleData string) error {
        if err := validateTableName(tableName); err != nil {
                return err
        }
        query := `
        INSERT INTO search_index_errors (table_name, start_id, end_id, attempts, last_error, sample_data, created_at)
        VALUES (?, ?, ?, ?, ?, ?, now())
    `
        if err := r.conn.Exec(ctx, query, tableName, startID, endID, attempts, lastError, sampleData); err != nil {
                return fmt.Errorf("failed to insert dead-letter: %w", err)
        }
        return nil
}

func (r *SearchRepository) IncRetryCount(n uint64) {
        atomic.AddUint64(&r.retryCount, n)
}

func (r *SearchRepository) IncDeadLetterCount(n uint64) {
        atomic.AddUint64(&r.deadLetterCount, n)
}

func (r *SearchRepository) IncCollisionCount(n uint64) {
        atomic.AddUint64(&r.collisionCount, n)
}

func (r *SearchRepository) SetIndexLag(v uint64) {
        atomic.StoreUint64(&r.indexLagGauge, v)
}

type RepoMetrics struct {
        RetryCount     uint64 `json:"retry_count"`
        DeadLetter     uint64 `json:"dead_letter_count"`
        Collisions     uint64 `json:"table_id_collisions"`
        IndexLagGauge  uint64 `json:"index_lag"`
        TableIDChecked bool   `json:"table_id_validated"`
}

func (r *SearchRepository) GetMetrics() RepoMetrics {
        return RepoMetrics{
                RetryCount:     atomic.LoadUint64(&r.retryCount),
                DeadLetter:     atomic.LoadUint64(&r.deadLetterCount),
                Collisions:     atomic.LoadUint64(&r.collisionCount),
                IndexLagGauge:  atomic.LoadUint64(&r.indexLagGauge),
                TableIDChecked: r.tableIDValidated,
        }
}
