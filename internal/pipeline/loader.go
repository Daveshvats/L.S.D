package pipeline

import (
        "context"
        "encoding/csv"
        "encoding/json"
        "fmt"
        "io"
        "log"
        "os"
        "path/filepath"
        "regexp"
        "sort"
        "strconv"
        "strings"
        "time"

        "github.com/jackc/pgx/v5/pgxpool"
        "github.com/xuri/excelize/v2"
        "golang.org/x/text/transform"
)

type LoadResult struct {
        TableName    string   `json:"table_name"`
        SheetName    string   `json:"sheet_name,omitempty"`
        RowsInserted int      `json:"rows_inserted"`
        Columns      []string `json:"columns"`
}

type FileLoader struct {
        pool      *pgxpool.Pool
        chunkSize int
}

func NewFileLoader(pool *pgxpool.Pool, chunkSize int) *FileLoader {
        return &FileLoader{
                pool:      pool,
                chunkSize: chunkSize,
        }
}

// LoadAndInsert is the main entry point for loading any file type
func (l *FileLoader) LoadAndInsert(ctx context.Context, filePath string) (interface{}, error) {
        ext := strings.ToLower(filepath.Ext(filePath))

        switch ext {
        case ".csv", ".txt":
                result, err := l.loadCSVStreaming(ctx, filePath)
                return result, err
        case ".xlsx", ".xls":
                // Returns []LoadResult for multi-sheet
                return l.loadExcelAllSheets(ctx, filePath)
        case ".json":
                result, err := l.loadJSON(ctx, filePath)
                return result, err
        case ".jsonl":
                result, err := l.loadJSONL(ctx, filePath)
                return result, err
        default:
                return nil, fmt.Errorf("unsupported file type: %s", ext)
        }
}

// loadCSVStreaming - memory-efficient streaming CSV loader
func (l *FileLoader) loadCSVStreaming(ctx context.Context, filePath string) (*LoadResult, error) {
        // Detect encoding
        encoding, err := DetectEncoding(filePath)
        if err != nil {
                encoding = "UTF-8"
        }
        log.Printf("Detected encoding: %s", encoding)

        // Detect delimiter
        delimiter, err := DetectDelimiter(filePath, encoding)
        if err != nil {
                delimiter = ','
        }
        log.Printf("Detected delimiter: %q", delimiter)

        // Detect header
        hasHeader, err := DetectHeader(filePath, encoding, delimiter)
        if err != nil {
                hasHeader = true
        }
        log.Printf("Has header: %v", hasHeader)

        // Open file
        file, err := os.Open(filePath)
        if err != nil {
                return nil, err
        }
        defer file.Close()

        fileInfo, _ := file.Stat()
        fileSize := fileInfo.Size()
        log.Printf("File size: %.2f MB", float64(fileSize)/(1024*1024))

        // Create reader
        decoder := GetDecoder(encoding)
        reader := csv.NewReader(transform.NewReader(file, decoder))
        reader.Comma = delimiter
        reader.LazyQuotes = true
        reader.TrimLeadingSpace = true
        reader.ReuseRecord = true

        // Read header
        var header []string
        firstRow, err := reader.Read()
        if err != nil {
                return nil, err
        }

        if hasHeader {
                header = CleanColumnNames(firstRow)
        } else {
                header = make([]string, len(firstRow))
                for i := range header {
                        header[i] = fmt.Sprintf("column_%d", i)
                }
                header = CleanColumnNames(header)
        }

        log.Printf("Columns: %d", len(header))

        // STEP 1: Sample 1000 rows for type inference
        log.Printf("Sampling first 1000 rows for type inference...")
        file.Seek(0, 0)
        sampleDecoder := GetDecoder(encoding)
        sampleReader := csv.NewReader(transform.NewReader(file, sampleDecoder))
        sampleReader.Comma = delimiter
        sampleReader.LazyQuotes = true

        if hasHeader {
                sampleReader.Read()
        }

        sampleRows := make([][]string, 0, 1000)
        for len(sampleRows) < 1000 {
                record, err := sampleReader.Read()
                if err == io.EOF {
                        break
                }
                if err != nil {
                        continue
                }
                rowCopy := make([]string, len(record))
                copy(rowCopy, record)
                sampleRows = append(sampleRows, rowCopy)
        }

        log.Printf("Sampled %d rows", len(sampleRows))

        // Infer types with 50% threshold (matching Python)
        columnTypes := InferColumnTypes(sampleRows, header)
        log.Printf("Inferred types: %v", columnTypes)

        // Remove columns that are >99% empty
        // indexMap maps new column index -> original data column index
        emptyColumns := IdentifyEmptyColumns(sampleRows, header, 0.99)
        var indexMap map[int]int
        if len(emptyColumns) > 0 {
                log.Printf("Removing %d empty columns (>99%% missing)", len(emptyColumns))
                header, columnTypes, indexMap = RemoveColumnsFromSchema(header, columnTypes, emptyColumns)
        }

        // Generate table name
        tableName := generateTableName(filePath)

        // Create table
        if err := l.createTable(ctx, tableName, header, columnTypes); err != nil {
                return nil, err
        }

        // STEP 2: Stream and insert in chunks
        log.Printf("Starting streaming insert (chunk size: %d)...", l.chunkSize)

        file.Seek(0, 0)
        streamDecoder := GetDecoder(encoding)
        streamReader := csv.NewReader(transform.NewReader(file, streamDecoder))
        streamReader.Comma = delimiter
        streamReader.LazyQuotes = true
        streamReader.TrimLeadingSpace = true
        streamReader.ReuseRecord = true

        if hasHeader {
                streamReader.Read()
        }

        chunk := make([][]string, 0, l.chunkSize)
        totalRows := 0
        seenRows := make(map[string]bool) // For duplicate detection

        for {
                record, err := streamReader.Read()
                if err == io.EOF {
                        if len(chunk) > 0 {
                                if err := l.insertBatch(ctx, tableName, header, columnTypes, chunk, indexMap); err != nil {
                                        return nil, fmt.Errorf("failed to insert final batch: %w", err)
                                }
                                totalRows += len(chunk)
                                log.Printf("Inserted final chunk: %d rows (total: %d)", len(chunk), totalRows)
                        }
                        break
                }
                if err != nil {
                        continue
                }

                // Check for duplicate rows
                rowHash := strings.Join(record, "|")
                if seenRows[rowHash] {
                        continue // Skip duplicate
                }
                seenRows[rowHash] = true

                // Check if row is completely empty
                if isEmptyRow(record) {
                        continue
                }

                rowCopy := make([]string, len(record))
                copy(rowCopy, record)
                chunk = append(chunk, rowCopy)

                if len(chunk) >= l.chunkSize {
                        if err := l.insertBatch(ctx, tableName, header, columnTypes, chunk, indexMap); err != nil {
                                return nil, fmt.Errorf("failed to insert batch: %w", err)
                        }
                        totalRows += len(chunk)
                        log.Printf("Inserted chunk: %d rows (total: %d)", len(chunk), totalRows)
                        chunk = chunk[:0]

                        // Clear seen rows periodically to avoid memory buildup
                        if totalRows%100000 == 0 {
                                seenRows = make(map[string]bool)
                        }
                }
        }

        return &LoadResult{
                TableName:    tableName,
                RowsInserted: totalRows,
                Columns:      header,
        }, nil
}

// loadExcelAllSheets - Process ALL sheets like Python
func (l *FileLoader) loadExcelAllSheets(ctx context.Context, filePath string) ([]LoadResult, error) {
        f, err := excelize.OpenFile(filePath)
        if err != nil {
                return nil, fmt.Errorf("failed to open Excel file: %w", err)
        }
        defer f.Close()

        sheets := f.GetSheetList()
        if len(sheets) == 0 {
                return nil, fmt.Errorf("no sheets found in Excel file")
        }

        log.Printf("Found %d sheets: %v", len(sheets), sheets)

        results := []LoadResult{}

        for _, sheetName := range sheets {
                log.Printf("Processing sheet: %s", sheetName)

                rows, err := f.GetRows(sheetName)
                if err != nil {
                        log.Printf("Failed to read sheet %s: %v", sheetName, err)
                        continue
                }

                if len(rows) < 2 {
                        log.Printf("Sheet %s has insufficient data, skipping", sheetName)
                        continue
                }

                result, err := l.processExcelSheet(ctx, filePath, sheetName, rows)
                if err != nil {
                        log.Printf("Failed to process sheet %s: %v", sheetName, err)
                        continue
                }

                results = append(results, *result)
                log.Printf("✓ Sheet '%s' → Table '%s' (%d rows)", sheetName, result.TableName, result.RowsInserted)
        }

        if len(results) == 0 {
                return nil, fmt.Errorf("no sheets could be processed")
        }

        return results, nil
}

func (l *FileLoader) processExcelSheet(ctx context.Context, filePath, sheetName string, rows [][]string) (*LoadResult, error) {
        if len(rows) < 2 {
                return nil, fmt.Errorf("insufficient data in sheet")
        }

        header := CleanColumnNames(rows[0])
        dataRows := rows[1:]

        // Remove empty rows
        cleanedRows := make([][]string, 0, len(dataRows))
        for _, row := range dataRows {
                if !isEmptyRow(row) {
                        cleanedRows = append(cleanedRows, row)
                }
        }

        log.Printf("After removing empty rows: %d rows", len(cleanedRows))

        // Identify and remove empty columns (>99% threshold)
        emptyColumns := IdentifyEmptyColumns(cleanedRows, header, 0.99)
        finalRows, finalHeader := RemoveEmptyColumns(cleanedRows, header, emptyColumns)

        if len(emptyColumns) > 0 {
                log.Printf("Removed %d empty columns", len(emptyColumns))
        }

        // Infer types
        sampleSize := min(1000, len(finalRows))
        columnTypes := InferColumnTypes(finalRows[:sampleSize], finalHeader)

        // Generate table name: filename_sheetname_timestamp
        tableName := generateTableNameWithSheet(filePath, sheetName)

        // Create table
        if err := l.createTable(ctx, tableName, finalHeader, columnTypes); err != nil {
                return nil, fmt.Errorf("failed to create table: %w", err)
        }

        // Insert data in chunks
        rowCount := 0
        seenRows := make(map[string]bool)

        for i := 0; i < len(finalRows); i += l.chunkSize {
                end := min(i+l.chunkSize, len(finalRows))
                chunk := finalRows[i:end]

                // Remove duplicates from chunk
                uniqueChunk := make([][]string, 0, len(chunk))
                for _, row := range chunk {
                        rowHash := strings.Join(row, "|")
                        if !seenRows[rowHash] {
                                seenRows[rowHash] = true
                                uniqueChunk = append(uniqueChunk, row)
                        }
                }

                if len(uniqueChunk) > 0 {
                        if err := l.insertBatch(ctx, tableName, finalHeader, columnTypes, uniqueChunk, nil); err != nil {
                                return nil, fmt.Errorf("failed to insert batch: %w", err)
                        }
                        rowCount += len(uniqueChunk)
                }
        }

        return &LoadResult{
                TableName:    tableName,
                SheetName:    sheetName,
                RowsInserted: rowCount,
                Columns:      finalHeader,
        }, nil
}

func (l *FileLoader) loadJSON(ctx context.Context, filePath string) (*LoadResult, error) {
        data, err := os.ReadFile(filePath)
        if err != nil {
                return nil, err
        }

        var rawRecords []interface{}
        if err := json.Unmarshal(data, &rawRecords); err != nil {
                var singleObj map[string]interface{}
                if err2 := json.Unmarshal(data, &singleObj); err2 != nil {
                        return nil, fmt.Errorf("invalid JSON: %w", err)
                }
                rawRecords = []interface{}{singleObj}
        }

        if len(rawRecords) == 0 {
                return nil, fmt.Errorf("no records in JSON file")
        }

        var flatRecords []map[string]interface{}
        for _, record := range rawRecords {
                flat := make(map[string]interface{})
                FlattenJSON(record, "", flat)
                flatRecords = append(flatRecords, flat)
        }

        allKeys := make(map[string]bool)
        for _, record := range flatRecords {
                for key := range record {
                        allKeys[key] = true
                }
        }

        var columns []string
        for key := range allKeys {
                columns = append(columns, key)
        }
        sort.Strings(columns)
        cleanedCols := CleanColumnNames(columns)

        var rows [][]string
        for _, record := range flatRecords {
                row := make([]string, len(columns))
                for i, col := range columns {
                        if val, ok := record[col]; ok && val != nil {
                                row[i] = fmt.Sprintf("%v", val)
                        }
                }
                rows = append(rows, row)
        }

        cleanedRows := CleanData(rows)
        emptyColumns := IdentifyEmptyColumns(cleanedRows, cleanedCols, 0.99)
        finalRows, finalHeader := RemoveEmptyColumns(cleanedRows, cleanedCols, emptyColumns)

        sampleSize := min(1000, len(finalRows))
        columnTypes := InferColumnTypes(finalRows[:sampleSize], finalHeader)

        tableName := generateTableName(filePath)

        if err := l.createTable(ctx, tableName, finalHeader, columnTypes); err != nil {
                return nil, err
        }

        rowCount := 0
        for i := 0; i < len(finalRows); i += l.chunkSize {
                end := min(i+l.chunkSize, len(finalRows))
                if err := l.insertBatch(ctx, tableName, finalHeader, columnTypes, finalRows[i:end], nil); err != nil {
                        return nil, err
                }
                rowCount += (end - i)
        }

        return &LoadResult{
                TableName:    tableName,
                RowsInserted: rowCount,
                Columns:      finalHeader,
        }, nil
}

func (l *FileLoader) loadJSONL(ctx context.Context, filePath string) (*LoadResult, error) {
        file, err := os.Open(filePath)
        if err != nil {
                return nil, err
        }
        defer file.Close()

        decoder := json.NewDecoder(file)
        var rawRecords []interface{}

        for decoder.More() {
                var record interface{}
                if err := decoder.Decode(&record); err != nil {
                        log.Printf("Skipping invalid JSON line: %v", err)
                        continue
                }
                rawRecords = append(rawRecords, record)
        }

        if len(rawRecords) == 0 {
                return nil, fmt.Errorf("no valid records in JSONL file")
        }

        // Same processing as JSON
        return l.loadJSON(ctx, filePath)
}

func (l *FileLoader) createTable(ctx context.Context, tableName string, columns []string, types map[string]string) error {
        dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS \"%s\"", tableName)
        if _, err := l.pool.Exec(ctx, dropSQL); err != nil {
                return err
        }

        var colDefs []string
        colDefs = append(colDefs, "s_indx BIGSERIAL PRIMARY KEY")

        for _, col := range columns {
                colType := types[col]
                if colType == "" {
                        colType = "TEXT"
                }
                colDefs = append(colDefs, fmt.Sprintf("\"%s\" %s", col, colType))
        }

        createSQL := fmt.Sprintf("CREATE TABLE \"%s\" (%s)", tableName, strings.Join(colDefs, ", "))
        if _, err := l.pool.Exec(ctx, createSQL); err != nil {
                return err
        }

        log.Printf("Created table: %s", tableName)
        return nil
}

func (l *FileLoader) insertBatch(ctx context.Context, tableName string, columns []string, types map[string]string, rows [][]string, indexMap map[int]int) error {
        if len(rows) == 0 {
                return nil
        }

        // Number of columns to insert (either all columns or filtered columns)
        activeColumns := len(columns)

        // PostgreSQL parameter limit is 65535
        // Calculate safe batch size: 65535 / number_of_columns
        maxParamsPerBatch := 65000 // Leave some buffer
        maxRowsPerBatch := maxParamsPerBatch / activeColumns

        if maxRowsPerBatch < 1 {
                maxRowsPerBatch = 1
        }

        log.Printf("Batch config: %d columns, max %d rows per batch (limit: 65535 params)", activeColumns, maxRowsPerBatch)

        // Split rows into smaller batches if needed
        for startIdx := 0; startIdx < len(rows); startIdx += maxRowsPerBatch {
                endIdx := startIdx + maxRowsPerBatch
                if endIdx > len(rows) {
                        endIdx = len(rows)
                }

                batch := rows[startIdx:endIdx]
                if err := l.executeBatch(ctx, tableName, columns, types, batch, indexMap); err != nil {
                        return err
                }
        }

        return nil
}

// executeBatch performs the actual INSERT query
// indexMap maps new column index -> original data column index
// If indexMap is nil, columns are used directly (no filtering was done)
func (l *FileLoader) executeBatch(ctx context.Context, tableName string, columns []string, types map[string]string, rows [][]string, indexMap map[int]int) error {
        if len(rows) == 0 {
                return nil
        }

        // If no indexMap, create a direct mapping (col i -> data index i)
        if indexMap == nil {
                indexMap = make(map[int]int)
                for i := range columns {
                        indexMap[i] = i
                }
        }

        var placeholders []string
        var values []interface{}
        paramIdx := 1

        for _, row := range rows {
                var rowPlaceholders []string

                // Iterate over columns using the indexMap to get correct data indices
                for newColIdx, col := range columns {
                        // Get the original data column index
                        origColIdx := indexMap[newColIdx]

                        var val interface{}
                        if origColIdx < len(row) {
                                val = strings.TrimSpace(row[origColIdx])

                                colType := types[col]
                                if colType == "INTEGER" || colType == "BIGINT" {
                                        if val != "" {
                                                if intVal, err := strconv.ParseInt(val.(string), 10, 64); err == nil {
                                                        val = intVal
                                                } else {
                                                        val = nil
                                                }
                                        } else {
                                                val = nil
                                        }
                                } else if colType == "NUMERIC" && val != "" {
                                        cleanVal := strings.ReplaceAll(val.(string), ",", "")
                                        cleanVal = strings.TrimPrefix(cleanVal, "$")
                                        cleanVal = strings.TrimPrefix(cleanVal, "€")
                                        if floatVal, err := strconv.ParseFloat(cleanVal, 64); err == nil {
                                                val = floatVal
                                        } else {
                                                val = nil
                                        }
                                } else if colType == "BOOLEAN" && val != "" {
                                        lower := strings.ToLower(val.(string))
                                        if lower == "true" || lower == "yes" || lower == "t" || lower == "y" || val == "1" {
                                                val = true
                                        } else if lower == "false" || lower == "no" || lower == "f" || lower == "n" || val == "0" {
                                                val = false
                                        } else {
                                                val = nil
                                        }
                                } else if val == "" {
                                        val = nil
                                }
                        } else {
                                val = nil
                        }

                        rowPlaceholders = append(rowPlaceholders, fmt.Sprintf("$%d", paramIdx))
                        values = append(values, val)
                        paramIdx++
                }

                placeholders = append(placeholders, "("+strings.Join(rowPlaceholders, ", ")+")")
        }

        // Quote column names
        quotedCols := make([]string, len(columns))
        for i, col := range columns {
                quotedCols[i] = fmt.Sprintf("\"%s\"", col)
        }

        insertSQL := fmt.Sprintf(
                "INSERT INTO \"%s\" (%s) VALUES %s",
                tableName,
                strings.Join(quotedCols, ", "),
                strings.Join(placeholders, ", "),
        )

        _, err := l.pool.Exec(ctx, insertSQL, values...)
        return err
}

func generateTableName(filePath string) string {
        base := filepath.Base(filePath)
        ext := filepath.Ext(base)
        name := strings.TrimSuffix(base, ext)

        re := regexp.MustCompile(`[^a-zA-Z0-9_]+`)
        clean := re.ReplaceAllString(name, "_")
        clean = strings.ToLower(strings.Trim(clean, "_"))

        timestamp := time.Now().Format("20060102_150405")
        tableName := fmt.Sprintf("%s_%s", clean, timestamp)

        if len(tableName) > 63 {
                tableName = tableName[:63]
        }

        return tableName
}

func generateTableNameWithSheet(filePath, sheetName string) string {
        base := filepath.Base(filePath)
        ext := filepath.Ext(base)
        name := strings.TrimSuffix(base, ext)

        re := regexp.MustCompile(`[^a-zA-Z0-9_]+`)

        cleanName := re.ReplaceAllString(name, "_")
        cleanName = strings.ToLower(strings.Trim(cleanName, "_"))

        cleanSheet := re.ReplaceAllString(sheetName, "_")
        cleanSheet = strings.ToLower(strings.Trim(cleanSheet, "_"))

        timestamp := time.Now().Format("20060102_150405")
        tableName := fmt.Sprintf("%s_%s_%s", cleanName, cleanSheet, timestamp)

        if len(tableName) > 63 {
                maxNameLen := 63 - len(cleanSheet) - len(timestamp) - 2
                if maxNameLen > 0 && len(cleanName) > maxNameLen {
                        cleanName = cleanName[:maxNameLen]
                        tableName = fmt.Sprintf("%s_%s_%s", cleanName, cleanSheet, timestamp)
                } else {
                        tableName = tableName[:63]
                }
        }

        return tableName
}
