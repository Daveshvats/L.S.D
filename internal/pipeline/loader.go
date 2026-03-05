package pipeline

import (
        "context"
        "encoding/csv"
        "encoding/json"
        "fmt"
        "io"
        "log/slog"
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

// LoadAndInsert is the main entry point for loading any file type (backward compatible)
func (l *FileLoader) LoadAndInsert(ctx context.Context, filePath string) (interface{}, error) {
        return l.LoadAndInsertWithOptions(ctx, filePath, DefaultLoadOptions())
}

// LoadAndInsertWithOptions is the enhanced entry point with full configuration
func (l *FileLoader) LoadAndInsertWithOptions(ctx context.Context, filePath string, opts LoadOptions) (interface{}, error) {
        // Apply defaults for nil values
        opts = opts.MergeWithDefaults()

        // Security check: file size
        fileInfo, err := os.Stat(filePath)
        if err != nil {
                return nil, fmt.Errorf("cannot access file: %w", err)
        }
        if fileInfo.Size() > opts.MaxFileSize {
                return nil, fmt.Errorf("file too large: %d bytes (max: %d)", fileInfo.Size(), opts.MaxFileSize)
        }

        ext := strings.ToLower(filepath.Ext(filePath))

        switch ext {
        case ".csv", ".txt":
                return l.loadCSVWithOptions(ctx, filePath, opts)
        case ".xlsx", ".xls":
                return l.loadExcelWithOptions(ctx, filePath, opts)
        case ".json":
                return l.loadJSONWithOptions(ctx, filePath, opts)
        case ".jsonl":
                return l.loadJSONLWithOptions(ctx, filePath, opts)
        default:
                return nil, fmt.Errorf("unsupported file type: %s", ext)
        }
}

// loadCSVStreaming - memory-efficient streaming CSV loader
func (l *FileLoader) loadCSVStreaming(ctx context.Context, filePath string) (*LoadResult, error) {
        logger := slog.With("operation", "load_csv_streaming", "path", filePath)
        logger.Debug("starting streaming CSV load")

        // Detect encoding with context
        encoding, err := DetectEncoding(ctx, filePath)
        if err != nil {
                encoding = "UTF-8"
        }
        logger.Debug("detected encoding", "encoding", encoding)

        // Detect delimiter with context
        delimiter, err := DetectDelimiter(ctx, filePath, encoding)
        if err != nil {
                delimiter = ','
        }
        logger.Debug("detected delimiter", "delimiter", string(delimiter))

        // Detect header
        hasHeader, err := DetectHeader(filePath, encoding, delimiter)
        if err != nil {
                hasHeader = true
        }
        logger.Debug("header detection", "has_header", hasHeader)

        // Open file
        file, err := os.Open(filePath)
        if err != nil {
                return nil, err
        }
        defer file.Close()

        fileInfo, _ := file.Stat()
        fileSize := fileInfo.Size()
        logger.Debug("file size", "size_mb", float64(fileSize)/(1024*1024))

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

        logger.Debug("columns detected", "count", len(header))

        // STEP 1: Sample 1000 rows for type inference
        logger.Debug("sampling rows for type inference", "sample_size", 1000)
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

        logger.Debug("sampled rows", "count", len(sampleRows))

        // Infer types with 50% threshold (matching Python)
        columnTypes := InferColumnTypes(sampleRows, header)
        logger.Debug("inferred column types", "types", columnTypes)

        // Remove columns that are >99% empty
        // indexMap maps new column index -> original data column index
        emptyColumns := IdentifyEmptyColumns(sampleRows, header, 0.99)
        var indexMap map[int]int
        if len(emptyColumns) > 0 {
                logger.Debug("removing empty columns", "count", len(emptyColumns), "threshold_pct", 99)
                header, columnTypes, indexMap = RemoveColumnsFromSchema(header, columnTypes, emptyColumns)
        }

        // Generate table name
        tableName := generateTableName(filePath)

        // Create table
        if err := l.createTable(ctx, tableName, header, columnTypes); err != nil {
                return nil, err
        }

        // STEP 2: Stream and insert in chunks
        logger.Debug("starting streaming insert", "chunk_size", l.chunkSize)

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
                                logger.Debug("inserted final chunk", "chunk_rows", len(chunk), "total_rows", totalRows)
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
                        logger.Debug("inserted chunk", "chunk_rows", len(chunk), "total_rows", totalRows)
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
        logger := slog.With("operation", "load_excel_all_sheets", "path", filePath)
        f, err := excelize.OpenFile(filePath)
        if err != nil {
                return nil, fmt.Errorf("failed to open Excel file: %w", err)
        }
        defer f.Close()

        sheets := f.GetSheetList()
        if len(sheets) == 0 {
                return nil, fmt.Errorf("no sheets found in Excel file")
        }

        logger.Debug("found sheets", "count", len(sheets), "sheets", sheets)

        results := []LoadResult{}

        for _, sheetName := range sheets {
                logger.Debug("processing sheet", "sheet_name", sheetName)

                rows, err := f.GetRows(sheetName)
                if err != nil {
                        logger.Debug("failed to read sheet", "sheet_name", sheetName, "error", err)
                        continue
                }

                if len(rows) < 2 {
                        logger.Debug("sheet has insufficient data, skipping", "sheet_name", sheetName)
                        continue
                }

                result, err := l.processExcelSheet(ctx, filePath, sheetName, rows)
                if err != nil {
                        logger.Debug("failed to process sheet", "sheet_name", sheetName, "error", err)
                        continue
                }

                results = append(results, *result)
                logger.Debug("sheet processed successfully", "sheet_name", sheetName, "table_name", result.TableName, "rows_inserted", result.RowsInserted)
        }

        if len(results) == 0 {
                return nil, fmt.Errorf("no sheets could be processed")
        }

        return results, nil
}

func (l *FileLoader) processExcelSheet(ctx context.Context, filePath, sheetName string, rows [][]string) (*LoadResult, error) {
        logger := slog.With("operation", "process_excel_sheet", "path", filePath, "sheet", sheetName)
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

        logger.Debug("after removing empty rows", "row_count", len(cleanedRows))

        // Identify and remove empty columns (>99% threshold)
        emptyColumns := IdentifyEmptyColumns(cleanedRows, header, 0.99)
        finalRows, finalHeader := RemoveEmptyColumns(cleanedRows, header, emptyColumns)

        if len(emptyColumns) > 0 {
                logger.Debug("removed empty columns", "count", len(emptyColumns))
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
        return l.loadJSONLWithOptions(ctx, filePath, DefaultLoadOptions())
}

// =============================================================================
// ENHANCED LOADERS WITH OPTIONS
// =============================================================================

// loadCSVWithOptions loads CSV with full header control and options
func (l *FileLoader) loadCSVWithOptions(ctx context.Context, filePath string, opts LoadOptions) (*LoadResult, error) {
        logger := slog.With("operation", "load_csv_with_options", "path", filePath)
        // Detect or use provided encoding
        encoding := opts.Encoding
        if encoding == "" {
                var err error
                encoding, err = DetectEncoding(ctx, filePath)
                if err != nil {
                        encoding = "UTF-8"
                }
        }
        logger.Debug("using encoding", "encoding", encoding)

        // Detect or use provided delimiter
        var delimiter rune
        if opts.Delimiter != "" {
                delimiter = rune(opts.Delimiter[0])
        } else {
                var err error
                delimiter, err = DetectDelimiter(ctx, filePath, encoding)
                if err != nil {
                        delimiter = ','
                }
        }
        logger.Debug("using delimiter", "delimiter", string(delimiter))

        // Open file
        file, err := os.Open(filePath)
        if err != nil {
                return nil, err
        }
        defer file.Close()

        fileInfo, _ := file.Stat()
        fileSize := fileInfo.Size()
        logger.Debug("file size", "size_mb", float64(fileSize)/(1024*1024))

        // Create reader
        decoder := GetDecoder(encoding)
        reader := csv.NewReader(transform.NewReader(file, decoder))
        reader.Comma = delimiter
        reader.LazyQuotes = true
        reader.TrimLeadingSpace = true
        reader.ReuseRecord = true

        // Handle skip rows
        for i := 0; i < opts.SkipRows; i++ {
                if _, err := reader.Read(); err != nil {
                        return nil, fmt.Errorf("error skipping rows: %w", err)
                }
        }

        // Determine header based on options
        var header []string
        var hasHeader bool

        if len(opts.CustomHeaders) > 0 {
                // User provided custom headers
                header = CleanColumnNames(opts.CustomHeaders)
                hasHeader = false
                logger.Debug("using custom headers", "headers", header)
        } else if opts.HeaderRow > 0 {
                // User specified which row is the header
                // Skip to the header row
                for i := 1; i < opts.HeaderRow; i++ {
                        if _, err := reader.Read(); err != nil {
                                return nil, fmt.Errorf("error reaching header row: %w", err)
                        }
                }
                headerRow, err := reader.Read()
                if err != nil {
                        return nil, fmt.Errorf("error reading header row: %w", err)
                }
                header = CleanColumnNames(headerRow)
                hasHeader = false
                logger.Debug("using row as header", "row_num", opts.HeaderRow, "headers", header)
        } else if opts.HasHeader != nil {
                // User explicitly specified whether header exists
                hasHeader = *opts.HasHeader
                logger.Debug("user specified has_header", "has_header", hasHeader)

                // Read first row
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
                        // Rewind to process first row as data
                        file.Seek(0, 0)
                        reader = csv.NewReader(transform.NewReader(file, decoder))
                        reader.Comma = delimiter
                        reader.LazyQuotes = true
                        reader.TrimLeadingSpace = true
                        // Re-skip rows
                        for i := 0; i < opts.SkipRows; i++ {
                                reader.Read()
                        }
                }
        } else {
                // Auto-detect header with enhanced detection
                headerInfo, err := DetectHeaderEnhanced(ctx, filePath, encoding, delimiter, 10)
                if err != nil {
                        hasHeader = true // Default to true on error
                } else {
                        hasHeader = headerInfo.HasHeader
                        logger.Debug("auto-detected header",
                                "has_header", hasHeader,
                                "confidence", headerInfo.Confidence,
                                "reason", headerInfo.Reason)
                }

                // Read first row
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
                        // Rewind
                        file.Seek(0, 0)
                        reader = csv.NewReader(transform.NewReader(file, decoder))
                        reader.Comma = delimiter
                        reader.LazyQuotes = true
                        reader.TrimLeadingSpace = true
                        for i := 0; i < opts.SkipRows; i++ {
                                reader.Read()
                        }
                }
        }

        logger.Debug("columns detected", "count", len(header))

        // Sample rows for type inference
        sampleRows := make([][]string, 0, 1000)
        sampleReader := reader
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
        logger.Debug("sampled rows for type inference", "count", len(sampleRows))

        // Infer types
        columnTypes := make(map[string]string)
        if opts.InferTypes != nil && *opts.InferTypes {
                columnTypes = InferColumnTypes(sampleRows, header)
        } else {
                for _, col := range header {
                        columnTypes[col] = "TEXT"
                }
        }
        logger.Debug("column types", "types", columnTypes)

        // Remove empty columns
        emptyColumns := IdentifyEmptyColumns(sampleRows, header, opts.EmptyThreshold)
        var indexMap map[int]int
        if len(emptyColumns) > 0 {
                logger.Debug("removing empty columns", "count", len(emptyColumns), "threshold_pct", opts.EmptyThreshold*100)
                header, columnTypes, indexMap = RemoveColumnsFromSchema(header, columnTypes, emptyColumns)
        }

        // Generate table name
        tableName := opts.TableName
        if tableName == "" {
                tableName = generateTableName(filePath)
        }

        // Create table
        if err := l.createTable(ctx, tableName, header, columnTypes); err != nil {
                return nil, err
        }

        // Rewind and stream insert
        file.Seek(0, 0)
        streamDecoder := GetDecoder(encoding)
        streamReader := csv.NewReader(transform.NewReader(file, streamDecoder))
        streamReader.Comma = delimiter
        streamReader.LazyQuotes = true
        streamReader.TrimLeadingSpace = true
        streamReader.ReuseRecord = true

        // Skip initial rows
        for i := 0; i < opts.SkipRows; i++ {
                streamReader.Read()
        }

        // Skip header row if needed
        if hasHeader {
                streamReader.Read()
        }

        // Stream and insert
        chunk := make([][]string, 0, l.chunkSize)
        totalRows := 0
        seenRows := make(map[string]bool)

        removeDupes := opts.RemoveDupes == nil || *opts.RemoveDupes
        removeEmpty := opts.RemoveEmpty == nil || *opts.RemoveEmpty

        for {
                record, err := streamReader.Read()
                if err == io.EOF {
                        if len(chunk) > 0 {
                                if err := l.insertBatch(ctx, tableName, header, columnTypes, chunk, indexMap); err != nil {
                                        return nil, fmt.Errorf("failed to insert final batch: %w", err)
                                }
                                totalRows += len(chunk)
                        }
                        break
                }
                if err != nil {
                        continue
                }

                // Check for duplicates
                if removeDupes {
                        rowHash := strings.Join(record, "|")
                        if seenRows[rowHash] {
                                continue
                        }
                        seenRows[rowHash] = true
                }

                // Check if row is empty
                if removeEmpty && isEmptyRow(record) {
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
                        logger.Debug("inserted chunk", "chunk_rows", len(chunk), "total_rows", totalRows)
                        chunk = chunk[:0]

                        if removeDupes && totalRows%100000 == 0 {
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

// loadExcelWithOptions loads Excel with full header control
func (l *FileLoader) loadExcelWithOptions(ctx context.Context, filePath string, opts LoadOptions) ([]LoadResult, error) {
        logger := slog.With("operation", "load_excel_with_options", "path", filePath)
        f, err := excelize.OpenFile(filePath)
        if err != nil {
                return nil, fmt.Errorf("failed to open Excel file: %w", err)
        }
        defer f.Close()

        sheets := f.GetSheetList()
        if len(sheets) == 0 {
                return nil, fmt.Errorf("no sheets found in Excel file")
        }

        // Filter sheets if specified
        if len(opts.Sheets) > 0 {
                sheets = filterSheets(sheets, opts.Sheets)
        }

        logger.Debug("processing sheets", "count", len(sheets), "sheets", sheets)

        results := []LoadResult{}

        for _, sheetName := range sheets {
                logger.Debug("processing sheet", "sheet_name", sheetName)

                rows, err := f.GetRows(sheetName)
                if err != nil {
                        logger.Debug("failed to read sheet", "sheet_name", sheetName, "error", err)
                        continue
                }

                if len(rows) < 2 {
                        logger.Debug("sheet has insufficient data, skipping", "sheet_name", sheetName)
                        continue
                }

                result, err := l.processExcelSheetWithOptions(ctx, filePath, sheetName, rows, opts)
                if err != nil {
                        logger.Debug("failed to process sheet", "sheet_name", sheetName, "error", err)
                        continue
                }

                results = append(results, *result)
                logger.Debug("sheet processed successfully", "sheet_name", sheetName, "table_name", result.TableName, "rows_inserted", result.RowsInserted)
        }

        if len(results) == 0 {
                return nil, fmt.Errorf("no sheets could be processed")
        }

        return results, nil
}

// processExcelSheetWithOptions processes Excel sheet with custom options
func (l *FileLoader) processExcelSheetWithOptions(ctx context.Context, filePath, sheetName string, rows [][]string, opts LoadOptions) (*LoadResult, error) {
        logger := slog.With("operation", "process_excel_sheet_with_options", "path", filePath, "sheet", sheetName)
        if len(rows) < 1 {
                return nil, fmt.Errorf("no data in sheet")
        }

        // Determine header based on options
        var header []string
        var dataRows [][]string

        if len(opts.CustomHeaders) > 0 {
                // Custom headers provided
                header = CleanColumnNames(opts.CustomHeaders)
                dataRows = rows
                // Apply skip rows
                if opts.SkipRows > 0 && opts.SkipRows < len(dataRows) {
                        dataRows = dataRows[opts.SkipRows:]
                }
                logger.Debug("using custom headers for Excel sheet", "headers", header)
        } else if opts.HeaderRow > 0 {
                // Specific row as header
                headerIdx := opts.HeaderRow - 1 // Convert to 0-indexed
                if headerIdx >= len(rows) {
                        return nil, fmt.Errorf("header row %d out of bounds", opts.HeaderRow)
                }
                header = CleanColumnNames(rows[headerIdx])
                dataRows = rows[headerIdx+1:]
                logger.Debug("using row as header for Excel", "row_num", opts.HeaderRow, "headers", header)
        } else if opts.HasHeader != nil {
                // User specified
                if *opts.HasHeader {
                        header = CleanColumnNames(rows[0])
                        dataRows = rows[1:]
                } else {
                        header = make([]string, len(rows[0]))
                        for i := range header {
                                header[i] = fmt.Sprintf("column_%d", i)
                        }
                        header = CleanColumnNames(header)
                        dataRows = rows
                }
                logger.Debug("user specified has_header for Excel", "has_header", *opts.HasHeader)
        } else {
                // Default: first row is header
                header = CleanColumnNames(rows[0])
                dataRows = rows[1:]
                logger.Debug("using default header for Excel", "header_row", 1)
        }

        // Apply skip rows
        if opts.SkipRows > 0 && opts.SkipRows < len(dataRows) {
                dataRows = dataRows[opts.SkipRows:]
        }

        // Remove empty rows
        removeEmpty := opts.RemoveEmpty == nil || *opts.RemoveEmpty
        if removeEmpty {
                cleanedRows := make([][]string, 0, len(dataRows))
                for _, row := range dataRows {
                        if !isEmptyRow(row) {
                                cleanedRows = append(cleanedRows, row)
                        }
                }
                dataRows = cleanedRows
                logger.Debug("after removing empty rows", "row_count", len(dataRows))
        }

        // Remove empty columns
        emptyColumns := IdentifyEmptyColumns(dataRows, header, opts.EmptyThreshold)
        finalRows, finalHeader := RemoveEmptyColumns(dataRows, header, emptyColumns)

        if len(emptyColumns) > 0 {
                logger.Debug("removed empty columns", "count", len(emptyColumns))
        }

        // Infer types
        sampleSize := min(1000, len(finalRows))
        columnTypes := make(map[string]string)
        if opts.InferTypes != nil && *opts.InferTypes {
                columnTypes = InferColumnTypes(finalRows[:sampleSize], finalHeader)
        } else {
                for _, col := range finalHeader {
                        columnTypes[col] = "TEXT"
                }
        }

        // Generate table name
        tableName := opts.TableName
        if tableName == "" {
                tableName = generateTableNameWithSheet(filePath, sheetName)
        }

        // Create table
        if err := l.createTable(ctx, tableName, finalHeader, columnTypes); err != nil {
                return nil, fmt.Errorf("failed to create table: %w", err)
        }

        // Insert data
        rowCount := 0
        removeDupes := opts.RemoveDupes == nil || *opts.RemoveDupes
        seenRows := make(map[string]bool)

        for i := 0; i < len(finalRows); i += l.chunkSize {
                end := min(i+l.chunkSize, len(finalRows))
                chunk := finalRows[i:end]

                // Remove duplicates
                if removeDupes {
                        uniqueChunk := make([][]string, 0, len(chunk))
                        for _, row := range chunk {
                                rowHash := strings.Join(row, "|")
                                if !seenRows[rowHash] {
                                        seenRows[rowHash] = true
                                        uniqueChunk = append(uniqueChunk, row)
                                }
                        }
                        chunk = uniqueChunk
                }

                if len(chunk) > 0 {
                        if err := l.insertBatch(ctx, tableName, finalHeader, columnTypes, chunk, nil); err != nil {
                                return nil, fmt.Errorf("failed to insert batch: %w", err)
                        }
                        rowCount += len(chunk)
                }
        }

        return &LoadResult{
                TableName:    tableName,
                SheetName:    sheetName,
                RowsInserted: rowCount,
                Columns:      finalHeader,
        }, nil
}

// loadJSONWithOptions loads JSON with streaming support for large files
func (l *FileLoader) loadJSONWithOptions(ctx context.Context, filePath string, opts LoadOptions) (*LoadResult, error) {
        logger := slog.With("operation", "load_json_with_options", "path", filePath)
        file, err := os.Open(filePath)
        if err != nil {
                return nil, err
        }
        defer file.Close()

        // Use streaming decoder for memory efficiency
        decoder := json.NewDecoder(file)

        // Peek at first token
        token, err := decoder.Token()
        if err != nil {
                return nil, fmt.Errorf("invalid JSON: %w", err)
        }

        var allRecords []map[string]interface{}

        switch token {
        case json.Delim('['):
                // Array of objects - stream through them
                for decoder.More() {
                        var record map[string]interface{}
                        if err := decoder.Decode(&record); err != nil {
                                logger.Debug("skipping invalid JSON record", "error", err)
                                continue
                        }
                        allRecords = append(allRecords, record)

                        // Process in batches to avoid memory issues
                        if len(allRecords) >= 10000 {
                                // Could insert batch here for truly streaming
                        }
                }
        case json.Delim('{'):
                // Single object
                var record map[string]interface{}
                if err := decoder.Decode(&record); err != nil {
                        return nil, fmt.Errorf("invalid JSON object: %w", err)
                }
                allRecords = []map[string]interface{}{record}
        default:
                return nil, fmt.Errorf("JSON must be an object or array of objects")
        }

        if len(allRecords) == 0 {
                return nil, fmt.Errorf("no records in JSON file")
        }

        // Flatten and process
        var flatRecords []map[string]interface{}
        for _, record := range allRecords {
                flat := make(map[string]interface{})
                FlattenJSON(record, "", flat)
                flatRecords = append(flatRecords, flat)
        }

        // Collect all keys
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

        // Apply custom headers if provided
        var header []string
        if len(opts.CustomHeaders) > 0 {
                header = CleanColumnNames(opts.CustomHeaders)
        } else {
                header = CleanColumnNames(columns)
        }

        // Build rows
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

        // Clean data
        removeEmpty := opts.RemoveEmpty == nil || *opts.RemoveEmpty
        if removeEmpty {
                rows = CleanData(rows)
        }

        // Remove empty columns
        emptyColumns := IdentifyEmptyColumns(rows, header, opts.EmptyThreshold)
        finalRows, finalHeader := RemoveEmptyColumns(rows, header, emptyColumns)

        // Infer types
        sampleSize := min(1000, len(finalRows))
        columnTypes := make(map[string]string)
        if opts.InferTypes != nil && *opts.InferTypes {
                columnTypes = InferColumnTypes(finalRows[:sampleSize], finalHeader)
        } else {
                for _, col := range finalHeader {
                        columnTypes[col] = "TEXT"
                }
        }

        // Generate table name
        tableName := opts.TableName
        if tableName == "" {
                tableName = generateTableName(filePath)
        }

        if err := l.createTable(ctx, tableName, finalHeader, columnTypes); err != nil {
                return nil, err
        }

        // Insert data
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

// loadJSONLWithOptions loads JSONL with options
func (l *FileLoader) loadJSONLWithOptions(ctx context.Context, filePath string, opts LoadOptions) (*LoadResult, error) {
        logger := slog.With("operation", "load_jsonl_with_options", "path", filePath)
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
                        logger.Debug("skipping invalid JSON line", "error", err)
                        continue
                }
                rawRecords = append(rawRecords, record)
        }

        if len(rawRecords) == 0 {
                return nil, fmt.Errorf("no valid records in JSONL file")
        }

        // Create temp JSON file and use JSON loader
        // For simplicity, convert to JSON processing
        var flatRecords []map[string]interface{}
        for _, record := range rawRecords {
                flat := make(map[string]interface{})
                FlattenJSON(record, "", flat)
                flatRecords = append(flatRecords, flat)
        }

        // Collect all keys
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

        var header []string
        if len(opts.CustomHeaders) > 0 {
                header = CleanColumnNames(opts.CustomHeaders)
        } else {
                header = CleanColumnNames(columns)
        }

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

        removeEmpty := opts.RemoveEmpty == nil || *opts.RemoveEmpty
        if removeEmpty {
                rows = CleanData(rows)
        }

        emptyColumns := IdentifyEmptyColumns(rows, header, opts.EmptyThreshold)
        finalRows, finalHeader := RemoveEmptyColumns(rows, header, emptyColumns)

        sampleSize := min(1000, len(finalRows))
        columnTypes := make(map[string]string)
        if opts.InferTypes != nil && *opts.InferTypes {
                columnTypes = InferColumnTypes(finalRows[:sampleSize], finalHeader)
        } else {
                for _, col := range finalHeader {
                        columnTypes[col] = "TEXT"
                }
        }

        tableName := opts.TableName
        if tableName == "" {
                tableName = generateTableName(filePath)
        }

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

// PreviewFile returns a preview of the file without loading
func (l *FileLoader) PreviewFile(ctx context.Context, filePath string, previewRows int) (*PreviewFileResult, error) {
        if previewRows <= 0 {
                previewRows = 20
        }

        ext := strings.ToLower(filepath.Ext(filePath))

        switch ext {
        case ".csv", ".txt":
                return l.previewCSV(ctx, filePath, previewRows)
        case ".xlsx", ".xls":
                return l.previewExcel(filePath, previewRows)
        case ".json", ".jsonl":
                return l.previewJSON(filePath, previewRows)
        default:
                return nil, fmt.Errorf("unsupported file type: %s", ext)
        }
}

// previewCSV returns a preview of CSV file
func (l *FileLoader) previewCSV(ctx context.Context, filePath string, maxRows int) (*PreviewFileResult, error) {
        result := &PreviewFileResult{}

        encoding, err := DetectEncoding(ctx, filePath)
        if err != nil {
                encoding = "UTF-8"
        }
        result.Encoding = encoding

        delimiter, err := DetectDelimiter(ctx, filePath, encoding)
        if err != nil {
                delimiter = ','
        }
        result.Delimiter = string(delimiter)

        headerInfo, err := DetectHeaderEnhanced(ctx, filePath, encoding, delimiter, maxRows)
        if err != nil {
                return nil, err
        }
        result.HeaderInfo = headerInfo
        result.ColumnCount = headerInfo.ColumnCount
        result.Preview = headerInfo.Preview

        if len(result.Preview) > 0 {
                result.ColumnNames = CleanColumnNames(result.Preview[0])
        }

        return result, nil
}

// previewExcel returns a preview of Excel file
func (l *FileLoader) previewExcel(filePath string, maxRows int) (*PreviewFileResult, error) {
        result := &PreviewFileResult{
                Encoding: "UTF-8",
        }

        f, err := excelize.OpenFile(filePath)
        if err != nil {
                return nil, fmt.Errorf("failed to open Excel file: %w", err)
        }
        defer f.Close()

        sheets := f.GetSheetList()
        if len(sheets) == 0 {
                return nil, fmt.Errorf("no sheets found")
        }

        // Preview first sheet
        rows, err := f.GetRows(sheets[0])
        if err != nil {
                return nil, err
        }

        preview := make([][]string, 0, maxRows)
        for i := 0; i < maxRows && i < len(rows); i++ {
                rowCopy := make([]string, len(rows[i]))
                copy(rowCopy, rows[i])
                preview = append(preview, rowCopy)
        }

        result.Preview = preview
        if len(preview) > 0 {
                result.ColumnCount = len(preview[0])
                result.ColumnNames = CleanColumnNames(preview[0])
        }

        result.HeaderInfo = &HeaderInfo{
                HasHeader:   true,
                HeaderRow:   1,
                Confidence:  0.8,
                Reason:      "Excel files typically have headers in row 1",
                Preview:     preview,
                ColumnCount: result.ColumnCount,
        }

        return result, nil
}

// previewJSON returns a preview of JSON file
func (l *FileLoader) previewJSON(filePath string, maxRows int) (*PreviewFileResult, error) {
        result := &PreviewFileResult{
                Encoding: "UTF-8",
        }

        file, err := os.Open(filePath)
        if err != nil {
                return nil, err
        }
        defer file.Close()

        decoder := json.NewDecoder(file)
        var records []map[string]interface{}

        // Check if array or object
        token, err := decoder.Token()
        if err != nil {
                return nil, err
        }

        if token == json.Delim('[') {
                count := 0
                for decoder.More() && count < maxRows {
                        var record map[string]interface{}
                        if err := decoder.Decode(&record); err != nil {
                                continue
                        }
                        flat := make(map[string]interface{})
                        FlattenJSON(record, "", flat)
                        records = append(records, flat)
                        count++
                }
        } else if token == json.Delim('{') {
                var record map[string]interface{}
                if err := decoder.Decode(&record); err != nil {
                        return nil, err
                }
                flat := make(map[string]interface{})
                FlattenJSON(record, "", flat)
                records = append(records, flat)
        }

        if len(records) == 0 {
                return nil, fmt.Errorf("no records in JSON file")
        }

        // Collect keys
        allKeys := make(map[string]bool)
        for _, record := range records {
                for key := range record {
                        allKeys[key] = true
                }
        }

        var columns []string
        for key := range allKeys {
                columns = append(columns, key)
        }
        sort.Strings(columns)
        result.ColumnNames = CleanColumnNames(columns)
        result.ColumnCount = len(columns)

        // Build preview rows
        preview := make([][]string, 0, len(records))
        for _, record := range records {
                row := make([]string, len(columns))
                for i, col := range columns {
                        if val, ok := record[col]; ok && val != nil {
                                row[i] = fmt.Sprintf("%v", val)
                        }
                }
                preview = append(preview, row)
        }
        result.Preview = preview

        result.HeaderInfo = &HeaderInfo{
                HasHeader:   false,
                Confidence:  1.0,
                Reason:      "JSON uses object keys as column names",
                ColumnCount: result.ColumnCount,
                SampleValues: preview[0],
        }

        return result, nil
}

// filterSheets filters sheets based on inclusion list
func filterSheets(allSheets, includeSheets []string) []string {
        includeMap := make(map[string]bool)
        for _, s := range includeSheets {
                includeMap[s] = true
        }

        var result []string
        for _, s := range allSheets {
                if includeMap[s] {
                        result = append(result, s)
                }
        }
        return result
}

func (l *FileLoader) createTable(ctx context.Context, tableName string, columns []string, types map[string]string) error {
        logger := slog.With("operation", "create_table", "table_name", tableName)
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

        logger.Debug("created table", "table_name", tableName)
        return nil
}

func (l *FileLoader) insertBatch(ctx context.Context, tableName string, columns []string, types map[string]string, rows [][]string, indexMap map[int]int) error {
        logger := slog.With("operation", "insert_batch", "table_name", tableName)
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

        logger.Debug("batch config", "columns", activeColumns, "max_rows_per_batch", maxRowsPerBatch, "param_limit", 65535)

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
