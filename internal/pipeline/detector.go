package pipeline

import (
        "bufio"
        "context"
        "encoding/csv"
        "errors"
        "fmt"
        "io"
        "io/fs"
        "log/slog"
        "math"
        "os"
        "path/filepath"
        "slices"
        "strconv"
        "strings"

        "github.com/extrame/xls"
        "github.com/wlynxg/chardet"
        "github.com/xuri/excelize/v2"
        "golang.org/x/text/encoding/charmap"
        "golang.org/x/text/encoding/unicode"
        "golang.org/x/text/transform"
)

// =============================================================================
// CONFIGURATION CONSTANTS (fixes R2 - extract repeated literals)
// =============================================================================

const (
        // MaxEncodingDetectionBytes is the max bytes read for encoding detection
        // Reduced from 100KB to 32KB for efficiency (fixes P2)
        MaxEncodingDetectionBytes = 32 * 1024

        // DefaultPreviewRows is the default number of rows for preview
        DefaultPreviewRows = 10

        // MaxPreviewRows is the maximum preview rows allowed
        MaxPreviewRows = 100

        // EncodingDetectionConfidenceThreshold is the minimum confidence for encoding detection
        EncodingDetectionConfidenceThreshold = 70
)

// SupportedExtensions contains the list of supported file extensions
// (fixes R2 - single source of truth)
var SupportedExtensions = map[string]bool{
        ".csv":   true,
        ".xlsx":  true,
        ".xls":   true,
        ".json":  true,
        ".jsonl": true,
        ".txt":   true,
}

// =============================================================================
// CONTEXT-AWARE FUNCTIONS (fixes M2 - context propagation)
// =============================================================================

// DetectEncoding detects file encoding (UTF-8, Latin-1, Windows-1252, etc.)
// Now accepts context for cancellation support.
func DetectEncoding(ctx context.Context, filePath string) (string, error) {
        logger := slog.With("operation", "detect_encoding", "path", filePath)
        logger.Debug("starting encoding detection")

        // Check for context cancellation
        if err := ctx.Err(); err != nil {
                return "", fmt.Errorf("encoding detection cancelled: %w", err)
        }

        file, err := os.Open(filePath)
        if err != nil {
                return "", fmt.Errorf("failed to open file %s: %w", filePath, err)
        }
        defer file.Close()

        // Get file size to limit bytes read (fixes P2)
        stat, err := file.Stat()
        if err != nil {
                return "", fmt.Errorf("failed to stat file %s: %w", filePath, err)
        }

        // Read min(fileSize, MaxEncodingDetectionBytes) for analysis
        bytesToRead := MaxEncodingDetectionBytes
        if stat.Size() < int64(bytesToRead) {
                bytesToRead = int(stat.Size())
        }

        buffer := make([]byte, bytesToRead)
        n, err := file.Read(buffer)
        if err != nil && !errors.Is(err, io.EOF) {
                return "", fmt.Errorf("failed to read file %s: %w", filePath, err)
        }

        result := chardet.Detect(buffer[:n])
        if result.Encoding == "" || result.Confidence < EncodingDetectionConfidenceThreshold {
                logger.Debug("falling back to UTF-8 encoding", "confidence", result.Confidence, "encoding", result.Encoding)
                return "UTF-8", nil // Fallback
        }

        logger.Debug("encoding detected", "encoding", result.Encoding, "confidence", result.Confidence)
        return result.Encoding, nil
}

// GetDecoder returns the appropriate transform.Transformer for encoding
func GetDecoder(encoding string) transform.Transformer {
        switch encoding {
        case "ISO-8859-1", "Latin-1":
                return charmap.ISO8859_1.NewDecoder()
        case "Windows-1252":
                return charmap.Windows1252.NewDecoder()
        case "UTF-16LE":
                return unicode.UTF16(unicode.LittleEndian, unicode.IgnoreBOM).NewDecoder()
        case "UTF-16BE":
                return unicode.UTF16(unicode.BigEndian, unicode.IgnoreBOM).NewDecoder()
        default:
                return unicode.UTF8.NewDecoder()
        }
}

// DetectDelimiter analyzes CSV file to determine the best delimiter
func DetectDelimiter(ctx context.Context, filePath string, encoding string) (rune, error) {
        // Check for context cancellation
        if err := ctx.Err(); err != nil {
                return ',', fmt.Errorf("delimiter detection cancelled: %w", err)
        }

        file, err := os.Open(filePath)
        if err != nil {
                return ',', fmt.Errorf("failed to open file %s: %w", filePath, err)
        }
        defer file.Close()

        scanner := bufio.NewScanner(file)

        // Read first 10 lines
        var lines []string
        for i := 0; i < 10 && scanner.Scan(); i++ {
                lines = append(lines, scanner.Text())
        }

        if len(lines) == 0 {
                return ',', nil
        }

        // Test delimiters: comma, semicolon, tab, pipe, space
        delimiters := []rune{',', ';', '\t', '|', ' '}
        maxCols := 0
        bestDelim := ','

        for _, delim := range delimiters {
                consistent := true
                firstCount := -1
                avgCount := 0

                for _, line := range lines {
                        count := strings.Count(line, string(delim))
                        avgCount += count

                        if firstCount == -1 {
                                firstCount = count
                        } else if count != firstCount {
                                consistent = false
                        }
                }

                if len(lines) > 0 {
                        avgCount = avgCount / len(lines)
                }

                // Prefer consistent delimiters with more columns
                if consistent && firstCount > maxCols {
                        maxCols = firstCount
                        bestDelim = delim
                } else if !consistent && avgCount > maxCols {
                        maxCols = avgCount
                        bestDelim = delim
                }
        }

        return bestDelim, nil
}

// DetectHeader determines if the first row is a header or data (backward compatible)
func DetectHeader(filePath string, encoding string, delimiter rune) (bool, error) {
        info, err := DetectHeaderEnhanced(context.Background(), filePath, encoding, delimiter, 2)
        if err != nil {
                return false, err
        }
        return info.HasHeader, nil
}

// DetectHeaderEnhanced provides detailed header analysis with configurable preview rows
// Now accepts context for cancellation support.
func DetectHeaderEnhanced(ctx context.Context, filePath string, encoding string, delimiter rune, previewRows int) (*HeaderInfo, error) {
        logger := slog.With("operation", "detect_header", "path", filePath)

        // Check for context cancellation
        if err := ctx.Err(); err != nil {
                return nil, fmt.Errorf("header detection cancelled: %w", err)
        }

        info := &HeaderInfo{
                Preview: make([][]string, 0, previewRows),
        }

        // Open file
        file, err := os.Open(filePath)
        if err != nil {
                return nil, fmt.Errorf("failed to open file %s: %w", filePath, err)
        }
        defer file.Close()

        // Create reader with encoding support
        decoder := GetDecoder(encoding)
        reader := csv.NewReader(transform.NewReader(file, decoder))
        reader.Comma = delimiter
        reader.LazyQuotes = true

        // Read preview rows
        for i := 0; i < previewRows; i++ {
                // Check for cancellation periodically
                if i%10 == 0 && ctx.Err() != nil {
                        return nil, fmt.Errorf("header detection cancelled: %w", ctx.Err())
                }

                record, err := reader.Read()
                if errors.Is(err, io.EOF) {
                        break
                }
                if err != nil {
                        continue
                }
                // Use slices.Clone instead of manual copy (fixes O2)
                info.Preview = append(info.Preview, slices.Clone(record))
        }

        if len(info.Preview) < 2 {
                info.HasHeader = false
                info.Confidence = 0.5
                info.Reason = "Insufficient rows to determine header"
                return info, nil
        }

        info.ColumnCount = len(info.Preview[0])

        // Score each row as potential header
        scores := make([]float64, min(5, len(info.Preview)))
        for i := 0; i < len(scores) && i < len(info.Preview); i++ {
                scores[i] = scoreRowAsHeader(info.Preview[i])
        }

        // Find best header candidate
        maxScore := 0.0
        bestRow := 0
        for i, score := range scores {
                if score > maxScore {
                        maxScore = score
                        bestRow = i
                }
        }

        // Check for common header keywords in first row
        keywordScore := 0.0
        hasKeywords := false
        if len(info.Preview) > 0 {
                for _, val := range info.Preview[0] {
                        val = strings.ToLower(strings.TrimSpace(val))
                        if isHeaderKeyword(val) {
                                hasKeywords = true
                                keywordScore += 0.15
                        }
                }
        }

        // Check for delimiter rows (----, ====, etc.)
        delimiterRowIdx := detectDelimiterRow(info.Preview)

        // Determine final result
        if delimiterRowIdx >= 0 {
                // Found a delimiter row, header is the row before it
                info.HasHeader = true
                info.HeaderRow = delimiterRowIdx // 1-indexed (the header row)
                info.Confidence = 0.95
                info.Reason = fmt.Sprintf("Delimiter row detected at row %d, header appears to be row %d", delimiterRowIdx+1, delimiterRowIdx)
        } else if hasKeywords || maxScore > 0.6 {
                info.HasHeader = true
                info.HeaderRow = bestRow + 1 // 1-indexed
                info.Confidence = math.Min(1.0, maxScore+keywordScore)
                info.Reason = fmt.Sprintf("Row %d appears to be header (score: %.2f, keywords: %v)", info.HeaderRow, maxScore, hasKeywords)
        } else if maxScore > 0.4 {
                // Low confidence - could be header or not
                info.HasHeader = true
                info.HeaderRow = 1
                info.Confidence = maxScore
                info.Reason = fmt.Sprintf("Row 1 might be header (score: %.2f) - low confidence", maxScore)
        } else {
                info.HasHeader = false
                info.HeaderRow = 0
                info.Confidence = 1.0 - maxScore
                info.Reason = "No clear header row detected"
        }

        // Get sample values from first data row
        dataRowIdx := 0
        if info.HasHeader && info.HeaderRow > 0 && info.HeaderRow <= len(info.Preview) {
                dataRowIdx = info.HeaderRow
        }
        if dataRowIdx < len(info.Preview) {
                info.SampleValues = info.Preview[dataRowIdx]
        }

        logger.Debug("header detection complete", "has_header", info.HasHeader, "confidence", info.Confidence)
        return info, nil
}

// DetectHeaderWithOptions allows overriding detection with user options
// Now accepts context for cancellation support.
func DetectHeaderWithOptions(ctx context.Context, filePath string, opts LoadOptions) (*HeaderInfo, error) {
        logger := slog.With("operation", "detect_header_with_options", "path", filePath)
        logger.Debug("starting header detection with options")

        // Check for context cancellation
        if err := ctx.Err(); err != nil {
                return nil, fmt.Errorf("header detection cancelled: %w", err)
        }

        // Check if Excel file
        ext := strings.ToLower(filepath.Ext(filePath))
        if ext == ".xlsx" {
                return detectExcelHeader(ctx, filePath, opts)
        }
        if ext == ".xls" {
                return detectXLSHeader(ctx, filePath, opts)
        }

        // Determine encoding
        encoding := opts.Encoding
        if encoding == "" {
                var err error
                encoding, err = DetectEncoding(ctx, filePath)
                if err != nil {
                        encoding = "UTF-8"
                }
        }

        // Determine delimiter
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

        // Get enhanced detection
        info, err := DetectHeaderEnhanced(ctx, filePath, encoding, delimiter, 20)
        if err != nil {
                return nil, err
        }

        // Apply user overrides
        if opts.HasHeader != nil {
                info.HasHeader = *opts.HasHeader
                info.Confidence = 1.0
                info.Reason = "User-specified header setting"
        }

        if opts.HeaderRow > 0 {
                info.HeaderRow = opts.HeaderRow
                info.HasHeader = true
                info.Confidence = 1.0
                info.Reason = fmt.Sprintf("User-specified header row: %d", opts.HeaderRow)
        }

        return info, nil
}

// =============================================================================
// EXCEL DETECTION - REFACTORED (fixes R1 - deduplication)
// =============================================================================

// detectExcelHeader detects header information from Excel files (.xlsx)
func detectExcelHeader(ctx context.Context, filePath string, opts LoadOptions) (*HeaderInfo, error) {
        logger := slog.With("operation", "detect_excel_header", "path", filePath)
        logger.Debug("opening xlsx file")

        // Check for context cancellation
        if err := ctx.Err(); err != nil {
                return nil, fmt.Errorf("excel header detection cancelled: %w", err)
        }

        // Open Excel file
        f, err := excelize.OpenFile(filePath)
        if err != nil {
                return nil, fmt.Errorf("failed to open Excel file %s: %w", filePath, err)
        }
        defer f.Close()

        // Get first sheet
        sheets := f.GetSheetList()
        if len(sheets) == 0 {
                return nil, fmt.Errorf("no sheets found in Excel file: %s", filePath)
        }

        firstSheet := sheets[0]
        rows, err := f.GetRows(firstSheet)
        if err != nil {
                return nil, fmt.Errorf("failed to read sheet %s: %w", firstSheet, err)
        }

        logger.Debug("excel file loaded", "sheet", firstSheet, "rows", len(rows))

        // Use shared header analysis logic (fixes R1)
        return analyzeHeaderFromRows(rows, opts), nil
}

// detectXLSHeader detects header information from old Excel 97-2003 (.xls) files
func detectXLSHeader(ctx context.Context, filePath string, opts LoadOptions) (*HeaderInfo, error) {
        logger := slog.With("operation", "detect_xls_header", "path", filePath)
        logger.Debug("opening xls file")

        // Check for context cancellation
        if err := ctx.Err(); err != nil {
                return nil, fmt.Errorf("xls header detection cancelled: %w", err)
        }

        // Open XLS file using the xls library
        f, err := xls.Open(filePath, "utf-8")
        if err != nil {
                return nil, fmt.Errorf("failed to open XLS file %s: %w", filePath, err)
        }

        // Get first sheet
        sheet := f.GetSheet(0)
        if sheet == nil {
                return nil, fmt.Errorf("no sheets found in XLS file: %s", filePath)
        }

        // Read rows from the sheet
        var rows [][]string
        maxRows := int(sheet.MaxRow)
        for i := 0; i <= maxRows && i < MaxPreviewRows; i++ {
                // Check for cancellation periodically
                if i%20 == 0 && ctx.Err() != nil {
                        return nil, fmt.Errorf("xls row reading cancelled: %w", ctx.Err())
                }

                row := sheet.Row(i)
                if row == nil {
                        continue
                }
                var rowData []string
                maxCol := row.LastCol()
                for j := 0; j < maxCol; j++ {
                        cell := row.Col(j)
                        rowData = append(rowData, cell)
                }
                if len(rowData) > 0 {
                        rows = append(rows, rowData)
                }
        }

        logger.Debug("xls file loaded", "rows", len(rows))

        // Use shared header analysis logic (fixes R1)
        return analyzeHeaderFromRows(rows, opts), nil
}

// =============================================================================
// SHARED HEADER ANALYSIS (fixes R1 - extracts common logic)
// =============================================================================

// analyzeHeaderFromRows analyzes rows to detect header information
// This is the shared implementation used by both XLSX and XLS handlers.
func analyzeHeaderFromRows(rows [][]string, opts LoadOptions) *HeaderInfo {
        info := &HeaderInfo{
                Preview: make([][]string, 0),
        }

        if len(rows) == 0 {
                info.HasHeader = false
                info.Confidence = 0.5
                info.Reason = "Empty file"
                return info
        }

        // Get preview rows (up to DefaultPreviewRows)
        previewCount := min(DefaultPreviewRows, len(rows))
        for i := 0; i < previewCount; i++ {
                // Use slices.Clone (fixes O2)
                info.Preview = append(info.Preview, slices.Clone(rows[i]))
        }

        info.ColumnCount = len(rows[0])

        // Score first row as header
        if len(rows) >= 2 {
                score := scoreRowAsHeader(rows[0])

                // Also check if second row looks like data
                dataScore := scoreRowAsHeader(rows[1])

                // If first row scores high as header and second row scores low, it's likely a header
                if score > 0.5 && dataScore < score {
                        info.HasHeader = true
                        info.HeaderRow = 1
                        info.Confidence = math.Min(0.95, score)
                        info.Reason = fmt.Sprintf("First row appears to be header (score: %.2f)", score)
                } else {
                        // Check for header keywords
                        keywordCount := 0
                        for _, val := range rows[0] {
                                if isHeaderKeyword(strings.ToLower(strings.TrimSpace(val))) {
                                        keywordCount++
                                }
                        }

                        if keywordCount > len(rows[0])/2 {
                                info.HasHeader = true
                                info.HeaderRow = 1
                                info.Confidence = 0.9
                                info.Reason = "First row contains common header keywords"
                        } else {
                                info.HasHeader = true // Default to true for Excel
                                info.HeaderRow = 1
                                info.Confidence = 0.7
                                info.Reason = "Assuming first row is header (Excel default)"
                        }
                }

                // Get sample values from second row (first data row)
                if len(rows) > 1 {
                        info.SampleValues = rows[1]
                }
        } else {
                // Only one row
                info.HasHeader = false
                info.Confidence = 0.5
                info.Reason = "Only one row in file, cannot determine if header"
                if len(rows) > 0 {
                        info.SampleValues = rows[0]
                }
        }

        // Apply user overrides
        if opts.HasHeader != nil {
                info.HasHeader = *opts.HasHeader
                info.Confidence = 1.0
                info.Reason = "User-specified header setting"
        }

        if opts.HeaderRow > 0 {
                info.HeaderRow = opts.HeaderRow
                info.HasHeader = true
                info.Confidence = 1.0
                info.Reason = fmt.Sprintf("User-specified header row: %d", opts.HeaderRow)
        }

        return info
}

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

// scoreRowAsHeader returns a score 0-1 indicating likelihood of being a header
func scoreRowAsHeader(row []string) float64 {
        if len(row) == 0 {
                return 0
        }

        score := 0.0
        nonEmptyCount := 0

        for _, val := range row {
                val = strings.TrimSpace(val)
                if val == "" {
                        continue
                }
                nonEmptyCount++

                // Check if value looks like a header
                if looksLikeHeader(val) {
                        score += 1.0
                }

                // Bonus for header keywords
                if isHeaderKeyword(strings.ToLower(val)) {
                        score += 0.5
                }

                // Penalize numeric values
                if _, err := strconv.ParseFloat(val, 64); err == nil {
                        score -= 0.5
                }

                // Penalize very long values
                if len(val) > 50 {
                        score -= 0.3
                }

                // Penalize email addresses or URLs
                if strings.Contains(val, "@") || strings.Contains(val, "://") {
                        score -= 0.5
                }

                // Penalize dates
                if looksLikeDate(val) {
                        score -= 0.3
                }
        }

        if nonEmptyCount == 0 {
                return 0
        }

        // Normalize score
        return math.Max(0.0, math.Min(1.0, score/float64(nonEmptyCount)))
}

// isHeaderKeyword checks for common header keywords
func isHeaderKeyword(val string) bool {
        keywords := []string{
                "id", "name", "email", "phone", "address", "date", "time",
                "first", "last", "city", "state", "country", "zip", "code",
                "amount", "price", "total", "quantity", "qty", "count",
                "status", "type", "category", "description", "title",
                "created", "updated", "deleted", "timestamp", "user",
                "customer", "order", "product", "item", "value", "number",
                "serial", "sku", "barcode", "reference", "ref", "key",
                "firstname", "lastname", "username", "password", "token",
                "company", "organization", "org", "department", "dept",
                "phone", "mobile", "fax", "website", "url", "link",
                "age", "gender", "sex", "birth", "birthday", "year",
                "month", "day", "hour", "minute", "second",
                "start", "end", "begin", "finish", "from", "to",
                "active", "enabled", "visible", "hidden", "deleted",
                "note", "notes", "comment", "comments", "remark",
                "file", "path", "filename", "extension", "size",
                "currency", "tax", "discount", "subtotal", "grand",
                "invoice", "receipt", "payment", "balance", "due",
                "latitude", "longitude", "lat", "lng", "lon", "geo",
                "tags", "label", "labels", "group", "groups",
        }

        for _, kw := range keywords {
                if val == kw || strings.HasSuffix(val, "_"+kw) || strings.HasPrefix(val, kw+"_") {
                        return true
                }
                // Check for plural forms
                if val == kw+"s" || strings.HasSuffix(val, "_"+kw+"s") {
                        return true
                }
        }
        return false
}

// looksLikeHeader checks if a value looks like a column header
func looksLikeHeader(val string) bool {
        // Headers are usually short
        if len(val) > 40 {
                return false
        }

        // Headers usually don't contain numbers at the start
        if len(val) > 0 && (val[0] >= '0' && val[0] <= '9') {
                return false
        }

        // Headers often use underscores or camelCase
        if strings.Contains(val, "_") {
                return true
        }

        // Check for camelCase (has both upper and lower case, no spaces)
        hasLower := false
        hasUpper := false
        hasSpace := false
        for _, ch := range val {
                if ch >= 'a' && ch <= 'z' {
                        hasLower = true
                }
                if ch >= 'A' && ch <= 'Z' {
                        hasUpper = true
                }
                if ch == ' ' {
                        hasSpace = true
                }
        }

        // If it has mixed case and no spaces, likely a header
        if hasLower && hasUpper && !hasSpace {
                return true
        }

        // If it's all lowercase with no spaces, could be a header
        if hasLower && !hasUpper && !hasSpace && len(val) > 2 {
                return true
        }

        return false
}

// looksLikeDate checks if a value looks like a date
func looksLikeDate(val string) bool {
        datePatterns := []string{
                "2006-01-02", "01-02-2006", "02-01-2006",
                "2006/01/02", "01/02/2006", "02/01/2006",
                "Jan 02, 2006", "02 Jan 2006", "January 02, 2006",
        }
        for _, pattern := range datePatterns {
                if len(val) >= len(pattern) {
                        // Simple heuristic: contains date separators
                        if strings.Contains(val, "-") || strings.Contains(val, "/") {
                                // Check if it looks like a date
                                parts := strings.FieldsFunc(val, func(r rune) bool {
                                        return r == '-' || r == '/' || r == ' '
                                })
                                for _, part := range parts {
                                        if num, err := strconv.Atoi(part); err == nil {
                                                if num >= 1900 && num <= 2100 {
                                                        return true
                                                }
                                                if num >= 1 && num <= 31 {
                                                        continue
                                                }
                                        }
                                }
                        }
                }
        }
        return false
}

// detectDelimiterRow checks for rows that are delimiters (----, ====, etc.)
func detectDelimiterRow(rows [][]string) int {
        for i, row := range rows {
                if len(row) == 1 {
                        val := strings.TrimSpace(row[0])
                        // Check if it's all dashes or equals
                        allDashes := true
                        allEquals := true
                        allUnderscores := true

                        for _, ch := range val {
                                if ch != '-' {
                                        allDashes = false
                                }
                                if ch != '=' {
                                        allEquals = false
                                }
                                if ch != '_' {
                                        allUnderscores = false
                                }
                        }

                        if (allDashes || allEquals || allUnderscores) && len(val) >= 3 {
                                return i // Return the index of delimiter row
                        }
                }
        }
        return -1
}

// =============================================================================
// BACKWARD COMPATIBILITY WRAPPERS (for non-context callers)
// =============================================================================

// DetectEncodingWithoutCtx provides backward compatibility without context
func DetectEncodingWithoutCtx(filePath string) (string, error) {
        return DetectEncoding(context.Background(), filePath)
}

// DetectHeaderWithOptionsWithoutCtx provides backward compatibility without context
func DetectHeaderWithOptionsWithoutCtx(filePath string, opts LoadOptions) (*HeaderInfo, error) {
        return DetectHeaderWithOptions(context.Background(), filePath, opts)
}

// IsSupportedExtension checks if a file extension is supported
func IsSupportedExtension(ext string) bool {
        return SupportedExtensions[strings.ToLower(ext)]
}

// =============================================================================
// PATH VALIDATION HELPER
// =============================================================================

// ValidateFilePath validates a file path for security
func ValidateFilePath(filePath string) error {
        absPath, err := filepath.Abs(filePath)
        if err != nil {
                return fmt.Errorf("invalid path: %w", err)
        }

        // Check for path traversal
        if strings.Contains(absPath, "..") {
                return fmt.Errorf("path traversal not allowed")
        }

        // Check if file exists and is a regular file
        info, err := os.Stat(absPath)
        if err != nil {
                return fmt.Errorf("cannot access file: %w", err)
        }

        if !info.Mode().IsRegular() {
                return fmt.Errorf("not a regular file: %s", absPath)
        }

        return nil
}

// ValidateFolderPath validates a folder path for security
func ValidateFolderPath(folderPath string) error {
        absPath, err := filepath.Abs(folderPath)
        if err != nil {
                return fmt.Errorf("invalid path: %w", err)
        }

        // Check for path traversal
        if strings.Contains(absPath, "..") {
                return fmt.Errorf("path traversal not allowed")
        }

        // Check if folder exists
        info, err := os.Stat(absPath)
        if err != nil {
                return fmt.Errorf("cannot access folder: %w", err)
        }

        if !info.IsDir() {
                return fmt.Errorf("not a directory: %s", absPath)
        }

        return nil
}

// min returns the smaller of two integers (Go 1.21 has built-in, but keeping for compatibility)
// Note: This can be removed when minimum Go version is 1.21+
func min(a, b int) int {
        if a < b {
                return a
        }
        return b
}

// Helper to check if error is a file not found error (fixes O1)
func isNotExist(err error) bool {
        return errors.Is(err, fs.ErrNotExist)
}
