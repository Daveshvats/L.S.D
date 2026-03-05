package pipeline

import (
        "fmt"
        "regexp"
        "strings"
        "unicode"
)

// ============================================================================
// COLUMN NAME CLEANING
// ============================================================================

// CleanColumnNames standardizes column names for PostgreSQL compatibility
// - Converts to lowercase
// - Replaces special characters with underscores
// - Handles duplicates with numeric suffixes
// - Ensures valid SQL identifiers
func CleanColumnNames(columns []string) []string {
        cleaned := make([]string, 0, len(columns))
        seen := make(map[string]int, len(columns))
        specialCharsRe := regexp.MustCompile(`[^a-zA-Z0-9_]+`)
        multiUnderscoreRe := regexp.MustCompile(`_+`)

        for idx, col := range columns {
                // Normalize whitespace and remove special characters
                colStr := strings.TrimSpace(col)
                colClean := specialCharsRe.ReplaceAllString(colStr, "_")
                colClean = multiUnderscoreRe.ReplaceAllString(colClean, "_")
                colClean = strings.ToLower(strings.Trim(colClean, "_"))

                // Handle empty or invalid names
                if colClean == "" || isNumericString(colClean) {
                        colClean = fmt.Sprintf("column_%d", idx)
                }

                // Ensure doesn't start with number (PostgreSQL requirement)
                if len(colClean) > 0 && unicode.IsDigit(rune(colClean[0])) {
                        colClean = "col_" + colClean
                }

                // Handle duplicates
                if count, exists := seen[colClean]; exists {
                        seen[colClean] = count + 1
                        colClean = fmt.Sprintf("%s_%d", colClean, count+1)
                } else {
                        seen[colClean] = 0
                }

                cleaned = append(cleaned, colClean)
        }

        return cleaned
}

// ============================================================================
// DATA CLEANING
// ============================================================================

// CleanData removes completely empty rows from the dataset
func CleanData(rows [][]string) [][]string {
        if len(rows) == 0 {
                return rows
        }

        cleaned := make([][]string, 0, len(rows))
        for _, row := range rows {
                if !isEmptyRow(row) {
                        cleaned = append(cleaned, row)
                }
        }
        return cleaned
}

// ============================================================================
// EMPTY COLUMN DETECTION
// ============================================================================

// IdentifyEmptyColumns finds columns exceeding the empty threshold
// threshold: 0.99 means 99% of values are empty/null
func IdentifyEmptyColumns(rows [][]string, header []string, threshold float64) map[int]bool {
        emptyColumns := make(map[int]bool)
        totalRows := len(rows)

        if totalRows == 0 {
                return emptyColumns
        }

        for colIdx := range header {
                emptyCount := 0

                for _, row := range rows {
                        if colIdx >= len(row) || strings.TrimSpace(row[colIdx]) == "" {
                                emptyCount++
                        }
                }

                emptyRatio := float64(emptyCount) / float64(totalRows)
                if emptyRatio > threshold {
                        emptyColumns[colIdx] = true
                }
        }

        return emptyColumns
}

// RemoveEmptyColumns filters out columns identified as empty
func RemoveEmptyColumns(rows [][]string, header []string, emptyColumns map[int]bool) ([][]string, []string) {
        if len(emptyColumns) == 0 {
                return rows, header
        }

        // Filter header
        newHeader := make([]string, 0, len(header))
        for i, col := range header {
                if !emptyColumns[i] {
                        newHeader = append(newHeader, col)
                }
        }

        // Filter rows
        newRows := make([][]string, 0, len(rows))
        for _, row := range rows {
                newRow := make([]string, 0, len(newHeader))
                for i, val := range row {
                        if !emptyColumns[i] {
                                newRow = append(newRow, val)
                        }
                }
                newRows = append(newRows, newRow)
        }

        return newRows, newHeader
}

// RemoveColumnsFromSchema removes columns from schema definition
// Returns: filtered header, filtered types, and a mapping from new column index to original data index
func RemoveColumnsFromSchema(header []string, types map[string]string, emptyColumns map[int]bool) ([]string, map[string]string, map[int]int) {
        newHeader := make([]string, 0, len(header))
        newTypes := make(map[string]string, len(header))
        indexMap := make(map[int]int) // new column index -> original data index

        newIdx := 0
        for origIdx, col := range header {
                if !emptyColumns[origIdx] {
                        newHeader = append(newHeader, col)
                        newTypes[col] = types[col]
                        indexMap[newIdx] = origIdx
                        newIdx++
                }
        }

        return newHeader, newTypes, indexMap
}

// ============================================================================
// TYPE INFERENCE (FORCED TEXT)
// ============================================================================

// InferColumnTypes forces TEXT for all columns.
// This preserves the exact raw formatting of data (e.g., integers vs floats),
// preventing search tokenization mismatches caused by numeric type casting.
func InferColumnTypes(rows [][]string, header []string) map[string]string {
        types := make(map[string]string, len(header))

        for _, col := range header {
                types[col] = "TEXT"
        }

        return types
}

// ============================================================================
// JSON FLATTENING
// ============================================================================

// FlattenJSON recursively flattens nested JSON structures
// Arrays are converted to JSON strings to avoid type issues
func FlattenJSON(data interface{}, prefix string, result map[string]interface{}) {
        switch v := data.(type) {
        case map[string]interface{}:
                for key, value := range v {
                        newKey := key
                        if prefix != "" {
                                newKey = prefix + "_" + key
                        }
                        FlattenJSON(value, newKey, result)
                }

        case []interface{}:
                // Convert arrays to JSON strings to avoid unhashable types
                result[prefix] = fmt.Sprintf("%v", v)

        default:
                result[prefix] = v
        }
}

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

// isEmptyRow checks if all values in a row are empty
func isEmptyRow(row []string) bool {
        for _, val := range row {
                if strings.TrimSpace(val) != "" {
                        return false
                }
        }
        return true
}

// isNumericString checks if string is purely numeric
func isNumericString(s string) bool {
        if s == "" {
                return false
        }

        // Compile regex once (could be moved to package level for performance)
        matched, _ := regexp.MatchString(`^-?\d+\.?\d*$`, s)
        return matched
}
