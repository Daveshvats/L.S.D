package pipeline

// ptrBool returns a pointer to a bool value
func ptrBool(v bool) *bool { return &v }

// LoadOptions contains all configuration for file loading
type LoadOptions struct {
        // Header Configuration
        HasHeader     *bool    `json:"has_header"`     // Override detection: true/false
        HeaderRow     int      `json:"header_row"`     // Row number (1-indexed), 0 = auto-detect
        CustomHeaders []string `json:"custom_headers"` // User-provided column names
        SkipRows      int      `json:"skip_rows"`      // Skip N rows before processing

        // Delimiter Configuration
        Delimiter string `json:"delimiter"` // Override delimiter (e.g., ",", ";", "\t")
        Encoding  string `json:"encoding"`  // Override encoding (e.g., "UTF-8", "Latin-1")

        // Data Quality Options
        RemoveEmpty    *bool   `json:"remove_empty"`     // Remove empty rows (default: true)
        RemoveDupes    *bool   `json:"remove_duplicates"` // Remove duplicate rows (default: true)
        InferTypes     *bool   `json:"infer_types"`      // Enable type inference (default: false)
        EmptyThreshold float64 `json:"empty_threshold"`  // Threshold for removing empty columns (default: 0.99)

        // Safety Options
        MaxFileSize int64  `json:"max_file_size"` // Max file size in bytes (default: 500MB)
        TableName   string `json:"table_name"`    // Custom table name (optional)

        // Sheet Selection (Excel only)
        Sheets []string `json:"sheets"` // Specific sheets to process (empty = all)
}

// DefaultLoadOptions returns sensible defaults
func DefaultLoadOptions() LoadOptions {
        return LoadOptions{
                HasHeader:      nil, // auto-detect
                HeaderRow:      0,   // auto-detect
                SkipRows:       0,
                RemoveEmpty:    ptrBool(true),
                RemoveDupes:    ptrBool(true),
                InferTypes:     ptrBool(false),
                EmptyThreshold: 0.99,
                MaxFileSize:    500 * 1024 * 1024, // 500MB
        }
}

// MergeWithDefaults applies defaults for nil/zero values
func (o *LoadOptions) MergeWithDefaults() LoadOptions {
        defaults := DefaultLoadOptions()
        result := *o // copy

        if result.HasHeader == nil {
                result.HasHeader = defaults.HasHeader
        }
        if result.RemoveEmpty == nil {
                result.RemoveEmpty = defaults.RemoveEmpty
        }
        if result.RemoveDupes == nil {
                result.RemoveDupes = defaults.RemoveDupes
        }
        if result.InferTypes == nil {
                result.InferTypes = defaults.InferTypes
        }
        if result.EmptyThreshold == 0 {
                result.EmptyThreshold = defaults.EmptyThreshold
        }
        if result.MaxFileSize == 0 {
                result.MaxFileSize = defaults.MaxFileSize
        }

        return result
}

// HeaderInfo contains detailed header detection results
type HeaderInfo struct {
        HasHeader    bool      `json:"has_header"`
        HeaderRow    int       `json:"header_row"`    // Detected header row (1-indexed)
        Confidence   float64   `json:"confidence"`    // Detection confidence 0-1
        Reason       string    `json:"reason"`        // Why this was detected
        Preview      [][]string `json:"preview"`      // First N rows for user review
        ColumnCount  int       `json:"column_count"`
        SampleValues []string  `json:"sample_values"` // First value from each column
}

// PreviewFileResult contains file preview information
type PreviewFileResult struct {
        Encoding       string            `json:"encoding"`
        Delimiter      string            `json:"delimiter"`
        HeaderInfo     *HeaderInfo       `json:"header_info"`
        Preview        [][]string        `json:"preview"`
        ColumnCount    int               `json:"column_count"`
        TotalRows      int               `json:"total_rows,omitempty"`
        DetectedTypes  map[string]string `json:"detected_types,omitempty"`
        ColumnNames    []string          `json:"column_names,omitempty"`
}
