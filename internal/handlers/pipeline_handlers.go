package handlers

import (
        "context"
        "encoding/json"
        "fmt"
        "io"
        "log/slog"
        "mime/multipart"
        "net/http"
        "os"
        "path/filepath"
        "strconv"
        "strings"
        "time"

        "highperf-api/internal/middleware"
        "highperf-api/internal/pipeline"

        "github.com/google/uuid"
)

type PipelineHandler struct {
        processor *pipeline.PipelineProcessor
        loader    *pipeline.FileLoader
        scanner   *pipeline.FolderScanner
        configRepo *pipeline.ConfigRepository
}

func NewPipelineHandler(processor *pipeline.PipelineProcessor, loader *pipeline.FileLoader) *PipelineHandler {
        return &PipelineHandler{
                processor: processor,
                loader:    loader,
                scanner:   pipeline.NewFolderScanner(10),
        }
}

// NewPipelineHandlerWithConfig creates a handler with config repository
func NewPipelineHandlerWithConfig(processor *pipeline.PipelineProcessor, loader *pipeline.FileLoader, configRepo *pipeline.ConfigRepository) *PipelineHandler {
        return &PipelineHandler{
                processor: processor,
                loader:    loader,
                scanner:   pipeline.NewFolderScanner(10),
                configRepo: configRepo,
        }
}

// Backward compatible constructor
func NewPipelineHandlerOnly(processor *pipeline.PipelineProcessor) *PipelineHandler {
        return &PipelineHandler{
                processor: processor,
        }
}

// =============================================================================
// REQUEST/RESPONSE STRUCTURES
// =============================================================================

type StartJobRequest struct {
        FolderPath     string                 `json:"folder_path"`
        Recursive      bool                   `json:"recursive"`
        DefaultOptions pipeline.LoadOptions   `json:"default_options"`
        FileOptions    map[string]pipeline.LoadOptions `json:"file_options"` // Per-file overrides
}

type PreviewFileRequest struct {
        FilePath    string `json:"file_path"`
        Rows        int    `json:"rows"`         // Number of rows to preview (default: 20)
        DetectTypes bool   `json:"detect_types"` // Detect column types
}

type UploadFileRequest struct {
        File    multipart.File
        Header  *multipart.FileHeader
        Options pipeline.LoadOptions
}

// =============================================================================
// EXISTING ENDPOINTS (Enhanced)
// =============================================================================

// POST /api/pipeline/start
func (h *PipelineHandler) StartJob(w http.ResponseWriter, r *http.Request) {
        var req StartJobRequest
        if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
                h.writeError(w, http.StatusBadRequest, "Invalid request body")
                return
        }

        if req.FolderPath == "" {
                h.writeError(w, http.StatusBadRequest, "folder_path is required")
                return
        }

        // Security: validate path
        if err := h.validatePath(req.FolderPath); err != nil {
                h.writeError(w, http.StatusForbidden, err.Error())
                return
        }

        jobID := uuid.New().String()

        if err := h.processor.StartJob(r.Context(), jobID, req.FolderPath, req.Recursive); err != nil {
                h.writeError(w, http.StatusInternalServerError, err.Error())
                return
        }

        h.writeJSON(w, http.StatusAccepted, map[string]interface{}{
                "job_id":  jobID,
                "message": "Job started successfully",
        })
}

// GET /api/pipeline/jobs/{job_id}
func (h *PipelineHandler) GetJobStatus(w http.ResponseWriter, r *http.Request) {
        jobID := r.PathValue("job_id")
        if jobID == "" {
                h.writeError(w, http.StatusBadRequest, "job_id is required")
                return
        }

        progress, err := h.processor.GetJobProgress(jobID)
        if err != nil {
                h.writeError(w, http.StatusNotFound, err.Error())
                return
        }

        h.writeJSON(w, http.StatusOK, progress)
}

// GET /api/pipeline/jobs
func (h *PipelineHandler) ListJobs(w http.ResponseWriter, r *http.Request) {
        jobs := h.processor.ListJobs()
        h.writeJSON(w, http.StatusOK, map[string]interface{}{
                "jobs":  jobs,
                "count": len(jobs),
        })
}

// GET /api/pipeline/jobs/{job_id}/stream (SSE for live progress)
func (h *PipelineHandler) StreamJobProgress(w http.ResponseWriter, r *http.Request) {
        jobID := r.PathValue("job_id")

        w.Header().Set("Content-Type", "text/event-stream")
        w.Header().Set("Cache-Control", "no-cache")
        w.Header().Set("Connection", "keep-alive")

        flusher, ok := w.(http.Flusher)
        if !ok {
                http.Error(w, "Streaming not supported", http.StatusInternalServerError)
                return
        }

        ticker := time.NewTicker(500 * time.Millisecond)
        defer ticker.Stop()

        for {
                select {
                case <-r.Context().Done():
                        return
                case <-ticker.C:
                        progress, err := h.processor.GetJobProgress(jobID)
                        if err != nil {
                                return
                        }

                        data, _ := json.Marshal(progress)
                        fmt.Fprintf(w, "data: %s\n\n", data)
                        flusher.Flush()

                        if progress.Status == "completed" || progress.Status == "failed" {
                                return
                        }
                }
        }
}

// GET /api/pipeline/jobs/{job_id}/logs
func (h *PipelineHandler) GetJobLogs(w http.ResponseWriter, r *http.Request) {
        jobID := r.PathValue("job_id")
        if jobID == "" {
                h.writeError(w, http.StatusBadRequest, "job_id is required")
                return
        }

        progress, err := h.processor.GetJobProgress(jobID)
        if err != nil {
                h.writeError(w, http.StatusNotFound, err.Error())
                return
        }

        if progress.LogPath == "" {
                h.writeError(w, http.StatusNotFound, "Log file not found")
                return
        }

        // Read log file
        logFile, err := os.Open(progress.LogPath)
        if err != nil {
                h.writeError(w, http.StatusInternalServerError, "Failed to open log file")
                return
        }
        defer logFile.Close()

        // Set headers
        w.Header().Set("Content-Type", "text/plain; charset=utf-8")
        w.WriteHeader(http.StatusOK)

        // Stream log content
        io.Copy(w, logFile)
}

// =============================================================================
// NEW ENDPOINTS - FILE PREVIEW
// =============================================================================

// POST /api/pipeline/preview
// Returns a preview of the file before loading
func (h *PipelineHandler) PreviewFile(w http.ResponseWriter, r *http.Request) {
        var req PreviewFileRequest
        if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
                h.writeError(w, http.StatusBadRequest, "Invalid request body")
                return
        }

        if req.FilePath == "" {
                h.writeError(w, http.StatusBadRequest, "file_path is required")
                return
        }

        // Security: validate path
        if err := h.validatePath(req.FilePath); err != nil {
                h.writeError(w, http.StatusForbidden, err.Error())
                return
        }

        if req.Rows <= 0 {
                req.Rows = 20
        }

        // Check if file exists
        if _, err := os.Stat(req.FilePath); os.IsNotExist(err) {
                h.writeError(w, http.StatusNotFound, "File not found")
                return
        }

        // Use the loader's preview functionality
        result, err := h.loader.PreviewFile(r.Context(), req.FilePath, req.Rows)
        if err != nil {
                h.writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to preview file: %v", err))
                return
        }

        // Add detected types if requested
        if req.DetectTypes && len(result.Preview) > 1 {
                // Sample rows for type detection
                sampleRows := result.Preview[1:]
                if len(sampleRows) > 1000 {
                        sampleRows = sampleRows[:1000]
                }
                header := result.ColumnNames
                if len(result.Preview) > 0 {
                        header = result.Preview[0]
                }
                result.DetectedTypes = pipeline.InferColumnTypes(sampleRows, header)
        }

        h.writeJSON(w, http.StatusOK, result)
}

// =============================================================================
// NEW ENDPOINTS - FILE UPLOAD
// =============================================================================

// POST /api/pipeline/upload
// Upload and process a single file with custom options
func (h *PipelineHandler) UploadFile(w http.ResponseWriter, r *http.Request) {
        // Parse multipart form
        maxMemory := int64(64 << 20) // 64MB
        if err := r.ParseMultipartForm(maxMemory); err != nil {
                h.writeError(w, http.StatusBadRequest, "Failed to parse form: "+err.Error())
                return
        }

        file, header, err := r.FormFile("file")
        if err != nil {
                h.writeError(w, http.StatusBadRequest, "No file provided")
                return
        }
        defer file.Close()

        // Parse options
        var opts pipeline.LoadOptions
        optionsJSON := r.FormValue("options")
        if optionsJSON != "" {
                if err := json.Unmarshal([]byte(optionsJSON), &opts); err != nil {
                        h.writeError(w, http.StatusBadRequest, "Invalid options JSON: "+err.Error())
                        return
                }
        } else {
                opts = pipeline.DefaultLoadOptions()
        }

        // Validate file extension
        ext := strings.ToLower(filepath.Ext(header.Filename))
        if !isValidFileType(ext) {
                h.writeError(w, http.StatusBadRequest, fmt.Sprintf("Unsupported file type: %s", ext))
                return
        }

        // Save to temp file
        tmpFile, err := os.CreateTemp("", "upload-*"+ext)
        if err != nil {
                h.writeError(w, http.StatusInternalServerError, "Failed to create temp file")
                return
        }
        tmpPath := tmpFile.Name()
        defer os.Remove(tmpPath)

        if _, err := io.Copy(tmpFile, file); err != nil {
                tmpFile.Close()
                h.writeError(w, http.StatusInternalServerError, "Failed to save file")
                return
        }
        tmpFile.Close()

        // Check file size
        fileInfo, _ := os.Stat(tmpPath)
        maxSize := opts.MaxFileSize
        if maxSize == 0 {
                maxSize = 500 * 1024 * 1024 // 500MB default
        }
        if fileInfo.Size() > maxSize {
                h.writeError(w, http.StatusBadRequest, fmt.Sprintf("File too large: %d bytes (max: %d)", fileInfo.Size(), maxSize))
                return
        }

        slog.Debug("processing uploaded file", "filename", header.Filename, "size_mb", float64(fileInfo.Size())/(1024*1024))

        // Process file with options
        result, err := h.loader.LoadAndInsertWithOptions(r.Context(), tmpPath, opts)
        if err != nil {
                h.writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
                        "error":   err.Error(),
                        "status":  "failed",
                        "file":    header.Filename,
                })
                return
        }

        // Add file info to response
        switch v := result.(type) {
        case *pipeline.LoadResult:
                v.TableName = opts.TableName
                if v.TableName == "" {
                        v.TableName = generateCleanTableName(header.Filename)
                }
                h.writeJSON(w, http.StatusOK, map[string]interface{}{
                        "status":       "success",
                        "file":         header.Filename,
                        "table_name":   v.TableName,
                        "rows_inserted": v.RowsInserted,
                        "columns":      v.Columns,
                })
        case []pipeline.LoadResult:
                // Excel with multiple sheets
                h.writeJSON(w, http.StatusOK, map[string]interface{}{
                        "status":  "success",
                        "file":    header.Filename,
                        "sheets":  v,
                        "count":   len(v),
                })
        default:
                h.writeJSON(w, http.StatusOK, result)
        }
}

// =============================================================================
// NEW ENDPOINTS - LOAD SINGLE FILE
// =============================================================================

// POST /api/pipeline/load
// Load a file from a path with custom options
func (h *PipelineHandler) LoadFile(w http.ResponseWriter, r *http.Request) {
        var req struct {
                FilePath string              `json:"file_path"`
                Options  pipeline.LoadOptions `json:"options"`
        }

        if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
                h.writeError(w, http.StatusBadRequest, "Invalid request body")
                return
        }

        if req.FilePath == "" {
                h.writeError(w, http.StatusBadRequest, "file_path is required")
                return
        }

        // Security: validate path
        if err := h.validatePath(req.FilePath); err != nil {
                h.writeError(w, http.StatusForbidden, err.Error())
                return
        }

        // Check if file exists
        if _, err := os.Stat(req.FilePath); os.IsNotExist(err) {
                h.writeError(w, http.StatusNotFound, "File not found: "+req.FilePath)
                return
        }

        // Process file with options
        result, err := h.loader.LoadAndInsertWithOptions(r.Context(), req.FilePath, req.Options)
        if err != nil {
                h.writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
                        "error":  err.Error(),
                        "status": "failed",
                        "file":   req.FilePath,
                })
                return
        }

        // Return result
        switch v := result.(type) {
        case *pipeline.LoadResult:
                h.writeJSON(w, http.StatusOK, map[string]interface{}{
                        "status":        "success",
                        "file":          req.FilePath,
                        "table_name":    v.TableName,
                        "rows_inserted": v.RowsInserted,
                        "columns":       v.Columns,
                })
        case []pipeline.LoadResult:
                h.writeJSON(w, http.StatusOK, map[string]interface{}{
                        "status": "success",
                        "file":   req.FilePath,
                        "sheets": v,
                        "count":  len(v),
                })
        default:
                h.writeJSON(w, http.StatusOK, result)
        }
}

// =============================================================================
// NEW ENDPOINTS - HEADER DETECTION
// =============================================================================

// POST /api/pipeline/detect-header
// Detect header information from a file
func (h *PipelineHandler) DetectHeader(w http.ResponseWriter, r *http.Request) {
        var req struct {
                FilePath string             `json:"file_path"`
                Options  pipeline.LoadOptions `json:"options"`
        }

        if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
                h.writeError(w, http.StatusBadRequest, "Invalid request body")
                return
        }

        if req.FilePath == "" {
                h.writeError(w, http.StatusBadRequest, "file_path is required")
                return
        }

        // Security: validate path
        if err := h.validatePath(req.FilePath); err != nil {
                h.writeError(w, http.StatusForbidden, err.Error())
                return
        }

        // Get header info
        headerInfo, err := pipeline.DetectHeaderWithOptions(r.Context(), req.FilePath, req.Options)
        if err != nil {
                h.writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to detect header: %v", err))
                return
        }

        h.writeJSON(w, http.StatusOK, headerInfo)
}

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

// validatePath checks if the path is allowed
func (h *PipelineHandler) validatePath(path string) error {
        absPath, err := filepath.Abs(path)
        if err != nil {
                return fmt.Errorf("invalid path")
        }

        // Check for path traversal
        if strings.Contains(absPath, "..") {
                return fmt.Errorf("path traversal not allowed")
        }

        // Check for sensitive system paths (Unix)
        unixSensitivePaths := []string{"/etc", "/root", "/home", "/var", "/usr", "/bin", "/sbin", "/boot", "/proc", "/sys"}
        for _, sensitive := range unixSensitivePaths {
                if strings.HasPrefix(absPath, sensitive+"/") || absPath == sensitive {
                        return fmt.Errorf("access to %s is not allowed", sensitive)
                }
        }

        // Check for sensitive system paths (Windows)
        // Normalize path for comparison
        normalizedPath := filepath.ToSlash(absPath)
        windowsSensitivePaths := []string{
                "C:/Windows", "C:/Program Files", "C:/Program Files (x86)",
                "C:/ProgramData", "C:/System Volume Information",
        }
        for _, sensitive := range windowsSensitivePaths {
                normalizedSensitive := filepath.ToSlash(sensitive)
                if strings.HasPrefix(normalizedPath, normalizedSensitive+"/") || 
                        strings.EqualFold(normalizedPath, normalizedSensitive) {
                        return fmt.Errorf("access to %s is not allowed", sensitive)
                }
        }

        return nil
}

// isValidFileType checks if the file extension is supported
func isValidFileType(ext string) bool {
        validTypes := map[string]bool{
                ".csv":   true,
                ".txt":   true,
                ".xlsx":  true,
                ".xls":   true,
                ".json":  true,
                ".jsonl": true,
        }
        return validTypes[ext]
}

// generateCleanTableName creates a clean table name from filename
func generateCleanTableName(filename string) string {
        base := filepath.Base(filename)
        ext := filepath.Ext(base)
        name := strings.TrimSuffix(base, ext)
        
        // Clean the name
        name = strings.Map(func(r rune) rune {
                if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' {
                        return r
                }
                return '_'
        }, name)
        
        name = strings.ToLower(name)
        name = strings.Trim(name, "_")
        
        if name == "" {
                name = "data"
        }
        
        // Add timestamp
        timestamp := time.Now().Format("20060102_150405")
        tableName := fmt.Sprintf("%s_%s", name, timestamp)
        
        // Limit length
        if len(tableName) > 63 {
                tableName = tableName[:63]
        }
        
        return tableName
}

// =============================================================================
// FOLDER SCANNING ENDPOINTS
// =============================================================================

// ScanFolderRequest represents a folder scan request
type ScanFolderRequest struct {
        FolderPath string `json:"folder_path"`
        Recursive  bool   `json:"recursive"`
        DeepScan   bool   `json:"deep_scan"`   // If true, analyze headers/delimiters
        Parallel   bool   `json:"parallel"`    // Use parallel processing
}

// POST /api/pipeline/scan-folder
// Scan a folder and return all files with detected properties
func (h *PipelineHandler) ScanFolder(w http.ResponseWriter, r *http.Request) {
        var req ScanFolderRequest
        if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
                h.writeError(w, http.StatusBadRequest, "Invalid request body")
                return
        }

        if req.FolderPath == "" {
                h.writeError(w, http.StatusBadRequest, "folder_path is required")
                return
        }

        // Security: validate path
        if err := h.validatePath(req.FolderPath); err != nil {
                h.writeError(w, http.StatusForbidden, err.Error())
                return
        }

        // Create a context with a longer timeout for folder scanning (5 minutes)
        ctx, cancel := context.WithTimeout(r.Context(), 5*time.Minute)
        defer cancel()

        var result *pipeline.FolderScanResult
        var err error

        slog.Info("scan-folder started",
                "path", req.FolderPath,
                "recursive", req.Recursive,
                "deep_scan", req.DeepScan,
                "parallel", req.Parallel)

        if req.DeepScan {
                if req.Parallel {
                        result, err = h.scanner.ScanFolderParallel(ctx, req.FolderPath, req.Recursive, 4)
                } else {
                        result, err = h.scanner.ScanFolder(ctx, req.FolderPath, req.Recursive)
                }
        } else {
                result, err = h.scanner.QuickScan(ctx, req.FolderPath, req.Recursive)
        }

        if err != nil {
                // Check for context cancellation
                if ctx.Err() == context.Canceled {
                        slog.Warn("scan-folder cancelled by client", "path", req.FolderPath)
                        h.writeError(w, http.StatusRequestTimeout, "Scan cancelled by client")
                        return
                }
                if ctx.Err() == context.DeadlineExceeded {
                        slog.Warn("scan-folder timed out", "path", req.FolderPath)
                        h.writeError(w, http.StatusGatewayTimeout, "Scan timed out - try using deep_scan=false or a smaller folder")
                        return
                }
                slog.Error("scan-folder failed", "path", req.FolderPath, "error", err)
                h.writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to scan folder: %v", err))
                return
        }

        slog.Info("scan-folder completed",
                "path", req.FolderPath,
                "total_files", result.TotalFiles,
                "supported_files", result.SupportedFiles,
                "duration_ms", result.ScanDurationMs)

        h.writeJSON(w, http.StatusOK, result)
}

// =============================================================================
// CONFIGURATION MANAGEMENT ENDPOINTS
// =============================================================================

// SaveConfigRequest represents a config save request
type SaveConfigRequest struct {
        ID          string                       `json:"id,omitempty"` // Optional for update
        Name        string                       `json:"name"`
        Description string                       `json:"description"`
        FolderPath  string                       `json:"folder_path"`
        Recursive   bool                         `json:"recursive"`
        DefaultOptions pipeline.LoadOptions      `json:"default_options"`
        FileOptions   map[string]pipeline.LoadOptions `json:"file_options"`
        FileDiscoveries []pipeline.FileDiscovery `json:"file_discoveries,omitempty"`
}

// POST /api/pipeline/configs
// Save a pipeline configuration
func (h *PipelineHandler) SaveConfig(w http.ResponseWriter, r *http.Request) {
        if h.configRepo == nil {
                h.writeError(w, http.StatusInternalServerError, "Configuration storage not available")
                return
        }

        var req SaveConfigRequest
        if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
                h.writeError(w, http.StatusBadRequest, "Invalid request body")
                return
        }

        if req.Name == "" {
                h.writeError(w, http.StatusBadRequest, "name is required")
                return
        }

        if req.FolderPath == "" {
                h.writeError(w, http.StatusBadRequest, "folder_path is required")
                return
        }

        // Security: validate path
        if err := h.validatePath(req.FolderPath); err != nil {
                h.writeError(w, http.StatusForbidden, err.Error())
                return
        }

        // Get user ID from context
        userCtx := middleware.GetUserContext(r.Context())
        if userCtx == nil {
                h.writeError(w, http.StatusUnauthorized, "Authentication required")
                return
        }

        // Convert user ID from string to int
        userID, err := strconv.Atoi(userCtx.UserID)
        if err != nil {
                h.writeError(w, http.StatusBadRequest, fmt.Sprintf("Invalid user ID format: %q", userCtx.UserID))
                return
        }

        // Validate user ID (fixes O4 - reject negative/zero values)
        if userID <= 0 {
                h.writeError(w, http.StatusBadRequest, fmt.Sprintf("Invalid user ID: must be positive, got %d", userID))
                return
        }

        config := &pipeline.PipelineConfig{
                ID:              req.ID,
                UserID:          userID,
                Name:            req.Name,
                Description:     req.Description,
                FolderPath:      req.FolderPath,
                Recursive:       req.Recursive,
                DefaultOptions:  req.DefaultOptions,
                FileOptions:     req.FileOptions,
                FileDiscoveries: req.FileDiscoveries,
                CreatedAt:       time.Now(),
                UpdatedAt:       time.Now(),
        }

        // Generate ID if not provided
        if config.ID == "" {
                config.ID = uuid.New().String()
        }

        if err := h.configRepo.Save(r.Context(), config); err != nil {
                h.writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to save config: %v", err))
                return
        }

        h.writeJSON(w, http.StatusOK, map[string]interface{}{
                "success": true,
                "message": "Configuration saved successfully",
                "config":  config,
        })
}

// GET /api/pipeline/configs
// List all configurations for the current user
func (h *PipelineHandler) ListConfigs(w http.ResponseWriter, r *http.Request) {
        if h.configRepo == nil {
                h.writeError(w, http.StatusInternalServerError, "Configuration storage not available")
                return
        }

        userCtx := middleware.GetUserContext(r.Context())
        if userCtx == nil {
                h.writeError(w, http.StatusUnauthorized, "Authentication required")
                return
        }

        // Convert user ID from string to int
        userID, err := strconv.Atoi(userCtx.UserID)
        if err != nil {
                h.writeError(w, http.StatusBadRequest, fmt.Sprintf("Invalid user ID format: %q", userCtx.UserID))
                return
        }

        // Validate user ID (fixes O4 - reject negative/zero values)
        if userID <= 0 {
                h.writeError(w, http.StatusBadRequest, fmt.Sprintf("Invalid user ID: must be positive, got %d", userID))
                return
        }

        configs, err := h.configRepo.GetByUser(r.Context(), userID)
        if err != nil {
                h.writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to get configs: %v", err))
                return
        }

        h.writeJSON(w, http.StatusOK, map[string]interface{}{
                "configs": configs,
                "count":   len(configs),
        })
}

// GET /api/pipeline/configs/{id}
// Get a specific configuration
func (h *PipelineHandler) GetConfig(w http.ResponseWriter, r *http.Request) {
        if h.configRepo == nil {
                h.writeError(w, http.StatusInternalServerError, "Configuration storage not available")
                return
        }

        configID := r.PathValue("id")
        if configID == "" {
                h.writeError(w, http.StatusBadRequest, "config_id is required")
                return
        }

        config, err := h.configRepo.Get(r.Context(), configID)
        if err != nil {
                h.writeError(w, http.StatusNotFound, "Configuration not found")
                return
        }

        h.writeJSON(w, http.StatusOK, config)
}

// DELETE /api/pipeline/configs/{id}
// Delete a configuration
func (h *PipelineHandler) DeleteConfig(w http.ResponseWriter, r *http.Request) {
        if h.configRepo == nil {
                h.writeError(w, http.StatusInternalServerError, "Configuration storage not available")
                return
        }

        configID := r.PathValue("id")
        if configID == "" {
                h.writeError(w, http.StatusBadRequest, "config_id is required")
                return
        }

        if err := h.configRepo.Delete(r.Context(), configID); err != nil {
                h.writeError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to delete config: %v", err))
                return
        }

        h.writeJSON(w, http.StatusOK, map[string]interface{}{
                "success": true,
                "message": "Configuration deleted successfully",
        })
}

// =============================================================================
// ENHANCED JOB START WITH CONFIGURATION
// =============================================================================

// StartJobWithConfigRequest represents a job start request with config
type StartJobWithConfigRequest struct {
        ConfigID      string                       `json:"config_id,omitempty"`
        FolderPath    string                       `json:"folder_path,omitempty"`
        Recursive     bool                         `json:"recursive"`
        DefaultOptions pipeline.LoadOptions        `json:"default_options"`
        FileOptions   map[string]pipeline.LoadOptions `json:"file_options"`
}

// POST /api/pipeline/start-with-config
// Start a job using saved configuration or inline options
func (h *PipelineHandler) StartJobWithConfig(w http.ResponseWriter, r *http.Request) {
        var req StartJobWithConfigRequest
        if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
                h.writeError(w, http.StatusBadRequest, "Invalid request body")
                return
        }

        // If config ID provided, load it
        var config *pipeline.PipelineConfig
        if req.ConfigID != "" && h.configRepo != nil {
                var err error
                config, err = h.configRepo.Get(r.Context(), req.ConfigID)
                if err != nil {
                        h.writeError(w, http.StatusNotFound, "Configuration not found")
                        return
                }
        }

        // Merge config with request (request overrides config)
        folderPath := req.FolderPath
        if folderPath == "" && config != nil {
                folderPath = config.FolderPath
        }
        if folderPath == "" {
                h.writeError(w, http.StatusBadRequest, "folder_path is required")
                return
        }

        // Security: validate path
        if err := h.validatePath(folderPath); err != nil {
                h.writeError(w, http.StatusForbidden, err.Error())
                return
        }

        recursive := req.Recursive
        if !recursive && config != nil {
                recursive = config.Recursive
        }

        defaultOpts := req.DefaultOptions
        if defaultOpts.TableName == "" && config != nil {
                defaultOpts = config.DefaultOptions
        }

        fileOpts := req.FileOptions
        if len(fileOpts) == 0 && config != nil {
                fileOpts = config.FileOptions
        }

        jobID := uuid.New().String()

        // Start the job with options
        if err := h.processor.StartJobWithOptions(r.Context(), jobID, folderPath, recursive, defaultOpts, fileOpts); err != nil {
                h.writeError(w, http.StatusInternalServerError, err.Error())
                return
        }

        h.writeJSON(w, http.StatusAccepted, map[string]interface{}{
                "job_id":  jobID,
                "message": "Job started successfully",
                "config_used": config != nil,
        })
}

// =============================================================================
// RESPONSE HELPERS
// =============================================================================

func (h *PipelineHandler) writeJSON(w http.ResponseWriter, status int, data interface{}) {
        w.Header().Set("Content-Type", "application/json")
        w.WriteHeader(status)
        json.NewEncoder(w).Encode(data)
}

func (h *PipelineHandler) writeError(w http.ResponseWriter, status int, message string) {
        h.writeJSON(w, status, map[string]string{
                "error":   message,
                "status":  "error",
                "code":    fmt.Sprintf("%d", status),
        })
}
