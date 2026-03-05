package pipeline

import (
        "context"
        "encoding/json"
        "fmt"
        "log/slog"
        "path/filepath"
        "strings"
        "sync"
        "time"

        "github.com/jackc/pgx/v5/pgxpool"
)

// JobStatus represents the current state of a job
type JobStatus string

const (
        JobStatusPending   JobStatus = "pending"
        JobStatusRunning   JobStatus = "running"
        JobStatusCompleted JobStatus = "completed"
        JobStatusFailed    JobStatus = "failed"
        JobStatusCancelled JobStatus = "cancelled"
)

// ScanResult stores the result of a folder scan for workflow enforcement
type ScanResult struct {
        ID           string          `json:"id"`
        FolderPath   string          `json:"folder_path"`
        Recursive    bool            `json:"recursive"`
        Files        []FileDiscovery `json:"files"`
        TotalFiles   int             `json:"total_files"`
        Warnings     []ScanWarning   `json:"warnings,omitempty"`
        CreatedAt    time.Time       `json:"created_at"`
        ExpiresAt    time.Time       `json:"expires_at"`
}

// ScanWarning represents a warning about a scanned file
type ScanWarning struct {
        FilePath   string `json:"file_path"`
        Type       string `json:"type"` // "low_confidence", "encoding_issue", "large_file"
        Message    string `json:"message"`
        Suggestion string `json:"suggestion,omitempty"`
}

// QuickOptions provides simplified configuration for common use cases
// This addresses "Options Overload" - users only need 1-3 fields instead of 12
type QuickOptions struct {
        // Header settings - most common option users change
        HasHeader *bool `json:"has_header,omitempty"` // true/false, null = auto-detect

        // Delimiter (for CSV) - second most common
        Delimiter *string `json:"delimiter,omitempty"` // ",", ";", "\t", null = auto-detect

        // Encoding (rarely needed) - for unusual files
        Encoding *string `json:"encoding,omitempty"` // "UTF-8", "Latin-1", etc.

        // Files to exclude - addresses UX finding about missing exclusion
        ExcludeFiles    []string `json:"exclude_files,omitempty"`    // Exact file names: ["temp.csv"]
        ExcludePatterns []string `json:"exclude_patterns,omitempty"` // Globs: ["*.tmp", "*_old.*"]
}

// ToLoadOptions converts QuickOptions to full LoadOptions with smart defaults
func (q *QuickOptions) ToLoadOptions() LoadOptions {
        opts := DefaultLoadOptions()
        if q != nil {
                if q.HasHeader != nil {
                        opts.HasHeader = q.HasHeader
                }
                if q.Delimiter != nil {
                        opts.Delimiter = *q.Delimiter
                }
                if q.Encoding != nil {
                        opts.Encoding = *q.Encoding
                }
        }
        return opts
}

// StartJobRequest is the UNIFIED entry point for starting jobs
// Merges /start and /start-with-config into single endpoint
type StartJobRequest struct {
        // === INPUT SOURCE (one required) ===
        // Option 1: Scan first, then start (recommended)
        ScanID string `json:"scan_id,omitempty"`

        // Option 2: Use saved configuration
        ConfigID string `json:"config_id,omitempty"`

        // Option 3: Quick start with auto-scan (power users)
        FolderPath string `json:"folder_path,omitempty"`
        Recursive  bool   `json:"recursive,omitempty"`

        // === OPTIONS ===
        // Simple options for most users (replaces 12-field LoadOptions)
        QuickOptions *QuickOptions `json:"quick_options,omitempty"`

        // Advanced: per-file overrides (paths must match scan results exactly)
        FileOptions map[string]LoadOptions `json:"file_options,omitempty"`

        // === PREVIEW MODE ===
        // If true, returns preview without loading data
        DryRun bool `json:"dry_run,omitempty"`

        // === POWER USER OPTIONS ===
        // Skip validation that file_options paths exist in scan
        SkipScanValidation bool `json:"skip_scan_validation,omitempty"`
}

// JobPreview shows what will happen without actually running
// Addresses "No Preview Mode" UX finding
type JobPreview struct {
        ScanID         string        `json:"scan_id"`
        FolderPath     string        `json:"folder_path"`
        TotalFiles     int           `json:"total_files"`
        FilesToProcess []FilePreview `json:"files_to_process"`
        FilesExcluded  []string      `json:"files_excluded,omitempty"`
        Warnings       []string      `json:"warnings,omitempty"`
        EstimatedRows  int           `json:"estimated_rows,omitempty"`
        Ready          bool          `json:"ready"`          // Can proceed without issues?
        Confidence     float64       `json:"confidence"`     // Average detection confidence
}

// FilePreview shows what will happen with a specific file
type FilePreview struct {
        FilePath       string      `json:"file_path"`
        FileName       string      `json:"file_name"`
        TableName      string      `json:"table_name,omitempty"`
        EstimatedRows  int         `json:"estimated_rows,omitempty"`
        Columns        []string    `json:"columns,omitempty"`
        OptionsApplied LoadOptions `json:"options_applied"`
        Confidence     float64     `json:"confidence,omitempty"`
        ConfidenceOK   bool        `json:"confidence_ok"` // Above threshold?
        Warnings       []string    `json:"warnings,omitempty"`
}

// JobManager handles job lifecycle including cancellation
type JobManager struct {
        pool       *pgxpool.Pool
        jobs       map[string]*JobState
        scans      map[string]*ScanResult
        mu         sync.RWMutex
        configRepo *ConfigRepository
        scanStore  *ScanStore
        processor  *PipelineProcessor
}

// JobState tracks running job state for cancellation
type JobState struct {
        JobID       string
        Status      JobStatus
        CancelFunc  context.CancelFunc
        Progress    *JobProgress
        StartedAt   time.Time
        CompletedAt *time.Time
}

// NewJobManager creates a new job manager
func NewJobManager(pool *pgxpool.Pool, configRepo *ConfigRepository, processor *PipelineProcessor) *JobManager {
        return &JobManager{
                pool:       pool,
                jobs:       make(map[string]*JobState),
                scans:      make(map[string]*ScanResult),
                configRepo: configRepo,
                scanStore:  NewScanStore(pool),
                processor:  processor,
        }
}

// StartJob is the UNIFIED entry point
// Returns either JobPreview (dry_run=true) or job started response
func (jm *JobManager) StartJob(ctx context.Context, req StartJobRequest, userID string) (interface{}, error) {
        // === VALIDATE INPUT ===
        sources := 0
        if req.ScanID != "" {
                sources++
        }
        if req.ConfigID != "" {
                sources++
        }
        if req.FolderPath != "" {
                sources++
        }

        if sources == 0 {
                return nil, NewActionableError(
                        "missing_input",
                        "One of scan_id, config_id, or folder_path is required",
                        "First call POST /api/pipeline/scan-folder to scan your files, then use the returned scan_id to start the job.",
                )
        }

        if sources > 1 {
                return nil, NewActionableError(
                        "conflicting_input",
                        "Provide only one of scan_id, config_id, or folder_path",
                        "Use scan_id for the recommended workflow: scan → preview → start.",
                )
        }

        // === GET OR CREATE SCAN ===
        var scan *ScanResult
        var err error

        switch {
        case req.ScanID != "":
                scan, err = jm.scanStore.Get(ctx, req.ScanID)
                if err != nil {
                        return nil, NewActionableError(
                                "scan_not_found",
                                fmt.Sprintf("Scan '%s' not found or expired", req.ScanID),
                                "Scans expire after 1 hour. Run POST /api/pipeline/scan-folder again to get a fresh scan.",
                        )
                }

        case req.FolderPath != "":
                // Perform new scan inline
                scanner := NewFolderScanner(10)
                result, err := scanner.ScanFolderParallel(ctx, req.FolderPath, req.Recursive, 4)
                if err != nil {
                        return nil, NewActionableError(
                                "scan_failed",
                                fmt.Sprintf("Failed to scan folder: %v", err),
                                "Check that the folder path exists and you have read permissions.",
                        )
                }

                scan = &ScanResult{
                        ID:         generateScanID(),
                        FolderPath: req.FolderPath,
                        Recursive:  req.Recursive,
                        Files:      result.Files,
                        TotalFiles: result.SupportedFiles,
                        Warnings:   extractWarnings(result.Files),
                        CreatedAt:  time.Now(),
                        ExpiresAt:  time.Now().Add(1 * time.Hour),
                }

                // Store for reference
                if err := jm.scanStore.Save(ctx, scan); err != nil {
                        // Non-fatal: log and continue
                        slog.Warn("failed to store scan", "error", err, "scan_id", scan.ID)
                }

        case req.ConfigID != "":
                config, err := jm.configRepo.Get(ctx, req.ConfigID)
                if err != nil {
                        return nil, NewActionableError(
                                "config_not_found",
                                fmt.Sprintf("Configuration '%s' not found", req.ConfigID),
                                "List your configurations with GET /api/pipeline/configs to find the correct ID.",
                        )
                }

                // Build scan from config
                scan = &ScanResult{
                        ID:         generateScanID(),
                        FolderPath: config.FolderPath,
                        Recursive:  config.Recursive,
                        Files:      config.FileDiscoveries,
                        TotalFiles: len(config.FileDiscoveries),
                        CreatedAt:  time.Now(),
                        ExpiresAt:  time.Now().Add(1 * time.Hour),
                }

                // Merge config options into request
                if req.QuickOptions == nil && len(req.FileOptions) == 0 {
                        req.QuickOptions = &QuickOptions{}
                        if config.DefaultOptions.HasHeader != nil {
                                req.QuickOptions.HasHeader = config.DefaultOptions.HasHeader
                        }
                        if config.DefaultOptions.Delimiter != "" {
                                req.QuickOptions.Delimiter = &config.DefaultOptions.Delimiter
                        }
                }
                if len(req.FileOptions) == 0 && len(config.FileOptions) > 0 {
                        req.FileOptions = config.FileOptions
                }
        }

        // === APPLY EXCLUSIONS ===
        filesToProcess, filesExcluded := jm.applyExclusions(scan.Files, req.QuickOptions)

        // === BUILD FILE OPTIONS ===
        fileOptions, validationErrors := jm.buildAndValidateFileOptions(filesToProcess, req.QuickOptions, req.FileOptions, scan)
        if len(validationErrors) > 0 && !req.SkipScanValidation {
                return nil, NewActionableError(
                        "invalid_file_options",
                        "File options contain invalid paths",
                        strings.Join(validationErrors, "; "),
                )
        }

        // === CHECK CONFIDENCE WARNINGS ===
        confidenceWarnings := checkConfidenceWarnings(filesToProcess)
        if len(confidenceWarnings) > 0 && !req.SkipScanValidation {
                warningMsgs := make([]string, len(confidenceWarnings))
                for i, w := range confidenceWarnings {
                        warningMsgs[i] = fmt.Sprintf("%s (%.0f%% confidence)", w.FilePath, w.Confidence*100)
                }
                return nil, &ConfidenceWarningError{
                        Warnings: confidenceWarnings,
                        Message: fmt.Sprintf("Some files have low detection confidence: %s. "+
                                "Review scan results or set skip_scan_validation=true to proceed anyway.",
                                strings.Join(warningMsgs, ", ")),
                }
        }

        // === DRY RUN: RETURN PREVIEW ===
        if req.DryRun {
                return jm.buildPreview(scan, filesToProcess, filesExcluded, fileOptions), nil
        }

        // === START ACTUAL JOB ===
        jobID := generateJobID()
        jobCtx, cancel := context.WithCancel(context.Background())

        progress := &JobProgress{
                JobID:      jobID,
                Status:     string(JobStatusPending),
                FolderPath: scan.FolderPath,
                TotalFiles: len(filesToProcess),
                Files:      make(map[string]*FileResult),
                StartedAt:  time.Now(),
        }

        jm.mu.Lock()
        jm.jobs[jobID] = &JobState{
                JobID:      jobID,
                Status:     JobStatusPending,
                CancelFunc: cancel,
                Progress:   progress,
                StartedAt:  time.Now(),
        }
        jm.mu.Unlock()

        // Run job in background
        go jm.runJob(jobCtx, jobID, scan.FolderPath, scan.Recursive, fileOptions)

        return map[string]interface{}{
                "job_id":     jobID,
                "scan_id":    scan.ID,
                "status":     "started",
                "total_files": len(filesToProcess),
                "message":    "Job started. Use GET /api/pipeline/jobs/{job_id} to monitor progress.",
                "monitor":    fmt.Sprintf("/api/pipeline/jobs/%s/stream", jobID),
        }, nil
}

// CancelJob cancels a running job
// Addresses "No Job Cancellation" UX finding
func (jm *JobManager) CancelJob(ctx context.Context, jobID string) error {
        jm.mu.Lock()
        defer jm.mu.Unlock()

        job, exists := jm.jobs[jobID]
        if !exists {
                return NewActionableError(
                        "job_not_found",
                        fmt.Sprintf("Job '%s' not found", jobID),
                        "List active jobs with GET /api/pipeline/jobs to find the correct job ID.",
                )
        }

        if job.Status != JobStatusRunning && job.Status != JobStatusPending {
                return NewActionableError(
                        "job_not_cancellable",
                        fmt.Sprintf("Job status is '%s', cannot cancel", job.Status),
                        "Only pending or running jobs can be cancelled.",
                )
        }

        // Cancel the context
        if job.CancelFunc != nil {
                job.CancelFunc()
        }

        // Update status
        job.Status = JobStatusCancelled
        now := time.Now()
        job.CompletedAt = &now
        job.Progress.Status = string(JobStatusCancelled)

        return nil
}

// GetJobProgress returns full job progress
func (jm *JobManager) GetJobProgress(ctx context.Context, jobID string) (*JobProgress, error) {
        jm.mu.RLock()
        defer jm.mu.RUnlock()

        job, exists := jm.jobs[jobID]
        if !exists {
                return nil, NewActionableError(
                        "job_not_found",
                        fmt.Sprintf("Job '%s' not found", jobID),
                        "Jobs are kept in memory. If the server restarted, the job is no longer available.",
                )
        }

        // Return a copy
        progress := *job.Progress
        return &progress, nil
}

// GetJobSummary returns lightweight summary for polling
// Addresses "SSE Connection Fragility" UX finding
func (jm *JobManager) GetJobSummary(ctx context.Context, jobID string) (map[string]interface{}, error) {
        progress, err := jm.GetJobProgress(ctx, jobID)
        if err != nil {
                return nil, err
        }

        return map[string]interface{}{
                "job_id":           progress.JobID,
                "status":           progress.Status,
                "total_files":      progress.TotalFiles,
                "processed_files":  progress.ProcessedFiles,
                "successful_files": progress.SuccessfulFiles,
                "failed_files":     progress.FailedFiles,
                "total_rows":       progress.TotalRows,
                "progress_percent": calculateProgressPercent(progress),
                "error":            progress.Error,
        }, nil
}

// ListJobs lists all jobs (active and recent completed)
func (jm *JobManager) ListJobs() []map[string]interface{} {
        jm.mu.RLock()
        defer jm.mu.RUnlock()

        var jobs []map[string]interface{}
        for id, job := range jm.jobs {
                jobs = append(jobs, map[string]interface{}{
                        "job_id":     id,
                        "status":     string(job.Status),
                        "folder_path": job.Progress.FolderPath,
                        "started_at": job.StartedAt,
                })
        }
        return jobs
}

// === HELPER METHODS ===

func (jm *JobManager) applyExclusions(files []FileDiscovery, quick *QuickOptions) ([]FileDiscovery, []string) {
        if quick == nil || (len(quick.ExcludeFiles) == 0 && len(quick.ExcludePatterns) == 0) {
                return files, nil
        }

        var result []FileDiscovery
        var excluded []string

        for _, f := range files {
                shouldExclude := false

                // Check exact file exclusions
                for _, ef := range quick.ExcludeFiles {
                        if filepath.Base(f.FilePath) == ef || f.FilePath == ef {
                                shouldExclude = true
                                excluded = append(excluded, f.FilePath)
                                break
                        }
                }

                // Check pattern exclusions
                if !shouldExclude {
                        for _, pattern := range quick.ExcludePatterns {
                                matched, _ := filepath.Match(pattern, filepath.Base(f.FilePath))
                                if matched {
                                        shouldExclude = true
                                        excluded = append(excluded, f.FilePath)
                                        break
                                }
                        }
                }

                if !shouldExclude {
                        result = append(result, f)
                }
        }

        return result, excluded
}

func (jm *JobManager) buildAndValidateFileOptions(files []FileDiscovery, quick *QuickOptions, explicit map[string]LoadOptions, scan *ScanResult) (map[string]LoadOptions, []string) {
        result := make(map[string]LoadOptions)
        var validationErrors []string

        // Build path set from scan for validation
        scanPaths := make(map[string]bool)
        for _, f := range scan.Files {
                scanPaths[filepath.Clean(f.FilePath)] = true
        }

        // Default options from quick options
        defaultOpts := quick.ToLoadOptions()

        // Build options for each file
        for _, f := range files {
                opts := defaultOpts

                // Merge with explicit options
                if explicitOpts, ok := explicit[f.FilePath]; ok {
                        opts = mergeOptions(defaultOpts, explicitOpts)
                }

                // Set detected values if not overridden
                if opts.Delimiter == "" && f.DetectedDelimiter != "" && f.DetectedDelimiter != "N/A" {
                        opts.Delimiter = f.DetectedDelimiter
                }
                if opts.Encoding == "" && f.Encoding != "" && f.Encoding != "N/A" {
                        opts.Encoding = f.Encoding
                }
                if opts.HasHeader == nil && f.DetectedHeader != nil {
                        opts.HasHeader = &f.DetectedHeader.HasHeader
                }

                result[f.FilePath] = opts
        }

        // Validate explicit paths
        for path := range explicit {
                normalized := filepath.Clean(path)
                if !scanPaths[normalized] {
                        suggestions := findSimilarPaths(normalized, scanPaths)
                        errMsg := fmt.Sprintf("path '%s' not found in scan results", path)
                        if len(suggestions) > 0 {
                                errMsg += fmt.Sprintf(". Did you mean: %s?", strings.Join(suggestions, ", "))
                        }
                        validationErrors = append(validationErrors, errMsg)
                }
        }

        return result, validationErrors
}

func (jm *JobManager) buildPreview(scan *ScanResult, files []FileDiscovery, excluded []string, fileOptions map[string]LoadOptions) *JobPreview {
        preview := &JobPreview{
                ScanID:        scan.ID,
                FolderPath:    scan.FolderPath,
                TotalFiles:    len(files),
                FilesExcluded: excluded,
                Ready:         true,
        }

        var totalConfidence float64
        var confidenceCount int

        for _, f := range files {
                opts := fileOptions[f.FilePath]

                filePreview := FilePreview{
                        FilePath:       f.FilePath,
                        FileName:       filepath.Base(f.FilePath),
                        TableName:      generateTableName(f.FilePath),
                        Columns:        extractColumns(f, opts),
                        OptionsApplied: opts,
                        ConfidenceOK:   true,
                        Warnings:       []string{},
                }

                if f.DetectedHeader != nil {
                        filePreview.Confidence = f.DetectedHeader.Confidence
                        totalConfidence += f.DetectedHeader.Confidence
                        confidenceCount++

                        if f.DetectedHeader.Confidence < 0.7 {
                                filePreview.ConfidenceOK = false
                                filePreview.Warnings = append(filePreview.Warnings,
                                        fmt.Sprintf("Low confidence (%.0f%%). Detected headers: %v",
                                                f.DetectedHeader.Confidence*100, f.DetectedHeader.Preview[0]))
                                preview.Ready = false
                        }
                }

                preview.FilesToProcess = append(preview.FilesToProcess, filePreview)
        }

        if confidenceCount > 0 {
                preview.Confidence = totalConfidence / float64(confidenceCount)
        }

        // Add overall warnings
        for _, w := range scan.Warnings {
                preview.Warnings = append(preview.Warnings, w.Message)
        }

        return preview
}

func (jm *JobManager) runJob(ctx context.Context, jobID, folderPath string, recursive bool, fileOptions map[string]LoadOptions) {
        jm.mu.Lock()
        job := jm.jobs[jobID]
        if job == nil {
                return
        }
        job.Status = JobStatusRunning
        job.Progress.Status = string(JobStatusRunning)
        jm.mu.Unlock()

        // Delegate to processor with options
        defaultOpts := DefaultLoadOptions()
        if len(fileOptions) > 0 {
                for _, opts := range fileOptions {
                        defaultOpts = opts
                        break
                }
        }

        if err := jm.processor.StartJobWithOptions(ctx, jobID, folderPath, recursive, defaultOpts, fileOptions); err != nil {
                jm.mu.Lock()
                job.Status = JobStatusFailed
                job.Progress.Status = string(JobStatusFailed)
                job.Progress.Error = err.Error()
                jm.mu.Unlock()
        }
}

// === ERROR TYPES ===

// ActionableError provides user-friendly error messages with suggestions
type ActionableError struct {
        Code       string `json:"code"`
        Message    string `json:"message"`
        Suggestion string `json:"suggestion"`
}

func NewActionableError(code, message, suggestion string) *ActionableError {
        return &ActionableError{
                Code:       code,
                Message:    message,
                Suggestion: suggestion,
        }
}

func (e *ActionableError) Error() string {
        return e.Message
}

// ConfidenceWarningError is returned when files have low confidence
type ConfidenceWarningError struct {
        Warnings []ConfidenceWarning
        Message  string
}

func (e *ConfidenceWarningError) Error() string {
        return e.Message
}

type ConfidenceWarning struct {
        FilePath   string  `json:"file_path"`
        Confidence float64 `json:"confidence"`
        Issue      string  `json:"issue"`
}

// === SCAN STORE ===

type ScanStore struct {
        pool *pgxpool.Pool
}

func NewScanStore(pool *pgxpool.Pool) *ScanStore {
        return &ScanStore{pool: pool}
}

func (s *ScanStore) CreateTable(ctx context.Context) error {
        query := `
        CREATE TABLE IF NOT EXISTS pipeline_scans (
                id VARCHAR(36) PRIMARY KEY,
                folder_path TEXT NOT NULL,
                recursive BOOLEAN DEFAULT false,
                files JSONB NOT NULL DEFAULT '[]',
                total_files INTEGER DEFAULT 0,
                warnings JSONB DEFAULT '[]',
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                expires_at TIMESTAMP WITH TIME ZONE NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_scans_folder ON pipeline_scans(folder_path);
        CREATE INDEX IF NOT EXISTS idx_scans_expires ON pipeline_scans(expires_at);
        `
        _, err := s.pool.Exec(ctx, query)
        return err
}

func (s *ScanStore) Save(ctx context.Context, scan *ScanResult) error {
        filesJSON, _ := json.Marshal(scan.Files)
        warningsJSON, _ := json.Marshal(scan.Warnings)

        query := `
        INSERT INTO pipeline_scans (id, folder_path, recursive, files, total_files, warnings, created_at, expires_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (id) DO UPDATE SET
                files = EXCLUDED.files,
                warnings = EXCLUDED.warnings,
                expires_at = EXCLUDED.expires_at
        `

        _, err := s.pool.Exec(ctx, query,
                scan.ID, scan.FolderPath, scan.Recursive, filesJSON, scan.TotalFiles,
                warningsJSON, scan.CreatedAt, scan.ExpiresAt,
        )

        return err
}

func (s *ScanStore) Get(ctx context.Context, id string) (*ScanResult, error) {
        query := `
        SELECT id, folder_path, recursive, files, total_files, warnings, created_at, expires_at
        FROM pipeline_scans WHERE id = $1 AND expires_at > NOW()
        `

        scan := &ScanResult{}
        var filesJSON, warningsJSON []byte

        err := s.pool.QueryRow(ctx, query, id).Scan(
                &scan.ID, &scan.FolderPath, &scan.Recursive, &filesJSON,
                &scan.TotalFiles, &warningsJSON, &scan.CreatedAt, &scan.ExpiresAt,
        )

        if err != nil {
                return nil, err
        }

        json.Unmarshal(filesJSON, &scan.Files)
        json.Unmarshal(warningsJSON, &scan.Warnings)

        return scan, nil
}

// === UTILITY FUNCTIONS ===

func generateScanID() string {
        return fmt.Sprintf("scan_%d", time.Now().UnixNano())
}

func generateJobID() string {
        return fmt.Sprintf("job_%d", time.Now().UnixNano())
}

func extractWarnings(files []FileDiscovery) []ScanWarning {
        var warnings []ScanWarning
        for _, f := range files {
                if f.DetectedHeader != nil && f.DetectedHeader.Confidence < 0.7 {
                        warnings = append(warnings, ScanWarning{
                                FilePath:   f.FilePath,
                                Type:       "low_confidence",
                                Message:    fmt.Sprintf("Header detection confidence is %.0f%%", f.DetectedHeader.Confidence*100),
                                Suggestion: "Review detected headers or use file_options to override",
                        })
                }
                if f.Error != "" {
                        warnings = append(warnings, ScanWarning{
                                FilePath: f.FilePath,
                                Type:     "detection_error",
                                Message:  f.Error,
                        })
                }
        }
        return warnings
}

func checkConfidenceWarnings(files []FileDiscovery) []ConfidenceWarning {
        var warnings []ConfidenceWarning
        for _, f := range files {
                if f.DetectedHeader != nil && f.DetectedHeader.Confidence < 0.7 {
                        warnings = append(warnings, ConfidenceWarning{
                                FilePath:   f.FilePath,
                                Confidence: f.DetectedHeader.Confidence,
                                Issue:      "low_confidence",
                        })
                }
        }
        return warnings
}

func findSimilarPaths(target string, paths map[string]bool) []string {
        var similar []string
        targetBase := filepath.Base(target)

        for path := range paths {
                pathBase := filepath.Base(path)
                if strings.Contains(pathBase, targetBase) || strings.Contains(targetBase, pathBase) {
                        similar = append(similar, path)
                }
        }

        if len(similar) > 3 {
                similar = similar[:3]
        }

        return similar
}

func mergeOptions(base, override LoadOptions) LoadOptions {
        result := base

        if override.HasHeader != nil {
                result.HasHeader = override.HasHeader
        }
        if override.HeaderRow > 0 {
                result.HeaderRow = override.HeaderRow
        }
        if len(override.CustomHeaders) > 0 {
                result.CustomHeaders = override.CustomHeaders
        }
        if override.SkipRows > 0 {
                result.SkipRows = override.SkipRows
        }
        if override.Delimiter != "" {
                result.Delimiter = override.Delimiter
        }
        if override.Encoding != "" {
                result.Encoding = override.Encoding
        }
        if override.TableName != "" {
                result.TableName = override.TableName
        }
        if override.RemoveEmpty != nil {
                result.RemoveEmpty = override.RemoveEmpty
        }
        if override.RemoveDupes != nil {
                result.RemoveDupes = override.RemoveDupes
        }

        return result
}

func extractColumns(f FileDiscovery, opts LoadOptions) []string {
        if len(opts.CustomHeaders) > 0 {
                return opts.CustomHeaders
        }
        if f.DetectedHeader != nil && len(f.DetectedHeader.Preview) > 0 {
                return f.DetectedHeader.Preview[0]
        }
        return []string{}
}

func calculateProgressPercent(progress *JobProgress) int {
        if progress.TotalFiles == 0 {
                return 0
        }
        return (progress.ProcessedFiles * 100) / progress.TotalFiles
}
