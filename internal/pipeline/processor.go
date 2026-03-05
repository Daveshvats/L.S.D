package pipeline

import (
        "context"
        "fmt"
        "io"
        "io/fs"
        "log/slog"
        "os"
        "path/filepath"
        "strings"
        "sync"
        "time"

        "highperf-api/internal/utils"

        "github.com/jackc/pgx/v5/pgxpool"
)

type FileStatus string

const (
        StatusPending    FileStatus = "pending"
        StatusProcessing FileStatus = "processing"
        StatusCompleted  FileStatus = "completed"
        StatusFailed     FileStatus = "failed"
)

type FileResult struct {
        FilePath     string     `json:"file_path"`
        TableName    string     `json:"table_name,omitempty"`
        SheetName    string     `json:"sheet_name,omitempty"`
        Status       FileStatus `json:"status"`
        RowsInserted int        `json:"rows_inserted"`
        Error        string     `json:"error,omitempty"`
        StartedAt    time.Time  `json:"started_at"`
        CompletedAt  *time.Time `json:"completed_at,omitempty"`
}

type JobProgress struct {
        JobID           string                 `json:"job_id"`
        Status          string                 `json:"status"`
        FolderPath      string                 `json:"folder_path"`
        TotalFiles      int                    `json:"total_files"`
        ProcessedFiles  int                    `json:"processed_files"`
        SuccessfulFiles int                    `json:"successful_files"`
        FailedFiles     int                    `json:"failed_files"`
        TotalRows       int                    `json:"total_rows"`
        Files           map[string]*FileResult `json:"files"`
        StartedAt       time.Time              `json:"started_at"`
        CompletedAt     *time.Time             `json:"completed_at,omitempty"`
        Error           string                 `json:"error,omitempty"`
        LogPath         string                 `json:"log_path,omitempty"`
}

// CDCTriggerFunc is a callback function that triggers CDC sync for a table
type CDCTriggerFunc func(tableName string) error

type PipelineProcessor struct {
        pool       *pgxpool.Pool
        jobs       map[string]*JobProgress
        mu         sync.RWMutex
        errorDir   string
        chunkSize  int
        cdcTrigger CDCTriggerFunc // NEW: CDC callback
}

func NewPipelineProcessor(pool *pgxpool.Pool, errorDir string) *PipelineProcessor {
        return &PipelineProcessor{
                pool:      pool,
                jobs:      make(map[string]*JobProgress),
                errorDir:  errorDir,
                chunkSize: 10000,
        }
}

// SetCDCTrigger sets the CDC trigger callback function
func (p *PipelineProcessor) SetCDCTrigger(trigger CDCTriggerFunc) {
        p.cdcTrigger = trigger
}

func (p *PipelineProcessor) StartJob(ctx context.Context, jobID, folderPath string, recursive bool) error {
        p.mu.Lock()
        if _, exists := p.jobs[jobID]; exists {
                p.mu.Unlock()
                return fmt.Errorf("job %s already exists", jobID)
        }

        job := &JobProgress{
                JobID:      jobID,
                Status:     "running",
                FolderPath: folderPath,
                Files:      make(map[string]*FileResult),
                StartedAt:  time.Now(),
        }
        p.jobs[jobID] = job
        p.mu.Unlock()

        // Run processing in background with new context (not cancelled by HTTP request)
        go p.processFolder(context.Background(), jobID, folderPath, recursive)

        return nil
}

// StartJobWithOptions starts a job with custom load options per file
func (p *PipelineProcessor) StartJobWithOptions(ctx context.Context, jobID, folderPath string, recursive bool, 
        defaultOpts LoadOptions, fileOpts map[string]LoadOptions) error {
        p.mu.Lock()
        if _, exists := p.jobs[jobID]; exists {
                p.mu.Unlock()
                return fmt.Errorf("job %s already exists", jobID)
        }

        job := &JobProgress{
                JobID:      jobID,
                Status:     "running",
                FolderPath: folderPath,
                Files:      make(map[string]*FileResult),
                StartedAt:  time.Now(),
        }
        p.jobs[jobID] = job
        p.mu.Unlock()

        // Run processing in background with options
        go p.processFolderWithOptions(context.Background(), jobID, folderPath, recursive, defaultOpts, fileOpts)

        return nil
}

func (p *PipelineProcessor) GetJobProgress(jobID string) (*JobProgress, error) {
        p.mu.RLock()
        defer p.mu.RUnlock()

        job, exists := p.jobs[jobID]
        if !exists {
                return nil, fmt.Errorf("job %s not found", jobID)
        }

        // Return a copy to avoid race conditions
        jobCopy := *job
        jobCopy.Files = make(map[string]*FileResult)
        for k, v := range job.Files {
                fileCopy := *v
                jobCopy.Files[k] = &fileCopy
        }

        return &jobCopy, nil
}

func (p *PipelineProcessor) ListJobs() map[string]*JobProgress {
        p.mu.RLock()
        defer p.mu.RUnlock()

        jobs := make(map[string]*JobProgress)
        for k, v := range p.jobs {
                jobCopy := *v
                jobs[k] = &jobCopy
        }
        return jobs
}

func (p *PipelineProcessor) processFolder(ctx context.Context, jobID, folderPath string, recursive bool) {
        slogLogger := slog.With("operation", "process_folder", "job_id", jobID, "path", folderPath)
        slogLogger.Debug("starting folder processing")

        // Initialize logger
        logger, err := utils.NewLogger(jobID)
        if err != nil {
                slogLogger.Error("failed to create logger", "error", err)
                p.updateJobError(jobID, fmt.Sprintf("Failed to create logger: %v", err))
                return
        }
        defer logger.Close()

        // Update job with log path
        p.mu.Lock()
        job := p.jobs[jobID]
        job.LogPath = logger.GetLogPath()
        p.mu.Unlock()

        logger.Info("=== JOB STARTED ===")
        logger.Info("Job ID: %s", jobID)
        logger.Info("Folder Path: %s", folderPath)
        logger.Info("Recursive: %v", recursive)

        // Create error folder if not exists
        logger.Info("Creating error directory: %s", p.errorDir)
        if err := os.MkdirAll(p.errorDir, 0755); err != nil {
                logger.Error("Failed to create error folder: %v", err)
                p.updateJobError(jobID, fmt.Sprintf("Failed to create error folder: %v", err))
                return
        }

        // Verify folder exists
        logger.Info("Verifying folder exists: %s", folderPath)
        if _, err := os.Stat(folderPath); os.IsNotExist(err) {
                logger.Error("Folder does not exist: %s", folderPath)
                p.updateJobError(jobID, fmt.Sprintf("Folder does not exist: %s", folderPath))
                return
        }

        // Discover files
        logger.Info("Discovering files...")
        files, err := p.discoverFiles(folderPath, recursive, logger)
        if err != nil {
                logger.Error("Failed to discover files: %v", err)
                p.updateJobError(jobID, fmt.Sprintf("Failed to discover files: %v", err))
                return
        }

        logger.Info("Found %d files to process", len(files))

        if len(files) == 0 {
                logger.Warn("No supported files found in folder")
                p.updateJobError(jobID, "No supported files found in folder")
                return
        }

        p.mu.Lock()
        job.TotalFiles = len(files)
        for _, file := range files {
                logger.Debug("Registered file: %s", file)
                job.Files[file] = &FileResult{
                        FilePath:  file,
                        Status:    StatusPending,
                        StartedAt: time.Now(),
                }
        }
        p.mu.Unlock()

        logger.Info("Starting file processing...")

        // Process each file
        for idx, file := range files {
                logger.Info("[%d/%d] Processing: %s", idx+1, len(files), filepath.Base(file))

                select {
                case <-ctx.Done():
                        logger.Warn("Job cancelled by context")
                        p.updateJobError(jobID, "Job cancelled")
                        return
                default:
                        p.processFile(ctx, jobID, file, logger)
                }
        }

        // Mark job as completed
        now := time.Now()
        p.mu.Lock()
        job.Status = "completed"
        job.CompletedAt = &now
        p.mu.Unlock()

        logger.Info("=== JOB COMPLETED ===")
        logger.Info("Total Files: %d", job.TotalFiles)
        logger.Info("Successful: %d", job.SuccessfulFiles)
        logger.Info("Failed: %d", job.FailedFiles)
        logger.Info("Total Rows: %d", job.TotalRows)
        logger.Info("Log saved to: %s", logger.GetLogPath())
}

// processFolderWithOptions processes a folder with custom load options per file
func (p *PipelineProcessor) processFolderWithOptions(ctx context.Context, jobID, folderPath string, recursive bool,
        defaultOpts LoadOptions, fileOpts map[string]LoadOptions) {
        slogLogger := slog.With("operation", "process_folder_with_options", "job_id", jobID, "path", folderPath)
        slogLogger.Debug("starting folder processing with options")

        // Initialize logger
        logger, err := utils.NewLogger(jobID)
        if err != nil {
                slogLogger.Error("failed to create logger", "error", err)
                p.updateJobError(jobID, fmt.Sprintf("Failed to create logger: %v", err))
                return
        }
        defer logger.Close()

        // Update job with log path
        p.mu.Lock()
        job := p.jobs[jobID]
        job.LogPath = logger.GetLogPath()
        p.mu.Unlock()

        logger.Info("=== JOB STARTED WITH OPTIONS ===")
        logger.Info("Job ID: %s", jobID)
        logger.Info("Folder Path: %s", folderPath)
        logger.Info("Recursive: %v", recursive)

        // Create error folder if not exists
        if err := os.MkdirAll(p.errorDir, 0755); err != nil {
                logger.Error("Failed to create error folder: %v", err)
                p.updateJobError(jobID, fmt.Sprintf("Failed to create error folder: %v", err))
                return
        }

        // Verify folder exists
        if _, err := os.Stat(folderPath); os.IsNotExist(err) {
                logger.Error("Folder does not exist: %s", folderPath)
                p.updateJobError(jobID, fmt.Sprintf("Folder does not exist: %s", folderPath))
                return
        }

        // Discover files
        files, err := p.discoverFiles(folderPath, recursive, logger)
        if err != nil {
                logger.Error("Failed to discover files: %v", err)
                p.updateJobError(jobID, fmt.Sprintf("Failed to discover files: %v", err))
                return
        }

        logger.Info("Found %d files to process", len(files))

        if len(files) == 0 {
                logger.Warn("No supported files found in folder")
                p.updateJobError(jobID, "No supported files found in folder")
                return
        }

        p.mu.Lock()
        job.TotalFiles = len(files)
        for _, file := range files {
                job.Files[file] = &FileResult{
                        FilePath:  file,
                        Status:    StatusPending,
                        StartedAt: time.Now(),
                }
        }
        p.mu.Unlock()

        logger.Info("Starting file processing with custom options...")

        // Process each file with its specific options
        for idx, file := range files {
                logger.Info("[%d/%d] Processing: %s", idx+1, len(files), filepath.Base(file))

                select {
                case <-ctx.Done():
                        logger.Warn("Job cancelled by context")
                        p.updateJobError(jobID, "Job cancelled")
                        return
                default:
                        // Get file-specific options or use default
                        opts := defaultOpts
                        if fileOpts != nil {
                                if specificOpts, ok := fileOpts[file]; ok {
                                        opts = specificOpts
                                        logger.Info("Using custom options for %s", filepath.Base(file))
                                }
                        }
                        p.processFileWithOptions(ctx, jobID, file, opts, logger)
                }
        }

        // Mark job as completed
        now := time.Now()
        p.mu.Lock()
        job.Status = "completed"
        job.CompletedAt = &now
        p.mu.Unlock()

        logger.Info("=== JOB COMPLETED ===")
        logger.Info("Total Files: %d", job.TotalFiles)
        logger.Info("Successful: %d", job.SuccessfulFiles)
        logger.Info("Failed: %d", job.FailedFiles)
        logger.Info("Total Rows: %d", job.TotalRows)
        logger.Info("Log saved to: %s", logger.GetLogPath())
}

func (p *PipelineProcessor) discoverFiles(folderPath string, recursive bool, logger *utils.Logger) ([]string, error) {
        // Use shared SupportedExtensions map (fixes R2 - single source of truth)
        var files []string

        logger.Debug("Walking directory: %s (recursive: %v)", folderPath, recursive)

        // Use WalkDir instead of Walk (fixes P1 - more efficient)
        err := filepath.WalkDir(folderPath, func(path string, d fs.DirEntry, err error) error {
                if err != nil {
                        logger.Warn("Error accessing path %s: %v", path, err)
                        return err
                }

                // Skip subdirectories if not recursive
                if !recursive && d.IsDir() && path != folderPath {
                        logger.Debug("Skipping subdirectory (not recursive): %s", path)
                        return fs.SkipDir
                }

                // Check if file has supported extension
                if !d.IsDir() {
                        ext := strings.ToLower(filepath.Ext(path))
                        if SupportedExtensions[ext] {
                                logger.Debug("Found supported file: %s (ext: %s)", path, ext)
                                files = append(files, path)
                        } else {
                                logger.Debug("Skipping unsupported file: %s (ext: %s)", path, ext)
                        }
                }

                return nil
        })

        return files, err
}

func (p *PipelineProcessor) processFile(ctx context.Context, jobID, filePath string, logger *utils.Logger) {
        fileName := filepath.Base(filePath)

        p.updateFileStatus(jobID, filePath, StatusProcessing, "", "", 0)
        logger.Info("Started processing: %s", fileName)

        fileCtx := context.Background()

        loader := NewFileLoader(p.pool, p.chunkSize)
        result, err := loader.LoadAndInsert(fileCtx, filePath)

        if err != nil {
                logger.Error("Failed to process %s: %v", fileName, err)
                p.updateFileStatus(jobID, filePath, StatusFailed, "", err.Error(), 0)

                if moveErr := p.moveToErrorFolder(filePath, logger); moveErr != nil {
                        logger.Error("Failed to move error file %s: %v", fileName, moveErr)
                }

                p.mu.Lock()
                job := p.jobs[jobID]
                job.FailedFiles++
                p.mu.Unlock()
        } else {
                // Collect table names for CDC trigger
                var tableNames []string
                var totalRows int

                // Handle both single result and multi-sheet results
                switch v := result.(type) {
                case *LoadResult:
                        // Single file (CSV, JSON, etc.)
                        logger.Info("Successfully processed %s -> table: %s (%d rows)",
                                fileName, v.TableName, v.RowsInserted)

                        p.updateFileStatus(jobID, filePath, StatusCompleted, v.TableName, "", v.RowsInserted)
                        totalRows = v.RowsInserted
                        tableNames = append(tableNames, v.TableName)

                case []LoadResult:
                        // Multi-sheet Excel
                        tableNamesList := []string{}
                        for _, sheetResult := range v {
                                totalRows += sheetResult.RowsInserted
                                tableNames = append(tableNames, sheetResult.TableName)
                                tableNamesList = append(tableNamesList, sheetResult.TableName)
                                logger.Info("Sheet '%s' -> table: %s (%d rows)",
                                        sheetResult.SheetName, sheetResult.TableName, sheetResult.RowsInserted)
                        }

                        p.updateFileStatus(jobID, filePath, StatusCompleted,
                                strings.Join(tableNamesList, ", "), "", totalRows)
                }

                p.mu.Lock()
                job := p.jobs[jobID]
                job.TotalRows += totalRows
                job.SuccessfulFiles++
                p.mu.Unlock()

                // ✨ TRIGGER CDC SYNC FOR NEW TABLES
                if p.cdcTrigger != nil && len(tableNames) > 0 {
                        for _, tableName := range tableNames {
                                logger.Info("🔄 Triggering CDC sync for table: %s", tableName)
                                if err := p.cdcTrigger(tableName); err != nil {
                                        logger.Warn("⚠️  CDC sync failed for %s: %v", tableName, err)
                                } else {
                                        logger.Info("✅ CDC sync completed for table: %s", tableName)
                                }
                        }
                }
        }

        p.mu.Lock()
        job := p.jobs[jobID]
        job.ProcessedFiles++
        p.mu.Unlock()

        logger.Info("Progress: %d/%d files processed", job.ProcessedFiles, job.TotalFiles)
}

// processFileWithOptions processes a single file with custom load options
func (p *PipelineProcessor) processFileWithOptions(ctx context.Context, jobID, filePath string, opts LoadOptions, logger *utils.Logger) {
        fileName := filepath.Base(filePath)

        p.updateFileStatus(jobID, filePath, StatusProcessing, "", "", 0)
        logger.Info("Started processing: %s (with custom options)", fileName)

        fileCtx := context.Background()

        loader := NewFileLoader(p.pool, p.chunkSize)
        result, err := loader.LoadAndInsertWithOptions(fileCtx, filePath, opts)

        if err != nil {
                logger.Error("Failed to process %s: %v", fileName, err)
                p.updateFileStatus(jobID, filePath, StatusFailed, "", err.Error(), 0)

                if moveErr := p.moveToErrorFolder(filePath, logger); moveErr != nil {
                        logger.Error("Failed to move error file %s: %v", fileName, moveErr)
                }

                p.mu.Lock()
                job := p.jobs[jobID]
                job.FailedFiles++
                p.mu.Unlock()
        } else {
                // Collect table names for CDC trigger
                var tableNames []string
                var totalRows int

                // Handle both single result and multi-sheet results
                switch v := result.(type) {
                case *LoadResult:
                        logger.Info("Successfully processed %s -> table: %s (%d rows)",
                                fileName, v.TableName, v.RowsInserted)

                        p.updateFileStatus(jobID, filePath, StatusCompleted, v.TableName, "", v.RowsInserted)
                        totalRows = v.RowsInserted
                        tableNames = append(tableNames, v.TableName)

                case []LoadResult:
                        // Multi-sheet Excel
                        tableNamesList := []string{}
                        for _, sheetResult := range v {
                                totalRows += sheetResult.RowsInserted
                                tableNames = append(tableNames, sheetResult.TableName)
                                tableNamesList = append(tableNamesList, sheetResult.TableName)
                                logger.Info("Sheet '%s' -> table: %s (%d rows)",
                                        sheetResult.SheetName, sheetResult.TableName, sheetResult.RowsInserted)
                        }

                        p.updateFileStatus(jobID, filePath, StatusCompleted,
                                strings.Join(tableNamesList, ", "), "", totalRows)
                }

                p.mu.Lock()
                job := p.jobs[jobID]
                job.TotalRows += totalRows
                job.SuccessfulFiles++
                p.mu.Unlock()

                // TRIGGER CDC SYNC FOR NEW TABLES
                if p.cdcTrigger != nil && len(tableNames) > 0 {
                        for _, tableName := range tableNames {
                                logger.Info("Triggering CDC sync for table: %s", tableName)
                                if err := p.cdcTrigger(tableName); err != nil {
                                        logger.Warn("CDC sync failed for %s: %v", tableName, err)
                                } else {
                                        logger.Info("CDC sync completed for table: %s", tableName)
                                }
                        }
                }
        }

        p.mu.Lock()
        job := p.jobs[jobID]
        job.ProcessedFiles++
        p.mu.Unlock()

        logger.Info("Progress: %d/%d files processed", job.ProcessedFiles, job.TotalFiles)
}

func (p *PipelineProcessor) updateFileStatus(jobID, filePath string, status FileStatus, tableName, errMsg string, rows int) {
        p.mu.Lock()
        defer p.mu.Unlock()

        job := p.jobs[jobID]
        if file, exists := job.Files[filePath]; exists {
                file.Status = status
                file.TableName = tableName
                file.Error = errMsg
                file.RowsInserted = rows
                if status == StatusCompleted || status == StatusFailed {
                        now := time.Now()
                        file.CompletedAt = &now
                }
        }
}

func (p *PipelineProcessor) updateJobError(jobID, errMsg string) {
        p.mu.Lock()
        defer p.mu.Unlock()

        if job, exists := p.jobs[jobID]; exists {
                job.Status = "failed"
                job.Error = errMsg
                now := time.Now()
                job.CompletedAt = &now
        }
}

func (p *PipelineProcessor) moveToErrorFolder(filePath string, logger *utils.Logger) error {
        timestamp := time.Now().Format("20060102_150405")
        filename := filepath.Base(filePath)
        ext := filepath.Ext(filename)
        name := strings.TrimSuffix(filename, ext)

        newFilename := fmt.Sprintf("%s_ERROR_%s%s", name, timestamp, ext)
        newPath := filepath.Join(p.errorDir, newFilename)

        logger.Info("Moving error file: %s -> %s", filename, newPath)

        // Try to move file
        if err := os.Rename(filePath, newPath); err != nil {
                // If rename fails (cross-device), try copy + delete
                logger.Warn("Rename failed, trying copy+delete: %v", err)
                if err := copyFile(filePath, newPath); err != nil {
                        return err
                }
                os.Remove(filePath)
        }

        logger.Info("Successfully moved error file to: %s", newPath)
        return nil
}

func copyFile(src, dst string) error {
        in, err := os.Open(src)
        if err != nil {
                return err
        }
        defer in.Close()

        out, err := os.Create(dst)
        if err != nil {
                return err
        }
        defer out.Close()

        _, err = io.Copy(out, in)
        return err
}
