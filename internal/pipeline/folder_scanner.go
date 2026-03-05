package pipeline

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/semaphore"
)

// =============================================================================
// DATA STRUCTURES
// =============================================================================

// FileDiscovery contains information about a discovered file
type FileDiscovery struct {
	FilePath          string      `json:"file_path"`
	FileName          string      `json:"file_name"`
	FileSize          int64       `json:"file_size"`
	Extension         string      `json:"extension"`
	Encoding          string      `json:"encoding"`
	DetectedHeader    *HeaderInfo `json:"detected_header"`
	DetectedDelimiter string      `json:"detectedDelimiter"`
	Preview           [][]string  `json:"preview,omitempty"`
	Error             string      `json:"error,omitempty"`
}

// FolderScanResult contains the complete scan results for a folder
type FolderScanResult struct {
	FolderPath     string          `json:"folder_path"`
	TotalFiles     int             `json:"total_files"`
	SupportedFiles int             `json:"supported_files"`
	Files          []FileDiscovery `json:"files"`
	ScanTime       string          `json:"scan_time"`
	ScanDurationMs int64           `json:"scan_duration_ms"` // Added for precise timing
	Errors         []string        `json:"errors,omitempty"`
}

// FolderScannerConfig holds configuration for the scanner
type FolderScannerConfig struct {
	PreviewRows   int
	MaxWorkers    int
	MaxFileSize   int64 // Maximum file size to analyze (0 = no limit)
	SkipLargeFile bool  // Skip files larger than MaxFileSize
}

// DefaultFolderScannerConfig returns sensible defaults
func DefaultFolderScannerConfig() FolderScannerConfig {
	return FolderScannerConfig{
		PreviewRows:   DefaultPreviewRows,
		MaxWorkers:    4,
		MaxFileSize:   500 * 1024 * 1024, // 500MB
		SkipLargeFile: true,
	}
}

// FolderScanner scans directories for files and detects their properties
type FolderScanner struct {
	config FolderScannerConfig
}

// NewFolderScanner creates a new folder scanner with default configuration
func NewFolderScanner(previewRows int) *FolderScanner {
	config := DefaultFolderScannerConfig()
	if previewRows > 0 {
		config.PreviewRows = previewRows
	}
	return &FolderScanner{config: config}
}

// NewFolderScannerWithConfig creates a new folder scanner with custom configuration
func NewFolderScannerWithConfig(config FolderScannerConfig) *FolderScanner {
	if config.PreviewRows <= 0 {
		config.PreviewRows = DefaultPreviewRows
	}
	if config.MaxWorkers <= 0 {
		config.MaxWorkers = 4
	}
	return &FolderScanner{config: config}
}

// =============================================================================
// SCANNER METHODS - CONTEXT AWARE (fixes M2)
// =============================================================================

// ScanFolder scans a folder and returns all files with detected properties.
// Now accepts context for cancellation support.
func (s *FolderScanner) ScanFolder(ctx context.Context, folderPath string, recursive bool) (*FolderScanResult, error) {
	logger := slog.With("operation", "scan_folder", "path", folderPath, "recursive", recursive)
	logger.Debug("starting folder scan")

	startTime := time.Now()
	result := &FolderScanResult{
		FolderPath: folderPath,
		Files:      make([]FileDiscovery, 0),
		Errors:     make([]string, 0),
	}

	// Verify folder exists (fixes O1 - use errors.Is)
	stat, err := os.Stat(folderPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, fmt.Errorf("folder does not exist: %s", folderPath)
		}
		return nil, fmt.Errorf("cannot access folder %s: %w", folderPath, err)
	}
	if !stat.IsDir() {
		return nil, fmt.Errorf("not a directory: %s", folderPath)
	}

	// Use WalkDir instead of Walk (fixes P1 - more efficient)
	err = filepath.WalkDir(folderPath, func(path string, d fs.DirEntry, err error) error {
		// Check for context cancellation
		if ctx.Err() != nil {
			return ctx.Err()
		}

		if err != nil {
			result.Errors = append(result.Errors, fmt.Sprintf("Error accessing %s: %v", path, err))
			return nil
		}

		// Skip subdirectories if not recursive
		if !recursive && d.IsDir() && path != folderPath {
			return fs.SkipDir
		}

		// Only process files
		if d.IsDir() {
			return nil
		}

		result.TotalFiles++
		ext := strings.ToLower(filepath.Ext(path))

		// Check if supported (fixes R2 - use shared constant)
		if !SupportedExtensions[ext] {
			return nil
		}

		result.SupportedFiles++

		// Get file info
		info, err := d.Info()
		if err != nil {
			result.Errors = append(result.Errors, fmt.Sprintf("Cannot get info for %s: %v", path, err))
			return nil
		}

		// Discover file properties
		fileInfo := s.discoverFile(ctx, path, info, ext)
		result.Files = append(result.Files, fileInfo)

		return nil
	})

	if err != nil && !errors.Is(err, context.Canceled) {
		return nil, fmt.Errorf("error scanning folder %s: %w", folderPath, err)
	}

	// Calculate scan time
	duration := time.Since(startTime)
	result.ScanTime = duration.String()
	result.ScanDurationMs = duration.Milliseconds()

	logger.Debug("folder scan complete",
		"total_files", result.TotalFiles,
		"supported_files", result.SupportedFiles,
		"duration_ms", result.ScanDurationMs)

	return result, nil
}

// ScanFolderParallel scans a folder using parallel workers with bounded parallelism.
// Now accepts context for cancellation support and uses semaphore for bounded parallelism (fixes P1, M4).
func (s *FolderScanner) ScanFolderParallel(ctx context.Context, folderPath string, recursive bool, workers int) (*FolderScanResult, error) {
	logger := slog.With("operation", "scan_folder_parallel", "path", folderPath, "recursive", recursive, "workers", workers)
	logger.Debug("starting parallel folder scan")

	startTime := time.Now()
	result := &FolderScanResult{
		FolderPath: folderPath,
		Files:      make([]FileDiscovery, 0),
		Errors:     make([]string, 0),
	}

	// Verify folder exists (fixes O1)
	stat, err := os.Stat(folderPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, fmt.Errorf("folder does not exist: %s", folderPath)
		}
		return nil, fmt.Errorf("cannot access folder %s: %w", folderPath, err)
	}
	if !stat.IsDir() {
		return nil, fmt.Errorf("not a directory: %s", folderPath)
	}

	// Limit workers to reasonable number
	if workers <= 0 {
		workers = s.config.MaxWorkers
	}
	if workers > 16 {
		workers = 16 // Cap at 16 to prevent resource exhaustion
	}

	// Channel for file paths (buffered for efficiency)
	fileChan := make(chan string, 100)
	resultsChan := make(chan FileDiscovery, 100)

	// Semaphore for bounded parallelism (fixes M4)
	sem := semaphore.NewWeighted(int64(workers))

	// Mutex for result aggregation
	var mu sync.Mutex

	// Producer goroutine - walks directory and sends file paths
	producerDone := make(chan error, 1)
	go func() {
		defer close(fileChan)

		err := filepath.WalkDir(folderPath, func(path string, d fs.DirEntry, err error) error {
			// Check for context cancellation
			if ctx.Err() != nil {
				return ctx.Err()
			}

			if err != nil {
				mu.Lock()
				result.Errors = append(result.Errors, fmt.Sprintf("Error accessing %s: %v", path, err))
				mu.Unlock()
				return nil
			}

			if !recursive && d.IsDir() && path != folderPath {
				return fs.SkipDir
			}

			if d.IsDir() {
				return nil
			}

			mu.Lock()
			result.TotalFiles++
			mu.Unlock()

			ext := strings.ToLower(filepath.Ext(path))
			if SupportedExtensions[ext] {
				select {
				case fileChan <- path:
				case <-ctx.Done():
					return ctx.Err()
				}
			}

			return nil
		})
		producerDone <- err
	}()

	// Consumer goroutines - process files with bounded parallelism
	var wg sync.WaitGroup
	consumerDone := make(chan struct{})

	go func() {
		for path := range fileChan {
			// Check for context cancellation
			if ctx.Err() != nil {
				break
			}

			// Acquire semaphore (bounded parallelism)
			if err := sem.Acquire(ctx, 1); err != nil {
				// Context cancelled
				break
			}

			wg.Add(1)
			go func(filePath string) {
				defer wg.Done()
				defer sem.Release(1)

				info, err := os.Stat(filePath)
				if err != nil {
					resultsChan <- FileDiscovery{
						FilePath: filePath,
						Error:    fmt.Errorf("cannot stat file: %w", err).Error(),
					}
					return
				}

				ext := strings.ToLower(filepath.Ext(filePath))
				fileInfo := s.discoverFile(ctx, filePath, info, ext)
				resultsChan <- fileInfo
			}(path)
		}

		wg.Wait()
		close(resultsChan)
		close(consumerDone)
	}()

	// Collect results
	go func() {
		<-consumerDone
	}()

	for fileInfo := range resultsChan {
		mu.Lock()
		result.SupportedFiles++
		result.Files = append(result.Files, fileInfo)
		mu.Unlock()
	}

	// Wait for producer and check for errors
	if err := <-producerDone; err != nil && !errors.Is(err, context.Canceled) {
		return nil, fmt.Errorf("error walking directory: %w", err)
	}

	// Calculate scan time
	duration := time.Since(startTime)
	result.ScanTime = duration.String()
	result.ScanDurationMs = duration.Milliseconds()

	logger.Debug("parallel folder scan complete",
		"total_files", result.TotalFiles,
		"supported_files", result.SupportedFiles,
		"duration_ms", result.ScanDurationMs)

	return result, nil
}

// QuickScan does a fast scan without deep analysis (just file listing).
// Now accepts context for cancellation support.
func (s *FolderScanner) QuickScan(ctx context.Context, folderPath string, recursive bool) (*FolderScanResult, error) {
	logger := slog.With("operation", "quick_scan", "path", folderPath, "recursive", recursive)
	logger.Debug("starting quick scan")

	startTime := time.Now()
	result := &FolderScanResult{
		FolderPath: folderPath,
		Files:      make([]FileDiscovery, 0),
		Errors:     make([]string, 0),
	}

	// Verify folder exists (fixes O1)
	stat, err := os.Stat(folderPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, fmt.Errorf("folder does not exist: %s", folderPath)
		}
		return nil, fmt.Errorf("cannot access folder %s: %w", folderPath, err)
	}
	if !stat.IsDir() {
		return nil, fmt.Errorf("not a directory: %s", folderPath)
	}

	// Use WalkDir for efficiency (fixes P1)
	err = filepath.WalkDir(folderPath, func(path string, d fs.DirEntry, err error) error {
		// Check for context cancellation
		if ctx.Err() != nil {
			return ctx.Err()
		}

		if err != nil {
			return nil
		}

		if !recursive && d.IsDir() && path != folderPath {
			return fs.SkipDir
		}

		if d.IsDir() {
			return nil
		}

		result.TotalFiles++
		ext := strings.ToLower(filepath.Ext(path))

		if SupportedExtensions[ext] {
			info, err := d.Info()
			if err != nil {
				return nil
			}
			result.SupportedFiles++
			result.Files = append(result.Files, FileDiscovery{
				FilePath:  path,
				FileName:  filepath.Base(path),
				FileSize:  info.Size(),
				Extension: ext,
			})
		}

		return nil
	})

	if err != nil && !errors.Is(err, context.Canceled) {
		return nil, fmt.Errorf("error scanning folder: %w", err)
	}

	// Calculate scan time
	duration := time.Since(startTime)
	result.ScanTime = duration.String()
	result.ScanDurationMs = duration.Milliseconds()

	logger.Debug("quick scan complete",
		"total_files", result.TotalFiles,
		"supported_files", result.SupportedFiles,
		"duration_ms", result.ScanDurationMs)

	return result, nil
}

// =============================================================================
// FILE DISCOVERY
// =============================================================================

// discoverFile analyzes a single file
func (s *FolderScanner) discoverFile(ctx context.Context, path string, info os.FileInfo, ext string) FileDiscovery {
	logger := slog.With("operation", "discover_file", "path", path)
	logger.Debug("analyzing file")

	fileInfo := FileDiscovery{
		FilePath:  path,
		FileName:  filepath.Base(path),
		FileSize:  info.Size(),
		Extension: ext,
	}

	// Check for context cancellation
	if ctx.Err() != nil {
		fileInfo.Error = "operation cancelled"
		return fileInfo
	}

	// Check file size limits
	if s.config.SkipLargeFile && s.config.MaxFileSize > 0 && info.Size() > s.config.MaxFileSize {
		fileInfo.Error = fmt.Sprintf("file too large: %d bytes (max: %d)", info.Size(), s.config.MaxFileSize)
		return fileInfo
	}

	// JSON files don't have headers/delimiters in the traditional sense
	if ext == ".json" || ext == ".jsonl" {
		fileInfo.Encoding = "UTF-8"
		fileInfo.DetectedDelimiter = "N/A"
		fileInfo.DetectedHeader = &HeaderInfo{
			HasHeader:  false,
			Confidence: 1.0,
			Reason:     "JSON/JSONL files use key-value pairs, not tabular headers",
		}
		return fileInfo
	}

	// Excel files
	if ext == ".xlsx" || ext == ".xls" {
		fileInfo.Encoding = "N/A"
		fileInfo.DetectedDelimiter = "N/A"
		// Excel detection uses shared function with context
		headerInfo, err := DetectHeaderWithOptions(ctx, path, LoadOptions{})
		if err != nil {
			fileInfo.Error = fmt.Sprintf("Failed to detect header: %v", err)
			return fileInfo
		}
		fileInfo.DetectedHeader = headerInfo
		fileInfo.Preview = headerInfo.Preview // Include the preview rows
		return fileInfo
	}

	// CSV/TXT files
	// Detect encoding
	encoding, err := DetectEncoding(ctx, path)
	if err != nil {
		logger.Debug("encoding detection failed, using UTF-8 fallback", "error", err)
		encoding = "UTF-8"
	}
	fileInfo.Encoding = encoding

	// Detect delimiter
	delimiter, err := DetectDelimiter(ctx, path, encoding)
	if err != nil {
		logger.Debug("delimiter detection failed, using comma fallback", "error", err)
		delimiter = ','
	}
	fileInfo.DetectedDelimiter = string(delimiter)

	// Detect header
	headerInfo, err := DetectHeaderEnhanced(ctx, path, encoding, delimiter, s.config.PreviewRows)
	if err != nil {
		fileInfo.Error = fmt.Sprintf("Failed to detect header: %v", err)
		return fileInfo
	}
	fileInfo.DetectedHeader = headerInfo
	fileInfo.Preview = headerInfo.Preview

	return fileInfo
}

// =============================================================================
// BACKWARD COMPATIBILITY WRAPPERS (for non-context callers)
// =============================================================================

// ScanFolderWithoutCtx provides backward compatibility without context
func (s *FolderScanner) ScanFolderWithoutCtx(folderPath string, recursive bool) (*FolderScanResult, error) {
	return s.ScanFolder(context.Background(), folderPath, recursive)
}

// ScanFolderParallelWithoutCtx provides backward compatibility without context
func (s *FolderScanner) ScanFolderParallelWithoutCtx(folderPath string, recursive bool, workers int) (*FolderScanResult, error) {
	return s.ScanFolderParallel(context.Background(), folderPath, recursive, workers)
}

// QuickScanWithoutCtx provides backward compatibility without context
func (s *FolderScanner) QuickScanWithoutCtx(folderPath string, recursive bool) (*FolderScanResult, error) {
	return s.QuickScan(context.Background(), folderPath, recursive)
}
