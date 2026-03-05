package pipeline

import (
        "context"
        "database/sql"
        "encoding/json"
        "fmt"
        "time"

        "github.com/jackc/pgx/v5/pgxpool"
)

// PipelineConfig represents a saved pipeline configuration
type PipelineConfig struct {
        ID          string                 `json:"id"`
        UserID      int                    `json:"user_id"`
        Name        string                 `json:"name"`
        Description string                 `json:"description"`
        FolderPath  string                 `json:"folder_path"`
        Recursive   bool                   `json:"recursive"`
        
        // Default options applied to all files
        DefaultOptions LoadOptions `json:"default_options"`
        
        // Per-file overrides: map[filePath]LoadOptions
        FileOptions map[string]LoadOptions `json:"file_options"`
        
        // File detection results (cached from last scan)
        FileDiscoveries []FileDiscovery `json:"file_discoveries,omitempty"`
        
        CreatedAt time.Time `json:"created_at"`
        UpdatedAt time.Time `json:"updated_at"`
}

// ConfigRepository handles persistence of pipeline configurations
type ConfigRepository struct {
        pool *pgxpool.Pool
}

// NewConfigRepository creates a new config repository
func NewConfigRepository(pool *pgxpool.Pool) *ConfigRepository {
        return &ConfigRepository{pool: pool}
}

// CreateTable creates the pipeline_configs table if it doesn't exist
func (r *ConfigRepository) CreateTable(ctx context.Context) error {
        query := `
        CREATE TABLE IF NOT EXISTS pipeline_configs (
                id VARCHAR(36) PRIMARY KEY,
                user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                name VARCHAR(255) NOT NULL,
                description TEXT,
                folder_path TEXT NOT NULL,
                recursive BOOLEAN DEFAULT false,
                default_options JSONB NOT NULL DEFAULT '{}',
                file_options JSONB NOT NULL DEFAULT '{}',
                file_discoveries JSONB,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );
        
        CREATE INDEX IF NOT EXISTS idx_pipeline_configs_user_id ON pipeline_configs(user_id);
        CREATE INDEX IF NOT EXISTS idx_pipeline_configs_folder_path ON pipeline_configs(folder_path);
        `
        
        _, err := r.pool.Exec(ctx, query)
        return err
}

// Save saves a pipeline configuration
func (r *ConfigRepository) Save(ctx context.Context, config *PipelineConfig) error {
        // Marshal JSON fields
        defaultOptsJSON, err := json.Marshal(config.DefaultOptions)
        if err != nil {
                return fmt.Errorf("failed to marshal default options: %w", err)
        }
        
        fileOptsJSON, err := json.Marshal(config.FileOptions)
        if err != nil {
                return fmt.Errorf("failed to marshal file options: %w", err)
        }
        
        var discoveriesJSON []byte
        if config.FileDiscoveries != nil {
                discoveriesJSON, err = json.Marshal(config.FileDiscoveries)
                if err != nil {
                        return fmt.Errorf("failed to marshal file discoveries: %w", err)
                }
        }
        
        query := `
        INSERT INTO pipeline_configs (id, user_id, name, description, folder_path, recursive, 
                default_options, file_options, file_discoveries, created_at, updated_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                description = EXCLUDED.description,
                folder_path = EXCLUDED.folder_path,
                recursive = EXCLUDED.recursive,
                default_options = EXCLUDED.default_options,
                file_options = EXCLUDED.file_options,
                file_discoveries = EXCLUDED.file_discoveries,
                updated_at = NOW()
        `
        
        _, err = r.pool.Exec(ctx, query,
                config.ID,
                config.UserID,
                config.Name,
                config.Description,
                config.FolderPath,
                config.Recursive,
                defaultOptsJSON,
                fileOptsJSON,
                discoveriesJSON,
                config.CreatedAt,
                config.UpdatedAt,
        )
        
        return err
}

// Get retrieves a configuration by ID
func (r *ConfigRepository) Get(ctx context.Context, id string) (*PipelineConfig, error) {
        query := `
        SELECT id, user_id, name, description, folder_path, recursive, 
                default_options, file_options, file_discoveries, created_at, updated_at
        FROM pipeline_configs WHERE id = $1
        `
        
        config := &PipelineConfig{}
        var defaultOptsJSON, fileOptsJSON, discoveriesJSON []byte
        
        err := r.pool.QueryRow(ctx, query, id).Scan(
                &config.ID,
                &config.UserID,
                &config.Name,
                &config.Description,
                &config.FolderPath,
                &config.Recursive,
                &defaultOptsJSON,
                &fileOptsJSON,
                &discoveriesJSON,
                &config.CreatedAt,
                &config.UpdatedAt,
        )
        
        if err != nil {
                return nil, err
        }
        
        // Unmarshal JSON fields
        if err := json.Unmarshal(defaultOptsJSON, &config.DefaultOptions); err != nil {
                return nil, fmt.Errorf("failed to unmarshal default options: %w", err)
        }
        
        if err := json.Unmarshal(fileOptsJSON, &config.FileOptions); err != nil {
                return nil, fmt.Errorf("failed to unmarshal file options: %w", err)
        }
        
        if len(discoveriesJSON) > 0 {
                if err := json.Unmarshal(discoveriesJSON, &config.FileDiscoveries); err != nil {
                        return nil, fmt.Errorf("failed to unmarshal file discoveries: %w", err)
                }
        }
        
        return config, nil
}

// GetByUser retrieves all configurations for a user
func (r *ConfigRepository) GetByUser(ctx context.Context, userID int) ([]*PipelineConfig, error) {
        query := `
        SELECT id, user_id, name, description, folder_path, recursive, 
                default_options, file_options, file_discoveries, created_at, updated_at
        FROM pipeline_configs WHERE user_id = $1 ORDER BY updated_at DESC
        `
        
        rows, err := r.pool.Query(ctx, query, userID)
        if err != nil {
                return nil, err
        }
        defer rows.Close()
        
        var configs []*PipelineConfig
        
        for rows.Next() {
                config := &PipelineConfig{}
                var defaultOptsJSON, fileOptsJSON, discoveriesJSON []byte
                
                err := rows.Scan(
                        &config.ID,
                        &config.UserID,
                        &config.Name,
                        &config.Description,
                        &config.FolderPath,
                        &config.Recursive,
                        &defaultOptsJSON,
                        &fileOptsJSON,
                        &discoveriesJSON,
                        &config.CreatedAt,
                        &config.UpdatedAt,
                )
                if err != nil {
                        return nil, err
                }
                
                // Unmarshal JSON fields
                json.Unmarshal(defaultOptsJSON, &config.DefaultOptions)
                json.Unmarshal(fileOptsJSON, &config.FileOptions)
                if len(discoveriesJSON) > 0 {
                        json.Unmarshal(discoveriesJSON, &config.FileDiscoveries)
                }
                
                configs = append(configs, config)
        }
        
        return configs, nil
}

// GetByFolderPath retrieves configurations for a specific folder path
func (r *ConfigRepository) GetByFolderPath(ctx context.Context, userID int, folderPath string) (*PipelineConfig, error) {
        query := `
        SELECT id, user_id, name, description, folder_path, recursive, 
                default_options, file_options, file_discoveries, created_at, updated_at
        FROM pipeline_configs WHERE user_id = $1 AND folder_path = $2
        ORDER BY updated_at DESC LIMIT 1
        `
        
        config := &PipelineConfig{}
        var defaultOptsJSON, fileOptsJSON, discoveriesJSON []byte
        
        err := r.pool.QueryRow(ctx, query, userID, folderPath).Scan(
                &config.ID,
                &config.UserID,
                &config.Name,
                &config.Description,
                &config.FolderPath,
                &config.Recursive,
                &defaultOptsJSON,
                &fileOptsJSON,
                &discoveriesJSON,
                &config.CreatedAt,
                &config.UpdatedAt,
        )
        
        if err == sql.ErrNoRows {
                return nil, nil // No config found
        }
        if err != nil {
                return nil, err
        }
        
        // Unmarshal JSON fields
        json.Unmarshal(defaultOptsJSON, &config.DefaultOptions)
        json.Unmarshal(fileOptsJSON, &config.FileOptions)
        if len(discoveriesJSON) > 0 {
                json.Unmarshal(discoveriesJSON, &config.FileDiscoveries)
        }
        
        return config, nil
}

// Delete removes a configuration
func (r *ConfigRepository) Delete(ctx context.Context, id string) error {
        query := `DELETE FROM pipeline_configs WHERE id = $1`
        _, err := r.pool.Exec(ctx, query, id)
        return err
}

// UpdateFileOptions updates just the file options for a config
func (r *ConfigRepository) UpdateFileOptions(ctx context.Context, id string, fileOptions map[string]LoadOptions) error {
        fileOptsJSON, err := json.Marshal(fileOptions)
        if err != nil {
                return err
        }
        
        query := `UPDATE pipeline_configs SET file_options = $1, updated_at = NOW() WHERE id = $2`
        _, err = r.pool.Exec(ctx, query, fileOptsJSON, id)
        return err
}

// UpdateFileDiscoveries updates the cached file discoveries
func (r *ConfigRepository) UpdateFileDiscoveries(ctx context.Context, id string, discoveries []FileDiscovery) error {
        discoveriesJSON, err := json.Marshal(discoveries)
        if err != nil {
                return err
        }
        
        query := `UPDATE pipeline_configs SET file_discoveries = $1, updated_at = NOW() WHERE id = $2`
        _, err = r.pool.Exec(ctx, query, discoveriesJSON, id)
        return err
}
