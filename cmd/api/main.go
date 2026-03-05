package main

import (
        "context"
        "fmt"
        "highperf-api/internal/auth"
        "highperf-api/internal/cache"
        "highperf-api/internal/clickhouse"
        "highperf-api/internal/config"
        "highperf-api/internal/database"
        "highperf-api/internal/handlers"
        "highperf-api/internal/middleware"
        "highperf-api/internal/pipeline"
        "highperf-api/internal/schema"
        "log"
        "net/http"
        "os"
        "os/signal"
        "syscall"
        "time"
)

func main() {
        cfg := config.LoadConfig()
        ctx := context.Background()

        log.Println("═══════════════════════════════════════════════════════════")
        log.Println("🚀 L.S.D (Large Search Data) API Server Starting")
        log.Println("═══════════════════════════════════════════════════════════")

        pool, err := database.NewPool(ctx, cfg.DatabaseURL)
        if err != nil {
                log.Fatalf("Failed to connect to database: %v", err)
        }
        defer pool.Close()
        log.Println("Database connected")

        redisCache := cache.NewRedisCache(
                cfg.RedisAddr,
                cfg.RedisPassword,
                cfg.RedisDB,
                5*time.Minute,
        )

        multiCache := cache.NewMultiLayerCache(redisCache, 30*time.Second)
        log.Println("Multi-layer cache initialized")

        registry := schema.NewSchemaRegistry(pool.Pool)
        if err := registry.LoadSchema(ctx); err != nil {
                log.Fatalf("Failed to load schema: %v", err)
        }
        log.Printf("Schema loaded: %d tables discovered", len(registry.GetAllTables()))

        // ⭐ ClickHouse Connection Pool
        chPool, err := clickhouse.NewConnectionPool(clickhouse.Config{
                Addr:     cfg.ClickHouseAddr,
                Database: cfg.ClickHouseDB,
                Username: cfg.ClickHouseUser,
                Password: cfg.ClickHousePassword,
        }, 5)

        if err != nil {
                log.Printf("ClickHouse pool creation failed: %v", err)
        }

        var chSearch *clickhouse.SearchRepository
        if chPool != nil && chPool.IsAvailable() {
                chSearch = clickhouse.NewSearchRepository(chPool, registry)
                log.Println("✅ ClickHouse search repository initialized with connection pool (5 connections)")
        } else {
                log.Println("⚠️  ClickHouse not available, search will use PostgreSQL only")
        }

        dynamicRepo := database.NewDynamicRepository(pool.Pool, registry)
        dynamicHandler := handlers.NewDynamicHandler(
                dynamicRepo,
                registry,
                multiCache,
                chSearch,
                50, 20,
                120*time.Second,
        )

        // Initialize CDC Manager
        var cdcManager *clickhouse.CDCManager
        if chPool != nil && chPool.IsAvailable() && cfg.EnableCDC {
                cdcConfig := clickhouse.CDCConfig{
                        BatchSize:       10000,
                        SyncInterval:    30 * time.Second,
                        ParallelWorkers: 5,
                        ChunkSize:       100000,
                }

                cdcManager = clickhouse.NewCDCManager(pool.Pool, chSearch, registry, cdcConfig)
                dynamicHandler.SetCDCManager(cdcManager)
                cdcManager.Start()
                log.Println("🚀 CDC Manager started with auto-discovery")
        }

        // Initialize Pipeline Processor
        pipelineProcessor := pipeline.NewPipelineProcessor(pool.Pool, "./ErrorFiles")

        // Initialize FileLoader for direct file operations
        fileLoader := pipeline.NewFileLoader(pool.Pool, 10000) // 10k chunk size

        // Initialize Config Repository for saving pipeline configurations
        configRepo := pipeline.NewConfigRepository(pool.Pool)
        if err := configRepo.CreateTable(ctx); err != nil {
                log.Printf("⚠️  Failed to create pipeline_configs table: %v", err)
        } else {
                log.Println("✅ Pipeline configs table initialized")
        }

        if cdcManager != nil {
                pipelineProcessor.SetCDCTrigger(func(tableName string) error {
                        log.Printf("🔄 Pipeline completed for table: %s, triggering CDC sync...", tableName)
                        if err := cdcManager.TriggerTableSync(tableName); err != nil {
                                log.Printf("⚠️  CDC sync failed for %s: %v", tableName, err)
                                return err
                        }
                        log.Printf("✅ CDC sync completed for table: %s", tableName)
                        return nil
                })
                log.Println("🔗 Pipeline-to-CDC integration enabled")
        }

        pipelineHandler := handlers.NewPipelineHandlerWithConfig(pipelineProcessor, fileLoader, configRepo)

        // ═══════════════════════════════════════════════════════════
        // 🔐 AUTHENTICATION SETUP (Updated with JWT)
        // ═══════════════════════════════════════════════════════════

        // SECURITY: JWT secret is loaded from config, which validates it in production
        // No fallback here - config.LoadConfig() handles secret generation/validation
        authService := auth.NewAuthService(cfg.JWTSecret)
        authHandler := handlers.NewAuthHandler(pool.Pool, authService)
        authMiddleware := middleware.NewAuthMiddleware(authService, pool.Pool)

        // ═══════════════════════════════════════════════════════════
        // 🌐 ROUTER SETUP
        // ═══════════════════════════════════════════════════════════

        mux := http.NewServeMux()

        // ═══════════════════════════════════════════════════════════
        // 🔓 PUBLIC ROUTES
        // ═══════════════════════════════════════════════════════════

        // SECURITY: Create stricter rate limiter for auth endpoints (brute-force protection)
        authRateLimiter := middleware.RateLimiterWithConfig(middleware.RateLimiterConfig{
                RequestsPerSecond: 1,  // 1 request per second
                Burst:             5,  // Allow short bursts
                CleanupInterval:   5 * time.Minute,
                VisitorExpiry:     15 * time.Minute,
        })

        // Auth Pages
        mux.HandleFunc("GET /login", func(w http.ResponseWriter, r *http.Request) {
                http.ServeFile(w, r, "./web/login.html")
        })
        mux.HandleFunc("GET /register", func(w http.ResponseWriter, r *http.Request) {
                http.ServeFile(w, r, "./web/register.html")
        })
        mux.HandleFunc("GET /dashboard", func(w http.ResponseWriter, r *http.Request) {
                http.ServeFile(w, r, "./web/dashboard.html")
        })

        // Docs Page (Scalar API Reference)
        mux.HandleFunc("GET /docs", func(w http.ResponseWriter, r *http.Request) {
                http.ServeFile(w, r, "./web/docs.html")
        })

        // Auth API Endpoints - SECURITY: Apply stricter rate limiting to prevent brute-force
        mux.Handle("POST /api/auth/register", authRateLimiter(http.HandlerFunc(authHandler.Register)))
        mux.Handle("POST /api/auth/login", authRateLimiter(http.HandlerFunc(authHandler.Login)))
        mux.HandleFunc("POST /api/auth/logout", authHandler.Logout)
        mux.HandleFunc("GET /api/auth/me", authMiddleware.RequireAuth(http.HandlerFunc(authHandler.GetMe)).ServeHTTP)

        // API Keys (Protected)
        mux.HandleFunc("GET /api/auth/api-keys", authMiddleware.RequireAuth(http.HandlerFunc(authHandler.ListAPIKeys)).ServeHTTP)
        mux.HandleFunc("POST /api/auth/api-keys", authMiddleware.RequireAuth(http.HandlerFunc(authHandler.CreateAPIKey)).ServeHTTP)
        mux.HandleFunc("DELETE /api/auth/api-keys/{id}", authMiddleware.RequireAuth(http.HandlerFunc(authHandler.RevokeAPIKey)).ServeHTTP)

        // Health Check
        mux.HandleFunc("GET /api/health", dynamicHandler.HealthCheck)

        // Static Files
        mux.HandleFunc("GET /swagger.yaml", func(w http.ResponseWriter, r *http.Request) {
                http.ServeFile(w, r, "./web/swagger.yaml")
        })
        mux.HandleFunc("GET /style.css", func(w http.ResponseWriter, r *http.Request) {
                http.ServeFile(w, r, "./web/style.css")
        })
        mux.HandleFunc("GET /app.js", func(w http.ResponseWriter, r *http.Request) {
                http.ServeFile(w, r, "./web/app.js")
        })
        // Assets folder (for scalar-standalone.js, images, etc.)
        mux.Handle("GET /assets/", http.StripPrefix("/assets/", http.FileServer(http.Dir("./web/assets"))))

        // Index Page (Documentation)
        mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
                if r.URL.Path == "/" {
                        http.ServeFile(w, r, "./web/index.html")
                } else {
                        http.NotFound(w, r)
                }
        })

        // ═══════════════════════════════════════════════════════════
        // 🔒 PROTECTED ROUTES (with Auth Middleware)
        // ═══════════════════════════════════════════════════════════

        // Table Endpoints
        mux.Handle("GET /api/tables", authMiddleware.RequireAuth(http.HandlerFunc(dynamicHandler.ListTables)))
        mux.Handle("GET /api/tables/{table}/schema", authMiddleware.RequireAuth(http.HandlerFunc(dynamicHandler.GetTableSchema)))
        mux.Handle("GET /api/tables/{table}/records", authMiddleware.RequireAuth(http.HandlerFunc(dynamicHandler.GetRecords)))
        mux.Handle("GET /api/tables/{table}/records/{pk}", authMiddleware.RequireAuth(http.HandlerFunc(dynamicHandler.GetRecordByPK)))
        mux.Handle("GET /api/tables/{table}/stats", authMiddleware.RequireAuth(http.HandlerFunc(dynamicHandler.GetTableStats)))
        mux.Handle("GET /api/tables/{table}/search", authMiddleware.RequireAuth(http.HandlerFunc(dynamicHandler.SearchRecords)))

        // Search Endpoints
        mux.Handle("GET /api/search/", authMiddleware.RequireAuth(http.HandlerFunc(dynamicHandler.SearchOptimized)))

        // Pipeline Endpoints
        mux.Handle("POST /api/pipeline/start", authMiddleware.RequireAuth(http.HandlerFunc(pipelineHandler.StartJob)))
        mux.Handle("GET /api/pipeline/jobs/{job_id}", authMiddleware.RequireAuth(http.HandlerFunc(pipelineHandler.GetJobStatus)))
        mux.Handle("GET /api/pipeline/jobs", authMiddleware.RequireAuth(http.HandlerFunc(pipelineHandler.ListJobs)))
        mux.Handle("GET /api/pipeline/jobs/{job_id}/stream", authMiddleware.RequireAuth(http.HandlerFunc(pipelineHandler.StreamJobProgress)))
        mux.Handle("GET /api/pipeline/jobs/{job_id}/logs", authMiddleware.RequireAuth(http.HandlerFunc(pipelineHandler.GetJobLogs)))
        
        // New Pipeline Endpoints (File Preview, Upload, Load)
        mux.Handle("POST /api/pipeline/preview", authMiddleware.RequireAuth(http.HandlerFunc(pipelineHandler.PreviewFile)))
        mux.Handle("POST /api/pipeline/upload", authMiddleware.RequireAuth(http.HandlerFunc(pipelineHandler.UploadFile)))
        mux.Handle("POST /api/pipeline/load", authMiddleware.RequireAuth(http.HandlerFunc(pipelineHandler.LoadFile)))
        mux.Handle("POST /api/pipeline/detect-header", authMiddleware.RequireAuth(http.HandlerFunc(pipelineHandler.DetectHeader)))

        // Folder Scanning & Configuration Endpoints (v2.2.0)
        mux.Handle("POST /api/pipeline/scan-folder", authMiddleware.RequireAuth(http.HandlerFunc(pipelineHandler.ScanFolder)))
        mux.Handle("GET /api/pipeline/configs", authMiddleware.RequireAuth(http.HandlerFunc(pipelineHandler.ListConfigs)))
        mux.Handle("POST /api/pipeline/configs", authMiddleware.RequireAuth(http.HandlerFunc(pipelineHandler.SaveConfig)))
        mux.Handle("GET /api/pipeline/configs/{id}", authMiddleware.RequireAuth(http.HandlerFunc(pipelineHandler.GetConfig)))
        mux.Handle("DELETE /api/pipeline/configs/{id}", authMiddleware.RequireAuth(http.HandlerFunc(pipelineHandler.DeleteConfig)))
        mux.Handle("POST /api/pipeline/start-with-config", authMiddleware.RequireAuth(http.HandlerFunc(pipelineHandler.StartJobWithConfig)))

        // CDC Status
        mux.Handle("GET /api/cdc/status", authMiddleware.RequireAuth(http.HandlerFunc(dynamicHandler.GetCDCStatus)))

        // ═══════════════════════════════════════════════════════════
        // 🛡️ GLOBAL MIDDLEWARE
        // ═══════════════════════════════════════════════════════════

        // SECURITY: Request body size limit (10MB)
        handler := middleware.RequestBodyLimiter(10 * 1024 * 1024)(mux)

        // SECURITY: Rate limiting
        handler = middleware.RateLimiter(handler)

        // SECURITY: CORS with configured origins
        corsConfig := middleware.DefaultCORSConfig()
        if cfg.IsProduction() {
                // In production, origins should be explicitly configured
                // Add your production origins here via environment variable
        }
        handler = middleware.CORS(corsConfig)(handler)
        handler = middleware.Logger(handler)

        server := &http.Server{
                Addr:         fmt.Sprintf(":%s", cfg.Port),
                Handler:      handler,
                ReadTimeout:  30 * time.Second,
                WriteTimeout: 5 * time.Minute, // Increased for long-running operations (folder scanning, file uploads)
                IdleTimeout:  120 * time.Second,
        }

        go func() {
                if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
                        log.Fatalf("Server failed to start: %v", err)
                }
        }()

        quit := make(chan os.Signal, 1)
        signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
        <-quit

        log.Println("═══════════════════════════════════════════════════════════")
        log.Println("🛑 Shutting down server gracefully...")
        log.Println("═══════════════════════════════════════════════════════════")

        if cdcManager != nil {
                log.Println("⏸️  Stopping CDC Manager...")
                cdcManager.Stop()
        }

        if chPool != nil {
                log.Println("🔌 Closing ClickHouse connection pool...")
                if err := chPool.Close(); err != nil {
                        log.Printf("⚠️  Error closing ClickHouse pool: %v", err)
                }
        }

        // SECURITY: Close cache to stop cleanup goroutine and prevent leaks
        if multiCache != nil {
                log.Println("📦 Closing cache...")
                if err := multiCache.Close(); err != nil {
                        log.Printf("⚠️  Error closing cache: %v", err)
                }
        }

        shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
        defer cancel()

        if err := server.Shutdown(shutdownCtx); err != nil {
                log.Fatalf("❌ Server forced to shutdown: %v", err)
        }

        log.Println("✅ Server exited properly")
}
