package config

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/joho/godotenv"
)

type Config struct {
	Port               string
	DatabaseURL        string
	ReadReplicaDSN     string
	RedisAddr          string
	RedisPassword      string
	RedisDB            int
	ClickHouseAddr     string
	ClickHouseDB       string
	ClickHouseUser     string
	ClickHousePassword string
	ClickHouseDSN      string
	EnableCDC          bool
	JWTSecret          string
	Environment        string // "development", "staging", "production"
}

// IsProduction returns true if running in production environment
func (c *Config) IsProduction() bool {
	return strings.ToLower(c.Environment) == "production"
}

// IsDevelopment returns true if running in development environment
func (c *Config) IsDevelopment() bool {
	return strings.ToLower(c.Environment) == "development" || c.Environment == ""
}

func LoadConfig() *Config {
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found, using environment variables")
	}

	// Detect environment
	env := getEnv("ENV", getEnv("ENVIRONMENT", "development"))

	// SECURITY: Require JWT_SECRET in production
	jwtSecret := os.Getenv("JWT_SECRET")
	if jwtSecret == "" {
		if strings.ToLower(env) == "production" {
			log.Fatal("SECURITY ERROR: JWT_SECRET environment variable must be set in production")
		}
		// Generate a random secret for development only
		jwtSecret = generateDevSecret()
		log.Println("⚠️  WARNING: Using auto-generated JWT secret. Set JWT_SECRET for production!")
	} else if len(jwtSecret) < 32 {
		log.Println("⚠️  WARNING: JWT_SECRET should be at least 32 characters for security")
	}

	// SECURITY: Require DATABASE_URL (no hardcoded credentials)
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		if strings.ToLower(env) == "production" {
			log.Fatal("SECURITY ERROR: DATABASE_URL environment variable must be set in production")
		}
		// Development fallback - but warn
		databaseURL = "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"
		log.Println("⚠️  WARNING: Using default DATABASE_URL. Set DATABASE_URL for production!")
	}

	addr := getEnv("CLICKHOUSE_ADDR", "localhost:9000")

	cfg := &Config{
		Port:               getEnv("PORT", "8080"),
		DatabaseURL:        databaseURL,
		ReadReplicaDSN:     getEnv("READ_REPLICA_DSN", ""),
		RedisAddr:          getEnv("REDIS_ADDR", "localhost:6379"),
		RedisPassword:      getEnv("REDIS_PASSWORD", ""),
		RedisDB:            getEnvAsInt("REDIS_DB", 0),
		ClickHouseAddr:     addr,
		ClickHouseDB:       getEnv("CLICKHOUSE_DB", "default"),
		ClickHouseUser:     getEnv("CLICKHOUSE_USER", "default"),
		ClickHousePassword: getEnv("CLICKHOUSE_PASSWORD", ""),
		EnableCDC:          getEnv("ENABLE_CDC", "true") == "true",
		JWTSecret:          jwtSecret,
		Environment:        env,
	}

	// ═══════════════════════════════════════════════════════════
	// ⭐ OPTIMIZATION: Build DSN with Async Inserts
	// async_insert=1: Batches data in RAM before writing to disk
	// wait_for_async_insert=0: Don't wait for disk, return immediately (Fastest CDC)
	// ═══════════════════════════════════════════════════════════
	cfg.ClickHouseDSN = fmt.Sprintf(
		"tcp://%s?database=%s&username=%s&password=%s&async_insert=1&wait_for_async_insert=1",
		addr,
		cfg.ClickHouseDB,
		cfg.ClickHouseUser,
		cfg.ClickHousePassword,
	)

	// Log configuration status
	log.Printf("Configuration loaded: ENV=%s, PORT=%s, CDC=%v", cfg.Environment, cfg.Port, cfg.EnableCDC)

	return cfg
}

// generateDevSecret generates a cryptographically secure random secret for development
func generateDevSecret() string {
	bytes := make([]byte, 32)
	if _, err := rand.Read(bytes); err != nil {
		// This should never fail, but fallback to timestamp-based if it does
		log.Printf("Warning: failed to generate random bytes: %v", err)
		return "dev-fallback-secret-" + hex.EncodeToString([]byte(fmt.Sprintf("%d", os.Getpid())))
	}
	return "dev-" + hex.EncodeToString(bytes)
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvAsInt(key string, defaultValue int) int {
	valueStr := getEnv(key, "")
	if value, err := strconv.Atoi(valueStr); err == nil {
		return value
	}
	return defaultValue
}

// ValidateConfig validates critical configuration settings
func ValidateConfig(cfg *Config) error {
	if cfg.IsProduction() {
		if cfg.JWTSecret == "" {
			return fmt.Errorf("JWT_SECRET must be set in production")
		}
		if strings.Contains(cfg.DatabaseURL, "password@") || strings.Contains(cfg.DatabaseURL, ":postgres@") {
			log.Println("⚠️  WARNING: Database URL appears to contain default credentials")
		}
		if len(cfg.JWTSecret) < 32 {
			return fmt.Errorf("JWT_SECRET must be at least 32 characters in production")
		}
	}
	return nil
}
