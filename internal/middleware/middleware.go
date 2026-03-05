package middleware

import (
	"context"
	"log"
	"net/http"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

func Logger(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		next.ServeHTTP(w, r)

		log.Printf("%s %s %s", r.Method, r.URL.Path, time.Since(start))
	})
}

// CORSConfig holds CORS configuration
type CORSConfig struct {
	AllowedOrigins   []string
	AllowedMethods   []string
	AllowedHeaders   []string
	AllowCredentials bool
}

// DefaultCORSConfig returns a secure default CORS configuration
func DefaultCORSConfig() CORSConfig {
	return CORSConfig{
		AllowedOrigins:   []string{}, // Empty = same origin only
		AllowedMethods:   []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowedHeaders:   []string{"Content-Type", "Authorization", "X-API-Key"},
		AllowCredentials: true,
	}
}

// CORS creates a CORS middleware with configuration
func CORS(config CORSConfig) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			origin := r.Header.Get("Origin")

			// Check if origin is allowed
			allowed := false
			for _, allowedOrigin := range config.AllowedOrigins {
				if allowedOrigin == "*" && len(config.AllowedOrigins) == 1 {
					// Only allow wildcard if it's the only configured origin
					allowed = true
					break
				}
				if allowedOrigin == origin {
					allowed = true
					break
				}
			}

			// For development, allow localhost origins
			if !allowed && origin != "" {
				if isLocalhostOrigin(origin) {
					allowed = true
				}
			}

			if allowed && origin != "" {
				w.Header().Set("Access-Control-Allow-Origin", origin)
				if config.AllowCredentials {
					w.Header().Set("Access-Control-Allow-Credentials", "true")
				}
			}

			w.Header().Set("Access-Control-Allow-Methods", joinStrings(config.AllowedMethods, ", "))
			w.Header().Set("Access-Control-Allow-Headers", joinStrings(config.AllowedHeaders, ", "))

			if r.Method == "OPTIONS" {
				w.WriteHeader(http.StatusOK)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

// isLocalhostOrigin checks if an origin is a localhost origin (for development)
func isLocalhostOrigin(origin string) bool {
	return origin == "http://localhost" ||
		origin == "http://localhost:3000" ||
		origin == "http://localhost:5173" ||
		origin == "http://localhost:8080" ||
		origin == "http://127.0.0.1" ||
		origin == "http://127.0.0.1:3000" ||
		origin == "http://127.0.0.1:5173" ||
		origin == "http://127.0.0.1:8080"
}

func joinStrings(strs []string, sep string) string {
	if len(strs) == 0 {
		return ""
	}
	result := strs[0]
	for i := 1; i < len(strs); i++ {
		result += sep + strs[i]
	}
	return result
}

type visitor struct {
	limiter  *rate.Limiter
	lastSeen time.Time
}

// RateLimiterConfig holds rate limiter configuration
type RateLimiterConfig struct {
	RequestsPerSecond int
	Burst             int
	CleanupInterval   time.Duration
	VisitorExpiry     time.Duration
}

// DefaultRateLimiterConfig returns sensible defaults
func DefaultRateLimiterConfig() RateLimiterConfig {
	return RateLimiterConfig{
		RequestsPerSecond: 10,
		Burst:            20,
		CleanupInterval:  5 * time.Minute,
		VisitorExpiry:    5 * time.Minute,
	}
}

// RateLimiterManager manages rate limiters with proper lifecycle
type RateLimiterManager struct {
	visitors map[string]*visitor
	mu       sync.Mutex
	config   RateLimiterConfig
	stopChan chan struct{}
	wg       sync.WaitGroup
}

var (
	rateLimiterManager *RateLimiterManager
	rateLimiterOnce    sync.Once
)

// GetRateLimiterManager returns a singleton rate limiter manager
func GetRateLimiterManager(config RateLimiterConfig) *RateLimiterManager {
	rateLimiterOnce.Do(func() {
		rateLimiterManager = &RateLimiterManager{
			visitors: make(map[string]*visitor),
			config:   config,
			stopChan: make(chan struct{}),
		}
		// Start cleanup goroutine ONCE
		rateLimiterManager.wg.Add(1)
		go rateLimiterManager.cleanupLoop()
	})
	return rateLimiterManager
}

// Stop gracefully stops the rate limiter manager
func (m *RateLimiterManager) Stop() {
	close(m.stopChan)
	m.wg.Wait()
}

func (m *RateLimiterManager) cleanupLoop() {
	defer m.wg.Done()
	ticker := time.NewTicker(m.config.CleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.cleanupVisitors()
		case <-m.stopChan:
			return
		}
	}
}

func (m *RateLimiterManager) cleanupVisitors() {
	m.mu.Lock()
	defer m.mu.Unlock()

	for ip, v := range m.visitors {
		if time.Since(v.lastSeen) > m.config.VisitorExpiry {
			delete(m.visitors, ip)
		}
	}
}

func (m *RateLimiterManager) getVisitor(ip string) *rate.Limiter {
	m.mu.Lock()
	defer m.mu.Unlock()

	v, exists := m.visitors[ip]
	if !exists {
		limiter := rate.NewLimiter(rate.Limit(m.config.RequestsPerSecond), m.config.Burst)
		m.visitors[ip] = &visitor{limiter: limiter, lastSeen: time.Now()}
		return limiter
	}

	v.lastSeen = time.Now()
	return v.limiter
}

// RateLimiter returns a rate limiting middleware
func RateLimiter(next http.Handler) http.Handler {
	manager := GetRateLimiterManager(DefaultRateLimiterConfig())

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ip := getRealIP(r)
		limiter := manager.getVisitor(ip)

		if !limiter.Allow() {
			http.Error(w, "Rate limit exceeded", http.StatusTooManyRequests)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// RateLimiterWithConfig returns a rate limiting middleware with custom config
func RateLimiterWithConfig(config RateLimiterConfig) func(http.Handler) http.Handler {
	manager := GetRateLimiterManager(config)

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ip := getRealIP(r)
			limiter := manager.getVisitor(ip)

			if !limiter.Allow() {
				http.Error(w, "Rate limit exceeded", http.StatusTooManyRequests)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

// getRealIP extracts the real client IP from request
func getRealIP(r *http.Request) string {
	// Check X-Forwarded-For header (reverse proxy)
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		// Take the first IP in the chain
		ips := splitAndTrim(xff, ",")
		if len(ips) > 0 {
			return ips[0]
		}
	}

	// Check X-Real-IP header
	if xri := r.Header.Get("X-Real-IP"); xri != "" {
		return xri
	}

	// Fallback to RemoteAddr
	return r.RemoteAddr
}

func splitAndTrim(s, sep string) []string {
	parts := make([]string, 0)
	for _, part := range splitString(s, sep) {
		trimmed := trimSpace(part)
		if trimmed != "" {
			parts = append(parts, trimmed)
		}
	}
	return parts
}

func splitString(s, sep string) []string {
	var result []string
	start := 0
	for i := 0; i <= len(s)-len(sep); i++ {
		if s[i:i+len(sep)] == sep {
			result = append(result, s[start:i])
			start = i + len(sep)
			i += len(sep) - 1
		}
	}
	result = append(result, s[start:])
	return result
}

func trimSpace(s string) string {
	start := 0
	for start < len(s) && (s[start] == ' ' || s[start] == '\t' || s[start] == '\n' || s[start] == '\r') {
		start++
	}
	end := len(s)
	for end > start && (s[end-1] == ' ' || s[end-1] == '\t' || s[end-1] == '\n' || s[end-1] == '\r') {
		end--
	}
	return s[start:end]
}

// RequestBodyLimiter limits the size of request bodies
func RequestBodyLimiter(maxBytes int64) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Limit request body size
			r.Body = http.MaxBytesReader(w, r.Body, maxBytes)
			next.ServeHTTP(w, r)
		})
	}
}

// Timeout adds a timeout to the request context
func Timeout(timeout time.Duration) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx, cancel := context.WithTimeout(r.Context(), timeout)
			defer cancel()

			r = r.WithContext(ctx)
			next.ServeHTTP(w, r)
		})
	}
}
