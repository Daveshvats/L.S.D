package middleware

import (
        "context"
        "encoding/json"
        "log"
        "net/http"
        "strings"
        "time"

        "highperf-api/internal/auth"

        "github.com/jackc/pgx/v5/pgxpool"
)

type ContextKey string

const (
        UserKey    ContextKey = "user"
        APIKeyID   ContextKey = "api_key_id"
        UserScopes ContextKey = "scopes"
)

// User represents authenticated user info in context
type UserContext struct {
        UserID   string
        Username string
        Email    string
        Role     string
        Scopes   []string
        AuthType string // "jwt" or "api_key"
}

// AuthMiddleware handles JWT and API Key authentication
type AuthMiddleware struct {
        authService *auth.AuthService
        db          *pgxpool.Pool
}

// NewAuthMiddleware creates a new AuthMiddleware
func NewAuthMiddleware(authService *auth.AuthService, db *pgxpool.Pool) *AuthMiddleware {
        return &AuthMiddleware{
                authService: authService,
                db:          db,
        }
}

// RequireAuth validates JWT or API Key and injects user context
func (m *AuthMiddleware) RequireAuth(next http.Handler) http.Handler {
        return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
                var userCtx *UserContext

                // 1. Try JWT authentication (Authorization: Bearer token)
                authHeader := r.Header.Get("Authorization")
                if authHeader != "" && strings.HasPrefix(authHeader, "Bearer ") {
                        token := strings.TrimPrefix(authHeader, "Bearer ")
                        claims, err := m.authService.VerifyAccessToken(token)
                        if err == nil {
                                userCtx = &UserContext{
                                        UserID:   claims.UserID,
                                        Username: claims.Username,
                                        Email:    claims.Email,
                                        Role:     claims.Role,
                                        Scopes:   claims.Scopes,
                                        AuthType: "jwt",
                                }
                        }
                }

                // 2. Try API Key authentication (X-API-Key header)
                if userCtx == nil {
                        apiKey := r.Header.Get("X-API-Key")
                        if apiKey != "" {
                                userCtx = m.validateAPIKey(r.Context(), apiKey)
                        }
                }

                // 3. Check cookie for web sessions (fallback)
                if userCtx == nil {
                        cookie, err := r.Cookie("access_token")
                        if err == nil && cookie.Value != "" {
                                claims, err := m.authService.VerifyAccessToken(cookie.Value)
                                if err == nil {
                                        userCtx = &UserContext{
                                                UserID:   claims.UserID,
                                                Username: claims.Username,
                                                Email:    claims.Email,
                                                Role:     claims.Role,
                                                Scopes:   claims.Scopes,
                                                AuthType: "jwt",
                                        }
                                }
                        }
                }

                // 4. No valid authentication found
                if userCtx == nil {
                        w.Header().Set("Content-Type", "application/json")
                        w.WriteHeader(http.StatusUnauthorized)
                        json.NewEncoder(w).Encode(map[string]string{
                                "error": "Authentication required. Provide X-API-Key header or Bearer token.",
                        })
                        return
                }

                // 5. Inject user info into context
                ctx := context.WithValue(r.Context(), UserKey, userCtx)
                next.ServeHTTP(w, r.WithContext(ctx))
        })
}

// OptionalAuth validates auth if present but doesn't require it
func (m *AuthMiddleware) OptionalAuth(next http.Handler) http.Handler {
        return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
                var userCtx *UserContext

                // Try JWT
                authHeader := r.Header.Get("Authorization")
                if authHeader != "" && strings.HasPrefix(authHeader, "Bearer ") {
                        token := strings.TrimPrefix(authHeader, "Bearer ")
                        claims, err := m.authService.VerifyAccessToken(token)
                        if err == nil {
                                userCtx = &UserContext{
                                        UserID:   claims.UserID,
                                        Username: claims.Username,
                                        Email:    claims.Email,
                                        Role:     claims.Role,
                                        Scopes:   claims.Scopes,
                                        AuthType: "jwt",
                                }
                        }
                }

                // Try API Key
                if userCtx == nil {
                        apiKey := r.Header.Get("X-API-Key")
                        if apiKey != "" {
                                userCtx = m.validateAPIKey(r.Context(), apiKey)
                        }
                }

                // Inject context if authenticated
                if userCtx != nil {
                        ctx := context.WithValue(r.Context(), UserKey, userCtx)
                        r = r.WithContext(ctx)
                }

                next.ServeHTTP(w, r)
        })
}

// RequireScope checks if the authenticated user has a specific scope
func RequireScope(scope string) func(http.Handler) http.Handler {
        return func(next http.Handler) http.Handler {
                return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
                        userCtx := GetUserContext(r.Context())
                        if userCtx == nil {
                                w.Header().Set("Content-Type", "application/json")
                                w.WriteHeader(http.StatusUnauthorized)
                                json.NewEncoder(w).Encode(map[string]string{"error": "Unauthorized"})
                                return
                        }

                        if !auth.HasScope(userCtx.Scopes, scope) {
                                w.Header().Set("Content-Type", "application/json")
                                w.WriteHeader(http.StatusForbidden)
                                json.NewEncoder(w).Encode(map[string]string{"error": "Insufficient permissions"})
                                return
                        }

                        next.ServeHTTP(w, r)
                })
        }
}

// GetUserContext retrieves user context from request
func GetUserContext(ctx context.Context) *UserContext {
        if user, ok := ctx.Value(UserKey).(*UserContext); ok {
                return user
        }
        return nil
}

// validateAPIKey validates an API key against the database
func (m *AuthMiddleware) validateAPIKey(ctx context.Context, apiKey string) *UserContext {
        keyHash := m.authService.HashAPIKey(apiKey)

        // Query the database for the API key
        var userID, username, email, role, scopesJSON string
        
        query := `
                SELECT ak.user_id, ak.scopes, u.username, u.email, u.role
                FROM api_keys ak
                JOIN users u ON ak.user_id = u.id
                WHERE ak.key_hash = $1 AND ak.revoked = false
        `
        
        err := m.db.QueryRow(ctx, query, keyHash).Scan(&userID, &scopesJSON, &username, &email, &role)
        if err != nil {
                return nil
        }

        // Parse scopes from JSON
        var scopes []string
        if err := json.Unmarshal([]byte(scopesJSON), &scopes); err != nil {
                scopes = []string{"read"} // default scope
        }

        // Update last_used_at asynchronously with proper error handling
        go func() {
                // SECURITY: Use context with timeout to prevent hanging
                ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
                defer cancel()

                _, err := m.db.Exec(ctx,
                        "UPDATE api_keys SET last_used_at = NOW() WHERE key_hash = $1",
                        keyHash)
                if err != nil {
                        log.Printf("Warning: failed to update API key last_used_at: %v", err)
                }
        }()

        return &UserContext{
                UserID:   userID,
                Username: username,
                Email:    email,
                Role:     role,
                Scopes:   scopes,
                AuthType: "api_key",
        }
}
