package handlers

import (
        "crypto/rand"
        "encoding/hex"
        "encoding/json"
        "fmt"
        "log"
        "net"
        "net/http"
        "regexp"
        "strings"
        "time"

        "highperf-api/internal/auth"
        "highperf-api/internal/middleware"

        "github.com/jackc/pgx/v5/pgxpool"
)

type AuthHandler struct {
        db          *pgxpool.Pool
        authService *auth.AuthService
}

func NewAuthHandler(db *pgxpool.Pool, authService *auth.AuthService) *AuthHandler {
        return &AuthHandler{
                db:          db,
                authService: authService,
        }
}

// LoginRequest represents the login request body
type LoginRequest struct {
        Identifier string `json:"identifier"` // email or username
        Password   string `json:"password"`
        RememberMe bool   `json:"rememberMe"`
}

// RegisterRequest represents the registration request body
type RegisterRequest struct {
        Username string `json:"username"`
        Email    string `json:"email"`
        Password string `json:"password"`
        Name     string `json:"name"`
}

// CreateAPIKeyRequest represents the API key creation request
type CreateAPIKeyRequest struct {
        Name        string   `json:"name"`
        Description string   `json:"description"`
        Scopes      []string `json:"scopes"`
        RateLimit   int      `json:"rateLimit"`
}

// Login handles user login
func (h *AuthHandler) Login(w http.ResponseWriter, r *http.Request) {
        var req LoginRequest
        if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
                http.Error(w, `{"error": "Invalid request"}`, http.StatusBadRequest)
                return
        }

        // Find user by email or username
        var id int
        var role, hash string
        var email, username, name string
        // var apiKey *string // nullable for backward compatibility

        // Try with api_key column first (new schema), fallback without (old schema)
        query := `
                SELECT id, email, username, COALESCE(role, 'user') as role, password_hash, name
                FROM users 
                WHERE email = $1 OR username = $1
        `
        err := h.db.QueryRow(r.Context(), query, req.Identifier).Scan(&id, &email, &username, &role, &hash, &name)
        if err != nil {
                http.Error(w, `{"error": "Invalid credentials"}`, http.StatusUnauthorized)
                return
        }

        // Verify password
        if !h.authService.CheckPasswordHash(req.Password, hash) {
                http.Error(w, `{"error": "Invalid credentials"}`, http.StatusUnauthorized)
                return
        }

        // Update last login (ignore error if column doesn't exist)
        h.db.Exec(r.Context(), "UPDATE users SET last_login_at = NOW() WHERE id = $1", id)

        // Determine scopes based on role
        scopes := []string{"read", "search"}
        if role == "admin" {
                scopes = []string{"read", "write", "search", "pipeline", "admin"}
        }

        // Generate tokens
        userID := fmt.Sprintf("%d", id)
        accessToken, err := h.authService.GenerateAccessToken(userID, email, username, role, scopes)
        if err != nil {
                http.Error(w, `{"error": "Failed to generate token"}`, http.StatusInternalServerError)
                return
        }

        refreshToken, err := h.authService.GenerateRefreshToken(userID)
        if err != nil {
                http.Error(w, `{"error": "Failed to generate token"}`, http.StatusInternalServerError)
                return
        }

        // Store session (ignore errors if sessions table doesn't exist)
        sessionID := generateID()
        sessionQuery := `
                INSERT INTO sessions (id, user_id, refresh_token_hash, user_agent, ip_address, expires_at)
                VALUES ($1, $2, $3, $4, $5, $6)
        `
        refreshTokenHash := h.authService.HashAPIKey(refreshToken)
        expiresAt := time.Now().Add(7 * 24 * time.Hour)
        if req.RememberMe {
                expiresAt = time.Now().Add(30 * 24 * time.Hour)
        }

        h.db.Exec(r.Context(), sessionQuery,
                sessionID, id, refreshTokenHash,
                r.Header.Get("User-Agent"), getClientIP(r), expiresAt,
        )

        // Set cookies
        cookieExpiry := 7 * 24 * time.Hour
        if req.RememberMe {
                cookieExpiry = 30 * 24 * time.Hour
        }

        http.SetCookie(w, &http.Cookie{
                Name:     "access_token",
                Value:    accessToken,
                Path:     "/",
                Expires:  time.Now().Add(15 * time.Minute),
                HttpOnly: true,
                Secure:   r.TLS != nil,
                SameSite: http.SameSiteLaxMode,
        })

        http.SetCookie(w, &http.Cookie{
                Name:     "refresh_token",
                Value:    refreshToken,
                Path:     "/",
                Expires:  time.Now().Add(cookieExpiry),
                HttpOnly: true,
                Secure:   r.TLS != nil,
                SameSite: http.SameSiteLaxMode,
        })

        // Response
        w.Header().Set("Content-Type", "application/json")
        json.NewEncoder(w).Encode(map[string]interface{}{
                "success":     true,
                "accessToken": accessToken,
                "user": map[string]string{
                        "id":       userID,
                        "email":    email,
                        "username": username,
                        "name":     name,
                        "role":     role,
                },
        })
}

// Register handles user registration
func (h *AuthHandler) Register(w http.ResponseWriter, r *http.Request) {
        var req RegisterRequest
        if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
                http.Error(w, `{"error": "Invalid request"}`, http.StatusBadRequest)
                return
        }

        // Validate input
        if req.Email == "" || req.Username == "" || req.Password == "" {
                http.Error(w, `{"error": "Email, username, and password are required"}`, http.StatusBadRequest)
                return
        }

        // Validate email
        emailRegex := regexp.MustCompile(`^[^\s@]+@[^\s@]+\.[^\s@]+$`)
        if !emailRegex.MatchString(req.Email) {
                http.Error(w, `{"error": "Invalid email format"}`, http.StatusBadRequest)
                return
        }

        // Validate username
        usernameRegex := regexp.MustCompile(`^[a-zA-Z0-9_]{3,30}$`)
        if !usernameRegex.MatchString(req.Username) {
                http.Error(w, `{"error": "Username must be 3-30 characters, letters, numbers, underscores only"}`, http.StatusBadRequest)
                return
        }

        // Validate password
        valid, errors := auth.ValidatePasswordStrength(req.Password)
        if !valid {
                w.Header().Set("Content-Type", "application/json")
                w.WriteHeader(http.StatusBadRequest)
                json.NewEncoder(w).Encode(map[string]interface{}{
                        "error":   "Password does not meet requirements",
                        "details": errors,
                })
                return
        }

        // Check if user exists
        var exists bool
        h.db.QueryRow(r.Context(),
                "SELECT EXISTS(SELECT 1 FROM users WHERE email = $1 OR username = $2)",
                req.Email, req.Username,
        ).Scan(&exists)
        if exists {
                http.Error(w, `{"error": "Email or username already registered"}`, http.StatusConflict)
                return
        }

        // Hash password
        hash, err := h.authService.HashPassword(req.Password)
        if err != nil {
                http.Error(w, `{"error": "Failed to create account"}`, http.StatusInternalServerError)
                return
        }

        // Generate verification token
        verificationToken := generateToken()

        // Create user
        var userID int
        err = h.db.QueryRow(r.Context(), `
                INSERT INTO users (email, username, password_hash, name, verification_token, role)
                VALUES ($1, $2, $3, $4, $5, 'user')
                RETURNING id
        `, req.Email, req.Username, hash, req.Name, verificationToken).Scan(&userID)
        if err != nil {
                http.Error(w, `{"error": "Failed to create account"}`, http.StatusInternalServerError)
                return
        }

        // Generate tokens
        accessToken, _ := h.authService.GenerateAccessToken(fmt.Sprintf("%d", userID), req.Email, req.Username, "user", []string{"read", "search"})
        refreshToken, _ := h.authService.GenerateRefreshToken(fmt.Sprintf("%d", userID))

        // Set cookies
        http.SetCookie(w, &http.Cookie{
                Name:     "access_token",
                Value:    accessToken,
                Path:     "/",
                Expires:  time.Now().Add(15 * time.Minute),
                HttpOnly: true,
                Secure:   r.TLS != nil,
                SameSite: http.SameSiteLaxMode,
        })

        http.SetCookie(w, &http.Cookie{
                Name:     "refresh_token",
                Value:    refreshToken,
                Path:     "/",
                Expires:  time.Now().Add(7 * 24 * time.Hour),
                HttpOnly: true,
                Secure:   r.TLS != nil,
                SameSite: http.SameSiteLaxMode,
        })

        w.Header().Set("Content-Type", "application/json")
        json.NewEncoder(w).Encode(map[string]interface{}{
                "success":     true,
                "message":     "Registration successful",
                "accessToken": accessToken,
                "user": map[string]string{
                        "id":       fmt.Sprintf("%d", userID),
                        "email":    req.Email,
                        "username": req.Username,
                },
        })
}

// Logout handles user logout
func (h *AuthHandler) Logout(w http.ResponseWriter, r *http.Request) {
        // Get refresh token from cookie
        cookie, err := r.Cookie("refresh_token")
        if err == nil {
                // Revoke session
                refreshTokenHash := h.authService.HashAPIKey(cookie.Value)
                h.db.Exec(r.Context(), "UPDATE sessions SET revoked = true WHERE refresh_token_hash = $1", refreshTokenHash)
        }

        // Clear cookies
        http.SetCookie(w, &http.Cookie{
                Name:     "access_token",
                Value:    "",
                Path:     "/",
                MaxAge:   -1,
                HttpOnly: true,
        })

        http.SetCookie(w, &http.Cookie{
                Name:     "refresh_token",
                Value:    "",
                Path:     "/",
                MaxAge:   -1,
                HttpOnly: true,
        })

        w.Header().Set("Content-Type", "application/json")
        json.NewEncoder(w).Encode(map[string]interface{}{
                "success": true,
                "message": "Logged out successfully",
        })
}

// GetMe returns current user info
func (h *AuthHandler) GetMe(w http.ResponseWriter, r *http.Request) {
        userCtx := middleware.GetUserContext(r.Context())
        if userCtx == nil {
                http.Error(w, `{"error": "Unauthorized"}`, http.StatusUnauthorized)
                return
        }

        // Get full user info from DB
        var email, username, name, role string
        var createdAt time.Time
        err := h.db.QueryRow(r.Context(), `
                SELECT email, username, name, role, created_at 
                FROM users WHERE id = $1
        `, userCtx.UserID).Scan(&email, &username, &name, &role, &createdAt)
        if err != nil {
                http.Error(w, `{"error": "User not found"}`, http.StatusNotFound)
                return
        }

        w.Header().Set("Content-Type", "application/json")
        json.NewEncoder(w).Encode(map[string]interface{}{
                "user": map[string]interface{}{
                        "id":        userCtx.UserID,
                        "email":     email,
                        "username":  username,
                        "name":      name,
                        "role":      role,
                        "createdAt": createdAt,
                        "authType":  userCtx.AuthType,
                },
        })
}

// ListAPIKeys returns user's API keys
func (h *AuthHandler) ListAPIKeys(w http.ResponseWriter, r *http.Request) {
        userCtx := middleware.GetUserContext(r.Context())
        if userCtx == nil {
                http.Error(w, `{"error": "Unauthorized"}`, http.StatusUnauthorized)
                return
        }

        rows, err := h.db.Query(r.Context(), `
                SELECT id, key_prefix, name, description, scopes, rate_limit, last_used_at, created_at, expires_at
                FROM api_keys 
                WHERE user_id = $1 AND revoked = false
                ORDER BY created_at DESC
        `, userCtx.UserID)
        if err != nil {
                http.Error(w, `{"error": "Failed to fetch API keys"}`, http.StatusInternalServerError)
                return
        }
        defer rows.Close()

        var apiKeys []map[string]interface{}
        for rows.Next() {
                var id, keyPrefix, name, scopesJSON string
                var description *string
                var rateLimit int
                var lastUsedAt, expiresAt *time.Time
                var createdAt time.Time

                rows.Scan(&id, &keyPrefix, &name, &description, &scopesJSON, &rateLimit, &lastUsedAt, &createdAt, &expiresAt)

                var scopes []string
                json.Unmarshal([]byte(scopesJSON), &scopes)

                apiKeys = append(apiKeys, map[string]interface{}{
                        "id":          id,
                        "keyPrefix":   keyPrefix,
                        "name":        name,
                        "description": description,
                        "scopes":      scopes,
                        "rateLimit":   rateLimit,
                        "lastUsedAt":  lastUsedAt,
                        "createdAt":   createdAt,
                        "expiresAt":   expiresAt,
                })
        }

        w.Header().Set("Content-Type", "application/json")
        json.NewEncoder(w).Encode(map[string]interface{}{
                "apiKeys": apiKeys,
        })
}

// CreateAPIKey creates a new API key
func (h *AuthHandler) CreateAPIKey(w http.ResponseWriter, r *http.Request) {
        userCtx := middleware.GetUserContext(r.Context())
        if userCtx == nil {
                http.Error(w, `{"error": "Unauthorized"}`, http.StatusUnauthorized)
                return
        }

        var req CreateAPIKeyRequest
        if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
                http.Error(w, `{"error": "Invalid request"}`, http.StatusBadRequest)
                return
        }

        // Validate scopes
        validScopes, invalidScopes := auth.ValidateScopes(req.Scopes)
        if len(invalidScopes) > 0 {
                http.Error(w, `{"error": "Invalid scopes: " + strings.Join(invalidScopes, ", ")}`, http.StatusBadRequest)
                return
        }

        // Default rate limit
        if req.RateLimit == 0 {
                req.RateLimit = 1000
        }

        // Generate API key
        key, keyHash, keyPrefix, err := h.authService.GenerateAPIKey()
        if err != nil {
                log.Printf("Failed to generate API key: %v", err)
                http.Error(w, `{"error": "Failed to generate API key"}`, http.StatusInternalServerError)
                return
        }
        scopesJSON, _ := json.Marshal(validScopes)

        // Insert into database
        apiKeyID := generateID()
        _, err = h.db.Exec(r.Context(), `
                INSERT INTO api_keys (id, user_id, key_hash, key_prefix, name, description, scopes, rate_limit)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        `, apiKeyID, userCtx.UserID, keyHash, keyPrefix, req.Name, req.Description, string(scopesJSON), req.RateLimit)
        if err != nil {
                http.Error(w, `{"error": "Failed to create API key"}`, http.StatusInternalServerError)
                return
        }

        w.Header().Set("Content-Type", "application/json")
        json.NewEncoder(w).Encode(map[string]interface{}{
                "success": true,
                "message": "API key created successfully",
                "apiKey": map[string]interface{}{
                        "id":        apiKeyID,
                        "key":       key, // Only shown once!
                        "keyPrefix": keyPrefix,
                        "name":      req.Name,
                        "scopes":    validScopes,
                        "rateLimit": req.RateLimit,
                },
                "warning": "Store this API key securely. It will not be shown again.",
        })
}

// RevokeAPIKey revokes an API key
func (h *AuthHandler) RevokeAPIKey(w http.ResponseWriter, r *http.Request) {
        userCtx := middleware.GetUserContext(r.Context())
        if userCtx == nil {
                http.Error(w, `{"error": "Unauthorized"}`, http.StatusUnauthorized)
                return
        }

        // Get API key ID from URL
        keyID := r.PathValue("id")
        if keyID == "" {
                http.Error(w, `{"error": "API key ID required"}`, http.StatusBadRequest)
                return
        }

        // Revoke the key (only if it belongs to the user)
        result, err := h.db.Exec(r.Context(), `
                UPDATE api_keys SET revoked = true 
                WHERE id = $1 AND user_id = $2 AND revoked = false
        `, keyID, userCtx.UserID)
        if err != nil {
                http.Error(w, `{"error": "Failed to revoke API key"}`, http.StatusInternalServerError)
                return
        }

        if result.RowsAffected() == 0 {
                http.Error(w, `{"error": "API key not found or already revoked"}`, http.StatusNotFound)
                return
        }

        w.Header().Set("Content-Type", "application/json")
        json.NewEncoder(w).Encode(map[string]interface{}{
                "success": true,
                "message": "API key revoked successfully",
        })
}

// Helper functions

// generateID generates a secure random ID
// SECURITY: Now handles random read errors
func generateID() string {
        bytes := make([]byte, 16)
        n, err := rand.Read(bytes)
        if err != nil || n != 16 {
                // SECURITY: Log error but still return something usable
                // In production, you might want to panic or return error
                log.Printf("CRITICAL: Failed to generate secure random ID: %v", err)
                // Use timestamp fallback (not ideal but better than nothing)
                return fmt.Sprintf("fallback-%d-%d", time.Now().UnixNano(), n)
        }
        return hex.EncodeToString(bytes)
}

// generateToken generates a secure random token
// SECURITY: Now handles random read errors
func generateToken() string {
        bytes := make([]byte, 32)
        n, err := rand.Read(bytes)
        if err != nil || n != 32 {
                log.Printf("CRITICAL: Failed to generate secure random token: %v", err)
                return fmt.Sprintf("fallback-%d-%d", time.Now().UnixNano(), n)
        }
        return hex.EncodeToString(bytes)
}

// getClientIP extracts the client IP from request with validation
// SECURITY: Validates IP addresses from X-Forwarded-For header
func getClientIP(r *http.Request) string {
        // Check X-Forwarded-For header (reverse proxy)
        forwarded := r.Header.Get("X-Forwarded-For")
        if forwarded != "" {
                // X-Forwarded-For can contain multiple IPs: client, proxy1, proxy2
                // Take the first one and validate it
                ips := strings.Split(forwarded, ",")
                if len(ips) > 0 {
                        ip := strings.TrimSpace(ips[0])
                        // SECURITY: Validate IP address format
                        if parsedIP := net.ParseIP(ip); parsedIP != nil {
                                return ip
                        }
                }
        }

        // Check X-Real-IP header
        if realIP := r.Header.Get("X-Real-IP"); realIP != "" {
                if parsedIP := net.ParseIP(realIP); parsedIP != nil {
                        return realIP
                }
        }

        // Fallback to RemoteAddr - extract host from host:port
        host, _, err := net.SplitHostPort(r.RemoteAddr)
        if err != nil {
                return r.RemoteAddr // Return as-is if parsing fails
        }
        return host
}
