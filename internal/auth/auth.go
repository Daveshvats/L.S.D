package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"golang.org/x/crypto/bcrypt"
)

// Configuration
const (
	AccessTokenExpiry  = 15 * time.Minute
	RefreshTokenExpiry = 7 * 24 * time.Hour
	APIKeyPrefix       = "lsd_live_"
	BcryptCost         = 12
	MaxPasswordLength  = 128 // Prevent DoS via memory exhaustion
)

// TokenType represents the type of JWT token
type TokenType string

const (
	AccessToken  TokenType = "access"
	RefreshToken TokenType = "refresh"
)

// APIKeyScope represents available API key scopes
type APIKeyScope string

const (
	ScopeRead     APIKeyScope = "read"
	ScopeWrite    APIKeyScope = "write"
	ScopeSearch   APIKeyScope = "search"
	ScopePipeline APIKeyScope = "pipeline"
	ScopeAdmin    APIKeyScope = "admin"
)

// Common password prefixes that should be rejected
var commonPasswords = map[string]bool{
	"password": true, "123456": true, "12345678": true, "qwerty": true,
	"abc123": true, "monkey": true, "master": true, "dragon": true,
	"letmein": true, "login": true, "princess": true, "admin": true,
	"welcome": true, "password1": true, "password123": true,
}

// JWTClaims represents the claims in a JWT token
type JWTClaims struct {
	UserID    string     `json:"user_id"`
	Email     string     `json:"email"`
	Username  string     `json:"username"`
	Role      string     `json:"role"`
	Scopes    []string   `json:"scopes"`
	TokenType TokenType  `json:"token_type"` // SECURITY: Distinguish access vs refresh
	jwt.RegisteredClaims
}

// RefreshTokenClaims represents claims in a refresh token
type RefreshTokenClaims struct {
	UserID    string    `json:"user_id"`
	TokenType TokenType `json:"token_type"`
	jwt.RegisteredClaims
}

// TokenValidationError represents token validation errors
type TokenValidationError struct {
	Reason string
}

func (e *TokenValidationError) Error() string {
	return e.Reason
}

// AuthService handles authentication operations
type AuthService struct {
	jwtSecret []byte
}

// NewAuthService creates a new AuthService
func NewAuthService(jwtSecret string) *AuthService {
	if len(jwtSecret) < 32 {
		log.Println("⚠️  WARNING: JWT secret should be at least 32 characters")
	}
	return &AuthService{
		jwtSecret: []byte(jwtSecret),
	}
}

// HashPassword generates a bcrypt hash
func (s *AuthService) HashPassword(password string) (string, error) {
	// SECURITY: Limit password length to prevent DoS
	if len(password) > MaxPasswordLength {
		return "", fmt.Errorf("password exceeds maximum length of %d characters", MaxPasswordLength)
	}
	bytes, err := bcrypt.GenerateFromPassword([]byte(password), BcryptCost)
	return string(bytes), err
}

// CheckPasswordHash compares password with hash
func (s *AuthService) CheckPasswordHash(password, hash string) bool {
	// SECURITY: Limit password length for comparison too
	if len(password) > MaxPasswordLength {
		return false
	}
	err := bcrypt.CompareHashAndPassword([]byte(hash), []byte(password))
	return err == nil
}

// GenerateAccessToken creates a JWT access token
func (s *AuthService) GenerateAccessToken(userID, email, username, role string, scopes []string) (string, error) {
	now := time.Now()
	claims := JWTClaims{
		UserID:    userID,
		Email:     email,
		Username:  username,
		Role:      role,
		Scopes:    scopes,
		TokenType: AccessToken, // SECURITY: Mark as access token
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(now.Add(AccessTokenExpiry)),
			NotBefore: jwt.NewNumericDate(now), // SECURITY: Prevent immediate use issues
			IssuedAt:  jwt.NewNumericDate(now),
			Issuer:    "lsd-api",
			Audience:  []string{"lsd-api-users"},
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString(s.jwtSecret)
}

// GenerateRefreshToken creates a JWT refresh token
func (s *AuthService) GenerateRefreshToken(userID string) (string, error) {
	now := time.Now()
	claims := RefreshTokenClaims{
		UserID:    userID,
		TokenType: RefreshToken, // SECURITY: Mark as refresh token
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(now.Add(RefreshTokenExpiry)),
			NotBefore: jwt.NewNumericDate(now),
			IssuedAt:  jwt.NewNumericDate(now),
			Issuer:    "lsd-api",
			Audience:  []string{"lsd-api-refresh"},
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString(s.jwtSecret)
}

// VerifyAccessToken validates an access token and returns claims
func (s *AuthService) VerifyAccessToken(tokenString string) (*JWTClaims, error) {
	token, err := jwt.ParseWithClaims(tokenString, &JWTClaims{}, func(token *jwt.Token) (interface{}, error) {
		// SECURITY: Validate signing method to prevent algorithm confusion attacks
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return s.jwtSecret, nil
	})

	if err != nil {
		return nil, err
	}

	claims, ok := token.Claims.(*JWTClaims)
	if !ok || !token.Valid {
		return nil, &TokenValidationError{Reason: "invalid token claims"}
	}

	// SECURITY: Verify this is actually an access token
	if claims.TokenType != AccessToken {
		return nil, &TokenValidationError{Reason: "token is not an access token"}
	}

	// Validate issuer
	if claims.Issuer != "lsd-api" {
		return nil, &TokenValidationError{Reason: "invalid token issuer"}
	}

	return claims, nil
}

// VerifyRefreshToken validates a refresh token and returns the user ID
func (s *AuthService) VerifyRefreshToken(tokenString string) (string, error) {
	token, err := jwt.ParseWithClaims(tokenString, &RefreshTokenClaims{}, func(token *jwt.Token) (interface{}, error) {
		// SECURITY: Validate signing method
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return s.jwtSecret, nil
	})

	if err != nil {
		return "", err
	}

	claims, ok := token.Claims.(*RefreshTokenClaims)
	if !ok || !token.Valid {
		return "", &TokenValidationError{Reason: "invalid token claims"}
	}

	// SECURITY: Verify this is actually a refresh token
	if claims.TokenType != RefreshToken {
		return "", &TokenValidationError{Reason: "token is not a refresh token"}
	}

	// Validate issuer
	if claims.Issuer != "lsd-api" {
		return "", &TokenValidationError{Reason: "invalid token issuer"}
	}

	return claims.UserID, nil
}

// GenerateAPIKey creates a new API key
// SECURITY: Now returns error for random generation failure
func (s *AuthService) GenerateAPIKey() (key, keyHash, keyPrefix string, err error) {
	// Generate random bytes
	randomBytes := make([]byte, 24)
	
	// SECURITY: Check for crypto/rand errors
	n, err := rand.Read(randomBytes)
	if err != nil {
		return "", "", "", fmt.Errorf("failed to generate random bytes: %w", err)
	}
	if n != 24 {
		return "", "", "", fmt.Errorf("failed to generate sufficient random bytes: got %d, want 24", n)
	}
	
	randomPart := hex.EncodeToString(randomBytes)
	key = APIKeyPrefix + randomPart

	// Hash for storage using SHA256
	// Note: For API keys, SHA256 is acceptable because keys are high-entropy
	// Unlike passwords, we don't need bcrypt because brute-force is impractical
	hash := sha256.Sum256([]byte(key))
	keyHash = hex.EncodeToString(hash[:])

	// Prefix for identification (safe to expose)
	keyPrefix = key[:16]

	return key, keyHash, keyPrefix, nil
}

// HashAPIKey hashes an API key for verification
func (s *AuthService) HashAPIKey(key string) string {
	hash := sha256.Sum256([]byte(key))
	return hex.EncodeToString(hash[:])
}

// HasScope checks if a scope list contains a required scope
func HasScope(scopes []string, required string) bool {
	// Admin has all permissions
	for _, s := range scopes {
		if s == string(ScopeAdmin) {
			return true
		}
		if s == required {
			return true
		}
	}
	return false
}

// ValidateScopes validates that all provided scopes are valid
func ValidateScopes(scopes []string) (valid []string, invalid []string) {
	validScopes := map[string]bool{
		string(ScopeRead):     true,
		string(ScopeWrite):    true,
		string(ScopeSearch):   true,
		string(ScopePipeline): true,
		string(ScopeAdmin):    true,
	}

	for _, scope := range scopes {
		if validScopes[scope] {
			valid = append(valid, scope)
		} else {
			invalid = append(invalid, scope)
		}
	}

	return valid, invalid
}

// ValidatePasswordStrength checks if password meets requirements
func ValidatePasswordStrength(password string) (valid bool, validationErrors []string) {
	// Check minimum length
	if len(password) < 8 {
		validationErrors = append(validationErrors, "Password must be at least 8 characters")
	}

	// SECURITY: Check maximum length to prevent DoS
	if len(password) > MaxPasswordLength {
		validationErrors = append(validationErrors, fmt.Sprintf("Password must be less than %d characters", MaxPasswordLength))
		return false, validationErrors
	}

	// Check for common passwords (case insensitive)
	lowerPass := strings.ToLower(password)
	if commonPasswords[lowerPass] {
		validationErrors = append(validationErrors, "Password is too common, please choose a stronger password")
	}

	hasUpper := false
	hasLower := false
	hasNumber := false
	hasSpecial := false

	for _, char := range password {
		switch {
		case 'A' <= char && char <= 'Z':
			hasUpper = true
		case 'a' <= char && char <= 'z':
			hasLower = true
		case '0' <= char && char <= '9':
			hasNumber = true
		case char >= 32 && char <= 126:
			hasSpecial = true
		}
	}

	if !hasUpper {
		validationErrors = append(validationErrors, "Password must contain at least one uppercase letter")
	}
	if !hasLower {
		validationErrors = append(validationErrors, "Password must contain at least one lowercase letter")
	}
	if !hasNumber {
		validationErrors = append(validationErrors, "Password must contain at least one number")
	}
	if !hasSpecial {
		validationErrors = append(validationErrors, "Password must contain at least one special character")
	}

	return len(validationErrors) == 0, validationErrors
}

// IsTokenRevoked checks if a token has been revoked
// This is a placeholder - implement with Redis for production
func (s *AuthService) IsTokenRevoked(tokenID string) bool {
	// TODO: Implement with Redis token blacklist
	return false
}

// RevokeToken adds a token to the revocation list
// This is a placeholder - implement with Redis for production
func (s *AuthService) RevokeToken(tokenID string, expiry time.Time) error {
	// TODO: Implement with Redis token blacklist
	return nil
}

// GenerateSecureToken generates a cryptographically secure random token
func GenerateSecureToken(length int) (string, error) {
	if length <= 0 {
		return "", errors.New("length must be positive")
	}
	bytes := make([]byte, length)
	n, err := rand.Read(bytes)
	if err != nil {
		return "", fmt.Errorf("failed to generate random bytes: %w", err)
	}
	if n != length {
		return "", fmt.Errorf("failed to generate sufficient random bytes")
	}
	return hex.EncodeToString(bytes), nil
}
