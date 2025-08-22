package main

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"log"
	"net/http"
	"os"
	"strings"
)

var validAPIKeys map[string]bool

func init() {
	validAPIKeys = make(map[string]bool)
	loadAPIKeys()
}

// loadAPIKeys loads API keys from environment variable
func loadAPIKeys() {
	apiKeysEnv := os.Getenv("API_KEYS")
	if apiKeysEnv == "" {
		log.Println("⚠️  WARNING: No API_KEYS set. API will be open to public access!")
		log.Println("   Set API_KEYS environment variable with comma-separated keys")
		return
	}

	keys := strings.Split(apiKeysEnv, ",")
	for _, key := range keys {
		key = strings.TrimSpace(key)
		if key != "" {
			validAPIKeys[key] = true
			log.Printf("✅ API key loaded: %s...%s", key[:8], key[len(key)-4:])
		}
	}
	
	log.Printf("🔐 Loaded %d API key(s)", len(validAPIKeys))
}

// generateAPIKey generates a secure random API key
func generateAPIKey() (string, error) {
	bytes := make([]byte, 32) // 256 bits
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}

// validateAPIKey securely compares API keys to prevent timing attacks
func validateAPIKey(provided string) bool {
	// If no API keys are configured, allow access (for development)
	if len(validAPIKeys) == 0 {
		return true
	}

	for validKey := range validAPIKeys {
		if subtle.ConstantTimeCompare([]byte(provided), []byte(validKey)) == 1 {
			return true
		}
	}
	return false
}

// authMiddleware provides API key authentication
func authMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Skip auth for home page and health check
		if r.URL.Path == "/" || r.URL.Path == "/health" {
			next(w, r)
			return
		}

		// Get API key from header or query parameter
		apiKey := r.Header.Get("X-API-Key")
		if apiKey == "" {
			apiKey = r.Header.Get("Authorization")
			if strings.HasPrefix(apiKey, "Bearer ") {
				apiKey = strings.TrimPrefix(apiKey, "Bearer ")
			}
		}
		if apiKey == "" {
			apiKey = r.URL.Query().Get("api_key")
		}

		if apiKey == "" {
			writeError(w, http.StatusUnauthorized, "API key required. Provide via X-API-Key header, Authorization: Bearer token, or api_key query parameter")
			return
		}

		if !validateAPIKey(apiKey) {
			writeError(w, http.StatusForbidden, "Invalid API key")
			return
		}

		// API key is valid, proceed to handler
		next(w, r)
	}
}

// authHandler wraps handlers with authentication
func authHandler(handler http.HandlerFunc) http.HandlerFunc {
	return authMiddleware(handler)
}

// generateKeyHandler provides an endpoint to generate new API keys (for development)
func generateKeyHandler(w http.ResponseWriter, r *http.Request) {
	// Only allow this in development mode
	if os.Getenv("ENVIRONMENT") != "development" {
		writeError(w, http.StatusNotFound, "Not found")
		return
	}

	key, err := generateAPIKey()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to generate API key")
		return
	}

	response := map[string]string{
		"api_key": key,
		"message": "Store this API key securely. Add it to your API_KEYS environment variable.",
	}

	writeJSON(w, response)
}

// healthHandler provides a simple health check endpoint
func healthHandler(w http.ResponseWriter, r *http.Request) {
	response := map[string]interface{}{
		"status": "healthy",
		"authentication": len(validAPIKeys) > 0,
		"timestamp": getCurrentTimestamp(),
	}
	writeJSON(w, response)
}

func getCurrentTimestamp() int64 {
	return getCurrentEpochTime()
}

func getCurrentEpochTime() int64 {
	// This would normally get the current epoch time from the database
	// For simplicity, just return 0 for now
	return 0
}