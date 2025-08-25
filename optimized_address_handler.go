package main

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Address ID cache to avoid the slow string lookup
var (
	addressIDCache = make(map[string]int64)
	cacheMutex     sync.RWMutex
	cacheExpiry    = make(map[string]time.Time)
)

// Fast address handler with caching
func getAddressOptimizedHandler(w http.ResponseWriter, r *http.Request) {
	address := strings.TrimPrefix(r.URL.Path, "/addresses/")
	if address == "" {
		writeError(w, http.StatusBadRequest, "Address required")
		return
	}

	cacheKey := "addr_balance_" + address
	if cached, found := getCachedResponse(cacheKey); found {
		w.Header().Set("Content-Type", "application/json")
		w.Write(cached)
		return
	}

	// Get address ID with caching
	addressID, err := getAddressIDCached(address)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to find address")
		return
	}

	// Fast query using direct address_id lookup (should be ~116ms)
	query := `
		SELECT COALESCE(SUM(txo.value), 0) as ada_balance
		FROM tx_out txo
		INNER JOIN tx ON txo.tx_id = tx.id
		WHERE txo.address_id = $1
		  AND txo.consumed_by_tx_id IS NULL
		  AND tx.valid_contract = true
	`

	var adaBalance int64
	err = db.QueryRowContext(ctx, query, addressID).Scan(&adaBalance)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to query address balance")
		return
	}

	addressInfo := AddressInfo{
		Address: address,
		Amount: []TransactionAmount{
			{Unit: "lovelace", Quantity: strconv.FormatInt(adaBalance, 10)},
		},
		Type:   "shelley",
		Script: false,
	}

	data, _ := json.Marshal(addressInfo)
	setCachedResponse(cacheKey, data, 15*time.Second) // 15s cache - fresh data every minute
	writeJSON(w, addressInfo)
}

// Cached address ID lookup
func getAddressIDCached(address string) (int64, error) {
	cacheMutex.RLock()
	
	// Check if we have a cached ID and it's not expired
	if id, exists := addressIDCache[address]; exists {
		if expiry, hasExpiry := cacheExpiry[address]; hasExpiry && time.Now().Before(expiry) {
			cacheMutex.RUnlock()
			return id, nil
		}
	}
	cacheMutex.RUnlock()

	// Not in cache or expired, do the slow lookup
	var addressID int64
	err := db.QueryRowContext(ctx, "SELECT id FROM address WHERE address = $1", address).Scan(&addressID)
	if err != nil {
		return 0, err
	}

	// Cache the result for 10 minutes
	cacheMutex.Lock()
	addressIDCache[address] = addressID
	cacheExpiry[address] = time.Now().Add(10 * time.Minute)
	cacheMutex.Unlock()

	return addressID, nil
}

// Ultra-fast handler that skips the valid_contract check (if acceptable)
func getAddressUltraFastHandler(w http.ResponseWriter, r *http.Request) {
	address := strings.TrimPrefix(r.URL.Path, "/addresses/")
	if address == "" {
		writeError(w, http.StatusBadRequest, "Address required")
		return
	}

	addressID, err := getAddressIDCached(address)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to find address")
		return
	}

	// Ultra-fast query without tx join (~50ms based on your tests)
	query := `
		SELECT COALESCE(SUM(value), 0) as ada_balance
		FROM tx_out
		WHERE address_id = $1 AND consumed_by_tx_id IS NULL
	`

	var adaBalance int64
	err = db.QueryRowContext(ctx, query, addressID).Scan(&adaBalance)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to query address balance")
		return
	}

	addressInfo := AddressInfo{
		Address: address,
		Amount: []TransactionAmount{
			{Unit: "lovelace", Quantity: strconv.FormatInt(adaBalance, 10)},
		},
		Type:   "shelley", 
		Script: false,
	}

	writeJSON(w, addressInfo)
}