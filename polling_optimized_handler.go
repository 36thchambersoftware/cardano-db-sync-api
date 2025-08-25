package main

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Polling-optimized address handler
// Perfect for 1-minute polling scenarios
func getAddressPollingHandler(w http.ResponseWriter, r *http.Request) {
	address := strings.TrimPrefix(r.URL.Path, "/addresses/")
	if address == "" {
		writeError(w, http.StatusBadRequest, "Address required")
		return
	}

	// Very short cache for balance data (10 seconds)
	// Ensures fresh data every minute while avoiding unnecessary DB hits
	cacheKey := "poll_addr_" + address
	if cached, found := getCachedResponse(cacheKey); found {
		w.Header().Set("Content-Type", "application/json")
		w.Write(cached)
		return
	}

	// Get address ID with longer cache (addresses don't change)
	addressID, err := getAddressIDCached(address)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to find address")
		return
	}

	// Fast query using direct address_id
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

	response := map[string]interface{}{
		"address": address,
		"balance": map[string]interface{}{
			"ada": map[string]interface{}{
				"lovelace":  strconv.FormatInt(adaBalance, 10),
				"ada":       float64(adaBalance) / 1000000.0,
			},
		},
		"utxo_count": 0, // Could add this if needed
		"timestamp":  time.Now().Unix(),
		"cached":     false,
	}

	data, _ := json.Marshal(response)
	
	// 10-second cache - perfect for 1-minute polling
	setCachedResponse(cacheKey, data, 10*time.Second)
	
	writeJSON(w, response)
}

// No-cache version for real-time data (when you need absolute freshness)
func getAddressRealTimeHandler(w http.ResponseWriter, r *http.Request) {
	address := strings.TrimPrefix(r.URL.Path, "/addresses/")
	if address == "" {
		writeError(w, http.StatusBadRequest, "Address required")
		return
	}

	// Still use address ID cache (these don't change)
	addressID, err := getAddressIDCached(address)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to find address")
		return
	}

	// Real-time balance query (no caching)
	query := `
		SELECT COALESCE(SUM(txo.value), 0) as ada_balance,
		       COUNT(*) as utxo_count
		FROM tx_out txo
		INNER JOIN tx ON txo.tx_id = tx.id
		WHERE txo.address_id = $1
		  AND txo.consumed_by_tx_id IS NULL
		  AND tx.valid_contract = true
	`

	var adaBalance int64
	var utxoCount int64
	err = db.QueryRowContext(ctx, query, addressID).Scan(&adaBalance, &utxoCount)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to query address balance")
		return
	}

	response := map[string]interface{}{
		"address": address,
		"balance": map[string]interface{}{
			"ada": map[string]interface{}{
				"lovelace": strconv.FormatInt(adaBalance, 10),
				"ada":      float64(adaBalance) / 1000000.0,
			},
		},
		"utxo_count": utxoCount,
		"timestamp":  time.Now().Unix(),
		"cached":     false,
		"real_time":  true,
	}

	writeJSON(w, response)
}

// Batch address handler for polling multiple addresses efficiently
func getAddressesBatchHandler(w http.ResponseWriter, r *http.Request) {
	// Parse addresses from query parameter: ?addresses=addr1,addr2,addr3
	addressesParam := r.URL.Query().Get("addresses")
	if addressesParam == "" {
		writeError(w, http.StatusBadRequest, "addresses parameter required")
		return
	}

	addresses := strings.Split(addressesParam, ",")
	if len(addresses) > 50 { // Limit batch size
		writeError(w, http.StatusBadRequest, "Maximum 50 addresses per batch")
		return
	}

	// Get all address IDs first (with caching)
	addressIDs := make(map[string]int64)
	for _, addr := range addresses {
		addr = strings.TrimSpace(addr)
		if addr == "" {
			continue
		}
		
		if id, err := getAddressIDCached(addr); err == nil {
			addressIDs[addr] = id
		}
	}

	if len(addressIDs) == 0 {
		writeError(w, http.StatusBadRequest, "No valid addresses found")
		return
	}

	// Build batch query for all addresses
	addressIDList := make([]interface{}, 0, len(addressIDs))
	addressIDToAddr := make(map[int64]string)
	
	for addr, id := range addressIDs {
		addressIDList = append(addressIDList, id)
		addressIDToAddr[id] = addr
	}

	// Efficient batch query
	query := `
		SELECT txo.address_id, 
		       COALESCE(SUM(txo.value), 0) as ada_balance,
		       COUNT(*) as utxo_count
		FROM tx_out txo
		INNER JOIN tx ON txo.tx_id = tx.id
		WHERE txo.address_id = ANY($1::bigint[])
		  AND txo.consumed_by_tx_id IS NULL
		  AND tx.valid_contract = true
		GROUP BY txo.address_id
	`

	rows, err := db.QueryContext(ctx, query, addressIDList)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to query address balances")
		return
	}
	defer rows.Close()

	results := make(map[string]interface{})
	timestamp := time.Now().Unix()

	for rows.Next() {
		var addressID, adaBalance, utxoCount int64
		if err := rows.Scan(&addressID, &adaBalance, &utxoCount); err != nil {
			continue
		}

		if addr, exists := addressIDToAddr[addressID]; exists {
			results[addr] = map[string]interface{}{
				"balance": map[string]interface{}{
					"ada": map[string]interface{}{
						"lovelace": strconv.FormatInt(adaBalance, 10),
						"ada":      float64(adaBalance) / 1000000.0,
					},
				},
				"utxo_count": utxoCount,
				"timestamp":  timestamp,
			}
		}
	}

	// Add entries for addresses with zero balance
	for addr := range addressIDs {
		if _, exists := results[addr]; !exists {
			results[addr] = map[string]interface{}{
				"balance": map[string]interface{}{
					"ada": map[string]interface{}{
						"lovelace": "0",
						"ada":      0.0,
					},
				},
				"utxo_count": 0,
				"timestamp":  timestamp,
			}
		}
	}

	response := map[string]interface{}{
		"addresses": results,
		"count":     len(results),
		"timestamp": timestamp,
	}

	writeJSON(w, response)
}