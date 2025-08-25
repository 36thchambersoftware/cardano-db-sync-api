package main

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Simplified address handler for ultra-fast balance queries
func getAddressSimpleHandler(w http.ResponseWriter, r *http.Request) {
	address := strings.TrimPrefix(r.URL.Path, "/addresses/")
	if address == "" {
		writeError(w, http.StatusBadRequest, "Address required")
		return
	}

	cacheKey := "addr_simple_" + address
	if cached, found := getCachedResponse(cacheKey); found {
		w.Header().Set("Content-Type", "application/json")
		w.Write(cached)
		return
	}

	// Ultra-simple query - just ADA balance first
	query := `
		SELECT COALESCE(SUM(txo.value), 0) as ada_balance
		FROM tx_out txo
		INNER JOIN tx ON txo.tx_id = tx.id
		WHERE txo.address_id = (SELECT id FROM address WHERE address = $1 LIMIT 1)
		  AND txo.consumed_by_tx_id IS NULL
		  AND tx.valid_contract = true
	`

	var adaBalance int64
	err := db.QueryRowContext(ctx, query, address).Scan(&adaBalance)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to query address balance")
		return
	}

	// Simple response with just ADA balance
	addressInfo := AddressInfo{
		Address: address,
		Amount: []TransactionAmount{
			{Unit: "lovelace", Quantity: strconv.FormatInt(adaBalance, 10)},
		},
		Type:   "shelley",
		Script: false,
	}

	data, _ := json.Marshal(addressInfo)
	setCachedResponse(cacheKey, data, 1*time.Minute)
	writeJSON(w, addressInfo)
}

// Fast UTXO count query without asset details
func getAddressUTXOCountHandler(w http.ResponseWriter, r *http.Request) {
	address := strings.TrimPrefix(r.URL.Path, "/addresses/")
	if address == "" {
		writeError(w, http.StatusBadRequest, "Address required")
		return
	}

	// Super fast count query
	query := `
		SELECT COUNT(*) as utxo_count, COALESCE(SUM(txo.value), 0) as total_value
		FROM tx_out txo
		INNER JOIN tx ON txo.tx_id = tx.id  
		WHERE txo.address_id = (SELECT id FROM address WHERE address = $1 LIMIT 1)
		  AND txo.consumed_by_tx_id IS NULL
		  AND tx.valid_contract = true
	`

	var utxoCount, totalValue int64
	err := db.QueryRowContext(ctx, query, address).Scan(&utxoCount, &totalValue)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to query UTXO count")
		return
	}

	response := map[string]interface{}{
		"address":    address,
		"utxo_count": utxoCount,
		"ada_value":  strconv.FormatInt(totalValue, 10),
	}

	writeJSON(w, response)
}

// Get address assets separately (optional, for when assets are actually needed)
func getAddressAssetsHandler(w http.ResponseWriter, r *http.Request) {
	address := strings.TrimPrefix(r.URL.Path, "/addresses/")
	address = strings.TrimSuffix(address, "/assets")
	
	if address == "" {
		writeError(w, http.StatusBadRequest, "Address required")
		return
	}

	// Query only assets (separate from balance for performance)
	query := `
		SELECT json_agg(
			json_build_object(
				'unit', CONCAT(encode(ma.policy, 'hex'), encode(ma.name, 'hex')),
				'quantity', mto.quantity::text
			)
		) as assets
		FROM tx_out txo
		INNER JOIN tx ON txo.tx_id = tx.id
		INNER JOIN ma_tx_out mto ON mto.tx_out_id = txo.id
		INNER JOIN multi_asset ma ON mto.ident = ma.id
		WHERE txo.address_id = (SELECT id FROM address WHERE address = $1)
		  AND txo.consumed_by_tx_id IS NULL
		  AND tx.valid_contract = true
		  AND mto.quantity > 0
	`

	var assetsJSON sql.NullString
	err := db.QueryRowContext(ctx, query, address).Scan(&assetsJSON)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to query address assets")
		return
	}

	var assets []TransactionAmount
	if assetsJSON.Valid && assetsJSON.String != "null" {
		if err := json.Unmarshal([]byte(assetsJSON.String), &assets); err != nil {
			assets = []TransactionAmount{} // Empty if parse fails
		}
	}

	if assets == nil {
		assets = []TransactionAmount{} // Ensure it's not null
	}

	response := map[string]interface{}{
		"address": address,
		"assets":  assets,
	}

	writeJSON(w, response)
}