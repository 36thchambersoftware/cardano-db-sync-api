package main

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Change detection response types
type AddressChange struct {
	Address     string                `json:"address"`
	OldBalance  []TransactionAmount   `json:"old_balance,omitempty"`
	NewBalance  []TransactionAmount   `json:"new_balance"`
	BalanceDiff []TransactionAmount   `json:"balance_diff,omitempty"`
	ChangeType  string                `json:"change_type"` // "new", "increased", "decreased", "unchanged"
	Timestamp   int64                 `json:"timestamp"`
}

type ChangeDetectionResponse struct {
	Changes       []AddressChange `json:"changes"`
	TotalChanges  int             `json:"total_changes"`
	Timestamp     int64           `json:"timestamp"`
	CheckDuration string          `json:"check_duration"`
}

// Get addresses that had activity since a specific timestamp
func getAddressChangesHandler(w http.ResponseWriter, r *http.Request) {
	// Get the "since" parameter (Unix timestamp)
	sinceStr := r.URL.Query().Get("since")
	if sinceStr == "" {
		writeError(w, http.StatusBadRequest, "since parameter required (Unix timestamp)")
		return
	}

	since, err := strconv.ParseInt(sinceStr, 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "Invalid since timestamp")
		return
	}

	// Convert to time for SQL
	sinceTime := time.Unix(since, 0)

	// Query to find addresses with recent activity
	// This identifies addresses that have had new transactions since the timestamp
	query := `
		WITH recent_activity AS (
			SELECT DISTINCT txo.address_id
			FROM tx_out txo
			JOIN tx t ON txo.tx_id = t.id
			JOIN block b ON t.block_id = b.id
			WHERE b.time >= $1
			UNION
			SELECT DISTINCT prev_txo.address_id
			FROM tx_in txi
			JOIN tx t ON txi.tx_in_id = t.id
			JOIN block b ON t.block_id = b.id
			JOIN tx_out prev_txo ON txi.tx_out_id = prev_txo.tx_id AND txi.tx_out_index = prev_txo.index
			WHERE b.time >= $1
		)
		SELECT 
			a.address,
			a.id,
			COALESCE(SUM(txo.value), 0) as current_balance,
			COUNT(txo.*) as utxo_count
		FROM recent_activity ra
		JOIN address a ON ra.address_id = a.id
		LEFT JOIN tx_out txo ON txo.address_id = a.id
		LEFT JOIN tx t ON txo.tx_id = t.id
		WHERE (txo.consumed_by_tx_id IS NULL OR txo.consumed_by_tx_id IS NULL)
		  AND (t.valid_contract = true OR t.valid_contract IS NULL)
		GROUP BY a.address, a.id
		ORDER BY current_balance DESC
		LIMIT 1000
	`

	rows, err := db.QueryContext(ctx, query, sinceTime)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to query address changes")
		return
	}
	defer rows.Close()

	var changes []AddressChange
	timestamp := time.Now().Unix()

	for rows.Next() {
		var address string
		var addressID, currentBalance, utxoCount int64

		err := rows.Scan(&address, &addressID, &currentBalance, &utxoCount)
		if err != nil {
			continue
		}

		change := AddressChange{
			Address: address,
			NewBalance: []TransactionAmount{
				{Unit: "lovelace", Quantity: strconv.FormatInt(currentBalance, 10)},
			},
			ChangeType: "changed", // We'd need more logic to determine exact change type
			Timestamp:  timestamp,
		}

		changes = append(changes, change)
	}

	response := ChangeDetectionResponse{
		Changes:      changes,
		TotalChanges: len(changes),
		Timestamp:    timestamp,
		CheckDuration: time.Since(sinceTime).String(),
	}

	writeJSON(w, response)
}

// Bulk balance checker - compares current balances with provided previous balances
func getBulkBalanceCompareHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		writeError(w, http.StatusMethodNotAllowed, "POST method required")
		return
	}

	// Parse JSON body with previous balance state
	var request struct {
		Addresses map[string]string `json:"addresses"` // address -> previous_balance_lovelace
	}

	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid JSON body")
		return
	}

	if len(request.Addresses) == 0 {
		writeError(w, http.StatusBadRequest, "No addresses provided")
		return
	}

	if len(request.Addresses) > 1000 {
		writeError(w, http.StatusBadRequest, "Maximum 1000 addresses per request")
		return
	}

	// Get current balances for all addresses efficiently
	addressList := make([]string, 0, len(request.Addresses))
	oldBalances := make(map[string]int64)

	for addr, oldBalanceStr := range request.Addresses {
		addressList = append(addressList, addr)
		if oldBalance, err := strconv.ParseInt(oldBalanceStr, 10, 64); err == nil {
			oldBalances[addr] = oldBalance
		}
	}

	// Efficient bulk query for current balances
	query := `
		SELECT 
			a.address,
			COALESCE(SUM(txo.value), 0) as current_balance
		FROM address a
		LEFT JOIN tx_out txo ON txo.address_id = a.id
		LEFT JOIN tx t ON txo.tx_id = t.id
		WHERE a.address = ANY($1::text[])
		  AND (txo.consumed_by_tx_id IS NULL OR txo.consumed_by_tx_id IS NULL)
		  AND (t.valid_contract = true OR t.valid_contract IS NULL)
		GROUP BY a.address
	`

	rows, err := db.QueryContext(ctx, query, addressList)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to query current balances")
		return
	}
	defer rows.Close()

	currentBalances := make(map[string]int64)
	for rows.Next() {
		var address string
		var balance int64
		if err := rows.Scan(&address, &balance); err == nil {
			currentBalances[address] = balance
		}
	}

	// Compare and build changes
	var changes []AddressChange
	timestamp := time.Now().Unix()

	for address, oldBalance := range oldBalances {
		currentBalance := currentBalances[address]
		
		if currentBalance != oldBalance {
			changeType := "unchanged"
			var balanceDiff []TransactionAmount

			if currentBalance > oldBalance {
				changeType = "increased"
				diff := currentBalance - oldBalance
				balanceDiff = []TransactionAmount{
					{Unit: "lovelace", Quantity: "+" + strconv.FormatInt(diff, 10)},
				}
			} else if currentBalance < oldBalance {
				changeType = "decreased"
				diff := oldBalance - currentBalance
				balanceDiff = []TransactionAmount{
					{Unit: "lovelace", Quantity: "-" + strconv.FormatInt(diff, 10)},
				}
			}

			change := AddressChange{
				Address: address,
				OldBalance: []TransactionAmount{
					{Unit: "lovelace", Quantity: strconv.FormatInt(oldBalance, 10)},
				},
				NewBalance: []TransactionAmount{
					{Unit: "lovelace", Quantity: strconv.FormatInt(currentBalance, 10)},
				},
				BalanceDiff: balanceDiff,
				ChangeType:  changeType,
				Timestamp:   timestamp,
			}

			changes = append(changes, change)
		}
	}

	response := ChangeDetectionResponse{
		Changes:      changes,
		TotalChanges: len(changes),
		Timestamp:    timestamp,
	}

	writeJSON(w, response)
}

// Get recent block activity summary for monitoring
func getRecentActivityHandler(w http.ResponseWriter, r *http.Request) {
	// Get activity in the last hour by default
	hoursStr := r.URL.Query().Get("hours")
	hours := 1
	if hoursStr != "" {
		if h, err := strconv.Atoi(hoursStr); err == nil && h > 0 && h <= 24 {
			hours = h
		}
	}

	sinceTime := time.Now().Add(time.Duration(-hours) * time.Hour)

	query := `
		SELECT 
			COUNT(DISTINCT t.id) as tx_count,
			COUNT(DISTINCT txo.address_id) as addresses_affected,
			COALESCE(SUM(txo.value), 0) as total_value_moved,
			MAX(b.block_no) as latest_block
		FROM tx t
		JOIN block b ON t.block_id = b.id
		LEFT JOIN tx_out txo ON t.id = txo.tx_id
		WHERE b.time >= $1
		  AND t.valid_contract = true
	`

	var txCount, addressesAffected, totalValueMoved, latestBlock int64
	err := db.QueryRowContext(ctx, query, sinceTime).Scan(&txCount, &addressesAffected, &totalValueMoved, &latestBlock)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to query recent activity")
		return
	}

	response := map[string]interface{}{
		"period_hours":       hours,
		"transactions":       txCount,
		"addresses_affected": addressesAffected,
		"total_value_moved":  strconv.FormatInt(totalValueMoved, 10),
		"latest_block":       latestBlock,
		"timestamp":          time.Now().Unix(),
		"period_start":       sinceTime.Unix(),
	}

	writeJSON(w, response)
}

// Get last activity timestamp for an address
func getAddressLastActivityHandler(w http.ResponseWriter, r *http.Request) {
	address := strings.TrimPrefix(r.URL.Path, "/addresses/")
	address = strings.TrimSuffix(address, "/last-activity")
	
	if address == "" {
		writeError(w, http.StatusBadRequest, "Address required")
		return
	}

	addressID, err := getAddressIDCached(address)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to find address")
		return
	}

	query := `
		SELECT MAX(b.time) as last_activity
		FROM (
			SELECT t.block_id
			FROM tx_out txo
			JOIN tx t ON txo.tx_id = t.id
			WHERE txo.address_id = $1
			UNION
			SELECT t.block_id
			FROM tx_in txi
			JOIN tx t ON txi.tx_in_id = t.id
			JOIN tx_out prev_txo ON txi.tx_out_id = prev_txo.tx_id AND txi.tx_out_index = prev_txo.index
			WHERE prev_txo.address_id = $1
		) activity
		JOIN block b ON activity.block_id = b.id
	`

	var lastActivity *time.Time
	err = db.QueryRowContext(ctx, query, addressID).Scan(&lastActivity)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to query last activity")
		return
	}

	response := map[string]interface{}{
		"address": address,
		"last_activity": nil,
		"last_activity_timestamp": nil,
		"timestamp": time.Now().Unix(),
	}

	if lastActivity != nil {
		response["last_activity"] = lastActivity.Format(time.RFC3339)
		response["last_activity_timestamp"] = lastActivity.Unix()
	}

	writeJSON(w, response)
}