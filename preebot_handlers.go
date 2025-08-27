package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// PREEBOT-specific response types
type AssetChange struct {
	Address      string `json:"address"`
	PolicyID     string `json:"policy_id"`
	AssetName    string `json:"asset_name,omitempty"`
	AssetID      string `json:"asset_id"`
	Quantity     string `json:"quantity"`
	ChangeType   string `json:"change_type"` // "received", "sent", "minted"
	TxHash       string `json:"tx_hash"`
	BlockTime    int64  `json:"block_time"`
	SlotNo       int64  `json:"slot_no"`
}

type NFTMint struct {
	PolicyID     string `json:"policy_id"`
	AssetName    string `json:"asset_name"`
	AssetID      string `json:"asset_id"`
	MintedTo     string `json:"minted_to"`
	Quantity     string `json:"quantity"`
	TxHash       string `json:"tx_hash"`
	BlockTime    int64  `json:"block_time"`
	SlotNo       int64  `json:"slot_no"`
	Metadata     interface{} `json:"metadata,omitempty"`
}

type TokenValue struct {
	PolicyID      string `json:"policy_id"`
	AssetName     string `json:"asset_name,omitempty"`
	AssetID       string `json:"asset_id"`
	TotalSupply   string `json:"total_supply"`
	HoldersCount  int64  `json:"holders_count"`
	LastTxTime    int64  `json:"last_tx_time"`
	VolumeToday   string `json:"volume_today"`
	TransfersToday int64 `json:"transfers_today"`
}

type TokenPrice struct {
	PolicyID      string  `json:"policy_id"`
	AssetName     string  `json:"asset_name,omitempty"`
	AssetID       string  `json:"asset_id"`
	PriceADA      string  `json:"price_ada"`
	PriceUSD      string  `json:"price_usd,omitempty"`
	Volume24h     string  `json:"volume_24h"`
	PriceChange24h string `json:"price_change_24h"`
	LastTrade     int64   `json:"last_trade"`
	MarketCap     string  `json:"market_cap,omitempty"`
	Timestamp     int64   `json:"timestamp"`
}

type AssetTransfer struct {
	FromAddress  string `json:"from_address,omitempty"`
	ToAddress    string `json:"to_address"`
	PolicyID     string `json:"policy_id"`
	AssetName    string `json:"asset_name,omitempty"`
	AssetID      string `json:"asset_id"`
	Quantity     string `json:"quantity"`
	TxHash       string `json:"tx_hash"`
	BlockTime    int64  `json:"block_time"`
	SlotNo       int64  `json:"slot_no"`
	TransferType string `json:"transfer_type"` // "mint", "transfer", "burn"
}

// PREEBOT Asset Changes - Poll for asset changes in the last 5 minutes (for Discord role changes)
// GET /preebot/asset-changes?since=<timestamp>&policies=<policy1,policy2>
func preebotAssetChangesHandler(w http.ResponseWriter, r *http.Request) {
	// Default to 5 minutes ago (holding changes polling frequency)
	since := time.Now().Add(-5 * time.Minute).Unix()
	if sinceStr := r.URL.Query().Get("since"); sinceStr != "" {
		if s, err := strconv.ParseInt(sinceStr, 10, 64); err == nil {
			since = s
		}
	}

	// Optional policy filter for specific tokens/NFTs
	policiesParam := r.URL.Query().Get("policies")
	var policyFilter []string
	if policiesParam != "" {
		policyFilter = strings.Split(policiesParam, ",")
	}

	sinceTime := time.Unix(since, 0)

	// Ultra-fast query for recent asset changes
	query := `
		WITH recent_asset_changes AS (
			SELECT 
				a.address,
				encode(ma.policy, 'hex') as policy_id,
				encode(ma.name, 'hex') as asset_name,
				encode(ma.policy, 'hex') || encode(ma.name, 'hex') as asset_id,
				mto.quantity::text,
				CASE 
					WHEN prev_txo.id IS NULL THEN 'received'  -- New output
					ELSE 'sent'  -- Spent output
				END as change_type,
				encode(t.hash, 'hex') as tx_hash,
				extract(epoch from b.time)::bigint as block_time,
				b.slot_no
			FROM ma_tx_out mto
			JOIN tx_out txo ON mto.tx_out_id = txo.id
			JOIN address a ON txo.address_id = a.id
			JOIN multi_asset ma ON mto.ident = ma.id
			JOIN tx t ON txo.tx_id = t.id
			JOIN block b ON t.block_id = b.id
			LEFT JOIN tx_out prev_txo ON txo.consumed_by_tx_id IS NOT NULL
			WHERE b.time >= $1
			  AND t.valid_contract = true
			  AND mto.quantity != 0
			UNION ALL
			-- Also check for spent assets (when UTXOs are consumed)
			SELECT 
				a.address,
				encode(ma.policy, 'hex') as policy_id,
				encode(ma.name, 'hex') as asset_name,
				encode(ma.policy, 'hex') || encode(ma.name, 'hex') as asset_id,
				'-' || mto.quantity::text as quantity,
				'sent' as change_type,
				encode(consuming_tx.hash, 'hex') as tx_hash,
				extract(epoch from consuming_b.time)::bigint as block_time,
				consuming_b.slot_no
			FROM ma_tx_out mto
			JOIN tx_out txo ON mto.tx_out_id = txo.id
			JOIN address a ON txo.address_id = a.id
			JOIN multi_asset ma ON mto.ident = ma.id
			JOIN tx consuming_tx ON txo.consumed_by_tx_id = consuming_tx.id
			JOIN block consuming_b ON consuming_tx.block_id = consuming_b.id
			WHERE consuming_b.time >= $1
			  AND consuming_tx.valid_contract = true
			  AND mto.quantity > 0
		)
		SELECT * FROM recent_asset_changes
		ORDER BY block_time DESC, slot_no DESC
		LIMIT 1000
	`

	rows, err := db.QueryContext(ctx, query, sinceTime)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to query asset changes")
		return
	}
	defer rows.Close()

	var changes []AssetChange
	for rows.Next() {
		var change AssetChange
		err := rows.Scan(
			&change.Address,
			&change.PolicyID,
			&change.AssetName,
			&change.AssetID,
			&change.Quantity,
			&change.ChangeType,
			&change.TxHash,
			&change.BlockTime,
			&change.SlotNo,
		)
		if err != nil {
			continue
		}

		// Apply policy filter if specified
		if len(policyFilter) > 0 {
			found := false
			for _, policy := range policyFilter {
				if policy == change.PolicyID {
					found = true
					break
				}
			}
			if !found {
				continue
			}
		}

		changes = append(changes, change)
	}

	response := map[string]interface{}{
		"changes":       changes,
		"total_changes": len(changes),
		"since":         since,
		"timestamp":     time.Now().Unix(),
		"period":        "300s", // 5 minutes
	}

	writeJSON(w, response)
}

// PREEBOT NFT Minting - Detect new NFTs minted in the last 1 minute (high-frequency polling)
// GET /preebot/nft-mints?since=<timestamp>&policies=<policy1,policy2>
func preebotNFTMintsHandler(w http.ResponseWriter, r *http.Request) {
	// Default to 1 minute ago (high-frequency polling for mints)
	since := time.Now().Add(-1 * time.Minute).Unix()
	if sinceStr := r.URL.Query().Get("since"); sinceStr != "" {
		if s, err := strconv.ParseInt(sinceStr, 10, 64); err == nil {
			since = s
		}
	}

	// Optional policy filter
	policiesParam := r.URL.Query().Get("policies")
	var policyFilter []string
	if policiesParam != "" {
		policyFilter = strings.Split(policiesParam, ",")
	}

	sinceTime := time.Unix(since, 0)

	// Query for NFT mints (quantity = 1 and new outputs)
	query := `
		SELECT 
			encode(ma.policy, 'hex') as policy_id,
			encode(ma.name, 'hex') as asset_name,
			encode(ma.policy, 'hex') || encode(ma.name, 'hex') as asset_id,
			a.address as minted_to,
			mto.quantity::text,
			encode(t.hash, 'hex') as tx_hash,
			extract(epoch from b.time)::bigint as block_time,
			b.slot_no
		FROM ma_tx_out mto
		JOIN tx_out txo ON mto.tx_out_id = txo.id
		JOIN address a ON txo.address_id = a.id
		JOIN multi_asset ma ON mto.ident = ma.id
		JOIN tx t ON txo.tx_id = t.id
		JOIN block b ON t.block_id = b.id
		WHERE b.time >= $1
		  AND t.valid_contract = true
		  AND mto.quantity = 1  -- NFTs typically have quantity 1
		  AND txo.consumed_by_tx_id IS NULL  -- Still unspent (newly minted)
		  -- Check if this is actually a new mint (not a transfer)
		  AND NOT EXISTS (
		      SELECT 1 FROM ma_tx_out prev_mto
		      JOIN tx_out prev_txo ON prev_mto.tx_out_id = prev_txo.id
		      JOIN tx prev_t ON prev_txo.tx_id = prev_t.id
		      JOIN block prev_b ON prev_t.block_id = prev_b.id
		      WHERE prev_mto.ident = mto.ident
		        AND prev_b.time < b.time
		        AND prev_t.valid_contract = true
		  )
		ORDER BY b.time DESC, b.slot_no DESC
		LIMIT 500
	`

	rows, err := db.QueryContext(ctx, query, sinceTime)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to query NFT mints")
		return
	}
	defer rows.Close()

	var mints []NFTMint
	for rows.Next() {
		var mint NFTMint
		err := rows.Scan(
			&mint.PolicyID,
			&mint.AssetName,
			&mint.AssetID,
			&mint.MintedTo,
			&mint.Quantity,
			&mint.TxHash,
			&mint.BlockTime,
			&mint.SlotNo,
		)
		if err != nil {
			continue
		}

		// Apply policy filter if specified
		if len(policyFilter) > 0 {
			found := false
			for _, policy := range policyFilter {
				if policy == mint.PolicyID {
					found = true
					break
				}
			}
			if !found {
				continue
			}
		}

		mints = append(mints, mint)
	}

	response := map[string]interface{}{
		"mints":       mints,
		"total_mints": len(mints),
		"since":       since,
		"timestamp":   time.Now().Unix(),
		"period":      "60s", // 1 minute (high-frequency)
	}

	writeJSON(w, response)
}

// PREEBOT Token Values - Get current stats for specific tokens
// GET /preebot/token-value?asset_id=<asset_id> or POST with multiple asset_ids
func preebotTokenValueHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		// Single asset query
		assetID := r.URL.Query().Get("asset_id")
		if assetID == "" {
			writeError(w, http.StatusBadRequest, "asset_id parameter required")
			return
		}

		value, err := getTokenValue(assetID)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "Failed to get token value")
			return
		}

		writeJSON(w, value)
	} else if r.Method == "POST" {
		// Batch token query
		var request struct {
			AssetIDs []string `json:"asset_ids"`
		}

		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			writeError(w, http.StatusBadRequest, "Invalid JSON body")
			return
		}

		if len(request.AssetIDs) == 0 || len(request.AssetIDs) > 100 {
			writeError(w, http.StatusBadRequest, "Provide 1-100 asset_ids")
			return
		}

		values := make(map[string]TokenValue)
		for _, assetID := range request.AssetIDs {
			if value, err := getTokenValue(assetID); err == nil {
				values[assetID] = *value
			}
		}

		response := map[string]interface{}{
			"tokens":    values,
			"count":     len(values),
			"timestamp": time.Now().Unix(),
		}

		writeJSON(w, response)
	} else {
		writeError(w, http.StatusMethodNotAllowed, "GET or POST method required")
	}
}

// Helper function to get token value/stats - CACHE-FIRST VERSION
func getTokenValue(assetID string) (*TokenValue, error) {
	if len(assetID) < 56 {
		return nil, writeErrorString("Invalid asset_id format")
	}

	policyID := assetID[:56]

	// CACHE-FIRST APPROACH: Try cache first, fallback to live calculation
	// 1. Check Redis cache (30-second cache)
	cacheKey := "token_value:" + assetID
	if cached, err := redisClient.Get(ctx, cacheKey).Result(); err == nil {
		var value TokenValue
		if json.Unmarshal([]byte(cached), &value) == nil {
			return &value, nil
		}
	}

	// 2. Try database cache table (if exists)
	var value TokenValue
	cacheQuery := `
		SELECT 
			policy_id,
			COALESCE(asset_name, ''),
			asset_id,
			total_supply::text,
			holders_count,
			last_tx_time,
			'0' as volume_today,
			0 as transfers_today
		FROM preebot_token_simple_cache
		WHERE asset_id = $1 OR policy_id = $2
		LIMIT 1
	`

	err := db.QueryRowContext(ctx, cacheQuery, assetID, policyID).Scan(
		&value.PolicyID,
		&value.AssetName,
		&value.AssetID,
		&value.TotalSupply,
		&value.HoldersCount,
		&value.LastTxTime,
		&value.VolumeToday,
		&value.TransfersToday,
	)

	if err == nil {
		// Cache in Redis for 30 seconds
		if data, _ := json.Marshal(value); len(data) > 0 {
			redisClient.Set(ctx, cacheKey, data, 30*time.Second)
		}
		return &value, nil
	}

	// 3. Fallback: Minimal live query (just basic stats)
	fallbackQuery := `
		WITH asset_lookup AS (
			SELECT id, encode(policy, 'hex') as policy_hex, encode(name, 'hex') as name_hex
			FROM multi_asset 
			WHERE encode(policy, 'hex') = $1
			LIMIT 1
		)
		SELECT 
			al.policy_hex as policy_id,
			al.name_hex as asset_name,
			al.policy_hex || al.name_hex as asset_id,
			'0' as total_supply,  -- Placeholder - too expensive to calculate live
			0 as holders_count,   -- Placeholder - too expensive to calculate live
			0 as last_tx_time,    -- Placeholder
			'0' as volume_today,
			0 as transfers_today
		FROM asset_lookup al
	`

	err = db.QueryRowContext(ctx, fallbackQuery, policyID).Scan(
		&value.PolicyID,
		&value.AssetName,
		&value.AssetID,
		&value.TotalSupply,
		&value.HoldersCount,
		&value.LastTxTime,
		&value.VolumeToday,
		&value.TransfersToday,
	)

	if err != nil {
		return nil, err
	}

	// Cache the result for 30 seconds
	if data, _ := json.Marshal(value); len(data) > 0 {
		redisClient.Set(ctx, cacheKey, data, 30*time.Second)
	}

	return &value, nil
}

// PREEBOT Asset Transfers - Track purchases/transfers of specific assets (1-minute polling)
// GET /preebot/asset-transfers?since=<timestamp>&policies=<policy1,policy2>&min_value=<lovelace>
func preebotAssetTransfersHandler(w http.ResponseWriter, r *http.Request) {
	// Default to 1 minute ago (high-frequency polling for purchases)
	since := time.Now().Add(-1 * time.Minute).Unix()
	if sinceStr := r.URL.Query().Get("since"); sinceStr != "" {
		if s, err := strconv.ParseInt(sinceStr, 10, 64); err == nil {
			since = s
		}
	}

	// Optional policy filter
	policiesParam := r.URL.Query().Get("policies")
	var policyFilter []string
	if policiesParam != "" {
		policyFilter = strings.Split(policiesParam, ",")
	}

	// Optional minimum ADA value filter (for purchase detection)
	minValue := int64(0)
	if minValueStr := r.URL.Query().Get("min_value"); minValueStr != "" {
		if mv, err := strconv.ParseInt(minValueStr, 10, 64); err == nil {
			minValue = mv
		}
	}

	sinceTime := time.Unix(since, 0)

	// Query for asset transfers with optional ADA value filter
	query := `
		SELECT 
			prev_a.address as from_address,
			curr_a.address as to_address,
			encode(ma.policy, 'hex') as policy_id,
			encode(ma.name, 'hex') as asset_name,
			encode(ma.policy, 'hex') || encode(ma.name, 'hex') as asset_id,
			curr_mto.quantity::text,
			encode(t.hash, 'hex') as tx_hash,
			extract(epoch from b.time)::bigint as block_time,
			b.slot_no,
			CASE 
				WHEN prev_txo.id IS NULL THEN 'mint'
				WHEN curr_txo.consumed_by_tx_id IS NOT NULL THEN 'burn'
				ELSE 'transfer'
			END as transfer_type
		FROM ma_tx_out curr_mto
		JOIN tx_out curr_txo ON curr_mto.tx_out_id = curr_txo.id
		JOIN address curr_a ON curr_txo.address_id = curr_a.id
		JOIN multi_asset ma ON curr_mto.ident = ma.id
		JOIN tx t ON curr_txo.tx_id = t.id
		JOIN block b ON t.block_id = b.id
		LEFT JOIN tx_in txi ON txi.tx_out_id = curr_txo.tx_id AND txi.tx_out_index = curr_txo.index
		LEFT JOIN tx_out prev_txo ON txi.tx_out_id = prev_txo.tx_id AND txi.tx_out_index = prev_txo.index
		LEFT JOIN address prev_a ON prev_txo.address_id = prev_a.id
		WHERE b.time >= $1
		  AND t.valid_contract = true
		  AND curr_mto.quantity > 0
		  AND ($2 = 0 OR curr_txo.value >= $2)  -- ADA value filter
		ORDER BY b.time DESC, b.slot_no DESC
		LIMIT 1000
	`

	rows, err := db.QueryContext(ctx, query, sinceTime, minValue)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to query asset transfers")
		return
	}
	defer rows.Close()

	var transfers []AssetTransfer
	for rows.Next() {
		var transfer AssetTransfer
		var fromAddress *string
		err := rows.Scan(
			&fromAddress,
			&transfer.ToAddress,
			&transfer.PolicyID,
			&transfer.AssetName,
			&transfer.AssetID,
			&transfer.Quantity,
			&transfer.TxHash,
			&transfer.BlockTime,
			&transfer.SlotNo,
			&transfer.TransferType,
		)
		if err != nil {
			continue
		}

		if fromAddress != nil {
			transfer.FromAddress = *fromAddress
		}

		// Apply policy filter if specified
		if len(policyFilter) > 0 {
			found := false
			for _, policy := range policyFilter {
				if policy == transfer.PolicyID {
					found = true
					break
				}
			}
			if !found {
				continue
			}
		}

		transfers = append(transfers, transfer)
	}

	response := map[string]interface{}{
		"transfers":       transfers,
		"total_transfers": len(transfers),
		"since":           since,
		"timestamp":       time.Now().Unix(),
		"period":          "60s", // 1 minute (high-frequency)
		"filters": map[string]interface{}{
			"policies":  policyFilter,
			"min_value": minValue,
		},
	}

	writeJSON(w, response)
}

// Helper function for error strings
func writeErrorString(message string) error {
	return &APIError{Message: message}
}

type APIError struct {
	Message string
}

func (e *APIError) Error() string {
	return e.Message
}

// PREEBOT Cache Management - Refresh token statistics for specific policies
// POST /preebot/refresh-cache with {"policy_ids": ["policy1", "policy2"]}
func preebotRefreshCacheHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		writeError(w, http.StatusMethodNotAllowed, "POST method required")
		return
	}

	var request struct {
		PolicyIDs []string `json:"policy_ids"`
	}

	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid JSON body")
		return
	}

	if len(request.PolicyIDs) == 0 || len(request.PolicyIDs) > 20 {
		writeError(w, http.StatusBadRequest, "Provide 1-20 policy_ids")
		return
	}

	results := make(map[string]interface{})
	successCount := 0

	for _, policyID := range request.PolicyIDs {
		// Try to refresh this policy's cache using the database function
		refreshQuery := `SELECT * FROM refresh_token_cache($1)`
		
		rows, err := db.QueryContext(ctx, refreshQuery, policyID)
		if err != nil {
			results[policyID] = map[string]interface{}{
				"success": false,
				"error":   "Failed to refresh cache",
			}
			continue
		}

		var tokens []map[string]interface{}
		for rows.Next() {
			var policy, name, assetID, supply string
			var holders, lastTx int64
			
			if err := rows.Scan(&policy, &name, &assetID, &supply, &holders, &lastTx); err == nil {
				tokens = append(tokens, map[string]interface{}{
					"asset_id":       assetID,
					"total_supply":   supply,
					"holders_count":  holders,
					"last_tx_time":   lastTx,
				})
			}
		}
		rows.Close()

		if len(tokens) > 0 {
			results[policyID] = map[string]interface{}{
				"success": true,
				"tokens":  tokens,
				"count":   len(tokens),
			}
			successCount++

			// Also clear Redis cache for these tokens
			for _, token := range tokens {
				if assetID, ok := token["asset_id"].(string); ok {
					redisClient.Del(ctx, "token_value:"+assetID)
				}
			}
		} else {
			results[policyID] = map[string]interface{}{
				"success": false,
				"error":   "No tokens found for policy",
			}
		}
	}

	response := map[string]interface{}{
		"results":        results,
		"total_policies": len(request.PolicyIDs),
		"successful":     successCount,
		"timestamp":      time.Now().Unix(),
	}

	writeJSON(w, response)
}

// PREEBOT Token Price - Get real-time token price (5-minute polling, NO CACHE)
// GET /preebot/token-price?asset_id=<asset_id> or POST for multiple tokens
func preebotTokenPriceHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		// Single token price query
		assetID := r.URL.Query().Get("asset_id")
		if assetID == "" {
			writeError(w, http.StatusBadRequest, "asset_id parameter required")
			return
		}

		price, err := getTokenPriceRealTime(assetID)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "Failed to get token price: "+err.Error())
			return
		}

		writeJSON(w, price)
	} else if r.Method == "POST" {
		// Batch token price query
		var request struct {
			AssetIDs []string `json:"asset_ids"`
		}

		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			writeError(w, http.StatusBadRequest, "Invalid JSON body")
			return
		}

		if len(request.AssetIDs) == 0 || len(request.AssetIDs) > 50 {
			writeError(w, http.StatusBadRequest, "Provide 1-50 asset_ids")
			return
		}

		prices := make(map[string]TokenPrice)
		for _, assetID := range request.AssetIDs {
			if price, err := getTokenPriceRealTime(assetID); err == nil {
				prices[assetID] = *price
			}
		}

		response := map[string]interface{}{
			"prices":    prices,
			"count":     len(prices),
			"timestamp": time.Now().Unix(),
		}

		writeJSON(w, response)
	} else {
		writeError(w, http.StatusMethodNotAllowed, "GET or POST method required")
	}
}

// Get real-time token price - LIGHTNING FAST using live tracking table
func getTokenPriceRealTime(assetID string) (*TokenPrice, error) {
	if len(assetID) < 56 {
		return nil, writeErrorString("Invalid asset_id format")
	}

	policyID := assetID[:56]

	// ULTRA-FAST QUERY: Direct lookup from live tracking table
	// This table is updated block-by-block, so data is always fresh
	query := `
		SELECT 
			policy_id,
			COALESCE(asset_name, ''),
			asset_id,
			current_price_ada::text,
			price_usd::text,
			volume_24h::text,
			price_change_24h::text,
			last_trade_time,
			market_cap::text,
			extract(epoch from updated_at)::bigint as timestamp
		FROM preebot_token_prices
		WHERE asset_id = $1 OR policy_id = $2
		LIMIT 1
	`

	var price TokenPrice
	err := db.QueryRowContext(ctx, query, assetID, policyID).Scan(
		&price.PolicyID,
		&price.AssetName,
		&price.AssetID,
		&price.PriceADA,
		&price.PriceUSD,
		&price.Volume24h,
		&price.PriceChange24h,
		&price.LastTrade,
		&price.MarketCap,
		&price.Timestamp,
	)

	if err != nil {
		// Fallback: basic asset info if no price data exists yet
		fallbackQuery := `
			SELECT 
				encode(policy, 'hex') as policy_id,
				encode(name, 'hex') as asset_name,
				encode(policy, 'hex') || encode(name, 'hex') as asset_id
			FROM multi_asset 
			WHERE encode(policy, 'hex') = $1
			LIMIT 1
		`

		var fallbackPrice TokenPrice
		fallbackErr := db.QueryRowContext(ctx, fallbackQuery, policyID).Scan(
			&fallbackPrice.PolicyID,
			&fallbackPrice.AssetName,
			&fallbackPrice.AssetID,
		)

		if fallbackErr != nil {
			return nil, fallbackErr
		}

		// Return with zero values
		fallbackPrice.PriceADA = "0"
		fallbackPrice.PriceUSD = "0"
		fallbackPrice.Volume24h = "0"
		fallbackPrice.PriceChange24h = "0"
		fallbackPrice.LastTrade = 0
		fallbackPrice.MarketCap = "0"
		fallbackPrice.Timestamp = time.Now().Unix()

		return &fallbackPrice, nil
	}

	return &price, nil
}

// PREEBOT Asset Holdings - Lightning fast holdings lookup using live tracking table
// GET /preebot/asset-holdings?address=<address> or POST for multiple addresses
func preebotAssetHoldingsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		// Single address query
		address := r.URL.Query().Get("address")
		if address == "" {
			writeError(w, http.StatusBadRequest, "address parameter required")
			return
		}

		holdings, err := getAssetHoldingsLive(address)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "Failed to get holdings: "+err.Error())
			return
		}

		response := map[string]interface{}{
			"address":   address,
			"holdings":  holdings,
			"count":     len(holdings),
			"timestamp": time.Now().Unix(),
		}

		writeJSON(w, response)
	} else if r.Method == "POST" {
		// Batch address query
		var request struct {
			Addresses []string `json:"addresses"`
		}

		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			writeError(w, http.StatusBadRequest, "Invalid JSON body")
			return
		}

		if len(request.Addresses) == 0 || len(request.Addresses) > 100 {
			writeError(w, http.StatusBadRequest, "Provide 1-100 addresses")
			return
		}

		allHoldings := make(map[string]interface{})
		for _, address := range request.Addresses {
			if holdings, err := getAssetHoldingsLive(address); err == nil {
				allHoldings[address] = holdings
			}
		}

		response := map[string]interface{}{
			"holdings":  allHoldings,
			"count":     len(allHoldings),
			"timestamp": time.Now().Unix(),
		}

		writeJSON(w, response)
	} else {
		writeError(w, http.StatusMethodNotAllowed, "GET or POST method required")
	}
}

// Get live asset holdings - LIGHTNING FAST using live tracking table with NFT metadata
func getAssetHoldingsLive(address string) ([]map[string]interface{}, error) {
	// Debug: First check if address exists and has any activity at all
	var addressExists bool
	var totalOutputs int64
	
	debugQuery := `
		SELECT 
			COUNT(*) > 0 as address_exists,
			COUNT(txo.id) as total_outputs
		FROM address a
		LEFT JOIN tx_out txo ON a.id = txo.address_id
		WHERE a.address = $1
		GROUP BY a.id
	`
	
	db.QueryRowContext(ctx, debugQuery, address).Scan(&addressExists, &totalOutputs)
	
	// First try the optimized live tracking table
	holdings, err := getAssetHoldingsFromCache(address)
	if err == nil && len(holdings) > 0 {
		return holdings, nil
	}

	// Fallback: Query main database directly (slower but works immediately)
	mainHoldings, err := getAssetHoldingsFromMainDB(address)
	if err != nil {
		// Return debug info if query fails
		debugHolding := map[string]interface{}{
			"debug":         true,
			"address_exists": addressExists,
			"total_outputs":  totalOutputs,
			"error":         err.Error(),
			"source":        "debug",
		}
		return []map[string]interface{}{debugHolding}, nil
	}
	
	return mainHoldings, nil
}

// Get holdings from live tracking table (fastest)
func getAssetHoldingsFromCache(address string) ([]map[string]interface{}, error) {
	query := `
		SELECT 
			pah.asset_id,
			pah.policy_id,
			pah.asset_name,
			pah.quantity,
			pah.last_tx_hash,
			pah.last_updated_block,
			extract(epoch from pah.updated_at)::bigint as updated_timestamp,
			-- Get NFT metadata from the original minting transaction
			COALESCE(tm.json, '{}'::jsonb) as metadata
		FROM preebot_asset_holdings pah
		LEFT JOIN (
			-- Find the original minting tx for metadata lookup
			SELECT DISTINCT ON (ma.id) 
				ma.id as multi_asset_id,
				t.id as tx_id,
				encode(ma.policy, 'hex') || encode(ma.name, 'hex') as asset_id
			FROM multi_asset ma
			JOIN ma_tx_out mto ON ma.id = mto.ident
			JOIN tx_out txo ON mto.tx_out_id = txo.id
			JOIN tx t ON txo.tx_id = t.id
			WHERE mto.quantity > 0
			ORDER BY ma.id, t.id ASC  -- First transaction = mint
		) mint_lookup ON mint_lookup.asset_id = pah.asset_id
		LEFT JOIN tx_metadata tm ON tm.tx_id = mint_lookup.tx_id 
			AND tm.key = 721  -- CIP-25 NFT metadata standard
		WHERE pah.address = $1 AND pah.quantity > 0
		ORDER BY pah.quantity DESC
		LIMIT 1000
	`

	return executeHoldingsQuery(query, address)
}

// Get holdings directly from main database (fallback)
func getAssetHoldingsFromMainDB(address string) ([]map[string]interface{}, error) {
	// Simplified direct query to main cardano-db-sync tables
	query := `
		SELECT 
			encode(ma.policy, 'hex') || encode(ma.name, 'hex') as asset_id,
			encode(ma.policy, 'hex') as policy_id,
			encode(ma.name, 'hex') as asset_name,
			SUM(mto.quantity) as total_quantity,
			encode(t.hash, 'hex') as latest_tx_hash,
			extract(epoch from b.time)::bigint as timestamp
		FROM address a
		JOIN tx_out txo ON a.id = txo.address_id
		JOIN ma_tx_out mto ON txo.id = mto.tx_out_id
		JOIN multi_asset ma ON mto.ident = ma.id
		JOIN tx t ON txo.tx_id = t.id
		JOIN block b ON t.block_id = b.id
		WHERE a.address = $1
		  AND txo.consumed_by_tx_id IS NULL  -- Only unspent outputs
		  AND t.valid_contract = true
		  AND mto.quantity > 0
		GROUP BY ma.id, ma.policy, ma.name, t.hash, b.time
		HAVING SUM(mto.quantity) > 0
		ORDER BY SUM(mto.quantity) DESC
		LIMIT 1000
	`

	rows, err := db.QueryContext(ctx, query, address)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var holdings []map[string]interface{}
	assetsSeen := make(map[string]bool) // Deduplicate assets
	
	for rows.Next() {
		var assetID, policyID, assetName, lastTxHash string
		var quantity, timestamp int64

		if err := rows.Scan(&assetID, &policyID, &assetName, &quantity, &lastTxHash, &timestamp); err != nil {
			continue
		}

		// Skip if we've already processed this asset (due to GROUP BY on tx.hash)
		if assetsSeen[assetID] {
			continue
		}
		assetsSeen[assetID] = true

		holding := map[string]interface{}{
			"asset_id":     assetID,
			"policy_id":    policyID,
			"asset_name":   assetName,
			"quantity":     strconv.FormatInt(quantity, 10),
			"last_tx_hash": lastTxHash,
			"timestamp":    timestamp,
			"source":       "main_db", // Indicate this came from main DB
		}

		// Try to get metadata for this asset (separate query for simplicity)
		if metadata := getAssetMetadata(assetID, policyID, assetName); metadata != nil {
			holding["metadata"] = metadata
			
			// Extract traits for Discord role assignment
			if traits := extractNFTTraits(metadata, policyID, assetName); len(traits) > 0 {
				holding["traits"] = traits
			}
			
			// Add useful NFT info
			if nftInfo := extractNFTInfo(metadata, policyID); nftInfo != nil {
				holding["nft_info"] = nftInfo
			}
		}

		holdings = append(holdings, holding)
	}

	return holdings, nil
}

// Get metadata for a specific asset
func getAssetMetadata(assetID, policyID, assetName string) map[string]interface{} {
	// Find the first minting transaction for this asset to get metadata
	query := `
		SELECT COALESCE(tm.json, '{}'::jsonb) as metadata
		FROM multi_asset ma
		JOIN ma_tx_out mto ON ma.id = mto.ident
		JOIN tx_out txo ON mto.tx_out_id = txo.id
		JOIN tx t ON txo.tx_id = t.id
		LEFT JOIN tx_metadata tm ON t.id = tm.tx_id AND tm.key = 721
		WHERE encode(ma.policy, 'hex') = $1
		  AND encode(ma.name, 'hex') = $2
		  AND mto.quantity > 0
		  AND t.valid_contract = true
		ORDER BY t.id ASC
		LIMIT 1
	`

	var metadataBytes []byte
	err := db.QueryRowContext(ctx, query, policyID, assetName).Scan(&metadataBytes)
	if err != nil {
		return nil
	}

	if len(metadataBytes) > 0 {
		var metadata map[string]interface{}
		if err := json.Unmarshal(metadataBytes, &metadata); err == nil {
			return metadata
		}
	}

	return nil
}

// Execute holdings query and parse results
func executeHoldingsQuery(query, address string) ([]map[string]interface{}, error) {
	rows, err := db.QueryContext(ctx, query, address)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var holdings []map[string]interface{}
	for rows.Next() {
		var assetID, policyID, assetName, lastTxHash string
		var quantity, lastBlock, timestamp int64
		var metadataBytes []byte

		if err := rows.Scan(&assetID, &policyID, &assetName, &quantity, &lastTxHash, &lastBlock, &timestamp, &metadataBytes); err != nil {
			continue
		}

		holding := map[string]interface{}{
			"asset_id":           assetID,
			"policy_id":          policyID,
			"asset_name":         assetName,
			"quantity":           strconv.FormatInt(quantity, 10),
			"last_tx_hash":       lastTxHash,
			"last_updated_block": lastBlock,
			"timestamp":          timestamp,
			"source":             "cache", // Indicate this came from cache
		}

		// Parse and extract NFT metadata/traits
		if len(metadataBytes) > 0 {
			var metadata map[string]interface{}
			if err := json.Unmarshal(metadataBytes, &metadata); err == nil {
				holding["metadata"] = metadata
				
				// Extract traits for Discord role assignment
				if traits := extractNFTTraits(metadata, policyID, assetName); len(traits) > 0 {
					holding["traits"] = traits
				}
				
				// Add useful NFT info
				if nftInfo := extractNFTInfo(metadata, policyID); nftInfo != nil {
					holding["nft_info"] = nftInfo
				}
			}
		}

		holdings = append(holdings, holding)
	}

	return holdings, nil
}

// Extract NFT traits from CIP-25 metadata for Discord role assignment
func extractNFTTraits(metadata map[string]interface{}, policyID, assetName string) map[string]interface{} {
	traits := make(map[string]interface{})
	
	// Navigate CIP-25 structure: metadata[policy_id][asset_name]
	if policyData, ok := metadata[policyID].(map[string]interface{}); ok {
		var assetData map[string]interface{}
		
		// Try with asset name, then without (some use empty string)
		if asset, exists := policyData[assetName].(map[string]interface{}); exists {
			assetData = asset
		} else if asset, exists := policyData[""].(map[string]interface{}); exists {
			assetData = asset
		}
		
		if assetData != nil {
			// Standard CIP-25 attributes
			if attrs, ok := assetData["attributes"].(map[string]interface{}); ok {
				for key, value := range attrs {
					traits[key] = value
				}
			}
			
			// Alternative structure: traits array
			if traitsArray, ok := assetData["traits"].([]interface{}); ok {
				for _, trait := range traitsArray {
					if traitMap, ok := trait.(map[string]interface{}); ok {
						if name, ok := traitMap["trait_type"].(string); ok {
							traits[name] = traitMap["value"]
						}
					}
				}
			}
			
			// Common properties that might be useful for roles
			if name, ok := assetData["name"].(string); ok {
				traits["name"] = name
			}
			if description, ok := assetData["description"].(string); ok {
				traits["description"] = description
			}
		}
	}
	
	return traits
}

// Extract basic NFT information
func extractNFTInfo(metadata map[string]interface{}, policyID string) map[string]interface{} {
	if policyData, ok := metadata[policyID].(map[string]interface{}); ok {
		info := make(map[string]interface{})
		
		// Count assets in this policy (collection size indication)
		info["collection_size"] = len(policyData)
		
		// Look for collection-level metadata
		for _, assetDataInterface := range policyData {
			if assetData, ok := assetDataInterface.(map[string]interface{}); ok {
				if name, ok := assetData["name"].(string); ok {
					info["collection_name"] = name
					break // Take the first name found
				}
			}
		}
		
		if len(info) > 0 {
			return info
		}
	}
	
	return nil
}

// PREEBOT NFT Traits - Discord role assignment optimized endpoint
// GET /preebot/nft-traits?address=<address>&policies=<policy1,policy2>&traits=<trait1:value1,trait2:value2>
// POST for multiple addresses: {"addresses": ["addr1", "addr2"], "policies": ["policy1"], "required_traits": {"rarity": "legendary"}}
func preebotNFTTraitsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		// Single address trait query
		address := r.URL.Query().Get("address")
		if address == "" {
			writeError(w, http.StatusBadRequest, "address parameter required")
			return
		}

		// Optional policy filter
		policiesParam := r.URL.Query().Get("policies")
		var policyFilter []string
		if policiesParam != "" {
			policyFilter = strings.Split(policiesParam, ",")
		}

		// Optional trait filter for role assignment
		traitsParam := r.URL.Query().Get("traits")
		requiredTraits := make(map[string]string)
		if traitsParam != "" {
			for _, traitPair := range strings.Split(traitsParam, ",") {
				if parts := strings.Split(traitPair, ":"); len(parts) == 2 {
					requiredTraits[parts[0]] = parts[1]
				}
			}
		}

		result, err := getNFTTraitsForAddress(address, policyFilter, requiredTraits)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "Failed to get NFT traits: "+err.Error())
			return
		}

		writeJSON(w, result)
	} else if r.Method == "POST" {
		// Batch trait query for multiple addresses
		var request struct {
			Addresses      []string          `json:"addresses"`
			Policies       []string          `json:"policies,omitempty"`
			RequiredTraits map[string]string `json:"required_traits,omitempty"`
		}

		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			writeError(w, http.StatusBadRequest, "Invalid JSON body")
			return
		}

		if len(request.Addresses) == 0 || len(request.Addresses) > 50 {
			writeError(w, http.StatusBadRequest, "Provide 1-50 addresses")
			return
		}

		results := make(map[string]interface{})
		for _, address := range request.Addresses {
			if result, err := getNFTTraitsForAddress(address, request.Policies, request.RequiredTraits); err == nil {
				results[address] = result
			}
		}

		response := map[string]interface{}{
			"results":   results,
			"count":     len(results),
			"timestamp": time.Now().Unix(),
		}

		writeJSON(w, response)
	} else {
		writeError(w, http.StatusMethodNotAllowed, "GET or POST method required")
	}
}

// Get NFT traits for a specific address with filtering for Discord role assignment
func getNFTTraitsForAddress(address string, policyFilter []string, requiredTraits map[string]string) (map[string]interface{}, error) {
	// Ultra-fast query focusing on NFTs (quantity = 1) with metadata
	query := `
		SELECT 
			pah.asset_id,
			pah.policy_id,
			pah.asset_name,
			pah.quantity,
			-- Get NFT metadata from the original minting transaction
			COALESCE(tm.json, '{}'::jsonb) as metadata
		FROM preebot_asset_holdings pah
		LEFT JOIN (
			-- Find the original minting tx for metadata lookup
			SELECT DISTINCT ON (ma.id) 
				ma.id as multi_asset_id,
				t.id as tx_id,
				encode(ma.policy, 'hex') || encode(ma.name, 'hex') as asset_id
			FROM multi_asset ma
			JOIN ma_tx_out mto ON ma.id = mto.ident
			JOIN tx_out txo ON mto.tx_out_id = txo.id
			JOIN tx t ON txo.tx_id = t.id
			WHERE mto.quantity > 0
			ORDER BY ma.id, t.id ASC  -- First transaction = mint
		) mint_lookup ON mint_lookup.asset_id = pah.asset_id
		LEFT JOIN tx_metadata tm ON tm.tx_id = mint_lookup.tx_id 
			AND tm.key = 721  -- CIP-25 NFT metadata standard
		WHERE pah.address = $1 
		  AND pah.quantity > 0
		  AND pah.quantity <= 10  -- Focus on likely NFTs (not fungible tokens)
		ORDER BY pah.policy_id, pah.asset_name
	`

	rows, err := db.QueryContext(ctx, query, address)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var matchingNFTs []map[string]interface{}
	var allNFTs []map[string]interface{}

	for rows.Next() {
		var assetID, policyID, assetName string
		var quantity int64
		var metadataBytes []byte

		if err := rows.Scan(&assetID, &policyID, &assetName, &quantity, &metadataBytes); err != nil {
			continue
		}

		// Apply policy filter if specified
		if len(policyFilter) > 0 {
			found := false
			for _, policy := range policyFilter {
				if policy == policyID {
					found = true
					break
				}
			}
			if !found {
				continue
			}
		}

		nft := map[string]interface{}{
			"asset_id":   assetID,
			"policy_id":  policyID,
			"asset_name": assetName,
			"quantity":   quantity,
		}

		// Parse metadata and extract traits
		if len(metadataBytes) > 0 {
			var metadata map[string]interface{}
			if err := json.Unmarshal(metadataBytes, &metadata); err == nil {
				if traits := extractNFTTraits(metadata, policyID, assetName); len(traits) > 0 {
					nft["traits"] = traits

					// Check if this NFT matches required traits for Discord roles
					matchesRequiredTraits := true
					if len(requiredTraits) > 0 {
						for requiredTrait, requiredValue := range requiredTraits {
							if traitValue, exists := traits[requiredTrait]; !exists || 
								!strings.EqualFold(fmt.Sprintf("%v", traitValue), requiredValue) {
								matchesRequiredTraits = false
								break
							}
						}
					}

					if matchesRequiredTraits {
						matchingNFTs = append(matchingNFTs, nft)
					}
				}
			}
		}

		allNFTs = append(allNFTs, nft)
	}

	// Summary for Discord role assignment
	result := map[string]interface{}{
		"address":       address,
		"total_nfts":    len(allNFTs),
		"matching_nfts": len(matchingNFTs),
		"nfts":          matchingNFTs, // Only return NFTs that match criteria
		"timestamp":     time.Now().Unix(),
	}

	// Add policy summary for role assignment logic
	policyCount := make(map[string]int)
	for _, nft := range matchingNFTs {
		if policyID, ok := nft["policy_id"].(string); ok {
			policyCount[policyID]++
		}
	}
	if len(policyCount) > 0 {
		result["policy_counts"] = policyCount
	}

	// Add filtering criteria to response
	if len(policyFilter) > 0 {
		result["filtered_policies"] = policyFilter
	}
	if len(requiredTraits) > 0 {
		result["required_traits"] = requiredTraits
	}

	return result, nil
}

// PREEBOT Cache NFT Metadata - Populate metadata cache for specific policies (Discord optimization)
// POST /preebot/cache-nft-metadata with {"policy_ids": ["policy1", "policy2"]}
func preebotCacheNFTMetadataHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		writeError(w, http.StatusMethodNotAllowed, "POST method required")
		return
	}

	var request struct {
		PolicyIDs []string `json:"policy_ids"`
	}

	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid JSON body")
		return
	}

	if len(request.PolicyIDs) == 0 || len(request.PolicyIDs) > 10 {
		writeError(w, http.StatusBadRequest, "Provide 1-10 policy_ids")
		return
	}

	results := make(map[string]interface{})
	successCount := 0
	totalCached := 0

	for _, policyID := range request.PolicyIDs {
		// Cache metadata for this policy
		cacheQuery := `SELECT cache_nft_metadata_for_policy($1)`
		
		var cachedCount int
		err := db.QueryRowContext(ctx, cacheQuery, policyID).Scan(&cachedCount)
		if err != nil {
			results[policyID] = map[string]interface{}{
				"success": false,
				"error":   "Failed to cache metadata: " + err.Error(),
				"cached":  0,
			}
			continue
		}

		results[policyID] = map[string]interface{}{
			"success": true,
			"cached":  cachedCount,
		}
		successCount++
		totalCached += cachedCount
	}

	response := map[string]interface{}{
		"results":        results,
		"total_policies": len(request.PolicyIDs),
		"successful":     successCount,
		"total_cached":   totalCached,
		"timestamp":      time.Now().Unix(),
	}

	writeJSON(w, response)
}