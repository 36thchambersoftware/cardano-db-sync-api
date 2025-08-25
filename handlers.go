package main

import (
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
)

const (
	defaultPageSize = 100
	maxPageSize     = 1000
)

// Helper functions
func writeJSON(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(data); err != nil {
		http.Error(w, "JSON encoding error", http.StatusInternalServerError)
	}
}

func writeError(w http.ResponseWriter, statusCode int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(ErrorResponse{
		StatusCode: statusCode,
		Error:      http.StatusText(statusCode),
		Message:    message,
	})
}

func getPaginationParams(r *http.Request) (count, page int, order string) {
	count = defaultPageSize
	page = 1
	order = "asc"

	if c := r.URL.Query().Get("count"); c != "" {
		if parsed, err := strconv.Atoi(c); err == nil && parsed > 0 && parsed <= maxPageSize {
			count = parsed
		}
	}

	if p := r.URL.Query().Get("page"); p != "" {
		if parsed, err := strconv.Atoi(p); err == nil && parsed > 0 {
			page = parsed
		}
	}

	if o := r.URL.Query().Get("order"); o == "desc" {
		order = "desc"
	}

	return count, page, order
}

func getCachedResponse(key string) ([]byte, bool) {
	cached, err := redisClient.Get(ctx, key).Result()
	if err == redis.Nil {
		return nil, false
	} else if err != nil {
		log.Printf("Redis get error: %v", err)
		return nil, false
	}
	return []byte(cached), true
}

func setCachedResponse(key string, data []byte, duration time.Duration) {
	if err := redisClient.Set(ctx, key, data, duration).Err(); err != nil {
		log.Printf("Redis set error: %v", err)
	}
}

// Network handlers
func getNetworkHandler(w http.ResponseWriter, r *http.Request) {
	cacheKey := "network_info"
	if cached, found := getCachedResponse(cacheKey); found {
		w.Header().Set("Content-Type", "application/json")
		w.Write(cached)
		return
	}

	// Query for network supply information
	query := `
		SELECT 
			(SELECT COALESCE(SUM(amount), 0) FROM tx_out txo 
			 JOIN tx ON txo.tx_id = tx.id 
			 WHERE tx.valid_contract = true) as total_supply,
			(SELECT COALESCE(SUM(amount), 0) FROM tx_out txo 
			 JOIN tx ON txo.tx_id = tx.id 
			 LEFT JOIN tx_in txi ON txo.tx_id = txi.tx_out_id AND txo.index = txi.tx_out_index
			 WHERE tx.valid_contract = true AND txi.tx_in_id IS NULL) as circulating_supply
	`

	var totalSupply, circulatingSupply int64
	err := db.QueryRowContext(ctx, query).Scan(&totalSupply, &circulatingSupply)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to query network info")
		return
	}

	networkInfo := NetworkInfo{
		Supply: NetworkSupply{
			Max:         "45000000000000000", // 45B ADA max supply
			Total:       strconv.FormatInt(totalSupply, 10),
			Circulating: strconv.FormatInt(circulatingSupply, 10),
			Locked:      "0",
		},
		Stake: NetworkStake{
			Live:   strconv.FormatInt(circulatingSupply, 10), // Simplified
			Active: strconv.FormatInt(circulatingSupply, 10), // Simplified
		},
	}

	data, _ := json.Marshal(networkInfo)
	setCachedResponse(cacheKey, data, 5*time.Minute)
	writeJSON(w, networkInfo)
}

// Block handlers
func getLatestBlockHandler(w http.ResponseWriter, r *http.Request) {
	cacheKey := "latest_block"
	if cached, found := getCachedResponse(cacheKey); found {
		w.Header().Set("Content-Type", "application/json")
		w.Write(cached)
		return
	}

	query := `
		SELECT b.hash, b.block_no, b.slot_no, b.epoch_no, b.epoch_slot_no,
			   EXTRACT(EPOCH FROM b.time)::bigint as time,
			   b.size, b.tx_count, COALESCE(b.vrf_key, '') as block_vrf,
			   COALESCE(prev.hash, '') as previous_block,
			   sl.description as slot_leader
		FROM block b
		LEFT JOIN block prev ON prev.block_no = b.block_no - 1
		LEFT JOIN slot_leader sl ON b.slot_leader_id = sl.id
		ORDER BY b.block_no DESC
		LIMIT 1
	`

	var block Block
	var slotLeader string
	err := db.QueryRowContext(ctx, query).Scan(
		&block.Hash, &block.Height, &block.Slot, &block.Epoch, &block.EpochSlot,
		&block.Time, &block.Size, &block.TxCount, &block.BlockVrf,
		&block.PreviousBlock, &slotLeader,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			writeError(w, http.StatusNotFound, "No blocks found")
		} else {
			writeError(w, http.StatusInternalServerError, "Failed to query latest block")
		}
		return
	}

	block.SlotLeader = slotLeader
	block.Confirmations = 0 // Latest block has 0 confirmations

	data, _ := json.Marshal(block)
	setCachedResponse(cacheKey, data, 30*time.Second)
	writeJSON(w, block)
}

func getBlockHandler(w http.ResponseWriter, r *http.Request) {
	hashOrNumber := strings.TrimPrefix(r.URL.Path, "/blocks/")
	if hashOrNumber == "" {
		writeError(w, http.StatusBadRequest, "Block hash or number required")
		return
	}

	cacheKey := "block_" + hashOrNumber
	if cached, found := getCachedResponse(cacheKey); found {
		w.Header().Set("Content-Type", "application/json")
		w.Write(cached)
		return
	}

	var query string
	var queryParam interface{}

	// Check if it's a number (block height) or hash
	if blockNum, err := strconv.ParseInt(hashOrNumber, 10, 64); err == nil {
		query = `
			SELECT b.hash, b.block_no, b.slot_no, b.epoch_no, b.epoch_slot_no,
				   EXTRACT(EPOCH FROM b.time)::bigint as time,
				   b.size, b.tx_count, COALESCE(b.vrf_key, '') as block_vrf,
				   COALESCE(prev.hash, '') as previous_block,
				   COALESCE(next.hash, '') as next_block,
				   sl.description as slot_leader
			FROM block b
			LEFT JOIN block prev ON prev.block_no = b.block_no - 1
			LEFT JOIN block next ON next.block_no = b.block_no + 1
			LEFT JOIN slot_leader sl ON b.slot_leader_id = sl.id
			WHERE b.block_no = $1
		`
		queryParam = blockNum
	} else {
		// Assume it's a hash
		decodedHash, err := hex.DecodeString(hashOrNumber)
		if err != nil {
			writeError(w, http.StatusBadRequest, "Invalid block hash format")
			return
		}
		query = `
			SELECT b.hash, b.block_no, b.slot_no, b.epoch_no, b.epoch_slot_no,
				   EXTRACT(EPOCH FROM b.time)::bigint as time,
				   b.size, b.tx_count, COALESCE(b.vrf_key, '') as block_vrf,
				   COALESCE(prev.hash, '') as previous_block,
				   COALESCE(next.hash, '') as next_block,
				   sl.description as slot_leader
			FROM block b
			LEFT JOIN block prev ON prev.block_no = b.block_no - 1
			LEFT JOIN block next ON next.block_no = b.block_no + 1
			LEFT JOIN slot_leader sl ON b.slot_leader_id = sl.id
			WHERE b.hash = $1
		`
		queryParam = decodedHash
	}

	var block Block
	var slotLeader, nextBlock string
	err := db.QueryRowContext(ctx, query, queryParam).Scan(
		&block.Hash, &block.Height, &block.Slot, &block.Epoch, &block.EpochSlot,
		&block.Time, &block.Size, &block.TxCount, &block.BlockVrf,
		&block.PreviousBlock, &nextBlock, &slotLeader,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			writeError(w, http.StatusNotFound, "Block not found")
		} else {
			log.Printf("DB error: %v", err)
			writeError(w, http.StatusInternalServerError, "Failed to query block")
		}
		return
	}

	block.SlotLeader = slotLeader
	if nextBlock != "" {
		block.NextBlock = &nextBlock
	}

	// Calculate confirmations (simplified - just use latest block number minus this block number)
	var latestBlockNum int64
	err = db.QueryRowContext(ctx, "SELECT MAX(block_no) FROM block").Scan(&latestBlockNum)
	if err == nil {
		block.Confirmations = latestBlockNum - block.Height
	}

	data, _ := json.Marshal(block)
	setCachedResponse(cacheKey, data, 2*time.Minute)
	writeJSON(w, block)
}

// Transaction handlers
func getTransactionHandler(w http.ResponseWriter, r *http.Request) {
	txHash := strings.TrimPrefix(r.URL.Path, "/txs/")
	if txHash == "" {
		writeError(w, http.StatusBadRequest, "Transaction hash required")
		return
	}

	decodedHash, err := hex.DecodeString(txHash)
	if err != nil {
		writeError(w, http.StatusBadRequest, "Invalid transaction hash format")
		return
	}

	cacheKey := "tx_" + txHash
	if cached, found := getCachedResponse(cacheKey); found {
		w.Header().Set("Content-Type", "application/json")
		w.Write(cached)
		return
	}

	query := `
		SELECT t.hash, b.hash as block_hash, b.block_no, 
			   EXTRACT(EPOCH FROM b.time)::bigint as block_time,
			   b.slot_no, t.block_index, t.out_sum, t.fee, t.deposit,
			   t.size, t.invalid_before, t.invalid_hereafter,
			   t.valid_contract
		FROM tx t
		JOIN block b ON t.block_id = b.id
		WHERE t.hash = $1
	`

	var tx Transaction
	var invalidBefore, invalidHereafter sql.NullString
	err = db.QueryRowContext(ctx, query, decodedHash).Scan(
		&tx.Hash, &tx.Block, &tx.BlockHeight, &tx.BlockTime,
		&tx.Slot, &tx.Index, &tx.OutputAmount, &tx.Fees, &tx.Deposit,
		&tx.Size, &invalidBefore, &invalidHereafter, &tx.ValidContract,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			writeError(w, http.StatusNotFound, "Transaction not found")
		} else {
			log.Printf("DB error: %v", err)
			writeError(w, http.StatusInternalServerError, "Failed to query transaction")
		}
		return
	}

	if invalidBefore.Valid {
		tx.InvalidBefore = &invalidBefore.String
	}
	if invalidHereafter.Valid {
		tx.InvalidHereafter = &invalidHereafter.String
	}

	// Convert hex hash back to string
	tx.Hash = hex.EncodeToString(decodedHash)

	data, _ := json.Marshal(tx)
	setCachedResponse(cacheKey, data, 5*time.Minute)
	writeJSON(w, tx)
}

// Address handlers  
func getAddressHandler(w http.ResponseWriter, r *http.Request) {
	address := strings.TrimPrefix(r.URL.Path, "/addresses/")
	if address == "" {
		writeError(w, http.StatusBadRequest, "Address required")
		return
	}

	cacheKey := "addr_" + address
	if cached, found := getCachedResponse(cacheKey); found {
		w.Header().Set("Content-Type", "application/json")
		w.Write(cached)
		return
	}

	// Query for address balance (optimized - ADA only first, then assets)
	query := `
		SELECT 
			COALESCE(SUM(txo.value), 0) as ada_balance,
			'[]'::json as assets
		FROM tx_out txo
		INNER JOIN tx ON txo.tx_id = tx.id
		WHERE txo.address_id = (SELECT id FROM address WHERE address = $1)
		  AND txo.consumed_by_tx_id IS NULL
		  AND tx.valid_contract = true
	`

	var adaBalance int64
	var assetsJSON sql.NullString
	err := db.QueryRowContext(ctx, query, address).Scan(&adaBalance, &assetsJSON)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to query address")
		return
	}

	addressInfo := AddressInfo{
		Address: address,
		Amount: []TransactionAmount{
			{Unit: "lovelace", Quantity: strconv.FormatInt(adaBalance, 10)},
		},
		Type: "shelley", // Simplified
		Script: false,   // Simplified
	}

	// Add assets if any
	if assetsJSON.Valid && assetsJSON.String != "null" {
		var assets []TransactionAmount
		if err := json.Unmarshal([]byte(assetsJSON.String), &assets); err == nil {
			addressInfo.Amount = append(addressInfo.Amount, assets...)
		}
	}

	data, _ := json.Marshal(addressInfo)
	setCachedResponse(cacheKey, data, 2*time.Minute)
	writeJSON(w, addressInfo)
}

// Asset handlers
func getAssetHandler(w http.ResponseWriter, r *http.Request) {
	assetId := strings.TrimPrefix(r.URL.Path, "/assets/")
	if assetId == "" {
		writeError(w, http.StatusBadRequest, "Asset ID required")
		return
	}

	if len(assetId) < 56 { // Policy ID should be at least 56 chars (28 bytes hex)
		writeError(w, http.StatusBadRequest, "Invalid asset ID format")
		return
	}

	policyId := assetId[:56]
	assetName := ""
	if len(assetId) > 56 {
		assetName = assetId[56:]
	}

	cacheKey := "asset_" + assetId
	if cached, found := getCachedResponse(cacheKey); found {
		w.Header().Set("Content-Type", "application/json")
		w.Write(cached)
		return
	}

	decodedPolicy, err := hex.DecodeString(policyId)
	if err != nil {
		writeError(w, http.StatusBadRequest, "Invalid policy ID format")
		return
	}

	var decodedName []byte
	if assetName != "" {
		decodedName, err = hex.DecodeString(assetName)
		if err != nil {
			writeError(w, http.StatusBadRequest, "Invalid asset name format")
			return
		}
	}

	query := `
		SELECT ma.policy, ma.name, 
			   COALESCE(SUM(mto.quantity), 0) as total_supply,
			   COUNT(DISTINCT mto.tx_out_id) as mint_count
		FROM multi_asset ma
		LEFT JOIN ma_tx_out mto ON ma.id = mto.ident
		LEFT JOIN tx_out txo ON mto.tx_out_id = txo.id
		LEFT JOIN tx_in txi ON txo.tx_id = txi.tx_out_id AND txo.index = txi.tx_out_index
		WHERE ma.policy = $1 AND ($2::bytea IS NULL OR ma.name = $2)
		  AND (txi.tx_in_id IS NULL OR mto.quantity < 0) -- Include unspent outputs or burns
		GROUP BY ma.policy, ma.name
	`

	var policy, name []byte
	var totalSupply, mintCount int64
	
	var nameParam interface{}
	if len(decodedName) > 0 {
		nameParam = decodedName
	} else {
		nameParam = nil
	}

	err = db.QueryRowContext(ctx, query, decodedPolicy, nameParam).Scan(&policy, &name, &totalSupply, &mintCount)
	if err != nil {
		if err == sql.ErrNoRows {
			writeError(w, http.StatusNotFound, "Asset not found")
		} else {
			log.Printf("DB error: %v", err)
			writeError(w, http.StatusInternalServerError, "Failed to query asset")
		}
		return
	}

	assetResponse := Asset{
		Asset:              assetId,
		PolicyId:           hex.EncodeToString(policy),
		Quantity:           strconv.FormatInt(totalSupply, 10),
		MintOrBurnCount:    mintCount,
		OnchainMetadata:    nil, // Would need to query tx metadata
		Metadata:           nil, // Would need external metadata service
	}

	if len(name) > 0 {
		nameStr := hex.EncodeToString(name)
		assetResponse.AssetName = &nameStr
	}

	data, _ := json.Marshal(assetResponse)
	setCachedResponse(cacheKey, data, 5*time.Minute)
	writeJSON(w, assetResponse)
}

// Epoch handlers
func getCurrentEpochHandler(w http.ResponseWriter, r *http.Request) {
	cacheKey := "current_epoch"
	if cached, found := getCachedResponse(cacheKey); found {
		w.Header().Set("Content-Type", "application/json")
		w.Write(cached)
		return
	}

	query := `
		SELECT e.no, EXTRACT(EPOCH FROM e.start_time)::bigint as start_time,
			   EXTRACT(EPOCH FROM e.end_time)::bigint as end_time,
			   e.blk_count, e.tx_count, e.out_sum, e.fees
		FROM epoch e
		ORDER BY e.no DESC
		LIMIT 1
	`

	var epoch Epoch
	err := db.QueryRowContext(ctx, query).Scan(
		&epoch.Epoch, &epoch.StartTime, &epoch.EndTime,
		&epoch.BlockCount, &epoch.TxCount, &epoch.Output, &epoch.Fees,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			writeError(w, http.StatusNotFound, "No epoch found")
		} else {
			writeError(w, http.StatusInternalServerError, "Failed to query current epoch")
		}
		return
	}

	data, _ := json.Marshal(epoch)
	setCachedResponse(cacheKey, data, 1*time.Minute)
	writeJSON(w, epoch)
}