package main

import (
	"encoding/hex"
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Additional endpoints for more comprehensive Blockfrost compatibility

// Address transactions handler
func getAddressTransactionsHandler(w http.ResponseWriter, r *http.Request) {
	address := strings.TrimPrefix(r.URL.Path, "/addresses/")
	if strings.HasSuffix(address, "/transactions") {
		address = strings.TrimSuffix(address, "/transactions")
	}

	if address == "" {
		writeError(w, http.StatusBadRequest, "Address required")
		return
	}

	count, page, order := getPaginationParams(r)
	offset := (page - 1) * count
	orderBy := "DESC"
	if order == "asc" {
		orderBy = "ASC"
	}

	cacheKey := "addr_txs_" + address + "_" + strconv.Itoa(page) + "_" + strconv.Itoa(count) + "_" + order
	if cached, found := getCachedResponse(cacheKey); found {
		w.Header().Set("Content-Type", "application/json")
		w.Write(cached)
		return
	}

	query := `
		SELECT DISTINCT encode(t.hash, 'hex') as tx_hash, t.block_index, b.block_no, 
			   EXTRACT(EPOCH FROM b.time)::bigint as block_time
		FROM tx t
		JOIN block b ON t.block_id = b.id
		JOIN tx_out txo ON t.id = txo.tx_id
		JOIN address addr ON txo.address_id = addr.id
		WHERE addr.address = $1
		UNION
		SELECT DISTINCT encode(t.hash, 'hex') as tx_hash, t.block_index, b.block_no,
			   EXTRACT(EPOCH FROM b.time)::bigint as block_time
		FROM tx t
		JOIN block b ON t.block_id = b.id
		JOIN tx_in txi ON t.id = txi.tx_in_id
		JOIN tx_out txo ON txi.tx_out_id = txo.tx_id AND txi.tx_out_index = txo.index
		JOIN address addr ON txo.address_id = addr.id
		WHERE addr.address = $1
		ORDER BY block_no ` + orderBy + `, block_index ` + orderBy + `
		LIMIT $2 OFFSET $3
	`

	rows, err := db.QueryContext(ctx, query, address, count, offset)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to query address transactions")
		return
	}
	defer rows.Close()

	var transactions []AddressTransaction
	for rows.Next() {
		var tx AddressTransaction
		err := rows.Scan(&tx.TxHash, &tx.TxIndex, &tx.BlockHeight, &tx.BlockTime)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "Error scanning transaction")
			return
		}
		transactions = append(transactions, tx)
	}

	data, _ := json.Marshal(transactions)
	setCachedResponse(cacheKey, data, 2*time.Minute)
	writeJSON(w, transactions)
}

// Address UTXOs handler
func getAddressUTXOsHandler(w http.ResponseWriter, r *http.Request) {
	address := strings.TrimPrefix(r.URL.Path, "/addresses/")
	if strings.HasSuffix(address, "/utxos") {
		address = strings.TrimSuffix(address, "/utxos")
	}

	if address == "" {
		writeError(w, http.StatusBadRequest, "Address required")
		return
	}

	count, page, order := getPaginationParams(r)
	offset := (page - 1) * count
	orderBy := "DESC"
	if order == "asc" {
		orderBy = "ASC"
	}

	cacheKey := "addr_utxos_" + address + "_" + strconv.Itoa(page) + "_" + strconv.Itoa(count) + "_" + order
	if cached, found := getCachedResponse(cacheKey); found {
		w.Header().Set("Content-Type", "application/json")
		w.Write(cached)
		return
	}

	query := `
		SELECT encode(t.hash, 'hex') as tx_hash, txo.index, txo.value,
			   COALESCE(json_agg(
				   CASE WHEN ma.policy IS NOT NULL THEN
					   json_build_object(
						   'unit', CONCAT(encode(ma.policy, 'hex'), encode(ma.name, 'hex')),
						   'quantity', mto.quantity::text
					   )
				   END
			   ) FILTER (WHERE ma.policy IS NOT NULL), '[]'::json) as assets
		FROM tx_out txo
		JOIN tx t ON txo.tx_id = t.id
		JOIN address addr ON txo.address_id = addr.id
		LEFT JOIN tx_in txi ON txo.tx_id = txi.tx_out_id AND txo.index = txi.tx_out_index
		LEFT JOIN ma_tx_out mto ON mto.tx_out_id = txo.id
		LEFT JOIN multi_asset ma ON mto.ident = ma.id
		WHERE addr.address = $1 AND txi.tx_in_id IS NULL
		GROUP BY t.hash, txo.index, txo.value, txo.id
		ORDER BY txo.id ` + orderBy + `
		LIMIT $2 OFFSET $3
	`

	rows, err := db.QueryContext(ctx, query, address, count, offset)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to query address UTXOs")
		return
	}
	defer rows.Close()

	var utxos []UTXO
	for rows.Next() {
		var utxo UTXO
		var assetsJSON string
		var value int64

		err := rows.Scan(&utxo.TxHash, &utxo.OutputIndex, &value, &assetsJSON)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "Error scanning UTXO")
			return
		}

		utxo.Address = address
		utxo.Amount = []TransactionAmount{
			{Unit: "lovelace", Quantity: strconv.FormatInt(value, 10)},
		}

		// Parse assets
		var assets []TransactionAmount
		if assetsJSON != "[]" {
			if err := json.Unmarshal([]byte(assetsJSON), &assets); err == nil {
				utxo.Amount = append(utxo.Amount, assets...)
			}
		}

		utxos = append(utxos, utxo)
	}

	data, _ := json.Marshal(utxos)
	setCachedResponse(cacheKey, data, 1*time.Minute)
	writeJSON(w, utxos)
}

// Transaction UTXOs handler
func getTransactionUTXOsHandler(w http.ResponseWriter, r *http.Request) {
	txHash := strings.TrimPrefix(r.URL.Path, "/txs/")
	if strings.HasSuffix(txHash, "/utxos") {
		txHash = strings.TrimSuffix(txHash, "/utxos")
	}

	if txHash == "" {
		writeError(w, http.StatusBadRequest, "Transaction hash required")
		return
	}

	decodedHash, err := hex.DecodeString(txHash)
	if err != nil {
		writeError(w, http.StatusBadRequest, "Invalid transaction hash format")
		return
	}

	cacheKey := "tx_utxos_" + txHash
	if cached, found := getCachedResponse(cacheKey); found {
		w.Header().Set("Content-Type", "application/json")
		w.Write(cached)
		return
	}

	// Get inputs
	inputQuery := `
		SELECT addr.address, prev_txo.value, prev_txo.index as output_index,
			   encode(prev_tx.hash, 'hex') as tx_hash,
			   COALESCE(json_agg(
				   CASE WHEN ma.policy IS NOT NULL THEN
					   json_build_object(
						   'unit', CONCAT(encode(ma.policy, 'hex'), encode(ma.name, 'hex')),
						   'quantity', mto.quantity::text
					   )
				   END
			   ) FILTER (WHERE ma.policy IS NOT NULL), '[]'::json) as assets
		FROM tx_in txi
		JOIN tx t ON txi.tx_in_id = t.id
		JOIN tx_out prev_txo ON txi.tx_out_id = prev_txo.tx_id AND txi.tx_out_index = prev_txo.index
		JOIN tx prev_tx ON prev_txo.tx_id = prev_tx.id
		JOIN address addr ON prev_txo.address_id = addr.id
		LEFT JOIN ma_tx_out mto ON mto.tx_out_id = prev_txo.id
		LEFT JOIN multi_asset ma ON mto.ident = ma.id
		WHERE t.hash = $1
		GROUP BY addr.address, prev_txo.value, prev_txo.index, prev_tx.hash
	`

	// Get outputs  
	outputQuery := `
		SELECT addr.address, txo.value, txo.index as output_index,
			   COALESCE(json_agg(
				   CASE WHEN ma.policy IS NOT NULL THEN
					   json_build_object(
						   'unit', CONCAT(encode(ma.policy, 'hex'), encode(ma.name, 'hex')),
						   'quantity', mto.quantity::text
					   )
				   END
			   ) FILTER (WHERE ma.policy IS NOT NULL), '[]'::json) as assets
		FROM tx_out txo
		JOIN tx t ON txo.tx_id = t.id
		JOIN address addr ON txo.address_id = addr.id
		LEFT JOIN ma_tx_out mto ON mto.tx_out_id = txo.id
		LEFT JOIN multi_asset ma ON mto.ident = ma.id
		WHERE t.hash = $1
		GROUP BY addr.address, txo.value, txo.index
		ORDER BY txo.index
	`

	var inputs, outputs []UTXO

	// Process inputs
	rows, err := db.QueryContext(ctx, inputQuery, decodedHash)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to query transaction inputs")
		return
	}
	defer rows.Close()

	for rows.Next() {
		var utxo UTXO
		var assetsJSON string
		var value int64

		err := rows.Scan(&utxo.Address, &value, &utxo.OutputIndex, &utxo.TxHash, &assetsJSON)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "Error scanning input")
			return
		}

		utxo.Amount = []TransactionAmount{
			{Unit: "lovelace", Quantity: strconv.FormatInt(value, 10)},
		}

		var assets []TransactionAmount
		if assetsJSON != "[]" {
			if err := json.Unmarshal([]byte(assetsJSON), &assets); err == nil {
				utxo.Amount = append(utxo.Amount, assets...)
			}
		}

		inputs = append(inputs, utxo)
	}

	// Process outputs
	rows, err = db.QueryContext(ctx, outputQuery, decodedHash)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to query transaction outputs")
		return
	}
	defer rows.Close()

	for rows.Next() {
		var utxo UTXO
		var assetsJSON string
		var value int64

		err := rows.Scan(&utxo.Address, &value, &utxo.OutputIndex, &assetsJSON)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "Error scanning output")
			return
		}

		utxo.TxHash = txHash
		utxo.Amount = []TransactionAmount{
			{Unit: "lovelace", Quantity: strconv.FormatInt(value, 10)},
		}

		var assets []TransactionAmount
		if assetsJSON != "[]" {
			if err := json.Unmarshal([]byte(assetsJSON), &assets); err == nil {
				utxo.Amount = append(utxo.Amount, assets...)
			}
		}

		outputs = append(outputs, utxo)
	}

	utxoResponse := TransactionUTXO{
		Inputs:  inputs,
		Outputs: outputs,
	}

	data, _ := json.Marshal(utxoResponse)
	setCachedResponse(cacheKey, data, 5*time.Minute)
	writeJSON(w, utxoResponse)
}

// Asset addresses handler
func getAssetAddressesHandler(w http.ResponseWriter, r *http.Request) {
	assetId := strings.TrimPrefix(r.URL.Path, "/assets/")
	if strings.HasSuffix(assetId, "/addresses") {
		assetId = strings.TrimSuffix(assetId, "/addresses")
	}

	if assetId == "" {
		writeError(w, http.StatusBadRequest, "Asset ID required")
		return
	}

	if len(assetId) < 56 {
		writeError(w, http.StatusBadRequest, "Invalid asset ID format")
		return
	}

	policyId := assetId[:56]
	assetName := ""
	if len(assetId) > 56 {
		assetName = assetId[56:]
	}

	count, page, order := getPaginationParams(r)
	offset := (page - 1) * count
	orderBy := "DESC"
	if order == "asc" {
		orderBy = "ASC"
	}

	cacheKey := "asset_addrs_" + assetId + "_" + strconv.Itoa(page) + "_" + strconv.Itoa(count) + "_" + order
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
		SELECT addr.address, SUM(mto.quantity)::TEXT as quantity
		FROM ma_tx_out mto
		JOIN tx_out txo ON mto.tx_out_id = txo.id
		JOIN address addr ON txo.address_id = addr.id
		JOIN multi_asset ma ON mto.ident = ma.id
		LEFT JOIN tx_in txi ON txo.tx_id = txi.tx_out_id AND txo.index = txi.tx_out_index
		WHERE ma.policy = $1 AND ($2::bytea IS NULL OR ma.name = $2)
		  AND txi.tx_in_id IS NULL
		  AND mto.quantity > 0
		GROUP BY addr.address
		HAVING SUM(mto.quantity) > 0
		ORDER BY SUM(mto.quantity) ` + orderBy + `
		LIMIT $3 OFFSET $4
	`

	var nameParam interface{}
	if len(decodedName) > 0 {
		nameParam = decodedName
	} else {
		nameParam = nil
	}

	rows, err := db.QueryContext(ctx, query, decodedPolicy, nameParam, count, offset)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to query asset addresses")
		log.Printf("DB error: %v", err)
		return
	}
	defer rows.Close()

	var addresses []AssetAddress
	for rows.Next() {
		var addr AssetAddress
		err := rows.Scan(&addr.Address, &addr.Quantity)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "Error scanning asset address")
			return
		}
		addresses = append(addresses, addr)
	}

	data, _ := json.Marshal(addresses)
	setCachedResponse(cacheKey, data, 2*time.Minute)
	writeJSON(w, addresses)
}