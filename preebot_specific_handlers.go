package main

import (
	"database/sql"
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

// PREEBOT-specific response types matching Koios and Blockfrost formats
type PreebotAddressInfo struct {
	Address        string   `json:"address"`
	Balance        string   `json:"balance"`
	StakeAddress   *string  `json:"stake_address"`
	Type           string   `json:"type"`
	Script         bool     `json:"script"`
	UtxoCount      int      `json:"utxo_count"`
	TransactionCount int    `json:"tx_count"`
}

type AccountInfo struct {
	StakeAddress     string  `json:"stake_address"`
	Active           bool    `json:"active"`
	ActiveEpoch      *int    `json:"active_epoch"`
	ControlledAmount string  `json:"controlled_amount"`
	RewardsSum       string  `json:"rewards_sum"`
	WithdrawalsSum   string  `json:"withdrawals_sum"`
	ReservesSum      string  `json:"reserves_sum"`
	TreasurySum      string  `json:"treasury_sum"`
	WithdrawableAmount string `json:"withdrawable_amount"`
	PoolId           *string `json:"pool_id"`
}

type DelegationHistory struct {
	ActiveEpoch int     `json:"active_epoch"`
	TxHash      string  `json:"tx_hash"`
	Amount      string  `json:"amount"`
	PoolId      string  `json:"pool_id"`
}

type PoolInfo struct {
	PoolId      string  `json:"pool_id"`
	Hex         string  `json:"hex"`
	VrfKey      string  `json:"vrf_key"`
	BlocksMinted int64  `json:"blocks_minted"`
	LiveStake   string  `json:"live_stake"`
	LiveSize    float64 `json:"live_size"`
	LiveSaturation float64 `json:"live_saturation"`
	LiveDelegators int64 `json:"live_delegators"`
	ActiveStake string  `json:"active_stake"`
	ActiveSize  float64 `json:"active_size"`
	DeclaredPledge string `json:"declared_pledge"`
	LivePledge     string `json:"live_pledge"`
	MarginCost     float64 `json:"margin_cost"`
	FixedCost      string `json:"fixed_cost"`
	RewardAccount  string `json:"reward_account"`
	Owners         []string `json:"owners"`
	Registration   []string `json:"registration"`
	Retirement     []string `json:"retirement"`
}

type PoolBlock struct {
	Epoch       int    `json:"epoch"`
	Slot        int64  `json:"slot"`
	Height      int64  `json:"height"`
	Hash        string `json:"hash"`
	Time        int64  `json:"time"`
}

type AssetsByPolicy struct {
	AssetId   string `json:"asset_id"`
	PolicyId  string `json:"policy_id"`
	AssetName string `json:"asset_name"`
	Quantity  string `json:"quantity"`
}

type PreebotTransactionUTXO struct {
	Hash    string `json:"hash"`
	Inputs  []UTXO `json:"inputs"`
	Outputs []UTXO `json:"outputs"`
}

// PREEBOT Address Info - Drop-in replacement for Koios GetAddressesInfo
// GET /preebot-api/addresses/{address}
func preebotAddressInfoHandler(w http.ResponseWriter, r *http.Request) {
	// Extract address from URL path
	path := strings.TrimPrefix(r.URL.Path, "/preebot-api/addresses/")
	address := strings.Split(path, "/")[0]
	
	if address == "" {
		writeError(w, http.StatusBadRequest, "Address required")
		return
	}

	query := `
		SELECT 
			a.address,
			COALESCE(SUM(CASE WHEN txo.consumed_by_tx_id IS NULL THEN txo.value ELSE 0 END), 0) as balance,
			sa.view as stake_address,
			CASE WHEN a.has_script THEN 'script' ELSE 'payment' END as address_type,
			a.has_script,
			COUNT(CASE WHEN txo.consumed_by_tx_id IS NULL THEN 1 END) as utxo_count,
			COUNT(DISTINCT txo.tx_id) as tx_count
		FROM address a
		LEFT JOIN tx_out txo ON a.id = txo.address_id
		LEFT JOIN stake_address sa ON txo.stake_address_id = sa.id
		WHERE a.address = $1
		GROUP BY a.address, sa.view, a.has_script
	`

	var addressInfo PreebotAddressInfo
	var stakeAddr sql.NullString
	var balance int64

	err := db.QueryRowContext(ctx, query, address).Scan(
		&addressInfo.Address,
		&balance,
		&stakeAddr,
		&addressInfo.Type,
		&addressInfo.Script,
		&addressInfo.UtxoCount,
		&addressInfo.TransactionCount,
	)

	if err != nil {
		writeError(w, http.StatusNotFound, "Address not found")
		return
	}

	addressInfo.Balance = strconv.FormatInt(balance, 10)
	if stakeAddr.Valid {
		addressInfo.StakeAddress = &stakeAddr.String
	}

	writeJSON(w, addressInfo)
}

// PREEBOT Account Info - Drop-in replacement for Blockfrost Account endpoint
// GET /preebot-api/accounts/{stake_address}
func preebotAccountInfoHandler(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/preebot-api/accounts/")
	stakeAddress := strings.Split(path, "/")[0]
	
	if stakeAddress == "" {
		writeError(w, http.StatusBadRequest, "Stake address required")
		return
	}

	query := `
		SELECT 
			sa.view as stake_address,
			CASE WHEN dr.id IS NOT NULL THEN true ELSE false END as active,
			dr.active_epoch_no as active_epoch,
			COALESCE(SUM(txo.value), 0) as controlled_amount,
			COALESCE(SUM(r.amount), 0) as rewards_sum,
			COALESCE(SUM(w.amount), 0) as withdrawals_sum,
			ph.pool_id
		FROM stake_address sa
		LEFT JOIN delegation dr ON sa.id = dr.addr_id 
			AND dr.active_epoch_no <= (SELECT MAX(no) FROM epoch)
		LEFT JOIN tx_out txo ON sa.id = txo.stake_address_id 
			AND txo.consumed_by_tx_id IS NULL
		LEFT JOIN reward r ON sa.id = r.addr_id
		LEFT JOIN withdrawal w ON sa.id = w.addr_id
		LEFT JOIN pool_hash ph ON dr.pool_hash_id = ph.id
		WHERE sa.view = $1
		GROUP BY sa.view, dr.id, dr.active_epoch_no, ph.pool_id
	`

	var accountInfo AccountInfo
	var activeEpoch sql.NullInt64
	var poolId sql.NullString
	var controlledAmount, rewardsSum, withdrawalsSum int64

	err := db.QueryRowContext(ctx, query, stakeAddress).Scan(
		&accountInfo.StakeAddress,
		&accountInfo.Active,
		&activeEpoch,
		&controlledAmount,
		&rewardsSum,
		&withdrawalsSum,
		&poolId,
	)

	if err != nil {
		writeError(w, http.StatusNotFound, "Stake address not found")
		return
	}

	accountInfo.ControlledAmount = strconv.FormatInt(controlledAmount, 10)
	accountInfo.RewardsSum = strconv.FormatInt(rewardsSum, 10)
	accountInfo.WithdrawalsSum = strconv.FormatInt(withdrawalsSum, 10)
	accountInfo.ReservesSum = "0"
	accountInfo.TreasurySum = "0"
	accountInfo.WithdrawableAmount = strconv.FormatInt(rewardsSum-withdrawalsSum, 10)

	if activeEpoch.Valid {
		epoch := int(activeEpoch.Int64)
		accountInfo.ActiveEpoch = &epoch
	}
	if poolId.Valid {
		accountInfo.PoolId = &poolId.String
	}

	writeJSON(w, accountInfo)
}

// PREEBOT Delegation History - For role assignment based on stake
// GET /preebot-api/accounts/{stake_address}/history
func preebotDelegationHistoryHandler(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/preebot-api/accounts/")
	parts := strings.Split(path, "/")
	if len(parts) < 2 || parts[1] != "history" {
		writeError(w, http.StatusBadRequest, "Invalid path")
		return
	}
	stakeAddress := parts[0]

	query := `
		SELECT 
			d.active_epoch_no,
			encode(tx.hash, 'hex') as tx_hash,
			COALESCE(SUM(txo.value), 0) as amount,
			ph.pool_id
		FROM stake_address sa
		JOIN delegation d ON sa.id = d.addr_id
		JOIN tx ON d.tx_id = tx.id
		LEFT JOIN pool_hash ph ON d.pool_hash_id = ph.id
		LEFT JOIN tx_out txo ON sa.id = txo.stake_address_id 
			AND txo.consumed_by_tx_id IS NULL
		WHERE sa.view = $1
		GROUP BY d.active_epoch_no, tx.hash, ph.pool_id
		ORDER BY d.active_epoch_no DESC
		LIMIT 100
	`

	rows, err := db.QueryContext(ctx, query, stakeAddress)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to query delegation history")
		return
	}
	defer rows.Close()

	var history []DelegationHistory
	for rows.Next() {
		var delegation DelegationHistory
		var amount int64

		if err := rows.Scan(&delegation.ActiveEpoch, &delegation.TxHash, &amount, &delegation.PoolId); err != nil {
			continue
		}

		delegation.Amount = strconv.FormatInt(amount, 10)
		history = append(history, delegation)
	}

	writeJSON(w, history)
}

// PREEBOT Pool Information - Drop-in replacement for Blockfrost Pool endpoint
// GET /preebot-api/pools/{pool_id}
func preebotPoolInfoHandler(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/preebot-api/pools/")
	poolId := strings.Split(path, "/")[0]
	
	if poolId == "" {
		writeError(w, http.StatusBadRequest, "Pool ID required")
		return
	}

	query := `
		SELECT 
			ph.pool_id,
			encode(ph.hash_raw, 'hex') as hex,
			encode(pu.vrf_key_hash, 'hex') as vrf_key,
			COUNT(b.id) as blocks_minted,
			COALESCE(ps.live_stake, 0) as live_stake,
			COALESCE(ps.live_size, 0) as live_size,
			COALESCE(ps.live_saturation, 0) as live_saturation,
			COALESCE(ps.live_delegators, 0) as live_delegators,
			COALESCE(ps.active_stake, 0) as active_stake,
			COALESCE(ps.active_size, 0) as active_size,
			COALESCE(pu.pledge, 0) as declared_pledge,
			COALESCE(ps.live_pledge, 0) as live_pledge,
			COALESCE(pu.margin, 0) as margin_cost,
			COALESCE(pu.fixed_cost, 0) as fixed_cost,
			sa.view as reward_account
		FROM pool_hash ph
		LEFT JOIN pool_update pu ON ph.id = pu.hash_id
		LEFT JOIN pool_stat ps ON ph.id = ps.pool_hash_id
		LEFT JOIN block b ON ph.id = b.pool_id
		LEFT JOIN stake_address sa ON pu.reward_addr_id = sa.id
		WHERE ph.pool_id = $1
		GROUP BY ph.pool_id, ph.hash_raw, pu.vrf_key_hash, ps.live_stake, ps.live_size,
			ps.live_saturation, ps.live_delegators, ps.active_stake, ps.active_size,
			pu.pledge, ps.live_pledge, pu.margin, pu.fixed_cost, sa.view
		ORDER BY pu.id DESC
		LIMIT 1
	`

	var poolInfo PoolInfo
	var liveStake, activeStake, declaredPledge, livePledge, fixedCost int64

	err := db.QueryRowContext(ctx, query, poolId).Scan(
		&poolInfo.PoolId,
		&poolInfo.Hex,
		&poolInfo.VrfKey,
		&poolInfo.BlocksMinted,
		&liveStake,
		&poolInfo.LiveSize,
		&poolInfo.LiveSaturation,
		&poolInfo.LiveDelegators,
		&activeStake,
		&poolInfo.ActiveSize,
		&declaredPledge,
		&livePledge,
		&poolInfo.MarginCost,
		&fixedCost,
		&poolInfo.RewardAccount,
	)

	if err != nil {
		writeError(w, http.StatusNotFound, "Pool not found")
		return
	}

	poolInfo.LiveStake = strconv.FormatInt(liveStake, 10)
	poolInfo.ActiveStake = strconv.FormatInt(activeStake, 10)
	poolInfo.DeclaredPledge = strconv.FormatInt(declaredPledge, 10)
	poolInfo.LivePledge = strconv.FormatInt(livePledge, 10)
	poolInfo.FixedCost = strconv.FormatInt(fixedCost, 10)

	// Get owners and registration info
	ownerQuery := `
		SELECT sa.view
		FROM pool_owner po
		JOIN stake_address sa ON po.addr_id = sa.id
		JOIN pool_hash ph ON po.pool_hash_id = ph.id
		WHERE ph.pool_id = $1
	`
	
	ownerRows, err := db.QueryContext(ctx, ownerQuery, poolId)
	if err == nil {
		defer ownerRows.Close()
		for ownerRows.Next() {
			var owner string
			if ownerRows.Scan(&owner) == nil {
				poolInfo.Owners = append(poolInfo.Owners, owner)
			}
		}
	}

	writeJSON(w, poolInfo)
}

// PREEBOT Pool Blocks - For block production monitoring
// GET /preebot-api/pools/{pool_id}/blocks?epoch={epoch}
func preebotPoolBlocksHandler(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/preebot-api/pools/")
	parts := strings.Split(path, "/")
	if len(parts) < 2 || parts[1] != "blocks" {
		writeError(w, http.StatusBadRequest, "Invalid path")
		return
	}
	poolId := parts[0]

	// Optional epoch filter
	epochParam := r.URL.Query().Get("epoch")
	var epochFilter string
	var args []interface{}
	args = append(args, poolId)
	
	if epochParam != "" {
		epochFilter = " AND e.no = $2"
		if epoch, err := strconv.Atoi(epochParam); err == nil {
			args = append(args, epoch)
		}
	}

	query := fmt.Sprintf(`
		SELECT 
			e.no as epoch,
			b.slot_no,
			b.block_no as height,
			encode(b.hash, 'hex') as hash,
			extract(epoch from b.time)::bigint as time
		FROM block b
		JOIN pool_hash ph ON b.pool_id = ph.id
		JOIN epoch e ON b.epoch_no = e.no
		WHERE ph.pool_id = $1%s
		ORDER BY b.block_no DESC
		LIMIT 100
	`, epochFilter)

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to query pool blocks")
		return
	}
	defer rows.Close()

	var blocks []PoolBlock
	for rows.Next() {
		var block PoolBlock
		if err := rows.Scan(&block.Epoch, &block.Slot, &block.Height, &block.Hash, &block.Time); err != nil {
			continue
		}
		blocks = append(blocks, block)
	}

	writeJSON(w, blocks)
}

// PREEBOT Assets by Policy - Critical for role assignment
// GET /preebot-api/assets/policy/{policy_id}
func preebotAssetsByPolicyHandler(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/preebot-api/assets/policy/")
	policyId := strings.Split(path, "/")[0]
	
	if policyId == "" {
		writeError(w, http.StatusBadRequest, "Policy ID required")
		return
	}

	query := `
		SELECT 
			encode(ma.policy, 'hex') || encode(ma.name, 'hex') as asset_id,
			encode(ma.policy, 'hex') as policy_id,
			encode(ma.name, 'hex') as asset_name,
			COALESCE(SUM(CASE WHEN txo.consumed_by_tx_id IS NULL THEN mto.quantity ELSE 0 END), 0) as quantity
		FROM multi_asset ma
		JOIN ma_tx_out mto ON ma.id = mto.ident
		JOIN tx_out txo ON mto.tx_out_id = txo.id
		WHERE encode(ma.policy, 'hex') = $1
		  AND mto.quantity > 0
		GROUP BY ma.policy, ma.name
		HAVING SUM(CASE WHEN txo.consumed_by_tx_id IS NULL THEN mto.quantity ELSE 0 END) > 0
		ORDER BY asset_name
		LIMIT 1000
	`

	rows, err := db.QueryContext(ctx, query, policyId)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to query assets by policy")
		return
	}
	defer rows.Close()

	var assets []AssetsByPolicy
	for rows.Next() {
		var asset AssetsByPolicy
		var quantity int64
		
		if err := rows.Scan(&asset.AssetId, &asset.PolicyId, &asset.AssetName, &quantity); err != nil {
			continue
		}
		
		asset.Quantity = strconv.FormatInt(quantity, 10)
		assets = append(assets, asset)
	}

	writeJSON(w, assets)
}

// PREEBOT Account Associated Assets - For role assignment based on holdings
// GET /preebot-api/accounts/{stake_address}/assets
func preebotAccountAssetsHandler(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/preebot-api/accounts/")
	parts := strings.Split(path, "/")
	if len(parts) < 2 || parts[1] != "assets" {
		writeError(w, http.StatusBadRequest, "Invalid path")
		return
	}
	stakeAddress := parts[0]

	query := `
		SELECT 
			encode(ma.policy, 'hex') || encode(ma.name, 'hex') as asset_id,
			encode(ma.policy, 'hex') as policy_id,
			encode(ma.name, 'hex') as asset_name,
			SUM(mto.quantity) as quantity
		FROM stake_address sa
		JOIN tx_out txo ON sa.id = txo.stake_address_id
		JOIN ma_tx_out mto ON txo.id = mto.tx_out_id
		JOIN multi_asset ma ON mto.ident = ma.id
		WHERE sa.view = $1
		  AND txo.consumed_by_tx_id IS NULL
		  AND mto.quantity > 0
		GROUP BY ma.policy, ma.name
		ORDER BY quantity DESC
		LIMIT 1000
	`

	rows, err := db.QueryContext(ctx, query, stakeAddress)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to query account assets")
		return
	}
	defer rows.Close()

	var assets []AssetsByPolicy
	for rows.Next() {
		var asset AssetsByPolicy
		var quantity int64
		
		if err := rows.Scan(&asset.AssetId, &asset.PolicyId, &asset.AssetName, &quantity); err != nil {
			continue
		}
		
		asset.Quantity = strconv.FormatInt(quantity, 10)
		assets = append(assets, asset)
	}

	writeJSON(w, assets)
}

// PREEBOT Address Transactions - For wallet verification
// GET /preebot-api/addresses/{address}/transactions?from={from}&to={to}
func preebotAddressTransactionsHandler(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/preebot-api/addresses/")
	parts := strings.Split(path, "/")
	if len(parts) < 2 || parts[1] != "transactions" {
		writeError(w, http.StatusBadRequest, "Invalid path")
		return
	}
	address := parts[0]

	// Optional time range filters for verification window
	fromParam := r.URL.Query().Get("from")
	toParam := r.URL.Query().Get("to")
	
	var timeFilter string
	var args []interface{}
	args = append(args, address)
	
	if fromParam != "" && toParam != "" {
		timeFilter = " AND b.time >= to_timestamp($2) AND b.time <= to_timestamp($3)"
		if fromTime, err := strconv.ParseInt(fromParam, 10, 64); err == nil {
			args = append(args, fromTime)
			if toTime, err := strconv.ParseInt(toParam, 10, 64); err == nil {
				args = append(args, toTime)
			}
		}
	}

	query := fmt.Sprintf(`
		SELECT 
			encode(t.hash, 'hex') as tx_hash,
			b.block_no as block_height,
			extract(epoch from b.time)::bigint as block_time,
			txo.value,
			txo.index as output_index
		FROM address a
		JOIN tx_out txo ON a.id = txo.address_id
		JOIN tx t ON txo.tx_id = t.id
		JOIN block b ON t.block_id = b.id
		WHERE a.address = $1%s
		  AND t.valid_contract = true
		ORDER BY b.time DESC, txo.index ASC
		LIMIT 100
	`, timeFilter)

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to query address transactions")
		return
	}
	defer rows.Close()

	var transactions []map[string]interface{}
	for rows.Next() {
		var txHash string
		var blockHeight, blockTime, value, outputIndex int64
		
		if err := rows.Scan(&txHash, &blockHeight, &blockTime, &value, &outputIndex); err != nil {
			continue
		}
		
		tx := map[string]interface{}{
			"tx_hash":      txHash,
			"block_height": blockHeight,
			"block_time":   blockTime,
			"amount":       []map[string]string{{"unit": "lovelace", "quantity": strconv.FormatInt(value, 10)}},
			"output_index": outputIndex,
		}
		transactions = append(transactions, tx)
	}

	writeJSON(w, transactions)
}

// PREEBOT Transaction UTXOs - For detailed verification
// GET /preebot-api/txs/{tx_hash}/utxos
func preebotTransactionUTXOsHandler(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/preebot-api/txs/")
	parts := strings.Split(path, "/")
	if len(parts) < 2 || parts[1] != "utxos" {
		writeError(w, http.StatusBadRequest, "Invalid path")
		return
	}
	txHash := parts[0]

	// Get transaction inputs
	inputQuery := `
		SELECT 
			prev_a.address,
			prev_txo.value,
			prev_txo.index,
			encode(prev_t.hash, 'hex') as prev_tx_hash
		FROM tx t
		JOIN tx_in txi ON t.id = txi.tx_in_id
		JOIN tx_out prev_txo ON txi.tx_out_id = prev_txo.tx_id AND txi.tx_out_index = prev_txo.index
		JOIN tx prev_t ON prev_txo.tx_id = prev_t.id
		JOIN address prev_a ON prev_txo.address_id = prev_a.id
		WHERE encode(t.hash, 'hex') = $1
		ORDER BY txi.id
	`

	inputRows, err := db.QueryContext(ctx, inputQuery, txHash)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to query transaction inputs")
		return
	}
	defer inputRows.Close()

	var inputs []UTXO
	for inputRows.Next() {
		var input UTXO
		var value int64
		var prevTxHash string
		
		if err := inputRows.Scan(&input.Address, &value, &input.OutputIndex, &prevTxHash); err != nil {
			continue
		}
		
		input.Amount = []TransactionAmount{{Unit: "lovelace", Quantity: strconv.FormatInt(value, 10)}}
		input.TxHash = prevTxHash
		inputs = append(inputs, input)
	}

	// Get transaction outputs
	outputQuery := `
		SELECT 
			a.address,
			txo.value,
			txo.index
		FROM tx t
		JOIN tx_out txo ON t.id = txo.tx_id
		JOIN address a ON txo.address_id = a.id
		WHERE encode(t.hash, 'hex') = $1
		ORDER BY txo.index
	`

	outputRows, err := db.QueryContext(ctx, outputQuery, txHash)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to query transaction outputs")
		return
	}
	defer outputRows.Close()

	var outputs []UTXO
	for outputRows.Next() {
		var output UTXO
		var value int64
		
		if err := outputRows.Scan(&output.Address, &value, &output.OutputIndex); err != nil {
			continue
		}
		
		output.Amount = []TransactionAmount{{Unit: "lovelace", Quantity: strconv.FormatInt(value, 10)}}
		output.TxHash = txHash
		outputs = append(outputs, output)
	}

	response := PreebotTransactionUTXO{
		Hash:    txHash,
		Inputs:  inputs,
		Outputs: outputs,
	}

	writeJSON(w, response)
}

// PREEBOT Blockchain Tip - For sync status
// GET /preebot-api/tip
func preebotBlockchainTipHandler(w http.ResponseWriter, r *http.Request) {
	query := `
		SELECT 
			b.block_no as height,
			encode(b.hash, 'hex') as hash,
			b.slot_no as slot,
			e.no as epoch,
			extract(epoch from b.time)::bigint as time
		FROM block b
		JOIN epoch e ON b.epoch_no = e.no
		ORDER BY b.block_no DESC
		LIMIT 1
	`

	var tip map[string]interface{}
	var height, slot, epoch, blockTime int64
	var hash string

	err := db.QueryRowContext(ctx, query).Scan(&height, &hash, &slot, &epoch, &blockTime)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to query blockchain tip")
		return
	}

	tip = map[string]interface{}{
		"height": height,
		"hash":   hash,
		"slot":   slot,
		"epoch":  epoch,
		"time":   blockTime,
	}

	writeJSON(w, tip)
}

// PREEBOT Asset Minting History - For tracking new mints
// GET /preebot-api/assets/policy/{policy_id}/mints?order={asc|desc}
func preebotAssetMintsHandler(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/preebot-api/assets/policy/")
	parts := strings.Split(path, "/")
	if len(parts) < 2 || parts[1] != "mints" {
		writeError(w, http.StatusBadRequest, "Invalid path")
		return
	}
	policyId := parts[0]

	// Order parameter (asc/desc) - PREEBOT expects creation_time sorting
	order := r.URL.Query().Get("order")
	if order != "asc" && order != "desc" {
		order = "desc" // Default to newest first
	}

	query := fmt.Sprintf(`
		SELECT 
			encode(ma.policy, 'hex') || encode(ma.name, 'hex') as asset_id,
			encode(ma.name, 'hex') as asset_name,
			mto.quantity,
			encode(t.hash, 'hex') as tx_hash,
			extract(epoch from b.time)::bigint as creation_time,
			b.block_no
		FROM multi_asset ma
		JOIN ma_tx_out mto ON ma.id = mto.ident
		JOIN tx_out txo ON mto.tx_out_id = txo.id
		JOIN tx t ON txo.tx_id = t.id
		JOIN block b ON t.block_id = b.id
		WHERE encode(ma.policy, 'hex') = $1
		  AND mto.quantity > 0
		  AND t.valid_contract = true
		  -- Only get the first minting transaction per asset
		  AND t.id = (
		      SELECT MIN(t2.id) 
		      FROM ma_tx_out mto2 
		      JOIN tx_out txo2 ON mto2.tx_out_id = txo2.id
		      JOIN tx t2 ON txo2.tx_id = t2.id
		      WHERE mto2.ident = ma.id AND mto2.quantity > 0
		  )
		ORDER BY b.time %s
		LIMIT 1000
	`, order)

	rows, err := db.QueryContext(ctx, query, policyId)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to query asset mints")
		return
	}
	defer rows.Close()

	var mints []map[string]interface{}
	for rows.Next() {
		var assetId, assetName, txHash string
		var quantity, creationTime, blockNo int64
		
		if err := rows.Scan(&assetId, &assetName, &quantity, &txHash, &creationTime, &blockNo); err != nil {
			continue
		}
		
		mint := map[string]interface{}{
			"asset_id":      assetId,
			"asset_name":    assetName,
			"quantity":      strconv.FormatInt(quantity, 10),
			"tx_hash":       txHash,
			"creation_time": creationTime,
			"block_number":  blockNo,
		}
		mints = append(mints, mint)
	}

	writeJSON(w, mints)
}

// PREEBOT ADA Handle Resolution - Drop-in replacement for handle.me API
// GET /preebot-api/handles/{handle}
func preebotHandleResolutionHandler(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/preebot-api/handles/")
	handle := strings.Split(path, "/")[0]
	
	if handle == "" {
		writeError(w, http.StatusBadRequest, "Handle required")
		return
	}

	// Remove $ prefix if present
	handle = strings.TrimPrefix(handle, "$")

	// Query handle resolution from tx_metadata (ADA handles are stored as metadata)
	query := `
		SELECT DISTINCT
			a.address
		FROM tx_metadata tm
		JOIN tx t ON tm.tx_id = t.id
		JOIN tx_out txo ON t.id = txo.tx_id
		JOIN address a ON txo.address_id = a.id
		WHERE tm.key = 222  -- Handle metadata key
		  AND tm.json ? $1   -- Handle name exists in metadata
		  AND txo.consumed_by_tx_id IS NULL  -- Current holder
		  AND t.valid_contract = true
		ORDER BY t.id DESC
		LIMIT 1
	`

	var resolvedAddress string
	err := db.QueryRowContext(ctx, query, handle).Scan(&resolvedAddress)
	
	if err != nil {
		// Fallback to external handle.me API if not found in local data
		// This ensures compatibility while we build up local handle data
		response := map[string]interface{}{
			"handle":      handle,
			"address":     "",
			"resolved":    false,
			"message":     "Handle not found in local database - consider using external handle.me API as fallback",
		}
		writeJSON(w, response)
		return
	}

	response := map[string]interface{}{
		"handle":   handle,
		"address":  resolvedAddress,
		"resolved": true,
	}

	writeJSON(w, response)
}

// PREEBOT Asset UTXOs - For specific asset tracking
// GET /preebot-api/assets/{asset_id}/utxos
func preebotAssetUTXOsHandler(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/preebot-api/assets/")
	parts := strings.Split(path, "/")
	if len(parts) < 2 || parts[1] != "utxos" {
		writeError(w, http.StatusBadRequest, "Invalid path")
		return
	}
	assetId := parts[0]

	if len(assetId) < 56 {
		writeError(w, http.StatusBadRequest, "Invalid asset ID format")
		return
	}

	policyId := assetId[:56]
	assetName := assetId[56:]

	query := `
		SELECT 
			a.address,
			encode(t.hash, 'hex') as tx_hash,
			txo.index as output_index,
			mto.quantity,
			txo.value as ada_amount
		FROM multi_asset ma
		JOIN ma_tx_out mto ON ma.id = mto.ident
		JOIN tx_out txo ON mto.tx_out_id = txo.id
		JOIN tx t ON txo.tx_id = t.id
		JOIN address a ON txo.address_id = a.id
		WHERE encode(ma.policy, 'hex') = $1
		  AND encode(ma.name, 'hex') = $2
		  AND txo.consumed_by_tx_id IS NULL
		  AND mto.quantity > 0
		ORDER BY txo.id
		LIMIT 1000
	`

	rows, err := db.QueryContext(ctx, query, policyId, assetName)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to query asset UTXOs")
		return
	}
	defer rows.Close()

	var utxos []map[string]interface{}
	for rows.Next() {
		var address, txHash string
		var outputIndex, quantity, adaAmount int64
		
		if err := rows.Scan(&address, &txHash, &outputIndex, &quantity, &adaAmount); err != nil {
			continue
		}
		
		utxo := map[string]interface{}{
			"address":      address,
			"tx_hash":      txHash,
			"output_index": outputIndex,
			"amount": []map[string]string{
				{"unit": "lovelace", "quantity": strconv.FormatInt(adaAmount, 10)},
				{"unit": assetId, "quantity": strconv.FormatInt(quantity, 10)},
			},
		}
		utxos = append(utxos, utxo)
	}

	writeJSON(w, utxos)
}

// PREEBOT Pagination Helper - Add pagination to any endpoint
func addPagination(r *http.Request) (int, int) {
	page := 1
	count := 100

	if pageParam := r.URL.Query().Get("page"); pageParam != "" {
		if p, err := strconv.Atoi(pageParam); err == nil && p > 0 {
			page = p
		}
	}

	if countParam := r.URL.Query().Get("count"); countParam != "" {
		if c, err := strconv.Atoi(countParam); err == nil && c > 0 && c <= 1000 {
			count = c
		}
	}

	return page, count
}