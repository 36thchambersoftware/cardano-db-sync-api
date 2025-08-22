package main

// Common response types
type ErrorResponse struct {
	StatusCode int    `json:"status_code"`
	Error      string `json:"error"`
	Message    string `json:"message"`
}

// Network types
type NetworkInfo struct {
	Supply        NetworkSupply `json:"supply"`
	Stake         NetworkStake  `json:"stake"`
}

type NetworkSupply struct {
	Max         string `json:"max"`
	Total       string `json:"total"`
	Circulating string `json:"circulating"`
	Locked      string `json:"locked"`
}

type NetworkStake struct {
	Live   string `json:"live"`
	Active string `json:"active"`
}

// Block types
type Block struct {
	Time         int64  `json:"time"`
	Height       int64  `json:"height,omitempty"`
	Hash         string `json:"hash"`
	Slot         int64  `json:"slot,omitempty"`
	Epoch        int64  `json:"epoch,omitempty"`
	EpochSlot    int64  `json:"epoch_slot,omitempty"`
	SlotLeader   string `json:"slot_leader"`
	Size         int64  `json:"size"`
	TxCount      int64  `json:"tx_count"`
	Output       string `json:"output,omitempty"`
	Fees         string `json:"fees,omitempty"`
	BlockVrf     string  `json:"block_vrf,omitempty"`
	PreviousBlock string  `json:"previous_block,omitempty"`
	NextBlock    *string `json:"next_block,omitempty"`
	Confirmations int64   `json:"confirmations"`
}

// Transaction types
type Transaction struct {
	Hash             string                   `json:"hash"`
	Block            string                   `json:"block"`
	BlockHeight      int64                    `json:"block_height"`
	BlockTime        int64                    `json:"block_time"`
	Slot             int64                    `json:"slot"`
	Index            int64                    `json:"index"`
	OutputAmount     []TransactionAmount      `json:"output_amount"`
	Fees             string                   `json:"fees"`
	Deposit          string                   `json:"deposit"`
	Size             int64                    `json:"size"`
	InvalidBefore    *string                  `json:"invalid_before"`
	InvalidHereafter *string                  `json:"invalid_hereafter"`
	UtxoCount        int64                    `json:"utxo_count"`
	WithdrawalCount  int64                    `json:"withdrawal_count"`
	MirCertCount     int64                    `json:"mir_cert_count"`
	DelegationCount  int64                    `json:"delegation_count"`
	StakeCertCount   int64                    `json:"stake_cert_count"`
	PoolUpdateCount  int64                    `json:"pool_update_count"`
	PoolRetireCount  int64                    `json:"pool_retire_count"`
	AssetMintCount   int64                    `json:"asset_mint_count"`
	RedeemerCount    int64                    `json:"redeemer_count"`
	ValidContract    bool                     `json:"valid_contract"`
}

type TransactionAmount struct {
	Unit     string `json:"unit"`
	Quantity string `json:"quantity"`
}

type TransactionUTXO struct {
	Inputs  []UTXO `json:"inputs"`
	Outputs []UTXO `json:"outputs"`
}

type UTXO struct {
	Address       string              `json:"address"`
	Amount        []TransactionAmount `json:"amount"`
	OutputIndex   int64               `json:"output_index,omitempty"`
	DataHash      *string             `json:"data_hash,omitempty"`
	InlineDatum   *string             `json:"inline_datum,omitempty"`
	ReferenceScriptHash *string       `json:"reference_script_hash,omitempty"`
	TxHash        string              `json:"tx_hash,omitempty"`
}

// Address types
type AddressInfo struct {
	Address      string              `json:"address"`
	Amount       []TransactionAmount `json:"amount"`
	StakeAddress *string             `json:"stake_address"`
	Type         string              `json:"type"`
	Script       bool                `json:"script"`
}

type AddressTransaction struct {
	TxHash      string `json:"tx_hash"`
	TxIndex     int64  `json:"tx_index"`
	BlockHeight int64  `json:"block_height"`
	BlockTime   int64  `json:"block_time"`
}

// Asset types
type Asset struct {
	Asset              string      `json:"asset"`
	PolicyId           string      `json:"policy_id"`
	AssetName          *string     `json:"asset_name"`
	Fingerprint        string      `json:"fingerprint"`
	Quantity           string      `json:"quantity"`
	InitialMintTxHash  string      `json:"initial_mint_tx_hash"`
	MintOrBurnCount    int64       `json:"mint_or_burn_count"`
	OnchainMetadata    interface{} `json:"onchain_metadata"`
	Metadata           interface{} `json:"metadata"`
}

type AssetAddress struct {
	Address  string `json:"address"`
	Quantity string `json:"quantity"`
}

// Epoch types
type Epoch struct {
	Epoch          int64   `json:"epoch"`
	StartTime      int64   `json:"start_time"`
	EndTime        int64   `json:"end_time"`
	FirstBlockTime int64   `json:"first_block_time"`
	LastBlockTime  int64   `json:"last_block_time"`
	BlockCount     int64   `json:"block_count"`
	TxCount        int64   `json:"tx_count"`
	Output         string  `json:"output"`
	Fees           string  `json:"fees"`
	ActiveStake    *string `json:"active_stake"`
}

// Pool types
type Pool struct {
	PoolId      string `json:"pool_id"`
	Hex         string `json:"hex"`
	VrfKey      string `json:"vrf_key"`
	BlocksMinted int64 `json:"blocks_minted"`
	BlocksEpoch int64  `json:"blocks_epoch"`
	LiveStake   string `json:"live_stake"`
	LiveSize    float64 `json:"live_size"`
	LiveSaturation float64 `json:"live_saturation"`
	LiveDelegators int64 `json:"live_delegators"`
	ActiveStake string `json:"active_stake"`
	ActiveSize  float64 `json:"active_size"`
	DeclaredPledge string `json:"declared_pledge"`
	LivePledge    string `json:"live_pledge"`
	MarginCost    float64 `json:"margin_cost"`
	FixedCost     string `json:"fixed_cost"`
	RewardAccount string `json:"reward_account"`
	Owners        []string `json:"owners"`
	Registration  []string `json:"registration"`
	Retirement    []string `json:"retirement"`
}

// Pagination
type PaginationQuery struct {
	Count int    `json:"count,omitempty"`
	Page  int    `json:"page,omitempty"`
	Order string `json:"order,omitempty"`
}

// Legacy NFT response type
type NFTResponse struct {
	Address string `json:"address"`
	Count   int64  `json:"count"`
}