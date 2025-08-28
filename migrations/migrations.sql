-- ========================================
-- CARDANO DB SYNC API OPTIMIZATIONS
-- ========================================
-- Essential indexes for high-performance Cardano blockchain queries
-- Run with: psql -d your_db_name -f migrations.sql

-- Stop the 108M row scan for stake address queries
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_tx_out_stake_address_id ON tx_out(stake_address_id);

-- Speed up drep lookups  
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_drep_registration_active
  ON drep_registration(drep_hash_id, tx_id DESC, cert_index DESC)
  WHERE deposit > 0;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_drep_registration_inactive
  ON drep_registration(drep_hash_id)
  WHERE deposit < 0;

-- Speed up delegation checks
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_delegation_vote_lookup
  ON delegation_vote(addr_id, drep_hash_id, tx_id DESC);

-- Speed up reward and withdrawal queries
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_reward_addr_epoch
  ON reward(addr_id, spendable_epoch, type);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_withdrawal_addr_epoch
  ON withdrawal(addr_id);

-- Jump straight to outputs for stake address lookups
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_tx_out_stake_view
ON tx_out(stake_address_id, tx_id, index, id)
INCLUDE (value);

-- Fast lookup for stake_address.view
CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS idx_stake_address_view
ON stake_address(view);

-- Speed up spent check (tx_in lookup)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_tx_in_src
ON tx_in(tx_out_id, tx_out_index);

-- Multi-asset joins optimization
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ma_tx_out_tx_out_id
ON ma_tx_out(tx_out_id, ident);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_multi_asset_ident
ON multi_asset(id, policy, name);

-- ========================================
-- CRITICAL PERFORMANCE INDEXES
-- ========================================
-- These indexes fix the major performance bottlenecks identified

-- CRITICAL: Address lookup (fixes 20+ second queries down to <100ms)
-- This is the most important index - without it, all address queries are extremely slow
CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS idx_address_main
ON address (address);

-- CRITICAL: Ultra-fast UTXO lookups by address (primary query pattern)
-- Optimized specifically for balance queries with consumed_by_tx_id IS NULL
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_tx_out_ultra_fast
ON tx_out (address_id, consumed_by_tx_id, tx_id, value)
WHERE consumed_by_tx_id IS NULL;

-- CRITICAL: Transaction validation lookups (used in most queries)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_tx_ultra_fast
ON tx (id, valid_contract)
WHERE valid_contract = true;

-- Block lookups by number and hash (API endpoints)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_block_block_no 
ON block (block_no DESC);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_block_hash 
ON block (hash);

-- Transaction lookups by hash (API endpoints)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_tx_hash 
ON tx (hash);

-- Multi-asset policy lookups (NFT/token queries)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_multi_asset_policy_name 
ON multi_asset (policy, name);

-- Asset holdings optimization
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ma_tx_out_optimized
ON ma_tx_out (tx_out_id, ident, quantity)
WHERE quantity > 0;

-- Epoch queries (API endpoints)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_epoch_no_desc 
ON epoch (no DESC);

-- Address transaction history (for /addresses/{addr}/transactions)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_tx_out_address_tx 
ON tx_out (address_id, tx_id DESC);

-- Transaction inputs for UTXO tracking
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_tx_in_tx_out_ref 
ON tx_in (tx_out_id, tx_out_index, tx_in_id);

-- ========================================
-- CHANGE DETECTION INDEXES (Hourly Monitoring)
-- ========================================
-- Optimized for the hourly balance change detection system

-- Time-based activity tracking (for /changes/addresses endpoint)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_block_time
ON block (time DESC);

-- Efficient batch address balance queries (for /changes/compare endpoint)  
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_tx_out_batch_balance
ON tx_out (address_id)
INCLUDE (value, consumed_by_tx_id, tx_id)
WHERE consumed_by_tx_id IS NULL;

-- ========================================
-- POSTGRESQL OPTIMIZATION NOTES
-- ========================================

-- IMPORTANT: If address queries are still slow after creating these indexes,
-- you may need to disable parallel query planning:
--
-- ALTER SYSTEM SET max_parallel_workers_per_gather = 0;
-- SELECT pg_reload_conf();
--
-- This forces PostgreSQL to use index scans instead of parallel seq scans
-- for better performance on this workload.

-- ========================================
-- ULTRA-FAST TOKEN STATISTICS INDEXES
-- ========================================
-- Critical indexes to fix 16+ minute token value queries

-- CRITICAL: Policy binary lookups (avoid encode() functions in indexes)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_multi_asset_policy_hex 
ON multi_asset (policy)
INCLUDE (id, name);

-- Policy + name binary lookup for specific assets (removed - too large for btree)
-- Use policy index + filter instead of composite index

-- CRITICAL: Asset quantity lookups with included columns
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ma_tx_out_token_fast
ON ma_tx_out (ident)
INCLUDE (quantity, tx_out_id)
WHERE quantity > 0;

-- CRITICAL: Unspent outputs for current supply calculations
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_tx_out_unspent_supply
ON tx_out (consumed_by_tx_id, address_id, tx_id, id)
WHERE consumed_by_tx_id IS NULL;

-- Time-based queries for daily volume (removed NOW() - cannot be used in partial indexes)
-- Use full time index instead
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_block_time_recent
ON block (time DESC);

-- ========================================
-- REAL-TIME PRICE QUERY INDEXES
-- ========================================
-- Optimized for PREEBOT's 5-minute price polling (no cache)

-- CRITICAL: Time-based trading activity queries (removed NOW() - use full index)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_block_recent_trading
ON block (time DESC);

-- Price discovery from recent trades (value + quantity correlation)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_tx_out_price_discovery
ON tx_out (value DESC, tx_id, address_id)
WHERE value > 1000000; -- Only significant ADA transactions

-- Trading activity by asset with time ordering
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ma_tx_out_trading
ON ma_tx_out (ident, tx_out_id)
INCLUDE (quantity)
WHERE quantity > 0;

-- ========================================
-- PREEBOT LIVE DATA SYSTEM
-- ========================================
-- Block-by-block incremental updates for real-time data

-- Token price tracking table
CREATE TABLE IF NOT EXISTS preebot_token_prices (
    asset_id TEXT PRIMARY KEY,
    policy_id TEXT NOT NULL,
    asset_name TEXT,
    current_price_ada NUMERIC DEFAULT 0,
    price_usd NUMERIC DEFAULT 0,
    volume_24h BIGINT DEFAULT 0,
    volume_7d BIGINT DEFAULT 0,
    trades_24h INTEGER DEFAULT 0,
    last_trade_time BIGINT DEFAULT 0,
    last_trade_price NUMERIC DEFAULT 0,
    price_change_24h NUMERIC DEFAULT 0,
    price_change_7d NUMERIC DEFAULT 0,
    market_cap BIGINT DEFAULT 0,
    total_supply BIGINT DEFAULT 0,
    holders_count INTEGER DEFAULT 0,
    last_updated_block BIGINT DEFAULT 0,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_preebot_prices_policy ON preebot_token_prices (policy_id);
CREATE INDEX IF NOT EXISTS idx_preebot_prices_updated ON preebot_token_prices (last_updated_block DESC);
CREATE INDEX IF NOT EXISTS idx_preebot_prices_volume ON preebot_token_prices (volume_24h DESC);

-- Asset holdings tracking table
CREATE TABLE IF NOT EXISTS preebot_asset_holdings (
    id BIGSERIAL PRIMARY KEY,
    address TEXT NOT NULL,
    asset_id TEXT NOT NULL,
    policy_id TEXT NOT NULL,
    asset_name TEXT,
    quantity BIGINT NOT NULL DEFAULT 0,
    last_tx_hash TEXT,
    last_updated_block BIGINT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(address, asset_id)
);

CREATE INDEX IF NOT EXISTS idx_preebot_holdings_address ON preebot_asset_holdings (address);
CREATE INDEX IF NOT EXISTS idx_preebot_holdings_asset ON preebot_asset_holdings (asset_id);
CREATE INDEX IF NOT EXISTS idx_preebot_holdings_policy ON preebot_asset_holdings (policy_id);
CREATE INDEX IF NOT EXISTS idx_preebot_holdings_updated ON preebot_asset_holdings (last_updated_block DESC);
CREATE INDEX IF NOT EXISTS idx_preebot_holdings_quantity ON preebot_asset_holdings (quantity) WHERE quantity > 0;

-- Recent activity tracking table
CREATE TABLE IF NOT EXISTS preebot_recent_activity (
    id BIGSERIAL PRIMARY KEY,
    asset_id TEXT NOT NULL,
    policy_id TEXT NOT NULL,
    tx_hash TEXT NOT NULL,
    from_address TEXT,
    to_address TEXT NOT NULL,
    quantity BIGINT NOT NULL,
    ada_value BIGINT,
    activity_type TEXT NOT NULL,
    block_time TIMESTAMP NOT NULL,
    block_no BIGINT NOT NULL,
    slot_no BIGINT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_preebot_activity_asset ON preebot_recent_activity (asset_id);
CREATE INDEX IF NOT EXISTS idx_preebot_activity_policy ON preebot_recent_activity (policy_id);
CREATE INDEX IF NOT EXISTS idx_preebot_activity_time ON preebot_recent_activity (block_time DESC);
CREATE INDEX IF NOT EXISTS idx_preebot_activity_block ON preebot_recent_activity (block_no DESC);
CREATE INDEX IF NOT EXISTS idx_preebot_activity_type ON preebot_recent_activity (activity_type);

-- Block processing state table
CREATE TABLE IF NOT EXISTS preebot_processing_state (
    id SERIAL PRIMARY KEY,
    component_name TEXT UNIQUE NOT NULL,
    last_processed_block BIGINT NOT NULL DEFAULT 0,
    last_processed_time TIMESTAMP DEFAULT NOW(),
    status TEXT DEFAULT 'active',
    error_message TEXT,
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Token cache table (simple cache for less critical data)
CREATE TABLE IF NOT EXISTS preebot_token_simple_cache (
    asset_id TEXT PRIMARY KEY,
    policy_id TEXT NOT NULL,
    asset_name TEXT,
    total_supply BIGINT DEFAULT 0,
    holders_count INT DEFAULT 0,
    last_tx_time BIGINT DEFAULT 0,
    cache_updated BIGINT DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_simple_cache_policy ON preebot_token_simple_cache (policy_id);
CREATE INDEX IF NOT EXISTS idx_simple_cache_updated ON preebot_token_simple_cache (cache_updated);

-- Initialize processing state
INSERT INTO preebot_processing_state (component_name, last_processed_block) 
VALUES 
    ('token_prices', 0),
    ('asset_holdings', 0), 
    ('recent_activity', 0)
ON CONFLICT (component_name) DO NOTHING;

-- ========================================
-- BLOCK PROCESSING FUNCTIONS
-- ========================================

-- Function to process a single block for token price updates
CREATE OR REPLACE FUNCTION process_block_for_prices(target_block_no BIGINT)
RETURNS TABLE(
    processed_assets INTEGER,
    processed_trades INTEGER,
    execution_time_ms INTEGER
) AS $$
DECLARE
    start_time TIMESTAMP := clock_timestamp();
    assets_count INTEGER := 0;
    trades_count INTEGER := 0;
BEGIN
    -- SCRIPT-BASED: Only include transactions with script addresses (DEX contracts)
    WITH block_trades AS (
        SELECT 
            encode(ma.policy, 'hex') || encode(ma.name, 'hex') as asset_id,
            encode(ma.policy, 'hex') as policy_id,
            encode(ma.name, 'hex') as asset_name,
            mto.quantity,
            txo.value as ada_value,
            extract(epoch from b.time)::bigint as trade_time,
            t.id as tx_id,
            encode(t.hash, 'hex') as tx_hash,
            a.address,
            -- TOKEN ↔ ADA swap confidence scoring
            CASE 
                -- High confidence: Single token policy + script (pure TOKEN ↔ ADA swap)
                WHEN (SELECT COUNT(DISTINCT ma2.policy) 
                      FROM tx_out txo2 
                      JOIN ma_tx_out mto2 ON mto2.tx_out_id = txo2.id 
                      JOIN multi_asset ma2 ON mto2.ident = ma2.id 
                      WHERE txo2.tx_id = t.id) = 1
                  AND a.has_script = true
                  AND (SELECT COUNT(*) FROM tx_out WHERE tx_id = t.id) BETWEEN 2 AND 4
                THEN 3
                
                -- Medium confidence: Script address with reasonable characteristics
                WHEN a.has_script = true 
                  AND txo.value > 1000000  -- > 1 ADA
                  AND txo.value::numeric / mto.quantity::numeric BETWEEN 0.0001 AND 10.0
                THEN 2
                
                -- Low confidence: Everything else
                ELSE 1
            END as confidence_weight,
            
            -- Filter non-swap transactions (focus on TOKEN ↔ ADA only)
            CASE 
                WHEN a.has_script = false THEN true  -- Not script address = not DEX
                -- Multi-token transactions are likely LP/complex operations, not simple swaps
                WHEN (SELECT COUNT(DISTINCT ma2.policy) 
                      FROM tx_out txo2 
                      JOIN ma_tx_out mto2 ON mto2.tx_out_id = txo2.id 
                      JOIN multi_asset ma2 ON mto2.ident = ma2.id 
                      WHERE txo2.tx_id = t.id) > 1 THEN true
                WHEN txo.value::numeric / mto.quantity::numeric < 0.0001 THEN true -- Dust
                WHEN txo.value::numeric / mto.quantity::numeric > 10 THEN true     -- Unrealistic for swaps
                WHEN txo.value < 100000 THEN true                                  -- < 0.1 ADA
                WHEN mto.quantity < 1000 THEN true                                 -- Dust quantity
                -- Complex transactions are likely not simple swaps
                WHEN (SELECT COUNT(*) FROM tx_out WHERE tx_id = t.id) > 4 THEN true
                ELSE false
            END as likely_non_trade
            
        FROM block b
        JOIN tx t ON t.block_id = b.id
        JOIN tx_out txo ON txo.tx_id = t.id
        JOIN address a ON txo.address_id = a.id
        JOIN ma_tx_out mto ON mto.tx_out_id = txo.id
        JOIN multi_asset ma ON mto.ident = ma.id
        WHERE b.block_no = target_block_no
          AND t.valid_contract = true
          AND mto.quantity > 0
          AND txo.value > 1000000  -- At least 1 ADA
          AND a.has_script = true  -- ONLY script addresses (DEX contracts)
    )
    INSERT INTO preebot_recent_activity (
        asset_id, policy_id, tx_hash, to_address, quantity, ada_value, 
        activity_type, block_time, block_no, slot_no
    )
    SELECT 
        bt.asset_id, bt.policy_id, bt.tx_hash, bt.address, bt.quantity, bt.ada_value,
        CASE 
            WHEN bt.confidence_weight = 3 THEN 'dex_multi'
            WHEN bt.confidence_weight = 2 THEN 'dex_pair'
            ELSE 'transfer'
        END as activity_type,
        to_timestamp(bt.trade_time), target_block_no, b.slot_no
    FROM block_trades bt
    JOIN block b ON b.block_no = target_block_no
    WHERE bt.likely_non_trade = false AND bt.confidence_weight >= 2;

    GET DIAGNOSTICS trades_count = ROW_COUNT;

    WITH price_updates AS (
        SELECT 
            asset_id, policy_id,
            -- Volume-weighted price calculation with confidence weighting
            CASE 
                WHEN SUM(quantity::numeric * confidence_weight) > 0 
                THEN SUM(ada_value::numeric * confidence_weight) / SUM(quantity::numeric * confidence_weight)
                ELSE 0
            END as new_price,
            SUM(ada_value) as volume_24h,
            COUNT(*) as trades_24h,
            MAX(extract(epoch from block_time)::bigint) as last_trade_time
        FROM (
            SELECT *,
                CASE 
                    WHEN activity_type = 'dex_multi' THEN 3
                    WHEN activity_type = 'dex_pair' THEN 2  
                    ELSE 1
                END as confidence_weight
            FROM preebot_recent_activity
            WHERE block_time >= NOW() - INTERVAL '24 hours'
        ) pra
        GROUP BY asset_id, policy_id
    )
    INSERT INTO preebot_token_prices (
        asset_id, policy_id, current_price_ada, volume_24h, trades_24h, 
        last_trade_time, last_updated_block, updated_at
    )
    SELECT 
        pu.asset_id, pu.policy_id, pu.new_price, pu.volume_24h, pu.trades_24h, 
        pu.last_trade_time, target_block_no, NOW()
    FROM price_updates pu
    ON CONFLICT (asset_id) DO UPDATE SET
        current_price_ada = EXCLUDED.current_price_ada,
        volume_24h = EXCLUDED.volume_24h,
        trades_24h = EXCLUDED.trades_24h,
        last_trade_time = EXCLUDED.last_trade_time,
        last_updated_block = EXCLUDED.last_updated_block,
        updated_at = NOW();

    GET DIAGNOSTICS assets_count = ROW_COUNT;

    UPDATE preebot_processing_state 
    SET last_processed_block = target_block_no, last_processed_time = NOW(),
        status = 'active', error_message = NULL, updated_at = NOW()
    WHERE component_name = 'token_prices';

    DELETE FROM preebot_recent_activity WHERE block_time < NOW() - INTERVAL '7 days';

    RETURN QUERY SELECT assets_count, trades_count,
        extract(milliseconds from (clock_timestamp() - start_time))::integer;

EXCEPTION WHEN OTHERS THEN
    UPDATE preebot_processing_state 
    SET status = 'error', error_message = SQLERRM, updated_at = NOW()
    WHERE component_name = 'token_prices';
    RAISE;
END;
$$ LANGUAGE plpgsql;

-- Function to process holdings updates for a block
CREATE OR REPLACE FUNCTION process_block_for_holdings(target_block_no BIGINT)
RETURNS INTEGER AS $$
DECLARE
    holdings_updated INTEGER := 0;
BEGIN
    WITH block_changes AS (
        SELECT 
            a.address,
            encode(ma.policy, 'hex') || encode(ma.name, 'hex') as asset_id,
            encode(ma.policy, 'hex') as policy_id,
            encode(ma.name, 'hex') as asset_name,
            SUM(mto.quantity) as quantity_change,
            encode(t.hash, 'hex') as tx_hash
        FROM block b
        JOIN tx t ON t.block_id = b.id
        JOIN tx_out txo ON txo.tx_id = t.id
        JOIN address a ON txo.address_id = a.id
        JOIN ma_tx_out mto ON mto.tx_out_id = txo.id
        JOIN multi_asset ma ON mto.ident = ma.id
        WHERE b.block_no = target_block_no
          AND t.valid_contract = true
          AND mto.quantity > 0
        GROUP BY a.address, ma.policy, ma.name, t.hash
        
        UNION ALL
        
        SELECT 
            a.address,
            encode(ma.policy, 'hex') || encode(ma.name, 'hex') as asset_id,
            encode(ma.policy, 'hex') as policy_id,
            encode(ma.name, 'hex') as asset_name,
            -SUM(mto.quantity) as quantity_change,
            encode(consuming_tx.hash, 'hex') as tx_hash
        FROM block b
        JOIN tx consuming_tx ON consuming_tx.block_id = b.id
        JOIN tx_out txo ON txo.consumed_by_tx_id = consuming_tx.id
        JOIN address a ON txo.address_id = a.id
        JOIN ma_tx_out mto ON mto.tx_out_id = txo.id
        JOIN multi_asset ma ON mto.ident = ma.id
        WHERE b.block_no = target_block_no
          AND consuming_tx.valid_contract = true
          AND mto.quantity > 0
        GROUP BY a.address, ma.policy, ma.name, consuming_tx.hash
    )
    INSERT INTO preebot_asset_holdings (
        address, asset_id, policy_id, asset_name, quantity, 
        last_tx_hash, last_updated_block, updated_at
    )
    SELECT 
        bc.address, bc.asset_id, bc.policy_id, bc.asset_name,
        SUM(bc.quantity_change) as total_quantity,
        bc.tx_hash, target_block_no, NOW()
    FROM block_changes bc
    GROUP BY bc.address, bc.asset_id, bc.policy_id, bc.asset_name, bc.tx_hash
    ON CONFLICT (address, asset_id) DO UPDATE SET
        quantity = preebot_asset_holdings.quantity + EXCLUDED.quantity,
        last_tx_hash = EXCLUDED.last_tx_hash,
        last_updated_block = EXCLUDED.last_updated_block,
        updated_at = NOW();

    GET DIAGNOSTICS holdings_updated = ROW_COUNT;

    UPDATE preebot_processing_state 
    SET last_processed_block = target_block_no, last_processed_time = NOW(),
        status = 'active', updated_at = NOW()
    WHERE component_name = 'asset_holdings';

    DELETE FROM preebot_asset_holdings WHERE quantity <= 0;

    RETURN holdings_updated;

EXCEPTION WHEN OTHERS THEN
    UPDATE preebot_processing_state 
    SET status = 'error', error_message = SQLERRM, updated_at = NOW()
    WHERE component_name = 'asset_holdings';
    RAISE;
END;
$$ LANGUAGE plpgsql;

-- Function to process the latest blocks
CREATE OR REPLACE FUNCTION process_latest_blocks()
RETURNS TABLE(
    component TEXT,
    blocks_processed INTEGER,
    execution_time_ms INTEGER
) AS $$
DECLARE
    latest_block BIGINT;
    price_last_block BIGINT;
    holdings_last_block BIGINT;
    start_time TIMESTAMP := clock_timestamp();
    blocks_count INTEGER := 0;
BEGIN
    SELECT COALESCE(MAX(block_no), 0) INTO latest_block FROM block;
    
    SELECT COALESCE(last_processed_block, 0) INTO price_last_block 
    FROM preebot_processing_state WHERE component_name = 'token_prices';
    
    SELECT COALESCE(last_processed_block, 0) INTO holdings_last_block 
    FROM preebot_processing_state WHERE component_name = 'asset_holdings';

    FOR i IN (price_last_block + 1)..LEAST(price_last_block + 10, latest_block) LOOP
        PERFORM process_block_for_prices(i);
        blocks_count := blocks_count + 1;
    END LOOP;

    RETURN QUERY SELECT 'token_prices'::TEXT, blocks_count,
        extract(milliseconds from (clock_timestamp() - start_time))::integer;

    blocks_count := 0;
    start_time := clock_timestamp();

    FOR i IN (holdings_last_block + 1)..LEAST(holdings_last_block + 10, latest_block) LOOP
        PERFORM process_block_for_holdings(i);
        blocks_count := blocks_count + 1;
    END LOOP;

    RETURN QUERY SELECT 'asset_holdings'::TEXT, blocks_count,
        extract(milliseconds from (clock_timestamp() - start_time))::integer;
END;
$$ LANGUAGE plpgsql;

-- Cache refresh function
CREATE OR REPLACE FUNCTION refresh_token_cache(target_policy TEXT) 
RETURNS TABLE(
    policy_id TEXT,
    asset_name TEXT, 
    asset_id TEXT,
    total_supply TEXT,
    holders_count BIGINT,
    last_tx_time BIGINT
) AS $$
BEGIN
    DELETE FROM preebot_token_simple_cache WHERE policy_id = target_policy;
    
    INSERT INTO preebot_token_simple_cache (
        asset_id, policy_id, asset_name, total_supply, holders_count, last_tx_time, cache_updated
    )
    SELECT 
        encode(ma.policy, 'hex') || encode(ma.name, 'hex') as asset_id,
        encode(ma.policy, 'hex') as policy_id,
        encode(ma.name, 'hex') as asset_name,
        COALESCE(SUM(CASE 
            WHEN txo.consumed_by_tx_id IS NULL AND mto.quantity > 0 
            THEN mto.quantity 
            ELSE 0 
        END), 0) as total_supply,
        COUNT(DISTINCT CASE 
            WHEN txo.consumed_by_tx_id IS NULL AND mto.quantity > 0 
            THEN txo.address_id 
            ELSE NULL 
        END) as holders_count,
        COALESCE(MAX(extract(epoch from b.time)::bigint), 0) as last_tx_time,
        extract(epoch from NOW())::bigint as cache_updated
    FROM multi_asset ma
    JOIN ma_tx_out mto ON ma.id = mto.ident
    JOIN tx_out txo ON mto.tx_out_id = txo.id  
    JOIN tx t ON txo.tx_id = t.id
    JOIN block b ON t.block_id = b.id
    WHERE t.valid_contract = true
      AND encode(ma.policy, 'hex') = target_policy
    GROUP BY ma.policy, ma.name;
    
    RETURN QUERY
    SELECT c.policy_id, c.asset_name, c.asset_id, c.total_supply::TEXT,
           c.holders_count::BIGINT, c.last_tx_time
    FROM preebot_token_simple_cache c
    WHERE c.policy_id = target_policy;
END;
$$ LANGUAGE plpgsql;

-- ========================================
-- NFT METADATA OPTIMIZATION INDEXES
-- ========================================
-- Critical indexes for fast NFT metadata and trait queries (Discord role assignment)

-- CRITICAL: TX metadata lookups by transaction ID and key (CIP-25 standard)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_tx_metadata_tx_key 
ON tx_metadata (tx_id, key)
WHERE key = 721;

-- CRITICAL: Multi-asset first mint transaction lookup
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ma_tx_out_first_mint
ON ma_tx_out (ident, tx_out_id)
WHERE quantity > 0;

-- NFT-focused asset holdings (quantity <= 10 for likely NFTs)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_preebot_holdings_nft
ON preebot_asset_holdings (address, quantity)
WHERE quantity > 0 AND quantity <= 10;

-- Fast policy-based NFT lookups for Discord role assignment
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_preebot_holdings_policy_nft
ON preebot_asset_holdings (policy_id, address)
WHERE quantity > 0 AND quantity <= 10;

-- ========================================
-- PREEBOT NFT METADATA CACHE TABLE
-- ========================================
-- Lightning-fast NFT metadata cache for Discord bot queries

CREATE TABLE IF NOT EXISTS preebot_nft_metadata (
    asset_id TEXT PRIMARY KEY,
    policy_id TEXT NOT NULL,
    asset_name TEXT,
    metadata JSONB,
    traits JSONB,
    collection_name TEXT,
    nft_name TEXT,
    image_url TEXT,
    first_mint_tx TEXT,
    cache_updated BIGINT DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_nft_metadata_policy ON preebot_nft_metadata (policy_id);
CREATE INDEX IF NOT EXISTS idx_nft_metadata_traits ON preebot_nft_metadata USING gin (traits);
CREATE INDEX IF NOT EXISTS idx_nft_metadata_collection ON preebot_nft_metadata (collection_name);
CREATE INDEX IF NOT EXISTS idx_nft_metadata_updated ON preebot_nft_metadata (cache_updated);

-- Function to populate NFT metadata cache for a specific policy
CREATE OR REPLACE FUNCTION cache_nft_metadata_for_policy(target_policy TEXT)
RETURNS INTEGER AS $$
DECLARE
    cached_count INTEGER := 0;
BEGIN
    INSERT INTO preebot_nft_metadata (
        asset_id, policy_id, asset_name, metadata, traits, collection_name, 
        nft_name, image_url, first_mint_tx, cache_updated
    )
    SELECT 
        encode(ma.policy, 'hex') || encode(ma.name, 'hex') as asset_id,
        encode(ma.policy, 'hex') as policy_id,
        encode(ma.name, 'hex') as asset_name,
        COALESCE(tm.json, '{}'::jsonb) as metadata,
        COALESCE(
            -- Extract traits from CIP-25 metadata
            CASE 
                WHEN tm.json ? encode(ma.policy, 'hex') THEN
                    COALESCE(
                        tm.json->encode(ma.policy, 'hex')->encode(ma.name, 'hex')->>'attributes',
                        tm.json->encode(ma.policy, 'hex')->encode(ma.name, 'hex')->>'traits',
                        tm.json->encode(ma.policy, 'hex')->''->'attributes',
                        '{}'::jsonb
                    )
                ELSE '{}'::jsonb
            END, '{}'::jsonb
        ) as traits,
        -- Collection name extraction
        COALESCE(
            tm.json->encode(ma.policy, 'hex')->encode(ma.name, 'hex')->>'name',
            tm.json->encode(ma.policy, 'hex')->''->'name'
        ) as collection_name,
        -- NFT name
        COALESCE(
            tm.json->encode(ma.policy, 'hex')->encode(ma.name, 'hex')->>'name',
            tm.json->encode(ma.policy, 'hex')->''->'name'
        ) as nft_name,
        -- Image URL
        COALESCE(
            tm.json->encode(ma.policy, 'hex')->encode(ma.name, 'hex')->>'image',
            tm.json->encode(ma.policy, 'hex')->''->'image'
        ) as image_url,
        encode(t.hash, 'hex') as first_mint_tx,
        extract(epoch from NOW())::bigint as cache_updated
    FROM (
        -- Find the first minting transaction for each asset
        SELECT DISTINCT ON (ma.id)
            ma.id, ma.policy, ma.name, t.id as tx_id, t.hash
        FROM multi_asset ma
        JOIN ma_tx_out mto ON ma.id = mto.ident
        JOIN tx_out txo ON mto.tx_out_id = txo.id
        JOIN tx t ON txo.tx_id = t.id
        WHERE encode(ma.policy, 'hex') = target_policy
          AND mto.quantity > 0
          AND t.valid_contract = true
        ORDER BY ma.id, t.id ASC  -- First transaction = mint
    ) first_mints
    JOIN multi_asset ma ON ma.id = first_mints.id
    JOIN tx t ON t.id = first_mints.tx_id
    LEFT JOIN tx_metadata tm ON tm.tx_id = t.id AND tm.key = 721  -- CIP-25
    ON CONFLICT (asset_id) DO UPDATE SET
        metadata = EXCLUDED.metadata,
        traits = EXCLUDED.traits,
        collection_name = EXCLUDED.collection_name,
        nft_name = EXCLUDED.nft_name,
        image_url = EXCLUDED.image_url,
        cache_updated = EXCLUDED.cache_updated;

    GET DIAGNOSTICS cached_count = ROW_COUNT;
    RETURN cached_count;

EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION 'Failed to cache NFT metadata for policy %: %', target_policy, SQLERRM;
END;
$$ LANGUAGE plpgsql;

-- ========================================
-- REAL-TIME MATERIALIZED VIEWS FOR DISCORD BOT
-- ========================================
-- Lightning-fast materialized views refreshed every minute for Discord role assignment

-- MATERIALIZED VIEW: Current asset holdings (refreshed every 1 minute)
CREATE MATERIALIZED VIEW IF NOT EXISTS discord_asset_holdings AS
SELECT 
    a.address,
    encode(ma.policy, 'hex') as policy_id,
    encode(ma.name, 'hex') as asset_name,
    encode(ma.policy, 'hex') || encode(ma.name, 'hex') as asset_id,
    SUM(mto.quantity) as total_quantity,
    COUNT(*) as utxo_count,
    MAX(extract(epoch from b.time)::bigint) as last_tx_time,
    MAX(encode(t.hash, 'hex')) as last_tx_hash
FROM address a
JOIN tx_out txo ON a.id = txo.address_id
JOIN ma_tx_out mto ON txo.id = mto.tx_out_id
JOIN multi_asset ma ON mto.ident = ma.id
JOIN tx t ON txo.tx_id = t.id
JOIN block b ON t.block_id = b.id
WHERE txo.consumed_by_tx_id IS NULL
  AND t.valid_contract = true
  AND mto.quantity > 0
GROUP BY a.address, ma.policy, ma.name
HAVING SUM(mto.quantity) > 0;

-- Indexes for lightning-fast Discord queries
CREATE UNIQUE INDEX IF NOT EXISTS idx_discord_holdings_unique 
ON discord_asset_holdings (address, asset_id);

CREATE INDEX IF NOT EXISTS idx_discord_holdings_address 
ON discord_asset_holdings (address);

CREATE INDEX IF NOT EXISTS idx_discord_holdings_policy 
ON discord_asset_holdings (policy_id);

CREATE INDEX IF NOT EXISTS idx_discord_holdings_quantity 
ON discord_asset_holdings (total_quantity DESC);

-- MATERIALIZED VIEW: NFT metadata cache (refreshed when needed)
CREATE MATERIALIZED VIEW IF NOT EXISTS discord_nft_metadata AS
SELECT DISTINCT ON (ma.id)
    encode(ma.policy, 'hex') as policy_id,
    encode(ma.name, 'hex') as asset_name,
    encode(ma.policy, 'hex') || encode(ma.name, 'hex') as asset_id,
    tm.json as metadata,
    -- Extract common traits directly in SQL for speed
    CASE 
        WHEN tm.json IS NOT NULL THEN
            COALESCE(
                tm.json->encode(ma.policy, 'hex')->encode(ma.name, 'hex')->'attributes',
                tm.json->encode(ma.policy, 'hex')->encode(ma.name, 'hex')->'traits',
                tm.json->encode(ma.policy, 'hex')->''->'attributes',
                '{}'::jsonb
            )
        ELSE '{}'::jsonb
    END as traits,
    -- Extract name
    COALESCE(
        tm.json->encode(ma.policy, 'hex')->encode(ma.name, 'hex')->>'name',
        tm.json->encode(ma.policy, 'hex')->''->'name'
    ) as nft_name,
    -- Extract image
    COALESCE(
        tm.json->encode(ma.policy, 'hex')->encode(ma.name, 'hex')->>'image',
        tm.json->encode(ma.policy, 'hex')->''->'image'
    ) as image_url,
    encode(t.hash, 'hex') as mint_tx_hash,
    extract(epoch from b.time)::bigint as mint_time
FROM multi_asset ma
JOIN ma_tx_out mto ON ma.id = mto.ident
JOIN tx_out txo ON mto.tx_out_id = txo.id
JOIN tx t ON txo.tx_id = t.id
JOIN block b ON t.block_id = b.id
LEFT JOIN tx_metadata tm ON t.id = tm.tx_id AND tm.key = 721
WHERE mto.quantity > 0
  AND t.valid_contract = true
ORDER BY ma.id, t.id ASC; -- First transaction = mint

-- Indexes for NFT metadata lookups
CREATE UNIQUE INDEX IF NOT EXISTS idx_discord_nft_metadata_asset 
ON discord_nft_metadata (asset_id);

CREATE INDEX IF NOT EXISTS idx_discord_nft_metadata_policy 
ON discord_nft_metadata (policy_id);

CREATE INDEX IF NOT EXISTS idx_discord_nft_metadata_traits 
ON discord_nft_metadata USING gin (traits);

-- MATERIALIZED VIEW: Combined holdings with metadata for Discord (ULTRA FAST)
CREATE MATERIALIZED VIEW IF NOT EXISTS discord_holdings_with_metadata AS
SELECT 
    dah.address,
    dah.policy_id,
    dah.asset_name,
    dah.asset_id,
    dah.total_quantity,
    dah.utxo_count,
    dah.last_tx_time,
    dah.last_tx_hash,
    COALESCE(dnm.metadata, '{}'::jsonb) as metadata,
    COALESCE(dnm.traits, '{}'::jsonb) as traits,
    dnm.nft_name,
    dnm.image_url,
    dnm.mint_tx_hash,
    -- Classify asset type for Discord role logic
    CASE 
        WHEN dah.total_quantity = 1 AND dnm.traits != '{}'::jsonb THEN 'nft'
        WHEN dah.total_quantity = 1 THEN 'potential_nft'
        WHEN dah.total_quantity > 1 AND dah.total_quantity <= 10000 THEN 'limited_token'
        ELSE 'fungible_token'
    END as asset_type
FROM discord_asset_holdings dah
LEFT JOIN discord_nft_metadata dnm ON dah.asset_id = dnm.asset_id;

-- Ultimate index for Discord queries
CREATE UNIQUE INDEX IF NOT EXISTS idx_discord_combined_unique 
ON discord_holdings_with_metadata (address, asset_id);

CREATE INDEX IF NOT EXISTS idx_discord_combined_address 
ON discord_holdings_with_metadata (address);

CREATE INDEX IF NOT EXISTS idx_discord_combined_policy 
ON discord_holdings_with_metadata (policy_id);

CREATE INDEX IF NOT EXISTS idx_discord_combined_type 
ON discord_holdings_with_metadata (asset_type);

CREATE INDEX IF NOT EXISTS idx_discord_combined_traits 
ON discord_holdings_with_metadata USING gin (traits);

-- ========================================
-- REFRESH FUNCTIONS FOR REAL-TIME UPDATES
-- ========================================

-- Function to refresh holdings (call every minute)
CREATE OR REPLACE FUNCTION refresh_discord_holdings()
RETURNS TEXT AS $$
DECLARE
    start_time TIMESTAMP := clock_timestamp();
    refresh_time TEXT;
BEGIN
    -- Refresh asset holdings (fast - usually under 10 seconds)
    REFRESH MATERIALIZED VIEW CONCURRENTLY discord_asset_holdings;
    
    -- Refresh combined view (very fast - just a JOIN)
    REFRESH MATERIALIZED VIEW discord_holdings_with_metadata;
    
    refresh_time := extract(milliseconds from (clock_timestamp() - start_time))::text || 'ms';
    
    RETURN 'Discord holdings refreshed in ' || refresh_time;
    
EXCEPTION WHEN OTHERS THEN
    RETURN 'Error refreshing discord holdings: ' || SQLERRM;
END;
$$ LANGUAGE plpgsql;

-- Function to refresh NFT metadata (call when new collections are added)
CREATE OR REPLACE FUNCTION refresh_discord_nft_metadata()
RETURNS TEXT AS $$
DECLARE
    start_time TIMESTAMP := clock_timestamp();
    refresh_time TEXT;
BEGIN
    -- Refresh NFT metadata (slower - only when needed)
    REFRESH MATERIALIZED VIEW discord_nft_metadata;
    
    -- Refresh combined view
    REFRESH MATERIALIZED VIEW discord_holdings_with_metadata;
    
    refresh_time := extract(milliseconds from (clock_timestamp() - start_time))::text || 'ms';
    
    RETURN 'Discord NFT metadata refreshed in ' || refresh_time;
    
EXCEPTION WHEN OTHERS THEN
    RETURN 'Error refreshing NFT metadata: ' || SQLERRM;
END;
$$ LANGUAGE plpgsql;

-- Function for initial population
CREATE OR REPLACE FUNCTION initialize_discord_views()
RETURNS TEXT AS $$
BEGIN
    -- Initial population of materialized views
    REFRESH MATERIALIZED VIEW discord_asset_holdings;
    REFRESH MATERIALIZED VIEW discord_nft_metadata;
    REFRESH MATERIALIZED VIEW discord_holdings_with_metadata;
    
    RETURN 'Discord materialized views initialized successfully';
    
EXCEPTION WHEN OTHERS THEN
    RETURN 'Error initializing discord views: ' || SQLERRM;
END;
$$ LANGUAGE plpgsql;

-- Create a table to track refresh times
CREATE TABLE IF NOT EXISTS discord_refresh_log (
    id SERIAL PRIMARY KEY,
    view_name TEXT NOT NULL,
    refreshed_at TIMESTAMP DEFAULT NOW(),
    refresh_duration_ms INTEGER,
    status TEXT DEFAULT 'success',
    error_message TEXT
);

-- Function to log refresh operations
CREATE OR REPLACE FUNCTION log_discord_refresh(view_name TEXT, duration_ms INTEGER, status TEXT DEFAULT 'success', error_msg TEXT DEFAULT NULL)
RETURNS VOID AS $$
BEGIN
    INSERT INTO discord_refresh_log (view_name, refresh_duration_ms, status, error_message)
    VALUES (view_name, duration_ms, status, error_msg);
    
    -- Keep only last 100 entries per view
    DELETE FROM discord_refresh_log 
    WHERE id NOT IN (
        SELECT id FROM discord_refresh_log 
        WHERE view_name = log_discord_refresh.view_name
        ORDER BY refreshed_at DESC 
        LIMIT 100
    ) AND view_name = log_discord_refresh.view_name;
END;
$$ LANGUAGE plpgsql;

-- Update statistics after creating indexes and tables
-- Run: ANALYZE address, tx_out, tx, multi_asset, ma_tx_out, block, tx_metadata, discord_asset_holdings, discord_nft_metadata, discord_holdings_with_metadata;