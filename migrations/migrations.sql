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

-- CRITICAL: Policy hex lookups (replaces slow encode() operations in WHERE clauses)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_multi_asset_policy_hex 
ON multi_asset (encode(policy, 'hex'))
INCLUDE (id, name);

-- Policy + name hex lookup for specific assets
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_multi_asset_policy_name_hex
ON multi_asset (encode(policy, 'hex'), encode(name, 'hex'))
INCLUDE (id);

-- CRITICAL: Asset quantity lookups with included columns
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ma_tx_out_token_fast
ON ma_tx_out (ident)
INCLUDE (quantity, tx_out_id)
WHERE quantity > 0;

-- CRITICAL: Unspent outputs for current supply calculations
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_tx_out_unspent_supply
ON tx_out (consumed_by_tx_id, address_id, tx_id, id)
WHERE consumed_by_tx_id IS NULL;

-- Time-based queries for daily volume (only index recent data)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_block_time_recent
ON block (time DESC)
WHERE time >= NOW() - INTERVAL '7 days';

-- ========================================
-- REAL-TIME PRICE QUERY INDEXES
-- ========================================
-- Optimized for PREEBOT's 5-minute price polling (no cache)

-- CRITICAL: Time-based trading activity queries (last 48 hours)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_block_recent_trading
ON block (time DESC)
WHERE time >= NOW() - INTERVAL '7 days';

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
            a.address
        FROM block b
        JOIN tx t ON t.block_id = b.id
        JOIN tx_out txo ON txo.tx_id = t.id
        JOIN address a ON txo.address_id = a.id
        JOIN ma_tx_out mto ON mto.tx_out_id = txo.id
        JOIN multi_asset ma ON mto.ident = ma.id
        WHERE b.block_no = target_block_no
          AND t.valid_contract = true
          AND mto.quantity > 0
          AND txo.value > 1000000
    )
    INSERT INTO preebot_recent_activity (
        asset_id, policy_id, tx_hash, to_address, quantity, ada_value, 
        activity_type, block_time, block_no, slot_no
    )
    SELECT 
        bt.asset_id, bt.policy_id, bt.tx_hash, bt.address, bt.quantity, bt.ada_value,
        'transfer', to_timestamp(bt.trade_time), target_block_no, b.slot_no
    FROM block_trades bt
    JOIN block b ON b.block_no = target_block_no;

    GET DIAGNOSTICS trades_count = ROW_COUNT;

    WITH price_updates AS (
        SELECT 
            asset_id, policy_id,
            CASE 
                WHEN SUM(quantity) > 0 
                THEN (SUM(ada_value::numeric) / SUM(quantity::numeric))
                ELSE 0
            END as new_price,
            SUM(ada_value) as volume_24h,
            COUNT(*) as trades_24h,
            MAX(extract(epoch from block_time)::bigint) as last_trade_time
        FROM preebot_recent_activity
        WHERE block_time >= NOW() - INTERVAL '24 hours'
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

-- Update statistics after creating indexes and tables
-- Run: ANALYZE address, tx_out, tx, multi_asset, ma_tx_out, block, preebot_token_prices, preebot_asset_holdings, preebot_recent_activity;