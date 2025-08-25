-- ========================================
-- PREEBOT SYSTEM BOOTSTRAP
-- ========================================
-- Run this after migrations.sql to initialize the live data system

\echo 'Bootstrapping PREEBOT live data system...';

-- Bootstrap asset holdings with current unspent UTXOs (recent data only for speed)
INSERT INTO preebot_asset_holdings (
    address, asset_id, policy_id, asset_name, quantity, 
    last_tx_hash, last_updated_block, updated_at
)
SELECT 
    a.address,
    encode(ma.policy, 'hex') || encode(ma.name, 'hex') as asset_id,
    encode(ma.policy, 'hex') as policy_id,
    encode(ma.name, 'hex') as asset_name,
    SUM(mto.quantity) as total_quantity,
    encode(t.hash, 'hex') as last_tx_hash,
    b.block_no,
    NOW()
FROM tx_out txo
JOIN address a ON txo.address_id = a.id
JOIN ma_tx_out mto ON mto.tx_out_id = txo.id
JOIN multi_asset ma ON mto.ident = ma.id
JOIN tx t ON txo.tx_id = t.id
JOIN block b ON t.block_id = b.id
WHERE txo.consumed_by_tx_id IS NULL
  AND t.valid_contract = true
  AND mto.quantity > 0
  AND b.time >= NOW() - INTERVAL '3 days'  -- Recent data only
GROUP BY a.address, ma.policy, ma.name, t.hash, b.block_no
HAVING SUM(mto.quantity) > 0
ON CONFLICT (address, asset_id) DO UPDATE SET
    quantity = EXCLUDED.quantity,
    last_tx_hash = EXCLUDED.last_tx_hash,
    last_updated_block = EXCLUDED.last_updated_block,
    updated_at = NOW();

-- Bootstrap recent activity with last 24 hours of significant trades
INSERT INTO preebot_recent_activity (
    asset_id, policy_id, tx_hash, to_address, quantity, ada_value,
    activity_type, block_time, block_no, slot_no
)
SELECT 
    encode(ma.policy, 'hex') || encode(ma.name, 'hex') as asset_id,
    encode(ma.policy, 'hex') as policy_id,
    encode(t.hash, 'hex') as tx_hash,
    a.address as to_address,
    mto.quantity,
    txo.value as ada_value,
    'transfer' as activity_type,
    b.time,
    b.block_no,
    b.slot_no
FROM block b
JOIN tx t ON t.block_id = b.id
JOIN tx_out txo ON txo.tx_id = t.id
JOIN address a ON txo.address_id = a.id
JOIN ma_tx_out mto ON mto.tx_out_id = txo.id
JOIN multi_asset ma ON mto.ident = ma.id
WHERE b.time >= NOW() - INTERVAL '24 hours'
  AND t.valid_contract = true
  AND mto.quantity > 0
  AND txo.value > 1000000
ORDER BY b.time DESC
LIMIT 5000;

-- Initialize processing state to current block
UPDATE preebot_processing_state 
SET last_processed_block = (SELECT COALESCE(MAX(block_no), 0) FROM block),
    last_processed_time = NOW(),
    status = 'active',
    error_message = NULL,
    updated_at = NOW();

-- Show results
\echo '';
\echo '=== BOOTSTRAP COMPLETE ===';

SELECT 'Holdings' as table_name, COUNT(*)::text as count FROM preebot_asset_holdings
UNION ALL
SELECT 'Recent Activity', COUNT(*)::text FROM preebot_recent_activity
UNION ALL  
SELECT 'Processing State', component_name FROM preebot_processing_state;

\echo '';
\echo 'PREEBOT system ready! Run: SELECT * FROM process_latest_blocks();';
\echo 'Set up cron job: * * * * * psql -d your_db -c "SELECT * FROM process_latest_blocks();"';