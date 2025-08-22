CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ma_tx_out_ident_policy_quantity ON ma_tx_out (ident) WHERE quantity = 1;
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_tx_out_consumed_by_tx_id ON tx_out (id) WHERE consumed_by_tx_id IS NULL;
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ma_tx_out_ident_quantityON ma_tx_out (ident) WHERE quantity = 1;
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_address_has_script ON address (id) WHERE has_script = false;
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_address_address ON address (id);
-- Stop the 108M row scan
CREATE INDEX idx_tx_out_stake_address_id ON tx_out(stake_address_id);

-- Speed up drep lookups
CREATE INDEX idx_drep_registration_active
  ON drep_registration(drep_hash_id, tx_id DESC, cert_index DESC)
  WHERE deposit > 0;

CREATE INDEX idx_drep_registration_inactive
  ON drep_registration(drep_hash_id)
  WHERE deposit < 0;

-- Speed up delegation checks
CREATE INDEX idx_delegation_vote_lookup
  ON delegation_vote(addr_id, drep_hash_id, tx_id DESC);

-- Speed up reward and withdrawal
CREATE INDEX idx_reward_addr_epoch
  ON reward(addr_id, spendable_epoch, type);

CREATE INDEX idx_withdrawal_addr_epoch
  ON withdrawal(addr_id);

  -- Jump straight to outputs for this stake address
CREATE INDEX idx_tx_out_stake_view
ON tx_out(stake_address_id, tx_id, index, id)
INCLUDE (value) -- optional if you often read value

-- Fast lookup for stake_address.view
CREATE UNIQUE INDEX idx_stake_address_view
ON stake_address(view);

-- Speed up spent check (tx_in lookup)
CREATE INDEX idx_tx_in_src
ON tx_in(tx_out_id, tx_out_index);

-- Multi-asset joins
CREATE INDEX idx_ma_tx_out_tx_out_id
ON ma_tx_out(tx_out_id, ident);

CREATE INDEX idx_multi_asset_ident
ON multi_asset(id, policy, name);

-- Valid contract filter on tx
CREATE INDEX idx_tx_valid_contract
ON tx(id)
WHERE valid_contract = true;



WITH queried_address AS (
    SELECT stake_address_id AS "stake_address_id"
    FROM address txo
    WHERE txo.address = 'addr1q8ur464mlqsqslh0dn9dqg88zn0q0sqag2hkxc0vhtrn5c7wkhumlr876ehcm8ltdwt7s49mwxfw47c4hcf5p6qdlavqaawfcs'
    LIMIT 1
), queried_amount AS (
    --    1443850 | {1050698866,1050698867,1050698868,1050698869,1050698870}
  SELECT COALESCE(txo.value, 0) AS "amount",
    array_agg(mto.id) AS "assets_ids"
  FROM tx_out txo
    JOIN tx ON (txo.tx_id = tx.id)
    LEFT JOIN tx_in txi ON (txo.tx_id = txi.tx_out_id)
    AND (txo.index = txi.tx_out_index)
    LEFT JOIN ma_tx_out mto ON (mto.tx_out_id = txo.id)
  WHERE txi IS NULL
    AND txo.address_id = (select id from address where address = 'addr1q8ur464mlqsqslh0dn9dqg88zn0q0sqag2hkxc0vhtrn5c7wkhumlr876ehcm8ltdwt7s49mwxfw47c4hcf5p6qdlavqaawfcs')
    AND tx.valid_contract = 'true' -- don't count utxos that are part of transaction that failed script validation at stage 2
  GROUP BY txo.id
)
SELECT (
    SELECT 'addr1q8ur464mlqsqslh0dn9dqg88zn0q0sqag2hkxc0vhtrn5c7wkhumlr876ehcm8ltdwt7s49mwxfw47c4hcf5p6qdlavqaawfcs'
  ) AS "address",
  (
    SELECT COALESCE(SUM(amount), 0)::TEXT -- cast to TEXT to avoid number overflow
    FROM queried_amount
  ) AS "amount_lovelace",
  (
    SELECT json_agg(
        json_build_object(
          'unit',
          token_name,
          'quantity',
          token_quantity::TEXT -- cast to TEXT to avoid number overflow
        )
      )
    FROM (
        SELECT CONCAT(encode(policy, 'hex'), encode(name, 'hex')) AS "token_name",
          SUM(quantity) AS "token_quantity"
        FROM ma_tx_out mto
          JOIN multi_asset ma ON (mto.ident = ma.id)
        WHERE mto.id IN (
            SELECT unnest(assets_ids)
            FROM queried_amount
          )
        GROUP BY ma.name,
          ma.policy
        ORDER BY (ma.policy, ma.name)
      ) AS "assets"
  ) AS "amount",
  (
    SELECT sa.view
    FROM stake_address sa
    WHERE sa.id = (
        SELECT *
        FROM queried_address
      )
  ) AS "stake_address"




  SELECT unit,
  quantity::TEXT -- cast to TEXT to avoid number overflow
FROM(
    SELECT CONCAT(encode(ma.policy, 'hex'), encode(ma.name, 'hex')) AS "unit",
      SUM(quantity) AS "quantity"
    FROM tx_out txo
      JOIN tx ON (tx.id = txo.tx_id)
      JOIN stake_address sa ON (sa.id = txo.stake_address_id)
      LEFT JOIN tx_in txi ON (txo.tx_id = txi.tx_out_id)
      AND (txo.index = txi.tx_out_index)
      JOIN ma_tx_out mto ON (mto.tx_out_id = txo.id)
      JOIN multi_asset ma ON (mto.ident = ma.id)
    WHERE txi IS NULL
      AND sa.view = 'stake1u88tt7dl3nldvmudnl4kh9lg2jahryh2lv2muy6qaqxl7kqhm44zt'
      AND tx.valid_contract = 'true' -- don't count utxos that are part of transaction that failed script validation at stage 2
    GROUP BY ma.policy,
      ma.name
    ORDER BY CASE
        WHEN LOWER('desc') = 'desc' THEN MAX(txo.id)
      END DESC,
      CASE
        WHEN LOWER('desc') <> 'desc'
        OR 'desc' IS NULL THEN MIN(txo.id)
      END ASC,
      (ma.policy, ma.name) ASC
    LIMIT 100
  ) AS "ordered_assets"