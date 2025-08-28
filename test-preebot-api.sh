#!/bin/bash

# PREEBOT API Test Script - Complete replacement for Koios and Blockfrost
# Tests all endpoints needed by PREEBOT Discord bot

API_BASE="http://localhost:8080"
API_KEY="${API_KEY:-ab7eb862bf9284d721268b4fcf44658799007315e595c40cfae2287357d508ca}"

# Test address (from your earlier tests)
TEST_ADDRESS="addr1q8ur464mlqsqslh0dn9dqg88zn0q0sqag2hkxc0vhtrn5c7wkhumlr876ehcm8ltdwt7s49mwxfw47c4hcf5p6qdlavqaawfcs"
TEST_STAKE_ADDRESS="stake1uy6yzwsxxc28lfms0qmpxvyz9a7y770rtcqx9y96m42cttqsrxjhe"
TEST_POOL_ID="pool1z4fqh2rj7p2f8wfhs73hzks8vd5x0w52t5fh3hqhs5l4jrf8ypa"
TEST_POLICY_ID="f6f49b186751e61f1fb8c64e7504e771f968b2b01c27e795f10c6d22"
TEST_TX_HASH="8f9e3a2b1c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a0b1c2d3e4f5a6b7c8d9e0f"
TEST_HANDLE="test"

echo "🧪 PREEBOT API Comprehensive Test Suite"
echo "======================================="
echo "Base URL: $API_BASE"
echo "Test Address: $TEST_ADDRESS"
echo ""

# Function to test an endpoint
test_endpoint() {
    local name="$1"
    local url="$2"
    local expected_fields="$3"
    
    echo "Testing: $name"
    echo "GET $url"
    
    response=$(curl -s -H "X-API-Key: $API_KEY" "$url")
    
    if echo "$response" | jq . > /dev/null 2>&1; then
        echo "✅ Valid JSON response"
        
        # Check for expected fields if provided
        if [ -n "$expected_fields" ]; then
            for field in $expected_fields; do
                if echo "$response" | jq -e ".$field" > /dev/null 2>&1; then
                    echo "  ✅ Field '$field' present"
                else
                    echo "  ❌ Field '$field' missing"
                fi
            done
        fi
        
        # Show response size
        size=$(echo "$response" | jq '. | length' 2>/dev/null || echo "N/A")
        echo "  📊 Response size: $size items"
    else
        echo "❌ Invalid JSON or error:"
        echo "$response" | head -5
    fi
    echo ""
}

echo "🔍 1. ADDRESS INFORMATION ENDPOINTS"
echo "===================================="

# Test address info (Koios replacement)
test_endpoint "Address Info" \
    "$API_BASE/preebot-api/addresses/$TEST_ADDRESS" \
    "address balance stake_address type"

# Test address transactions (wallet verification)
test_endpoint "Address Transactions" \
    "$API_BASE/preebot-api/addresses/$TEST_ADDRESS/transactions" \
    ""

echo "👤 2. ACCOUNT & DELEGATION ENDPOINTS"
echo "====================================="

# Test account info (Blockfrost replacement)
test_endpoint "Account Info" \
    "$API_BASE/preebot-api/accounts/$TEST_STAKE_ADDRESS" \
    "stake_address active controlled_amount pool_id"

# Test delegation history (for role assignment)
test_endpoint "Delegation History" \
    "$API_BASE/preebot-api/accounts/$TEST_STAKE_ADDRESS/history" \
    ""

# Test account assets (for role assignment)
test_endpoint "Account Assets" \
    "$API_BASE/preebot-api/accounts/$TEST_STAKE_ADDRESS/assets" \
    ""

echo "🏊 3. POOL INFORMATION ENDPOINTS"
echo "==============================="

# Test pool info
test_endpoint "Pool Information" \
    "$API_BASE/preebot-api/pools/$TEST_POOL_ID" \
    "pool_id live_stake live_delegators blocks_minted"

# Test pool blocks (for notifications)
test_endpoint "Pool Blocks" \
    "$API_BASE/preebot-api/pools/$TEST_POOL_ID/blocks" \
    ""

echo "🎨 4. ASSET & POLICY ENDPOINTS"
echo "=============================="

# Test assets by policy (critical for role assignment)
test_endpoint "Assets by Policy" \
    "$API_BASE/preebot-api/assets/policy/$TEST_POLICY_ID" \
    ""

# Test asset mints (for tracking)
test_endpoint "Asset Mints" \
    "$API_BASE/preebot-api/assets/policy/$TEST_POLICY_ID/mints" \
    ""

echo "💳 5. TRANSACTION ENDPOINTS"
echo "==========================="

# Test transaction UTXOs (for verification)
test_endpoint "Transaction UTXOs" \
    "$API_BASE/preebot-api/txs/$TEST_TX_HASH/utxos" \
    "hash inputs outputs"

echo "⛓️  6. BLOCKCHAIN INFO ENDPOINTS"
echo "==============================="

# Test blockchain tip (for sync status)
test_endpoint "Blockchain Tip" \
    "$API_BASE/preebot-api/tip" \
    "height hash epoch time"

echo "🏷️  7. HANDLE RESOLUTION"
echo "======================="

# Test handle resolution
test_endpoint "Handle Resolution" \
    "$API_BASE/preebot-api/handles/$TEST_HANDLE" \
    "handle resolved"

echo "🚀 8. DISCORD-OPTIMIZED ENDPOINTS"
echo "================================="

# Test ultra-fast asset holdings (Discord bot)
test_endpoint "Ultra-Fast Asset Holdings" \
    "$API_BASE/preebot/asset-holdings?address=$TEST_ADDRESS" \
    "address holdings count"

# Test NFT traits for role assignment
test_endpoint "NFT Traits (Role Assignment)" \
    "$API_BASE/preebot/nft-traits?address=$TEST_ADDRESS" \
    "address total_nfts matching_nfts"

echo "📊 PERFORMANCE COMPARISON"
echo "========================"

echo "Testing response times..."

# Time the ultra-fast Discord endpoint
echo -n "Discord endpoint: "
time_result=$(curl -s -w "%{time_total}" -o /dev/null -H "X-API-Key: $API_KEY" \
    "$API_BASE/preebot/asset-holdings?address=$TEST_ADDRESS")
echo "${time_result}s"

# Time the PREEBOT API endpoint
echo -n "PREEBOT API endpoint: "
time_result=$(curl -s -w "%{time_total}" -o /dev/null -H "X-API-Key: $API_KEY" \
    "$API_BASE/preebot-api/addresses/$TEST_ADDRESS")
echo "${time_result}s"

echo ""
echo "✅ PREEBOT API Testing Complete!"
echo "================================"
echo ""
echo "🎯 Ready to Replace Koios and Blockfrost!"
echo ""
echo "📋 PREEBOT Integration Endpoints:"
echo "   • Address Info:        GET /preebot-api/addresses/{address}"
echo "   • Account Info:        GET /preebot-api/accounts/{stake_address}"
echo "   • Delegation History:  GET /preebot-api/accounts/{stake_address}/history"
echo "   • Account Assets:      GET /preebot-api/accounts/{stake_address}/assets"
echo "   • Pool Info:           GET /preebot-api/pools/{pool_id}"
echo "   • Pool Blocks:         GET /preebot-api/pools/{pool_id}/blocks"
echo "   • Assets by Policy:    GET /preebot-api/assets/policy/{policy_id}"
echo "   • Asset Mints:         GET /preebot-api/assets/policy/{policy_id}/mints"
echo "   • Transaction UTXOs:   GET /preebot-api/txs/{tx_hash}/utxos"
echo "   • Blockchain Tip:      GET /preebot-api/tip"
echo "   • Handle Resolution:   GET /preebot-api/handles/{handle}"
echo ""
echo "⚡ Discord-Optimized:"
echo "   • Fast Holdings:       GET /preebot/asset-holdings?address={address}"
echo "   • Role Assignment:     GET /preebot/nft-traits?address={address}&policies={policies}"
echo ""
echo "🔄 To keep Discord data real-time, run every minute:"
echo "   curl -H \"X-API-Key: $API_KEY\" -H \"Content-Type: application/json\" \\"
echo "     -d '{\"action\": \"holdings\"}' $API_BASE/preebot/refresh-discord-views"
echo ""