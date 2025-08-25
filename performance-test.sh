#!/bin/bash

# Performance testing script for Cardano DB Sync API
# Run this after applying the performance indexes

set -e

API_BASE="${API_BASE:-http://localhost:8080}"
API_KEY="${API_KEY:-}"

if [ -z "$API_KEY" ]; then
    echo "⚠️  Warning: No API_KEY set. Make sure to set API_KEY environment variable"
    echo "   or the API must be running without authentication"
fi

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to make authenticated API calls
api_call() {
    local endpoint="$1"
    local description="$2"
    
    echo -e "${BLUE}Testing: ${description}${NC}"
    echo -e "${YELLOW}GET ${endpoint}${NC}"
    
    if [ -n "$API_KEY" ]; then
        time curl -s -H "X-API-Key: $API_KEY" "${API_BASE}${endpoint}" > /dev/null
    else
        time curl -s "${API_BASE}${endpoint}" > /dev/null
    fi
    
    echo ""
}

# Function to test with timing
time_api_call() {
    local endpoint="$1"
    local description="$2"
    
    echo -e "${BLUE}⏱️  Timing: ${description}${NC}"
    
    if [ -n "$API_KEY" ]; then
        local response_time=$(curl -s -w "%{time_total}" -H "X-API-Key: $API_KEY" "${API_BASE}${endpoint}" -o /dev/null)
    else
        local response_time=$(curl -s -w "%{time_total}" "${API_BASE}${endpoint}" -o /dev/null)
    fi
    
    echo -e "${GREEN}Response time: ${response_time}s${NC}"
    
    # Check if response time is under 1 second
    if (( $(echo "$response_time < 1.0" | bc -l) )); then
        echo -e "${GREEN}✅ Fast (< 1s)${NC}"
    else
        echo -e "${RED}⚠️  Slow (>= 1s)${NC}"
    fi
    
    echo ""
}

echo "========================================="
echo "🚀 Cardano DB Sync API Performance Test"
echo "========================================="
echo "API Base: $API_BASE"
echo ""

# Test basic endpoints first
echo -e "${BLUE}📊 Testing Basic Endpoints${NC}"
echo "========================================="

api_call "/health" "Health check"
time_api_call "/network" "Network information"
time_api_call "/blocks/latest" "Latest block"
time_api_call "/epochs/latest" "Current epoch"

echo ""
echo -e "${BLUE}🧱 Testing Block Endpoints${NC}"
echo "========================================="

# Get latest block number for testing
if [ -n "$API_KEY" ]; then
    LATEST_BLOCK=$(curl -s -H "X-API-Key: $API_KEY" "${API_BASE}/blocks/latest" | jq -r '.height' 2>/dev/null || echo "")
else
    LATEST_BLOCK=$(curl -s "${API_BASE}/blocks/latest" | jq -r '.height' 2>/dev/null || echo "")
fi

if [ -n "$LATEST_BLOCK" ] && [ "$LATEST_BLOCK" != "null" ]; then
    echo "Latest block: $LATEST_BLOCK"
    time_api_call "/blocks/$LATEST_BLOCK" "Block by number"
    
    # Test a few blocks back
    OLDER_BLOCK=$((LATEST_BLOCK - 100))
    time_api_call "/blocks/$OLDER_BLOCK" "Older block by number"
else
    echo "⚠️  Could not get latest block number"
fi

echo ""
echo -e "${BLUE}💰 Testing Transaction Endpoints${NC}"
echo "========================================="

# These would need real transaction hashes from your database
echo "ℹ️  Transaction tests require real tx hashes from your database"
echo "   Example: time_api_call '/txs/YOUR_TX_HASH' 'Transaction details'"

echo ""
echo -e "${BLUE}🏠 Testing Address Endpoints (Most Critical)${NC}"
echo "========================================="

# Test with common address patterns - you'll need to replace with real addresses
echo "ℹ️  Address tests require real addresses from your database"
echo "   These are the most performance-critical endpoints:"
echo ""

# Sample address format (replace with real ones)
SAMPLE_ADDRESSES=(
    "addr1qxyz..." # Replace with real addresses
    "addr1qabc..." # Replace with real addresses  
)

for addr in "${SAMPLE_ADDRESSES[@]}"; do
    if [[ $addr != "addr1q"* ]]; then
        continue # Skip sample addresses
    fi
    
    echo -e "${YELLOW}Testing address: ${addr:0:20}...${NC}"
    time_api_call "/addresses/$addr" "Address balance"
    time_api_call "/addresses/$addr/transactions?count=10" "Address transactions (10)"
    time_api_call "/addresses/$addr/utxos?count=10" "Address UTXOs (10)"
    echo ""
done

echo ""
echo -e "${BLUE}🪙 Testing Asset Endpoints${NC}"
echo "========================================="

echo "ℹ️  Asset tests require real policy IDs from your database"
echo "   Example: time_api_call '/assets/POLICY_ID' 'Asset info'"

echo ""
echo -e "${BLUE}🔍 Testing NFT Endpoints${NC}"
echo "========================================="

echo "ℹ️  NFT tests require real policy IDs from your database"
echo "   Example: time_api_call '/nft-owners?policy_id=POLICY_ID' 'NFT owners'"

echo ""
echo "========================================="
echo -e "${GREEN}✅ Performance Test Complete!${NC}"
echo "========================================="
echo ""
echo "🎯 Performance Goals:"
echo "   • Network info: < 0.5s"
echo "   • Block queries: < 0.3s"  
echo "   • Address balance: < 0.5s"
echo "   • Address transactions: < 1.0s"
echo "   • Address UTXOs: < 1.0s"
echo "   • Asset queries: < 0.5s"
echo ""
echo "🔧 If any endpoints are slow:"
echo "   1. Make sure you've run: migrations/migrations.sql"
echo "   2. Check database connection pool settings"
echo "   3. Monitor database CPU/memory usage"
echo "   4. Consider adding more specific indexes for your data patterns"
echo ""
echo "📊 To profile specific queries:"
echo "   1. Connect to your database"
echo "   2. Run: EXPLAIN ANALYZE [your slow query]"
echo "   3. Look for sequential scans and high cost operations"