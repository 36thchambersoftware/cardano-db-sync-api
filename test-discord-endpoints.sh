#!/bin/bash

# Test script for Discord bot optimized endpoints
# Make sure the API server is running and you have a valid API key

API_BASE="http://localhost:8080"
API_KEY="your-api-key-here"

echo "🚀 Testing PREEBOT Discord Bot Endpoints"
echo "========================================="

# Test 1: Enhanced asset holdings with NFT metadata
echo ""
echo "1️⃣  Testing enhanced asset holdings endpoint..."
echo "GET /preebot/asset-holdings?address=<test-address>"
curl -s -H "X-API-Key: $API_KEY" \
  "$API_BASE/preebot/asset-holdings?address=addr_test1234" \
  | jq '.' || echo "❌ Failed"

# Test 2: NFT traits for Discord role assignment
echo ""
echo "2️⃣  Testing NFT traits endpoint..."
echo "GET /preebot/nft-traits?address=<test-address>&policies=<policy-id>"
curl -s -H "X-API-Key: $API_KEY" \
  "$API_BASE/preebot/nft-traits?address=addr_test1234&policies=abc123" \
  | jq '.' || echo "❌ Failed"

# Test 3: Batch NFT traits query
echo ""
echo "3️⃣  Testing batch NFT traits query..."
echo "POST /preebot/nft-traits"
curl -s -H "X-API-Key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"addresses": ["addr_test1234"], "required_traits": {"rarity": "legendary"}}' \
  "$API_BASE/preebot/nft-traits" \
  | jq '.' || echo "❌ Failed"

# Test 4: Cache NFT metadata
echo ""
echo "4️⃣  Testing NFT metadata caching..."
echo "POST /preebot/cache-nft-metadata"
curl -s -H "X-API-Key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"policy_ids": ["test_policy_123"]}' \
  "$API_BASE/preebot/cache-nft-metadata" \
  | jq '.' || echo "❌ Failed"

echo ""
echo "✅ Discord bot endpoint testing complete!"
echo ""
echo "📋 Available endpoints for your Discord bot:"
echo "   • GET  /preebot/asset-holdings?address=<addr>           - Fast holdings with metadata"
echo "   • GET  /preebot/nft-traits?address=<addr>&traits=<...>  - Role assignment queries"
echo "   • POST /preebot/nft-traits                              - Batch role checks"
echo "   • POST /preebot/cache-nft-metadata                      - Optimize performance"
echo ""
echo "🔧 Usage for Discord role assignment:"
echo "   1. Use asset-holdings to see all NFTs a user holds"
echo "   2. Use nft-traits to check specific trait requirements"
echo "   3. Cache metadata for collections you frequently check"
echo ""