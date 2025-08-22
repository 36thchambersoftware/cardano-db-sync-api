#!/bin/bash

# Test script for API authentication
# Make sure the API is running first: ./cardano-api

API_BASE="http://localhost:8080"

echo "🧪 Testing Cardano DB Sync API Authentication"
echo "============================================="

# Test public endpoints (should work without auth)
echo ""
echo "📋 Testing public endpoints..."
echo "GET /health"
curl -s "$API_BASE/health" | jq . || echo "❌ Health check failed"

echo ""
echo "GET / (home page should contain HTML)"
curl -s "$API_BASE/" | head -n 1

# Test protected endpoints without auth (should fail)
echo ""
echo "🚫 Testing protected endpoints without auth (should fail)..."
echo "GET /network (should return 401)"
HTTP_STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$API_BASE/network")
if [ "$HTTP_STATUS" == "401" ]; then
    echo "✅ Correctly rejected unauthorized request"
else
    echo "❌ Expected 401, got $HTTP_STATUS"
fi

# Generate API key in development mode
if [ "$ENVIRONMENT" == "development" ]; then
    echo ""
    echo "🔑 Generating API key..."
    API_KEY=$(curl -s "$API_BASE/generate-key" | jq -r .api_key)
    if [ "$API_KEY" != "null" ] && [ -n "$API_KEY" ]; then
        echo "✅ Generated API key: ${API_KEY:0:16}..."
        
        # Test with API key
        echo ""
        echo "🔐 Testing with API key..."
        echo "GET /network"
        curl -s -H "X-API-Key: $API_KEY" "$API_BASE/network" | jq . || echo "❌ Network request failed"
        
        echo ""
        echo "✅ Authentication working correctly!"
        echo "Add this to your .env file: API_KEYS=$API_KEY"
    else
        echo "❌ Failed to generate API key"
    fi
else
    echo ""
    echo "ℹ️  Set ENVIRONMENT=development to test key generation"
    echo "   For production, manually set API_KEYS in your .env file"
fi

echo ""
echo "🎉 Authentication test complete!"