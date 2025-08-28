# 🤖 Discord Bot Integration Guide - ULTRA-FAST Real-Time System

This API provides **lightning-fast** endpoints optimized for Discord bot role assignment based on Cardano NFT holdings and traits, using **materialized views** that refresh every minute for real-time data with maximum 1-minute staleness.

## 🚀 Quick Start

### 0. Setup Ultra-Fast System (First Time Only)
Initialize the materialized views for sub-second Discord queries:

```bash
# Run the setup script
./setup-discord-views.sh

# Or manually:
psql $DB_URL -f migrations/migrations.sql
curl -X POST -H "X-API-Key: $API_KEY" -H "Content-Type: application/json" \
  -d '{"action": "initialize"}' \
  http://localhost:8080/preebot/refresh-discord-views
```

### ⚡ Keep Data Real-Time (Cron Job)
Set up automatic refresh every minute for real-time Discord data:

```bash
# Add to crontab (crontab -e)
* * * * * curl -s -H "X-API-Key: YOUR_API_KEY" -H "Content-Type: application/json" -d '{"action": "holdings"}' http://localhost:8080/preebot/refresh-discord-views
```

### 1. Enhanced Asset Holdings
Get all assets (including NFTs) that an address holds with metadata and traits:

```bash
GET /preebot/asset-holdings?address=addr1xyz...
```

**Response includes:**
- All tokens and NFTs
- CIP-25 metadata for NFTs
- Extracted traits for role assignment
- Collection information

### 2. NFT Trait Queries for Role Assignment
Check if users hold NFTs with specific traits:

```bash
# Single address with trait requirements
GET /preebot/nft-traits?address=addr1xyz...&policies=policy123&traits=rarity:legendary,type:warrior

# Batch check multiple addresses
POST /preebot/nft-traits
{
  "addresses": ["addr1...", "addr2..."],
  "policies": ["policy123", "policy456"],
  "required_traits": {
    "rarity": "legendary",
    "faction": "fire"
  }
}
```

## 📋 Discord Role Assignment Examples

### Example 1: Basic NFT Holder Role
```javascript
// Check if user holds any NFT from a specific collection
const response = await fetch(`${API_BASE}/preebot/asset-holdings?address=${address}`, {
  headers: { 'X-API-Key': API_KEY }
});
const data = await response.json();

// Check if they hold NFTs from your policy
const hasCollectionNFT = data.holdings.some(nft => 
  nft.policy_id === "your_collection_policy_id"
);

if (hasCollectionNFT) {
  // Assign "NFT Holder" role
  await member.roles.add(nftHolderRoleId);
}
```

### Example 2: Trait-Based Roles
```javascript
// Check for specific traits (e.g., legendary rarity)
const response = await fetch(`${API_BASE}/preebot/nft-traits?address=${address}&policies=policy123&traits=rarity:legendary`, {
  headers: { 'X-API-Key': API_KEY }
});
const data = await response.json();

if (data.matching_nfts > 0) {
  // Assign "Legendary Holder" role
  await member.roles.add(legendaryRoleId);
}

// Check policy counts for tiered roles
if (data.policy_counts?.policy123 >= 5) {
  // Assign "Whale" role for holding 5+ NFTs
  await member.roles.add(whaleRoleId);
}
```

### Example 3: Batch Role Updates
```javascript
// Update roles for multiple users efficiently
const addresses = members.map(m => m.walletAddress);
const response = await fetch(`${API_BASE}/preebot/nft-traits`, {
  method: 'POST',
  headers: { 
    'X-API-Key': API_KEY,
    'Content-Type': 'application/json'
  },
  body: JSON.stringify({
    addresses: addresses,
    policies: ["collection_policy_id"],
    required_traits: { "rarity": "rare" }
  })
});

const data = await response.json();

// Process each user's results
for (const [address, result] of Object.entries(data.results)) {
  const member = findMemberByAddress(address);
  if (result.matching_nfts > 0) {
    await member.roles.add(rareHolderRoleId);
  }
}
```

## ⚡ Performance Optimization

### Pre-cache NFT Metadata
For collections you check frequently, pre-cache metadata for instant queries:

```javascript
// Cache metadata for your main collections
await fetch(`${API_BASE}/preebot/cache-nft-metadata`, {
  method: 'POST',
  headers: { 
    'X-API-Key': API_KEY,
    'Content-Type': 'application/json' 
  },
  body: JSON.stringify({
    policy_ids: [
      "your_main_collection_policy",
      "your_other_collection_policy"
    ]
  })
});
```

### Rate Limiting & Caching
- Use batch endpoints when possible
- Cache user roles locally for 5-10 minutes
- Use Redis/memory cache for frequently accessed data
- Implement exponential backoff for failed requests

## 🛠️ Database Setup

Run the migration to set up optimized indexes and caching tables:

```bash
psql $DB_URL -f migrations/migrations.sql
```

## 📊 Response Formats

### Asset Holdings Response
```json
{
  "address": "addr1...",
  "holdings": [
    {
      "asset_id": "policy123asset456",
      "policy_id": "policy123",
      "asset_name": "asset456",
      "quantity": "1",
      "traits": {
        "rarity": "legendary",
        "faction": "fire",
        "power": "9000"
      },
      "metadata": { /* Full CIP-25 metadata */ },
      "nft_info": {
        "collection_name": "Cool Dragons",
        "collection_size": 10000
      }
    }
  ],
  "count": 1,
  "timestamp": 1625097600
}
```

### NFT Traits Response
```json
{
  "address": "addr1...",
  "total_nfts": 5,
  "matching_nfts": 2,
  "nfts": [
    {
      "asset_id": "policy123asset456",
      "policy_id": "policy123",
      "traits": {
        "rarity": "legendary",
        "faction": "fire"
      }
    }
  ],
  "policy_counts": {
    "policy123": 2
  },
  "required_traits": {
    "rarity": "legendary"
  },
  "timestamp": 1625097600
}
```

## 🔒 Authentication

All endpoints require API key authentication:

```javascript
// Header method (recommended)
headers: { 'X-API-Key': 'your-api-key' }

// Bearer token method
headers: { 'Authorization': 'Bearer your-api-key' }

// Query parameter method
const url = `${API_BASE}/preebot/asset-holdings?address=${addr}&api_key=${key}`;
```

## 🎯 Common Use Cases

1. **NFT Holder Verification**: Check if user holds any NFT from collection
2. **Trait-Based Roles**: Assign roles based on NFT traits (rarity, faction, etc.)
3. **Tiered Membership**: Different roles based on number of NFTs held
4. **Collection Completion**: Roles for users who hold rare combinations
5. **Dynamic Role Updates**: Automatically update roles when holdings change

## 📈 Monitoring & Analytics

- Monitor response times using the `timestamp` field
- Track `total_nfts` vs `matching_nfts` for role assignment rates  
- Use `policy_counts` for collection popularity analytics
- Cache hit rates for performance optimization

## 🚨 Error Handling

```javascript
try {
  const response = await fetch(apiEndpoint, options);
  if (!response.ok) {
    throw new Error(`API error: ${response.status}`);
  }
  const data = await response.json();
  // Process successful response
} catch (error) {
  console.error('Discord role update failed:', error);
  // Handle gracefully - maybe retry later or use cached data
}
```