# Cardano DB Sync API

A comprehensive REST API for querying Cardano blockchain data from your own Cardano DB Sync instance. This API provides Blockfrost-compatible endpoints, allowing you to replace external services with your own infrastructure.

## Features

- 🚀 **Blockfrost-compatible endpoints** - Drop-in replacement for most Blockfrost API calls
- 💰 **Cost-effective** - Run on your own infrastructure instead of paying per API call
- ⚡ **Fast caching** - Redis-based caching for optimal performance
- 🔍 **Comprehensive data** - Access blocks, transactions, addresses, assets, epochs, and more
- 📄 **Pagination support** - Handle large datasets efficiently
- 🛡️ **Error handling** - Robust error responses and validation

## Quick Start

### Prerequisites

- [Cardano DB Sync](https://github.com/IntersectMBO/cardano-db-sync) running and synced
- PostgreSQL database with Cardano DB Sync data
- Redis server for caching
- Go 1.23+ for building from source

### Security Notice

🔐 **This API requires authentication** - All endpoints (except `/` and `/health`) require a valid API key.

### Installation

1. Clone this repository:
```bash
git clone https://github.com/your-username/cardano-db-sync-api.git
cd cardano-db-sync-api
```

2. Copy environment configuration:
```bash
cp .env.example .env
```

3. Update `.env` with your database connection details:
```env
DB_URL=postgres://username:password@localhost:5432/cexplorer?sslmode=disable
REDIS_URL=redis://localhost:6379
PORT=8080

# Generate API keys for security
API_KEYS=your-secret-key-1,your-secret-key-2
ENVIRONMENT=production
```

4. Build and run:
```bash
go build -o cardano-api .
./cardano-api
```

The API will be available at `http://localhost:8080`

## Authentication

### Generating API Keys

For development, you can generate secure API keys:

```bash
# Set environment to development
export ENVIRONMENT=development

# Start the API
./cardano-api

# Generate a new API key
curl http://localhost:8080/generate-key
```

This returns a 64-character hex API key that you can add to your `API_KEYS` environment variable.

### Using API Keys

Include your API key in requests using any of these methods:

**Method 1: Header**
```bash
curl -H "X-API-Key: your-api-key" http://localhost:8080/network
```

**Method 2: Bearer Token**
```bash
curl -H "Authorization: Bearer your-api-key" http://localhost:8080/network
```

**Method 3: Query Parameter**
```bash
curl "http://localhost:8080/network?api_key=your-api-key"
```

### Public Endpoints

These endpoints don't require authentication:
- `GET /` - API documentation (home page)
- `GET /health` - Health check endpoint
- `GET /generate-key` - Generate API key (development mode only)

## API Endpoints

**⚠️ All endpoints below require authentication**

### Network Information
- `GET /network` - Get network supply and stake information

### Blocks
- `GET /blocks/latest` - Get the latest block
- `GET /blocks/{hash_or_number}` - Get specific block by hash or number

### Transactions
- `GET /txs/{hash}` - Get transaction details
- `GET /txs/{hash}/utxos` - Get transaction inputs and outputs

### Addresses
- `GET /addresses/{address}` - Get address information and balance
- `GET /addresses/{address}/transactions` - Get transactions for address (paginated)
- `GET /addresses/{address}/utxos` - Get UTXOs for address (paginated)

### Assets
- `GET /assets/{asset_id}` - Get asset information
- `GET /assets/{asset_id}/addresses` - Get addresses holding the asset (paginated)

### Epochs
- `GET /epochs/latest` - Get current epoch information

### Pagination

All paginated endpoints support these query parameters:
- `count` - Number of results per page (default: 100, max: 1000)
- `page` - Page number (default: 1)
- `order` - Sort order: `asc` or `desc` (default: `asc`)

Example: `GET /addresses/addr1.../transactions?count=50&page=2&order=desc`

### Response Format

All responses are in JSON format. Error responses follow this structure:

```json
{
  "status_code": 400,
  "error": "Bad Request",
  "message": "Descriptive error message"
}
```

## Migrating from Blockfrost

This API is designed to be compatible with Blockfrost endpoints. To migrate:

1. Change your base URL from `https://cardano-mainnet.blockfrost.io/api/v0` to your API instance
2. Remove the `project_id` from your requests (no authentication required)
3. Update any hardcoded rate limits (this API has no built-in rate limiting)

### Example Migration

**Before (Blockfrost):**
```javascript
const response = await fetch('https://cardano-mainnet.blockfrost.io/api/v0/addresses/addr1...', {
  headers: { 'project_id': 'your-project-id' }
});
```

**After (Your API):**
```javascript
const response = await fetch('http://your-api-server:8080/addresses/addr1...', {
  headers: { 'X-API-Key': 'your-api-key' }
});
```

## Database Optimization

For optimal performance, ensure these indexes exist in your Cardano DB Sync database:

```sql
-- The migrations/migrations.sql file contains optimized indexes
-- Run these for better performance:

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ma_tx_out_ident_policy_quantity 
  ON ma_tx_out (ident) WHERE quantity = 1;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_tx_out_consumed_by_tx_id 
  ON tx_out (id) WHERE consumed_by_tx_id IS NULL;

-- See migrations/migrations.sql for the complete list
```

## Configuration

### Environment Variables

- `DB_URL` - PostgreSQL connection string for Cardano DB Sync database
- `REDIS_URL` - Redis connection string for caching (optional but recommended)
- `PORT` - Server port (default: 8080)
- `HOST` - Server host (default: 0.0.0.0)
- `MAX_PAGE_SIZE` - Maximum results per page (default: 1000)
- `DEFAULT_PAGE_SIZE` - Default results per page (default: 100)
- `CACHE_DURATION_SECONDS` - Cache duration in seconds (default: 300)
- `API_KEYS` - Comma-separated list of valid API keys for authentication
- `ENVIRONMENT` - Set to "development" to enable the `/generate-key` endpoint

### Security

The API includes several security features:

- **API Key Authentication**: All endpoints (except home and health) require valid API keys
- **Timing Attack Protection**: API key validation uses constant-time comparison
- **Multiple Auth Methods**: Support for headers, bearer tokens, and query parameters
- **Development Mode**: Safe key generation only in development environment
- **No Rate Limiting**: Implement your own rate limiting as needed (e.g., nginx, reverse proxy)

**Security Recommendations:**
- Use strong, randomly generated API keys (64+ characters)
- Rotate API keys regularly
- Use HTTPS in production
- Keep API keys secure and never commit them to version control
- Consider implementing rate limiting at the infrastructure level

### Caching

The API uses Redis for caching frequently requested data:

- **Network info**: 5 minutes
- **Latest block**: 30 seconds
- **Specific blocks**: 2 minutes
- **Transactions**: 5 minutes
- **Address balances**: 2 minutes
- **UTXOs**: 1 minute
- **Asset info**: 5 minutes

## Performance Considerations

- **Database Connection Pooling**: Configure your PostgreSQL connection pool based on expected load
- **Redis Memory**: Monitor Redis memory usage, especially for high-traffic deployments
- **Database Indexes**: Use the provided migration file for optimal query performance
- **Horizontal Scaling**: Run multiple API instances behind a load balancer for high availability

## Legacy NFT Endpoints

This API maintains backward compatibility with existing NFT-specific endpoints:

- `GET /nft-owners?policy_id={policy_id}` - Get NFT holders for a policy

## Development

### Building
```bash
go build -o cardano-api .
```

### Running Tests
```bash
go test ./...
```

### Code Structure

- `main.go` - Server setup and routing
- `handlers.go` - Main API handlers
- `additional_handlers.go` - Extended functionality handlers
- `types.go` - Type definitions
- `migrations/migrations.sql` - Database optimizations

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

- Create an issue on GitHub for bug reports or feature requests
- Check the [Cardano DB Sync documentation](https://github.com/IntersectMBO/cardano-db-sync) for database-related questions

## Acknowledgments

- Built for the Cardano ecosystem
- Compatible with [Blockfrost API](https://blockfrost.io/) endpoints
- Powered by [Cardano DB Sync](https://github.com/IntersectMBO/cardano-db-sync)