package main

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"html/template"
	"log"
	"log/slog"
	"net/http"
	"os"
	"strings"

	"github.com/go-redis/redis/v8"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
)

var (
	ctx         = context.Background()
	db          *sql.DB
	redisClient *redis.Client
	routes      []Route
)

// Route stores metadata for an API endpoint
type Route struct {
	Path        string
	Description string
	Usage       string
}

func init() {
	var err error

	err = godotenv.Load()
    if err != nil {
        log.Println("No .env file found or couldn't load it")
    }

	// Connect to Postgres
	dbURL := os.Getenv("DB_URL") // Example: postgres://user:pass@localhost/dbname?sslmode=disable
	db, err = sql.Open("postgres", dbURL)
	if err != nil {
		log.Fatalf("Failed to connect to DB: %v", err)
	}

	// Connect to Redis
	redisClient = redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
}

// registerRoute binds a handler to a path and records route metadata
func registerRoute(path, description, usage string, handler http.HandlerFunc) {
	routes = append(routes, Route{Path: path, Description: description, Usage: usage})
	http.HandleFunc(path, handler)
}

func homeHandler(w http.ResponseWriter, r *http.Request) {
	tmpl := `
<!DOCTYPE html>
<html>
<head>
    <title>Cardano DB Sync API</title>
    <style>
        body { font-family: sans-serif; padding: 2em; }
        code { background: #eee; padding: 0.2em 0.4em; border-radius: 4px; }
        li { margin-bottom: 1em; }
        .category { margin-top: 2em; }
        .category h3 { color: #333; border-bottom: 2px solid #eee; padding-bottom: 0.5em; }
    </style>
</head>
<body>
    <h1>Cardano DB Sync API</h1>
    <p>A comprehensive API for querying Cardano blockchain data, compatible with Blockfrost API endpoints.</p>
    
    <div style="background: #f0f8ff; padding: 1em; border-radius: 8px; margin: 1em 0;">
        <h3>🔐 Authentication</h3>
        <p>All API endpoints require authentication. Include your API key using one of these methods:</p>
        <ul>
            <li><strong>Header:</strong> <code>X-API-Key: your-api-key</code></li>
            <li><strong>Bearer Token:</strong> <code>Authorization: Bearer your-api-key</code></li>
            <li><strong>Query Parameter:</strong> <code>?api_key=your-api-key</code></li>
        </ul>
    </div>

    <div class="category">
        <h3>Network</h3>
        <ul>
            <li><strong><code>/network</code></strong><br>Get network information including supply and stake</li>
        </ul>
    </div>

    <div class="category">
        <h3>Blocks</h3>
        <ul>
            <li><strong><code>/blocks/latest</code></strong><br>Get the latest block</li>
            <li><strong><code>/blocks/{hash_or_number}</code></strong><br>Get specific block by hash or number</li>
        </ul>
    </div>

    <div class="category">
        <h3>Transactions</h3>
        <ul>
            <li><strong><code>/txs/{hash}</code></strong><br>Get transaction details by hash</li>
            <li><strong><code>/txs/{hash}/utxos</code></strong><br>Get transaction inputs and outputs</li>
        </ul>
    </div>

    <div class="category">
        <h3>Addresses</h3>
        <ul>
            <li><strong><code>/addresses/{address}</code></strong><br>Get address information and balance</li>
            <li><strong><code>/addresses/{address}/transactions</code></strong><br>Get transactions for address</li>
            <li><strong><code>/addresses/{address}/utxos</code></strong><br>Get UTXOs for address</li>
        </ul>
    </div>

    <div class="category">
        <h3>Assets</h3>
        <ul>
            <li><strong><code>/assets/{asset_id}</code></strong><br>Get asset information</li>
            <li><strong><code>/assets/{asset_id}/addresses</code></strong><br>Get addresses holding the asset</li>
        </ul>
    </div>

    <div class="category">
        <h3>Epochs</h3>
        <ul>
            <li><strong><code>/epochs/latest</code></strong><br>Get current epoch information</li>
        </ul>
    </div>

    <div class="category">
        <h3>Legacy NFT Endpoints</h3>
        <ul>
            {{range .}}
            <li>
                <strong><code>{{.Path}}</code></strong><br>
                {{.Description}}<br>
                <em>Usage:</em> <code>{{.Usage}}</code>
            </li>
            {{end}}
        </ul>
    </div>
</body>
</html>`

	t := template.Must(template.New("home").Parse(tmpl))
	t.Execute(w, routes)
}

func nftOwnersHandler(w http.ResponseWriter, r *http.Request) {
	policyID := r.URL.Query().Get("policy_id")
	if policyID == "" {
		http.Error(w, "Missing policy_id parameter", http.StatusBadRequest)
		return
	}

	// cacheKey := "nft_owners:" + policyID
	// if cached, err := redisClient.Get(ctx, cacheKey).Result(); err == nil {
	// 	w.Header().Set("Content-Type", "application/json")
	// 	w.Write([]byte(cached))
	// 	return
	// }

	decodedPolicy, err := hex.DecodeString(policyID)
    if err != nil {
        http.Error(w, "Invalid policy_id hex", http.StatusBadRequest)
        return
    }
	slog.Default().Info("Fetching NFT owners", "policy_id", decodedPolicy)

	query := `
	SELECT
		address.address AS owner_address,
		SUM(mto.quantity)::BIGINT AS total_quantity
	FROM ma_tx_out mto
	JOIN tx_out txo ON mto.tx_out_id = txo.id
	JOIN address ON txo.address_id = address.id
	JOIN multi_asset ma ON mto.ident = ma.id
	WHERE ma.policy = $1
	  AND txo.consumed_by_tx_id IS NULL
	  AND mto.quantity = 1
	  AND address.has_script = FALSE
	GROUP BY address.address
	ORDER BY total_quantity DESC;
	`

	rows, err := db.QueryContext(ctx, query, decodedPolicy)
	if err != nil {
		http.Error(w, "Database query error", http.StatusInternalServerError)
		log.Printf("DB error: %v", err)
		return
	}
	defer rows.Close()

	slog.Default().Info("Query executed", "query", query)
	slog.Default().Info("Rows returned", "count", rows.Err() == nil)
	if err := rows.Err(); err != nil {
		http.Error(w, "Error reading rows", http.StatusInternalServerError)
		log.Printf("Row error: %v", err)
		return
	}

	var results []NFTResponse
	for rows.Next() {
		var res NFTResponse
		if err := rows.Scan(&res.Address, &res.Count); err != nil {
			http.Error(w, "Scan error", http.StatusInternalServerError)
			return
		}
		results = append(results, res)
	}

	slog.Default().Info("NFT owners fetched", "count", len(results))

	jsonData, err := json.Marshal(results)
	if err != nil {
		http.Error(w, "JSON encode error", http.StatusInternalServerError)
		return
	}

	// redisClient.Set(ctx, cacheKey, jsonData, 1*time.Minute)

	w.Header().Set("Content-Type", "application/json")
	w.Write(jsonData)
}


func main() {
	// Public endpoints (no auth required)
	http.HandleFunc("/", homeHandler)
	http.HandleFunc("/health", healthHandler)
	
	// Development endpoint (only in dev mode)
	http.HandleFunc("/generate-key", generateKeyHandler)
	
	// Protected API endpoints (require authentication)
	http.HandleFunc("/network", authHandler(getNetworkHandler))
	
	// Block endpoints
	http.HandleFunc("/blocks/latest", authHandler(getLatestBlockHandler))
	http.HandleFunc("/blocks/", authHandler(func(w http.ResponseWriter, r *http.Request) {
		getBlockHandler(w, r)
	}))
	
	// Transaction endpoints
	http.HandleFunc("/txs/", authHandler(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		if strings.HasSuffix(path, "/utxos") {
			getTransactionUTXOsHandler(w, r)
		} else {
			getTransactionHandler(w, r)
		}
	}))
	
	// Address endpoints
	http.HandleFunc("/addresses/", authHandler(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		if strings.HasSuffix(path, "/transactions") {
			getAddressTransactionsHandler(w, r)
		} else if strings.HasSuffix(path, "/utxos") {
			getAddressUTXOsHandler(w, r)
		} else {
			getAddressHandler(w, r)
		}
	}))
	
	// Asset endpoints
	http.HandleFunc("/assets/", authHandler(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		if strings.HasSuffix(path, "/addresses") {
			getAssetAddressesHandler(w, r)
		} else {
			getAssetHandler(w, r)
		}
	}))
	
	// Epoch endpoints
	http.HandleFunc("/epochs/latest", authHandler(getCurrentEpochHandler))
	
	// Legacy endpoints (also protected)
	registerRoute("/nft-owners", "Get address → NFT count for a specific policy ID.", "/nft-owners?policy_id=<your_policy_id>", authHandler(nftOwnersHandler))

	log.Println("🚀 Cardano DB Sync API running at http://localhost:8080")
	log.Println("📖 Visit http://localhost:8080 for API documentation")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
