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

// NFTResponse represents an address and the number of NFTs it holds
type NFTResponse struct {
	Address string `json:"address"`
	Count   int64  `json:"count"`
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
    <title>Cardano NFT API</title>
    <style>
        body { font-family: sans-serif; padding: 2em; }
        code { background: #eee; padding: 0.2em 0.4em; border-radius: 4px; }
        li { margin-bottom: 1em; }
    </style>
</head>
<body>
    <h1>Cardano NFT API</h1>
    <p>This server provides endpoints to query Cardano NFT ownership data.</p>

    <h2>Available Endpoints</h2>
    <ul>
        {{range .}}
        <li>
            <strong><code>{{.Path}}</code></strong><br>
            {{.Description}}<br>
            <em>Usage:</em> <code>{{.Usage}}</code>
        </li>
        {{end}}
    </ul>
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

func getCurrentEpoch(w http.ResponseWriter, r *http.Request) {
	query := `SELECT MAX(no) FROM epoch`
	row := db.QueryRowContext(ctx, query)

	var epoch int64
	if err := row.Scan(&epoch); err != nil {
		http.Error(w, "Failed to get current epoch", http.StatusInternalServerError)
		log.Printf("DB error: %v", err)
		return
	}

	response := map[string]int64{"current_epoch": epoch}
	jsonData, err := json.Marshal(response)
	if err != nil {
		http.Error(w, "JSON encode error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(jsonData)
}

func main() {
	registerRoute("/", "Home page listing all available API endpoints.", "/", homeHandler)
	registerRoute("/nft-owners", "Get address → NFT count for a specific policy ID.", "/nft-owners?policy_id=<your_policy_id>", nftOwnersHandler)
	registerRoute("/current-epoch", "Get the current Cardano epoch.", "/current-epoch", getCurrentEpoch)

	log.Println("API running at http://localhost:8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
