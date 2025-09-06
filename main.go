package main

import (
	"context"
	"database/sql"
	"log"
	"os"

	"github.com/go-redis/redis/v8"
	"github.com/joho/godotenv"

	_ "github.com/lib/pq"
)

var (
	ctx         = context.Background()
	db          *sql.DB
	redisClient *redis.Client

)

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





