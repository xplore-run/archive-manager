package redis

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
)

type Client struct {
	Client *redis.Client
}

// NewRedisClient creates a new client of Redis database.
func NewRedisClient() (*Client, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client := redis.NewClient(&redis.Options{
		Addr:     os.Getenv("DEFAULT_REDIS_ADDRESS"),
		Password: os.Getenv("DEFAULT_REDIS_PASSWORD"),
		Username: os.Getenv("DEFAULT_REDIS_USERNAME"),
		DB:       0, // use default DB
	})
	// Test the connection to Redis
	if err := client.Ping(ctx).Err(); err != nil {
		fmt.Println("Failed to connect to Redis:", err)
		return nil, err
	}
	fmt.Println("Connected to Redis")
	return &Client{Client: client}, nil
}

// Function to close redis client.
func (rc *Client) Close() {
	err := rc.Client.Close()
	if err != nil {
		fmt.Println("Failed to disconnect from Redis:", err)
		return
	}
	fmt.Println("Disconnected from Redis")
}

// Function to get the default client from the redis client.
func (rc *Client) GetClient() *redis.Client {
	return rc.Client
}
