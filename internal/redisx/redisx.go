// Package redisx centralises Redis client construction so every component (rate
// limiter, circuit breaker) shares one connection pool and one place to tune it.
package redisx

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// New builds and pings a Redis client. It retries the ping briefly so the gateway
// can start right after `make infra-up` while Redis is still coming up.
func New(ctx context.Context, addr, password string, db int) (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr:         addr,
		Password:     password,
		DB:           db,
		PoolSize:     100, // gateways are high-concurrency; size the pool generously
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
	})

	var pingErr error
	for i := 0; i < 10; i++ {
		if pingErr = client.Ping(ctx).Err(); pingErr == nil {
			return client, nil
		}
		select {
		case <-ctx.Done():
			_ = client.Close()
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
	}
	_ = client.Close()
	return nil, fmt.Errorf("ping redis: %w", pingErr)
}
