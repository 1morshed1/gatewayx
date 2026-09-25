## Project 7: API Gateway & Rate Limiter with Plugin Architecture (Mini-Kong)

### Concept & Why It Matters

You are building a programmable API gateway that sits in front of multiple backend services, handles authentication, rate limiting, request transformation, circuit breaking, and load balancing. It supports a plugin system where you can write custom middleware in Go and load it at runtime.

This is a different category from the other projects. Instead of building an application, you are building infrastructure — a piece of software that other engineers use. API gateways are one of the most critical components in any microservices architecture (every request flows through them), and building one from scratch demonstrates an understanding of networking, concurrency, and reliability that application-level projects do not.

### Architecture Diagram

```
  External Clients                                                            
       │                                                                      
       │  HTTPS                                                               
       ▼                                                                      
┌──────────────────────────────────────────────────────────────────────────────┐
│                        KUBERNETES CLUSTER                                    │
│                                                                              │
│  ┌──────────────────────────────────────────────────────────────────────┐    │
│  │  Go API Gateway                                                      │   │
│  │                                                                      │   │
│  │  ┌──────────────────────────────────────────────────────────────┐    │   │
│  │  │  Plugin Chain (executed in order per request)                 │    │   │
│  │  │                                                              │    │   │
│  │  │  ┌─────────┐  ┌──────────┐  ┌───────────┐  ┌─────────────┐ │    │   │
│  │  │  │  Auth   │─▶│  Rate    │─▶│  Request  │─▶│  Circuit    │ │    │   │
│  │  │  │  Plugin │  │  Limiter │  │  Transform│  │  Breaker    │ │    │   │
│  │  │  │         │  │  Plugin  │  │  Plugin   │  │  Plugin     │ │    │   │
│  │  │  └─────────┘  └──────────┘  └───────────┘  └─────────────┘ │    │   │
│  │  └──────────────────────────────────────┬───────────────────────┘    │   │
│  │                                         │                            │   │
│  │                                         │  Reverse proxy             │   │
│  │                                         ▼                            │   │
│  │  ┌──────────────────────────────────────────────────────────────┐    │   │
│  │  │  Load Balancer                                               │    │   │
│  │  │  (weighted round-robin, least-connections, consistent hash)  │    │   │
│  │  └──────────────────────────────────────────────────────────────┘    │   │
│  └──────────────────────────────────────────────────────────────────────┘    │
│             │                          │                         │           │
│             ▼                          ▼                         ▼           │
│  ┌──────────────┐           ┌──────────────┐         ┌──────────────┐       │
│  │  Service A   │           │  Service B   │         │  Service C   │       │
│  │  (Go)        │           │  (Python)    │         │  (Node)      │       │
│  └──────────────┘           └──────────────┘         └──────────────┘       │
│                                                                              │
│  ┌──────────────────────────────────────────────────┐                        │
│  │  Redis                                           │                       │
│  │  - Rate limit counters (sliding window)          │                       │
│  │  - Circuit breaker state                         │                       │
│  │  - Session/token cache                           │                       │
│  └──────────────────────────────────────────────────┘                        │
│                                                                              │
│  ┌──────────────────────────────────────────────────┐                        │
│  │  PostgreSQL                                      │                       │
│  │  - Route configuration                           │                       │
│  │  - API key registry                              │                       │
│  │  - Plugin configuration                          │                       │
│  │  - Request analytics (aggregated)                │                       │
│  └──────────────────────────────────────────────────┘                        │
│                                                                              │
│  ┌──────────────────────────────────────────────────┐                        │
│  │  Admin API                                       │                       │
│  │  POST /admin/routes                              │                       │
│  │  POST /admin/plugins                             │                       │
│  │  GET  /admin/metrics                             │                       │
│  └──────────────────────────────────────────────────┘                        │
│                                                                              │
│  Kafka: request-logs topic (every proxied request, for analytics)            │
└──────────────────────────────────────────────────────────────────────────────┘
```

### Tech Stack Breakdown

| Layer | Technology | Why This Choice |
|---|---|---|
| Gateway Core | Go + `net/http/httputil.ReverseProxy` | Go's standard library reverse proxy is production-grade. Combined with custom `http.Handler` middleware, it gives you full control over every request and response. |
| Rate Limiting | Redis (sliding window log algorithm) | Redis's atomic operations (`ZADD`, `ZREMRANGEBYSCORE`, `ZCARD`) make it ideal for distributed rate limiting. The sliding window log algorithm is more accurate than fixed-window or token bucket for bursty traffic. |
| Circuit Breaking | Redis (shared state) or in-memory | Circuit breaker state (closed/open/half-open) must be shared across all gateway instances. Redis provides this. Alternatively, use in-memory per-instance circuit breakers if you accept slightly inconsistent behavior across pods. |
| Config Store | PostgreSQL | Route definitions, plugin configurations, and API keys are stored in Postgres. The gateway watches for changes (via PostgreSQL LISTEN/NOTIFY or periodic polling) and hot-reloads its configuration without restart. |
| Analytics | Apache Kafka | Every proxied request is logged to a Kafka topic for downstream analytics (Spark batch job computing daily API usage reports, top consumers, error rates by route). |
| Load Balancing | Custom (Go) | Implement three algorithms: weighted round-robin (simple, works for most cases), least-connections (better for long-running requests), and consistent hashing (for sticky sessions or cache-friendly routing). |

### Key Implementation Details

#### The Plugin System

This is the most architecturally interesting part. Each plugin implements a simple interface:

```go
type Plugin interface {
    Name() string
    Priority() int  // lower = executes first
    
    // Called before the request is proxied to the upstream
    OnRequest(ctx *RequestContext) error
    
    // Called after the response is received from the upstream
    OnResponse(ctx *ResponseContext) error
}

type RequestContext struct {
    Request     *http.Request
    Route       *Route
    Consumer    *Consumer    // the authenticated API consumer
    Metadata    map[string]interface{}  // shared state between plugins
    Abort       func(statusCode int, body interface{})
}
```

The gateway builds a plugin chain for each route based on its configuration:

```go
func (g *Gateway) buildChain(route *Route) http.Handler {
    plugins := g.pluginRegistry.GetPlugins(route.PluginConfig)
    sort.Slice(plugins, func(i, j int) bool {
        return plugins[i].Priority() < plugins[j].Priority()
    })

    // Build the chain from inside out
    handler := g.reverseProxy(route)
    for i := len(plugins) - 1; i >= 0; i-- {
        plugin := plugins[i]
        next := handler
        handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
            ctx := &RequestContext{Request: r, Route: route}
            if err := plugin.OnRequest(ctx); err != nil {
                http.Error(w, err.Error(), http.StatusInternalServerError)
                return
            }
            next.ServeHTTP(w, r)
        })
    }
    return handler
}
```

#### Sliding Window Rate Limiter (Redis)

The sliding window log algorithm is more accurate than fixed-window counters. It prevents the "double burst" problem where a burst at the end of one window and the start of the next allows 2x the intended rate:

```go
func (rl *RateLimiter) Allow(key string, limit int, windowSeconds int) (bool, error) {
    now := time.Now().UnixMicro()
    windowStart := now - int64(windowSeconds)*1_000_000

    pipe := rl.redis.Pipeline()
    
    // Remove entries outside the window
    pipe.ZRemRangeByScore(ctx, key, "-inf", fmt.Sprintf("%d", windowStart))
    
    // Count entries in the window
    countCmd := pipe.ZCard(ctx, key)
    
    // Add current request
    pipe.ZAdd(ctx, key, redis.Z{Score: float64(now), Member: fmt.Sprintf("%d", now)})
    
    // Set expiry on the key
    pipe.Expire(ctx, key, time.Duration(windowSeconds)*time.Second)
    
    _, err := pipe.Exec(ctx)
    if err != nil {
        return false, err
    }

    count := countCmd.Val()
    if count >= int64(limit) {
        return false, nil
    }
    return true, nil
}
```

The rate limit key is constructed from the consumer ID and the route: `ratelimit:{consumer_id}:{route_id}`. This allows per-consumer, per-route rate limits.

#### Circuit Breaker

Implement the three-state circuit breaker pattern:

- **Closed** (normal): Requests pass through. Track failures. If failures exceed a threshold (e.g., 5 failures in 30 seconds), transition to Open.
- **Open** (failing): All requests are immediately rejected with 503. After a timeout (e.g., 30 seconds), transition to Half-Open.
- **Half-Open** (testing): Allow one request through. If it succeeds, transition to Closed. If it fails, transition back to Open.

```go
type CircuitBreaker struct {
    state          string  // closed, open, half_open
    failureCount   int
    failureThreshold int
    lastFailureTime time.Time
    openTimeout    time.Duration
    redis          *redis.Client
    key            string  // per-upstream-service
}

func (cb *CircuitBreaker) Execute(fn func() (*http.Response, error)) (*http.Response, error) {
    state := cb.getState()
    
    switch state {
    case "open":
        if time.Since(cb.lastFailureTime) > cb.openTimeout {
            cb.setState("half_open")
            // fall through to execute
        } else {
            return nil, ErrCircuitOpen
        }
    case "half_open":
        // Allow one request
    case "closed":
        // Normal operation
    }
    
    resp, err := fn()
    
    if err != nil || resp.StatusCode >= 500 {
        cb.recordFailure()
        if cb.failureCount >= cb.failureThreshold {
            cb.setState("open")
        }
        return resp, err
    }
    
    cb.reset()  // success resets the counter
    return resp, nil
}
```

#### Hot Configuration Reload

The gateway loads its route table and plugin configuration from PostgreSQL at startup. When an admin updates a route via the Admin API, the gateway needs to pick up the change without restarting:

```go
func (g *Gateway) watchConfigChanges(ctx context.Context) {
    // Option 1: PostgreSQL LISTEN/NOTIFY
    conn := g.db.Acquire(ctx)
    conn.Exec(ctx, "LISTEN route_changes")
    
    for {
        notification, err := conn.WaitForNotification(ctx)
        if err != nil {
            continue
        }
        
        // Reload the affected route
        routeID := notification.Payload
        route := g.configStore.GetRoute(ctx, routeID)
        g.routeTable.Update(route)
        g.rebuildChain(route)
        
        log.Info("hot-reloaded route", "route_id", routeID)
    }
}
```

### Load Testing

Use `k6` to benchmark the gateway:
- **Throughput:** How many requests per second can the gateway proxy with all plugins enabled? Target: 20,000+ req/sec on a single pod.
- **Latency overhead:** What does the gateway add on top of the upstream's response time? Target: under 5ms p99 overhead.
- **Rate limiter accuracy:** Send exactly 100 req/sec when the limit is 50 req/sec. Verify that exactly 50% are rejected (not more, not fewer). This validates the sliding window algorithm.
- **Circuit breaker responsiveness:** Take down an upstream service. Measure how quickly the circuit opens and how quickly it recovers.

---
---