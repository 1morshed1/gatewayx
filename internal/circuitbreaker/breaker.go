// Package circuitbreaker implements the three-state circuit breaker pattern with
// state shared across gateway replicas in Redis.
//
// The pattern (per upstream host):
//
//	CLOSED    normal. Count failures. Threshold failures within a window -> OPEN.
//	OPEN      fail fast: reject immediately with 503, don't touch the upstream.
//	          After OpenTimeout -> HALF-OPEN.
//	HALF-OPEN let a probe through. Success -> CLOSED. Failure -> OPEN again.
//
// Why it exists: when an upstream is already failing, hammering it with every
// request makes things worse (resource exhaustion, cascading failure) and makes
// clients wait for doomed calls. The breaker "gives up fast" and periodically
// checks whether the upstream has recovered.
//
// Why the state is in Redis (ADR-004): with per-replica breakers, one upstream
// could be OPEN on pod A and CLOSED on pod B, so half the fleet keeps hammering a
// sick backend and clients see inconsistent behaviour. Shared state means the
// whole fleet trips and recovers together.
package circuitbreaker

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// State is a breaker state.
type State string

const (
	StateClosed   State = "closed"
	StateOpen     State = "open"
	StateHalfOpen State = "half_open"
)

// Config tunes a breaker.
type Config struct {
	FailureThreshold int           // failures within Window before opening
	OpenTimeout      time.Duration // time to stay open before allowing a probe
	Window           time.Duration // rolling window over which failures are counted
}

// allowScript decides, atomically, whether a request may proceed and performs the
// OPEN->HALF-OPEN transition when the open timeout has elapsed.
//
// KEYS[1]=state key, KEYS[2]=opened_at key
// ARGV[1]=now(ms), ARGV[2]=openTimeout(ms)
// returns the effective state string.
const allowScript = `
local state = redis.call('GET', KEYS[1])
if state == 'open' then
  local openedAt = tonumber(redis.call('GET', KEYS[2]) or '0')
  if (tonumber(ARGV[1]) - openedAt) >= tonumber(ARGV[2]) then
    redis.call('SET', KEYS[1], 'half_open')
    return 'half_open'
  end
  return 'open'
end
if state == false or state == nil then
  return 'closed'
end
return state
`

// recordScript records the outcome of a request and performs CLOSED<->OPEN
// transitions.
//
// KEYS[1]=state, KEYS[2]=opened_at, KEYS[3]=fails
// ARGV[1]=success(1/0), ARGV[2]=threshold, ARGV[3]=now(ms), ARGV[4]=window(ms)
// returns the resulting state string.
const recordScript = `
if ARGV[1] == '1' then
  redis.call('DEL', KEYS[3])
  redis.call('DEL', KEYS[2])
  redis.call('SET', KEYS[1], 'closed')
  return 'closed'
end
local n = redis.call('INCR', KEYS[3])
redis.call('PEXPIRE', KEYS[3], tonumber(ARGV[4]))
if n >= tonumber(ARGV[2]) then
  redis.call('SET', KEYS[1], 'open')
  redis.call('SET', KEYS[2], ARGV[3])
  return 'open'
end
local cur = redis.call('GET', KEYS[1])
if cur == false or cur == nil then return 'closed' end
return cur
`

// Manager owns the Redis-backed breaker logic and the shared config.
type Manager struct {
	redis  *redis.Client
	cfg    Config
	allow  *redis.Script
	record *redis.Script
}

// NewManager creates a breaker manager.
func NewManager(client *redis.Client, cfg Config) *Manager {
	return &Manager{
		redis:  client,
		cfg:    cfg,
		allow:  redis.NewScript(allowScript),
		record: redis.NewScript(recordScript),
	}
}

// Allow reports whether a request to the given breaker key may proceed, and the
// effective state. A request is denied only when the breaker is fully OPEN.
func (m *Manager) Allow(ctx context.Context, key string) (State, bool, error) {
	stateKey, openedKey, _ := keysFor(key)
	res, err := m.allow.Run(ctx, m.redis,
		[]string{stateKey, openedKey},
		time.Now().UnixMilli(), m.cfg.OpenTimeout.Milliseconds(),
	).Text()
	if err != nil {
		return StateClosed, true, err // fail-open on Redis error
	}
	state := State(res)
	return state, state != StateOpen, nil
}

// Record updates breaker state given whether the request succeeded.
func (m *Manager) Record(ctx context.Context, key string, success bool) (State, error) {
	stateKey, openedKey, failsKey := keysFor(key)
	succ := "0"
	if success {
		succ = "1"
	}
	res, err := m.record.Run(ctx, m.redis,
		[]string{stateKey, openedKey, failsKey},
		succ, m.cfg.FailureThreshold, time.Now().UnixMilli(), m.cfg.Window.Milliseconds(),
	).Text()
	if err != nil {
		return StateClosed, err
	}
	return State(res), nil
}

func keysFor(key string) (state, opened, fails string) {
	return "cb:" + key + ":state", "cb:" + key + ":opened_at", "cb:" + key + ":fails"
}

// StateOf reads the current stored state (for metrics/admin), without mutating it.
func (m *Manager) StateOf(ctx context.Context, key string) (State, error) {
	stateKey, _, _ := keysFor(key)
	res, err := m.redis.Get(ctx, stateKey).Result()
	if err == redis.Nil {
		return StateClosed, nil
	}
	if err != nil {
		return StateClosed, fmt.Errorf("read breaker state: %w", err)
	}
	return State(res), nil
}
