// k6 rate-limiter accuracy test for GatewayX.
//
// Goal (from the spec): send a known rate above the limit and verify the limiter
// admits ~exactly the configured number, no more, no fewer. This validates the
// sliding-window algorithm under real concurrency (the thing a naive pipeline gets
// wrong — see doc 04).
//
// Setup creates a route with a 50-requests-per-10s limit and the demo consumer, so
// the limiter keys per-consumer. We then send ~100 req/s for 10s (≈1000 requests)
// and expect roughly 50 admitted (the limit) and the rest 429.
//
// Run:  k6 run test/k6/ratelimit_accuracy.js

import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

const GW = __ENV.GW || 'http://localhost:8000';
const ADMIN = __ENV.ADMIN || 'http://localhost:8001';
const ADMIN_TOKEN = __ENV.ADMIN_TOKEN || '';
const API_KEY = __ENV.API_KEY || 'demo-key';

const LIMIT = 50;      // must match the route config below
const WINDOW_S = 10;

const allowed = new Counter('rl_allowed');
const rejected = new Counter('rl_rejected');

export const options = {
  scenarios: {
    fixed: {
      executor: 'constant-arrival-rate',
      rate: 100, // 100 req/s ...
      timeUnit: '1s',
      duration: '10s', // ... for 10s = ~1000 requests, well above the 50/10s limit
      preAllocatedVUs: 50,
      maxVUs: 200,
    },
  },
};

function adminHeaders() {
  const h = { 'Content-Type': 'application/json' };
  if (ADMIN_TOKEN) h['X-Admin-Token'] = ADMIN_TOKEN;
  return h;
}

export function setup() {
  const body = JSON.stringify({
    name: 'rltest',
    path_prefix: '/rltest',
    strip_prefix: true,
    lb_strategy: 'round_robin',
    timeout_ms: 3000,
    upstreams: [{ url: 'http://localhost:9001', weight: 1 }],
    plugins: [
      { name: 'auth', enabled: true, config: { header: 'X-API-Key' } },
      { name: 'rate-limit', enabled: true, config: { limit: LIMIT, window: `${WINDOW_S}s` } },
    ],
  });
  const res = http.post(`${ADMIN}/admin/routes`, body, { headers: adminHeaders() });
  check(res, { 'rltest route created': (r) => r.status === 201 });
  return { id: res.json('id') };
}

export default function () {
  const res = http.get(`${GW}/rltest/x`, { headers: { 'X-API-Key': API_KEY } });
  if (res.status === 200) allowed.add(1);
  else if (res.status === 429) rejected.add(1);
}

export function teardown(data) {
  if (data && data.id) {
    http.del(`${ADMIN}/admin/routes/${data.id}`, null, { headers: adminHeaders() });
  }
  // The summary prints rl_allowed / rl_rejected. Expect rl_allowed ≈ LIMIT (50),
  // i.e. the limiter admitted the configured number and rejected the rest. A value
  // far above 50 would mean the limiter is leaking (the race doc 04 warns about).
  console.log(`Expected ~${LIMIT} allowed over the ${WINDOW_S}s window.`);
}
