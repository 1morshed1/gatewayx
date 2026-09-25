// k6 throughput + latency-overhead benchmark for GatewayX.
//
// Goal (from the plan):
//   - Throughput: how many req/s can the gateway proxy?
//   - Latency overhead: p99 the gateway adds on top of the upstream.
//
// This script is self-contained: setup() creates a dedicated `bench` route (no
// auth / no rate limit, so nothing throttles the benchmark), the scenario hammers
// it, and teardown() deletes the route. Point it at a running gateway + upstreams.
//
// Run:  k6 run test/k6/throughput.js
// Env:  GW=http://localhost:8000  ADMIN=http://localhost:8001  ADMIN_TOKEN=...

import http from 'k6/http';
import { check } from 'k6';

const GW = __ENV.GW || 'http://localhost:8000';
const ADMIN = __ENV.ADMIN || 'http://localhost:8001';
const ADMIN_TOKEN = __ENV.ADMIN_TOKEN || '';

export const options = {
  scenarios: {
    // Ramp arriving requests up to a high fixed rate to find sustainable throughput.
    ramp_rps: {
      executor: 'ramping-arrival-rate',
      startRate: 500,
      timeUnit: '1s',
      preAllocatedVUs: 200,
      maxVUs: 2000,
      stages: [
        { target: 2000, duration: '15s' },
        { target: 5000, duration: '15s' },
        { target: 5000, duration: '20s' },
      ],
    },
  },
  thresholds: {
    // Target from the plan: <5ms p99 gateway overhead. Against the local echo
    // upstream, total p99 tracks overhead closely. Loosen if your upstream is slow.
    http_req_duration: ['p(99)<15'],
    http_req_failed: ['rate<0.01'],
  },
};

function adminHeaders() {
  const h = { 'Content-Type': 'application/json' };
  if (ADMIN_TOKEN) h['X-Admin-Token'] = ADMIN_TOKEN;
  return h;
}

export function setup() {
  const body = JSON.stringify({
    name: 'bench',
    path_prefix: '/bench',
    strip_prefix: true,
    lb_strategy: 'round_robin',
    timeout_ms: 3000,
    upstreams: [
      { url: 'http://localhost:9001', weight: 1 },
      { url: 'http://localhost:9002', weight: 1 },
    ],
    plugins: [], // no auth, no rate limit — we're measuring the proxy itself
  });
  const res = http.post(`${ADMIN}/admin/routes`, body, { headers: adminHeaders() });
  check(res, { 'bench route created': (r) => r.status === 201 });
  return { id: res.json('id') };
}

export default function () {
  const res = http.get(`${GW}/bench/ping`);
  check(res, { 'status 200': (r) => r.status === 200 });
}

export function teardown(data) {
  if (data && data.id) {
    http.del(`${ADMIN}/admin/routes/${data.id}`, null, { headers: adminHeaders() });
  }
}
