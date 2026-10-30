// k6 load test for POST /v1/events.
//
// The arrival rate is fixed (constant-arrival-rate executor): k6 starts RATE requests per second
// regardless of how fast the API answers. A closed model ("N users in a loop") would slow down when
// the server slows down and hide the latency that real clients see (coordinated omission).
//
// Configuration (environment variables):
//   TOKENS      required: comma-separated bearer tokens, one per tenant. run.sh creates them for load-0 ... load-N-1
//               with scripts/DevJwt.java (valid 60 minutes); the tenant is the token's tenant_id claim.
//   BASE_URL    default http://host.docker.internal:8080  (k6 runs in Docker, the API on the host)
//   RATE        requests per second                       default 50
//   BATCH       events per request (1..500)               default 20
//   DURATION    e.g. 60s, 5m                              default 60s
//   VUS         pre-allocated virtual users               default 50
//
// Events per second offered = RATE x BATCH. See load-tests/README.md for how to run it.
import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

const BASE_URL = __ENV.BASE_URL || 'http://host.docker.internal:8080';
const RATE = Number(__ENV.RATE || 50);
const BATCH = Number(__ENV.BATCH || 20);
const DURATION = __ENV.DURATION || '60s';
const VUS = Number(__ENV.VUS || 50);
const TOKENS = (__ENV.TOKENS || '').split(',').filter((token) => token);
if (TOKENS.length === 0) {
  throw new Error('TOKENS is not set; see load-tests/README.md');
}

const eventsAccepted = new Counter('events_accepted');
const eventsRejected = new Counter('events_rejected');

export const options = {
  scenarios: {
    ingest: {
      executor: 'constant-arrival-rate',
      rate: RATE,
      timeUnit: '1s',
      duration: DURATION,
      preAllocatedVUs: VUS,
      maxVUs: VUS * 4,
    },
  },
  // Percentiles shown in the end-of-test summary and written by --summary-export.
  summaryTrendStats: ['avg', 'min', 'med', 'p(95)', 'p(99)', 'max'],
  thresholds: {
    // A baseline is a measurement, not a pass/fail gate; these only flag runs that are clearly broken.
    http_req_failed: ['rate<0.01'],
    dropped_iterations: ['count<1'],
  },
};

const TYPES = ['page.view', 'button.click', 'order.created', 'order.paid'];

// UUIDv7: 48-bit Unix milliseconds followed by random bits (RFC 9562). Time-ordered IDs keep
// B-tree inserts append-mostly, which matters once Stage 3 stores event IDs for deduplication.
function uuidv7() {
  const bytes = new Uint8Array(16);
  crypto.getRandomValues(bytes);
  let millis = Date.now();
  for (let i = 5; i >= 0; i--) {
    bytes[i] = millis % 256;
    millis = Math.floor(millis / 256);
  }
  bytes[6] = (bytes[6] & 0x0f) | 0x70; // version 7
  bytes[8] = (bytes[8] & 0x3f) | 0x80; // RFC 9562 variant
  const hex = Array.from(bytes, (b) => b.toString(16).padStart(2, '0')).join('');
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

function batch(now) {
  const events = [];
  for (let i = 0; i < BATCH; i++) {
    events.push({
      eventId: uuidv7(),
      type: TYPES[Math.floor(Math.random() * TYPES.length)],
      occurredAt: now,
      value: Math.round(Math.random() * 10000) / 100,
    });
  }
  return JSON.stringify({ events });
}

export default function () {
  const token = TOKENS[Math.floor(Math.random() * TOKENS.length)];
  const response = http.post(`${BASE_URL}/v1/events`, batch(new Date().toISOString()), {
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
    // Every response with another status counts as a failure in http_req_failed.
    responseCallback: http.expectedStatuses(202),
  });

  if (check(response, { 'status is 202': (r) => r.status === 202 })) {
    eventsAccepted.add(BATCH);
  } else {
    eventsRejected.add(BATCH);
  }
}
