// k6 traffic for the resilience scenarios (scripts/resilience/run.sh). Unlike ingest.js, every event is
// deterministic and every batch is logged, so the run can be checked against PostgreSQL afterwards.
//
// One JSON line per batch goes to the console log; run.sh writes it to manifest.jsonl with
//   k6 run --log-format raw --log-output file=/out/manifest.jsonl
// {"batch":"0-17","phase":0,"tenant":"r1a2b3c4d-5","occurredAt":"...","statuses":[202],"outcome":"accepted",
//  "events":[{"eventId":"...","type":"page.view","value":18}, ...]}
//
// Configuration (environment variables):
//   RUN_HEX   required: 8 hex digits, unique per run; the first group of every eventId
//   TENANTS   required: comma-separated tenant ids; TOKENS holds their bearer tokens in the same order
//   TOKENS    required
//   PHASES    required: consecutive load phases "RATExBATCHxSECONDS,..." (requests/s x events per request x seconds),
//             e.g. 50x50x15,4x50x20
//   HOT_EVERY every n-th batch goes to the last tenant in TENANTS, to provoke 429s (0 = off)   default 0
//   RETRIES   extra attempts for an ambiguous response (5xx other than "Server busy", or no response), with the
//             same body and eventIds                                                          default 0
//   BASE_URL  default http://host.docker.internal:8080
//   VUS       pre-allocated virtual users per phase                                            default 50
//
// Outcome of a batch: "accepted" if any attempt returned 202; otherwise "ambiguous" if any attempt may have
// reached Kafka (5xx, timeout, connection error); otherwise "rejected" (4xx including 429, and 503 "Server busy",
// which the API sends before reading the body).
import http from 'k6/http';
import exec from 'k6/execution';
import { sleep } from 'k6';

const BASE_URL = __ENV.BASE_URL || 'http://host.docker.internal:8080';
const RUN_HEX = __ENV.RUN_HEX || '';
const TENANTS = (__ENV.TENANTS || '').split(',').filter((t) => t);
const TOKENS = (__ENV.TOKENS || '').split(',').filter((t) => t);
const HOT_EVERY = Number(__ENV.HOT_EVERY || 0);
const RETRIES = Number(__ENV.RETRIES || 0);
const VUS = Number(__ENV.VUS || 50);
const PHASES = (__ENV.PHASES || '').split(',').filter((p) => p).map((p) => {
  const [rate, batch, seconds] = p.split('x').map(Number);
  return { rate, batch, seconds };
});
if (!/^[0-9a-f]{8}$/.test(RUN_HEX) || TENANTS.length === 0 || TENANTS.length !== TOKENS.length
    || PHASES.length === 0 || PHASES.some((p) => !(p.rate > 0 && p.batch >= 1 && p.batch <= 500 && p.seconds > 0))) {
  throw new Error('Set RUN_HEX, TENANTS, TOKENS (same length) and PHASES; see the header of this file');
}
const REGULAR = HOT_EVERY > 0 ? TENANTS.length - 1 : TENANTS.length;

const scenarios = {};
let start = 0;
PHASES.forEach((p, index) => {
  scenarios[`phase${index}`] = {
    executor: 'constant-arrival-rate',
    rate: p.rate,
    timeUnit: '1s',
    duration: `${p.seconds}s`,
    startTime: `${start}s`,
    preAllocatedVUs: VUS,
    maxVUs: VUS * 4,
    env: { PHASE: String(index), BATCH: String(p.batch) },
  };
  start += p.seconds;
});

export const options = { scenarios };

const TYPES = ['page.view', 'button.click', 'order.created', 'order.paid'];

function hex(n, width) {
  return n.toString(16).padStart(width, '0');
}

// A valid UUID (version nibble 7, RFC variant 8) that encodes run, phase, iteration and position, so the same
// event always gets the same id and ids never collide within or across runs.
function eventId(phase, iteration, i) {
  return `${RUN_HEX}-${hex(phase, 4)}-7${hex(i, 3)}-8000-${hex(iteration, 12)}`;
}

function outcomeOf(response) {
  if (response.status === 202) return 'accepted';
  if (response.status === 503 && response.body && response.body.includes('"Server busy"')) return 'rejected';
  if (response.status === 0 || response.status >= 500) return 'ambiguous';
  return 'rejected';
}

export default function () {
  const phase = Number(__ENV.PHASE);
  const size = Number(__ENV.BATCH);
  const iteration = exec.scenario.iterationInTest;
  const tenantIndex = HOT_EVERY > 0 && iteration % HOT_EVERY === 0 ? TENANTS.length - 1 : iteration % REGULAR;
  const occurredAt = new Date().toISOString();

  const events = [];
  for (let i = 0; i < size; i++) {
    events.push({
      eventId: eventId(phase, iteration, i),
      type: TYPES[(iteration + i) % TYPES.length],
      occurredAt,
      value: ((iteration * 7 + i) % 100) + 1,
    });
  }
  const body = JSON.stringify({ events });
  const params = {
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${TOKENS[tenantIndex]}` },
    timeout: '10s',
    responseCallback: http.expectedStatuses(202),
  };

  const statuses = [];
  const outcomes = [];
  for (let attempt = 0; attempt <= RETRIES; attempt++) {
    if (attempt > 0) sleep(1);
    const response = http.post(`${BASE_URL}/v1/events`, body, params);
    statuses.push(response.status);
    outcomes.push(outcomeOf(response));
    if (outcomes[attempt] !== 'ambiguous') break;
  }
  const outcome = outcomes.includes('accepted') ? 'accepted' : outcomes.includes('ambiguous') ? 'ambiguous' : 'rejected';

  console.log(JSON.stringify({
    batch: `${phase}-${iteration}`,
    phase,
    tenant: TENANTS[tenantIndex],
    occurredAt,
    statuses,
    outcome,
    events: events.map((e) => ({ eventId: e.eventId, type: e.type, value: e.value })),
  }));
}
