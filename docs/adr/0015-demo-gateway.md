# ADR-0015: Interactive demo through a server-side gateway with temporary tenants

- Status: Accepted (2026-10-28); implemented in Stage 6, local only.
- Stage: 6
- Assessment: **Design-time.** The alternatives below were compared before the gateway was written.

## Problem

- Visitors should see the platform's guarantees for themselves: ingestion under load, deduplication, tenant isolation,
  rate limits and quotas. Within 2–3 minutes, without tools, from a browser.
- The platform authenticates with RS256 JWTs signed by a private key ([ADR-0013](0013-jwt-without-identity-provider.md)).
  The private key and the tokens must never reach a browser: anyone holding them could write as any tenant.
- A public demo invites abuse. Load must be bounded per visitor and system-wide, and a visitor must not be able to read
  or exhaust another visitor's tenant.
- Results must be honest. A 202 means "stored in Kafka", not "counted" ([ADR-0002](0002-acceptance-semantics.md)); the
  demo may call something processed only when the statistics API reports it.
- The core services must not change for the demo. Their validation stays as strict as before.

## Decision

- A separate deployable, **`demo-gateway`** (Spring Boot, port 8082), serves a small static UI (`demo-ui`, React +
  Vite, no UI or chart libraries) and a JSON API under `/demo`. It calls the platform's public API exactly as a tenant
  application would: `POST /v1/events` and `GET /v1/stats` with a JWT.
- **Sessions.** `POST /demo/session` creates a session with a 256-bit random id in an `HttpOnly`, `SameSite=Strict`
  cookie (`Path=/demo`, `Secure` behind HTTPS) and two fresh tenants, `demo-<16 hex>-a` and `demo-<16 hex>-b`. Sessions
  live in memory for 15 minutes and are removed on expiry (checked on access and every 10 s). A reload finds the
  session again by its cookie; an expired or unknown session is refused with 401.
- **Tokens stay in the gateway.** The gateway signs a short-lived token (5 minutes) per call for the session's own
  tenant; the browser never sees a token or a key, only decoded claims. Signing configuration (`demo.token.*`) is
  separate from the platform's verification settings.
- **Plans by pattern.** `ingestion-api` gains `rate-limit.tenant-patterns`: a list of `{pattern, plan}` matched in full
  after the exact `rate-limit.tenants` entries and before `default-plan`. `demo-[0-9a-f]{16}-a` maps to `demo-standard`
  (1,000/s, burst 2,000, 60,000 per day), `-b` to `demo-small` (100/s, burst 500, 1,000 per day). Existing mappings and
  the default are unchanged.
- **Server-side limits, checked atomically:**

  | Limit                                     | Value                        | Enforcement                                       |
  | ----------------------------------------- | ---------------------------- | ------------------------------------------------- |
  | Active sessions                           | 20 (5 per client address)    | synchronized create; 503 / 429 with `Retry-After` |
  | Concurrent load runs, system-wide         | 1                            | compare-and-set slot held until drained; 409      |
  | Rate / duration / events per run          | 1,000 events/s, 30 s, 20,000 | request validation, 400                           |
  | Events per session (all scenarios)        | 50,000                       | compare-and-set reservation, 422; unsent refunded |
  | One scenario of a kind at a time per user | –                            | per-session busy set, 409                         |
  | Session lifetime                          | 15 minutes                   | expiry check on every access                      |

- **Honest states.** A run reports submitted, accepted by Kafka (202), aggregated in PostgreSQL (from `/v1/stats`) and
  still pending; it is _completed_ only when the aggregated count reaches the accepted count. Deduplication is verified
  by replaying the same event ids, then sending a marker event and waiting until the marker is counted (same tenant,
  same partition, so the replay was processed first) before comparing counts. Isolation checks send real requests
  with a valid, an altered and a foreign-signed token. Limits come from real 429 answers; an exhausted quota is never
  reset by the demo.
- **Pacing.** The load generator samples progress on its own thread and catches up at most 0.5 s of schedule after a
  stall, so a run at the plan's maximum rate stays within the 2,000-event burst. Before this, a stall of about 2 s
  made it send the whole shortfall at once, and the platform correctly answered 429 (about 400 of 10,000 events at
  1,000/s). The platform's limiter is unchanged; refused batches are counted as rejected and not retried.
- **Atomic admission.** A batch is admitted whole or not at all. A batch larger than the remaining daily quota is
  refused as _quota_ even while quota remains; the API says so ("The batch exceeds this tenant's remaining daily
  event quota"), and the UI shows the remaining quota and distinguishes this case from an exhausted quota.
- **Hardening.** Strict CSP (`'self'` only, no inline script or style), `X-Frame-Options: DENY`, `no-referrer`,
  `no-store` for `/demo`, and every state-changing request must carry `X-Demo-Client: 1` (a custom header a
  cross-site form cannot send). System metrics come from fixed PromQL queries; the browser cannot send a query.

### Signing keys in a public deployment (Stage 7)

Locally the gateway signs with the development key pair from `java scripts/DevJwt.java keys`. A public deployment
uses a **dedicated demo issuer and key pair**: the gateway alone holds the demo private key, and the platform is
configured to trust that issuer's public key with the same checks as today (signature, issuer, audience, expiry,
`tenant_id`). The development key pair is never deployed. Validation is not weakened for the demo.

## Alternatives considered

1. **Tokens in the browser.** The UI calls the platform directly with a token from a token endpoint. Simplest, but a
   visitor could copy the token, script unlimited traffic until it expires and read any response; the limits above
   could not be enforced per session.
2. **Demo endpoints inside `ingestion-api`.** No new deployable, but session state, load generation and demo-only
   routes would live on the hot path of the core service and in its security configuration.
3. **Load generated in the browser.** Shows client behaviour, but rates depend on the visitor's network, and it
   needs a token in the browser (1).
4. **A shared, pre-provisioned demo tenant.** No pattern mapping needed, but visitors would see and exhaust each
   other's data and quota, so isolation and quota could not be shown honestly.
5. **Persistent sessions (Valkey or PostgreSQL).** Survive a gateway restart and allow several gateway instances.
   Not needed for one small instance with 15-minute sessions.

## Rationale

- The gateway is a tenant application with a UI. The platform is exercised through its public contract, so what the
  demo shows is what any client gets.
- Keeping tokens and keys server-side is the only way to bound what a visitor can do; the budget and the run slot
  are then simple in-process checks.
- Pattern mapping is a small, general configuration feature, tested on its own. It keeps the demo's tenants
  unlimited in number without touching plan semantics.

## Consequences

- A third deployable, with an in-memory session store: a gateway restart ends all sessions, and it cannot be scaled
  beyond one instance without moving sessions to a shared store.
- One load run at a time, system-wide. A visitor may wait up to about 2.5 minutes (30 s of sending plus the drain
  timeout).
- The per-client cap uses the TCP peer address. Behind a load balancer it must come from a trusted forwarded header
  (Stage 7).
- Platform-wide metrics are shown as such: the platform has no per-tenant metrics, so session figures come from the
  statistics API and the overview is labelled "Whole system".
- If a run ends incomplete (drain timeout), its late events are aggregated afterwards and can raise the processed
  count of the next run of the same event type in that session. The count is clamped to the accepted count; the
  incomplete state is shown.
- A session open across 00:00 UTC sees tenant B's quota refilled by the platform, as for any tenant.

## When to reconsider

- More than one gateway instance, or sessions that must survive a restart: move sessions to Valkey.
- If the demo key pair could be exposed: restrict the demo issuer to `demo-*` tenants in the platform.
- If the demo should show per-tenant throughput: add tenant-tagged metrics (with bounded cardinality) to the platform.
