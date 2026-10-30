# ADR-0012: Pooled multi-tenancy, isolated by application predicates

- Status: Accepted (2026-10-27)
- Stage: 1 (implemented). The source of tenant identity changes in Stage 2 ([ADR-0013](0013-jwt-without-identity-provider.md)).
- Assessment: **Retrospective.** This decision was implicit and never written down. The proposal did not discuss
  tenancy models.

## Problem

- Many tenants share the platform. One tenant must never read another tenant's aggregates.
- A noisy tenant must not starve the others. That is covered by rate limits ([ADR-0004](0004-rate-limit-store-outage.md))
  and the partition key ([ADR-0003](0003-partition-key.md)).
- Tenants should be added without provisioning work: no new topic, schema or database per tenant.
- Some tenants may later need their data deleted on request.

## Decision

- **Pool model at every layer:**
  - one topic `events.v1` for all tenants, with key `tenantId`;
  - one consumer group;
  - one table `event_stats_minute` with `tenant_id` as the first primary-key column.
- Isolation is enforced **only by the application**. Every query has `WHERE tenant_id = :tenantId`, and the tenant
  comes from the caller's identity.
  - Stage 1: an unauthenticated `X-Tenant-Id` header, validated against `^[a-z0-9][a-z0-9-]{1,62}$`.
  - Stage 2: a JWT claim.
- No PostgreSQL row-level security (RLS). One database role owns the tables and runs all queries.

## Alternatives considered

1. **Pool plus PostgreSQL RLS.** A policy `USING (tenant_id = current_setting('app.tenant_id'))` gives a second line
   of defence on the read path.
2. **Bridge model.** One PostgreSQL schema per tenant (or per tier) in a shared database.
3. **Silo model.** A database, and possibly a topic, per tenant or per premium tier.

## Comparison

| Criterion                       | **Pool, application predicates**                        | Pool + RLS                                                                                               | Bridge (schema per tenant)                    | Silo (database/topic per tenant)       |
| ------------------------------- | ------------------------------------------------------- | -------------------------------------------------------------------------------------------------------- | --------------------------------------------- | -------------------------------------- |
| Effect of one missing predicate | **Cross-tenant data leak**                              | Rows filtered by the database                                                                            | Wrong `search_path` leaks a whole schema      | Mistakes stay within one tenant        |
| Onboarding                      | Nothing to provision                                    | Nothing to provision                                                                                     | Create a schema and run migrations per tenant | Provision a database and a topic       |
| Migrations                      | Once                                                    | Once                                                                                                     | N times                                       | N times                                |
| Erasure of one tenant           | `DELETE` aggregates; Kafka keeps events until retention | Same                                                                                                     | `DROP SCHEMA`; Kafka same                     | Drop the database and delete the topic |
| Cost and complexity             | Lowest                                                  | Every connection must set the tenant per transaction; the owner role bypasses RLS unless `FORCE` is used | Many schemas; catalog bloat at scale          | Highest                                |

## Rationale

- Aggregates are small, tenants are expected to be many, and onboarding must be free. Pool is the standard choice for
  this profile (AWS SaaS Lens).
- Every access path already starts with `tenant_id` (primary-key prefix), so the predicate is also needed for
  performance and is hard to drop by accident.
- **RLS would mostly protect the read path.** The consumer legitimately writes rows for all tenants, so it would need
  a role that bypasses RLS anyway.
  - Today the application role owns the tables. Table owners bypass RLS unless `FORCE ROW LEVEL SECURITY` is set.
  - The tenant would have to be set per transaction (`set_config(..., true)`), but the Stage 1 statements run in
    auto-commit mode.
  - These are real costs, and they are why RLS was not added. **The decision is still questionable:** the read path
    is exactly where RLS is cheap.

## Consequences

- **There is a single line of defence.** A new query without the tenant predicate leaks data. Code review and tests
  are the only protection.
- **In Stage 1, tenant identity is not authenticated.** Any caller can read any tenant's stats by changing a header.
  This is acceptable only for the local skeleton; Stage 2 replaces it.
- **Erasure is bounded by Kafka retention.** Deleting a tenant's aggregates does not remove its events from
  `events.v1`, where they stay up to 8 days (retention plus segment time). A replay would also recreate the
  aggregates.
- Noisy-neighbour effects in Kafka and PostgreSQL are shared. See ADR-0003 and ADR-0004.

## Reconsider when

- Stage 2 adds authentication. That is the right time to add RLS on the stats read path, or at least a test that
  fails when a query lacks the tenant predicate (Q5 in
  [technology-selection.md](../architecture/technology-selection.md#6-open-questions-and-recommendations)).
- A tenant needs contractual isolation, its own encryption keys or data residency. Then use the bridge or silo model
  for that tier.
- Erasure must be faster than topic retention. Then use shorter retention, per-tenant encryption keys (crypto-shredding)
  or a silo topic.

## References

- AWS SaaS Lens, silo, pool and bridge models:
  https://docs.aws.amazon.com/wellarchitected/latest/saas-lens/silo-pool-and-bridge-models.html
- PostgreSQL row security policies (owner bypass, `FORCE ROW LEVEL SECURITY`):
  https://www.postgresql.org/docs/17/ddl-rowsecurity.html
- PostgreSQL schemas: https://www.postgresql.org/docs/17/ddl-schemas.html
- OWASP Top 10, A01 Broken Access Control: https://owasp.org/Top10/A01_2021-Broken_Access_Control/
