# ADR-0006: Stats endpoint lives in the event processor

- Status: Accepted (2026-10-27). Comparison and references added in the documentation review (2026-10-27).
- Stage: 1 (implemented)
- Assessment: **Design-time.** The architecture review weighed the three placements below, and its approval asked
  for the resource-sharing trade-off to be documented. The read-replica row is a retrospective addition.

## Context

`GET /v1/stats` reads the aggregates. Options: serve it from `event-processor` (owner of the schema), from
`ingestion-api` (would add a database dependency to the hot path), or from a separate query service.

## Decision

Serve it from `event-processor`. One service owns and reads the schema; `ingestion-api` stays database-free so a
PostgreSQL outage cannot stop ingestion.

## Alternatives compared

| Placement                               | Database clients       | Effect of a PostgreSQL outage on ingestion                                       | Isolation of reads from consumption | Cost                                                                            |
| --------------------------------------- | ---------------------- | -------------------------------------------------------------------------------- | ----------------------------------- | ------------------------------------------------------------------------------- |
| **`event-processor` (chosen)**          | 1 service              | None                                                                             | None: shared JVM and pool           | Resource sharing, below                                                         |
| `ingestion-api`                         | 2 services             | None for `POST`, but the API now carries a pool, driver and migrations knowledge | Separate from consumption           | Ingestion instances scale for writes, so read capacity follows the wrong signal |
| Separate query service (CQRS read side) | 2 services, one schema | None                                                                             | Full                                | A third deployable, and two services coupled to one schema                      |
| Read replica (with any placement)       | Same as placement      | None                                                                             | Database load only, not the JVM     | Replication lag in results; another database to operate                         |

A separate query service is the textbook answer, but it buys isolation that Stage 1 has no evidence of needing, and
it shares the schema with the processor anyway. The split below is kept cheap instead.

## Trade-off: shared resources

The endpoint shares one JVM with the Kafka consumers:

| Shared resource        | Risk                                                                                                                              |
| ---------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| CPU and heap           | Expensive queries or large responses slow consumption; GC pauses affect both                                                      |
| HikariCP pool          | Long or concurrent stats queries hold connections the consumers need → consumers wait (`hikaricp.connections.pending`), lag grows |
| Deployment and scaling | Scaling consumers scales the API and vice versa; a stats-only fix redeploys the consumer                                          |
| Failure                | A consumer stuck in DB retry still serves stats (fine), but a crash loses both                                                    |

Mitigations in Stage 1:

- Query range limited to 24 h and a bucket-count cap; queries hit the primary-key prefix `(tenant_id, …)`.
- JDBC query timeout (`spring.jdbc.template.query-timeout`) so a slow query releases its connection.
- Pool size ≥ consumer concurrency + headroom; Hikari metrics expose pending threads and acquire time.

## When to split

Move stats to a separate deployable (or a read replica) when measurements show stats traffic increasing consumer lag
or Hikari acquire time, or when stats need independent scaling or availability. The HTTP contract does not change.

## References

- HikariCP pool sizing: https://github.com/brettwooldridge/HikariCP/wiki/About-Pool-Sizing
- Spring Boot JDBC template properties (`spring.jdbc.template.query-timeout`):
  https://docs.spring.io/spring-boot/appendix/application-properties/index.html
- PostgreSQL hot standby (read replicas): https://www.postgresql.org/docs/17/hot-standby.html
