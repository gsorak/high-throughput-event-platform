# ADR-0007: Spring JDBC (`JdbcClient`) instead of JPA

- Status: Accepted (2026-10-27). Alternatives and references added in the documentation review (2026-10-27).
- Stage: 1 (implemented)
- Assessment: **Partly design-time.** The proposal rejected JPA in one sentence: "The work is bulk inserts, ON
  CONFLICT upserts and range queries. JPA adds per-entity overhead and hides the SQL that matters here." Spring Data
  JDBC, jOOQ and MyBatis were not compared at the time. That comparison is retrospective.

## Context

The processor's database work is a single-statement upsert per event (Stage 1), a dedup insert plus upsert in one
transaction (Stage 3), batched upserts (Stage 4) and range queries. Performance depends on the exact SQL
(`INSERT … ON CONFLICT DO UPDATE`, statement batching, round trips).

Stage 1 has one table, one upsert and four read statements (`EventStatsRepository`). No row is ever loaded, changed
in memory and written back. Every write is an increment that the database applies atomically.

## Decision

Use Spring JDBC's `JdbcClient` with hand-written SQL, HikariCP, and Flyway migrations. No JPA/Hibernate.
Alternatives to Flyway and HikariCP are in [technology-selection.md](../architecture/technology-selection.md#24-persistence).

## Alternatives considered

1. **JPA with Hibernate**, through Spring Data JPA repositories.
2. **Spring Data JDBC.** Repositories for aggregates, without lazy loading, dirty checking or a persistence context.
3. **jOOQ.** A type-safe SQL DSL, with classes generated from the schema.
4. **MyBatis.** SQL in XML or annotations, mapped to objects.

## Comparison

| Criterion                                  | **`JdbcClient` (chosen)**                                  | JPA / Hibernate                                          | Spring Data JDBC                                                 | jOOQ                                             | MyBatis                      |
| ------------------------------------------ | ---------------------------------------------------------- | -------------------------------------------------------- | ---------------------------------------------------------------- | ------------------------------------------------ | ---------------------------- |
| `ON CONFLICT … DO UPDATE` with expressions | Plain SQL                                                  | Native query; the entity model is bypassed               | `@Query` with SQL; `save()` chooses INSERT or UPDATE, not upsert | In the DSL (`onConflict().doUpdate()`)           | Plain SQL in the mapper      |
| Statement batching (Stage 4)               | `NamedParameterJdbcTemplate.batchUpdate`, or multi-row SQL | `hibernate.jdbc.batch_size`; depends on flush behaviour  | `saveAll`, or drop down to `JdbcTemplate`                        | `batch(…)`                                       | `ExecutorType.BATCH`         |
| What reaches the database                  | The SQL string in the code                                 | Generated; depends on flush, cascades and dirty checking | Generated for CRUD; explicit for `@Query`                        | Generated, close to 1:1 with the DSL             | The SQL string in the mapper |
| SQL checked at compile time                | No                                                         | No (JPQL is checked at startup)                          | No                                                               | **Yes**, against the generated schema classes    | No                           |
| Build and runtime cost                     | None                                                       | Persistence context, proxies, entity metadata            | Small mapping layer                                              | A code-generation step from DDL or a live schema | Mapper files                 |

## Rationale

- **The hot statement is a PostgreSQL upsert with an atomic increment.** An ORM would send it as a native query
  anyway, so the ORM would sit unused on the one path that matters for throughput.
- **There is no object graph.** Rows are counters and are never loaded for modification. The persistence context,
  dirty checking and lazy loading would either be unused or work against the design. For example, the
  first-level cache grows during a large batch unless it is cleared.
- **Stage 4 controls round trips directly.** The experiments vary JDBC batching, pgjdbc's `reWriteBatchedInserts`
  and multi-row statements. These are JDBC and driver features. `JdbcClient` and `JdbcTemplate` (same module) expose
  them without a translation layer.
- **jOOQ is the most credible alternative.** Its value is compile-time checking of SQL against the schema. With one
  table and five statements, all exercised by Testcontainers tests against real PostgreSQL after Flyway migrations,
  that value does not yet pay for a code-generation step.
- `JdbcClient` rather than `JdbcTemplate`: same engine, but a fluent API with named parameters. `JdbcClient` has no
  batch API (checked in spring-jdbc 7.0.10), so Stage 4 batching will use `NamedParameterJdbcTemplate.batchUpdate`
  from the same module.

## Consequences

- The SQL that matters is visible in the code and in reviews; no entity state, dirty checking, or flush ordering to
  reason about.
- `ON CONFLICT` upserts and batching are expressed directly instead of through native-query escapes.
- Row mapping is manual (a few lines per query). Acceptable for a handful of statements.
- SQL is not checked against the schema at compile time. A renamed column fails at runtime. The integration tests
  catch this because they run every statement against the migrated schema.
- Flyway runs only in `event-processor` (the schema owner) at startup.

## Reconsider when

- The schema grows beyond a few tables, or queries are built dynamically (optional filters, user-defined grouping).
  Then use jOOQ for type safety.
- A domain model with invariants across related entities appears. That would be a different service; Spring Data
  JDBC or JPA would fit there, not in the aggregation path.
- Stage 4 shows that even batched statements are the limit. Then use PostgreSQL `COPY` through pgjdbc's
  `CopyManager` into a staging table, which is still no ORM.

## References

- Spring Framework, JDBC core classes (`JdbcClient`, `JdbcTemplate`, batch operations):
  https://docs.spring.io/spring-framework/reference/data-access/jdbc/core.html
- Spring Data JPA: https://spring.io/projects/spring-data-jpa; Hibernate ORM: https://hibernate.org/orm/documentation/
- Spring Data JDBC: https://spring.io/projects/spring-data-jdbc
- jOOQ manual: https://www.jooq.org/doc/latest/manual/
- MyBatis: https://mybatis.org/mybatis-3/
- PostgreSQL `INSERT … ON CONFLICT`: https://www.postgresql.org/docs/17/sql-insert.html
- pgjdbc connection properties (`reWriteBatchedInserts`): https://jdbc.postgresql.org/documentation/use/
