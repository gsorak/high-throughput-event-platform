# ADR-0009: Spring MVC on virtual threads

- Status: Accepted (2026-10-27)
- Stage: 1 (implemented in both services)
- Assessment: **Partly retrospective.** The original proposal compared virtual threads with WebFlux in one sentence.
  Platform threads and MVC async were not considered. Stage 4 measures virtual threads against platform threads.

## Problem

- `POST /v1/events` blocks while Kafka acknowledges every record ([ADR-0002](0002-acceptance-semantics.md)). In the
  baseline the publish p99 stayed at or below 168 ms, and the wait can last up to the 2.5 s publish deadline. On a
  thread-per-request server, waiting requests hold threads.
- `event-processor` blocks on JDBC in 3 listener threads and in `GET /v1/stats`.
- The libraries in use (kafka-clients, the PostgreSQL JDBC driver, HikariCP) have blocking APIs.

## Decision

- Both services use Spring MVC on Tomcat with `spring.threads.virtual.enabled: true` (JDK 25).
- Request code stays blocking and sequential: send all records, then wait on `CompletableFuture.allOf(...)` with a
  deadline.

## Alternatives considered

1. **MVC on platform threads.** Tomcat's default pool is 200 threads (`server.tomcat.threads.max`).
2. **MVC async.** Return a `CompletableFuture` or `DeferredResult` built from the Kafka send futures, so the Tomcat
   thread is released while waiting.
3. **WebFlux on Reactor Netty.** In `event-processor` this would also mean R2DBC, or JDBC on a bounded scheduler.

## Comparison

| Criterion                             | **MVC + virtual threads**                                       | MVC + platform threads                                     | MVC async                                        | WebFlux                                                        |
| ------------------------------------- | --------------------------------------------------------------- | ---------------------------------------------------------- | ------------------------------------------------ | -------------------------------------------------------------- |
| Concurrent waiting requests           | Not limited by a thread pool                                    | 200 (pool size)                                            | Not limited by a thread pool                     | Not limited by a thread pool                                   |
| Implicit overload limit               | **None** except `server.tomcat.max-connections` (default 8,192) | The pool: requests queue at 200 and then in `accept-count` | None                                             | None                                                           |
| Code style                            | Blocking, sequential                                            | Blocking, sequential                                       | Futures, or callbacks around the blocking parts  | Reactive operators; blocking calls must be isolated            |
| Fit with Kafka/JDBC/Hikari            | Direct; since JDK 24 `synchronized` no longer pins (JEP 491)    | Direct                                                     | Kafka fits (futures); JDBC still blocks a thread | Kafka needs a reactive wrapper; JDBC needs R2DBC or offloading |
| Stack traces, debugging, thread dumps | Ordinary                                                        | Ordinary                                                   | Split across threads                             | Split across operators                                         |
| Test slices and filters               | MVC (`@WebMvcTest`)                                             | MVC                                                        | MVC with async dispatch                          | WebFlux (`@WebFluxTest`)                                       |

## Rationale

- The workload is "wait on I/O, then answer". Virtual threads make that wait cheap without changing the programming
  model, and the code remains a direct reading of ADR-0002.
- JDK 25 removes the `synchronized` pinning that would have distorted results on JDK 21
  ([ADR-0001](0001-platform-versions.md)).
- WebFlux would not raise the processor's throughput. The processor is limited by one database round trip per event
  ([00-baseline.md](../performance/00-baseline.md)), and a reactive rewrite of the JDBC path would not remove that.
- MVC async would also avoid holding a pool thread on the API side. It keeps the code in a familiar model at the cost
  of future composition in the controller. Virtual threads give the same benefit with less code.

## Consequences

- **There is no admission control in Stage 1.** With platform threads, the pool of 200 acts as an implicit limit. With
  virtual threads, Tomcat's thread settings have no effect (Spring Boot documentation). Concurrency is then limited
  only by `max-connections` (8,192) and, during a Kafka outage, by the producer buffer filling up and `send()` failing
  after `max.block.ms` (500 ms).
  - During a broker stall, every request can hold its parsed batch (up to a 1 MiB body) for up to 2.5 s.
  - Heap use under a request flood is therefore unbounded in practice.
  - Stage 2 adds an explicit limiter that rejects requests quickly
    ([technology-selection.md, admission control](../architecture/technology-selection.md#32-admission-control)).
- **In `event-processor`, virtual threads give little benefit.** It has 3 listener threads and a Hikari pool of 10, so
  thread count was never the limit. The risk is low, but the docs should not claim a benefit there.
- `ThreadLocal`-heavy libraries can allocate per request because virtual threads are not pooled. None was observed in
  Stage 1.

## Reconsider when

- The Stage 4 virtual vs platform thread experiment shows no throughput or latency gain for the API. Then platform
  threads with an explicit pool size are simpler to bound.
- A non-blocking end-to-end stack becomes a requirement, for example a streaming response or a very large number of
  long-lived connections. Then reconsider WebFlux.

## References

- JEP 444, Virtual Threads: https://openjdk.org/jeps/444
- JEP 491, Synchronize Virtual Threads without Pinning: https://openjdk.org/jeps/491
- Spring Boot, virtual threads:
  https://docs.spring.io/spring-boot/reference/features/spring-application.html#features.spring-application.virtual-threads
- Spring MVC asynchronous requests: https://docs.spring.io/spring-framework/reference/web/webmvc/mvc-ann-async.html
- Spring WebFlux: https://docs.spring.io/spring-framework/reference/web/webflux.html
- Oracle, virtual threads guide: https://docs.oracle.com/en/java/javase/24/core/virtual-threads.html
