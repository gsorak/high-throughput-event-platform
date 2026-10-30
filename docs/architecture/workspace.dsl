workspace "Multi-Tenant Event Ingestion Platform" "C4 model. Elements tagged 'Planned' are not implemented yet (see implementation-plan.md)." {

    !identifiers hierarchical

    model {
        operator = person "Platform Operator" "Runs load and failure experiments; watches throughput, latency and lag."

        tenantApp = softwareSystem "Tenant Application" "Customer system that sends batches of events over HTTP(S)." "External"
        tokenIssuer = softwareSystem "Token Issuer" "Issues JWTs with tenant_id and plan claims. Local: static RSA key + mint script. (Stage 2)" "External,Planned"

        observability = softwareSystem "Observability Stack" {
            prometheus = container "Prometheus" "Scrapes and stores metrics." "Prometheus 3" "Infrastructure"
            lagExporter = container "Lag Exporter" "Reads consumer-group offsets via the Kafka Admin API so lag is visible even when consumers are down." "kafka-exporter" "Infrastructure"
            grafana = container "Grafana" "Dashboards as code. (Stage 4)" "Grafana" "Infrastructure,Planned"
            tempo = container "Tempo" "Trace storage. (Stage 5)" "Grafana Tempo" "Infrastructure,Planned"
        }

        platform = softwareSystem "Event Ingestion Platform" {
            lb = container "Load Balancer" "Spreads HTTP traffic across API instances. (Stage 2)" "nginx (local and on the AWS host; no ALB)" "Infrastructure,Planned"
            api = container "Ingestion API" "Identifies the tenant, validates batches (all-or-nothing), publishes every event and returns 202 only after Kafka acknowledged all of them; 503 + Retry-After otherwise. Stage 2 adds JWT, per-tenant rate limits and admission control." "Java 25, Spring Boot 4.1, Spring MVC, virtual threads"
            rateStore = container "Rate-Limit Store" "Per-tenant token buckets (events/s + daily quota). Not a source of truth; availability-first fallback on outage. (Stage 2)" "Valkey" "Datastore"
            eventLog = container "Event Log" "Topic events.v1: 12 partitions, key = tenantId (load/contention choice, ordering not required). events.v1.DLT for records the processor cannot aggregate." "Apache Kafka 4.3, KRaft" "Queue"
            processor = container "Event Processor" "Consumes events.v1 one record at a time and upserts per-minute aggregates, skipping events already in the dedup ledger (same transaction); failed records go to the DLT. Also serves the low-volume stats API (shares JVM and pool with consumption)." "Java 25, Spring Boot 4.1, Spring for Apache Kafka, Spring JDBC"
            db = container "Usage Database" "Per-minute aggregates; dedup ledger with 21-day retention. Schema owned by the processor via Flyway." "PostgreSQL 17" "Datastore"
        }

        tenantApp -> tokenIssuer "Obtains access token (Stage 2)" "OAuth2 client credentials"
        tenantApp -> platform.api "POST /v1/events" "HTTP + Bearer JWT"
        tenantApp -> platform.processor "GET /v1/stats" "HTTP + Bearer JWT"
        platform.lb -> platform.api "Routes /v1/events" "HTTP"
        platform.lb -> platform.processor "Routes /v1/stats" "HTTP"
        platform.api -> platform.rateStore "Consumes N tokens per batch atomically" "RESP via Lettuce + Bucket4j"
        platform.api -> platform.eventLog "Produces one record per event" "Kafka; acks=all, idempotent producer"
        platform.processor -> platform.eventLog "Polls events; commits offsets after processing" "Kafka consumer group 'event-processor'"
        platform.processor -> platform.db "Upserts aggregates; queries stats" "JDBC / HikariCP"

        observability.prometheus -> platform.api "Scrapes /actuator/prometheus" "HTTP"
        observability.prometheus -> platform.processor "Scrapes /actuator/prometheus" "HTTP"
        observability.prometheus -> observability.lagExporter "Scrapes lag metrics" "HTTP"
        observability.lagExporter -> platform.eventLog "Reads committed offsets and log-end offsets" "Kafka Admin API"
        observability.grafana -> observability.prometheus "Queries" "PromQL"
        platform.api -> observability.tempo "Exports spans (Stage 5)" "OTLP"
        platform.processor -> observability.tempo "Exports spans (Stage 5)" "OTLP"
        operator -> observability.grafana "Views dashboards and traces" "HTTPS"
        operator -> observability.prometheus "Queries metrics (Stage 1)" "HTTP"
    }

    views {
        systemContext platform "SystemContext" {
            include *
            autolayout lr
        }
        container platform "Containers" {
            include *
            autolayout lr
        }
        container observability "Observability" {
            include *
            autolayout lr
        }
        styles {
            element "Person" {
                shape Person
            }
            element "Datastore" {
                shape Cylinder
            }
            element "Queue" {
                shape Pipe
            }
            element "External" {
                background #8a8a8a
                color #ffffff
            }
            element "Infrastructure" {
                background #dddddd
            }
            element "Planned" {
                border dashed
                opacity 60
            }
        }
    }
}
