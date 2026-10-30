# ADR-0010: JSON envelope with an explicit codec, no schema registry

- Status: Accepted (2026-10-27)
- Stage: 1 (implemented)
- Assessment: **Partly retrospective.** The proposal ruled out Schema Registry and Avro in one line ("adds moving
  parts"). Protobuf, CloudEvents and Spring's type headers were not discussed. The evolution risk below was found
  during this review.

## Problem

- Two deployables exchange events through `events.v1` ([ADR-0008](0008-kafka-buffer-two-deployables.md)). The wire
  format must be readable by any consumer, including tools and a future consumer in another language.
- The format must be able to evolve while producers and consumers are deployed at different times. Records written
  with an older format stay in the topic for 7 days, and up to 21 days counting replays
  ([ADR-0005](0005-deduplication-retention.md)).
- Malformed records must be recognisable as such, so that the error handler can skip them (Stage 1) or send them to
  a dead-letter topic (Stage 3) instead of retrying them.

## Decision

- The value is JSON produced by `EventEnvelopeCodec` in `event-contract`, a library without Spring that both services
  depend on. Kafka sees only bytes (`ByteArraySerializer` / `ByteArrayDeserializer`).
- Spring's `JsonSerializer` type headers are not used.
- The envelope carries `schemaVersion` (currently `1`). The decoder:
  - ignores unknown properties (tolerant reader), so added optional fields are compatible;
  - rejects missing required fields and any `schemaVersion` other than 1 with `EnvelopeDecodingException`. That
    exception is the only one the error handler treats as non-retryable.
- No schema registry. Compatibility is enforced by the shared module and its codec tests, because both services build
  from one repository.

## Alternatives considered

1. **Spring Kafka `JsonSerializer` / `JsonDeserializer`.** By default the serializer adds `__TypeId__` headers that
   name the producer's Java class.
2. **Avro with a schema registry.** Either Confluent Schema Registry (Confluent Community License) or Apicurio Registry
   (Apache 2.0). The registry checks compatibility when a schema is registered.
3. **Protobuf**, with or without a registry. Compatibility comes from field numbers.
4. **CloudEvents** (CNCF) as the envelope, in structured JSON mode or binary mode where attributes become Kafka
   headers.

## Comparison

| Criterion                 | **Codec + JSON (chosen)**                              | Spring JSON with type headers            | Avro + registry                        | Protobuf                           | CloudEvents                          |
| ------------------------- | ------------------------------------------------------ | ---------------------------------------- | -------------------------------------- | ---------------------------------- | ------------------------------------ |
| Readable without our code | Yes (`kcat`, console consumer)                         | Yes, but headers name Java classes       | Needs the registry or the schema       | Needs the `.proto` file            | Yes                                  |
| Compatibility check       | Codec tests in this repository only                    | None                                     | Enforced on registration (per subject) | By convention and with `buf` tools | Envelope only; payload is still ours |
| Size and CPU              | Largest: field names in every record. **Not measured** | Same as JSON                             | Compact binary                         | Compact binary                     | JSON plus attributes, or headers     |
| Extra infrastructure      | None                                                   | None                                     | Registry service (HA, backups, auth)   | None, or a registry                | None                                 |
| Coupling                  | Shared Java module                                     | Consumer depends on producer class names | Schema in the registry                 | Shared `.proto` files              | Standard attributes                  |

## Rationale

- With one producer and one consumer in one repository, a shared module gives the same compile-time agreement a
  registry would give, without another service.
- Readable records make the Stage 1–3 failure work (skips, DLT inspection, redrives) easier to check by hand.
- Writing the codec explicitly keeps decoding errors inside the listener, where the error handler can classify them,
  instead of failing inside the deserializer.

## Consequences

- **Rollout order matters, and nothing enforces it.** If a producer emitting `schemaVersion: 2` is deployed before
  the consumers, every v2 record counts as malformed. In Stage 1 it is skipped, and the only trace is the
  `processor.events.skipped{reason="malformed"}` counter. That is silent data loss. Since Stage 3 the records go to the
  DLT (`processor.events.dead_lettered{reason="malformed"}`) and can be redriven after the consumers are upgraded.
- A renamed or retyped optional field is not detected: the old name is ignored and the new field decodes as `null`.
- The JSON size and CPU cost have not been measured. Stage 4's compression experiment (`lz4`/`zstd`) is the first
  measurement.
- Compatibility checks are tests in one repository. A second team or language would need a published schema.

## Reconsider when

- A second producer or consumer appears outside this repository, or in another language. Then publish a schema
  (Avro or Protobuf with a registry, or JSON Schema) and add a CI compatibility check.
- Stage 4 shows that serialization or record size is a significant share of CPU, network or broker disk. Then compare
  JSON + zstd against Avro or Protobuf.
- Interoperability with external event consumers is required. Then consider CloudEvents attributes as Kafka headers.

## Recommendations (not implemented; see [technology-selection.md, Q2](../architecture/technology-selection.md#6-open-questions-and-recommendations))

- Write down the rule "deploy consumers before producers when `schemaVersion` changes" in the contract module.
- Alert when `processor.events.dead_lettered` is greater than zero.
- Consider treating "valid envelope, newer version" as retryable or as DLT input rather than "malformed".

## References

- Tolerant Reader: https://martinfowler.com/bliki/TolerantReader.html
- Confluent Schema Registry (licence and compatibility modes):
  https://docs.confluent.io/platform/current/schema-registry/index.html
- Apicurio Registry: https://www.apicur.io/registry/
- Apache Avro: https://avro.apache.org/
- Protocol Buffers: https://protobuf.dev/
- CloudEvents: https://cloudevents.io/ and the Kafka protocol binding in https://github.com/cloudevents/spec
