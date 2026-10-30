package io.highthroughput.ingest.web;

import io.highthroughput.contract.EventEnvelope;
import io.highthroughput.contract.TenantIds;
import io.highthroughput.ingest.config.IngestionConfiguration;
import io.highthroughput.ingest.publish.EventPublisher;
import io.highthroughput.ingest.publish.PublishFailedException;
import io.highthroughput.ingest.ratelimit.RateLimitExceededException;
import io.highthroughput.ingest.ratelimit.TenantRateLimiter;
import io.highthroughput.ingest.security.SecurityConfiguration;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.assertj.MockMvcTester;
import org.springframework.test.web.servlet.assertj.MvcTestResult;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.assertj.core.api.Assertions.within;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt;

/**
 * HTTP contract of {@code POST /v1/events}, without Kafka or Valkey: the
 * publisher and the rate limiter are Mockito
 * mocks, and requests carry an already-authenticated JWT (signature and claim
 * checks are covered by
 * {@code AuthenticationIT}).
 * {@code @WebMvcTest} starts only the web layer (controllers, advice, filters)
 * and drives it through MockMvc,
 * which calls the DispatcherServlet in-process instead of opening a socket.
 */
@WebMvcTest(EventsController.class)
@Import({ BatchValidator.class, IngestionConfiguration.class, SecurityConfiguration.class })
class EventsControllerTest {

        private static final String TENANT = "acme";

        @Autowired
        MockMvcTester mvc;

        @MockitoBean
        EventPublisher publisher;

        @MockitoBean
        TenantRateLimiter rateLimiter;

        /**
         * Replaces the decoder that would read the public key file; jwt() below
         * bypasses decoding.
         */
        @MockitoBean
        JwtDecoder jwtDecoder;

        @Test
        void validBatchIsPublishedAndAccepted() {
                UUID first = UUID.randomUUID();
                UUID second = UUID.randomUUID();
                String body = batch(
                                event(first, "api.call", Instant.now().minusSeconds(60),
                                                "\"value\": 12.5, \"attributes\": {\"region\": \"eu\"}"),
                                event(second, "api.call", Instant.now().minusSeconds(30), ""));

                assertThat(post(TENANT, body))
                                .hasStatus(HttpStatus.ACCEPTED)
                                .bodyJson().extractingPath("$.accepted").isEqualTo(2);

                verify(rateLimiter).acquire(TENANT, 2);
                @SuppressWarnings("unchecked")
                ArgumentCaptor<List<EventEnvelope>> published = ArgumentCaptor.forClass(List.class);
                verify(publisher).publish(published.capture());
                assertThat(published.getValue())
                                .extracting(EventEnvelope::eventId, EventEnvelope::tenantId)
                                .containsExactly(tuple(first, TENANT), tuple(second, TENANT));
                EventEnvelope envelope = published.getValue().getFirst();
                assertThat(envelope.value()).isEqualTo(12.5);
                assertThat(envelope.attributes()).containsEntry("region", "eu");
                assertThat(envelope.receivedAt()).isCloseTo(Instant.now(), within(5, ChronoUnit.SECONDS));
        }

        @Test
        void oneInvalidEventRejectsTheWholeBatchWithAPointerToIt() {
                String body = batch(
                                event(UUID.randomUUID(), "api.call", Instant.now(), ""),
                                event(UUID.randomUUID(), "Not A Valid Type", Instant.now(), ""));

                MvcTestResult result = post(TENANT, body).exchange();

                assertThat(result).hasStatus(HttpStatus.BAD_REQUEST)
                                .hasContentType(MediaType.APPLICATION_PROBLEM_JSON);
                assertThat(result).bodyJson().extractingPath("$.title").isEqualTo("Invalid event batch");
                assertThat(result).bodyJson().extractingPath("$.errors[0].pointer").isEqualTo("/events/1/type");
                verifyNoInteractions(publisher);
                // An invalid batch does not use up the tenant's rate limit.
                verifyNoInteractions(rateLimiter);
        }

        @Test
        void everyViolationIsReportedSortedByPointer() {
                String body = """
                                {"events": [
                                  {"type": "api.call", "occurredAt": "%s"},
                                  {"eventId": "%s", "occurredAt": "%s", "attributes": {"k": "%s"}}
                                ]}""".formatted(Instant.now(), UUID.randomUUID(), Instant.now(), "x".repeat(257));

                assertThat(post(TENANT, body)).hasStatus(HttpStatus.BAD_REQUEST)
                                .bodyJson().extractingPath("$.errors[*].pointer")
                                .asArray()
                                .containsExactly("/events/0/eventId", "/events/1/attributes/k", "/events/1/type");
                verifyNoInteractions(publisher);
        }

        @Test
        void emptyBatchIsRejected() {
                assertThat(post(TENANT, "{\"events\": []}")).hasStatus(HttpStatus.BAD_REQUEST)
                                .bodyJson().extractingPath("$.errors[0].pointer").isEqualTo("/events");
        }

        @Test
        void batchOverFiveHundredEventsIsRejected() {
                List<String> events = new ArrayList<>();
                for (int i = 0; i <= IngestEventsRequest.MAX_EVENTS; i++) {
                        events.add(event(UUID.randomUUID(), "api.call", Instant.now(), ""));
                }

                assertThat(post(TENANT, batch(events.toArray(String[]::new)))).hasStatus(HttpStatus.BAD_REQUEST)
                                .bodyJson().extractingPath("$.errors[0].pointer").isEqualTo("/events");
                verifyNoInteractions(publisher);
        }

        @Test
        void duplicateEventIdWithinABatchIsRejected() {
                UUID id = UUID.randomUUID();
                String body = batch(event(id, "api.call", Instant.now(), ""), event(id, "api.call", Instant.now(), ""));

                assertThat(post(TENANT, body)).hasStatus(HttpStatus.BAD_REQUEST)
                                .bodyJson().extractingPath("$.errors[0].pointer").isEqualTo("/events/1/eventId");
        }

        @Test
        void occurredAtOutsideTheAcceptanceWindowIsRejected() {
                String body = batch(
                                event(UUID.randomUUID(), "api.call", Instant.now().plusSeconds(10 * 60), ""),
                                event(UUID.randomUUID(), "api.call", Instant.now().minus(Duration.ofDays(8)), ""));

                assertThat(post(TENANT, body)).hasStatus(HttpStatus.BAD_REQUEST)
                                .bodyJson().extractingPath("$.errors[*].pointer")
                                .asArray().containsExactly("/events/0/occurredAt", "/events/1/occurredAt");
        }

        @Test
        void malformedJsonIsRejected() {
                assertThat(post(TENANT, "{\"events\": [")).hasStatus(HttpStatus.BAD_REQUEST)
                                .bodyJson().extractingPath("$.title").isEqualTo("Malformed request body");
        }

        @Test
        void wrongFieldTypeIsReportedWithAPointer() {
                String body = """
                                {"events": [{"eventId": "not-a-uuid", "type": "api.call", "occurredAt": "%s"}]}"""
                                .formatted(Instant.now());

                assertThat(post(TENANT, body)).hasStatus(HttpStatus.BAD_REQUEST)
                                .bodyJson().extractingPath("$.errors[0].pointer").isEqualTo("/events/0/eventId");
        }

        @Test
        void valueOutsideTheAllowedRangeIsRejected() {
                String body = batch(
                                event(UUID.randomUUID(), "api.call", Instant.now(), "\"value\": 1e13"),
                                event(UUID.randomUUID(), "api.call", Instant.now(), "\"value\": -1e13"),
                                event(UUID.randomUUID(), "api.call", Instant.now(), "\"value\": 1e12"));

                assertThat(post(TENANT, body)).hasStatus(HttpStatus.BAD_REQUEST)
                                .bodyJson().extractingPath("$.errors[*].pointer")
                                .asArray().containsExactly("/events/0/value", "/events/1/value");
                verifyNoInteractions(publisher);
        }

        @Test
        void missingTokenIsUnauthorized() {
                String body = batch(event(UUID.randomUUID(), "api.call", Instant.now(), ""));

                MvcTestResult result = mvc.post().uri("/v1/events").contentType(MediaType.APPLICATION_JSON)
                                .content(body)
                                .exchange();

                assertThat(result).hasStatus(HttpStatus.UNAUTHORIZED)
                                .hasContentType(MediaType.APPLICATION_PROBLEM_JSON);
                assertThat(result.getResponse().getHeader("WWW-Authenticate")).startsWith("Bearer");
                verifyNoInteractions(publisher);
        }

        @Test
        void tenantHeaderIsIgnoredInFavourOfTheToken() {
                String body = batch(event(UUID.randomUUID(), "api.call", Instant.now(), ""));

                assertThat(post(TENANT, body).header("X-Tenant-Id", "other")).hasStatus(HttpStatus.ACCEPTED);

                @SuppressWarnings("unchecked")
                ArgumentCaptor<List<EventEnvelope>> published = ArgumentCaptor.forClass(List.class);
                verify(publisher).publish(published.capture());
                assertThat(published.getValue()).extracting(EventEnvelope::tenantId).containsOnly(TENANT);
                verify(rateLimiter).acquire(TENANT, 1);
        }

        @Test
        void rateLimitedBatchIsTooManyRequestsWithRetryAfter() {
                doThrow(new RateLimitExceededException(RateLimitExceededException.Limit.RATE, Duration.ofSeconds(3)))
                                .when(rateLimiter).acquire(anyString(), anyInt());

                MvcTestResult result = post(TENANT, batch(event(UUID.randomUUID(), "api.call", Instant.now(), "")))
                                .exchange();

                assertThat(result).hasStatus(HttpStatus.TOO_MANY_REQUESTS)
                                .hasHeader("Retry-After", "3")
                                .hasContentType(MediaType.APPLICATION_PROBLEM_JSON);
                assertThat(result).bodyJson().extractingPath("$.limit").isEqualTo("rate");
                verifyNoInteractions(publisher);
        }

        @Test
        void bodyLargerThanOneMebibyteIsRejectedBeforeParsing() {
                String body = " ".repeat(1024 * 1024 + 1);

                assertThat(post(TENANT, body)).hasStatus(HttpStatus.CONTENT_TOO_LARGE)
                                .bodyJson().extractingPath("$.title").isEqualTo("Request body too large");
                verifyNoInteractions(publisher);
        }

        @Test
        void publishFailureIsServiceUnavailableWithRetryAfter() {
                doThrow(new PublishFailedException("1 of 1 events were not acknowledged", null))
                                .when(publisher).publish(any());

                assertThat(post(TENANT, batch(event(UUID.randomUUID(), "api.call", Instant.now(), ""))))
                                .hasStatus(HttpStatus.SERVICE_UNAVAILABLE)
                                .hasHeader("Retry-After", "1")
                                .hasContentType(MediaType.APPLICATION_PROBLEM_JSON);
        }

        private MockMvcTester.MockMvcRequestBuilder post(String tenant, String body) {
                return mvc.post().uri("/v1/events")
                                .with(jwt().jwt(token -> token.claim(TenantIds.JWT_CLAIM, tenant)))
                                .contentType(MediaType.APPLICATION_JSON)
                                .content(body);
        }

        private static String batch(String... events) {
                return "{\"events\": [" + String.join(",", events) + "]}";
        }

        /**
         * @param extraFields optional JSON members to append, e.g. {@code "value": 1}
         */
        private static String event(UUID id, String type, Instant occurredAt, String extraFields) {
                String fields = List.of(
                                "\"eventId\": \"%s\"".formatted(id),
                                "\"type\": \"%s\"".formatted(type),
                                "\"occurredAt\": \"%s\"".formatted(occurredAt),
                                extraFields).stream()
                                .filter(field -> !field.isEmpty())
                                .collect(Collectors.joining(", "));
                return "{" + fields + "}";
        }
}
