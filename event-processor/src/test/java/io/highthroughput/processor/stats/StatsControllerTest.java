package io.highthroughput.processor.stats;

import io.highthroughput.contract.TenantIds;
import io.highthroughput.processor.aggregation.EventStatsRepository;
import io.highthroughput.processor.aggregation.EventStatsRepository.MinuteBucket;
import io.highthroughput.processor.aggregation.EventStatsRepository.Totals;
import io.highthroughput.processor.config.ProcessorConfiguration;
import io.highthroughput.processor.security.SecurityConfiguration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.assertj.MockMvcTester;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt;

/**
 * Request validation and response shape of {@code GET /v1/stats}; the
 * repository is a mock, and requests carry an already-authenticated JWT.
 */
@WebMvcTest(StatsController.class)
@Import({ ProcessorConfiguration.class, SecurityConfiguration.class })
class StatsControllerTest {

    private static final Instant FROM = Instant.parse("2026-10-27T10:00:00Z");
    private static final Instant TO = Instant.parse("2026-10-27T11:00:00Z");

    @Autowired
    MockMvcTester mvc;

    @MockitoBean
    EventStatsRepository repository;

    /**
     * Replaces the decoder that would read the public key file; jwt() below
     * bypasses decoding.
     */
    @MockitoBean
    JwtDecoder jwtDecoder;

    @Test
    void returnsBucketsAndTotals() {
        when(repository.findBuckets(eq("acme"), eq(FROM), eq(TO), isNull(), anyInt()))
                .thenReturn(List.of(new MinuteBucket(FROM, "api.call", 3, 4.5)));
        when(repository.totals("acme", FROM, TO, null)).thenReturn(new Totals(3, 4.5));

        assertThat(get("acme", "from=" + FROM + "&to=" + TO))
                .hasStatus(HttpStatus.OK)
                .bodyJson().isLenientlyEqualTo("""
                        {"tenantId": "acme", "from": "2026-10-27T10:00:00Z", "to": "2026-10-27T11:00:00Z",
                         "type": null, "totalCount": 3, "totalValueSum": 4.5, "truncated": false,
                         "buckets": [{"bucketStart": "2026-10-27T10:00:00Z", "type": "api.call",
                                      "count": 3, "valueSum": 4.5}]}""");
    }

    @Test
    void passesTheTypeFilterToTheRepository() {
        when(repository.totals(any(), any(), any(), any())).thenReturn(new Totals(0, 0));

        assertThat(get("acme", "from=" + FROM + "&to=" + TO + "&type=login")).hasStatus(HttpStatus.OK);
        verify(repository).findBuckets("acme", FROM, TO, "login", 5001);
    }

    @Test
    void moreBucketsThanTheLimitAreTruncated() {
        List<MinuteBucket> tooMany = IntStream.range(0, 5001)
                .mapToObj(i -> new MinuteBucket(FROM.plusSeconds(i), "api.call", 1, 0))
                .toList();
        when(repository.findBuckets(any(), any(), any(), any(), anyInt())).thenReturn(tooMany);
        when(repository.totals(any(), any(), any(), any())).thenReturn(new Totals(5001, 0));

        var result = get("acme", "from=" + FROM + "&to=" + TO).exchange();

        assertThat(result).hasStatus(HttpStatus.OK);
        assertThat(result).bodyJson().extractingPath("$.truncated").isEqualTo(true);
        assertThat(result).bodyJson().extractingPath("$.buckets.length()").isEqualTo(5000);
        assertThat(result).bodyJson().extractingPath("$.totalCount").isEqualTo(5001);
    }

    @Test
    void missingTokenIsUnauthorized() {
        var result = mvc.get().uri("/v1/stats?from=" + FROM + "&to=" + TO).exchange();

        assertThat(result).hasStatus(HttpStatus.UNAUTHORIZED).hasContentType(MediaType.APPLICATION_PROBLEM_JSON);
        assertThat(result.getResponse().getHeader("WWW-Authenticate")).isEqualTo("Bearer");
        verifyNoInteractions(repository);
    }

    @Test
    void tenantComesFromTheTokenNotFromAHeader() {
        when(repository.totals(any(), any(), any(), any())).thenReturn(new Totals(0, 0));

        assertThat(get("acme", "from=" + FROM + "&to=" + TO).header("X-Tenant-Id", "other"))
                .hasStatus(HttpStatus.OK)
                .bodyJson().extractingPath("$.tenantId").isEqualTo("acme");
        verify(repository).totals("acme", FROM, TO, null);
        verify(repository, never()).totals(eq("other"), any(), any(), any());
    }

    @Test
    void fromMustBeBeforeTo() {
        assertInvalid("from=" + TO + "&to=" + FROM);
    }

    @Test
    void rangeIsLimitedTo24Hours() {
        assertInvalid("from=" + FROM + "&to=" + FROM.plus(24, ChronoUnit.HOURS).plusSeconds(1));
    }

    @Test
    void typeMustBeAValidEventType() {
        assertInvalid("from=" + FROM + "&to=" + TO + "&type=Not%20Valid");
    }

    @Test
    void missingOrUnparsableParametersAreBadRequests() {
        assertThat(get("acme", "to=" + TO)).hasStatus(HttpStatus.BAD_REQUEST)
                .hasContentType(MediaType.APPLICATION_PROBLEM_JSON);
        assertThat(get("acme", "from=yesterday&to=" + TO)).hasStatus(HttpStatus.BAD_REQUEST)
                .hasContentType(MediaType.APPLICATION_PROBLEM_JSON);
        verifyNoInteractions(repository);
    }

    private void assertInvalid(String query) {
        assertThat(get("acme", query)).hasStatus(HttpStatus.BAD_REQUEST)
                .hasContentType(MediaType.APPLICATION_PROBLEM_JSON)
                .bodyJson().extractingPath("$.title").isEqualTo("Invalid stats query");
        verifyNoInteractions(repository);
    }

    private MockMvcTester.MockMvcRequestBuilder get(String tenant, String query) {
        return mvc.get().uri("/v1/stats?" + query)
                .with(jwt().jwt(token -> token.claim(TenantIds.JWT_CLAIM, tenant)));
    }
}
