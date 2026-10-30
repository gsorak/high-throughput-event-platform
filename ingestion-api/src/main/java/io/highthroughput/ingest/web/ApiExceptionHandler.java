package io.highthroughput.ingest.web;

import io.highthroughput.ingest.config.IngestionProperties;
import io.highthroughput.ingest.publish.PublishFailedException;
import io.highthroughput.ingest.ratelimit.RateLimitExceededException;
import io.highthroughput.ingest.ratelimit.RateLimitExceededException.Limit;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.security.core.AuthenticationException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;
import tools.jackson.core.JacksonException;

import java.util.List;
import java.util.Locale;

/**
 * Maps failures to RFC 9457 {@code application/problem+json}. Extending
 * {@link ResponseEntityExceptionHandler} gives
 * problem details for Spring MVC's own errors too (415 wrong content type, 405,
 * ...).
 */
@RestControllerAdvice
public class ApiExceptionHandler extends ResponseEntityExceptionHandler {

    private final IngestionProperties properties;

    public ApiExceptionHandler(IngestionProperties properties) {
        this.properties = properties;
    }

    @ExceptionHandler(InvalidBatchException.class)
    ProblemDetail invalidBatch(InvalidBatchException e) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, e.getMessage());
        problem.setTitle("Invalid event batch");
        problem.setProperty("errors", e.violations());
        return problem;
    }

    @ExceptionHandler(AuthenticationException.class)
    ProblemDetail unauthenticated(AuthenticationException e) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.UNAUTHORIZED,
                "A valid bearer token is required; see the WWW-Authenticate header for the reason");
        problem.setTitle("Unauthorized");
        return problem;
    }

    @ExceptionHandler(RateLimitExceededException.class)
    ResponseEntity<ProblemDetail> rateLimited(RateLimitExceededException e) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.TOO_MANY_REQUESTS,
                e.getMessage() + ". No event of the batch was accepted.");
        problem.setTitle(e.limit() == Limit.QUOTA ? "Daily quota exceeded" : "Rate limit exceeded");
        problem.setProperty("limit", e.limit().name().toLowerCase(Locale.ROOT));
        return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                .header(HttpHeaders.RETRY_AFTER, String.valueOf(e.retryAfter().toSeconds()))
                .body(problem);
    }

    @ExceptionHandler(ServerBusyException.class)
    ResponseEntity<ProblemDetail> serverBusy(ServerBusyException e) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.SERVICE_UNAVAILABLE, e.getMessage());
        problem.setTitle("Server busy");
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .header(HttpHeaders.RETRY_AFTER, String.valueOf(properties.retryAfter().toSeconds()))
                .body(problem);
    }

    /**
     * Some events of the batch may already be in Kafka. The client must retry the
     * <em>whole</em> batch with the same
     * eventIds; see docs/architecture/delivery-guarantees.md.
     */
    @ExceptionHandler(PublishFailedException.class)
    ResponseEntity<ProblemDetail> publishFailed(PublishFailedException e) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.SERVICE_UNAVAILABLE,
                "The batch was not fully acknowledged by the event log. Retry the whole batch with the same eventIds.");
        problem.setTitle("Event log unavailable");
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .header(HttpHeaders.RETRY_AFTER, String.valueOf(properties.retryAfter().toSeconds()))
                .body(problem);
    }

    /**
     * Covers malformed JSON, wrong field types, and bodies over the size limit (see
     * {@link RequestBodyLimitFilter}).
     */
    @Override
    protected ResponseEntity<Object> handleHttpMessageNotReadable(HttpMessageNotReadableException ex,
            HttpHeaders headers, HttpStatusCode status, WebRequest request) {

        PayloadTooLargeException tooLarge = findCause(ex, PayloadTooLargeException.class);
        if (tooLarge != null) {
            ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.CONTENT_TOO_LARGE,
                    "Request body must not exceed " + tooLarge.maxBytes() + " bytes");
            problem.setTitle("Request body too large");
            return handleExceptionInternal(ex, problem, headers, HttpStatus.CONTENT_TOO_LARGE, request);
        }

        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST,
                "The request body is missing, is not valid JSON, or has a field of the wrong type");
        problem.setTitle("Malformed request body");
        JacksonException jackson = findCause(ex, JacksonException.class);
        if (jackson != null && !jackson.getPath().isEmpty()) {
            problem.setProperty("errors",
                    List.of(new FieldViolation(JsonPointers.from(jackson.getPath()), "invalid value or type")));
        }
        return handleExceptionInternal(ex, problem, headers, HttpStatus.BAD_REQUEST, request);
    }

    /**
     * Jackson and Spring each may wrap the original exception, so search the whole
     * cause chain.
     */
    private static <T extends Throwable> T findCause(Throwable throwable, Class<T> type) {
        for (Throwable t = throwable; t != null; t = t.getCause()) {
            if (type.isInstance(t)) {
                return type.cast(t);
            }
        }
        return null;
    }
}
