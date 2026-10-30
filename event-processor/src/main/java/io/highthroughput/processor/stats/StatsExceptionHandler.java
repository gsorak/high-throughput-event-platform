package io.highthroughput.processor.stats;

import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.security.core.AuthenticationException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

/**
 * Maps failures to RFC 9457 {@code application/problem+json}. The base class
 * covers Spring MVC's own errors,
 * e.g. a missing or unparsable {@code from}/{@code to} parameter (400).
 */
@RestControllerAdvice
public class StatsExceptionHandler extends ResponseEntityExceptionHandler {

    @ExceptionHandler(InvalidStatsQueryException.class)
    ProblemDetail invalidQuery(InvalidStatsQueryException e) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, e.getMessage());
        problem.setTitle("Invalid stats query");
        return problem;
    }

    @ExceptionHandler(AuthenticationException.class)
    ProblemDetail unauthenticated(AuthenticationException e) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.UNAUTHORIZED,
                "A valid bearer token is required; see the WWW-Authenticate header for the reason");
        problem.setTitle("Unauthorized");
        return problem;
    }
}
