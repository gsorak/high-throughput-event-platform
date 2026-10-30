package io.highthroughput.ingest.web;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.web.filter.OncePerRequestFilter;
import org.springframework.web.servlet.HandlerExceptionResolver;

import java.io.IOException;
import java.util.concurrent.Semaphore;

/**
 * Admission control (ADR-0004): at most {@code maxInFlight} ingestion requests
 * are processed at once. A permit is
 * held for the whole request — including the wait for Kafka's acknowledgement —
 * and a request that finds none is
 * rejected at once with 503 and {@code Retry-After}, before its body is read.
 * This bounds memory and queued work
 * when Kafka slows down, instead of letting virtual threads pile up.
 */
public class AdmissionControlFilter extends OncePerRequestFilter {

    private final Semaphore permits;
    private final HandlerExceptionResolver exceptionResolver;

    public AdmissionControlFilter(int maxInFlight, HandlerExceptionResolver exceptionResolver) {
        this.permits = new Semaphore(maxInFlight);
        this.exceptionResolver = exceptionResolver;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        if (!permits.tryAcquire()) {
            exceptionResolver.resolveException(request, response, null, new ServerBusyException());
            return;
        }
        try {
            chain.doFilter(request, response);
        } finally {
            permits.release();
        }
    }
}
