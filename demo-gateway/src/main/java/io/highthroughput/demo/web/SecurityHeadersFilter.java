package io.highthroughput.demo.web;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.http.MediaType;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.Set;

/**
 * Security headers on every response, and a guard for state-changing demo requests: they must carry
 * {@value #CLIENT_HEADER}, which a cross-site form or image cannot send. With the {@code SameSite=Strict} cookie this
 * stops cross-site request forgery.
 */
public class SecurityHeadersFilter extends OncePerRequestFilter {

    static final String CLIENT_HEADER = "X-Demo-Client";
    private static final Set<String> SAFE_METHODS = Set.of("GET", "HEAD", "OPTIONS");
    private static final String CSP = "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; "
            + "connect-src 'self'; object-src 'none'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'";

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        response.setHeader("Content-Security-Policy", CSP);
        response.setHeader("X-Content-Type-Options", "nosniff");
        response.setHeader("X-Frame-Options", "DENY");
        response.setHeader("Referrer-Policy", "no-referrer");
        response.setHeader("Cross-Origin-Opener-Policy", "same-origin");

        String path = request.getRequestURI().substring(request.getContextPath().length());
        if (path.equals("/demo") || path.startsWith("/demo/")) {
            response.setHeader("Cache-Control", "no-store");
            if (!SAFE_METHODS.contains(request.getMethod()) && !"1".equals(request.getHeader(CLIENT_HEADER))) {
                response.setStatus(HttpServletResponse.SC_FORBIDDEN);
                response.setContentType(MediaType.APPLICATION_PROBLEM_JSON_VALUE);
                response.getWriter().write("{\"type\":\"about:blank\",\"title\":\"Forbidden\",\"status\":403,"
                        + "\"detail\":\"Missing " + CLIENT_HEADER + " header.\"}");
                return;
            }
        }
        chain.doFilter(request, response);
    }
}
