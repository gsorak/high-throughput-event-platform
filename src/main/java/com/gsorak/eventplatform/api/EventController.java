package com.gsorak.eventplatform.api;

import com.gsorak.eventplatform.service.EventIngestionService;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.Valid;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.CompletableFuture;

@RestController
@RequestMapping("/api/v1/events")
public class EventController {

    private final EventIngestionService ingestionService;

    public EventController(EventIngestionService ingestionService) {
        this.ingestionService = ingestionService;
    }

    @PostMapping
    public CompletableFuture<ResponseEntity<EventReceipt>> ingest(
            @Valid @RequestBody EventRequest event,
            @RequestHeader(name = "X-Client-Id", required = false) String clientId,
            HttpServletRequest request
    ) {
        String identity = clientId == null || clientId.isBlank()
                ? request.getRemoteAddr()
                : clientId.trim();
        if (identity.length() > 128) {
            return CompletableFuture.completedFuture(ResponseEntity.badRequest().build());
        }

        return ingestionService.ingest(event, identity).thenApply(result -> switch (result) {
            case ACCEPTED -> ResponseEntity.accepted()
                    .body(new EventReceipt(event.eventId(), "accepted"));
            case RATE_LIMITED -> ResponseEntity.status(429)
                    .header(HttpHeaders.RETRY_AFTER, "1")
                    .body(new EventReceipt(event.eventId(), "rate_limited"));
            case UNAVAILABLE -> ResponseEntity.status(503)
                    .body(new EventReceipt(event.eventId(), "unavailable"));
        });
    }
}
