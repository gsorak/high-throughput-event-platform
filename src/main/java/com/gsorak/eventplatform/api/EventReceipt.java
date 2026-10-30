package com.gsorak.eventplatform.api;

import java.util.UUID;

public record EventReceipt(UUID eventId, String status) {
}
