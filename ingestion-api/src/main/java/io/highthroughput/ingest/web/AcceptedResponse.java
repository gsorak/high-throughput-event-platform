package io.highthroughput.ingest.web;

/**
 * Body of a 202 response: how many events Kafka acknowledged. Always the full
 * batch size.
 */
public record AcceptedResponse(int accepted) {
}
