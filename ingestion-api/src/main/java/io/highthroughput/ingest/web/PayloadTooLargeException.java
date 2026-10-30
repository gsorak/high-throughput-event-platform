package io.highthroughput.ingest.web;

import java.io.IOException;

/**
 * Thrown while reading a request body that exceeds the configured limit. It is
 * an {@link IOException} because it
 * surfaces from {@code InputStream.read}, where only I/O errors are allowed.
 */
public class PayloadTooLargeException extends IOException {

    private final long maxBytes;

    public PayloadTooLargeException(long maxBytes) {
        super("Request body exceeds " + maxBytes + " bytes");
        this.maxBytes = maxBytes;
    }

    public long maxBytes() {
        return maxBytes;
    }
}
