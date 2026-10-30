package io.highthroughput.ingest.web;

import jakarta.validation.ElementKind;
import jakarta.validation.Path;
import tools.jackson.core.JacksonException;

import java.util.List;

/**
 * Builds RFC 6901 JSON Pointers from Bean Validation and Jackson error paths.
 */
final class JsonPointers {

    private JsonPointers() {
    }

    /**
     * Bean Validation puts a list index or map key on the node <em>after</em> the
     * container, e.g. the path
     * {@code events[3].type} is the nodes {@code events} and
     * {@code type (index 3)}. Container-element nodes
     * ({@code <list element>}, {@code <map value>}) have no JSON name and are
     * skipped.
     */
    static String from(Path path) {
        StringBuilder pointer = new StringBuilder();
        for (Path.Node node : path) {
            if (node.isInIterable()) {
                if (node.getIndex() != null) {
                    pointer.append('/').append(node.getIndex());
                } else if (node.getKey() != null) {
                    pointer.append('/').append(escape(String.valueOf(node.getKey())));
                }
            }
            if (node.getKind() != ElementKind.CONTAINER_ELEMENT && node.getName() != null) {
                pointer.append('/').append(escape(node.getName()));
            }
        }
        return pointer.toString();
    }

    static String from(List<JacksonException.Reference> path) {
        StringBuilder pointer = new StringBuilder();
        for (JacksonException.Reference reference : path) {
            if (reference.getPropertyName() != null) {
                pointer.append('/').append(escape(reference.getPropertyName()));
            } else if (reference.getIndex() >= 0) {
                pointer.append('/').append(reference.getIndex());
            }
        }
        return pointer.toString();
    }

    private static String escape(String token) {
        return token.replace("~", "~0").replace("/", "~1");
    }
}
