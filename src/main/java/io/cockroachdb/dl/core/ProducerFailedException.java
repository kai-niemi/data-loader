package io.cockroachdb.dl.core;

public class ProducerFailedException extends RuntimeException {
    public ProducerFailedException(String message, Throwable cause) {
        super(message, cause);
    }
}
