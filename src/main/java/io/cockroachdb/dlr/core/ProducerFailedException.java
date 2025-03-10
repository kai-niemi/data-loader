package io.cockroachdb.dlr.core;

public class ProducerFailedException extends RuntimeException {
    public ProducerFailedException(String message, Throwable cause) {
        super(message, cause);
    }
}
