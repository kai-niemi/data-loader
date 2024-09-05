package io.roach.volt.csv;

public class ProducerFailedException extends RuntimeException {
    public ProducerFailedException(String message, Throwable cause) {
        super(message, cause);
    }
}
