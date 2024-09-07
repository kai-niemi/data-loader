package io.roach.volt.web.csv;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
public class CsvStreamingException extends RuntimeException {
    public CsvStreamingException(String message) {
        super(message);
    }

    public CsvStreamingException(Throwable cause) {
        super(cause);
    }
}
