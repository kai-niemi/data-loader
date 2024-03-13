package io.roach.volt.csv.listener;

import io.roach.volt.csv.event.GenericEvent;
import io.roach.volt.shell.support.AnsiConsole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEventPublisher;

public abstract class AbstractEventPublisher {
    protected final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    protected AnsiConsole console;

    @Autowired
    private ApplicationEventPublisher applicationEventPublisher;

    protected <T> void publishEvent(T event) {
        applicationEventPublisher.publishEvent(GenericEvent.of(this, event));
    }
}
