package io.roach.volt.csv.event;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEventPublisher;

public abstract class AbstractEventPublisher {
    @Autowired
    private ApplicationEventPublisher applicationEventPublisher;

    protected <T> void publishEvent(T event) {
        applicationEventPublisher.publishEvent(GenericEvent.of(this, event));
    }
}
