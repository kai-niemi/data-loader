package io.roach.volt.csv.listener;

import io.roach.volt.csv.event.ExitEvent;
import io.roach.volt.csv.event.GenericEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

@Component
public class ExitListener {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private ConfigurableApplicationContext applicationContext;

    @EventListener
    public void onExitEvent(GenericEvent<ExitEvent> event) {
        logger.trace("Received exit event code: " + event.getTarget().getExitCode());
        SpringApplication.exit(applicationContext, () -> 0);
    }
}
