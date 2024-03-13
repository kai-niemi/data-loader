package io.roach.volt.csv.listener;

import io.roach.volt.csv.event.CompletionEvent;
import io.roach.volt.csv.event.GenericEvent;
import io.roach.volt.csv.producer.ImportIntoHelper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * Completion event listener that generates a topologically ordered import file
 * for issuing IMPORT commands.
 */
@Component
public class ImportFileListener extends AbstractEventPublisher {
    @Autowired
    private ImportIntoHelper importCommand;

    @EventListener
    public void onCompletionEvent(GenericEvent<CompletionEvent> event) throws IOException {
        importCommand.generate(event.getTarget().getPaths());
    }
}