package io.cockroachdb.dl.core.event;

/**
 * Event published when all producers completed successfully.
 */
public class ProducersCompletedEvent {
    private final boolean quit;

    public ProducersCompletedEvent(boolean quit) {
        this.quit = quit;
    }

    public boolean isQuit() {
        return quit;
    }
}
