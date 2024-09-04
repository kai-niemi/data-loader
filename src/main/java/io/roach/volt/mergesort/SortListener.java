package io.roach.volt.mergesort;

import io.roach.volt.csv.event.GenericEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Component
public class SortListener {
    @EventListener
    public CompletableFuture<Void> onMergeEvent(GenericEvent<ExternalMerge> event) {
        try {
            event.getTarget().merge();
            return CompletableFuture.completedFuture(null);
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @EventListener
    public CompletableFuture<Void> onSortEvent(GenericEvent<ExternalMergeSort> event) {
        try {
            event.getTarget().sort();
            return CompletableFuture.completedFuture(null);
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @EventListener
    public CompletableFuture<Void> onSplitEvent(GenericEvent<ExternalSplit> event) {
        try {
            event.getTarget().split();
            return CompletableFuture.completedFuture(null);
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }
}
