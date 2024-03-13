package io.roach.volt.csv.producer;

import io.roach.volt.csv.model.Column;
import io.roach.volt.csv.model.Ref;

import java.util.LinkedHashMap;
import java.util.Map;

public class BlockingMapX {
    /*
List<CompletableFuture<Object>> allFutures = new ArrayList<>();
Future<Void> f= CompletableFuture.runAsync(() -> {

});
//                CompletableFuture.allOf(allFutures.toArray(new CompletableFuture[] {})).join();
CompletableFuture<List<Object>> future = ConcurrencyUtils.joinAndMapResults(allFutures);
List<Object> fi = future.join();
*/

//    public void dd() {
//        Map<String, Object> orderedValues = new LinkedHashMap<>();
//
//        for (Column col : getTable().filterColumns(column -> true)) {
//            Object v;
//            if (col.getEach() != null) {
//                v = upstreamValues.get(each.getColumn());
//            } else {
//                Ref ref = col.getRef();
//                if (ref != null) {
//                    if (ref.getName().equals(each.getName())) {
//                        v = upstreamValues.get(ref.getColumn());
//                    } else {
//                        Map<String, Object> values =
//                                refValues.computeIfAbsent(ref.getName(), getFifoQueue()::selectRandom);
//                        v = values.get(ref.getColumn());
//                    }
//                } else {
//                    v = getColumnGenerators().get(col).nextValue();
//                }
//            }
//
//            orderedValues.put(col.getName(), v);
//        }
//    }
}
