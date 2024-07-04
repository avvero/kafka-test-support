package pw.avvero.test.kafka;

import lombok.extern.slf4j.Slf4j;
import org.awaitility.Awaitility;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@Slf4j
public class RecordCaptor implements RecordCaptorAccess {

    private final Map<String, Map<String, Map<Object, List<RecordSnapshot>>>> topicKeyRecords = new ConcurrentHashMap<>();

    public RecordCaptor() {
        registerKey("ID");
    }

    public void registerKey(String name) {

    }

    public void capture(RecordSnapshot recordSnapshot) {
        log.debug("[KTS] Record captured for topic {} for key {}\n    Headers: {}\n    Value: {}", recordSnapshot.getTopic(),
                recordSnapshot.getKey(), recordSnapshot.getHeaders(), recordSnapshot.getValue());
        topicKeyRecords.computeIfAbsent(recordSnapshot.getTopic(), k -> new ConcurrentHashMap<>())
                .computeIfAbsent("ID", k -> new ConcurrentHashMap<>())
                .computeIfAbsent(recordSnapshot.getKey() != null ? recordSnapshot.getKey() : "", k -> new CopyOnWriteArrayList<>())
                .add(recordSnapshot);
    }

    @Override
    public List<RecordSnapshot> getRecords(String topic, Object key) {
        return topicKeyRecords.getOrDefault(topic, Collections.emptyMap())
                .getOrDefault("ID", Collections.emptyMap())
                .getOrDefault(key, Collections.emptyList());
    }

    public List<RecordSnapshot> getRecords(String topic, Predicate<RecordSnapshot> predicate) {
        return topicKeyRecords.getOrDefault(topic, Collections.emptyMap())
                .values().stream()
                .flatMap(m -> m.values().stream())
                .flatMap(Collection::stream)
                .filter(predicate)
                .toList();
    }

    public RecordCaptorAccess awaitAtMost(int numberOrRecords, long millis) {
        RecordCaptor recordCaptor = this;
        return (topic, key) -> {
            Supplier<List<RecordSnapshot>> supplier = () -> recordCaptor.getRecords(topic, key);
            Awaitility.await()
                    .atMost(millis, TimeUnit.MILLISECONDS)
                    .pollInterval(50, TimeUnit.MILLISECONDS)
                    .until(() -> supplier.get().size() != numberOrRecords);
            return supplier.get();
        };
    }

    public void clear() {
        topicKeyRecords.clear();
    }
}
