package pw.avvero.test.kafka;

import lombok.extern.slf4j.Slf4j;
import org.awaitility.Awaitility;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;

@Slf4j
public class RecordCaptor {

    private final Map<String, RecordSnapshotKey> keysRegister = new ConcurrentHashMap<>();
    private final Map<String, Map<String, Map<Object, List<RecordSnapshot>>>> topicKeyRecords = new TreeMap<>();

    public RecordCaptor() {
        registerKey("ID", record -> String.valueOf(record.getKey()));
    }

    public void registerKey(String name, RecordSnapshotKey recordSnapshotKey) {
        keysRegister.putIfAbsent(name, recordSnapshotKey);
    }

    public void capture(RecordSnapshot recordSnapshot) {
        log.debug("[KTS] Record captured for topic {} for key {}\n    Headers: {}\n    Value: {}", recordSnapshot.getTopic(),
                recordSnapshot.getKey(), recordSnapshot.getHeaders(), recordSnapshot.getValue());
        for (Map.Entry<String, RecordSnapshotKey> keyRegister : keysRegister.entrySet()) {
            String key = keyRegister.getValue().getValue(recordSnapshot);
            topicKeyRecords
                    .computeIfAbsent(recordSnapshot.getTopic(), k -> new ConcurrentHashMap<>())
                    .computeIfAbsent(keyRegister.getKey(), k -> new ConcurrentHashMap<>())
                    .computeIfAbsent(key, k -> new CopyOnWriteArrayList<>())
                    .add(recordSnapshot);
        }
    }

    public List<RecordSnapshot> getRecords(String topic, String key, Object keyValue) {
        return topicKeyRecords.getOrDefault(topic, Collections.emptyMap())
                .getOrDefault(key, Collections.emptyMap())
                .getOrDefault(keyValue, Collections.emptyList());
    }

    public List<RecordSnapshot> getRecords(String topic, Object value) {
        return getRecords(topic, "ID", value);
    }

    public List<RecordSnapshot> getRecords(String topic, Predicate<RecordSnapshot> predicate) {
        return topicKeyRecords.getOrDefault(topic, Collections.emptyMap())
                .values().stream()
                .flatMap(m -> m.values().stream())
                .flatMap(Collection::stream)
                .filter(predicate)
                .toList();
    }

    public void clear() {
        topicKeyRecords.clear();
    }
}
