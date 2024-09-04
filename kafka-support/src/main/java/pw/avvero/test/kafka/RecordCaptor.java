package pw.avvero.test.kafka;

import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * The RecordCaptor class is responsible for capturing and storing Kafka record snapshots based on their topics and keys.
 * It provides methods to register custom keys for filtering and retrieving records based on specific criteria.
 */
@Slf4j
public class RecordCaptor {

    private final Map<String, Function<RecordSnapshot, Comparable<?>>> indexRegister = new ConcurrentHashMap<>();
    private static final String DEFAULT_KEY = "MESSAGE_KEY";
    private final Map<String, Map<String, Map<Object, List<RecordSnapshot>>>> topicKeyRecords = new TreeMap<>();

    /**
     * Constructs a RecordCaptor instance and registers a default key "MESSAGE_KEY" to capture records.
     */
    public RecordCaptor() {
        registerIndex(DEFAULT_KEY, record -> String.valueOf(record.getKey()));
    }

    /**
     * Registers a custom index with the specified name for capturing and categorizing records.
     *
     * @param name the name of the index.
     * @param recordSnapshotKey a function to extract the key value from a record snapshot.
     */
    public void registerIndex(String name, Function<RecordSnapshot, Comparable<?>> recordSnapshotKey) {
        indexRegister.putIfAbsent(name, recordSnapshotKey);
    }

    /**
     * Captures a RecordSnapshot and stores it based on its topic and the registered keys.
     *
     * @param recordSnapshot the RecordSnapshot to capture.
     */
    public void capture(RecordSnapshot recordSnapshot) {
        log.debug("[KTS] Record captured for topic {} for key {}\n    Headers: {}\n    Value: {}", recordSnapshot.getTopic(),
                recordSnapshot.getKey(), recordSnapshot.getHeaders(), recordSnapshot.getValue());
        for (Map.Entry<String, Function<RecordSnapshot, Comparable<?>>> keyRegister : indexRegister.entrySet()) {
            Comparable<?> key = keyRegister.getValue().apply(recordSnapshot);
            topicKeyRecords
                    .computeIfAbsent(recordSnapshot.getTopic(), k -> new ConcurrentHashMap<>())
                    .computeIfAbsent(keyRegister.getKey(), k -> new ConcurrentHashMap<>())
                    .computeIfAbsent(key, k -> new CopyOnWriteArrayList<>())
                    .add(recordSnapshot);
        }
    }

    /**
     * Retrieves records for the specified topic, key, and key value.
     *
     * @param topic the topic of the records.
     * @param keyName the registered key name.
     * @param keyValue the value of the key to filter records.
     * @return a list of RecordSnapshots matching the specified criteria.
     */
    public List<RecordSnapshot> getRecords(String topic, String keyName, Object keyValue) {
        return topicKeyRecords.getOrDefault(topic, Collections.emptyMap())
                .getOrDefault(keyName, Collections.emptyMap())
                .getOrDefault(keyValue, Collections.emptyList());
    }

    /**
     * Retrieves records for the specified topic using the default key "MESSAGE_KEY".
     *
     * @param topic the topic of the records.
     * @param value the value of the default key to filter records.
     * @return a list of RecordSnapshots matching the specified topic and key value.
     */
    public List<RecordSnapshot> getRecords(String topic, Object value) {
        return getRecords(topic, DEFAULT_KEY, value);
    }

    /**
     * Retrieves records for the specified topic that match the provided predicate.
     *
     * @param topic the topic of the records.
     * @param predicate a predicate to filter records.
     * @return a list of RecordSnapshots matching the specified topic and predicate.
     */
    public List<RecordSnapshot> getRecords(String topic, Predicate<RecordSnapshot> predicate) {
        return topicKeyRecords.getOrDefault(topic, Collections.emptyMap())
                .values().stream()
                .flatMap(m -> m.values().stream())
                .flatMap(Collection::stream)
                .filter(predicate)
                .toList();
    }

    /**
     * Clears all captured records from the internal data structure.
     */
    public void clear() {
        topicKeyRecords.clear();
    }
}
