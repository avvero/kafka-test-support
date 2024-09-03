package pw.avvero.test.kafka;

import lombok.extern.slf4j.Slf4j;
import org.awaitility.Awaitility;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * The RecordCaptor class is responsible for capturing and storing Kafka record snapshots based on their topics and keys.
 * It provides methods to register custom keys for filtering and retrieving records based on specific criteria.
 */
@Slf4j
public class RecordCaptor {

    private final Map<String, Function<RecordSnapshot, Comparable<?>>> keysRegister = new ConcurrentHashMap<>();
    private static final String DEFAULT_KEY = "MESSAGE_KEY";
    private final Map<String, Map<String, Map<Object, List<RecordSnapshot>>>> topicKeyRecords = new TreeMap<>();

    /**
     * Constructs a RecordCaptor instance and registers a default key "ID" to capture records based on their keys.
     */
    public RecordCaptor() {
        registerKey(DEFAULT_KEY, record -> String.valueOf(record.getKey()));
    }

    /**
     * Registers a custom key with the specified name and RecordSnapshotKey implementation.
     * This allows capturing and categorizing records based on custom key criteria.
     *
     * @param name the name of the custom key.
     * @param recordSnapshotKey the RecordSnapshotKey implementation used to extract the key value from a record.
     */
    public void registerKey(String name, Function<RecordSnapshot, Comparable<?>> recordSnapshotKey) {
        keysRegister.putIfAbsent(name, recordSnapshotKey);
    }

    /**
     * Captures a RecordSnapshot and stores it in an internal data structure based on its topic and registered keys.
     *
     * @param recordSnapshot the RecordSnapshot to be captured.
     */
    public void capture(RecordSnapshot recordSnapshot) {
        log.debug("[KTS] Record captured for topic {} for key {}\n    Headers: {}\n    Value: {}", recordSnapshot.getTopic(),
                recordSnapshot.getKey(), recordSnapshot.getHeaders(), recordSnapshot.getValue());
        for (Map.Entry<String, Function<RecordSnapshot, Comparable<?>>> keyRegister : keysRegister.entrySet()) {
            Comparable<?> key = keyRegister.getValue().apply(recordSnapshot);
            topicKeyRecords
                    .computeIfAbsent(recordSnapshot.getTopic(), k -> new ConcurrentHashMap<>())
                    .computeIfAbsent(keyRegister.getKey(), k -> new ConcurrentHashMap<>())
                    .computeIfAbsent(key, k -> new CopyOnWriteArrayList<>())
                    .add(recordSnapshot);
        }
    }

    /**
     * Retrieves a list of RecordSnapshots for the specified topic, key, and key value.
     *
     * @param topic the topic of the records.
     * @param keyName the registered key name.
     * @param keyValue the value of the key to filter records.
     * @return a list of RecordSnapshots matching the specified topic, key, and key value.
     */
    public List<RecordSnapshot> getRecords(String topic, String keyName, Object keyValue) {
        return topicKeyRecords.getOrDefault(topic, Collections.emptyMap())
                .getOrDefault(keyName, Collections.emptyMap())
                .getOrDefault(keyValue, Collections.emptyList());
    }

    /**
     * Retrieves a list of RecordSnapshots for the specified topic and key value using the default message key.
     *
     * @param topic the topic of the records.
     * @param value the value of the key to filter records.
     * @return a list of RecordSnapshots matching the specified topic and key value.
     */
    public List<RecordSnapshot> getRecords(String topic, Object value) {
        return getRecords(topic, DEFAULT_KEY, value);
    }

    /**
     * Retrieves a list of RecordSnapshots for the specified topic that match the provided predicate.
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
