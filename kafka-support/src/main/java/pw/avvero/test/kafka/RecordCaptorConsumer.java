package pw.avvero.test.kafka;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.stereotype.Component;

import java.util.Map;

@Data
@Slf4j
@Component
@RequiredArgsConstructor
public class RecordCaptorConsumer {

    private final RecordCaptor recordCaptor;
    private final RecordSnapshotMapper mapper = new RecordSnapshotMapper();

    /**
     * Consume a {@link ConsumerRecord}.
     *
     * @param record         The Kafka {@link ConsumerRecord} to be captured.
     * @param boundedHeaders A map of headers provided by Spring that includes both
     *                       user-defined headers and potentially extraneous system headers.
     *                       unrelated or system headers.
     */
    @KafkaListener(id = "recordCaptor", topics = "#{'${test.record-captor.topics}'.split(',')}", groupId = "test")
    public void eventCaptorListener(ConsumerRecord<Object, Object> record,
                                    @Headers Map<String, Object> boundedHeaders) {
        RecordSnapshot recordSnapshot = mapper.recordToSnapshot(record, boundedHeaders);
        recordCaptor.capture(recordSnapshot);
    }
}
