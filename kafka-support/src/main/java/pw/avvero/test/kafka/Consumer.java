package pw.avvero.test.kafka;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Data
@Slf4j
@Component
@AllArgsConstructor
public class Consumer {

    private final RecordCaptor recordCaptor;

    @KafkaListener(id = "recordCaptor", topics = "#{'${test.record-captor.topics}'.split(',')}", groupId = "test")
    public void eventCaptorListener(ConsumerRecord<Object, Object> record) {
        recordCaptor.capture(record);
    }
}
