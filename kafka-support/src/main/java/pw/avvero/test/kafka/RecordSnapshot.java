package pw.avvero.test.kafka;

import lombok.Builder;
import lombok.Data;

import java.util.Map;

@Data
@Builder
public class RecordSnapshot {

    public RecordSnapshot(String topic, Object key, Map<String, Object> headers, Object value) {
        this.topic = topic;
        this.key = key;
        this.headers = headers;
        this.value = value;
    }

    private String topic;
    private Object key;
    private Map<String, Object> headers;
    private Object value;

}
