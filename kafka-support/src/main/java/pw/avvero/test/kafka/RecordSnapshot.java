package pw.avvero.test.kafka;

import lombok.Builder;
import lombok.Data;

import java.util.Map;

@Data
@Builder
public class RecordSnapshot {

    private String topic;
    private Object key;
    private Map<String, Object> headers;
    private Object value;

}
