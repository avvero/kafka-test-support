package pw.avvero.test.kafka;

import java.util.List;

public interface RecordCaptorAccess {

    List<Object> getRecords(String topic, Object key);
}
