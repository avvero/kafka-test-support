package pw.avvero.test.kafka;

import java.util.List;

public interface RecordCaptorAccess {

    List<RecordSnapshot> getRecords(String topic, Object key);
}
