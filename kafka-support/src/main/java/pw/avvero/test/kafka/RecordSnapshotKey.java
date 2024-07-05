package pw.avvero.test.kafka;

public interface RecordSnapshotKey {

    String getValue(RecordSnapshot record);

}