package pw.avvero.test.kafka

import spock.lang.Specification
import spock.lang.Unroll

import java.util.function.Predicate

class RecordCaptorTests extends Specification {

    def "No records are captured"() {
        setup:
        def captor = new RecordCaptor()
        expect:
        captor.getRecords("topic1", "1").size() == 0
    }

    @Unroll
    def "Find captured record by ID"() {
        setup:
        def captor = new RecordCaptor()
        when:
        captor.capture(RecordSnapshot.builder().topic("topic1").key("1").build())
        then:
        captor.getRecords("topic1", key).size() == records
        where:
        key || records
        "1" || 1
        "0" || 0
    }

    @Unroll
    def "Find captured record by header filter"() {
        setup:
        def captor = new RecordCaptor()
        when:
        captor.capture(RecordSnapshot.builder().topic("topic1").headers(["header1": "value1"]).build())
        then:
        captor.getRecords("topic1", predicateHeader(headerName, headerValue)).size() == records
        where:
        headerName | headerValue || records
        "header1"  | "value1"    || 1
        "header0"  | "value1"    || 0
        "header1"  | "value0"    || 0

    }

    @Unroll
    def "Find captured record by header key"() {
        setup:
        def captor = new RecordCaptor()
        def header1Key = new RecordSnapshotKey(){
            @Override
            String getValue(RecordSnapshot record) {
                return String.valueOf(record.headers["header1"])
            }
        }
        captor.registerKey("header1", header1Key)
        when:
        captor.capture(RecordSnapshot.builder().topic("topic1").headers(["header1": "value1"]).build())
        then:
        captor.getRecords("topic1", headerName, headerValue).size() == records
        where:
        headerName | headerValue || records
        "header1"  | "value1"    || 1
        "header0"  | "value1"    || 0
        "header1"  | "value0"    || 0

    }

    def predicateHeader(Object headerName, Object headerValue) {
        return new Predicate<RecordSnapshot>() {
            @Override
            boolean test(RecordSnapshot record) {
                return record.headers[headerName] == headerValue
            }
        }
    }
}
