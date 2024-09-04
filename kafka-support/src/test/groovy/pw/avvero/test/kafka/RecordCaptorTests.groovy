package pw.avvero.test.kafka

import groovy.json.JsonSlurper
import spock.lang.Specification
import spock.lang.Unroll

import java.util.function.Function
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
        captor.getRecords("topic1", "MESSAGE_KEY", key).size() == records
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
        captor.getRecords("topic1", { r -> r.headers[headerName] == headerValue } as Predicate).size() == records
        where:
        headerName | headerValue || records
        "header1"  | "value1"    || 1
        "header0"  | "value1"    || 0
        "header1"  | "value0"    || 0

    }

    @Unroll
    def "Find captured record by indexed key"() {
        setup:
        def slurper = new JsonSlurper()
        def captor = new RecordCaptor()
        captor.registerIndex("headerId", { r -> return r.headers["id"] } as Function)
        captor.registerIndex("header1", { r -> return r.headers["header1"] } as Function)
        captor.registerIndex("currency", { r -> return slurper.parseText(r.value).currency } as Function)
        when:
        captor.capture(new RecordSnapshot("topic1", null, ["header1": "value1", "id": 1], '{"currency": "USD", "amount": "1.00"}'))
        captor.capture(new RecordSnapshot("topic1", null, ["header1": "value1", "id": 2], '{"currency": "RUB", "amount": "1.00"}'))
        captor.capture(new RecordSnapshot("topic1", null, ["header1": "value2", "id": 3], '{"currency": "RUB", "amount": "1.00"}'))
        then:
        captor.getRecords("topic1", "headerId", 1).headers['id'] == [1]
        captor.getRecords("topic1", "headerId", 2).headers['id'] == [2]
        captor.getRecords("topic1", "headerId", 3).headers['id'] == [3]
        captor.getRecords("topic1", "header1", "value1").headers['id'] == [1, 2]
        captor.getRecords("topic1", "currency", "USD").headers['id'] == [1]
        captor.getRecords("topic1", "currency", "RUB").headers['id'] == [2, 3]
    }
}
