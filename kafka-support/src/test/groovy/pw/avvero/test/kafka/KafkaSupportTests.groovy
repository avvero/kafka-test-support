package pw.avvero.test.kafka

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.ApplicationContext
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.Message
import org.springframework.messaging.support.MessageBuilder
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.ContextConfiguration
import spock.lang.Specification

@SpringBootTest
@ActiveProfiles(profiles = "test")
@AutoConfigureMockMvc
@ContextConfiguration(classes = [TestApplication, KafkaContainerConfiguration])
@DirtiesContext
class KafkaSupportTests extends Specification {

    @Autowired
    RecordCaptor recordCaptor
    @Autowired
    KafkaTemplate<Object, Object> kafkaTemplate
    @Autowired
    ApplicationContext applicationContext

    def "Captor returns empty list if there is no records for key"() {
        expect:
        recordCaptor.getRecords("topic1", IdGenerator.next) == []
    }

    def "Can send message to topic and receive message from it"() {
        setup:
        KafkaSupport.waitForPartitionAssignment(applicationContext)
        def key = IdGenerator.next
        when:
        Message message = MessageBuilder
                .withPayload("value1")
                .setHeader(KafkaHeaders.TOPIC, "topic1")
                .setHeader("customHeader", "header1")
                .setHeader("customHeader2", 1)
                .setHeader(KafkaHeaders.KEY, key)
                .build()
        kafkaTemplate.send(message).get()
        KafkaSupport.waitForPartitionOffsetCommit(applicationContext)
        then:
        recordCaptor.getRecords("topic1", key).last.headers["customHeader"] == "header1"
        recordCaptor.getRecords("topic1", key).last.headers["customHeader2"] == 1
        recordCaptor.getRecords("topic1", key).last.value == "value1"
        and:
        recordCaptor.getRecords("topic1", IdGenerator.next) == []
    }

    def "Can send many messages to the same topic and receive them from it"() {
        setup:
        KafkaSupport.waitForPartitionAssignment(applicationContext)
        def key = IdGenerator.getNext()
        when:
        n.times {
            Message message = MessageBuilder
                    .withPayload("value" + it)
                    .setHeader(KafkaHeaders.TOPIC, "topic1")
                    .setHeader(KafkaHeaders.KEY, key)
                    .build()
            kafkaTemplate.send(message).get()
        }
        KafkaSupport.waitForPartitionOffsetCommit(applicationContext)
        then:
        recordCaptor.getRecords("topic1", key).size() == n
        where:
        n = 1000
    }

    def "Can send many messages in N threads to the same topic and receive them from it"() {
        setup:
        KafkaSupport.waitForPartitionAssignment(applicationContext)
        def key = IdGenerator.getNext()
        when:
        10.times {
            def tread = new Thread(new Runnable() {
                @Override
                void run() {
                    1000.times {
                        Message message = MessageBuilder
                                .withPayload("value" + it)
                                .setHeader(KafkaHeaders.TOPIC, "topic1")
                                .setHeader(KafkaHeaders.KEY, key)
                                .build()
                        kafkaTemplate.send(message).get()
                    }
                }
            })
            tread.start()
            tread.join()
        }
        KafkaSupport.waitForPartitionOffsetCommit(applicationContext)
        then:
        recordCaptor.getRecords("topic1", key).size() == 10000
    }

    def "Can send many messages to the different topics and receive them"() {
        setup:
        KafkaSupport.waitForPartitionAssignment(applicationContext)
        def key = IdGenerator.getNext()
        when:
        n.times {
            Message message = MessageBuilder
                    .withPayload("value" + it)
                    .setHeader(KafkaHeaders.TOPIC, "topic" + it)
                    .setHeader(KafkaHeaders.KEY, key)
                    .build()
            kafkaTemplate.send(message).get()
        }
        KafkaSupport.waitForPartitionOffsetCommit(applicationContext)
        then:
        n.times {
            assert recordCaptor.getRecords("topic" + it, key).size() == 1
        }
        where:
        n = 10
    }

    def "Message from the topic which is not captured can't be acquired"() {
        setup:
        KafkaSupport.waitForPartitionAssignment(applicationContext)
        def key = IdGenerator.getNext()
        when:
        Message message = MessageBuilder
                .withPayload("value")
                .setHeader(KafkaHeaders.TOPIC, topic)
                .setHeader(KafkaHeaders.KEY, key)
                .build()
        kafkaTemplate.send(message).get()
        KafkaSupport.waitForPartitionOffsetCommit(applicationContext)
        then:
        noExceptionThrown()
        and:
        recordCaptor.getRecords(topic, key).size() == 0
        where:
        topic = "topic_UNEXPECTED"
    }
}
