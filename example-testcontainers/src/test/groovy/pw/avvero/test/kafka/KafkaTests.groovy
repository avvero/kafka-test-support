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
class KafkaTests extends Specification {

    @Autowired
    RecordCaptor recordCaptor
    @Autowired
    KafkaTemplate<Object, Object> kafkaTemplate
    @Autowired
    ApplicationContext applicationContext

    def "Can send event to topic and receive event from it"() {
        setup:
        KafkaSupport.waitForPartitionAssignment(applicationContext)
        when:
        Message message = MessageBuilder
                .withPayload("value1")
                .setHeader(KafkaHeaders.TOPIC, "topic1")
                .setHeader("customHeader", "header1")
                .setHeader("customHeader2", 1)
                .build()
        kafkaTemplate.send(message).get()
        KafkaSupport.waitForPartitionOffsetCommit(applicationContext)
        then:
        recordCaptor.getRecords("topic1").last.headers["customHeader"] == "header1"
        recordCaptor.getRecords("topic1").last.value == "value1"
    }
}
