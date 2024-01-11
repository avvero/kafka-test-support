package pw.avvero.emk_live

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
import org.springframework.test.context.TestPropertySource
import pw.avvero.emk.KafkaSupport
import pw.avvero.emk_container.Consumer
import pw.avvero.emk_container.TestApplication
import spock.lang.Ignore
import spock.lang.Specification

@SpringBootTest
@ActiveProfiles(profiles = "test")
@AutoConfigureMockMvc
@ContextConfiguration(classes = [TestApplication])
@TestPropertySource(properties = [
        "spring.kafka.bootstrap-servers=localhost:9092"
])
@DirtiesContext
@Ignore("For Reachability Metadata purpose only")
class KafkaContainerTests extends Specification {

    @Autowired
    Consumer consumer
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
                .build()
        kafkaTemplate.send(message).get()
        Thread.sleep(2000) // TODO
        then:
        consumer.events == ["value1"]
    }
}
