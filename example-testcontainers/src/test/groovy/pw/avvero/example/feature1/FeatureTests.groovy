package pw.avvero.example.feature1

import org.skyscreamer.jsonassert.JSONAssert
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.ApplicationContext
import org.springframework.http.HttpMethod
import org.springframework.http.MediaType
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.ContextConfiguration
import org.springframework.test.web.client.MockRestServiceServer
import org.springframework.test.web.servlet.MockMvc
import org.springframework.web.client.RestTemplate
import pw.avvero.test.kafka.KafkaSupport
import pw.avvero.test.kafka.RecordCaptor
import pw.avvero.example.KafkaContainerConfiguration
import pw.avvero.example.RecordCaptorConfiguration
import pw.avvero.example.RequestCaptor
import spock.lang.Shared
import spock.lang.Specification

import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE
import static org.springframework.test.web.client.ExpectedCount.manyTimes
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status

@SpringBootTest
@ActiveProfiles(profiles = "test")
@AutoConfigureMockMvc
@ContextConfiguration(classes = [KafkaContainerConfiguration, RecordCaptorConfiguration])
@DirtiesContext
class FeatureTests extends Specification {

    @Autowired
    RecordCaptor recordCaptor
    @Autowired
    RestTemplate restTemplate
    @Autowired
    ApplicationContext applicationContext
    @Autowired
    MockMvc mockMvc
    @Shared
    MockRestServiceServer restMock

    def setup() {
        restMock = MockRestServiceServer.bindTo(restTemplate).ignoreExpectOrder(true).build()
    }

    def cleanup() {
        restMock.reset()
    }

    def "Can send event to topic and receive event from it"() {
        setup:
        KafkaSupport.waitForPartitionAssignment(applicationContext)
        and:
        def openaiRequestCaptor = new RequestCaptor()
        restMock.expect(manyTimes(), requestTo("https://api.openai.com/v1/chat/completions"))
                .andExpect(method(HttpMethod.POST))
                .andExpect(openaiRequestCaptor)
                .andRespond(withSuccess('{"content": "Hi, how can i help you?"}', MediaType.APPLICATION_JSON))
        and:
        def telegramRequestCaptor = new RequestCaptor()
        restMock.expect(manyTimes(), requestTo("https://api.telegram.org/sendMessage"))
                .andExpect(method(HttpMethod.POST))
                .andExpect(telegramRequestCaptor)
                .andRespond(withSuccess('{}', MediaType.APPLICATION_JSON))
        when:
        mockMvc.perform(post("/telegram/webhook")
                .contentType(APPLICATION_JSON_VALUE)
                .content("""{
                  "message": {
                    "from": {
                      "id": 10000000
                    },
                    "chat": {
                      "id": 20000000
                    },
                    "text": "Hello!"
                  }
                }""".toString())
                .accept(APPLICATION_JSON_VALUE))
                .andExpect(status().isOk())
        KafkaSupport.waitForPartitionOffsetCommit(applicationContext)
        then:
        openaiRequestCaptor.times == 1
        JSONAssert.assertEquals("""{
            "content": "Hello!"
        }""", openaiRequestCaptor.bodyString, false)
        and:
        telegramRequestCaptor.times == 1
        JSONAssert.assertEquals("""{
            "chatId": "20000000",
            "text": "Hi, how can i help you?"
        }""", telegramRequestCaptor.bodyString, false)
    }
}
