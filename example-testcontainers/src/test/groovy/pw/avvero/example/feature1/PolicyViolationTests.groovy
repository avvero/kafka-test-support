package pw.avvero.example.feature1

import org.skyscreamer.jsonassert.JSONAssert
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.ApplicationContext
import org.springframework.http.HttpMethod
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.ContextConfiguration
import org.springframework.test.web.client.MockRestServiceServer
import org.springframework.test.web.servlet.MockMvc
import org.springframework.web.client.RestTemplate
import pw.avvero.example.KafkaContainerConfiguration
import pw.avvero.example.RecordCaptorConfiguration
import pw.avvero.example.RequestCaptor
import pw.avvero.test.kafka.KafkaSupport
import pw.avvero.test.kafka.RecordCaptor
import spock.lang.Shared
import spock.lang.Specification

import static org.springframework.http.MediaType.APPLICATION_JSON
import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE
import static org.springframework.test.web.client.ExpectedCount.manyTimes
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo
import static org.springframework.test.web.client.response.MockRestResponseCreators.withBadRequest
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status

@SpringBootTest
@ActiveProfiles(profiles = "test")
@AutoConfigureMockMvc
@ContextConfiguration(classes = [KafkaContainerConfiguration, RecordCaptorConfiguration])
@DirtiesContext
class PolicyViolationTests extends Specification {

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

    def "User Message Processing with OpenAI"() {
        setup:
        KafkaSupport.waitForPartitionAssignment(applicationContext)                           // 1
        and:
        def openaiRequestCaptor = new RequestCaptor()
        restMock.expect(manyTimes(), requestTo("https://api.openai.com/v1/chat/completions")) // 2
                .andExpect(method(HttpMethod.POST))
                .andExpect(openaiRequestCaptor)
                .andRespond(withBadRequest().contentType(APPLICATION_JSON).body("""{
                                              "error": {
                                                "code": "content_policy_violation",
                                                "message": "Your request was rejected as a result of our safety system."
                                              }
                                            }"""))
        and:
        def telegramRequestCaptor = new RequestCaptor()
        restMock.expect(manyTimes(), requestTo("https://api.telegram.org/sendMessage"))       // 3
                .andExpect(method(HttpMethod.POST))
                .andExpect(telegramRequestCaptor)
                .andRespond(withSuccess('{}', APPLICATION_JSON))
        when:
        mockMvc.perform(post("/telegram/webhook")                                             // 4
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
        KafkaSupport.waitForPartitionOffsetCommit(applicationContext)                         // 5
        then:
        openaiRequestCaptor.times == 1                                                        // 6
        JSONAssert.assertEquals("""{
            "content": "Hello!"
        }""", openaiRequestCaptor.bodyString, false)
        and:
        telegramRequestCaptor.times == 1
        JSONAssert.assertEquals("""{
            "chatId": "20000000",
            "text": "Your request was rejected as a result of our safety system."
        }""", telegramRequestCaptor.bodyString, false)
        when:
        def message = recordCaptor.getRecords("topicC", "20000000").last
        then:
        message != null
    }
}
