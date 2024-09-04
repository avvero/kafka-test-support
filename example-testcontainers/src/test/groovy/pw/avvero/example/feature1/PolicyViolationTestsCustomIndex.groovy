package pw.avvero.example.feature1

import groovy.json.JsonSlurper
import org.skyscreamer.jsonassert.JSONAssert
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.ApplicationContext
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.ContextConfiguration
import org.springframework.test.web.servlet.MockMvc
import org.springframework.web.client.RestTemplate
import pw.avvero.example.KafkaContainerConfiguration
import pw.avvero.example.RecordCaptorConfiguration
import pw.avvero.example.RestExpectation
import pw.avvero.test.kafka.KafkaSupport
import pw.avvero.test.kafka.RecordCaptor
import spock.lang.Shared
import spock.lang.Specification

import java.util.function.Function

import static org.springframework.http.MediaType.APPLICATION_JSON
import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE
import static org.springframework.test.web.client.response.MockRestResponseCreators.withBadRequest
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status

@SpringBootTest
@ActiveProfiles(profiles = "test")
@AutoConfigureMockMvc
@ContextConfiguration(classes = [KafkaContainerConfiguration, RecordCaptorConfiguration])
@DirtiesContext
class PolicyViolationTestsCustomIndex extends Specification {

    @Autowired
    RecordCaptor recordCaptor
    @Autowired
    RestTemplate restTemplate
    @Autowired
    ApplicationContext applicationContext
    @Autowired
    MockMvc mockMvc
    @Shared
    RestExpectation restExpectation

    def setup() {
        restExpectation = new RestExpectation(restTemplate)
        def slurper = new JsonSlurper()
        recordCaptor.registerIndex("errorCode", { r -> return slurper.parseText(r.value).error?.code } as Function)
    }

    def cleanup() {
        restExpectation.cleanup()
        recordCaptor.clear()
    }

    def "User Message Processing with OpenAI"() {
        setup:
        KafkaSupport.waitForPartitionAssignment(applicationContext)                           // 1
        and:                                                                                  // 2
        def openaiRequestCaptor = restExpectation.openai.completions(withBadRequest().contentType(APPLICATION_JSON)
                .body("""{
                  "error": {
                    "code": "content_policy_violation",
                    "message": "Your request was rejected as a result of our safety system."
                  }
                }"""))
        def telegramRequestCaptor = restExpectation.telegram.sendMessage(withSuccess('{}', APPLICATION_JSON))
        when:
        mockMvc.perform(post("/telegram/webhook")                                             // 3
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
        KafkaSupport.waitForPartitionOffsetCommit(applicationContext)                         // 4
        then:
        openaiRequestCaptor.times == 1                                                        // 5
        JSONAssert.assertEquals("""{
            "content": "Hello!"
        }""", openaiRequestCaptor.bodyString, false)
        and:
        telegramRequestCaptor.times == 1
        JSONAssert.assertEquals("""{
            "chatId": "20000000",
            "text": "Your request was rejected as a result of our safety system."
        }""", telegramRequestCaptor.bodyString, false)
        when:                                                                                 // 6
        def message = recordCaptor.getRecords("topicC", "errorCode", "content_policy_violation").last
        then:
        message != null
        JSONAssert.assertEquals("""{
          "webhookMessage": {
            "message": {
              "chat": {
                "id": "20000000"
              },
              "text": "Hello!"
            }
          },
          "error": {
            "code": "content_policy_violation",
            "message": "Your request was rejected as a result of our safety system."
          }
        }""", message.value as String, false)
    }
}
