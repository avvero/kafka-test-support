package pw.avvero.example;

import org.springframework.http.HttpMethod;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.test.web.client.ResponseCreator;
import org.springframework.web.client.RestTemplate;

import static org.springframework.test.web.client.ExpectedCount.manyTimes;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;

public class RestExpectation {

    public interface OpenaiMock {
        RequestCaptor completions(ResponseCreator responseCreator);
    }

    public interface TelegramMock {
        RequestCaptor sendMessage(ResponseCreator responseCreator);
    }

    private final MockRestServiceServer mockServer;
    public final OpenaiMock openai;
    public final TelegramMock telegram;

    public RestExpectation(RestTemplate restTemplate) {
        mockServer = MockRestServiceServer.bindTo(restTemplate).ignoreExpectOrder(true).build();
        this.openai = responseCreator -> map(mockServer, "https://api.openai.com/v1/chat/completions", responseCreator);
        this.telegram = responseCreator -> map(mockServer, "https://api.telegram.org/sendMessage", responseCreator);
    }

    protected RequestCaptor map(MockRestServiceServer mockServer, String uri, ResponseCreator responseCreator) {
        RequestCaptor requestCaptor = new RequestCaptor();
        mockServer.expect(manyTimes(), requestTo(uri))
                .andExpect(method(HttpMethod.POST))
                .andExpect(requestCaptor)
                .andRespond(responseCreator);
        return requestCaptor;
    }

    public void cleanup() {
        mockServer.reset();
    }

}
