package pw.avvero.test.kafka.example;

import groovy.json.JsonSlurper;
import lombok.Getter;
import org.awaitility.Awaitility;
import org.springframework.http.HttpHeaders;
import org.springframework.http.client.ClientHttpRequest;
import org.springframework.mock.http.client.MockClientHttpRequest;
import org.springframework.test.web.client.RequestMatcher;
import org.springframework.util.StringUtils;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Getter
public class RequestCaptor implements RequestMatcher {

    private final AtomicInteger times = new AtomicInteger(0);
    private String bodyString;
    private Object body;
    private HttpHeaders headers = new HttpHeaders();

    public void match(ClientHttpRequest request) throws AssertionError {
        this.times.incrementAndGet();
        MockClientHttpRequest mockRequest = (MockClientHttpRequest) request;
        this.bodyString = mockRequest.getBodyAsString();
        if (StringUtils.hasLength(this.bodyString)) {
            this.body = (new JsonSlurper()).parseText(mockRequest.getBodyAsString());
        }

        this.headers = mockRequest.getHeaders();
    }

    public int getTimes() {
        return times.get();
    }

    public void await(long timeout, TimeUnit unit) {
        Awaitility.await()
                .alias("Request for " + this.getClass().getCanonicalName())
                .atMost(timeout, unit)
                .pollInterval(100L, TimeUnit.MILLISECONDS).until(() -> this.body != null);
    }

    public void clear() {
        this.times.set(0);
        this.body = null;
        this.headers = null;
    }
}
