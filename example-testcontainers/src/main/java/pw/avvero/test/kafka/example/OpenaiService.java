package pw.avvero.test.kafka.example;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

@Service
public class OpenaiService {
    private final RestTemplate restTemplate;
    private final String url;
    public OpenaiService(RestTemplate restTemplate,
                         @Value("${openai.uri}") String url) {
        this.restTemplate = restTemplate;
        this.url = url;
    }
    public String process(String content) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        CompletionRequest request = new CompletionRequest(content);
        HttpEntity<CompletionRequest> requestEntity = new HttpEntity<>(request, headers);
        CompletionResponse response = restTemplate.postForObject(url + "/v1/chat/completions", requestEntity,
                CompletionResponse.class);
        return response.content;
    }

    public record CompletionRequest(String content) {}
    public record CompletionResponse(String content) {}
}
