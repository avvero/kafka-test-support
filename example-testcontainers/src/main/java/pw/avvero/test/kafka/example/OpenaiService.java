package pw.avvero.test.kafka.example;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

@Service
@RequiredArgsConstructor
public class OpenaiService {

    private final RestTemplate restTemplate;
    @Value("${openai.uri}")
    private String url;

    public record CompletionRequest(String content) {
    }

    public record CompletionResponse(String content) {
    }

    public String process(String text) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        CompletionRequest request = new CompletionRequest(text);
        HttpEntity<CompletionRequest> requestEntity = new HttpEntity<>(request, headers);
        CompletionResponse response = restTemplate.postForObject(url + "/v1/chat/completions", requestEntity, CompletionResponse.class);
        return response.content;
    }
}
