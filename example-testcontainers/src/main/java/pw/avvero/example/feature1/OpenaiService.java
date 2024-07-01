package pw.avvero.example.feature1;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.SneakyThrows;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

@Service
public class OpenaiService {
    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper;
    private final String url;
    public OpenaiService(RestTemplate restTemplate,
                         ObjectMapper objectMapper,
                         @Value("${openai.uri}") String url) {
        this.restTemplate = restTemplate;
        this.objectMapper = objectMapper;
        this.url = url;
    }

    @SneakyThrows
    public String process(String content) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        CompletionRequest request = new CompletionRequest(content);
        HttpEntity<CompletionRequest> requestEntity = new HttpEntity<>(request, headers);
        try {
            CompletionResponse response = restTemplate.postForObject(url + "/v1/chat/completions", requestEntity,
                    CompletionResponse.class);
            return response.content;
        } catch (HttpClientErrorException e) {
            CompletionResponse response = objectMapper.readValue(e.getResponseBodyAsString(), CompletionResponse.class);
            if (response != null && response.error != null) {
                throw new OpenaiException(response.error.code, response.error.message);
            }
            throw e;
        }
    }

    public record CompletionRequest(String content) {}
    public record CompletionResponse(String content, CompletionError error) {}
    public record CompletionError(String code, String message){}

    @Data
    @EqualsAndHashCode(callSuper = true)
    public static class OpenaiException extends RuntimeException {
        private String code;

        public OpenaiException(String code, String message) {
            super(message);
            this.code = code;
        }
    }
}
