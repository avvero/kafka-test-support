package pw.avvero.example.feature1;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

@Service
public class TelegramService {
    private final RestTemplate restTemplate;
    private final String url;
    public TelegramService(RestTemplate restTemplate,
                           @Value("${telegram.uri}") String url) {
        this.restTemplate = restTemplate;
        this.url = url;
    }
    public void sendMessage(SendMessageRequest request) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<SendMessageRequest> requestEntity = new HttpEntity<>(request, headers);
        restTemplate.postForObject(url + "/sendMessage", requestEntity, Object.class);
    }

    public record SendMessageRequest(String chatId, String text) {}
}
