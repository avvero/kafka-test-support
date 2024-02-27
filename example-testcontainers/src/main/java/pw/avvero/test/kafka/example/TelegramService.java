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
public class TelegramService {

    private final RestTemplate restTemplate;
    @Value("${telegram.uri}")
    private String url;

    public record SendMessageRequest(String chatId, String text) {

    }

    public void sendMessage(SendMessageRequest request) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<SendMessageRequest> requestEntity = new HttpEntity<>(request, headers);
        Object response = restTemplate.postForObject(url + "/sendMessage", requestEntity, Object.class);
    }
}
