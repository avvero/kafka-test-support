package pw.avvero.example.feature1;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;
import pw.avvero.example.feature1.TelegramService.SendMessageRequest;

import java.util.concurrent.ExecutionException;

@Service
@RequiredArgsConstructor
public class ServiceA {

    private final KafkaTemplate<Object, Object> kafkaTemplate;
    private final TelegramService telegramService;
    private final ObjectMapper objectMapper;

    public void processWebhook(String webhookRequest) throws ExecutionException, InterruptedException {
        Message message = MessageBuilder
                .withPayload(webhookRequest)
                .setHeader(KafkaHeaders.TOPIC, "topicA")
                .build();
        kafkaTemplate.send(message).get();
    }

    @KafkaListener(id = "topicBConsumer", topics = "topicB")
    public void consume(@Payload String body) throws JsonProcessingException {
        SendMessageRequest sendMessageRequest = objectMapper.readValue(body, SendMessageRequest.class);
        telegramService.sendMessage(sendMessageRequest);
    }
}
