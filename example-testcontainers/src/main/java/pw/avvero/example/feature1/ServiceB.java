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
import org.springframework.stereotype.Component;
import pw.avvero.example.feature1.OpenaiService.OpenaiException;

import java.util.concurrent.ExecutionException;

@Component
@RequiredArgsConstructor
public class ServiceB {

    private final ObjectMapper objectMapper;
    private final OpenaiService openaiService;
    private final KafkaTemplate<Object, Object> kafkaTemplate;

    @KafkaListener(id = "topicAConsumer", topics = "topicA")
    public void consume(@Payload String webhookRequest) throws JsonProcessingException, ExecutionException,
            InterruptedException {
        TelegramWebhookMessage webhookMessage = objectMapper.readValue(webhookRequest, TelegramWebhookMessage.class);
        try {
            String openAiResponseContent = openaiService.process(webhookMessage.getMessage().getText());
            TelegramService.SendMessageRequest sendMessageRequest = new TelegramService.SendMessageRequest(
                    webhookMessage.getMessage().getChat().getId(),
                    openAiResponseContent);
            sendMessage("topicB", webhookMessage.getMessage().getChat().getId(), sendMessageRequest);
        } catch (OpenaiException e) {
            TelegramService.SendMessageRequest sendMessageRequest = new TelegramService.SendMessageRequest(
                    webhookMessage.getMessage().getChat().getId(), e.getMessage());
            sendMessage("topicB", webhookMessage.getMessage().getChat().getId(), sendMessageRequest);
            sendMessage("topicC", webhookMessage.getMessage().getChat().getId(), e.getMessage());
        }
    }

    private void sendMessage(String topic, String key, Object payload) throws JsonProcessingException, ExecutionException,
            InterruptedException {
        Message message = MessageBuilder
                .withPayload(objectMapper.writeValueAsString(payload))
                .setHeader(KafkaHeaders.TOPIC, topic)
                .setHeader(KafkaHeaders.RECEIVED_KEY, key)
                .build();
        kafkaTemplate.send(message).get();
    }
}
