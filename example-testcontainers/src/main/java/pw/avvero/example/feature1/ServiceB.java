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
        String openAiResponseContent = openaiService.process(webhookMessage.getMessage().getText());
        TelegramService.SendMessageRequest sendMessageRequest = new TelegramService.SendMessageRequest(
                webhookMessage.getMessage().getChat().getId(),
                openAiResponseContent);
        Message message = MessageBuilder
                .withPayload(objectMapper.writeValueAsString(sendMessageRequest))
                .setHeader(KafkaHeaders.TOPIC, "topicB")
                .build();
        kafkaTemplate.send(message).get();
    }
}
