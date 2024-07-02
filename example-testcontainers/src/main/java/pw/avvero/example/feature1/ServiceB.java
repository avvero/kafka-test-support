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
import pw.avvero.example.feature1.TelegramService.SendMessageRequest;

import java.util.concurrent.ExecutionException;

@Component
@RequiredArgsConstructor
public class ServiceB {

    private final ObjectMapper objectMapper;
    private final OpenaiService openaiService;
    private final KafkaTemplate<Object, Object> kafkaTemplate;

    @KafkaListener(id = "topicAConsumer", topics = "topicA")
    public void consume(@Payload String webhookRequestString) throws JsonProcessingException, ExecutionException,
            InterruptedException {
        TelegramWebhookMessage webhookRequest = objectMapper.readValue(webhookRequestString, TelegramWebhookMessage.class);
        String chatId = webhookRequest.getMessage().getChat().getId();
        try {
            String openAiResponseContent = openaiService.process(webhookRequest.getMessage().getText());
            sendMessage("topicB", chatId, new SendMessageRequest(chatId, openAiResponseContent));
        } catch (OpenaiException e) {
            sendMessage("topicB", chatId, new SendMessageRequest(chatId, e.getMessage()));
            sendMessage("topicC", chatId, new MessageProcessingError(webhookRequest, new ErrorDetails(e.getCode(), e.getMessage())));
        }
    }

    private void sendMessage(String topic, String key, Object payload) throws JsonProcessingException, ExecutionException,
            InterruptedException {
        Message message = MessageBuilder
                .withPayload(objectMapper.writeValueAsString(payload))
                .setHeader(KafkaHeaders.TOPIC, topic)
                .setHeader(KafkaHeaders.KEY, key)
                .build();
        kafkaTemplate.send(message).get();
    }

    public record MessageProcessingError(TelegramWebhookMessage webhookMessage, ErrorDetails error){}
    public record ErrorDetails(String code, String message){}
}
