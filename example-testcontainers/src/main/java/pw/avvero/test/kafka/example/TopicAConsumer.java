package pw.avvero.test.kafka.example;

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

import static pw.avvero.test.kafka.example.TelegramService.*;

@Component
@RequiredArgsConstructor
public class TopicAConsumer {

    private final ObjectMapper objectMapper;
    private final OpenaiService openaiService;
    private final TelegramService telegramService;
    private final KafkaTemplate<Object, Object> kafkaTemplate;

    @KafkaListener(id = "topicAConsumer", topics = "topicA")
    public void consume(@Payload String body) throws JsonProcessingException, ExecutionException, InterruptedException {
        TelegramWebhookMessage webhookMessage = objectMapper.readValue(body, TelegramWebhookMessage.class);
        String content = openaiService.process(webhookMessage.getMessage().getText());
        SendMessageRequest sendMessageRequest = new SendMessageRequest(
                webhookMessage.getMessage().getChat().getId(),
                content);
        Message message = MessageBuilder
                .withPayload(objectMapper.writeValueAsString(sendMessageRequest))
                .setHeader(KafkaHeaders.TOPIC, "topicB")
                .build();
        kafkaTemplate.send(message).get();
    }

    @KafkaListener(id = "topicBConsumer", topics = "topicB")
    public void consume2(@Payload String body) throws JsonProcessingException, ExecutionException, InterruptedException {
        SendMessageRequest sendMessageRequest = objectMapper.readValue(body, SendMessageRequest.class);
        telegramService.sendMessage(sendMessageRequest);
    }

}
