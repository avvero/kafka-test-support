package pw.avvero.test.kafka.example;

import lombok.Data;

import java.io.Serializable;

@Data
public class TelegramWebhookMessage implements Serializable {
    private Message message;

    @Data
    public static class Message implements Serializable {
        private Chat chat;
        private String text;
    }

    @Data
    public static class Chat implements Serializable {
        private String id;
    }
}
