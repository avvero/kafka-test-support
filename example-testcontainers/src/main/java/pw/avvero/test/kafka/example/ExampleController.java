package pw.avvero.test.kafka.example;

import lombok.AllArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.ExecutionException;

@RestController
@AllArgsConstructor
public class ExampleController {

    private final KafkaTemplate<Object, Object> kafkaTemplate;

    @PostMapping("/telegram/webhook")
    public void webhook(@RequestBody String body) throws ExecutionException, InterruptedException {
        Message message = MessageBuilder
                .withPayload(body)
                .setHeader(KafkaHeaders.TOPIC, "topicA")
//                .setHeader(KafkaHeaders.KEY, key)
                .build();
        kafkaTemplate.send(message).get();
    }
}
