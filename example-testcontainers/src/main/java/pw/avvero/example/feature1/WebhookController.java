package pw.avvero.example.feature1;

import lombok.AllArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.ExecutionException;

@RestController
@AllArgsConstructor
public class WebhookController {

    private final ServiceA serviceA;

    @PostMapping("/telegram/webhook")
    public void process(@RequestBody String webhookRequest) throws ExecutionException, InterruptedException {
        serviceA.processWebhook(webhookRequest);
    }
}
