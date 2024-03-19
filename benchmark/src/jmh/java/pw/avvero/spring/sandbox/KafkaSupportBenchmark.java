package pw.avvero.spring.sandbox;

import org.apache.kafka.clients.admin.AdminClient;
import org.openjdk.jmh.annotations.Benchmark;
import pw.avvero.emk.EmbeddedKafkaContainer;

import java.util.concurrent.ExecutionException;

import static java.util.Collections.singletonMap;
import static org.apache.kafka.clients.admin.AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG;

public class KafkaSupportBenchmark {

    @Benchmark
    public void emkNativeKafkaStart() throws ExecutionException, InterruptedException {
        EmbeddedKafkaContainer container = new EmbeddedKafkaContainer("avvero/emk-native:1.0.0");
        container.start();
        container.stop();
    }

    @Benchmark
    public void emkNativeKafkaStartAndReady() throws ExecutionException, InterruptedException {
        EmbeddedKafkaContainer container = new EmbeddedKafkaContainer("avvero/emk-native:1.0.0");
        container.start();
        checkKafkaReadiness(container.getBootstrapServers());
        container.stop();
    }

    private void checkKafkaReadiness(String bootstrapServers) throws ExecutionException, InterruptedException {
        AdminClient admin = AdminClient.create(singletonMap(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers));
        admin.listTopics().names().get();
        admin.close();
    }
}
