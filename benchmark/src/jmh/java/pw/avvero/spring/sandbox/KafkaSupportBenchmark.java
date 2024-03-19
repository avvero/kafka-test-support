package pw.avvero.spring.sandbox;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.openjdk.jmh.annotations.Benchmark;
import pw.avvero.emk.EmbeddedKafkaContainer;
import pw.avvero.test.kafka.KafkaSupport;

import java.util.ArrayList;
import java.util.List;
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

    @Benchmark
    public void startAndCreateOneTopic() throws ExecutionException, InterruptedException {
        EmbeddedKafkaContainer container = new EmbeddedKafkaContainer("avvero/emk-native:1.0.0");
        container.start();
        createTopics(container.getBootstrapServers(), 1);
        container.stop();
    }

    @Benchmark
    public void startAndCreateOneHundredTopics() throws ExecutionException, InterruptedException {
        EmbeddedKafkaContainer container = new EmbeddedKafkaContainer("avvero/emk-native:1.0.0");
        container.start();
        createTopics(container.getBootstrapServers(), 100);
        container.stop();
    }

    @Benchmark
    public void waitForPartitionOffsetCommitForOneTopic() throws ExecutionException, InterruptedException {
        EmbeddedKafkaContainer container = new EmbeddedKafkaContainer("avvero/emk-native:1.0.0");
        container.start();
        createTopics(container.getBootstrapServers(), 1);
        KafkaSupport.waitForPartitionOffsetCommit(List.of(container.getBootstrapServers()));
        container.stop();
    }

    @Benchmark
    public void waitForPartitionOffsetCommitForOneHundredTopics() throws ExecutionException, InterruptedException {
        EmbeddedKafkaContainer container = new EmbeddedKafkaContainer("avvero/emk-native:1.0.0");
        container.start();
        createTopics(container.getBootstrapServers(), 100);
        KafkaSupport.waitForPartitionOffsetCommit(List.of(container.getBootstrapServers()));
        container.stop();
    }

    //

    private void checkKafkaReadiness(String bootstrapServers) throws ExecutionException, InterruptedException {
        try(AdminClient admin = AdminClient.create(singletonMap(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers))) {
            admin.listTopics().names().get();
        }
    }

    private void createTopics(String bootstrapServers, int number) {
        List<NewTopic> topics = new ArrayList<>(number);
        for (int i = 0; i < number; i++) {
            topics.add(new NewTopic("topic" + i, 1, (short) 1));
        }
        try(AdminClient admin = AdminClient.create(singletonMap(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers))) {
            admin.createTopics(topics);
        }
    }
}
