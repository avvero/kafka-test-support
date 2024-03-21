package pw.avvero.test.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.springframework.boot.autoconfigure.kafka.KafkaConnectionDetails;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.MessageListenerContainer;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toMap;
import static org.apache.kafka.clients.admin.AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG;

/**
 * Utility class providing support functions for Kafka in Spring applications.
 */
@Slf4j
public class KafkaSupport {

    public static int OFFSET_COMMIT_WAIT_ATTEMPTS_MAX = 200;
    public static int OFFSET_COMMIT_WAIT_TIME = 10;

    /**
     * Waits for the partition assignment for all Kafka listener containers in the application context.
     * This method ensures that each Kafka listener container is assigned at least one partition
     * before proceeding. It also initializes Kafka producer by sending a test message.
     *
     * <p>This method is useful in scenarios where the application needs to wait for the Kafka
     * consumers to be fully set up and ready before performing certain operations.</p>
     *
     * @param applicationContext the Spring application context containing the Kafka listener containers.
     * @throws Exception if an error occurs during the process.
     */
    public static void waitForPartitionAssignment(ApplicationContext applicationContext) throws Exception {
        detectMultipleContainersForSameTopicWithinSameGroup(applicationContext);
        //
        KafkaListenerEndpointRegistry registry = applicationContext.getBean(KafkaListenerEndpointRegistry.class);
        log.debug("[KTS] Waiting for partition assignment is requested");
        long startTime = System.currentTimeMillis();
        for (MessageListenerContainer messageListenerContainer : registry.getListenerContainers()) {
            long partStartTime = System.currentTimeMillis();
            log.debug("[KTS] Waiting for partition assignment started for {}", messageListenerContainer.getListenerId());
            int partitions = ContainerTestUtils.waitForAssignment(messageListenerContainer, 1);
            long partGauge = System.currentTimeMillis() - partStartTime;
            if (partitions > 0) {
                String topics = Objects.requireNonNull(messageListenerContainer.getAssignedPartitions()).stream()
                        .map(TopicPartition::topic).collect(Collectors.joining(", "));
                log.debug("[KTS] Waiting for partition assignment for {} is succeeded in {} ms, topics: {}",
                        messageListenerContainer.getListenerId(), partGauge, topics);
            } else {
                log.error("[KTS] Waiting for partition assignment for {} is failed in {} ms",
                        messageListenerContainer.getListenerId(), partGauge);
            }
        }
        long gauge = System.currentTimeMillis() - startTime;
        log.debug("[KTS] Waiting for partition assignment is finished in {} ms. " +
                "At least one partition is assigned for every container", gauge);
    }

    /**
     * Detects and throws an exception if multiple Kafka listener containers are found for the same topic within
     * the same group in the given Spring application context.
     *
     * @param applicationContext the Spring {@link ApplicationContext}
     * @throws RuntimeException if multiple containers are detected
     */
    private static void detectMultipleContainersForSameTopicWithinSameGroup(ApplicationContext applicationContext) {
        KafkaListenerEndpointRegistry registry = applicationContext.getBean(KafkaListenerEndpointRegistry.class);
        Map<String, List<MessageListenerContainer>> containersPerTopicInSameGroup = new HashMap<>();
        for (MessageListenerContainer container : registry.getListenerContainers()) {
            ContainerProperties containerProperties = container.getContainerProperties();
            if (containerProperties.getTopics() == null) continue;
            for (String topic : containerProperties.getTopics()) {
                containersPerTopicInSameGroup
                        .computeIfAbsent(containerProperties.getGroupId() + " : " + topic, (k) -> new ArrayList<>())
                        .add(container);
            }
        }
        containersPerTopicInSameGroup.forEach((key, list) -> {
            if (list.size() > 1) {
                String[] parts = key.split(" : ");
                String groupId = parts[0];
                String topic = parts[1];
                String containerNames = list.stream()
                        .map(MessageListenerContainer::getListenerId)
                        .collect(Collectors.joining(", "));
                throw new RuntimeException(String.format("Detected multiple Kafka listener containers (%s) configured to " +
                                "listen to topic '%s' within the same group '%s'. " +
                                "This configuration may lead to unexpected behavior or message duplication. " +
                                "Please ensure each topic is consumed by a unique group or container.",
                        containerNames, topic, groupId));
            }
        });
    }

    /**
     * Waits for the offset commit for a given list of bootstrap servers retrieved from the application context.
     *
     * @param applicationContext The Spring application context from which to retrieve Kafka connection details.
     * @throws ExecutionException   if an error occurs during the fetching of consumer group or topic information.
     * @throws InterruptedException if the current thread is interrupted while waiting.
     */
    public static void waitForPartitionOffsetCommit(ApplicationContext applicationContext) throws ExecutionException,
            InterruptedException {
        List<String> bootstrapServers = applicationContext.getBean(KafkaConnectionDetails.class).getBootstrapServers();
        waitForPartitionOffsetCommit(bootstrapServers);
    }

    /**
     * Waits for the offset commit across all consumer groups for all topics in the provided list of bootstrap servers.
     * This method checks the offset commit for each partition of each topic and ensures that all consumer groups have
     * committed their offsets. It continuously checks the offsets until they are committed or until a maximum number of
     * attempts is reached.
     *
     * @param bootstrapServers The list of bootstrap servers for the Kafka cluster.
     * @throws InterruptedException if the thread is interrupted while waiting for the offsets to commit.
     * @throws ExecutionException   if an error occurs during the fetching of consumer group or topic information.
     */
    public static void waitForPartitionOffsetCommit(List<String> bootstrapServers) {
        try (AdminClient adminClient = AdminClient.create(singletonMap(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers))) {
            try {
                waitForPartitionOffsetCommit(adminClient);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static void waitForPartitionOffsetCommit(AdminClient adminClient)
            throws ExecutionException, InterruptedException {
        // List the topics available in the cluster
        Set<String> topics = adminClient.listTopics().namesToListings().get().keySet();
        waitForPartitionOffsetCommitForTopics(adminClient, topics);
    }

    public static void waitForPartitionOffsetCommitForTopics(AdminClient adminClient, Set<String> topics)
            throws ExecutionException, InterruptedException {
        // List the topics available in the cluster
        Set<TopicPartition> topicPartitions = getPartitions(adminClient, topics);
        waitForPartitionOffsetCommitForPartitions(adminClient, topicPartitions);
    }

    public static void waitForPartitionOffsetCommitForPartitions(AdminClient adminClient,
                                                                 Set<TopicPartition> topicPartitions)
            throws ExecutionException, InterruptedException {
        Set<String> consumerGroups = adminClient.listConsumerGroups().all().get()
                .stream().map(ConsumerGroupListing::groupId).collect(Collectors.toSet());
        waitForPartitionOffsetCommitForPartitions(adminClient, topicPartitions, consumerGroups);
    }

    static ThreadLocal<Long> topicsOffsetsTotalInThread = new ThreadLocal<>(); //experimental

    public static void waitForPartitionOffsetCommitForPartitions(AdminClient adminClient,
                                                                 Set<TopicPartition> topicPartitions,
                                                                 Set<String> consumerGroups)
            throws InterruptedException, ExecutionException {
        long startTime = System.currentTimeMillis();
        int attempt = 0;
        boolean offsetCommitted = false;
        while (!offsetCommitted) {
            if (++attempt > OFFSET_COMMIT_WAIT_ATTEMPTS_MAX) {
                throw new RuntimeException("Exceeded maximum attempts (" + OFFSET_COMMIT_WAIT_ATTEMPTS_MAX
                        + ") waiting for offset commit for partitions.");
            }
            log.debug("[KTS] Waiting for offset commit is requested, attempt {}", attempt);
            Map<TopicPartition, Long> topicsOffsets = getOffsetsForPartitions(adminClient, topicPartitions);
            Long topicsOffsetsTotal = topicsOffsets.values().stream().mapToLong(Long::longValue).sum();
            if (topicsOffsetsTotal.equals(topicsOffsetsTotalInThread.get())) {
                log.debug("[KTS] Topic offset is not changed; Waiting for offset commit is finished in {} ms",
                        System.currentTimeMillis() - startTime);
                return;
            }
            // Get current offsets for partitions
            Map<String, Map<TopicPartition, Long>> consumerGroupsOffsets = getOffsetsForConsumerGroups(adminClient,
                    consumerGroups, topicPartitions);
            offsetCommitted = checkOffsetCommitted(topicPartitions, consumerGroups, topicsOffsets, consumerGroupsOffsets);
            if (offsetCommitted) {
                //Do recheck
                Long topicsOffsetsTotalFinish = getOffsetsForPartitions(adminClient, topicPartitions).values()
                        .stream().mapToLong(Long::longValue).sum();
                offsetCommitted = topicsOffsetsTotal.equals(topicsOffsetsTotalFinish);
            }
            //
            if (offsetCommitted) {
                topicsOffsetsTotalInThread.set(topicsOffsetsTotal);
            } else {
                log.warn("[KTS] Some offsets are not equal. Waiting for further message processing before proceeding. " +
                        "Refreshing end offsets and reevaluating.");
                try {
                    Thread.sleep(OFFSET_COMMIT_WAIT_TIME); // NOSONAR magic #
                } catch (@SuppressWarnings("unused") InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        log.debug("[KTS] Waiting for offset commit is finished in {} ms", System.currentTimeMillis() - startTime);
    }

    private static boolean checkOffsetCommitted(Set<TopicPartition> topicPartitions,
                                                Set<String> consumerGroups,
                                                Map<TopicPartition, Long> topicsOffsets,
                                                Map<String, Map<TopicPartition, Long>> consumerGroupsOffsets) {
        boolean result = true;
        OffsetSnapshotFrame offsetSnapshotFrame = new OffsetSnapshotFrame();
        for (String consumerGroup : consumerGroups) {
            for (TopicPartition topicPartition : topicPartitions) {
                Long consumerGroupOffset = consumerGroupsOffsets
                        .getOrDefault(consumerGroup, Map.of())
                        .get(topicPartition);
                if (consumerGroupOffset == null) { // There is no consumer in Consumer group for topic
                    continue;
                }
                Long partitionOffset = topicsOffsets.get(topicPartition);
                boolean equal = partitionOffset == null || partitionOffset == 0L || partitionOffset.equals(consumerGroupOffset);
                result = result && equal;
                //
                offsetSnapshotFrame.append(consumerGroup, topicPartition, consumerGroupOffset, partitionOffset);
            }
            offsetSnapshotFrame.split();
        }
        log.debug(offsetSnapshotFrame.toString());
        return result;
    }


    public static Set<TopicPartition> getPartitions(AdminClient adminClient, Set<String> topics)
            throws ExecutionException, InterruptedException {
        Set<TopicPartition> topicPartitions = new HashSet<>();
        DescribeTopicsResult topicInfo = adminClient.describeTopics(topics);
        for (String topic : topics) {
            int partitions = topicInfo.topicNameValues().get(topic).get().partitions().size();
            for (int i = 0; i < partitions; i++) {
                topicPartitions.add(new TopicPartition(topic, i));
            }
        }
        return topicPartitions;
    }

    public static Map<TopicPartition, Long> getOffsetsForPartitions(AdminClient adminClient,
                                                                    Set<TopicPartition> topicPartitions)
            throws ExecutionException, InterruptedException {
        Map<TopicPartition, OffsetSpec> topicPartitionsWithSpecs = topicPartitions.stream()
                .collect(toMap(tp -> tp, tp -> OffsetSpec.latest()));
        return adminClient.listOffsets(topicPartitionsWithSpecs, new ListOffsetsOptions(IsolationLevel.READ_COMMITTED))
                .all()
                .get()
                .entrySet()
                .stream()
                .collect(toMap(Map.Entry::getKey, entry -> entry.getValue().offset()));
    }

    public static Map<String, Map<TopicPartition, Long>> getOffsetsForConsumerGroups(
            AdminClient adminClient,
            Set<String> consumerGroups,
            Set<TopicPartition> topicPartitions)
            throws ExecutionException, InterruptedException {
        Map<String, ListConsumerGroupOffsetsSpec> groupSpecs = new HashMap<>();
        for (String consumerGroup : consumerGroups) {
            ListConsumerGroupOffsetsSpec spec = new ListConsumerGroupOffsetsSpec();
            spec.topicPartitions(topicPartitions);
            groupSpecs.put(consumerGroup, spec);
        }
        ListConsumerGroupOffsetsResult offsetsResult = adminClient.listConsumerGroupOffsets(groupSpecs);
        Map<String, Map<TopicPartition, Long>> currentOffsets = new HashMap<>();
        for (String consumerGroup : consumerGroups) {
            offsetsResult.partitionsToOffsetAndMetadata(consumerGroup).get().forEach((tp, oam) -> {
                if (oam == null) return;
                currentOffsets
                        .computeIfAbsent(consumerGroup, c -> new HashMap<>())
                        .put(tp, oam.offset());
            });
        }
        return currentOffsets;
    }
}