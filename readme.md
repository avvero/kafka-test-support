# Kafka test support

Provides utility functions for Kafka integration within Spring applications, focusing on partition assignment
and offset commit verification. This library supports ensuring that Kafka listeners are properly assigned to
partitions and that offsets are committed as expected across consumer groups and topics. It is designed to be
used in scenarios where a precise control over Kafka consumer initialization and message processing state is required.

Key features include:
- Waiting for partition assignment to Kafka listener containers.
- Detecting configuration issues related to multiple listener containers for the same topic and group.
- Waiting for offset commits across consumer groups and topics, ensuring message processing completeness.

Usage involves invoking static methods with the necessary Kafka and Spring context configurations.

## Using Record Captor

Main function of the Record Captor is to "capture" messages from a specified list of topics and provide access to these 
messages for the step of verifying the results of the test scenario. Technically, it is a simple consumer for a Kafka
topic with a mechanism for storing messages and an access interface to them.

### Step 1: Add Dependency
First, include the necessary dependency in your project's build configuration to utilize Record Captor:

```gradle
testImplementation 'pw.avvero:kafka-test-support:1.0.0'
```

This library provides the necessary components to integrate Record Captor into your test suite.

### Step 2: Configure Test Beans
Next, configure the beans required for Record Captor in your test context. Create a new Java class annotated with 
`@TestConfiguration`. This configuration class should define beans for both `RecordCaptor` and `RecordCaptorConsumer`. 
Example:
```java
@TestConfiguration(proxyBeanMethods = false)
public class RecordCaptorConfiguration {
    @Bean
    RecordCaptor recordCaptor() {
        return new RecordCaptor();
    }

    @Bean
    RecordCaptorConsumer recordCaptorConsumer(RecordCaptor recordCaptor) {
        return new RecordCaptorConsumer(recordCaptor, new RecordSnapshotMapper());
    }
}
```

### Step 3: Specify Topics to Capture
Specify the list of Kafka topics from which messages should be captured. This is done by setting 
the `test.record-captor.topics` property with a comma-separated list of topic names.

```properties
test.record-captor.topics=topic1,topicA,topicB
```

### Usage
With these steps completed, Record Captor is ready for use in your test suite. It will automatically capture messages
from the specified topics during test execution, storing them for later verification. 

### Example

Example for application with tests is provided in module [example-testcontainers](https://github.com/avvero/kafka-test-support/tree/sb3/example-testcontainers).