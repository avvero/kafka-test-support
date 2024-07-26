### Overview
**Problem**: Writing and maintaining integration tests with Kafka is challenging due to the complexity of working with topics.\
**Solution**: An approach that ensures test isolation and provides tools for comprehensive message access.\
**Status**: Completed, article is published, currently using.

----

# Kafka test support

<div align="center">
    <img src="assets/image.jpg" width="400" height="auto">
</div>

Provides utility functions for Kafka integration within Spring applications, focusing on partition assignment
and offset commit verification. This library supports ensuring that Kafka listeners are properly assigned to
partitions and that offsets are committed as expected across consumer groups and topics. It is designed to be
used in scenarios where a precise control over Kafka consumer initialization and message processing state is required.

Key features include:
- Waiting for partition assignment to Kafka listener containers.
- Detecting configuration issues related to multiple listener containers for the same topic and group.
- Waiting for offset commits across consumer groups and topics, ensuring message processing completeness.

Usage involves invoking static methods with the necessary Kafka and Spring context configurations.

## Publications

Article on Habr - [Изоляция в тестах с Kafka](https://habr.com/ru/articles/797049)

Article on Medium - [Isolation in Testing with Kafka](https://medium.com/@avvero.abernathy/isolation-in-testing-with-kafka-16e00f5d5d7e)

## Using Record Captor

Main function of the Record Captor is to "capture" messages from a specified list of topics and provide access to these 
messages for the step of verifying the results of the test scenario. Technically, it is a simple consumer for a Kafka
topic with a mechanism for storing messages and an access interface to them.

### Step 1: Add Dependency
First, include the necessary dependency in your project's build configuration to utilize Record Captor:

```gradle
testImplementation 'pw.avvero:kafka-test-support:1.1.0'
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
