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
