package pw.avvero.test.kafka;

import org.apache.kafka.common.TopicPartition;

/**
 * The {@code OffsetSnapshotFrame} class is designed for creating and managing a formatted string
 * representation of offset snapshots within a Kafka environment. It primarily supports logging offset
 * comparisons and highlights discrepancies between consumer group offsets and partition offsets.
 */
public class OffsetSnapshotFrame {

    private final StringBuilder sb;

    public OffsetSnapshotFrame() {
        this.sb = new StringBuilder("[KTS] Offset comparison frame\n");
        sb.append(" ______________________________________________________________________________________________________\n");
        sb.append("| Consumer group             | Partition                                              | CGF    | PO    |\n");
    }

    /**
     * Appends a new row to the offset snapshot frame with provided offset details.
     *
     * @param consumerGroup       the name of the consumer group
     * @param topicPartition      the topic and partition information
     * @param consumerGroupOffset the offset of the consumer group for the given partition
     * @param partitionOffset     the current offset of the partition
     */
    public void append(String consumerGroup, TopicPartition topicPartition, Long consumerGroupOffset, Long partitionOffset) {
        boolean equal = partitionOffset == null || partitionOffset == 0L || partitionOffset.equals(consumerGroupOffset);
        char[] frameRow = "| cg                         | t                                                      | o      | o     |          \n".toCharArray();
        replace(frameRow, 2, 27, consumerGroup);
        replace(frameRow, 31, 84, topicPartition.toString());
        replace(frameRow, 88, 92, consumerGroupOffset.toString());
        replace(frameRow, 97, 102, partitionOffset != null ? partitionOffset.toString() : " ");
        replace(frameRow, 104, 108, equal ? "   " : "<-- error");
        sb.append(new String(frameRow));
    }

    public void split() {
        sb.append(" ______________________________________________________________________________________________________\n");
    }

    public String toString() {
        return sb.toString();
    }

    private void replace(char[] target, int s, int e, String substring) {
        int length = Math.min(e - s + 1, substring.length());
        for (int i = 0; i < length; i++) {
            target[s + i] = substring.charAt(i);
        }
        for (int i = length; i < e - s + 1; i++) {
            target[s + i] = ' ';
        }
    }
}
