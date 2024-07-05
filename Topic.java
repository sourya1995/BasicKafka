
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;
import java.util.Optional;

public class Topic {
    private static final Logger LOGGER = Logger.getLogger(Topic.class.getName());
    private String name;
    private int numPartitions;
    private BlockingQueue<Message>[] partitions;
    private int[][] consumerOffsets;

    @SuppressWarnings("unchecked")
    public Topic(String name, int numPartitions) {
        this.name = name;
        this.numPartitions = numPartitions;
        this.partitions = new LinkedBlockingQueue[numPartitions];
        for (int i = 0; i < numPartitions; i++) {
            partitions[i] = new LinkedBlockingQueue<>();
            consumerOffsets[i] = new int[0];
        }
    }

    public boolean produce(String messageContent, int partition) {
        Message message = new Message(messageContent);
        try {
            partitions[partition].put(message);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /*
     * public Optional<Message> consume(String consumerId, int partition, int
     * offset) {
     * // Check if the partition exists and has enough messages
     * if (partition >= 0 && partition < partitions.length &&
     * partitions[partition].size() > offset) {
     * // Attempt to retrieve the message at the specified offset
     * try {
     * Message message = partitions[partition].peek(offset); // Use peek(offset) if
     * available, otherwise
     * // poll(offset)
     * if (message != null) {
     * return Optional.ofNullable(message);
     * } else {
     * return Optional.empty();
     * }
     * } catch (Exception e) {
     * // Handle exceptions, possibly logging them
     * System.err.println("Error consuming message: " + e.getMessage());
     * return Optional.empty();
     * }
     * }
     * return Optional.empty(); // Return an empty Optional if the partition is
     * invalid or there's no message at
     * // the specified offset
     * }
     */

    public Optional<Message> consume(String consumerId, int partition, int offset) {
        int targetPartition = Math.abs(consumerId.hashCode()) % numPartitions;
        BlockingQueue<Message> partitionQueue = partitions[targetPartition]; // Use renamed variable here
        if (offset < consumerOffsets[targetPartition].length) { // And here
            try {
                Message message = partitionQueue.poll(); // Poll to retrieve and remove the head of the queue
                if (message != null) {
                    return Optional.ofNullable(message);
                }
            } catch (Exception e) {
                LOGGER.severe(() -> "could not consume message: " + e.getMessage());
            }
        }

        return Optional.empty(); // Return an empty Optional if no message is available
    }

    public Message getMessage(int partition) {
        Optional<Message> optionalMessage = Optional.ofNullable(partitions[partition]).map(queue -> {
            try {
                return queue.take();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return null;
            }
        });

        return optionalMessage.orElse(null);
    }

    public void commitOffset(String consumerId, int partition, int offset){
        int[] offsets = consumerOffsets[partition];
        if(offset < offsets.length && offsets[offset] == 0){
            offsets[offset] = 1; //commit offset
        }
    }

    public int getPartitionSize(int partition) {
        return partitions[partition].size();
    }

    public String getName() {
        return name;
    }

}
