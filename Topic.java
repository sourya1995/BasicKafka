
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.Optional;

public class Topic {
    private static final Logger LOGGER = Logger.getLogger(Topic.class.getName());
    private String name;
    private int numPartitions;
    private int numReplicas;
    private BlockingQueue<Message>[] partitions;
    private int[][] consumerOffsets;
    private BlockingQueue<Message>[] replicaPartitions;
    private boolean[] isLeader;
    private ExecutorService leaderExecutor;

    @SuppressWarnings("unchecked")
    public Topic(String name, int numPartitions, int numReplicas) {
        this.name = name;
        this.numPartitions = numPartitions;
        this.numReplicas = numReplicas;
        this.partitions = new LinkedBlockingQueue[numPartitions];
        this.consumerOffsets = new int[numPartitions][];
        this.replicaPartitions = new LinkedBlockingQueue[numPartitions * numReplicas];
        this.isLeader = new boolean[numPartitions * numReplicas];
        this.leaderExecutor = Executors.newFixedThreadPool(numPartitions);
        for (int i = 0; i < numPartitions; i++) {
            partitions[i] = new LinkedBlockingQueue<>();
            consumerOffsets[i] = new int[0];

            for (int j = 0; j < numReplicas; j++) {
                replicaPartitions[i * numReplicas + j] = new LinkedBlockingQueue<>();
                isLeader[i * numReplicas + j] = (j == 0);
            }

            startLeaderTask(i);
        }

    }

    private void startLeaderTask(int partition) {
        leaderExecutor.submit(() -> {
            while (true) {
                try {
                    if (isLeader[partition]) {
                        replicateMessages(partition);
                    }
                    Thread.sleep(1000);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    LOGGER.severe(() -> "Exception" + ex);
                    break;
                }
            }
        });
    }

    public boolean produce(String messageContent, int partition) {
        Message message = new Message(messageContent);
        int targetPartition = getPartition(messageContent);
        try {
            boolean success = forwardToLeader(message, targetPartition);
            if (success) {
                acknowledgeMessage(message, partition);
            }
            return success;
        } catch (Exception e) {
            Thread.currentThread().interrupt();
            LOGGER.severe(() -> "Exception" + e);
            return false;
        }
    }

    private boolean forwardToLeader(Message message, int partition) {
        try {
            int leaderIndex = partition * replicaPartitions.length / numPartitions;
            return replicaPartitions[leaderIndex].offer(message, 10, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.severe(() -> "Exception" + e);
            return false;
        }
    }

    private void replicateToFollowers(Message message, int partition) {
        for (int replica = 1; replica < replicaPartitions.length / numPartitions; replica++) {
            try {
                replicaPartitions[partition * numReplicas + replica].put(message);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOGGER.severe(() -> "Exception" + e);
            }
        }
    }

    private void replicateMessages(int partition) {
        while (true) {
            try {
                Message message = replicaPartitions[partition].poll(100, TimeUnit.MILLISECONDS);
                if (message != null) {
                    replicateToFollowers(message, partition);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOGGER.severe(() -> "Exception" + e);
            }
        }
    }

    /*
     * private void replicateMessage(Message message, int partition) {
     * for (int replica = 0; replica < replicaPartitions.length / numPartitions;
     * replica++) {
     * try {
     * replicaPartitions[partition * numReplicas + replica].put(message);
     * } catch (InterruptedException e) {
     * Thread.currentThread().interrupt();
     * LOGGER.severe(() -> "Exception" + e);
     * }
     * }
     * }
     */
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

    private void acknowledgeMessage(Message message, int partition) {
        LOGGER.info(() -> "Message acknowledged:" + message);
    }

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

    public int getPartition(String key) {
        return Math.abs(key.hashCode()) % numPartitions;
    }

    public void commitOffset(String consumerId, int partition, int offset) {
        int[] offsets = consumerOffsets[partition];
        if (offset < offsets.length && offsets[offset] == 0) {
            offsets[offset] = 1; // commit offset
        }
    }

    public int getConsumerOffset(String consumerId, int partition) {
        return consumerOffsets[partition].length > 0 ? consumerOffsets[partition][consumerOffsets[partition].length - 1]
                : 0;
    }

    public void updateConsumerOffset(String consumerId, int partition, int offset) {
        int[] offsets = consumerOffsets[partition];
        int[] newOffsets = new int[offsets.length + 1];
        System.arraycopy(offsets, 0, newOffsets, 0, offsets.length);
        newOffsets[offsets.length] = offset;
        consumerOffsets[partition] = newOffsets;
    }

    public int getPartitionSize(int partition) {
        return partitions[partition].size();
    }

    public String getName() {
        return name;
    }

    public void shutdown() {
        leaderExecutor.shutdownNow();
    }

}
