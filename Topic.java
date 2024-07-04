
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;
import java.util.Optional;

public class Topic {
    private static final Logger LOGGER = Logger.getLogger(Topic.class.getName());
    private String name;
    private int numPartitions;
    private BlockingQueue<Message>[] partitions;

    @SuppressWarnings("unchecked")
    public Topic(String name, int numPartitions) {
        this.name = name;
        this.numPartitions = numPartitions;
        this.partitions = new LinkedBlockingQueue[numPartitions];
        for (int i = 0; i < numPartitions; i++) {
            partitions[i] = new LinkedBlockingQueue<>();
        }
    }

    public void produce(String messageContent) {
        int partition = Math.abs(messageContent.hashCode()) % numPartitions;
        Message message = new Message(messageContent);
        partitions[partition].offer(message);
        LOGGER.info(() -> "Produced to topic" + name + "partition" + partition + ":" + message);

    }

   /*  public Optional<Message> consume(String consumerId, int partition, int offset) {
        // Check if the partition exists and has enough messages
        if (partition >= 0 && partition < partitions.length && partitions[partition].size() > offset) {
            // Attempt to retrieve the message at the specified offset
            try {
                Message message = partitions[partition].peek(offset); // Use peek(offset) if available, otherwise
                                                                      // poll(offset)
                if (message != null) {
                    return Optional.ofNullable(message);
                } else {
                    return Optional.empty();
                }
            } catch (Exception e) {
                // Handle exceptions, possibly logging them
                System.err.println("Error consuming message: " + e.getMessage());
                return Optional.empty();
            }
        }
        return Optional.empty(); // Return an empty Optional if the partition is invalid or there's no message at
                                 // the specified offset
    } */

    public Optional<Message> consume(String consumerId) {
       int partition = Math.abs(consumerId.hashCode()) % numPartitions;
       BlockingQueue<Message> partitionQueue = partitions[partition];

       try {
           Message message = partitionQueue.poll(); // Poll to retrieve and remove the head of the queue
           if (message != null) {
               return Optional.ofNullable(message);
           }
       } catch (Exception e) {
           LOGGER.severe(() -> "could not consume message: " + e.getMessage());
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

    public int getPartitionSize(int partition) {
        return partitions[partition].size();
    }

}
