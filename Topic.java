
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

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
        for (int i = 0; i < numPartitions; i++){
            partitions[i] = new LinkedBlockingQueue<>();
        }
    }

    public void produce(String messageContent){
        int partition = Math.abs(messageContent.hashCode()) % numPartitions;
        Message message = new Message(messageContent);
        partitions[partition].offer(message);
        LOGGER.info(() -> "Produced to topic" + name + "partition" + partition + ":" + message);

    }

    public Message consume(String consumerId){
        int partition = Math.abs(consumerId.hashCode()) % numPartitions;
        Message message = partitions[partition].poll();
        if (message != null){
            LOGGER.info(() -> "Consumed from topic" + name + "partition" + partition + ":" + message);
        }

        return message;
    }

    public int getPartitionSize(int partition) {
        return numPartitions;
    }

    
}
