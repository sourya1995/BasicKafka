import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;

public class Consumer implements Runnable {

    private static final Logger LOGGER = Logger.getLogger(Consumer.class.getName());
    private String consumerId;
    private Topic topic;
    private int numPartitions;
    private int[] offsets;

    public Consumer(String consumerId, Topic topic, int numPartitions) {
        this.consumerId = consumerId;
        this.topic = topic;
        this.numPartitions = numPartitions;
        this.offsets = new int[numPartitions];
    }


    @Override
    public void run() {
        try {
            while (true) {
                for(int partition = 0; partition < numPartitions; partition++) {
                    while (offsets[partition] < topic.getPartitionSize(partition)) {
                        Optional<Message> message = topic.consume(consumerId);
                        if(message != null){
                            offsets[partition]++;
                        }
                        Thread.sleep(1000);
                    }
            }
            Thread.sleep(2000);
        } 
        }catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.severe(() -> consumerId + "interrupted with exception" + e);
        }
    }

}
