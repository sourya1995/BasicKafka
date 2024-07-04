import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;

public class Producer implements Runnable {

    private static final Logger LOGGER = Logger.getLogger(Producer.class.getName());
    private String producerId;
    private Topic topic;
    private int numPartitions;

    public Producer(String producerId, Topic topic, int numPartitions) {
        this.producerId = producerId;
        this.topic = topic;
        this.numPartitions = numPartitions;
    }

    @Override
    public void run() {
        try {
            for (int i = 0; i < 10; i++) {
                String messageContent = producerId + "message" + i;
                topic.produce(messageContent);
                LOGGER.info(() -> "Produced message: ");
                Thread.sleep(1000);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.severe(() -> "Producer interrupted with Exception" + e);
        }
    }

}
