import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.logging.Logger;

public class Consumer implements Runnable {

    private static final Logger LOGGER = Logger.getLogger(Consumer.class.getName());
    private String consumerId;
    private Topic topic;
    private int numPartitions;
    private int[] offsets;
    private ExecutorService executorService;

    public Consumer(String consumerId, Topic topic, int numPartitions, ExecutorService executorService) {
        this.consumerId = consumerId;
        this.topic = topic;
        this.numPartitions = numPartitions;
        this.offsets = new int[numPartitions];
        this.executorService = executorService;
    }

    @Override
    public void run() {
        try {
            while (true) {
                for (int partition = 0; partition < numPartitions; partition++) {
                    while (offsets[partition] < topic.getPartitionSize(partition)) {
                        Optional<Message> message = topic.consume(consumerId);
                        if (message != null) {
                            offsets[partition]++;
                        }
                        Thread.sleep(1000);
                    }
                }
                Thread.sleep(2000);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.severe(() -> consumerId + "interrupted with exception" + e);
        }
    }

    private void processMessage(Message message) {
        LOGGER.info(() -> "COnsumer" + consumerId + "processed message" + message);

        if (Math.random() < 0.1) {
            LOGGER.warning(() -> "error processing message");
            handleProcessingError(message);
        }
    }

    private void handleProcessingError(Message message) {
        LOGGER.warning(() -> "retrying processing message");

        Runnable retryTask = () -> {
            try {
                Thread.sleep(1000);
                processMessage(message);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                LOGGER.severe(() -> "Exception" + ex);

            }
        };

        executorService.submit(retryTask);
    }

}
