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
                        Optional<Message> message = topic.consume(consumerId, partition, offsets[partition]);
                        if (message != null) {
                            processMessage(message);
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

    private void processMessage(Optional<Message> message) {
        LOGGER.info(() -> "COnsumer" + consumerId + "processed message" + message);

        if (Math.random() < 0.1) {
            LOGGER.warning(() -> "error processing message");
            handleProcessingError(message);
        } else{
            acknowledgeMessage(message);
        }
    }

    // Utility method to safely retrieve the partition from an Optional<Message>
    private int getMessagePartition(Optional<Message> messageOptional) {
        if (messageOptional.isPresent()) {
            return messageOptional.get().getPartition();
        } else {
            throw new IllegalStateException("No message present in Optional");
        }
    }

    private void handleProcessingError(Optional<Message> messageOptional) {
        LOGGER.warning(() -> "retrying processing message");
        int partition = getMessagePartition(messageOptional);
        offsets[partition]--;

        Runnable retryTask = () -> {
            try {
                Thread.sleep(1000);
                processMessage(messageOptional); // Ensure this also handles Optional correctly
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                LOGGER.severe(() -> "Exception" + ex);
            }
        };

        executorService.submit(retryTask);
    }

    private void acknowledgeMessage(Optional<Message> message) {
        // Simulate acknowledging the message (committing offset)
        if (message.isPresent()) {
            Message msg = message.get();
            LOGGER.info(() -> "Consumer '" + consumerId + "' acknowledged message: " + msg);
            topic.commitOffset(consumerId, msg.getPartition(), offsets[msg.getPartition()]);
        } else {
            LOGGER.warning(() -> "Acknowledging failed because message was not present.");
        }
    }

}
