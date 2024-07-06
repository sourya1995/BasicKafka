package Kafka;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.logging.Logger;

public class Producer implements Runnable {

    private static final Logger LOGGER = Logger.getLogger(Producer.class.getName());
    private String producerId;
    private Topic topic;
    private int numPartitions;
    private ExecutorService executorService;

    public Producer(String producerId, Topic topic, int numPartitions, ExecutorService executorService) {
        this.producerId = producerId;
        this.topic = topic;
        this.numPartitions = numPartitions;
        this.executorService = executorService;
    }

    @Override
    public void run() {
        try {
            for (int i = 0; i < 10; i++) {
                String messageContent = producerId + "message" + i;
                Message message = new Message(messageContent, i);
                boolean sent = false;
                while (!sent) {
                    sent = sendMessage(message);
                }
                Thread.sleep(1000);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.severe(() -> "Producer interrupted with Exception" + e);
        }
    }

    private boolean sendMessage(Message message) {
        int partition = Math.abs(message.getContent().hashCode()) % numPartitions;
        executorService.submit(() -> {
            try {
                boolean acknowledged = topic.produce(message.getContent(), partition);
                if (!acknowledged) {
                    handleDeliveryFailure(message, partition);
                    return false;
                }
                LOGGER.info("Produced to topic" + topic.getName() + "partition: " + partition + ":" + message);
                return true;
            } catch (Exception e) {
                LOGGER.severe(() -> "Error Sending message:" + "Exception" + e);
                handleDeliveryFailure(message, partition);
                return false;
            }
        });
        return false;
    }

    private void handleDeliveryFailure(Message message, int partition) {
        LOGGER.warning(() -> "retrying processing message");

        Runnable retryTask = () -> {
            try {
                Thread.sleep(1000);
                sendMessage(message);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                LOGGER.severe(() -> "Exception" + ex);

            }
        };

        executorService.submit(retryTask);
    }

}
