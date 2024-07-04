import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;

public class Producer implements Runnable {

    private static final Logger LOGGER = Logger.getLogger(Producer.class.getName());
    private BlockingQueue<Message> queue;
    private String producerName;

    public Producer(String producerName, BlockingQueue<Message> queue) {
        this.producerName = producerName;
        this.queue = queue;
    }

    @Override
    public void run() {
        try {
            for (int i = 0; i < 10; i++) {
                String messageContent = producerName + "message" + i;
                Message message = new Message(messageContent);
                queue.put(message);
                System.out.println("Produced" + message);
                Thread.sleep(1000);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.severe(() -> "Producer interrupted");
        }
    }

}
