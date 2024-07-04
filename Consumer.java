import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;

public class Consumer implements Runnable {

    private static final Logger LOGGER = Logger.getLogger(Consumer.class.getName());
    private BlockingQueue<Message> queue;
    private String consumerName;

    public Consumer(String consumerName, BlockingQueue<Message> queue) {
        this.consumerName = consumerName;
        this.queue = queue;
    }

    @Override
    public void run() {
        try {
            while (true) {
                Message message = queue.take();
                LOGGER.info(() -> consumerName + "consumed" + message);
                Thread.sleep(2000);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.severe(() -> consumerName + "interrupted with exception" + e);
        }
    }

}
