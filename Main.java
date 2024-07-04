import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

public class Main {
    public static void main(String[] args) {
        BlockingQueue<Message> queue = new LinkedBlockingQueue<>();
        ExecutorService executor = Executors.newFixedThreadPool(2);

        executor.submit(new Producer("Producer 1", queue));

        executor.submit(new Consumer("Consumer 1", queue));

        executor.shutdown();
    }
}
