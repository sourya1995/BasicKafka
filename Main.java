import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

public class Main {
    public static void main(String[] args) {
        int numPartitions = 3;
        Topic topic = new Topic("myTopic", numPartitions);

        ExecutorService executor = Executors.newFixedThreadPool(4);

        // Start producers
        executor.submit(new Producer("Producer1", topic, numPartitions));
        executor.submit(new Producer("Producer2", topic, numPartitions));

        // Start consumers
        executor.submit(new Consumer("Consumer1", topic, numPartitions));
        executor.submit(new Consumer("Consumer2", topic, numPartitions));

        executor.shutdown();
    }
}
