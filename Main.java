import java.util.concurrent.*;

public class Main {
    public static void main(String[] args) {
        int numPartitions = 3;
        int numReplicas = 2;
        Topic topic = new Topic("myTopic", numPartitions, numReplicas);

        try (ScheduledExecutorService executor = Executors.newScheduledThreadPool(10)) {

            // Start producers
            for (int i = 1; i <= 2; i++) {
                executor.submit(new Producer("Producer" + i, topic, numPartitions, executor));
            }

            // Start consumers
            for (int i = 1; i <= 4; i++) {
                executor.submit(new Consumer("Consumer" + i, topic, numPartitions, executor));
            }

            // Simulate scaling up and down
            executor.schedule(() -> {
                for (int i = 3; i <= 5; i++) {
                    executor.submit(new Producer("Producer" + i, topic, numPartitions, executor));
                }
            }, 30, TimeUnit.SECONDS);

            executor.shutdown();

            executor.shutdown();
        }
    }
}
