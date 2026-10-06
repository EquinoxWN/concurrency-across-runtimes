package portfolio.concurrency;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/** Prints how the JVM schedules blocking and CPU-bound work on virtual and platform threads. */
public final class Scheduling {
    private Scheduling() {
    }

    /** Run the three experiments and print a Markdown table. */
    public static void main(String[] args) throws Exception {
        int cores = Runtime.getRuntime().availableProcessors();
        System.out.println("| Experiment (Java " + Runtime.version().feature() + ", " + cores + " cores) | Wall time | OS threads used |");
        System.out.println("|---|---|---|");
        sleepers("10,000 tasks sleeping 100 ms, one virtual thread each", 10_000,
                Executors.newVirtualThreadPerTaskExecutor());
        sleepers("1,000 tasks sleeping 100 ms, pool of 50 platform threads", 1_000,
                Executors.newFixedThreadPool(50));
        cpu(cores);
    }

    private static void sleepers(String label, int tasks, ExecutorService executor) throws Exception {
        Set<String> carriers = ConcurrentHashMap.newKeySet();
        long start = System.nanoTime();
        try (executor) {
            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < tasks; i++) {
                futures.add(executor.submit(() -> {
                    carriers.add(carrier());
                    Thread.sleep(Duration.ofMillis(100));
                    return null;
                }));
            }
            for (Future<?> f : futures) {
                f.get();
            }
        }
        System.out.printf("| %s | %d ms | %d |%n", label, (System.nanoTime() - start) / 1_000_000, carriers.size());
    }

    private static void cpu(int cores) throws Exception {
        long single = time(1, ConcurrentHashMap.newKeySet());
        Set<String> carriers = ConcurrentHashMap.newKeySet();
        long parallel = time(cores, carriers);
        System.out.printf("| CPU-bound: %d busy tasks on virtual threads vs the same work on 1 thread | %d ms vs %d ms (%.1fx) | %d |%n",
                cores, parallel, single * cores, (double) single * cores / parallel, carriers.size());
    }

    private static long time(int tasks, Set<String> carriers) throws Exception {
        long start = System.nanoTime();
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<Long>> futures = new ArrayList<>();
            for (int i = 0; i < tasks; i++) {
                futures.add(executor.submit(() -> {
                    carriers.add(carrier());
                    return spin();
                }));
            }
            for (Future<Long> f : futures) {
                f.get();
            }
        }
        return Math.max(1, (System.nanoTime() - start) / 1_000_000);
    }

    private static long spin() {
        long x = 0;
        for (long i = 0; i < 400_000_000L; i++) {
            x += i ^ (x >>> 3);
        }
        return x;
    }

    /** Name of the OS thread running the current (possibly virtual) thread. */
    private static String carrier() {
        String s = Thread.currentThread().toString();
        int at = s.indexOf('@');
        return at >= 0 ? s.substring(at + 1) : Thread.currentThread().getName();
    }
}
