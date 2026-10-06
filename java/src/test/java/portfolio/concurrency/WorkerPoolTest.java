package portfolio.concurrency;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(30)
class WorkerPoolTest {

    @Test
    void everyResultComesBackAndConcurrencyNeverExceedsTheWorkerCount() throws Exception {
        int workers = Scenarios.get("worker_pool.workers");
        int tasks = Scenarios.get("worker_pool.tasks");
        int taskMs = Scenarios.get("worker_pool.task_ms");
        List<CompletableFuture<Integer>> futures = new ArrayList<>();
        WorkerPool pool = new WorkerPool(workers, Scenarios.get("worker_pool.queue_capacity"));
        try (pool) {
            for (int i = 0; i < tasks; i++) {
                int n = i;
                futures.add(pool.submit(() -> {
                    Thread.sleep(Duration.ofMillis(taskMs));
                    return n * n;
                }));
            }
            for (int i = 0; i < tasks; i++) {
                assertEquals(i * i, futures.get(i).get());
            }
        }
        assertEquals(workers, pool.maxActive(), "blocking tasks keep every worker busy, never more");
        assertTrue(pool.terminated());
    }

    @Test
    void aFailingTaskFailsOnlyItsOwnFuture() throws Exception {
        try (WorkerPool pool = new WorkerPool(2, 4)) {
            CompletableFuture<Integer> bad = pool.submit(() -> {
                throw new IllegalStateException("boom");
            });
            CompletableFuture<Integer> good = pool.submit(() -> 42);
            ExecutionException e = assertThrows(ExecutionException.class, bad::get);
            assertEquals("boom", e.getCause().getMessage());
            assertEquals(42, good.get());
            assertEquals(7, pool.submit(() -> 7).get(), "the pool keeps working");
        }
    }

    @Test
    void shutdownFinishesQueuedTasksThenRejectsNewOnes() throws Exception {
        WorkerPool pool = new WorkerPool(2, 32);
        AtomicInteger done = new AtomicInteger();
        for (int i = 0; i < 20; i++) {
            pool.submit(() -> {
                Thread.sleep(5);
                return done.incrementAndGet();
            });
        }
        pool.shutdown();
        assertEquals(20, done.get());
        assertTrue(pool.terminated());
        assertThrows(RejectedExecutionException.class, () -> pool.submit(() -> 1));
    }

    @Test
    void submitBlocksWhenTheQueueIsFull() throws Exception {
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch started = new CountDownLatch(1);
        try (WorkerPool pool = new WorkerPool(1, 1)) {
            pool.submit(() -> {
                started.countDown();
                release.await();
                return 1;
            });
            started.await();
            pool.submit(() -> 2);
            Thread third = Thread.ofPlatform().start(() -> {
                try {
                    pool.submit(() -> 3);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            BoundedBufferTest.awaitState(third, Thread.State.WAITING);
            release.countDown();
            third.join(5_000);
            assertTrue(!third.isAlive(), "submit resumes once the worker frees a slot");
        }
    }
}
