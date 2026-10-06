package portfolio.concurrency;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A fixed number of virtual-thread workers pulling tasks from a bounded queue. The bound gives
 * backpressure: submit blocks when the queue is full instead of growing memory without limit.
 */
public final class WorkerPool implements AutoCloseable {
    private final BoundedBuffer<Runnable> queue;
    private final List<Thread> workers = new ArrayList<>();
    private final AtomicInteger active = new AtomicInteger();
    private final AtomicInteger maxActive = new AtomicInteger();

    public WorkerPool(int workerCount, int queueCapacity) {
        if (workerCount < 1) {
            throw new IllegalArgumentException("need at least one worker");
        }
        this.queue = new BoundedBuffer<>(queueCapacity);
        for (int i = 0; i < workerCount; i++) {
            workers.add(Thread.ofVirtual().name("pool-worker-" + i).start(this::work));
        }
    }

    /** Queue a task (blocking while the queue is full); its result arrives in the future. */
    public <T> CompletableFuture<T> submit(Callable<T> task) throws InterruptedException {
        CompletableFuture<T> result = new CompletableFuture<>();
        try {
            queue.put(() -> run(task, result));
        } catch (BoundedBuffer.ClosedException e) {
            throw new RejectedExecutionException("pool is shut down", e);
        }
        return result;
    }

    private <T> void run(Callable<T> task, CompletableFuture<T> result) {
        int now = active.incrementAndGet();
        maxActive.accumulateAndGet(now, Math::max);
        try {
            result.complete(task.call());
        } catch (Exception e) {
            result.completeExceptionally(e);
        } finally {
            active.decrementAndGet();
        }
    }

    private void work() {
        try {
            while (true) {
                Optional<Runnable> next = queue.take();
                if (next.isEmpty()) {
                    return;
                }
                next.get().run();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Stop accepting tasks, finish the queued ones and wait for every worker. */
    public void shutdown() throws InterruptedException {
        queue.close();
        for (Thread t : workers) {
            t.join();
        }
    }

    /** Highest number of tasks that ever ran at the same time. */
    public int maxActive() {
        return maxActive.get();
    }

    /** True once every worker thread has exited. */
    public boolean terminated() {
        return workers.stream().noneMatch(Thread::isAlive);
    }

    @Override
    public void close() {
        try {
            shutdown();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
