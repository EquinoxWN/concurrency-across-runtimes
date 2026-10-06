package portfolio.concurrency;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Philosophers share forks; each needs both neighbours to eat. Deadlock is detected with
 * timed lock attempts rather than hanging, so the naive strategy can be shown failing safely.
 */
public final class DiningPhilosophers {
    private DiningPhilosophers() {
    }

    /** How a philosopher picks up forks. */
    public enum Strategy {
        /** Left fork, then right: a circular wait is possible. */
        NAIVE,
        /** Lower-numbered fork first: breaks the circular wait. */
        ORDERED,
        /** At most n-1 philosophers at the table: the cycle can never close. */
        SEATS
    }

    /**
     * Result of one dinner.
     *
     * @param deadlocked a philosopher timed out waiting for its second fork while holding its first
     * @param cycleFormed every philosopher held its first fork at the same moment
     * @param meals meals eaten by each philosopher
     * @param violations times a fork was found held by someone else when taken
     */
    public record Outcome(boolean deadlocked, boolean cycleFormed, List<Integer> meals, int violations) {
    }

    /**
     * Run a dinner. With {@code forceCycle}, everyone tries to hold the first fork at the same
     * moment before reaching for the second, which is exactly the schedule that deadlocks.
     */
    public static Outcome dine(int n, int meals, Strategy strategy, boolean forceCycle, Duration timeout)
            throws InterruptedException {
        ReentrantLock[] forks = new ReentrantLock[n];
        AtomicIntegerArray owner = new AtomicIntegerArray(n);
        for (int i = 0; i < n; i++) {
            forks[i] = new ReentrantLock();
            owner.set(i, -1);
        }
        Semaphore seats = new Semaphore(strategy == Strategy.SEATS ? n - 1 : n);
        CyclicBarrier holding = new CyclicBarrier(n);
        AtomicBoolean deadlocked = new AtomicBoolean();
        AtomicBoolean cycle = new AtomicBoolean();
        AtomicInteger violations = new AtomicInteger();
        AtomicIntegerArray eaten = new AtomicIntegerArray(n);
        List<Thread> threads = new ArrayList<>();
        for (int p = 0; p < n; p++) {
            int id = p;
            threads.add(Thread.ofVirtual().name("philosopher-" + p).start(() -> {
                int left = id;
                int right = (id + 1) % n;
                int first = strategy == Strategy.ORDERED ? Math.min(left, right) : left;
                int second = first == left ? right : left;
                try {
                    for (int meal = 0; meal < meals && !deadlocked.get(); meal++) {
                        boolean forced = forceCycle && meal == 0;
                        if (!eat(id, first, second, forks, owner, seats, holding, forced, timeout, cycle,
                                violations)) {
                            deadlocked.set(true);
                            return;
                        }
                        eaten.incrementAndGet(id);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }));
        }
        for (Thread t : threads) {
            t.join();
        }
        List<Integer> counts = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            counts.add(eaten.get(i));
        }
        return new Outcome(deadlocked.get(), cycle.get(), counts, violations.get());
    }

    private static boolean eat(int id, int first, int second, ReentrantLock[] forks, AtomicIntegerArray owner,
            Semaphore seats, CyclicBarrier holding, boolean forced, Duration timeout, AtomicBoolean cycle,
            AtomicInteger violations) throws InterruptedException {
        seats.acquire();
        try {
            forks[first].lockInterruptibly();
            try {
                take(owner, first, id, violations);
                if (forced) {
                    awaitEveryoneHolding(holding, timeout, cycle);
                }
                if (!forks[second].tryLock(timeout.toNanos(), TimeUnit.NANOSECONDS)) {
                    release(owner, first, id);
                    return false;
                }
                try {
                    take(owner, second, id, violations);
                    release(owner, second, id);
                } finally {
                    forks[second].unlock();
                }
                release(owner, first, id);
                return true;
            } finally {
                if (forks[first].isHeldByCurrentThread()) {
                    forks[first].unlock();
                }
            }
        } finally {
            seats.release();
        }
    }

    private static void awaitEveryoneHolding(CyclicBarrier holding, Duration timeout, AtomicBoolean cycle)
            throws InterruptedException {
        try {
            holding.await(timeout.toNanos(), TimeUnit.NANOSECONDS);
            cycle.set(true);
        } catch (TimeoutException | BrokenBarrierException e) {
            // the cycle could not form: someone is still waiting for a first fork or a seat
        }
    }

    private static void take(AtomicIntegerArray owner, int fork, int id, AtomicInteger violations) {
        if (!owner.compareAndSet(fork, -1, id)) {
            violations.incrementAndGet();
        }
    }

    private static void release(AtomicIntegerArray owner, int fork, int id) {
        owner.compareAndSet(fork, id, -1);
    }
}
