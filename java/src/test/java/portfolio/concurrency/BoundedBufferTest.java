package portfolio.concurrency;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(30)
class BoundedBufferTest {

    @Test
    void everyItemIsConsumedExactlyOnceAndCapacityIsNeverExceeded() throws Exception {
        int capacity = Scenarios.get("bounded_buffer.capacity");
        int producers = Scenarios.get("bounded_buffer.producers");
        int consumers = Scenarios.get("bounded_buffer.consumers");
        int perProducer = Scenarios.get("bounded_buffer.items_per_producer");
        BoundedBuffer<Integer> buffer = new BoundedBuffer<>(capacity);
        List<List<Integer>> received = new ArrayList<>();
        List<Thread> consumerThreads = new ArrayList<>();
        for (int c = 0; c < consumers; c++) {
            List<Integer> mine = new ArrayList<>();
            received.add(mine);
            consumerThreads.add(Thread.ofVirtual().start(() -> {
                try {
                    for (Optional<Integer> x = buffer.take(); x.isPresent(); x = buffer.take()) {
                        mine.add(x.get());
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }));
        }
        List<Thread> producerThreads = new ArrayList<>();
        for (int p = 0; p < producers; p++) {
            int id = p;
            producerThreads.add(Thread.ofVirtual().start(() -> {
                try {
                    for (int seq = 0; seq < perProducer; seq++) {
                        buffer.put(id * 1_000_000 + seq);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }));
        }
        for (Thread t : producerThreads) {
            t.join();
        }
        buffer.close();
        for (Thread t : consumerThreads) {
            t.join();
        }
        Set<Integer> all = new HashSet<>();
        int total = 0;
        for (List<Integer> mine : received) {
            total += mine.size();
            all.addAll(mine);
            int[] last = new int[producers];
            java.util.Arrays.fill(last, -1);
            for (int x : mine) {
                int producer = x / 1_000_000;
                int seq = x % 1_000_000;
                assertTrue(seq > last[producer], "producer " + producer + " out of order");
                last[producer] = seq;
            }
        }
        assertEquals(producers * perProducer, total, "no item lost or duplicated");
        assertEquals(producers * perProducer, all.size());
        assertTrue(buffer.maxObserved() <= capacity);
        assertEquals(capacity, buffer.maxObserved(), "producers outpace consumers, so the buffer fills");
    }

    @Test
    void closeLetsConsumersDrainThenSeeEmpty() throws Exception {
        BoundedBuffer<String> buffer = new BoundedBuffer<>(3);
        buffer.put("a");
        buffer.put("b");
        buffer.close();
        assertThrows(BoundedBuffer.ClosedException.class, () -> buffer.put("c"));
        assertEquals(Optional.of("a"), buffer.take());
        assertEquals(Optional.of("b"), buffer.take());
        assertEquals(Optional.empty(), buffer.take());
    }

    @Test
    void aProducerBlocksWhileFullAndResumesWhenSpaceFrees() throws Exception {
        BoundedBuffer<Integer> buffer = new BoundedBuffer<>(1);
        buffer.put(1);
        Thread producer = Thread.ofPlatform().start(() -> {
            try {
                buffer.put(2);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        awaitState(producer, Thread.State.WAITING);
        assertEquals(1, buffer.size());
        assertEquals(Optional.of(1), buffer.take());
        producer.join(5_000);
        assertFalse(producer.isAlive());
        assertEquals(Optional.of(2), buffer.take());
    }

    @Test
    void abortWakesEveryWaiterAndDropsItems() throws Exception {
        BoundedBuffer<Integer> full = new BoundedBuffer<>(1);
        full.put(1);
        AtomicReference<Throwable> producerError = new AtomicReference<>();
        Thread producer = Thread.ofPlatform().start(() -> {
            try {
                full.put(2);
            } catch (Throwable t) {
                producerError.set(t);
            }
        });
        BoundedBuffer<Integer> empty = new BoundedBuffer<>(1);
        AtomicReference<Optional<Integer>> taken = new AtomicReference<>();
        Thread consumer = Thread.ofPlatform().start(() -> {
            try {
                taken.set(empty.take());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        awaitState(producer, Thread.State.WAITING);
        awaitState(consumer, Thread.State.WAITING);
        full.abort();
        empty.abort();
        producer.join(5_000);
        consumer.join(5_000);
        assertTrue(producerError.get() instanceof BoundedBuffer.ClosedException);
        assertEquals(Optional.empty(), taken.get());
        assertEquals(0, full.size());
    }

    @Test
    void timedOperationsGiveUp() throws Exception {
        BoundedBuffer<Integer> buffer = new BoundedBuffer<>(1);
        assertEquals(Optional.empty(), buffer.poll(Duration.ofMillis(20)));
        assertTrue(buffer.offer(1, Duration.ofMillis(20)));
        assertFalse(buffer.offer(2, Duration.ofMillis(20)));
    }

    @Test
    void aBlockedConsumerCanBeInterrupted() throws Exception {
        BoundedBuffer<Integer> buffer = new BoundedBuffer<>(1);
        AtomicReference<Throwable> error = new AtomicReference<>();
        Thread consumer = Thread.ofPlatform().start(() -> {
            try {
                buffer.take();
            } catch (Throwable t) {
                error.set(t);
            }
        });
        awaitState(consumer, Thread.State.WAITING);
        consumer.interrupt();
        consumer.join(5_000);
        assertTrue(error.get() instanceof InterruptedException);
    }

    @Test
    void invalidUseIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> new BoundedBuffer<>(0));
        assertThrows(NullPointerException.class, () -> new BoundedBuffer<String>(1).put(null));
    }

    static void awaitState(Thread t, Thread.State state) throws InterruptedException {
        long deadline = System.nanoTime() + 5_000_000_000L;
        while (t.getState() != state) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError(t.getName() + " never reached " + state + ", is " + t.getState());
            }
            Thread.sleep(1);
        }
    }
}
