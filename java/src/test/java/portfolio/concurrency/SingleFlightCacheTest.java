package portfolio.concurrency;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(30)
class SingleFlightCacheTest {

    @Test
    void concurrentMissesShareOneLoad() throws Exception {
        int requests = Scenarios.get("cache.concurrent_requests");
        int loadMs = Scenarios.get("cache.load_ms");
        SingleFlightCache<String, Object> cache = new SingleFlightCache<>(3, key -> {
            Thread.sleep(Duration.ofMillis(loadMs));
            return new Object();
        });
        CountDownLatch go = new CountDownLatch(1);
        Set<Object> seen = ConcurrentHashMap.newKeySet();
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < requests; i++) {
            threads.add(Thread.ofVirtual().start(() -> {
                try {
                    go.await();
                    seen.add(cache.get("report"));
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }));
        }
        go.countDown();
        for (Thread t : threads) {
            t.join();
        }
        assertEquals(1, cache.loads(), "the loader ran once for " + requests + " callers");
        assertEquals(1, seen.size(), "every caller got the same value");
    }

    @Test
    void differentKeysLoadInParallel() throws Exception {
        CountDownLatch bStarted = new CountDownLatch(1);
        SingleFlightCache<String, String> cache = new SingleFlightCache<>(3, key -> {
            if (key.equals("a")) {
                if (!bStarted.await(5, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("b never started while a was loading");
                }
            } else {
                bStarted.countDown();
            }
            return key.toUpperCase(java.util.Locale.ROOT);
        });
        Thread a = Thread.ofVirtual().start(() -> {
            try {
                assertEquals("A", cache.get("a"));
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        });
        assertEquals("B", cache.get("b"));
        a.join();
        assertEquals(2, cache.loads());
    }

    @Test
    void aFailedLoadIsNotCachedAndEveryWaiterSeesTheError() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        CountDownLatch go = new CountDownLatch(1);
        SingleFlightCache<String, String> cache = new SingleFlightCache<>(3, key -> {
            if (attempts.incrementAndGet() == 1) {
                Thread.sleep(300);
                throw new IllegalStateException("database down");
            }
            return "ok";
        });
        AtomicInteger failures = new AtomicInteger();
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            threads.add(Thread.ofVirtual().start(() -> {
                try {
                    go.await();
                    cache.get("k");
                } catch (IllegalStateException e) {
                    failures.incrementAndGet();
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }));
        }
        go.countDown();
        for (Thread t : threads) {
            t.join();
        }
        assertEquals(1, cache.loads());
        assertEquals(8, failures.get(), "every waiter of the failed flight sees the error");
        assertEquals("ok", cache.get("k"), "the next request retries");
    }

    @Test
    void leastRecentlyUsedKeyIsEvicted() throws Exception {
        int capacity = Scenarios.get("cache.capacity");
        SingleFlightCache<String, String> cache = new SingleFlightCache<>(capacity, key -> key + "!");
        for (String k : List.of("a", "b", "c", "a", "d")) {
            cache.get(k);
        }
        assertEquals(List.of("c", "a", "d"), cache.keys());
        assertEquals(4, cache.loads());
        cache.get("b");
        assertEquals(5, cache.loads(), "b was evicted, so it loads again");
        assertEquals(capacity, cache.keys().size());
    }

    @Test
    void nullValuesAndBadCapacityAreRejected() {
        SingleFlightCache<String, String> cache = new SingleFlightCache<>(1, key -> null);
        assertThrows(NullPointerException.class, () -> cache.get("x"));
        assertThrows(IllegalArgumentException.class, () -> new SingleFlightCache<String, String>(0, k -> k));
    }
}
