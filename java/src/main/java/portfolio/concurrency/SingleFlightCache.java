package portfolio.concurrency;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Memoizing LRU cache where concurrent misses on one key share a single load. The loader runs
 * outside the lock, so loads of different keys proceed in parallel.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class SingleFlightCache<K, V> {

    /** Loads a value for a key; may throw. */
    @FunctionalInterface
    public interface Loader<K, V> {
        /** Produce the value for a key. */
        V load(K key) throws Exception;
    }

    private final int capacity;
    private final Loader<K, V> loader;
    private final ReentrantLock lock = new ReentrantLock();
    private final LinkedHashMap<K, V> values = new LinkedHashMap<>(16, 0.75f, true);
    private final Map<K, CompletableFuture<V>> inFlight = new HashMap<>();
    private final LongAdder loads = new LongAdder();

    public SingleFlightCache(int capacity, Loader<K, V> loader) {
        if (capacity < 1) {
            throw new IllegalArgumentException("capacity must be at least 1");
        }
        this.capacity = capacity;
        this.loader = Objects.requireNonNull(loader, "loader");
    }

    /** Cached value, or the result of the one load shared by all concurrent callers. */
    public V get(K key) throws Exception {
        CompletableFuture<V> flight;
        boolean leader = false;
        lock.lock();
        try {
            V cached = values.get(key);
            if (cached != null) {
                return cached;
            }
            flight = inFlight.get(key);
            if (flight == null) {
                flight = new CompletableFuture<>();
                inFlight.put(key, flight);
                leader = true;
            }
        } finally {
            lock.unlock();
        }
        if (leader) {
            load(key, flight);
        }
        try {
            return flight.get();
        } catch (ExecutionException e) {
            throw e.getCause() instanceof Exception cause ? cause : e;
        }
    }

    private void load(K key, CompletableFuture<V> flight) {
        loads.increment();
        try {
            V value = Objects.requireNonNull(loader.load(key), "loader returned null");
            lock.lock();
            try {
                values.put(key, value);
                while (values.size() > capacity) {
                    K eldest = values.keySet().iterator().next();
                    values.remove(eldest);
                }
                inFlight.remove(key);
            } finally {
                lock.unlock();
            }
            flight.complete(value);
        } catch (Exception e) {
            lock.lock();
            try {
                inFlight.remove(key);
            } finally {
                lock.unlock();
            }
            flight.completeExceptionally(e);
        }
    }

    /** Number of times the loader has been called. */
    public long loads() {
        return loads.sum();
    }

    /** Keys currently cached, least recently used first. */
    public java.util.List<K> keys() {
        lock.lock();
        try {
            return java.util.List.copyOf(values.keySet());
        } finally {
            lock.unlock();
        }
    }
}
