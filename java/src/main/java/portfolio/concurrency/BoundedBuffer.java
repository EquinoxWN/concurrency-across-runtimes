package portfolio.concurrency;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Fixed-capacity FIFO buffer built from one lock and two conditions, so producers wait only
 * for space and consumers wait only for items.
 *
 * @param <T> item type
 */
public final class BoundedBuffer<T> {
    private final Object[] items;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notFull = lock.newCondition();
    private final Condition notEmpty = lock.newCondition();
    private int head;
    private int tail;
    private int count;
    private int maxObserved;
    private boolean closed;

    public BoundedBuffer(int capacity) {
        if (capacity < 1) {
            throw new IllegalArgumentException("capacity must be at least 1");
        }
        this.items = new Object[capacity];
    }

    /** Wait for space, then add; fails once the buffer is closed. */
    public void put(T item) throws InterruptedException {
        Objects.requireNonNull(item, "item");
        lock.lockInterruptibly();
        try {
            while (count == items.length && !closed) {
                notFull.await();
            }
            enqueue(item);
        } finally {
            lock.unlock();
        }
    }

    /** Add if space frees up within the timeout; false on timeout. */
    public boolean offer(T item, Duration timeout) throws InterruptedException {
        Objects.requireNonNull(item, "item");
        long nanos = timeout.toNanos();
        lock.lockInterruptibly();
        try {
            while (count == items.length && !closed) {
                if (nanos <= 0) {
                    return false;
                }
                nanos = notFull.awaitNanos(nanos);
            }
            enqueue(item);
            return true;
        } finally {
            lock.unlock();
        }
    }

    private void enqueue(T item) {
        if (closed) {
            throw new ClosedException();
        }
        items[tail] = item;
        tail = (tail + 1) % items.length;
        count++;
        maxObserved = Math.max(maxObserved, count);
        notEmpty.signal();
    }

    /** Wait for an item; empty once the buffer is closed and drained. */
    public Optional<T> take() throws InterruptedException {
        lock.lockInterruptibly();
        try {
            while (count == 0 && !closed) {
                notEmpty.await();
            }
            return count == 0 ? Optional.empty() : Optional.of(dequeue());
        } finally {
            lock.unlock();
        }
    }

    /** Like take, but gives up after the timeout. */
    public Optional<T> poll(Duration timeout) throws InterruptedException {
        long nanos = timeout.toNanos();
        if (!lock.tryLock(nanos, TimeUnit.NANOSECONDS)) {
            return Optional.empty();
        }
        try {
            while (count == 0 && !closed) {
                if (nanos <= 0) {
                    return Optional.empty();
                }
                nanos = notEmpty.awaitNanos(nanos);
            }
            return count == 0 ? Optional.empty() : Optional.of(dequeue());
        } finally {
            lock.unlock();
        }
    }

    @SuppressWarnings("unchecked")
    private T dequeue() {
        T item = (T) items[head];
        items[head] = null;
        head = (head + 1) % items.length;
        count--;
        notFull.signal();
        return item;
    }

    /** Refuse new items; consumers drain what is left, then see empty. */
    public void close() {
        lock.lock();
        try {
            closed = true;
            notFull.signalAll();
            notEmpty.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /** Close and drop every queued item, waking all waiters. */
    public void abort() {
        lock.lock();
        try {
            closed = true;
            java.util.Arrays.fill(items, null);
            head = 0;
            tail = 0;
            count = 0;
            notFull.signalAll();
            notEmpty.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /** Items currently queued. */
    public int size() {
        lock.lock();
        try {
            return count;
        } finally {
            lock.unlock();
        }
    }

    /** Fixed capacity. */
    public int capacity() {
        return items.length;
    }

    /** Highest number of items ever queued at once. */
    public int maxObserved() {
        lock.lock();
        try {
            return maxObserved;
        } finally {
            lock.unlock();
        }
    }

    /** Thrown by put and offer after close. */
    public static final class ClosedException extends IllegalStateException {
        private static final long serialVersionUID = 1L;

        public ClosedException() {
            super("buffer is closed");
        }
    }
}
