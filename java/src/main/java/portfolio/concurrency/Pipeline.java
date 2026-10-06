package portfolio.concurrency;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Stages connected by bounded buffers, each stage with its own number of virtual-thread
 * workers. A failure in any stage aborts every buffer, so all workers stop and the caller gets
 * one error naming the stage and the item.
 *
 * @param <I> input type
 * @param <O> output type
 */
public final class Pipeline<I, O> {

    /** A stage function that may throw. */
    @FunctionalInterface
    public interface Step<A, B> {
        /** Transform one item. */
        B apply(A input) throws Exception;
    }

    /** Thrown when a stage fails; names the stage and the input index. */
    public static final class PipelineException extends Exception {
        private static final long serialVersionUID = 1L;
        private final String stage;
        private final long index;

        PipelineException(String stage, long index, Throwable cause) {
            super("stage '" + stage + "' failed on item " + index + ": " + cause.getMessage(), cause);
            this.stage = stage;
            this.index = index;
        }

        /** Stage that failed. */
        public String stage() {
            return stage;
        }

        /** Position of the failing item in the input. */
        public long index() {
            return index;
        }
    }

    private record Stage(String name, int workers, Step<Object, Object> step) {
    }

    private record Item(long index, Object value) {
    }

    private record Failure(String stage, long index, Throwable cause) {
    }

    private final List<Stage> stages;
    private final int capacity;
    private final boolean ordered;
    private final List<BoundedBuffer<Item>> lastBuffers = new ArrayList<>();
    private final List<Thread> lastThreads = new ArrayList<>();

    private Pipeline(List<Stage> stages, int capacity, boolean ordered) {
        this.stages = List.copyOf(stages);
        this.capacity = capacity;
        this.ordered = ordered;
    }

    /** Start building a pipeline whose buffers hold at most capacity items each. */
    public static <I> Builder<I, I> builder(int capacity) {
        return new Builder<>(new ArrayList<>(), capacity);
    }

    /** Typed builder; each stage changes the current output type. */
    public static final class Builder<I, C> {
        private final List<Stage> stages;
        private final int capacity;

        private Builder(List<Stage> stages, int capacity) {
            this.stages = stages;
            this.capacity = capacity;
        }

        /** Append a stage run by the given number of workers. */
        @SuppressWarnings("unchecked")
        public <N> Builder<I, N> stage(String name, int workers, Step<? super C, ? extends N> step) {
            if (workers < 1) {
                throw new IllegalArgumentException("stage " + name + " needs at least one worker");
            }
            List<Stage> next = new ArrayList<>(stages);
            next.add(new Stage(name, workers, (Step<Object, Object>) (Step<?, ?>) step));
            return new Builder<>(next, capacity);
        }

        /** Finish; ordered pipelines return results in input order. */
        public Pipeline<I, C> build(boolean ordered) {
            if (stages.isEmpty()) {
                throw new IllegalArgumentException("a pipeline needs at least one stage");
            }
            return new Pipeline<>(stages, capacity, ordered);
        }
    }

    /** Push every input through all stages and collect the outputs. */
    @SuppressWarnings("unchecked")
    public List<O> run(List<I> inputs) throws PipelineException, InterruptedException {
        List<BoundedBuffer<Item>> buffers = new ArrayList<>();
        for (int i = 0; i <= stages.size(); i++) {
            buffers.add(new BoundedBuffer<>(capacity));
        }
        AtomicReference<Failure> failure = new AtomicReference<>();
        List<Thread> threads = new ArrayList<>();
        threads.add(Thread.ofVirtual().name("feeder").start(() -> feed(inputs, buffers.getFirst(), failure)));
        for (int s = 0; s < stages.size(); s++) {
            Stage stage = stages.get(s);
            BoundedBuffer<Item> in = buffers.get(s);
            BoundedBuffer<Item> out = buffers.get(s + 1);
            AtomicInteger running = new AtomicInteger(stage.workers());
            for (int w = 0; w < stage.workers(); w++) {
                threads.add(Thread.ofVirtual().name(stage.name() + "-" + w)
                        .start(() -> work(stage, in, out, running, buffers, failure)));
            }
        }
        List<Item> results = new ArrayList<>();
        try {
            BoundedBuffer<Item> last = buffers.getLast();
            for (Optional<Item> next = last.take(); next.isPresent(); next = last.take()) {
                results.add(next.get());
            }
        } finally {
            if (failure.get() == null && results.size() != inputs.size()) {
                buffers.forEach(BoundedBuffer::abort);
            }
            for (Thread t : threads) {
                t.join();
            }
            synchronized (lastThreads) {
                lastThreads.clear();
                lastThreads.addAll(threads);
                lastBuffers.clear();
                lastBuffers.addAll(buffers);
            }
        }
        Failure f = failure.get();
        if (f != null) {
            throw new PipelineException(f.stage(), f.index(), f.cause());
        }
        if (ordered) {
            results.sort(Comparator.comparingLong(Item::index));
        }
        return results.stream().map(item -> (O) item.value()).toList();
    }

    private static <I> void feed(List<I> inputs, BoundedBuffer<Item> first, AtomicReference<Failure> failure) {
        try {
            long index = 0;
            for (I input : inputs) {
                first.put(new Item(index++, input));
            }
            first.close();
        } catch (BoundedBuffer.ClosedException e) {
            // aborted by a failing stage
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void work(Stage stage, BoundedBuffer<Item> in, BoundedBuffer<Item> out, AtomicInteger running,
            List<BoundedBuffer<Item>> all, AtomicReference<Failure> failure) {
        try {
            for (Optional<Item> next = in.take(); next.isPresent(); next = in.take()) {
                Item item = next.get();
                Object value;
                try {
                    value = stage.step().apply(item.value());
                } catch (Exception e) {
                    if (failure.compareAndSet(null, new Failure(stage.name(), item.index(), e))) {
                        all.forEach(BoundedBuffer::abort);
                    }
                    return;
                }
                out.put(new Item(item.index(), value));
            }
        } catch (BoundedBuffer.ClosedException e) {
            // aborted by another stage
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            if (running.decrementAndGet() == 0) {
                out.close();
            }
        }
    }

    /** Highest occupancy of each buffer in the last run (feeder buffer first). */
    public List<Integer> lastMaxOccupancy() {
        synchronized (lastThreads) {
            return lastBuffers.stream().map(BoundedBuffer::maxObserved).toList();
        }
    }

    /** True when every thread of the last run has exited. */
    public boolean lastRunTerminated() {
        synchronized (lastThreads) {
            return lastThreads.stream().noneMatch(Thread::isAlive);
        }
    }
}
