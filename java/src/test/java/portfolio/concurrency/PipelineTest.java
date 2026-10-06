package portfolio.concurrency;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(30)
class PipelineTest {
    private static final int ITEMS = Scenarios.get("pipeline.items");
    private static final int CAPACITY = Scenarios.get("pipeline.queue_capacity");
    private static final List<String> INPUTS = IntStream.rangeClosed(1, ITEMS).mapToObj(Integer::toString).toList();
    private static final List<String> EXPECTED = IntStream.rangeClosed(1, ITEMS)
            .mapToObj(i -> i + "^2=" + (long) i * i).toList();

    private static final AtomicInteger FORMATTED = new AtomicInteger();

    private static Pipeline<String, String> pipeline(boolean ordered, int failAt) {
        FORMATTED.set(0);
        return Pipeline.<String>builder(CAPACITY)
                .stage("parse", Scenarios.get("pipeline.workers.parse"), Integer::parseInt)
                .stage("square", Scenarios.get("pipeline.workers.square"), (Integer n) -> {
                    if (n == failAt) {
                        throw new IllegalArgumentException("cannot square " + n);
                    }
                    return new long[] {n, (long) n * n};
                })
                .stage("format", Scenarios.get("pipeline.workers.format"), (long[] p) -> {
                    FORMATTED.incrementAndGet();
                    return p[0] + "^2=" + p[1];
                })
                .build(ordered);
    }

    @Test
    void orderedOutputEqualsTheSequentialResult() throws Exception {
        Pipeline<String, String> p = pipeline(true, -1);
        assertEquals(EXPECTED, p.run(INPUTS));
        assertTrue(p.lastRunTerminated());
        p.lastMaxOccupancy().forEach(max -> assertTrue(max <= CAPACITY, "buffer over capacity: " + max));
    }

    @Test
    void unorderedOutputHasTheSameItems() throws Exception {
        List<String> out = pipeline(false, -1).run(INPUTS);
        assertEquals(EXPECTED.stream().sorted().toList(), out.stream().sorted().toList());
    }

    @Test
    void oneFailureStopsEveryStageAndNamesTheItem() throws Exception {
        int failAt = Scenarios.get("pipeline.fail_at");
        Pipeline<String, String> p = pipeline(true, failAt);
        Pipeline.PipelineException e = assertThrows(Pipeline.PipelineException.class, () -> p.run(INPUTS));
        assertEquals("square", e.stage());
        assertEquals(failAt - 1, e.index());
        assertEquals("stage 'square' failed on item " + (failAt - 1) + ": cannot square " + failAt, e.getMessage());
        assertTrue(p.lastRunTerminated(), "no worker is left running");
    }

    @Test
    void aFailureEarlyInALongInputStopsTheRemainingWork() throws Exception {
        int items = Scenarios.get("pipeline.early_stop_items");
        List<String> inputs = IntStream.rangeClosed(1, items).mapToObj(Integer::toString).toList();
        Pipeline<String, String> p = pipeline(true, Scenarios.get("pipeline.fail_at"));
        assertThrows(Pipeline.PipelineException.class, () -> p.run(inputs));
        assertTrue(FORMATTED.get() < items / 2, FORMATTED.get() + " of " + items + " items were still formatted");
        assertTrue(p.lastRunTerminated());
    }

    @Test
    void aSlowLastStageFillsTheBuffersButNeverOverflowsThem() throws Exception {
        Pipeline<Integer, Integer> p = Pipeline.<Integer>builder(4)
                .stage("fast", 4, (Integer x) -> x + 1)
                .stage("slow", 1, (Integer x) -> {
                    Thread.sleep(1);
                    return x;
                })
                .build(true);
        p.run(IntStream.range(0, 200).boxed().toList());
        List<Integer> max = p.lastMaxOccupancy();
        assertEquals(4, (int) max.get(1), "the fast stage is held back by the full buffer");
        max.forEach(m -> assertTrue(m <= 4));
    }

    @Test
    void emptyInputAndBadConfigurations() throws Exception {
        assertEquals(List.of(), pipeline(true, -1).run(List.of()));
        assertThrows(IllegalArgumentException.class, () -> Pipeline.<String>builder(2).build(true));
        assertThrows(IllegalArgumentException.class,
                () -> Pipeline.<String>builder(2).stage("x", 0, (String s) -> s));
    }
}
