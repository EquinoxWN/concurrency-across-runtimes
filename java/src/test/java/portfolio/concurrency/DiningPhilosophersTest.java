package portfolio.concurrency;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Collections;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import portfolio.concurrency.DiningPhilosophers.Outcome;
import portfolio.concurrency.DiningPhilosophers.Strategy;

@Timeout(60)
class DiningPhilosophersTest {
    private static final int N = Scenarios.get("philosophers.count");
    private static final int MEALS = Scenarios.get("philosophers.meals");
    private static final Duration TIMEOUT = Duration.ofMillis(Scenarios.get("philosophers.timeout_ms"));

    @Test
    void naiveStrategyDeadlocksWhenEveryoneHoldsTheLeftFork() throws Exception {
        Outcome o = DiningPhilosophers.dine(N, MEALS, Strategy.NAIVE, true, TIMEOUT);
        assertTrue(o.cycleFormed(), "every philosopher held the left fork at once");
        assertTrue(o.deadlocked(), "then nobody could get the right fork: detected, not hung");
        assertEquals(0, o.violations());
    }

    @ParameterizedTest
    @EnumSource(value = Strategy.class, names = {"ORDERED", "SEATS"})
    void theFixedStrategiesCannotFormTheCycle(Strategy strategy) throws Exception {
        Outcome o = DiningPhilosophers.dine(N, MEALS, strategy, true, TIMEOUT);
        assertFalse(o.cycleFormed(), "the forced schedule cannot put every fork in a cycle");
        assertFalse(o.deadlocked());
        assertEquals(Collections.nCopies(N, MEALS), o.meals(), "everyone ate every meal");
        assertEquals(0, o.violations(), "a fork is never held by two philosophers");
    }

    @ParameterizedTest
    @EnumSource(value = Strategy.class, names = {"ORDERED", "SEATS"})
    void theFixedStrategiesSurviveRepeatedContention(Strategy strategy) throws Exception {
        for (int round = 0; round < 10; round++) {
            Outcome o = DiningPhilosophers.dine(N, MEALS, strategy, false, TIMEOUT);
            assertFalse(o.deadlocked(), "round " + round);
            assertEquals(Collections.nCopies(N, MEALS), o.meals());
            assertEquals(0, o.violations());
        }
    }
}
