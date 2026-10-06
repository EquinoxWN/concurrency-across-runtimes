# ADR 0002: Detect deadlock with timed acquisition and force the deadlocking schedule

- **Status:** Accepted

## Context

A test that demonstrates a deadlock has two problems. If the deadlock happens, a naive test hangs
forever (and so does CI). If it does not happen (most runs, because the bad interleaving is rare),
the test passes and proves nothing. Watchdogs that inspect lock owners exist on the JVM
(`ThreadMXBean.findDeadlockedThreads`) but not in Python or Node.js, and this project must check
the same thing in all three.

## Decision

- Every second-fork acquisition is a timed attempt (`tryLock(timeout)`, `acquire(timeout=...)`,
  `wait_for(lock.acquire(), ...)`, an async mutex with a timer). A timeout while holding the first
  fork is reported as a detected deadlock, and the philosopher backs off.
- The test does not wait for the bad schedule; it forces it. A barrier makes every philosopher
  hold its first fork at the same moment before reaching for the second. The barrier also has a
  timeout: if it cannot fill (because the strategy makes the cycle impossible), it breaks and
  dinner continues normally. The outcome reports both "the cycle formed" and "a deadlock was
  detected".
- Every test suite also has a global timeout (`@Timeout`, `pytest-timeout`, `--test-timeout`).

## Consequences

- The naive strategy deadlocks deterministically, in every run and every runtime, and the test
  finishes in about the timeout (300 ms).
- The fixed strategies are proved under the same forced schedule: the cycle never forms, and all
  1,000 meals (5 philosophers x 200) are eaten with no fork ever held twice.
- A timed attempt is not proof of deadlock in general (a slow holder looks the same), so the
  timeout must stay long compared to the critical section; this is documented in RFC 0001.
