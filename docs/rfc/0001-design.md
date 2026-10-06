# RFC 0001: concurrency-across-runtimes design

- **Status:** Accepted (M1 implemented)
- **Author:** EquinoxWN
- **Created:** 2026

## Problem

The concurrency bugs that hurt most (lost updates, deadlocks, queues that grow until the process
dies, workers left running after an error) almost never show up in unit tests on a laptop; they
show up in production under load. They are also runtime-specific in subtle ways: the same
"thread pool" means OS threads on the JVM, a GIL-bound interpreter in CPython (or a truly parallel
one on the free-threaded build), and a single event loop plus optional worker threads in Node.js.
Engineers who move between stacks carry the wrong mental model with them. This project solves the
same five classic problems in Java, Python and JavaScript, checks the same invariants in all of
them, and measures how each runtime actually schedules the work.

## Goals

- Five shared problems, each with written invariants: bounded buffer, worker pool, pipeline,
  single-flight cache, dining philosophers ([`spec/problems.md`](../../spec/problems.md)).
- One parameter file, [`spec/scenarios.json`](../../spec/scenarios.json), read by the tests of every
  language, so the invariants are checked identically.
- Each runtime uses its own native model: Java virtual threads and `java.util.concurrent`; Python
  `threading` and `asyncio` side by side (and the free-threaded build in CI); Node.js event loop,
  `worker_threads`, and `SharedArrayBuffer` with `Atomics`.
- Concurrency bugs must fail tests, not hang them: deadlock is detected with timed lock attempts,
  every suite has timeouts, and a mutation check proves the tests catch deliberate bugs.
- Later: a lock-free Michael-Scott queue and the ABA problem (M2), jcstress and ThreadSanitizer
  stress runs and Porcupine linearizability checks (M2), scaling benchmarks by core count (M3).

## Non-goals

- A concurrency library for production use; the implementations are small on purpose so each
  line can be explained.
- Distributed concurrency (consensus, distributed locks); that is the job of other repos in the
  portfolio (multi-raft-kv).
- Comparing languages for speed: M1's scheduling numbers explain models, they are not benchmarks.

## Proposed design

![architecture](../architecture.png)

```
spec/scenarios.json ──► java/   (virtual threads, ReentrantLock + Condition, CompletableFuture)
                    ├─► python/ threads  (threading.Lock/Condition, concurrent.futures.Future)
                    │           asyncio  (asyncio.Condition, TaskGroup, Barrier)
                    └─► js/     event loop (promises), worker_threads (pool), Atomics (shared ring)
```

| Problem | Java | Python threads | Python asyncio | JavaScript |
|---|---|---|---|---|
| Bounded buffer | `ReentrantLock` with `notFull`/`notEmpty` conditions; close and abort | `threading.Condition` pair on one lock | `asyncio.Condition` pair | Promise queue on the event loop; `SharedRingBuffer` in shared memory for threads |
| Worker pool | N virtual threads on a bounded queue; `CompletableFuture` per task | N threads; `concurrent.futures.Future` | N tasks; `asyncio.Future` | N `worker_threads` (real parallelism); named tasks |
| Pipeline | Stage workers joined by bounded buffers; first failure aborts every buffer | Same design with threads | One `asyncio.TaskGroup`: a failing task cancels the group | Same design on the event loop |
| Single-flight cache | Loader runs outside the lock; LRU via access-ordered `LinkedHashMap` | Same with `OrderedDict` | No lock needed between awaits | One shared promise per key |
| Dining philosophers | `ReentrantLock.tryLock(timeout)` detects the deadlock; `CyclicBarrier` forces the cycle | `Lock.acquire(timeout)`, `threading.Barrier` | `asyncio.Lock` with `wait_for`, `asyncio.Barrier` | Async mutex with timeout and a barrier |

The dining philosophers test forces the worst schedule instead of hoping for it: every
philosopher must hold its first fork at the same moment (a barrier) before reaching for the
second. With the naive strategy that is exactly the circular wait, and the timed acquire reports
the deadlock. With the ordered and seats strategies the barrier can never fill, because the cycle
cannot form, so it times out and dinner proceeds; every meal is eaten.

## Alternatives considered

| Option | Why not (yet) |
|---|---|
| Use each language's standard queue (`ArrayBlockingQueue`, `queue.Queue`, a library) | Correct and faster, but the point is to show the lock and condition logic and to have the same `close`/`abort` contract in every runtime. M3 benchmarks the hand-written versions against the standard ones. |
| One implementation in one language, explained for the others | Cheaper, but the interesting differences (GIL, event loop, virtual-thread pinning) only show up when the same invariants are run in each runtime. |
| Detect deadlock with a watchdog thread that inspects lock owners | More general, but runtime-specific (the JVM has `ThreadMXBean`, Python and Node do not); timed acquisition works the same everywhere and keeps tests from hanging. See ADR 0002. |
| Timing-based assertions ("parallel loads finish in under 2x the load time") | Flaky on shared CI machines. Parallelism is proved with rendezvous instead (load A waits until load B has started), and early stopping is checked on a long input where only "far fewer than all" is asserted. |
| `Promise.all` without a bounded queue in JS | The common Node.js pattern, but it starts every task at once and has no backpressure, which is the bug the bounded buffer exists to prevent. |

## Measurement plan

- M1: 101 tests (Java 27, Python 40, JavaScript 34) reading one scenario file, each suite repeated
  to check for flakiness, and a mutation check where seven deliberate bugs are each caught. The
  scheduling experiments print, per runtime, wall time and OS threads used for 10,000 sleeping
  tasks and for CPU-bound work.
- M2: jcstress on the Java buffer and the lock-free queue; ThreadSanitizer on the free-threaded
  build; Porcupine linearizability checks on recorded histories.
- M3: throughput and latency of each problem as cores increase, with a real race caught by a
  detector and its fix.

## Milestones

- **M1 (done):** five problems in Java, Python (threads and asyncio) and JavaScript (event loop,
  worker threads, Atomics), shared scenarios, 101 tests, scheduling experiments.
- **M2:** Michael-Scott lock-free queue and the ABA problem; jcstress, ThreadSanitizer and
  linearizability checking.
- **M3:** scaling benchmarks by core count; write-up on the Java memory model, the GIL and the
  single-threaded event loop.

## Risks and open questions

- Timed deadlock detection can report a false deadlock on a badly overloaded machine; the timeout
  (300 ms) is long compared to a meal (microseconds), and the forced-cycle test makes the
  interesting case deterministic.
- The JavaScript worker pool only runs named tasks from `tasks.js`, because functions cannot be
  sent to another thread; real systems pass a module path instead.
- Free-threaded CPython is new; CI checks the suite on 3.14t, but performance there is M3.
