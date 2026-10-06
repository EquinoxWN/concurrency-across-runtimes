# The five shared problems

Every language implements the same five problems and its tests read the same parameters from
[`scenarios.json`](scenarios.json), so the invariants below are checked identically in Java,
Python (threads and asyncio) and JavaScript (event loop, `worker_threads` and `Atomics`).

## 1. Bounded buffer (producer/consumer)

A fixed-capacity FIFO queue. `put` blocks while the buffer is full and `take` blocks while it is
empty; `close` lets consumers drain what is left and then stop, and `abort` drops everything and
wakes every waiter.

Invariants checked:

- Every item produced is consumed exactly once (no loss, no duplicates), with several producers
  and several consumers.
- The buffer never holds more than its capacity (the highest occupancy ever seen is recorded).
- Items from one producer reach a single consumer in the order they were produced (FIFO).
- After `close`, `put` fails and `take` returns the remaining items, then "empty".

## 2. Worker pool

A fixed number of workers pull tasks from a bounded queue.

- Every task's result comes back through its own future or promise.
- No more than `workers` tasks ever run at the same time, and with blocking tasks all workers are
  used.
- A task that fails fails only its own future; the pool keeps working.
- `shutdown` stops accepting tasks, finishes the queued ones, and waits for every worker.

## 3. Pipeline

`parse` (text to number) then `square` then `format`, each stage with its own number of workers,
connected by bounded buffers.

- The output equals the sequential result; in ordered mode it is also in input order.
- No buffer between stages ever exceeds its capacity (backpressure: a slow stage slows the
  producer instead of growing a queue).
- When one item fails (`fail_at`), the whole pipeline stops, the error names the stage and the
  item, and no worker is left running.
- Stopping is prompt: when the failure comes early in a long input (`early_stop_items`), most of
  the input is never processed. (Abort is asynchronous, so the exact number of items that finish
  after the failure depends on scheduling; only "far fewer than all" is guaranteed.)

## 4. Single-flight cache

A memoizing cache with a fixed capacity (least recently used is evicted).

- Many concurrent requests for the same missing key call the loader exactly once and all get the
  same value.
- Loads of different keys run in parallel (proved with a rendezvous, not with timing).
- A failed load is not cached: every waiter gets the error and the next request retries.
- The cache never holds more than its capacity, and evicts the least recently used key.

## 5. Dining philosophers

Five philosophers share five forks; each needs both neighbouring forks to eat.

- `naive` (left fork, then right): when every philosopher is made to hold the left fork first, the
  circular wait forms and the deadlock is **detected** by a timeout instead of hanging the test.
- `ordered` (lower-numbered fork first) and `seats` (at most n-1 philosophers at the table): the
  same forced schedule cannot form the cycle, and every philosopher eats every meal.
- A fork is never held by two philosophers at once.
