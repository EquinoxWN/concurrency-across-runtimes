# ADR 0003: Every queue is bounded, and producers wait when it is full

- **Status:** Accepted

## Context

The most common production failure in concurrent code is not a race but an unbounded queue: a
fast producer feeds a slow consumer, the queue grows, latency rises, and eventually memory runs
out. Unbounded structures are the default in all three runtimes (`LinkedBlockingQueue`,
`queue.Queue()`, `asyncio.Queue()`, an array in JavaScript, `Promise.all` over every task).

## Decision

Every buffer in the project has a fixed capacity, and `put` (or `submit`) blocks or suspends while
it is full. The worker pool queue, every buffer between pipeline stages and the shared-memory ring
are all bounded. Each buffer records the highest occupancy it ever reached so tests can check the
bound.

## Consequences

- Backpressure is automatic: a slow stage slows the stage before it, all the way back to the
  feeder. Tests check that the buffers fill up to their capacity and never past it, in every
  language.
- Throughput can drop when capacities are too small; choosing them is a tuning decision that M3
  will measure.
- Shutdown needs care: a producer blocked on a full queue must be released. Every buffer
  therefore has `close` (drain, then stop) and `abort` (drop and wake everyone), and tests check
  that blocked producers and consumers are woken by both.
