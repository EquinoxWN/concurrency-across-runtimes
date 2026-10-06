# Python implementations

Two versions of each problem, so the runtimes can be compared side by side:

- `concurrency_across_runtimes.threads`: OS threads with `threading` locks and conditions. With the
  GIL, threads overlap waiting but not computing; on the free-threaded build (3.13t+) they also
  compute in parallel. CI runs the suite on both builds.
- `concurrency_across_runtimes.aio`: `asyncio` on one thread. Tasks switch only at `await`, so data
  races on plain Python objects cannot happen, but logical races between awaits still can (the
  dining philosophers deadlock just the same).

Run `python -m concurrency_across_runtimes.scheduling` for the scheduling experiments.
