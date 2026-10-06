"""Fixed worker threads pulling tasks from a bounded buffer."""

from __future__ import annotations

import threading
from collections.abc import Callable
from concurrent.futures import Future
from typing import Any, TypeVar

from concurrency_across_runtimes.errors import ClosedError
from concurrency_across_runtimes.threads.buffer import BoundedBuffer

R = TypeVar("R")


class PoolClosedError(RuntimeError):
    """Raised by submit after shutdown."""


class WorkerPool:
    """A fixed number of threads; submit blocks while the queue is full (backpressure)."""

    def __init__(self, workers: int, queue_capacity: int) -> None:
        if workers < 1:
            raise ValueError("need at least one worker")
        self._queue: BoundedBuffer[Callable[[], None]] = BoundedBuffer(queue_capacity)
        self._lock = threading.Lock()
        self._active = 0
        self.max_active = 0
        self._threads = [
            threading.Thread(target=self._work, name=f"pool-worker-{i}", daemon=True)
            for i in range(workers)
        ]
        for t in self._threads:
            t.start()

    def submit(self, fn: Callable[..., R], *args: Any) -> Future[R]:
        """Queue a call; its result or exception arrives in the returned future."""
        future: Future[R] = Future()

        def run() -> None:
            with self._lock:
                self._active += 1
                self.max_active = max(self.max_active, self._active)
            try:
                future.set_result(fn(*args))
            except Exception as e:  # the task's error belongs to its future
                future.set_exception(e)
            finally:
                with self._lock:
                    self._active -= 1

        try:
            self._queue.put(run)
        except ClosedError as e:
            raise PoolClosedError("pool is shut down") from e
        return future

    def _work(self) -> None:
        while (task := self._queue.take()) is not None:
            task()

    def shutdown(self) -> None:
        """Stop accepting tasks, finish the queued ones and wait for every worker."""
        self._queue.close()
        for t in self._threads:
            t.join()

    @property
    def terminated(self) -> bool:
        """True once every worker thread has exited."""
        return not any(t.is_alive() for t in self._threads)

    def __enter__(self) -> WorkerPool:
        return self

    def __exit__(self, *exc: object) -> None:
        self.shutdown()
