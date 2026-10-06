"""A fixed number of worker tasks pulling coroutines from a bounded buffer."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

from concurrency_across_runtimes.aio.buffer import AsyncBoundedBuffer
from concurrency_across_runtimes.errors import ClosedError

R = TypeVar("R")


class AsyncWorkerPool:
    """Limits how many coroutines run at once; submit waits while the queue is full."""

    def __init__(self, workers: int, queue_capacity: int) -> None:
        if workers < 1:
            raise ValueError("need at least one worker")
        self._queue: AsyncBoundedBuffer[Callable[[], Awaitable[None]]] = AsyncBoundedBuffer(
            queue_capacity
        )
        self._active = 0
        self.max_active = 0
        self._tasks = [
            asyncio.create_task(self._work(), name=f"worker-{i}") for i in range(workers)
        ]

    async def submit(self, fn: Callable[..., Awaitable[R]], *args: Any) -> asyncio.Future[R]:
        """Queue a coroutine call; its result arrives in the returned future."""
        future: asyncio.Future[R] = asyncio.get_running_loop().create_future()

        async def run() -> None:
            self._active += 1
            self.max_active = max(self.max_active, self._active)
            try:
                future.set_result(await fn(*args))
            except Exception as e:  # the task's error belongs to its future
                future.set_exception(e)
            finally:
                self._active -= 1

        try:
            await self._queue.put(run)
        except ClosedError as e:
            raise RuntimeError("pool is shut down") from e
        return future

    async def _work(self) -> None:
        while (task := await self._queue.take()) is not None:
            await task()

    async def shutdown(self) -> None:
        """Stop accepting work, finish what is queued and wait for every worker."""
        await self._queue.close()
        await asyncio.gather(*self._tasks)

    @property
    def terminated(self) -> bool:
        """True once every worker task has finished."""
        return all(t.done() for t in self._tasks)
