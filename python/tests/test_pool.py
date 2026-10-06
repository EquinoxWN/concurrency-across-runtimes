"""Worker pool: results, bounded concurrency, failures and shutdown."""

from __future__ import annotations

import asyncio
import threading
import time
from typing import Any

import pytest

from concurrency_across_runtimes.aio import AsyncWorkerPool
from concurrency_across_runtimes.threads import WorkerPool
from concurrency_across_runtimes.threads.pool import PoolClosedError


def test_threads_return_every_result_with_bounded_concurrency(scenarios: dict[str, Any]) -> None:
    cfg = scenarios["worker_pool"]

    def task(n: int) -> int:
        time.sleep(cfg["task_ms"] / 1000)
        return n * n

    with WorkerPool(cfg["workers"], cfg["queue_capacity"]) as pool:
        futures = [pool.submit(task, i) for i in range(cfg["tasks"])]
        assert [f.result() for f in futures] == [i * i for i in range(cfg["tasks"])]
    assert pool.max_active == cfg["workers"]
    assert pool.terminated


def test_asyncio_returns_every_result_with_bounded_concurrency(scenarios: dict[str, Any]) -> None:
    cfg = scenarios["worker_pool"]

    async def task(n: int) -> int:
        await asyncio.sleep(cfg["task_ms"] / 1000)
        return n * n

    async def main() -> tuple[list[int], int, bool]:
        pool = AsyncWorkerPool(cfg["workers"], cfg["queue_capacity"])
        futures = [await pool.submit(task, i) for i in range(cfg["tasks"])]
        results = [await f for f in futures]
        await pool.shutdown()
        return results, pool.max_active, pool.terminated

    results, max_active, terminated = asyncio.run(main())
    assert results == [i * i for i in range(cfg["tasks"])]
    assert max_active == cfg["workers"]
    assert terminated


def test_a_failing_task_fails_only_its_future() -> None:
    def boom() -> int:
        raise RuntimeError("boom")

    with WorkerPool(2, 4) as pool:
        bad = pool.submit(boom)
        good = pool.submit(lambda: 42)
        with pytest.raises(RuntimeError, match="boom"):
            bad.result()
        assert good.result() == 42
        assert pool.submit(lambda: 7).result() == 7


def test_asyncio_failing_task_fails_only_its_future() -> None:
    async def boom() -> int:
        raise RuntimeError("boom")

    async def ok() -> int:
        return 42

    async def main() -> int:
        pool = AsyncWorkerPool(2, 4)
        bad = await pool.submit(boom)
        good = await pool.submit(ok)
        with pytest.raises(RuntimeError, match="boom"):
            await bad
        result = await good
        await pool.shutdown()
        return result

    assert asyncio.run(main()) == 42


def test_shutdown_finishes_queued_tasks_then_rejects() -> None:
    done: list[int] = []
    lock = threading.Lock()

    def task(i: int) -> None:
        time.sleep(0.005)
        with lock:
            done.append(i)

    pool = WorkerPool(2, 32)
    for i in range(20):
        pool.submit(task, i)
    pool.shutdown()
    assert sorted(done) == list(range(20))
    assert pool.terminated
    with pytest.raises(PoolClosedError):
        pool.submit(task, 99)


def test_submit_blocks_while_the_queue_is_full() -> None:
    release = threading.Event()
    with WorkerPool(1, 1) as pool:
        pool.submit(release.wait)
        time.sleep(0.05)
        pool.submit(lambda: 2)
        submitted = threading.Event()

        def third() -> None:
            pool.submit(lambda: 3)
            submitted.set()

        t = threading.Thread(target=third)
        t.start()
        assert not submitted.wait(0.1), "submit waits for queue space"
        release.set()
        assert submitted.wait(5)
        t.join()
