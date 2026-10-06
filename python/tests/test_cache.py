"""Single-flight cache: one load per key, parallel keys, failures not cached, LRU."""

from __future__ import annotations

import asyncio
import threading
import time
from typing import Any

import pytest

from concurrency_across_runtimes.aio import AsyncSingleFlightCache
from concurrency_across_runtimes.threads import SingleFlightCache


def test_threads_concurrent_misses_share_one_load(scenarios: dict[str, Any]) -> None:
    cfg = scenarios["cache"]

    def load(key: str) -> object:
        time.sleep(cfg["load_ms"] / 1000)
        return object()

    cache: SingleFlightCache[str, object] = SingleFlightCache(cfg["capacity"], load)
    go = threading.Event()
    seen: list[object] = []
    lock = threading.Lock()

    def request() -> None:
        go.wait()
        value = cache.get("report")
        with lock:
            seen.append(value)

    threads = [threading.Thread(target=request) for _ in range(cfg["concurrent_requests"])]
    for t in threads:
        t.start()
    go.set()
    for t in threads:
        t.join()
    assert cache.loads == 1
    assert len({id(v) for v in seen}) == 1
    assert len(seen) == cfg["concurrent_requests"]


def test_asyncio_concurrent_misses_share_one_load(scenarios: dict[str, Any]) -> None:
    cfg = scenarios["cache"]

    async def load(key: str) -> object:
        await asyncio.sleep(cfg["load_ms"] / 1000)
        return object()

    async def main() -> tuple[int, set[int]]:
        cache: AsyncSingleFlightCache[str, object] = AsyncSingleFlightCache(cfg["capacity"], load)
        values = await asyncio.gather(
            *(cache.get("report") for _ in range(cfg["concurrent_requests"]))
        )
        return cache.loads, {id(v) for v in values}

    loads, ids = asyncio.run(main())
    assert loads == 1
    assert len(ids) == 1


def test_different_keys_load_in_parallel() -> None:
    b_started = threading.Event()

    def load(key: str) -> str:
        if key == "a":
            if not b_started.wait(5):
                raise RuntimeError("b never started while a was loading")
        else:
            b_started.set()
        return key.upper()

    cache: SingleFlightCache[str, str] = SingleFlightCache(3, load)
    results: list[str] = []
    t = threading.Thread(target=lambda: results.append(cache.get("a")))
    t.start()
    assert cache.get("b") == "B"
    t.join()
    assert results == ["A"]


def test_a_failed_load_is_not_cached() -> None:
    attempts = [0]
    go = threading.Event()

    def load(key: str) -> str:
        attempts[0] += 1
        if attempts[0] == 1:
            time.sleep(0.3)
            raise RuntimeError("database down")
        return "ok"

    cache: SingleFlightCache[str, str] = SingleFlightCache(3, load)
    failures: list[BaseException] = []
    lock = threading.Lock()

    def request() -> None:
        go.wait()
        try:
            cache.get("k")
        except RuntimeError as e:
            with lock:
                failures.append(e)

    threads = [threading.Thread(target=request) for _ in range(8)]
    for t in threads:
        t.start()
    go.set()
    for t in threads:
        t.join()
    assert cache.loads == 1
    assert len(failures) == 8, "every waiter of the failed flight sees the error"
    assert cache.get("k") == "ok", "the next request retries"


def test_asyncio_failed_load_is_not_cached() -> None:
    attempts = [0]

    async def load(key: str) -> str:
        attempts[0] += 1
        await asyncio.sleep(0.01)
        if attempts[0] == 1:
            raise RuntimeError("database down")
        return "ok"

    async def main() -> tuple[list[Any], str]:
        cache: AsyncSingleFlightCache[str, str] = AsyncSingleFlightCache(3, load)
        first = await asyncio.gather(*(cache.get("k") for _ in range(8)), return_exceptions=True)
        return first, await cache.get("k")

    first, retry = asyncio.run(main())
    assert all(isinstance(r, RuntimeError) for r in first)
    assert retry == "ok"


def test_least_recently_used_key_is_evicted(scenarios: dict[str, Any]) -> None:
    capacity = scenarios["cache"]["capacity"]
    cache: SingleFlightCache[str, str] = SingleFlightCache(capacity, lambda k: k + "!")
    for k in ["a", "b", "c", "a", "d"]:
        cache.get(k)
    assert cache.keys() == ["c", "a", "d"]
    assert cache.loads == 4
    cache.get("b")
    assert cache.loads == 5
    assert len(cache.keys()) == capacity


def test_none_values_and_bad_capacity_are_rejected() -> None:
    with pytest.raises(ValueError, match="None"):
        SingleFlightCache[str, Any](1, lambda k: None).get("x")
    with pytest.raises(ValueError, match="capacity"):
        SingleFlightCache[str, str](0, str)
