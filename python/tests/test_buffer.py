"""Bounded buffer: exactly-once delivery, capacity, FIFO per producer, close and abort."""

from __future__ import annotations

import asyncio
import threading
import time
from typing import Any

import pytest

from concurrency_across_runtimes.aio import AsyncBoundedBuffer
from concurrency_across_runtimes.errors import ClosedError
from concurrency_across_runtimes.threads import BoundedBuffer


def check_delivery(received: list[list[int]], producers: int, per_producer: int) -> None:
    """Every item exactly once, and each consumer sees each producer's items in order."""
    flat = [x for mine in received for x in mine]
    assert len(flat) == producers * per_producer, "no item lost or duplicated"
    assert sorted(flat) == sorted(
        p * 1_000_000 + s for p in range(producers) for s in range(per_producer)
    )
    for mine in received:
        last = [-1] * producers
        for x in mine:
            p, s = divmod(x, 1_000_000)
            assert s > last[p], f"producer {p} out of order"
            last[p] = s


def test_threads_deliver_every_item_exactly_once(scenarios: dict[str, Any]) -> None:
    cfg = scenarios["bounded_buffer"]
    buf: BoundedBuffer[int] = BoundedBuffer(cfg["capacity"])
    received: list[list[int]] = [[] for _ in range(cfg["consumers"])]

    def consume(mine: list[int]) -> None:
        while (x := buf.take()) is not None:
            mine.append(x)

    def produce(p: int) -> None:
        for s in range(cfg["items_per_producer"]):
            buf.put(p * 1_000_000 + s)

    consumers = [threading.Thread(target=consume, args=(m,)) for m in received]
    producers = [threading.Thread(target=produce, args=(p,)) for p in range(cfg["producers"])]
    for t in consumers + producers:
        t.start()
    for t in producers:
        t.join()
    buf.close()
    for t in consumers:
        t.join()
    check_delivery(received, cfg["producers"], cfg["items_per_producer"])
    assert buf.max_observed <= cfg["capacity"]


def test_asyncio_delivers_every_item_exactly_once(scenarios: dict[str, Any]) -> None:
    cfg = scenarios["bounded_buffer"]

    async def main() -> tuple[list[list[int]], int]:
        buf: AsyncBoundedBuffer[int] = AsyncBoundedBuffer(cfg["capacity"])
        received: list[list[int]] = [[] for _ in range(cfg["consumers"])]

        async def consume(mine: list[int]) -> None:
            while (x := await buf.take()) is not None:
                mine.append(x)

        async def produce(p: int) -> None:
            for s in range(cfg["items_per_producer"]):
                await buf.put(p * 1_000_000 + s)

        consumers = [asyncio.create_task(consume(m)) for m in received]
        await asyncio.gather(*(produce(p) for p in range(cfg["producers"])))
        await buf.close()
        await asyncio.gather(*consumers)
        return received, buf.max_observed

    received, max_observed = asyncio.run(main())
    check_delivery(received, cfg["producers"], cfg["items_per_producer"])
    assert max_observed == cfg["capacity"], "producers outpace consumers, so the buffer fills"


def test_close_drains_then_reports_empty() -> None:
    buf: BoundedBuffer[str] = BoundedBuffer(3)
    buf.put("a")
    buf.put("b")
    buf.close()
    with pytest.raises(ClosedError):
        buf.put("c")
    assert [buf.take(), buf.take(), buf.take()] == ["a", "b", None]


def test_asyncio_close_drains_then_reports_empty() -> None:
    async def main() -> list[str | None]:
        buf: AsyncBoundedBuffer[str] = AsyncBoundedBuffer(3)
        await buf.put("a")
        await buf.close()
        with pytest.raises(ClosedError):
            await buf.put("b")
        return [await buf.take(), await buf.take()]

    assert asyncio.run(main()) == ["a", None]


def test_a_full_buffer_blocks_the_producer_until_space_frees() -> None:
    buf: BoundedBuffer[int] = BoundedBuffer(1)
    buf.put(1)
    done = threading.Event()

    def produce() -> None:
        buf.put(2)
        done.set()

    t = threading.Thread(target=produce)
    t.start()
    assert not done.wait(0.1), "put must wait while the buffer is full"
    assert buf.take() == 1
    assert done.wait(5)
    t.join()
    assert buf.take() == 2


def test_abort_wakes_blocked_producers_and_consumers() -> None:
    full: BoundedBuffer[int] = BoundedBuffer(1)
    full.put(1)
    empty: BoundedBuffer[int] = BoundedBuffer(1)
    errors: list[BaseException] = []
    taken: list[int | None] = []

    def produce() -> None:
        try:
            full.put(2)
        except ClosedError as e:
            errors.append(e)

    threads = [
        threading.Thread(target=produce),
        threading.Thread(target=lambda: taken.append(empty.take())),
    ]
    for t in threads:
        t.start()
    time.sleep(0.05)
    full.abort()
    empty.abort()
    for t in threads:
        t.join(5)
    assert len(errors) == 1
    assert taken == [None]
    assert len(full) == 0


def test_timeouts_give_up() -> None:
    buf: BoundedBuffer[int] = BoundedBuffer(1)
    assert buf.take(timeout=0.02) is None
    assert buf.put(1, timeout=0.02)
    assert not buf.put(2, timeout=0.02)


def test_invalid_use_is_rejected() -> None:
    with pytest.raises(ValueError, match="capacity"):
        BoundedBuffer[int](0)
    with pytest.raises(ValueError, match="None"):
        BoundedBuffer[Any](1).put(None)
