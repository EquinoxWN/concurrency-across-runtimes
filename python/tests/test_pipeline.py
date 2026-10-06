"""Pipeline: sequential result, backpressure, and one failure stopping every stage."""

from __future__ import annotations

import asyncio
import time
from typing import Any

import pytest

from concurrency_across_runtimes.aio import AsyncPipeline, AsyncStage
from concurrency_across_runtimes.errors import PipelineError
from concurrency_across_runtimes.threads import Pipeline, Stage


def expected(n: int) -> list[str]:
    """What a sequential loop produces."""
    return [f"{i}^2={i * i}" for i in range(1, n + 1)]


FORMATTED = [0]


def fmt_pair(p: tuple[int, int]) -> str:
    """Format stage; counts how many items got this far."""
    FORMATTED[0] += 1
    return f"{p[0]}^2={p[1]}"


def reset() -> None:
    """Clear the counter before a run."""
    FORMATTED[0] = 0


def thread_pipeline(cfg: dict[str, Any], *, ordered: bool, fail_at: int = -1) -> Pipeline:
    reset()

    def square(n: int) -> tuple[int, int]:
        if n == fail_at:
            raise ValueError(f"cannot square {n}")
        return n, n * n

    w = cfg["workers"]
    return Pipeline(
        [
            Stage("parse", w["parse"], int),
            Stage("square", w["square"], square),
            Stage("format", w["format"], fmt_pair),
        ],
        cfg["queue_capacity"],
        ordered=ordered,
    )


def async_pipeline(cfg: dict[str, Any], *, ordered: bool, fail_at: int = -1) -> AsyncPipeline:
    reset()

    async def parse(s: str) -> int:
        return int(s)

    async def square(n: int) -> tuple[int, int]:
        await asyncio.sleep(0)
        if n == fail_at:
            raise ValueError(f"cannot square {n}")
        return n, n * n

    async def fmt(p: tuple[int, int]) -> str:
        return fmt_pair(p)

    w = cfg["workers"]
    return AsyncPipeline(
        [
            AsyncStage("parse", w["parse"], parse),
            AsyncStage("square", w["square"], square),
            AsyncStage("format", w["format"], fmt),
        ],
        cfg["queue_capacity"],
        ordered=ordered,
    )


def test_threads_ordered_output_equals_sequential(scenarios: dict[str, Any]) -> None:
    cfg = scenarios["pipeline"]
    p = thread_pipeline(cfg, ordered=True)
    assert p.run([str(i) for i in range(1, cfg["items"] + 1)]) == expected(cfg["items"])
    assert p.last_run_terminated
    assert all(m <= cfg["queue_capacity"] for m in p.last_max_occupancy)


def test_threads_unordered_output_has_the_same_items(scenarios: dict[str, Any]) -> None:
    cfg = scenarios["pipeline"]
    out = thread_pipeline(cfg, ordered=False).run([str(i) for i in range(1, cfg["items"] + 1)])
    assert sorted(out) == sorted(expected(cfg["items"]))


def test_threads_failure_stops_every_stage_and_names_the_item(scenarios: dict[str, Any]) -> None:
    cfg = scenarios["pipeline"]
    p = thread_pipeline(cfg, ordered=True, fail_at=cfg["fail_at"])
    with pytest.raises(PipelineError) as info:
        p.run([str(i) for i in range(1, cfg["items"] + 1)])
    assert info.value.stage == "square"
    assert info.value.index == cfg["fail_at"] - 1
    assert (
        str(info.value)
        == f"stage 'square' failed on item {cfg['fail_at'] - 1}: cannot square {cfg['fail_at']}"
    )
    assert p.last_run_terminated, "no worker thread is left running"


def test_threads_failure_early_in_a_long_input_stops_the_work(scenarios: dict[str, Any]) -> None:
    cfg = scenarios["pipeline"]
    items = cfg["early_stop_items"]
    p = thread_pipeline(cfg, ordered=True, fail_at=cfg["fail_at"])
    with pytest.raises(PipelineError):
        p.run([str(i) for i in range(1, items + 1)])
    assert FORMATTED[0] < items // 2, f"{FORMATTED[0]} of {items} items were still formatted"


def test_asyncio_failure_early_in_a_long_input_stops_the_work(scenarios: dict[str, Any]) -> None:
    cfg = scenarios["pipeline"]
    items = cfg["early_stop_items"]
    p = async_pipeline(cfg, ordered=True, fail_at=cfg["fail_at"])
    with pytest.raises(PipelineError):
        asyncio.run(p.run([str(i) for i in range(1, items + 1)]))
    assert FORMATTED[0] < items // 2, f"{FORMATTED[0]} of {items} items were still formatted"


def test_threads_slow_stage_fills_buffers_without_overflow() -> None:
    def slow(x: int) -> int:
        time.sleep(0.001)
        return x

    p = Pipeline([Stage("fast", 4, lambda x: x + 1), Stage("slow", 1, slow)], 4, ordered=True)
    p.run(list(range(200)))
    assert p.last_max_occupancy[1] == 4, "the fast stage is held back by the full buffer"
    assert all(m <= 4 for m in p.last_max_occupancy)


def test_asyncio_ordered_output_equals_sequential(scenarios: dict[str, Any]) -> None:
    cfg = scenarios["pipeline"]
    p = async_pipeline(cfg, ordered=True)
    out = asyncio.run(p.run([str(i) for i in range(1, cfg["items"] + 1)]))
    assert out == expected(cfg["items"])
    assert p.last_run_terminated
    assert all(m <= cfg["queue_capacity"] for m in p.last_max_occupancy)


def test_asyncio_failure_cancels_the_task_group(scenarios: dict[str, Any]) -> None:
    cfg = scenarios["pipeline"]
    p = async_pipeline(cfg, ordered=True, fail_at=cfg["fail_at"])
    with pytest.raises(PipelineError) as info:
        asyncio.run(p.run([str(i) for i in range(1, cfg["items"] + 1)]))
    assert (info.value.stage, info.value.index) == ("square", cfg["fail_at"] - 1)
    assert p.last_run_terminated, "every task in the group finished"


def test_bad_configurations_are_rejected() -> None:
    with pytest.raises(ValueError, match="at least one stage"):
        Pipeline([], 2, ordered=True)
    with pytest.raises(ValueError, match="worker"):
        Pipeline([Stage("x", 0, str)], 2, ordered=True)
    assert Pipeline([Stage("x", 1, str)], 2, ordered=True).run([]) == []
