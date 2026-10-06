"""Dining philosophers: the naive deadlock is detected; the fixes cannot form the cycle."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from concurrency_across_runtimes import aio, threads
from concurrency_across_runtimes.philosophers_common import Outcome, Strategy


def run(kind: str, cfg: dict[str, Any], strategy: Strategy, *, force_cycle: bool) -> Outcome:
    args = (cfg["count"], cfg["meals"], strategy)
    limit = cfg["timeout_ms"] / 1000
    if kind == "threads":
        return threads.dine(*args, force_cycle=force_cycle, limit_s=limit)
    return asyncio.run(aio.dine(*args, force_cycle=force_cycle, limit_s=limit))


@pytest.mark.parametrize("kind", ["threads", "asyncio"])
def test_naive_strategy_deadlocks_and_is_detected(kind: str, scenarios: dict[str, Any]) -> None:
    o = run(kind, scenarios["philosophers"], Strategy.NAIVE, force_cycle=True)
    assert o.cycle_formed, "every philosopher held the left fork at once"
    assert o.deadlocked, "then nobody got the right fork: detected, not hung"
    assert o.violations == 0


@pytest.mark.parametrize("kind", ["threads", "asyncio"])
@pytest.mark.parametrize("strategy", [Strategy.ORDERED, Strategy.SEATS])
def test_fixed_strategies_cannot_form_the_cycle(
    kind: str, strategy: Strategy, scenarios: dict[str, Any]
) -> None:
    cfg = scenarios["philosophers"]
    o = run(kind, cfg, strategy, force_cycle=True)
    assert not o.cycle_formed
    assert not o.deadlocked
    assert o.meals == [cfg["meals"]] * cfg["count"]
    assert o.violations == 0


@pytest.mark.parametrize("kind", ["threads", "asyncio"])
@pytest.mark.parametrize("strategy", [Strategy.ORDERED, Strategy.SEATS])
def test_fixed_strategies_survive_repeated_contention(
    kind: str, strategy: Strategy, scenarios: dict[str, Any]
) -> None:
    cfg = scenarios["philosophers"]
    for _ in range(5):
        o = run(kind, cfg, strategy, force_cycle=False)
        assert not o.deadlocked
        assert o.meals == [cfg["meals"]] * cfg["count"]
        assert o.violations == 0
