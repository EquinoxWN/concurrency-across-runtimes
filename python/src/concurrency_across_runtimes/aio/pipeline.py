"""Stages joined by bounded buffers, run inside one asyncio.TaskGroup."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from typing import Any, NamedTuple

from concurrency_across_runtimes.aio.buffer import AsyncBoundedBuffer
from concurrency_across_runtimes.errors import PipelineError


class AsyncStage(NamedTuple):
    """One step: a name, a worker count and the coroutine function applied to each item."""

    name: str
    workers: int
    fn: Callable[[Any], Awaitable[Any]]


class _Item(NamedTuple):
    pos: int
    value: Any


class _StageFailedError(Exception):
    def __init__(self, error: PipelineError) -> None:
        super().__init__(str(error))
        self.error = error


class AsyncPipeline:
    """Structured concurrency: one failing task cancels every other task in the group."""

    def __init__(self, stages: Sequence[AsyncStage], capacity: int, *, ordered: bool) -> None:
        if not stages:
            raise ValueError("a pipeline needs at least one stage")
        if any(s.workers < 1 for s in stages):
            raise ValueError("every stage needs at least one worker")
        self.stages = list(stages)
        self.capacity = capacity
        self.ordered = ordered
        self.last_max_occupancy: list[int] = []
        self._last_tasks: list[asyncio.Task[None]] = []

    async def run(self, inputs: Sequence[Any]) -> list[Any]:
        """Push every input through all stages and collect the outputs."""
        buffers: list[AsyncBoundedBuffer[_Item]] = [
            AsyncBoundedBuffer(self.capacity) for _ in range(len(self.stages) + 1)
        ]
        results: list[_Item] = []
        tasks: list[asyncio.Task[None]] = []

        async def feed() -> None:
            for i, x in enumerate(inputs):
                await buffers[0].put(_Item(i, x))
            await buffers[0].close()

        async def work(stage: AsyncStage, s: int, remaining: list[int]) -> None:
            while (item := await buffers[s].take()) is not None:
                try:
                    value = await stage.fn(item.value)
                except Exception as e:
                    raise _StageFailedError(PipelineError(stage.name, item.pos, e)) from e
                await buffers[s + 1].put(_Item(item.pos, value))
            remaining[0] -= 1
            if remaining[0] == 0:
                await buffers[s + 1].close()

        async def collect() -> None:
            while (item := await buffers[-1].take()) is not None:
                results.append(item)

        try:
            async with asyncio.TaskGroup() as group:
                tasks.append(group.create_task(feed()))
                for s, stage in enumerate(self.stages):
                    remaining = [stage.workers]
                    tasks += [
                        group.create_task(work(stage, s, remaining)) for _ in range(stage.workers)
                    ]
                tasks.append(group.create_task(collect()))
        except* _StageFailedError as group_error:
            first = group_error.exceptions[0]
            if isinstance(first, _StageFailedError):
                raise first.error from None
            raise
        finally:
            self.last_max_occupancy = [b.max_observed for b in buffers]
            self._last_tasks = tasks
        if self.ordered:
            results.sort(key=lambda r: r.pos)
        return [r.value for r in results]

    @property
    def last_run_terminated(self) -> bool:
        """True when every task of the last run has finished."""
        return all(t.done() for t in self._last_tasks)
