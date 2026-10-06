"""Stages joined by bounded buffers, each stage with its own worker threads."""

from __future__ import annotations

import threading
from collections.abc import Callable, Sequence
from typing import Any, NamedTuple

from concurrency_across_runtimes.errors import ClosedError, PipelineError
from concurrency_across_runtimes.threads.buffer import BoundedBuffer


class Stage(NamedTuple):
    """One step: a name, a worker count and the function applied to each item."""

    name: str
    workers: int
    fn: Callable[[Any], Any]


class _Item(NamedTuple):
    pos: int
    value: Any


class Pipeline:
    """A failure in any stage aborts every buffer, so all workers stop and run raises once."""

    def __init__(self, stages: Sequence[Stage], capacity: int, *, ordered: bool) -> None:
        if not stages:
            raise ValueError("a pipeline needs at least one stage")
        if any(s.workers < 1 for s in stages):
            raise ValueError("every stage needs at least one worker")
        self.stages = list(stages)
        self.capacity = capacity
        self.ordered = ordered
        self.last_max_occupancy: list[int] = []
        self._last_threads: list[threading.Thread] = []

    def run(self, inputs: Sequence[Any]) -> list[Any]:
        """Push every input through all stages and collect the outputs."""
        buffers: list[BoundedBuffer[_Item]] = [
            BoundedBuffer(self.capacity) for _ in range(len(self.stages) + 1)
        ]
        failure: list[PipelineError] = []
        failure_lock = threading.Lock()

        def fail(stage: str, index: int, cause: Exception) -> None:
            with failure_lock:
                if not failure:
                    failure.append(PipelineError(stage, index, cause))
                    for b in buffers:
                        b.abort()

        def feed() -> None:
            try:
                for i, x in enumerate(inputs):
                    buffers[0].put(_Item(i, x))
                buffers[0].close()
            except ClosedError:
                pass  # aborted by a failing stage

        def work(stage: Stage, s: int, remaining: list[int], lock: threading.Lock) -> None:
            src, dst = buffers[s], buffers[s + 1]
            try:
                while (item := src.take()) is not None:
                    try:
                        value = stage.fn(item.value)
                    except Exception as e:  # reported once for the whole pipeline
                        fail(stage.name, item.pos, e)
                        return
                    dst.put(_Item(item.pos, value))
            except ClosedError:
                pass  # aborted by another stage
            finally:
                with lock:
                    remaining[0] -= 1
                    last = remaining[0] == 0
                if last:
                    dst.close()

        threads = [threading.Thread(target=feed, name="feeder")]
        for s, stage in enumerate(self.stages):
            shared = ([stage.workers], threading.Lock())
            threads += [
                threading.Thread(target=work, args=(stage, s, *shared), name=f"{stage.name}-{w}")
                for w in range(stage.workers)
            ]
        for t in threads:
            t.start()
        results: list[_Item] = []
        while (item := buffers[-1].take()) is not None:
            results.append(item)
        for t in threads:
            t.join()
        self.last_max_occupancy = [b.max_observed for b in buffers]
        self._last_threads = threads
        if failure:
            raise failure[0]
        if self.ordered:
            results.sort(key=lambda r: r.pos)
        return [r.value for r in results]

    @property
    def last_run_terminated(self) -> bool:
        """True when every thread of the last run has exited."""
        return not any(t.is_alive() for t in self._last_threads)
