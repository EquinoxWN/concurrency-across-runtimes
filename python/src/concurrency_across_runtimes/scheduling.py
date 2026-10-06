"""Print how CPython schedules blocking and CPU-bound work on asyncio and threads."""

from __future__ import annotations

import asyncio
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor


def _gil() -> str:
    check = getattr(sys, "_is_gil_enabled", None)
    if check is None:
        return "GIL, no free-threaded build"
    return "GIL enabled" if check() else "free-threaded, GIL disabled"


def _spin(n: int = 6_000_000) -> int:
    x = 0
    for i in range(n):
        x ^= i
    return x


async def _sleepers(count: int) -> tuple[float, int]:
    threads = {threading.get_ident()}

    async def one() -> None:
        threads.add(threading.get_ident())
        await asyncio.sleep(0.1)

    start = time.perf_counter()
    await asyncio.gather(*(one() for _ in range(count)))
    return time.perf_counter() - start, len(threads)


def _thread_sleepers(count: int, pool: int) -> tuple[float, int]:
    threads: set[int] = set()
    lock = threading.Lock()

    def one(_: int) -> None:
        with lock:
            threads.add(threading.get_ident())
        time.sleep(0.1)

    start = time.perf_counter()
    with ThreadPoolExecutor(pool) as ex:
        list(ex.map(one, range(count)))
    return time.perf_counter() - start, len(threads)


def main() -> None:
    """Run the experiments and print a Markdown table."""
    tasks = min(4, os.cpu_count() or 1)
    print(
        f"| Experiment (Python {sys.version.split()[0]}, {_gil()}) | Wall time | OS threads used |"
    )
    print("|---|---|---|")
    wall, used = asyncio.run(_sleepers(10_000))
    print(f"| 10,000 asyncio tasks sleeping 100 ms | {wall * 1000:.0f} ms | {used} |")
    wall, used = _thread_sleepers(1_000, 50)
    print(f"| 1,000 tasks sleeping 100 ms, pool of 50 threads | {wall * 1000:.0f} ms | {used} |")
    start = time.perf_counter()
    for _ in range(tasks):
        _spin()
    sequential = time.perf_counter() - start
    start = time.perf_counter()
    with ThreadPoolExecutor(tasks) as ex:
        list(ex.map(lambda _: _spin(), range(tasks)))
    threaded = time.perf_counter() - start
    print(
        f"| CPU-bound: {tasks} busy tasks on {tasks} threads vs one after another | "
        f"{threaded * 1000:.0f} ms vs {sequential * 1000:.0f} ms "
        f"({sequential / threaded:.1f}x) | {tasks} |"
    )


if __name__ == "__main__":
    main()
