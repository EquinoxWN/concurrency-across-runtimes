"""Dining philosophers on the event loop: no threads, yet the same deadlock."""

from __future__ import annotations

import asyncio

from concurrency_across_runtimes.philosophers_common import Outcome, Strategy, fork_order


async def dine(
    n: int, meals: int, strategy: Strategy, *, force_cycle: bool, limit_s: float
) -> Outcome:
    """Run a dinner; with force_cycle everyone tries to hold the first fork at once."""
    forks = [asyncio.Lock() for _ in range(n)]
    owner = [-1] * n
    seats = asyncio.Semaphore(n - 1 if strategy is Strategy.SEATS else n)
    holding = asyncio.Barrier(n)
    flags = {"deadlocked": False, "cycle": False}
    violations = [0]
    eaten = [0] * n

    def take(fork: int, who: int) -> None:
        if owner[fork] != -1:
            violations[0] += 1
        owner[fork] = who

    async def philosopher(who: int) -> None:
        first, second = fork_order(who, n, strategy)
        for meal in range(meals):
            if flags["deadlocked"]:
                return
            async with seats, forks[first]:
                take(first, who)
                await asyncio.sleep(0)  # yield, as real work between the two forks would
                if force_cycle and meal == 0:
                    try:
                        await asyncio.wait_for(holding.wait(), limit_s)
                        flags["cycle"] = True
                    except (TimeoutError, asyncio.BrokenBarrierError):
                        await holding.abort()  # the cycle could not form
                try:
                    await asyncio.wait_for(forks[second].acquire(), limit_s)
                except TimeoutError:
                    owner[first] = -1
                    flags["deadlocked"] = True
                    return
                try:
                    take(second, who)
                    await asyncio.sleep(0)
                    owner[second] = -1
                finally:
                    forks[second].release()
                owner[first] = -1
            eaten[who] += 1

    await asyncio.gather(*(philosopher(p) for p in range(n)))
    return Outcome(flags["deadlocked"], flags["cycle"], eaten, violations[0])
