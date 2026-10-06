"""Dining philosophers on threads; deadlock is detected with timed lock attempts."""

from __future__ import annotations

import threading

from concurrency_across_runtimes.philosophers_common import Outcome, Strategy, fork_order


def dine(n: int, meals: int, strategy: Strategy, *, force_cycle: bool, limit_s: float) -> Outcome:
    """Run a dinner; with force_cycle everyone tries to hold the first fork at once."""
    forks = [threading.Lock() for _ in range(n)]
    owner = [-1] * n
    owner_lock = threading.Lock()
    seats = threading.Semaphore(n - 1 if strategy is Strategy.SEATS else n)
    holding = threading.Barrier(n)
    flags = {"deadlocked": False, "cycle": False}
    violations = [0]
    eaten = [0] * n

    def take(fork: int, who: int) -> None:
        with owner_lock:
            if owner[fork] != -1:
                violations[0] += 1
            owner[fork] = who

    def release(fork: int, who: int) -> None:
        with owner_lock:
            if owner[fork] == who:
                owner[fork] = -1

    def philosopher(who: int) -> None:
        first, second = fork_order(who, n, strategy)
        for meal in range(meals):
            if flags["deadlocked"]:
                return
            with seats, forks[first]:
                take(first, who)
                if force_cycle and meal == 0:
                    try:
                        holding.wait(limit_s)
                        flags["cycle"] = True
                    except threading.BrokenBarrierError:
                        pass  # the cycle could not form
                if not forks[second].acquire(timeout=limit_s):
                    release(first, who)
                    flags["deadlocked"] = True
                    return
                try:
                    take(second, who)
                    release(second, who)
                finally:
                    forks[second].release()
                release(first, who)
            eaten[who] += 1

    threads = [threading.Thread(target=philosopher, args=(p,)) for p in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return Outcome(flags["deadlocked"], flags["cycle"], eaten, violations[0])
