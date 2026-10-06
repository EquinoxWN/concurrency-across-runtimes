"""Strategies and outcome shared by both dining-philosopher implementations."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class Strategy(Enum):
    """How a philosopher picks up forks."""

    NAIVE = "naive"
    ORDERED = "ordered"
    SEATS = "seats"


@dataclass(frozen=True)
class Outcome:
    """Result of one dinner."""

    deadlocked: bool
    cycle_formed: bool
    meals: list[int]
    violations: int


def fork_order(philosopher: int, n: int, strategy: Strategy) -> tuple[int, int]:
    """First and second fork for a philosopher."""
    left, right = philosopher, (philosopher + 1) % n
    if strategy is Strategy.ORDERED:
        return min(left, right), max(left, right)
    return left, right
