"""Bounded FIFO buffer for asyncio tasks."""

from __future__ import annotations

import asyncio
from collections import deque
from typing import Generic, TypeVar

from concurrency_across_runtimes.errors import ClosedError

T = TypeVar("T")


class AsyncBoundedBuffer(Generic[T]):
    """Same contract as the thread version; waiting suspends the task, not a thread."""

    def __init__(self, capacity: int) -> None:
        if capacity < 1:
            raise ValueError("capacity must be at least 1")
        self.capacity = capacity
        self._items: deque[T] = deque()
        lock = asyncio.Lock()
        self._not_full = asyncio.Condition(lock)
        self._not_empty = asyncio.Condition(lock)
        self._closed = False
        self.max_observed = 0

    async def put(self, item: T) -> None:
        """Wait for space and add; ClosedError once closed."""
        if item is None:
            raise ValueError("None cannot be queued; it means 'closed and drained'")
        async with self._not_full:
            await self._not_full.wait_for(lambda: len(self._items) < self.capacity or self._closed)
            if self._closed:
                raise ClosedError
            self._items.append(item)
            self.max_observed = max(self.max_observed, len(self._items))
            self._not_empty.notify()

    async def take(self) -> T | None:
        """Next item; None when closed and drained."""
        async with self._not_empty:
            await self._not_empty.wait_for(lambda: bool(self._items) or self._closed)
            if not self._items:
                return None
            item = self._items.popleft()
            self._not_full.notify()
            return item

    async def close(self) -> None:
        """Refuse new items; consumers drain the rest, then get None."""
        async with self._not_full:
            self._closed = True
            self._not_full.notify_all()
            self._not_empty.notify_all()

    async def abort(self) -> None:
        """Close and drop everything queued, waking every waiter."""
        async with self._not_full:
            self._closed = True
            self._items.clear()
            self._not_full.notify_all()
            self._not_empty.notify_all()

    def __len__(self) -> int:
        return len(self._items)
