"""Bounded FIFO buffer from one lock and two conditions."""

from __future__ import annotations

import threading
import time
from collections import deque
from typing import Generic, TypeVar

from concurrency_across_runtimes.errors import ClosedError

T = TypeVar("T")


class BoundedBuffer(Generic[T]):
    """Fixed-capacity FIFO; producers wait for space, consumers wait for items."""

    def __init__(self, capacity: int) -> None:
        if capacity < 1:
            raise ValueError("capacity must be at least 1")
        self.capacity = capacity
        self._items: deque[T] = deque()
        self._lock = threading.Lock()
        self._not_full = threading.Condition(self._lock)
        self._not_empty = threading.Condition(self._lock)
        self._closed = False
        self._max_observed = 0

    def put(self, item: T, timeout: float | None = None) -> bool:
        """Wait for space and add; False on timeout, ClosedError once closed."""
        if item is None:
            raise ValueError("None cannot be queued; it means 'closed and drained'")
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._not_full:
            while len(self._items) >= self.capacity and not self._closed:
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    return False
                self._not_full.wait(remaining)
            if self._closed:
                raise ClosedError
            self._items.append(item)
            self._max_observed = max(self._max_observed, len(self._items))
            self._not_empty.notify()
            return True

    def take(self, timeout: float | None = None) -> T | None:
        """Next item; None when closed and drained, or on timeout."""
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._not_empty:
            while not self._items and not self._closed:
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    return None
                self._not_empty.wait(remaining)
            if not self._items:
                return None
            item = self._items.popleft()
            self._not_full.notify()
            return item

    def close(self) -> None:
        """Refuse new items; consumers drain the rest, then get None."""
        with self._lock:
            self._closed = True
            self._not_full.notify_all()
            self._not_empty.notify_all()

    def abort(self) -> None:
        """Close and drop everything queued, waking every waiter."""
        with self._lock:
            self._closed = True
            self._items.clear()
            self._not_full.notify_all()
            self._not_empty.notify_all()

    def __len__(self) -> int:
        with self._lock:
            return len(self._items)

    @property
    def max_observed(self) -> int:
        """Highest number of items ever queued at once."""
        with self._lock:
            return self._max_observed
