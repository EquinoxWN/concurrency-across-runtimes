"""LRU cache where concurrent misses on one key share a single load."""

from __future__ import annotations

import threading
from collections import OrderedDict
from collections.abc import Callable, Hashable
from concurrent.futures import Future
from typing import Generic, TypeVar

K = TypeVar("K", bound=Hashable)
V = TypeVar("V")


class SingleFlightCache(Generic[K, V]):
    """The loader runs outside the lock, so different keys load in parallel."""

    def __init__(self, capacity: int, loader: Callable[[K], V]) -> None:
        if capacity < 1:
            raise ValueError("capacity must be at least 1")
        self.capacity = capacity
        self._loader = loader
        self._lock = threading.Lock()
        self._values: OrderedDict[K, V] = OrderedDict()
        self._in_flight: dict[K, Future[V]] = {}
        self.loads = 0

    def get(self, key: K) -> V:
        """Cached value, or the result of the one load shared by all concurrent callers."""
        with self._lock:
            if key in self._values:
                self._values.move_to_end(key)
                return self._values[key]
            flight = self._in_flight.get(key)
            leader = flight is None
            if flight is None:
                flight = Future()
                self._in_flight[key] = flight
                self.loads += 1
        if leader:
            self._load(key, flight)
        return flight.result()

    def _load(self, key: K, flight: Future[V]) -> None:
        try:
            value = self._loader(key)
            if value is None:
                raise ValueError("loader returned None")
        except Exception as e:  # every waiter sees the failure; nothing is cached
            with self._lock:
                del self._in_flight[key]
            flight.set_exception(e)
            return
        with self._lock:
            self._values[key] = value
            while len(self._values) > self.capacity:
                self._values.popitem(last=False)
            del self._in_flight[key]
        flight.set_result(value)

    def keys(self) -> list[K]:
        """Cached keys, least recently used first."""
        with self._lock:
            return list(self._values)
