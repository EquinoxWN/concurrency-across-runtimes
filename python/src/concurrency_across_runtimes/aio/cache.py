"""LRU cache for coroutine loaders where concurrent misses share one load."""

from __future__ import annotations

import asyncio
from collections import OrderedDict
from collections.abc import Awaitable, Callable, Hashable
from typing import Generic, TypeVar

K = TypeVar("K", bound=Hashable)
V = TypeVar("V")


class AsyncSingleFlightCache(Generic[K, V]):
    """No lock is needed: between awaits nothing else runs on the event loop."""

    def __init__(self, capacity: int, loader: Callable[[K], Awaitable[V]]) -> None:
        if capacity < 1:
            raise ValueError("capacity must be at least 1")
        self.capacity = capacity
        self._loader = loader
        self._values: OrderedDict[K, V] = OrderedDict()
        self._in_flight: dict[K, asyncio.Future[V]] = {}
        self.loads = 0

    async def get(self, key: K) -> V:
        """Cached value, or the result of the one load shared by all concurrent callers."""
        if key in self._values:
            self._values.move_to_end(key)
            return self._values[key]
        flight = self._in_flight.get(key)
        if flight is None:
            flight = asyncio.get_running_loop().create_future()
            self._in_flight[key] = flight
            self.loads += 1
            await self._load(key, flight)
        return await asyncio.shield(flight)

    async def _load(self, key: K, flight: asyncio.Future[V]) -> None:
        try:
            value = await self._loader(key)
            if value is None:
                raise ValueError("loader returned None")
        except Exception as e:  # every waiter sees the failure; nothing is cached
            del self._in_flight[key]
            flight.set_exception(e)
            return
        self._values[key] = value
        while len(self._values) > self.capacity:
            self._values.popitem(last=False)
        del self._in_flight[key]
        flight.set_result(value)

    def keys(self) -> list[K]:
        """Cached keys, least recently used first."""
        return list(self._values)
