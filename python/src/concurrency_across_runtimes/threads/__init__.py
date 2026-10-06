"""The five problems with OS threads and threading primitives."""

from concurrency_across_runtimes.threads.buffer import BoundedBuffer
from concurrency_across_runtimes.threads.cache import SingleFlightCache
from concurrency_across_runtimes.threads.philosophers import dine
from concurrency_across_runtimes.threads.pipeline import Pipeline, Stage
from concurrency_across_runtimes.threads.pool import WorkerPool

__all__ = ["BoundedBuffer", "Pipeline", "SingleFlightCache", "Stage", "WorkerPool", "dine"]
