"""Errors shared by the thread and asyncio implementations."""

from __future__ import annotations


class ClosedError(Exception):
    """Raised by put after the buffer was closed or aborted."""

    def __init__(self) -> None:
        super().__init__("buffer is closed")


class PipelineError(Exception):
    """A stage failed; names the stage and the index of the failing input."""

    def __init__(self, stage: str, index: int, cause: BaseException) -> None:
        super().__init__(f"stage '{stage}' failed on item {index}: {cause}")
        self.stage = stage
        self.index = index
        self.__cause__ = cause
