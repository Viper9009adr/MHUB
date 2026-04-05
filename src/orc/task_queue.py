"""In-memory task queue backed by asyncio.Queue for orc coordination."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any


@dataclass
class TaskItem:
    """A single unit of work enqueued for orc dispatch."""

    task_id: str
    payload: dict[str, Any]
    priority: int = 0


class TaskQueue:
    """Asyncio-based task queue for coordinating orc dispatch work items."""

    def __init__(self, maxsize: int = 0) -> None:
        """Create a task queue with optional bounded capacity.

        Args:
            maxsize: Maximum number of items. 0 means unbounded.
        """
        self._queue: asyncio.Queue[TaskItem] = asyncio.Queue(maxsize=maxsize)
        self._processed: list[TaskItem] = []

    async def enqueue(self, item: TaskItem) -> None:
        """Place a task item onto the queue.

        Blocks if the queue is full (bounded mode).
        """
        await self._queue.put(item)

    async def dequeue(self) -> TaskItem:
        """Remove and return the next task item.

        Blocks until an item is available.
        """
        item = await self._queue.get()
        self._processed.append(item)
        return item

    def enqueue_nowait(self, item: TaskItem) -> None:
        """Place a task item without blocking.

        Raises asyncio.QueueFull if the queue is at capacity.
        """
        self._queue.put_nowait(item)

    def dequeue_nowait(self) -> TaskItem:
        """Remove and return the next task item without blocking.

        Raises asyncio.QueueEmpty if no items are available.
        """
        item = self._queue.get_nowait()
        self._processed.append(item)
        return item

    @property
    def size(self) -> int:
        """Current number of items waiting in the queue."""
        return self._queue.qsize()

    @property
    def processed_count(self) -> int:
        """Number of items that have been dequeued."""
        return len(self._processed)

    @property
    def is_empty(self) -> bool:
        """True when no items are waiting."""
        return self._queue.empty()


__all__ = ["TaskItem", "TaskQueue"]
