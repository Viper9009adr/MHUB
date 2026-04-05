"""Tests for the asyncio-based task queue."""

from __future__ import annotations

import asyncio
import pytest

from src.orc.task_queue import TaskItem, TaskQueue


class TestTaskQueue:
    def test_enqueue_and_dequeue(self) -> None:
        q = TaskQueue()
        item = TaskItem(task_id="t1", payload={"key": "val"})
        asyncio.run(q.enqueue(item))
        assert q.size == 1

        result = asyncio.run(q.dequeue())
        assert result.task_id == "t1"
        assert q.size == 0
        assert q.processed_count == 1

    def test_enqueue_nowait(self) -> None:
        q = TaskQueue()
        item = TaskItem(task_id="t2", payload={})
        q.enqueue_nowait(item)
        assert q.size == 1

    def test_dequeue_nowait(self) -> None:
        q = TaskQueue()
        item = TaskItem(task_id="t3", payload={"data": 1})
        q.enqueue_nowait(item)
        result = q.dequeue_nowait()
        assert result.task_id == "t3"
        assert q.processed_count == 1

    def test_dequeue_nowait_empty_raises(self) -> None:
        q = TaskQueue()
        with pytest.raises(asyncio.QueueEmpty):
            q.dequeue_nowait()

    def test_bounded_queue_blocks_when_full(self) -> None:
        q = TaskQueue(maxsize=1)
        item1 = TaskItem(task_id="a", payload={})
        q.enqueue_nowait(item1)
        assert q.size == 1

        item2 = TaskItem(task_id="b", payload={})
        with pytest.raises(asyncio.QueueFull):
            q.enqueue_nowait(item2)

    def test_is_empty_property(self) -> None:
        q = TaskQueue()
        assert q.is_empty is True
        q.enqueue_nowait(TaskItem(task_id="x", payload={}))
        assert q.is_empty is False

    def test_processed_count_tracks_dequeues(self) -> None:
        q = TaskQueue()
        for i in range(3):
            asyncio.run(q.enqueue(TaskItem(task_id=f"t{i}", payload={})))
        for _ in range(3):
            asyncio.run(q.dequeue())
        assert q.processed_count == 3
        assert q.size == 0
