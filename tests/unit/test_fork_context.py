"""Unit tests for src.orc.fork_context — set/get/clear operations."""

from __future__ import annotations

import pytest

from src.orc import fork_context


@pytest.fixture(autouse=True)
def clear_fork_contexts() -> None:
    """Clear the module-level _contexts dict after every test."""
    yield
    fork_context._contexts.clear()


def test_set_and_get_round_trip() -> None:
    """set_context followed by get_context returns the same dict."""
    fork_context.set_context("fork-1", {"parent_hub_id": "hub-A", "hal_agent_id": "hal-1"})
    result = fork_context.get_context("fork-1")
    assert result == {"parent_hub_id": "hub-A", "hal_agent_id": "hal-1"}


def test_get_unknown_key_returns_none() -> None:
    """get_context on a key that was never set returns None."""
    result = fork_context.get_context("nonexistent-fork")
    assert result is None


def test_clear_removes_key() -> None:
    """clear_context removes a key that was previously set."""
    fork_context.set_context("fork-2", {"x": "y"})
    fork_context.clear_context("fork-2")
    assert fork_context.get_context("fork-2") is None


def test_clear_nonexistent_does_not_raise() -> None:
    """clear_context on a key that doesn't exist does not raise."""
    fork_context.clear_context("never-set")  # must not raise


def test_isolation_between_fork_ids() -> None:
    """Context set for one fork_id does not bleed into another."""
    fork_context.set_context("fork-A", {"val": "alpha"})
    fork_context.set_context("fork-B", {"val": "beta"})

    assert fork_context.get_context("fork-A") == {"val": "alpha"}
    assert fork_context.get_context("fork-B") == {"val": "beta"}

    fork_context.clear_context("fork-A")
    assert fork_context.get_context("fork-A") is None
    assert fork_context.get_context("fork-B") == {"val": "beta"}
