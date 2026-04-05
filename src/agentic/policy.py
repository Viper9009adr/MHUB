"""Policy engine for agent access control.

Provides a simple policy engine that checks if agents are allowed
to perform specific actions on resources.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


class PolicyEngine:
    """Engine for checking agent permissions.

    Implements a simple allow/deny policy system. By default, all
    actions are allowed unless explicitly denied.

    Future versions will support loading policies from configuration
    or database.
    """

    def __init__(self) -> None:
        self._deny_rules: dict[str, set[str]] = {}
        self._allow_rules: dict[str, set[str]] = {}

    def add_deny_rule(self, agent_id: str, action: str) -> None:
        """Add a deny rule for an agent action.

        Args:
            agent_id: Agent to deny.
            action: Action to deny.
        """
        if agent_id not in self._deny_rules:
            self._deny_rules[agent_id] = set()
        self._deny_rules[agent_id].add(action)
        logger.debug("Added deny rule: agent=%s action=%s", agent_id, action)

    def add_allow_rule(self, agent_id: str, action: str) -> None:
        """Add an allow rule for an agent action.

        Args:
            agent_id: Agent to allow.
            action: Action to allow.
        """
        if agent_id not in self._allow_rules:
            self._allow_rules[agent_id] = set()
        self._allow_rules[agent_id].add(action)
        logger.debug("Added allow rule: agent=%s action=%s", agent_id, action)

    async def check(
        self,
        agent_id: str,
        action: str,
        resource: str,
    ) -> bool:
        """Check if an agent is allowed to perform an action.

        Uses the following logic:
        1. If there's an explicit deny rule, return False.
        2. If there's an explicit allow rule, return True.
        3. Default: allow all actions.

        Args:
            agent_id: Agent requesting the action.
            action: Action being requested.
            resource: Resource being accessed.

        Returns:
            True if the action is allowed.
        """
        # Check deny rules first
        deny_set = self._deny_rules.get(agent_id, set())
        if action in deny_set:
            logger.info(
                "Policy denied: agent=%s action=%s resource=%s",
                agent_id,
                action,
                resource,
            )
            return False

        # Check allow rules
        allow_set = self._allow_rules.get(agent_id, set())
        if action in allow_set:
            logger.debug(
                "Policy allowed (explicit): agent=%s action=%s resource=%s",
                agent_id,
                action,
                resource,
            )
            return True

        # Default: allow
        logger.debug(
            "Policy allowed (default): agent=%s action=%s resource=%s",
            agent_id,
            action,
            resource,
        )
        return True

    def clear_rules(self) -> None:
        """Clear all policy rules."""
        self._deny_rules.clear()
        self._allow_rules.clear()
        logger.debug("Cleared all policy rules")


__all__ = ["PolicyEngine"]
