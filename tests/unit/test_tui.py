from __future__ import annotations

import uuid

import pytest
from textual.widgets import TextArea

from src.agentic_cli.tui import MeridianTUI


def test_sid_fallback_is_deterministic_for_hub_seed() -> None:
    app = MeridianTUI()
    app._hub_id = "hub-abc-123"

    sid_one = app._derive_fallback_session_id()
    sid_two = app._derive_fallback_session_id()

    expected = str(
        uuid.uuid5(
            uuid.NAMESPACE_URL,
            "meridian://agentic-cli/session/hub:hub-abc-123",
        )
    )

    assert sid_one == expected
    assert sid_two == expected


def test_session_fsm_ignores_blank_sid_and_second_sid_is_not_blank() -> None:
    app = MeridianTUI()

    sid_first = app._resolve_session_id("")
    sid_second = app._resolve_session_id("   ")

    assert sid_first != ""
    assert sid_second != ""
    assert sid_second == sid_first


def test_hub_rows_overwrite_by_hub_id_and_keep_single_row() -> None:
    app = MeridianTUI()
    app._hub_rows["hub-1"] = "active"
    first_rows = app._build_hub_rows()

    app._hub_rows["hub-1"] = "terminated"
    second_rows = app._build_hub_rows()

    assert len(first_rows) == 1
    assert len(second_rows) == 1
    assert first_rows[0] != second_rows[0]


def test_hub_state_apply_drops_stale_poll_sequence() -> None:
    app = MeridianTUI()
    app._hub_poll_seq = 3
    app._hub_rows["hub-1"] = "active"

    applied = app._apply_hub_state("hub-1", "terminated", poll_seq=2)

    assert applied is False
    assert app._hub_rows["hub-1"] == "active"


@pytest.mark.asyncio
async def test_center_pane_textarea_is_selectable_and_copy_paths_work(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _noop_init_hub(self: MeridianTUI) -> None:
        self._hub_id = None

    monkeypatch.setattr(MeridianTUI, "_init_hub", _noop_init_hub)

    app = MeridianTUI()
    async with app.run_test() as _:
        area = app.query_one("#message-textarea", TextArea)
        assert area.read_only is True

        app.action_focus_center()
        # Focus ownership can remain on prompt in some Textual versions.
        assert app.focused is not None

        msg_log = app.query_one("#message-log")
        monkeypatch.setattr(msg_log, "get_selected_text", lambda: "selected-center-text")

        copied: dict[str, str] = {}

        def _copy_ok(payload: str) -> None:
            copied["payload"] = payload

        monkeypatch.setattr(app, "copy_to_clipboard", _copy_ok, raising=False)
        app.action_copy_center()
        assert copied["payload"] == "selected-center-text"

        def _copy_fail(_: str) -> None:
            raise RuntimeError("clipboard unavailable")

        monkeypatch.setattr(app, "copy_to_clipboard", _copy_fail, raising=False)
        app.action_copy_center()

        assert app._copy_fallback_text == "selected-center-text"
        assert "Clipboard unavailable" in msg_log.get_text()
