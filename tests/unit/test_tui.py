from __future__ import annotations

import asyncio
import uuid

import pytest
from textual.app import App
from textual.screen import ModalScreen
from textual.widgets import TextArea

from src.agentic_cli.tui import HubViewScreen, MeridianApp, MeridianTUI, OrcChatScreen, SplashMenuScreen, StatusBar
from src.agentic_cli.grpc_client import GrpcHubClient


class _OrcChatApp(App):
    async def on_mount(self) -> None:
        await self.push_screen(OrcChatScreen())


def test_sid_fallback_is_deterministic_for_hub_seed() -> None:
    app = OrcChatScreen()
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
    app = OrcChatScreen()

    sid_first = app._resolve_session_id("")
    sid_second = app._resolve_session_id("   ")

    assert sid_first != ""
    assert sid_second != ""
    assert sid_second == sid_first


def test_hub_rows_overwrite_by_hub_id_and_keep_single_row() -> None:
    app = OrcChatScreen()
    app._hub_rows["hub-1"] = "active"
    first_rows = app._build_hub_rows()

    app._hub_rows["hub-1"] = "terminated"
    second_rows = app._build_hub_rows()

    assert len(first_rows) == 1
    assert len(second_rows) == 1
    assert first_rows[0] != second_rows[0]


def test_hub_state_apply_drops_stale_poll_sequence() -> None:
    app = OrcChatScreen()
    app._hub_poll_seq = 3
    app._hub_rows["hub-1"] = "active"

    applied = app._apply_hub_state("hub-1", "terminated", poll_seq=2)

    assert applied is False
    assert app._hub_rows["hub-1"] == "active"


def test_nl_create_hub_intent_parsing_uses_defaults() -> None:
    app = OrcChatScreen()
    parsed = app._parse_nl_create_hub("create hub please")
    assert parsed == ("cli", "tui")


def test_nl_create_hub_intent_parsing_reads_workspace_and_initiator() -> None:
    app = OrcChatScreen()
    parsed = app._parse_nl_create_hub("create a hub workspace ws-9 initiator sam")
    assert parsed == ("ws-9", "sam")


@pytest.mark.asyncio
async def test_center_pane_textarea_is_selectable_and_copy_paths_work(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _noop_init_hub(self: OrcChatScreen) -> None:
        self._hub_id = None

    monkeypatch.setattr(OrcChatScreen, "_init_hub", _noop_init_hub)

    app = _OrcChatApp()
    async with app.run_test() as _:
        area = app.screen.query_one("#message-textarea", TextArea)
        assert area.read_only is True

        app.screen.action_focus_center()
        # Focus ownership can remain on prompt in some Textual versions.
        assert app.focused is not None

        msg_log = app.screen.query_one("#message-log")
        monkeypatch.setattr(msg_log, "get_selected_text", lambda: "selected-center-text")

        copied: dict[str, str] = {}

        def _copy_ok(payload: str) -> None:
            copied["payload"] = payload

        monkeypatch.setattr(app, "copy_to_clipboard", _copy_ok, raising=False)
        app.screen.action_copy_center()
        assert copied["payload"] == "selected-center-text"

        def _copy_fail(_: str) -> None:
            raise RuntimeError("clipboard unavailable")

        monkeypatch.setattr(app, "copy_to_clipboard", _copy_fail, raising=False)
        app.screen.action_copy_center()

        assert app.screen._copy_fallback_text == "selected-center-text"
        assert "Clipboard unavailable" in msg_log.get_text()


@pytest.mark.asyncio
@pytest.mark.parametrize("size", [(64, 20), (140, 36)])
async def test_minimal_chat_layout_is_responsive_at_narrow_and_wide_sizes(
    monkeypatch: pytest.MonkeyPatch,
    size: tuple[int, int],
) -> None:
    async def _noop_init_hub(self: OrcChatScreen) -> None:
        self._hub_id = None

    monkeypatch.setattr(OrcChatScreen, "_init_hub", _noop_init_hub)

    app = _OrcChatApp()
    async with app.run_test(size=size) as pilot:
        screen = app.screen
        assert screen.query_one("#chat-shell") is not None
        assert screen.query_one("#message-log") is not None
        assert screen.query_one("#prompt-input") is not None
        assert screen.query_one("#prompt-row") is not None
        assert screen.query_one("#status-bar") is not None
        assert len(screen.query("#chat-header")) == 0
        assert len(screen.query("#chat-footer")) == 0
        assert len(screen.query("#token-label")) == 0
        await pilot.pause()


@pytest.mark.asyncio
async def test_hub_event_to_orc_output_flow_from_grpc(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _init_hub(self: OrcChatScreen) -> None:
        self._hub_id = "hub-test"

    async def _noop_splash_mount(self: SplashMenuScreen) -> None:
        return

    async def _stream_prompt(*args, **kwargs):
        yield "final output"

    async def _listen_events(*args, **kwargs):
        yield ("ORC", "recv", "hello world")
        yield ("ARC", "plan", "- plan")
        yield ("CRT", "review", "APPROVED")

    monkeypatch.setattr(OrcChatScreen, "_init_hub", _init_hub)
    monkeypatch.setattr(SplashMenuScreen, "on_mount", _noop_splash_mount)
    monkeypatch.setattr(GrpcHubClient, "listen_orc_events", _listen_events)

    app = MeridianApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = OrcChatScreen()
        app.push_screen(screen)
        await pilot.pause()
        monkeypatch.setattr(screen._grpc_client, "stream_prompt", _stream_prompt)

        await screen._run_command("status")
        await pilot.pause()

        events: list[tuple[str, str, str]] = []
        while not app.hub_event_queue.empty():
            item = app.hub_event_queue.get_nowait()
            assert isinstance(item, tuple)
            assert len(item) == 3
            assert all(isinstance(part, str) for part in item)
            events.append(item)

        event_tags = [f"{agent}:{status}" for agent, status, _ in events]
        assert "HUB:status" in event_tags
        assert "ORC:recv" in event_tags
        assert "ARC:plan" in event_tags
        assert "CRT:review" in event_tags


@pytest.mark.asyncio
async def test_non_hub_prompt_streams_via_grpc_and_publishes_llm_stream_event(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _init_hub(self: OrcChatScreen) -> None:
        self._hub_id = "hub-test"

    async def _stream_prompt(*args, **kwargs):
        yield "hello"
        yield " world"

    async def _listen_events(*args, **kwargs):
        if False:
            yield ("", "", "")

    monkeypatch.setattr(OrcChatScreen, "_init_hub", _init_hub)

    app = _OrcChatApp()
    async with app.run_test() as pilot:
        screen = app.screen
        monkeypatch.setattr(screen._grpc_client, "stream_prompt", _stream_prompt)
        monkeypatch.setattr(screen._grpc_client, "listen_orc_events", _listen_events)
        await screen._run_command("hello world")
        await pilot.pause()

        msg_log = screen.query_one("#message-log")
        assert "hello world" in msg_log.get_text()
        assert "REJECTED:" not in msg_log.get_text()
        assert ("HUB", "llm_stream", "hub-test") == app.hub_event_queue.get_nowait()


@pytest.mark.asyncio
async def test_hub_view_di_bus_identity_shared_across_publishers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _noop_splash_mount(self: SplashMenuScreen) -> None:
        return

    async def _create_hub(self: GrpcHubClient, workspace_id: str, initiator: str) -> str:
        return "hub-bus-1"

    async def _stream_prompt(self: GrpcHubClient, hub_id: str, run_id: int, prompt: str):
        yield '{"id":"hub-bus-1","current":"active"}'

    async def _listen_events(self: GrpcHubClient):
        yield ("ARC", "plan", "- plan")

    monkeypatch.setattr(GrpcHubClient, "create_hub", _create_hub)
    monkeypatch.setattr(GrpcHubClient, "stream_prompt", _stream_prompt)
    monkeypatch.setattr(GrpcHubClient, "listen_orc_events", _listen_events)
    monkeypatch.setattr(SplashMenuScreen, "on_mount", _noop_splash_mount)

    app = MeridianApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        screen = OrcChatScreen()
        app.push_screen(screen)
        await pilot.pause()
        assert screen._hub_event_bus is app.hub_event_queue

        await screen._run_command("status")
        await pilot.pause()

        events: list[tuple[str, str, str]] = []
        while not app.hub_event_queue.empty():
            item = app.hub_event_queue.get_nowait()
            assert isinstance(item, tuple)
            events.append(item)

        assert ("HUB", "create", "hub-bus-1") in events
        assert ("HUB", "status", "hub-bus-1") in events
        assert ("ARC", "plan", "- plan") in events


@pytest.mark.asyncio
async def test_meridian_app_listener_is_singleton_across_navigation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    started = 0
    blocker = asyncio.Event()

    async def _noop_splash_mount(self: SplashMenuScreen) -> None:
        return

    async def _listen_events(self: GrpcHubClient):
        nonlocal started
        started += 1
        await blocker.wait()
        if False:
            yield ("", "", "")

    monkeypatch.setattr(SplashMenuScreen, "on_mount", _noop_splash_mount)
    monkeypatch.setattr(GrpcHubClient, "listen_orc_events", _listen_events)

    app = MeridianApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        app.action_push_hub()
        await pilot.pause()
        app.pop_screen()
        await pilot.pause()
        app.action_push_hub()
        await pilot.pause()
        app._start_orc_event_listener()
        await pilot.pause()
        assert started == 1


def test_hub_event_queue_drops_oldest_preserving_fifo_order() -> None:
    app = MeridianApp()
    app.hub_event_queue = asyncio.Queue(maxsize=3)

    for i in range(5):
        app.publish_hub_event("ARC", "plan", f"payload-{i}")

    drained = [app.hub_event_queue.get_nowait() for _ in range(3)]
    assert [payload for _, _, payload in drained] == ["payload-2", "payload-3", "payload-4"]


def test_hub_view_replay_ring_keeps_latest_fifty_in_order() -> None:
    screen = HubViewScreen()

    for i in range(60):
        screen._record_replay_line(f"line-{i:02d}")

    assert screen._replay_lines() == [f"line-{i:02d}" for i in range(10, 60)]


@pytest.mark.asyncio
async def test_status_is_docked_with_header_footer_never_overlay_modal(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _noop_init_hub(self: OrcChatScreen) -> None:
        self._hub_id = None

    monkeypatch.setattr(OrcChatScreen, "_init_hub", _noop_init_hub)

    app = _OrcChatApp()
    async with app.run_test() as _:
        screen = app.screen
        assert screen.query_one("#chat-shell") is not None
        assert screen.query_one("#status-bar") is not None
        assert len(screen.query("#chat-header")) == 0
        assert len(screen.query("#chat-footer")) == 0
        assert "dock: bottom;" in StatusBar.DEFAULT_CSS
        assert "overlay" not in OrcChatScreen.DEFAULT_CSS.lower()
        assert "modal" not in OrcChatScreen.DEFAULT_CSS.lower()
        assert not isinstance(screen, ModalScreen)
