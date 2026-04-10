"""Interactive TUI for the Meridian agentic CLI.

Multi-screen Textual application with five screens:

- SplashMenuScreen: startup menu with hub online/offline detection.
- OrcChatScreen: minimal chat shell with transcript, prompt row, and status bar.
- HubViewScreen: live OrchestratorEvents stream viewer.
- ProviderScreen: step-through LLM provider + model configuration wizard.
- McpScreen: MCP server management (read/add entries in .mcp.json).

Key bindings
------------
SplashMenuScreen:
  Up/Down       Navigate menu items
  Enter         Activate selection
  q / Ctrl+C    Quit

OrcChatScreen:
  Enter         Send prompt
  Ctrl+K        Clear conversation log
  Ctrl+X        Push HubViewScreen
  Ctrl+E        Focus center output pane
  Ctrl+Shift+C  Copy selected (or full) output to clipboard
  /connect      Switch LLM provider (pushes ProviderScreen)

HubViewScreen:
  Escape        Return to OrcChatScreen

ProviderScreen / McpScreen:
  Up/Down/Enter Navigate wizard steps or list items
  Escape        Return to previous screen

The legacy ``MeridianTUI`` name is preserved as an alias for ``MeridianApp``
for backward compatibility with any external callers. ``run_tui`` launches
``MeridianApp`` which pushes ``SplashMenuScreen`` on startup.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import uuid
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Container, Horizontal, Vertical
from textual.events import Key
from textual.reactive import reactive
from textual.screen import Screen
from textual.widgets import (
    Button,
    Input,
    Label,
    LoadingIndicator,
    RichLog,
    Static,
    TextArea,
)

from .config import AgenticCliConfig
from .contracts import RunCommand, RunReceipt
from .grpc_client import GrpcHubClient
from .service import AgenticCliService
from .memory import InMemoryConversationStore
from src.orc.dispatch import resolve_pre_llm_hub_intent

logger = logging.getLogger(__name__)

_SESSION_FALLBACK_NAMESPACE = uuid.NAMESPACE_URL
_SESSION_FALLBACK_SEED_PREFIX = "meridian://agentic-cli/session"

AGENT_PROMPTS: dict[str, str] = {
    "ORC": (
        "You are an orchestrator. In 1-2 sentences, restate the user's task clearly "
        "and identify what kind of response is needed."
    ),
    "ARC": (
        "You are an architect. Given the task below, produce a concise plan in 3-5 "
        "bullet points covering what needs to be done. Be specific."
    ),
    "CRT": (
        "You are a critic. Review the plan below. Reply with exactly 'APPROVED' on "
        "the first line if the plan is sound, or 'REJECTED: <reason>' on the first "
        "line if it has critical flaws. Then briefly explain."
    ),
    "IMP": (
        "You are an implementer. Given the task and approved plan below, produce the "
        "actual response or solution for the user. Be direct, complete, and helpful."
    ),
}


# ─── Shared widgets ───────────────────────────────────────────────────────────

@dataclass
class _MessageEntry:
    """A single message displayed in the conversation log."""

    role: str  # "user", "assistant", "system", "error"
    content: str
    turn_index: int | None = None


class MessageLog(Vertical):
    """Scrollable message display area with color-coded entries."""

    DEFAULT_CSS = """
    MessageLog {
        height: 1fr;
        background: $surface;
        border: none;
        padding: 1 2;
    }
    MessageLog > TextArea {
        background: transparent;
    }
    """

    def compose(self) -> ComposeResult:
        self._entries: list[str] = []
        self._text_area = TextArea("", id="message-textarea", read_only=True)
        yield self._text_area

    def _append_line(self, line: str) -> None:
        self._entries.append(line)
        self._text_area.load_text("\n".join(self._entries))

    def add_message(self, entry: _MessageEntry) -> None:
        """Append a color-coded message to the log."""
        if entry.role == "user":
            self._append_line(f"▸ {entry.content}")
        elif entry.role == "assistant":
            self._append_line(f"▸ {entry.content}")
        elif entry.role == "error":
            self._append_line(f"✗ {entry.content}")
        elif entry.role == "system":
            self._append_line(f"ℹ {entry.content}")

    def get_text(self) -> str:
        """Return full message text currently visible in the center pane."""
        return "\n".join(self._entries)

    def get_selected_text(self) -> str:
        """Best-effort selected text extraction across Textual versions."""
        selected = getattr(self._text_area, "selected_text", "")
        if callable(selected):
            try:
                maybe_text = selected()
                return maybe_text if isinstance(maybe_text, str) else ""
            except Exception:
                return ""
        return selected if isinstance(selected, str) else ""

    def focus_text(self) -> None:
        """Focus the center text area for mouse/keyboard selection."""
        self._text_area.focus()

    def clear_log(self) -> None:
        """Clear all messages."""
        self._entries.clear()
        self._text_area.load_text("")


class StatusBar(Horizontal):
    """Status bar showing session info and keybindings."""

    DEFAULT_CSS = """
    StatusBar {
        dock: bottom;
        height: 1;
        background: #111;
        color: #666;
        padding: 0 1;
    }
    StatusBar .status-item {
        padding: 0 1;
    }
    StatusBar .key-hint {
        color: $text-muted;
        padding: 0 1;
    }
    """

    session_id: reactive[str] = reactive("")
    model_name: reactive[str] = reactive("")
    backend_mode: reactive[str] = reactive("")

    def __init__(
        self,
        session_id: str = "",
        model_name: str = "",
        backend_mode: str = "",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.session_id = session_id
        self.model_name = model_name
        self.backend_mode = backend_mode

    def compose(self) -> ComposeResult:
        yield Label(f"Session: {self.session_id}", id="session-label", classes="status-item")
        yield Label(f"Model: {self.model_name}", id="model-label", classes="status-item")
        yield Label(f"Backend: {self.backend_mode}", id="backend-label", classes="status-item")
        yield Label("Enter: send | Ctrl+C: quit | Ctrl+K: clear", classes="key-hint")

    def watch_session_id(self, value: str) -> None:
        try:
            label = self.query_one("#session-label", Label)
            label.update(f"Session: {value}")
        except Exception:
            pass

    def watch_model_name(self, value: str) -> None:
        try:
            label = self.query_one("#model-label", Label)
            label.update(f"Model: {value}")
        except Exception:
            pass

    def watch_backend_mode(self, value: str) -> None:
        try:
            label = self.query_one("#backend-label", Label)
            label.update(f"Backend: {value}")
        except Exception:
            pass


# ─── App entry-point ──────────────────────────────────────────────────────────

class MeridianApp(App):
    """Root app — pushes SplashMenuScreen on startup."""

    TITLE = "Meridian Hub"

    CSS = """
    Screen {
        background: #0d0d0d;
        color: #e0e0e0;
    }
    """

    BINDINGS = [Binding("ctrl+x", "push_hub", "HUB View", priority=True)]

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.hub_event_queue: asyncio.Queue[tuple[str, str, str]] = asyncio.Queue(maxsize=200)
        self._orc_event_listener_task: asyncio.Task[None] | None = None
        self._orc_event_client = GrpcHubClient()

    def action_push_hub(self) -> None:
        self.push_screen(HubViewScreen())

    def on_mount(self) -> None:
        self._start_orc_event_listener()
        self.push_screen(SplashMenuScreen())

    def _enqueue_hub_event_with_drop_oldest(self, event: tuple[str, str, str]) -> None:
        """Append event to bounded queue; drop oldest on overflow."""
        if self.hub_event_queue.full():
            try:
                self.hub_event_queue.get_nowait()
            except asyncio.QueueEmpty:
                pass
        self.hub_event_queue.put_nowait(event)

    def publish_hub_event(self, agent: str, status: str, payload: str) -> None:
        """Publish a HUB-VIEW frame on the app queue."""
        self._enqueue_hub_event_with_drop_oldest((agent, status, payload))

    def _start_orc_event_listener(self) -> None:
        """Start global ORC event listener once for app lifetime."""
        if self._orc_event_listener_task is not None and not self._orc_event_listener_task.done():
            return
        self._orc_event_listener_task = asyncio.create_task(self._listen_orc_events())

    async def _listen_orc_events(self) -> None:
        """Forward live orchestrator events from gRPC into app queue, with reconnect."""
        backoff = 2.0
        while True:
            try:
                # Re-create the client each attempt so the channel is fresh.
                self._orc_event_client = GrpcHubClient()
                async for agent, status, payload in self._orc_event_client.listen_orc_events():
                    backoff = 2.0  # reset on successful receive
                    self.publish_hub_event(agent, status, payload)
            except asyncio.CancelledError:
                return
            except Exception:
                pass
            # Brief pause before reconnecting; cap at 30 s.
            try:
                await asyncio.sleep(backoff)
            except asyncio.CancelledError:
                return
            backoff = min(backoff * 2, 30.0)

    async def on_shutdown(self) -> None:
        """Cancel global listener and close client on app shutdown."""
        if self._orc_event_listener_task is not None:
            self._orc_event_listener_task.cancel()
            try:
                await self._orc_event_listener_task
            except asyncio.CancelledError:
                pass
            self._orc_event_listener_task = None
        try:
            await self._orc_event_client.close()
        except Exception:
            pass


# ─── Splash / Main Menu ───────────────────────────────────────────────────────

class SplashMenuScreen(Screen):
    """Startup menu with hub online/offline detection."""

    BINDINGS = [
        ("q", "quit", "Quit"),
        ("ctrl+c", "quit", "Quit"),
    ]

    DEFAULT_CSS = """
    SplashMenuScreen {
        align: center middle;
    }
    #splash-container {
        width: 50;
        height: auto;
        padding: 2 4;
    }
    #splash-title {
        text-align: center;
        text-style: bold;
        color: #5f9ea0;
        padding: 0 0 1 0;
    }
    .menu-item {
        padding: 0 2;
        height: 1;
        color: $text;
    }
    .menu-item-selected {
        padding: 0 2;
        height: 1;
        color: #6c9bcf;
        text-style: bold;
    }
    .menu-item-dimmed {
        padding: 0 2;
        height: 1;
        color: $text-muted;
    }
    #hub-status-note {
        margin: 1 0 0 0;
        color: $warning;
        text-align: center;
    }
    """

    MENU_ITEMS = ["Start Chat", "Connect Provider", "MCP Settings", "Quit"]

    _selected: reactive[int] = reactive(0)
    _hub_online: bool = False

    def compose(self) -> ComposeResult:
        with Vertical(id="splash-container"):
            yield Label("Meridian Hub", id="splash-title")
            for i, item in enumerate(self.MENU_ITEMS):
                css_class = "menu-item-selected" if i == 0 else "menu-item"
                yield Label(f"  {item}", id=f"menu-item-{i}", classes=css_class)
            yield Label("", id="hub-status-note")

    async def on_mount(self) -> None:
        """Ping hub to detect online state; dim Start Chat if offline."""
        grpc_client = GrpcHubClient()
        try:
            await grpc_client.hub_status("ping-check")
            self._hub_online = True
        except Exception:
            self._hub_online = False
            try:
                note = self.query_one("#hub-status-note", Label)
                note.update("Hub offline — Start Chat requires hub connection")
            except Exception:
                pass
        finally:
            try:
                await grpc_client.close()
            except Exception:
                pass
        self._refresh_menu()

    def watch__selected(self, value: int) -> None:
        self._refresh_menu()

    def _refresh_menu(self) -> None:
        """Re-render menu item labels based on current selection."""
        for i, item in enumerate(self.MENU_ITEMS):
            try:
                label = self.query_one(f"#menu-item-{i}", Label)
            except Exception:
                continue
            is_selected = i == self._selected
            is_dimmed = (i == 0 and not self._hub_online)
            if is_selected:
                label.add_class("menu-item-selected")
                label.remove_class("menu-item")
                label.remove_class("menu-item-dimmed")
                label.update(f"> {item}")
            elif is_dimmed:
                label.add_class("menu-item-dimmed")
                label.remove_class("menu-item")
                label.remove_class("menu-item-selected")
                label.update(f"  {item}")
            else:
                label.add_class("menu-item")
                label.remove_class("menu-item-selected")
                label.remove_class("menu-item-dimmed")
                label.update(f"  {item}")

    def on_key(self, event: Key) -> None:
        """Navigate with Up/Down; activate with Enter."""
        if event.key == "up":
            self._selected = max(0, self._selected - 1)
        elif event.key == "down":
            self._selected = min(len(self.MENU_ITEMS) - 1, self._selected + 1)
        elif event.key == "enter":
            self._activate_selection()

    def _activate_selection(self) -> None:
        """Route to the appropriate screen based on current selection."""
        idx = self._selected
        if idx == 0:
            self.app.push_screen(OrcChatScreen())
        elif idx == 1:
            self.app.push_screen(ProviderScreen())
        elif idx == 2:
            self.app.push_screen(McpScreen())
        elif idx == 3:
            self.app.exit()

    def action_quit(self) -> None:
        self.app.exit()


# ─── ORC Chat Screen ──────────────────────────────────────────────────────────

class OrcChatScreen(Screen):
    """Main chat screen with transcript pane, prompt input, and status bar."""

    BINDINGS = [
        ("ctrl+k", "clear_history", "Clear"),
        ("ctrl+c", "quit", "Quit"),
        ("ctrl+e", "focus_center", "Focus Output"),
        ("ctrl+shift+c", "copy_center", "Copy Output"),
    ]

    DEFAULT_CSS = """
    OrcChatScreen {
        layout: vertical;
        align: center top;
        padding: 0 1;
    }
    #chat-shell {
        width: 100%;
        max-width: 108;
        height: 1fr;
        layout: vertical;
        padding: 1 0;
    }
    #loading-indicator {
        display: none;
        height: 1;
    }
    #center-panel {
        width: 1fr;
        layout: vertical;
        border: none;
    }
    #prompt-row {
        height: 3;
        layout: horizontal;
        margin: 1 0 0 0;
    }
    #prompt-row Input {
        width: 1fr;
    }
    """

    mouse_support = True

    def __init__(
        self,
        service: "AgenticCliService | None" = None,
        config: "AgenticCliConfig | None" = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self._config = config if config is not None else AgenticCliConfig()
        self._service = service if service is not None else AgenticCliService(config=self._config)
        self._current_session_id: str | None = None
        self._grpc_client = GrpcHubClient()
        self._hub_id: str | None = None
        self._run_counter: int = 0
        self._copy_fallback_text: str = ""
        self._hub_rows: dict[str, str] = {}
        self._hub_poll_seq: int = 0
        self._hub_event_bus: asyncio.Queue[tuple[str, str, str]] | None = None

        load_dotenv(Path(__file__).parent.parent.parent / ".env")

        self._llm_provider = None

    def compose(self) -> ComposeResult:
        with Vertical(id="chat-shell"):
            yield Vertical(
                MessageLog(id="message-log"),
                LoadingIndicator(id="loading-indicator"),
                id="center-panel",
            )
            yield Horizontal(
                Input(placeholder="Type your prompt... (Enter to send)", id="prompt-input"),
                id="prompt-row",
            )
        yield StatusBar(
            session_id="(none)",
            model_name=self._config.llm_model,
            backend_mode=self._config.backend_mode,
            id="status-bar",
        )

    async def on_mount(self) -> None:
        """Show welcome message and initialise hub connection."""
        if getattr(self.app, "hub_event_queue", None) is None:
            self.app.hub_event_queue = asyncio.Queue(maxsize=200)
        self._hub_event_bus = self.app.hub_event_queue
        msg_log = self.query_one("#message-log", MessageLog)
        msg_log.add_message(_MessageEntry(
            role="system",
            content="Welcome to Meridian — Agentic CLI",
        ))
        actual_provider = os.environ.get("LLM_PROVIDER", "local")
        actual_model = os.environ.get("LLM_MODEL", self._config.llm_model)
        backend_label = "hub-orchestrated"
        msg_log.add_message(_MessageEntry(
            role="system",
            content=f"Backend: {backend_label} | Model: {actual_model} | Provider: {actual_provider}",
        ))
        msg_log.add_message(_MessageEntry(
            role="system",
            content=(
                "Type a prompt and press Enter to begin. "
                "Ctrl+K: clear | Ctrl+X: HUB view | /connect: switch provider"
            ),
        ))
        self.query_one("#prompt-input", Input).focus()
        await self._init_hub()

    def _publish_hub_view_event(self, agent: str, status: str, payload: str) -> None:
        """Publish a HUB-VIEW frame on the single injected app queue."""
        publisher = getattr(self.app, "publish_hub_event", None)
        if callable(publisher):
            publisher(agent, status, payload)
            return
        if self._hub_event_bus is None:
            return
        event = (agent, status, payload)
        if self._hub_event_bus.full():
            try:
                self._hub_event_bus.get_nowait()
            except asyncio.QueueEmpty:
                pass
        self._hub_event_bus.put_nowait(event)

    async def _init_hub(self) -> None:
        """Create a hub connection."""
        try:
            self._hub_id = await self._grpc_client.create_hub(
                workspace_id="cli",
                initiator="tui",
            )
            self._publish_hub_view_event("HUB", "create", self._hub_id)
        except Exception as exc:
            logger.warning("Hub creation failed: %s", exc)

    def _derive_fallback_session_id(self) -> str:
        """Derive deterministic SID from hub/local context."""
        sid_source = f"hub:{self._hub_id}" if self._hub_id else "local"
        sid_seed = f"{_SESSION_FALLBACK_SEED_PREFIX}/{sid_source}"
        return str(uuid.uuid5(_SESSION_FALLBACK_NAMESPACE, sid_seed))

    def _resolve_session_id(self, candidate_session_id: "str | None") -> str:
        """Resolve and persist a non-blank session id across turns."""
        candidate = (candidate_session_id or "").strip()
        if candidate:
            self._current_session_id = candidate
        elif self._current_session_id is None:
            self._current_session_id = self._derive_fallback_session_id()
        return self._current_session_id or self._derive_fallback_session_id()

    def _build_hub_rows(self) -> list[str]:
        """Return a list of formatted row strings from _hub_rows, one per hub_id."""
        return [f"{hub_id}:{status}" for hub_id, status in self._hub_rows.items()]

    def _apply_hub_state(self, hub_id: str, status: str, poll_seq: int) -> bool:
        """Update hub state only if poll_seq is not stale. Returns True if applied."""
        if poll_seq < self._hub_poll_seq:
            return False
        self._hub_rows[hub_id] = status
        self._hub_poll_seq = poll_seq
        return True

    def _parse_nl_create_hub(self, prompt_text: str) -> tuple[str, str] | None:
        """Return (workspace_id, initiator) for NL hub-creation prompts."""
        normalized = " ".join(prompt_text.strip().lower().split())
        starters = ("create hub", "create a hub", "new hub", "start hub")
        if not normalized.startswith(starters):
            return None
        tokens = prompt_text.strip().split()
        workspace_id = "cli"
        initiator = "tui"
        for idx, token in enumerate(tokens[:-1]):
            key = token.lower().strip(":=")
            if key == "workspace":
                workspace_id = tokens[idx + 1]
            elif key == "initiator":
                initiator = tokens[idx + 1]
        return (workspace_id, initiator)

    def on_input_submitted(self, event: Input.Submitted) -> None:
        """Handle Enter key in the prompt input."""
        prompt_text = event.value.strip()
        if not prompt_text:
            return
        event.input.value = ""

        # /create-hub intercept
        if prompt_text.startswith("/create-hub"):
            parts = prompt_text.split()
            args = parts[1:]
            self.call_later(self._handle_hub_command, "create_hub", args)
            return

        # /connect intercept
        if prompt_text.startswith("/connect"):
            self.app.push_screen(ProviderScreen())
            return

        # natural-language create-hub intercept
        nl_create = self._parse_nl_create_hub(prompt_text)
        if nl_create is not None:
            self.call_later(self._handle_hub_command, "create_hub", list(nl_create))
            return

        msg_log = self.query_one("#message-log", MessageLog)
        msg_log.add_message(_MessageEntry(role="user", content=prompt_text))
        self.run_worker(self._run_command(prompt_text), exclusive=True)

    async def _handle_hub_command(self, intent: str, args: list[str]) -> None:
        """Handle slash commands that target the hub (e.g. /create-hub)."""
        msg_log = self.query_one("#message-log", MessageLog)
        if intent == "create_hub":
            if len(args) != 2:
                msg_log.add_message(_MessageEntry(
                    role="system",
                    content="Usage: /create-hub <workspace_id> <initiator>",
                ))
                return
            if self._grpc_client is None:
                msg_log.add_message(_MessageEntry(
                    role="error",
                    content="Hub client not available.",
                ))
                return
            try:
                hub_id = await self._grpc_client.create_hub(args[0], args[1])
                self._hub_id = hub_id
                msg_log.add_message(_MessageEntry(
                    role="system",
                    content=f"Hub created: {hub_id}",
                ))
            except Exception as exc:
                msg_log.add_message(_MessageEntry(
                    role="error",
                    content=f"create_hub failed: {exc}",
                ))

    async def _run_command(self, prompt_text: str) -> None:
        """Execute a run command and display the result."""
        msg_log = self.query_one("#message-log", MessageLog)
        try:
            loading = self.query_one("#loading-indicator", LoadingIndicator)
            if self._hub_id is None:
                await self._init_hub()

            self._run_counter += 1
            run_id = self._run_counter
            accumulated: list[str] = []
            loading.display = True
            try:
                hub_intent = resolve_pre_llm_hub_intent(prompt_text)
                if hub_intent is not None:
                    # Hub-specific command (status, report, location)
                    self._publish_hub_view_event("HUB", hub_intent, self._hub_id or "")
                    accumulated.append(f"Hub {hub_intent}: {self._hub_id or 'no hub'}")
                elif self._hub_id is not None:
                    # Hub available — stream via gRPC
                    self._publish_hub_view_event("HUB", "llm_stream", self._hub_id)
                    async for chunk_text in self._grpc_client.stream_prompt(
                        hub_id=self._hub_id,
                        run_id=run_id,
                        prompt=prompt_text,
                    ):
                        accumulated.append(chunk_text)
                else:
                    # Hub unavailable — fall back to direct LLM
                    if self._llm_provider is None:
                        from src.llm.factory import create_provider
                        provider_name = os.environ.get("LLM_PROVIDER", self._config.llm_provider)
                        self._llm_provider = create_provider(provider_name)
                    llm_model = os.environ.get("LLM_MODEL", self._config.llm_model)
                    command = RunCommand(prompt=prompt_text, session_id=self._current_session_id)
                    async for chunk in self._service.async_stream_with_llm(
                        command, self._llm_provider, llm_model=llm_model
                    ):
                        accumulated.append(chunk)
            finally:
                loading.display = False
            full_output = "".join(accumulated)
            receipt_session_id = self._current_session_id or ""
            receipt_turn_index = run_id

            resolved_session_id = self._resolve_session_id(receipt_session_id)

            status_bar = self.query_one("#status-bar", StatusBar)
            status_bar.session_id = resolved_session_id

            msg_log.add_message(_MessageEntry(
                role="assistant",
                content=full_output,
                turn_index=receipt_turn_index,
            ))
        except Exception as exc:
            msg_log.add_message(_MessageEntry(
                role="error",
                content=f"Error: {exc}",
            ))

    def action_quit(self) -> None:
        self.app.exit()

    def action_clear_history(self) -> None:
        """Clear the message log."""
        msg_log = self.query_one("#message-log", MessageLog)
        msg_log.clear_log()
        msg_log.add_message(_MessageEntry(
            role="system",
            content="Conversation cleared. Type a new prompt to begin.",
        ))

    def action_focus_center(self) -> None:
        """Focus the center pane for keyboard and mouse selection."""
        self.query_one("#message-log", MessageLog).focus_text()

    def action_copy_center(self) -> None:
        """Copy selected center-pane text to clipboard with fallback."""
        msg_log = self.query_one("#message-log", MessageLog)
        selected_text = msg_log.get_selected_text() or msg_log.get_text()
        if not selected_text:
            msg_log.add_message(_MessageEntry(role="system", content="Nothing selected to copy."))
            return
        try:
            self.app.copy_to_clipboard(selected_text)
            msg_log.add_message(_MessageEntry(role="system", content="Copied selection to clipboard."))
            self._copy_fallback_text = ""
        except Exception:
            self._copy_fallback_text = selected_text
            msg_log.add_message(_MessageEntry(
                role="system",
                content="Clipboard unavailable; selection kept in session for manual copy.",
            ))


# ─── HUB View Screen ──────────────────────────────────────────────────────────

class HubViewScreen(Screen):
    """Live OrchestratorEvents stream viewer."""

    BINDINGS = [("escape", "pop_screen", "Back")]

    DEFAULT_CSS = """
    HubViewScreen {
        layout: vertical;
    }
    #hub-view-header {
        height: 1;
        color: $accent;
        text-align: center;
        text-style: bold;
        padding: 0 1;
    }
    #hub-event-log {
        height: 1fr;
        border-top: solid #333;
        padding: 0 1;
    }
    #hub-view-footer {
        height: 1;
        color: $text-muted;
        text-align: center;
        padding: 0 1;
    }
    """

    AGENT_COLORS: dict[str, str] = {
        "ARC": "cyan",
        "CRT": "yellow",
        "IMP": "green",
        "IMP2": "blue",
        "TST": "magenta",
        "DOC": "white",
        "ORC": "bright_white",
    }

    def compose(self) -> ComposeResult:
        yield Label("HUB VIEW — ESC to exit", id="hub-view-header")
        yield RichLog(id="hub-event-log", auto_scroll=True, markup=True, highlight=False, wrap=True)
        yield Label("Ctrl+C: quit | ESC: back", id="hub-view-footer")

    _received_event: bool = False
    _render_seq: int = 0

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._replay_ring: deque[str] = deque(maxlen=50)

    async def on_mount(self) -> None:
        """Start streaming worker; show placeholder after 3 seconds if quiet."""
        self.run_worker(self._stream_events, exclusive=True)
        self.set_timer(3.0, self._maybe_show_placeholder)

    def _maybe_show_placeholder(self) -> None:
        """Show placeholder only if no events have arrived yet."""
        if self._received_event:
            return
        try:
            log = self.query_one("#hub-event-log", RichLog)
        except Exception:
            return
        log.write("[dim]Waiting for agent events — send a message in chat[/dim]")

    async def _stream_events(self) -> None:
        """Read from app-level hub_event_queue and display events."""
        try:
            log = self.query_one("#hub-event-log", RichLog)
        except Exception:
            return
        queue: asyncio.Queue = getattr(self.app, "hub_event_queue", None)
        if queue is None:
            log.write("[dim]No event queue available[/dim]")
            return
        while True:
            try:
                agent_code, status, payload = await asyncio.wait_for(queue.get(), timeout=30.0)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                return
            except Exception:
                return
            self._received_event = True
            self._render_seq += 1
            color = self.AGENT_COLORS.get(agent_code.upper(), "white")
            status_part = f" [{status}]" if status else ""
            rendered = f"[dim]{self._render_seq:03d}[/dim] [{color}]{agent_code}[/{color}]{status_part} > {payload}"
            self._record_replay_line(rendered)
            log.write(rendered)

    def _record_replay_line(self, line: str) -> None:
        """Track the latest rendered lines in a local replay ring."""
        self._replay_ring.append(line)

    def _replay_lines(self) -> list[str]:
        """Return local replay lines in FIFO order."""
        return list(self._replay_ring)

    def action_pop_screen(self) -> None:
        self.app.pop_screen()


# ─── Provider Connect Screen ──────────────────────────────────────────────────

class ProviderScreen(Screen):
    """Step-through LLM provider and model configuration wizard."""

    BINDINGS = [("escape", "pop_screen", "Back")]

    DEFAULT_CSS = """
    ProviderScreen {
        align: center middle;
    }
    #provider-container {
        width: 60;
        height: auto;
        padding: 2 4;
    }
    #provider-title {
        text-align: center;
        text-style: bold;
        color: #5f9ea0;
        padding: 0 0 1 0;
    }
    #provider-subtitle {
        color: $text-muted;
        text-align: center;
        padding: 0 0 1 0;
    }
    .provider-item {
        padding: 0 2;
        height: 1;
        color: $text;
    }
    .provider-item-selected {
        padding: 0 2;
        height: 1;
        background: $accent;
        color: $background;
        text-style: bold;
    }
    #provider-api-key-input {
        width: 1fr;
        margin: 1 0 0 0;
    }
    #provider-base-url-input {
        width: 1fr;
        margin: 0 0 1 0;
    }
    #provider-confirm-btn {
        width: 100%;
        margin: 1 0 0 0;
    }
    #provider-status {
        color: $success;
        text-align: center;
        margin: 1 0 0 0;
    }
    """

    PROVIDERS: dict[str, Any] = {
        "Anthropic": {
            "env_key": "ANTHROPIC_API_KEY",
            "env_provider": "anthropic",
            "models": [
                ("claude-opus-4-6", "Most capable"),
                ("claude-sonnet-4-6", "Balanced"),
                ("claude-haiku-4-5", "Fast"),
            ],
        },
        "OpenAI": {
            "env_key": "OPENAI_API_KEY",
            "env_provider": "openai",
            "models": [
                ("gpt-4o", "Flagship"),
                ("gpt-4o-mini", "Efficient"),
                ("o3-mini", "Reasoning"),
            ],
        },
        "Nvidia NIM": {
            "env_key": "NVIDIA_API_KEY",
            "env_provider": "nvidia",
            "models": [
                ("meta/llama-3.1-70b-instruct", "Open 70B"),
                ("nvidia/llama-3.1-nemotron-70b-reward", "Reward model"),
            ],
        },
        "OpenRouter": {
            "env_key": "OPENROUTER_API_KEY",
            "env_provider": "openrouter",
            "models": [
                ("auto", "Best available"),
                ("google/gemini-2.5-pro", "Gemini Pro"),
                ("meta-llama/llama-3.3-70b-instruct", "Llama 70B"),
            ],
        },
    }

    # Steps: 0 = provider list, 1 = API key entry, 2 = model selection
    _step: reactive[int] = reactive(0)
    _provider_selected: reactive[int] = reactive(0)
    _model_selected: reactive[int] = reactive(0)
    _chosen_provider: str = ""

    def compose(self) -> ComposeResult:
        provider_names = list(self.PROVIDERS.keys())
        with Vertical(id="provider-container"):
            yield Label("Connect Provider", id="provider-title")
            yield Label("Step 1: Choose a provider", id="provider-subtitle")
            for i, name in enumerate(provider_names):
                css_class = "provider-item-selected" if i == 0 else "provider-item"
                yield Label(f"  {name}", id=f"provider-item-{i}", classes=css_class)
            yield Input(
                placeholder="API Key (password)",
                id="provider-api-key-input",
                password=True,
            )
            yield Input(
                placeholder="Base URL (optional, leave blank for default)",
                id="provider-base-url-input",
            )
            for i, (model_id, desc) in enumerate(
                self.PROVIDERS[provider_names[0]]["models"]
            ):
                css_class = "provider-item-selected" if i == 0 else "provider-item"
                yield Label(
                    f"  {model_id} — {desc}",
                    id=f"model-item-{i}",
                    classes=css_class,
                )
            yield Button("Confirm", id="provider-confirm-btn", variant="primary")
            yield Label("", id="provider-status")

        # Initial state: show only provider list
        self._set_step(0)

    def _set_step(self, step: int) -> None:
        """Show/hide widgets for the given wizard step."""
        self._step = step
        provider_names = list(self.PROVIDERS.keys())
        n_providers = len(provider_names)

        # Determine counts for current provider models
        if self._chosen_provider and self._chosen_provider in self.PROVIDERS:
            models = self.PROVIDERS[self._chosen_provider]["models"]
        else:
            models = self.PROVIDERS[provider_names[0]]["models"]
        n_models = len(models)

        try:
            subtitle = self.query_one("#provider-subtitle", Label)
            api_key_input = self.query_one("#provider-api-key-input", Input)
            base_url_input = self.query_one("#provider-base-url-input", Input)
            confirm_btn = self.query_one("#provider-confirm-btn", Button)
        except Exception:
            return

        if step == 0:
            subtitle.update("Step 1: Choose a provider  (Up/Down + Enter)")
            api_key_input.display = False
            base_url_input.display = False
            confirm_btn.display = False
            for i in range(n_providers):
                try:
                    self.query_one(f"#provider-item-{i}", Label).display = True
                except Exception:
                    pass
            for i in range(n_models):
                try:
                    self.query_one(f"#model-item-{i}", Label).display = False
                except Exception:
                    pass
        elif step == 1:
            subtitle.update("Step 2: Enter API key  (Tab to base URL, Enter to continue)")
            for i in range(n_providers):
                try:
                    self.query_one(f"#provider-item-{i}", Label).display = False
                except Exception:
                    pass
            for i in range(n_models):
                try:
                    self.query_one(f"#model-item-{i}", Label).display = False
                except Exception:
                    pass
            api_key_input.display = True
            base_url_input.display = True
            confirm_btn.display = False
            api_key_input.focus()
        elif step == 2:
            subtitle.update("Step 3: Choose a model  (Up/Down + Enter to confirm)")
            api_key_input.display = False
            base_url_input.display = False
            confirm_btn.display = True
            for i in range(n_providers):
                try:
                    self.query_one(f"#provider-item-{i}", Label).display = False
                except Exception:
                    pass
            for i in range(n_models):
                try:
                    self.query_one(f"#model-item-{i}", Label).display = True
                except Exception:
                    pass

    def _refresh_provider_selection(self) -> None:
        provider_names = list(self.PROVIDERS.keys())
        for i in range(len(provider_names)):
            try:
                label = self.query_one(f"#provider-item-{i}", Label)
                if i == self._provider_selected:
                    label.add_class("provider-item-selected")
                    label.remove_class("provider-item")
                else:
                    label.add_class("provider-item")
                    label.remove_class("provider-item-selected")
            except Exception:
                pass

    def _refresh_model_selection(self) -> None:
        if not self._chosen_provider:
            return
        models = self.PROVIDERS[self._chosen_provider]["models"]
        for i in range(len(models)):
            try:
                label = self.query_one(f"#model-item-{i}", Label)
                if i == self._model_selected:
                    label.add_class("provider-item-selected")
                    label.remove_class("provider-item")
                else:
                    label.add_class("provider-item")
                    label.remove_class("provider-item-selected")
            except Exception:
                pass

    def _update_model_labels(self) -> None:
        """Update model labels for the chosen provider."""
        if not self._chosen_provider or self._chosen_provider not in self.PROVIDERS:
            return
        models = self.PROVIDERS[self._chosen_provider]["models"]
        for i, (model_id, desc) in enumerate(models):
            try:
                label = self.query_one(f"#model-item-{i}", Label)
                label.update(f"  {model_id} — {desc}")
            except Exception:
                pass
        # Hide extra model slots not used by this provider
        max_models = max(
            len(p["models"]) for p in self.PROVIDERS.values()
        )
        for i in range(len(models), max_models):
            try:
                self.query_one(f"#model-item-{i}", Label).display = False
            except Exception:
                pass

    def on_key(self, event: Key) -> None:
        """Navigate list items with Up/Down; advance steps with Enter."""
        provider_names = list(self.PROVIDERS.keys())

        if self._step == 0:
            if event.key == "up":
                self._provider_selected = max(0, self._provider_selected - 1)
                self._refresh_provider_selection()
            elif event.key == "down":
                self._provider_selected = min(
                    len(provider_names) - 1, self._provider_selected + 1
                )
                self._refresh_provider_selection()
            elif event.key == "enter":
                self._chosen_provider = provider_names[self._provider_selected]
                self._model_selected = 0
                self._update_model_labels()
                self._set_step(1)

        elif self._step == 1:
            if event.key == "enter":
                self._set_step(2)
                self._refresh_model_selection()

        elif self._step == 2:
            models = self.PROVIDERS[self._chosen_provider]["models"]
            if event.key == "up":
                self._model_selected = max(0, self._model_selected - 1)
                self._refresh_model_selection()
            elif event.key == "down":
                self._model_selected = min(len(models) - 1, self._model_selected + 1)
                self._refresh_model_selection()
            elif event.key == "enter":
                self._do_confirm()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "provider-confirm-btn":
            self._do_confirm()

    def _do_confirm(self) -> None:
        """Read inputs, call _save_env, update status label."""
        if not self._chosen_provider:
            return
        provider_info = self.PROVIDERS[self._chosen_provider]
        models = provider_info["models"]

        try:
            api_key_input = self.query_one("#provider-api-key-input", Input)
            base_url_input = self.query_one("#provider-base-url-input", Input)
        except Exception:
            return

        api_key = api_key_input.value.strip()
        base_url = base_url_input.value.strip()
        model_id = models[self._model_selected][0]

        updates: dict[str, str] = {
            provider_info["env_key"]: api_key,
            "LLM_PROVIDER": provider_info["env_provider"],
            "LLM_MODEL": model_id,
        }
        if base_url:
            updates["LLM_BASE_URL"] = base_url

        self._save_env(updates)

        try:
            status = self.query_one("#provider-status", Label)
            status.update(
                f"Saved: provider={provider_info['env_provider']} model={model_id}"
            )
        except Exception:
            pass

    def _save_env(self, updates: dict[str, str]) -> None:
        """Read-then-rewrite .env with comment/blank line preservation."""
        env_path = Path(".env")
        try:
            lines = env_path.read_text().splitlines()
        except FileNotFoundError:
            lines = []

        existing: dict[str, str] = {}
        order: list[str] = []
        for line in lines:
            if "=" in line and not line.startswith("#"):
                k, _, v = line.partition("=")
                existing[k.strip()] = v.strip()
                order.append(k.strip())
            else:
                order.append(line)  # preserve comments and blanks

        for k, v in updates.items():
            if k not in existing:
                order.append(k)
            existing[k] = v

        result_lines: list[str] = []
        seen: set[str] = set()
        for item in order:
            if item in existing and item not in seen:
                result_lines.append(f"{item}={existing[item]}")
                seen.add(item)
            elif item not in existing:
                result_lines.append(item)  # comment or blank line

        env_path.write_text("\n".join(result_lines) + "\n")

    def action_pop_screen(self) -> None:
        self.app.pop_screen()


# ─── MCP Settings Screen ──────────────────────────────────────────────────────

class McpScreen(Screen):
    """MCP server management — reads and adds entries in .mcp.json."""

    BINDINGS = [("escape", "pop_screen", "Back")]

    DEFAULT_CSS = """
    McpScreen {
        align: center middle;
    }
    #mcp-container {
        width: 64;
        height: auto;
        padding: 2 4;
    }
    #mcp-title {
        text-align: center;
        text-style: bold;
        color: $accent;
        padding: 0 0 1 0;
    }
    #mcp-server-log {
        height: 8;
        border-top: solid #333;
        margin: 0 0 1 0;
    }
    #mcp-name-input {
        width: 1fr;
        margin: 0 0 0 0;
    }
    #mcp-url-input {
        width: 1fr;
        margin: 0 0 1 0;
    }
    #mcp-add-btn {
        width: 100%;
    }
    #mcp-status {
        color: $success;
        text-align: center;
        margin: 1 0 0 0;
    }
    """

    _MCP_PATH = Path(".mcp.json")

    def compose(self) -> ComposeResult:
        with Vertical(id="mcp-container"):
            yield Label("MCP Server Settings", id="mcp-title")
            yield RichLog(
                id="mcp-server-log",
                auto_scroll=True,
                markup=False,
                highlight=False,
                wrap=True,
            )
            yield Input(
                placeholder="Server name (e.g. my-server)",
                id="mcp-name-input",
            )
            yield Input(
                placeholder="SSE URL (e.g. http://localhost:8080/sse)",
                id="mcp-url-input",
            )
            yield Button("Add Server", id="mcp-add-btn", variant="primary")
            yield Label("", id="mcp-status")

    def on_mount(self) -> None:
        """Read .mcp.json and populate the server list."""
        self._load_servers()

    def _load_servers(self) -> None:
        """Read .mcp.json and write current servers to the log widget."""
        try:
            log = self.query_one("#mcp-server-log", RichLog)
        except Exception:
            return
        log.clear()
        data = self._read_mcp()
        servers = data.get("mcpServers", {})
        if not servers:
            log.write("(no servers configured)")
            return
        for name, cfg in servers.items():
            url = cfg.get("url", "(no url)")
            log.write(f"{name}  →  {url}")

    def _read_mcp(self) -> dict[str, Any]:
        """Read .mcp.json; return empty structure on missing/invalid file."""
        try:
            raw = self._MCP_PATH.read_text()
            return json.loads(raw)
        except (FileNotFoundError, json.JSONDecodeError):
            return {"mcpServers": {}}

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "mcp-add-btn":
            self._handle_add()

    def _handle_add(self) -> None:
        """Read inputs and delegate to _add_server."""
        try:
            name_input = self.query_one("#mcp-name-input", Input)
            url_input = self.query_one("#mcp-url-input", Input)
        except Exception:
            return
        name = name_input.value.strip()
        url = url_input.value.strip()
        if not name or not url:
            try:
                self.query_one("#mcp-status", Label).update("Name and URL are required.")
            except Exception:
                pass
            return
        self._add_server(name, url)
        name_input.value = ""
        url_input.value = ""
        self._load_servers()
        try:
            self.query_one("#mcp-status", Label).update(f"Added: {name}")
        except Exception:
            pass

    def _add_server(self, name: str, url: str) -> None:
        """Read .mcp.json, add server entry under mcpServers[name], write back."""
        data = self._read_mcp()
        if "mcpServers" not in data:
            data["mcpServers"] = {}
        data["mcpServers"][name] = {"url": url}
        self._MCP_PATH.write_text(json.dumps(data, indent=2) + "\n")

    def action_pop_screen(self) -> None:
        self.app.pop_screen()


# ─── Backward-compatibility alias ─────────────────────────────────────────────

# cli.py calls: from .tui import run_tui
# External code that imported MeridianTUI directly still works.
MeridianTUI = MeridianApp


# ─── Entry point ──────────────────────────────────────────────────────────────

def run_tui(
    service: "AgenticCliService | None" = None,
    config: "AgenticCliConfig | None" = None,
) -> None:
    """Launch the Meridian TUI application.

    Args:
        service: Optional pre-configured AgenticCliService instance.
        config: Optional AgenticCliConfig instance for defaults.
    """
    app = MeridianApp()
    app.run()
