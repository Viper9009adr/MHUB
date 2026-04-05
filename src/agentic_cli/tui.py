"""Interactive TUI for the Meridian agentic CLI.

Wraps AgenticCliService in a Textual terminal interface with a 3-panel
horizontal layout:

- Left panel (width 20): ``RichLog`` (id ``hub-status-log``) showing live
  hub connection state, polled every 2 seconds via ``set_interval``.
- Center panel: scrollable ``MessageLog`` (RichLog) conversation history
  + ``LoadingIndicator``.
- Right panel (width 20): reserved for future context/metadata display.
- Bottom bar: prompt ``Input`` + approximate token count ``Label`` (docked).
- ``StatusBar``: session ID, model name, and backend mode (docked).

Response routing — three-branch logic in ``_run_command``:

1. **gRPC stream** (preferred): when ``on_mount`` succeeds in creating a hub
   via ``GrpcHubClient.create_hub``, prompts are sent through
   ``GrpcHubClient.stream_prompt``.  The hub_id is encoded in the
   ``agent_id`` field as ``"hub:<hub_id>:agent:cli"`` so the server-side
   ``StreamRouter`` can route the stream to the correct hub.

2. **Local LLM fallback**: when no hub is available but an LLM provider is
   configured (``LLM_PROVIDER`` env var, not ``"local"``), responses are
   streamed token-by-token via ``AgenticCliService.async_stream_with_llm``.

3. **Sync echo**: when neither a hub nor an LLM provider is available, the
   synchronous ``AgenticCliService.run`` path is used.

The ``LoadingIndicator`` is shown for the duration of any streaming path and
hidden on completion or error.

Hub status polling: ``on_mount`` registers a 2-second interval timer that
calls ``GrpcHubClient.hub_status``. The TUI stores hub state by ``hub_id`` in
``_hub_rows`` and re-renders the panel in one pass, so each hub appears once
and status updates happen in place instead of appending duplicate lines.

Session continuity: ``_resolve_session_id`` ignores blank/whitespace-only
candidate IDs and preserves the current non-blank session, falling back to a
deterministic UUID5 seed only when no session has been established yet.

Center-pane selection/copy: ``MessageLog`` uses a read-only ``TextArea`` that
supports selection. ``Ctrl+E`` focuses the center pane and ``Ctrl+Shift+C``
copies selected text (or full pane text as fallback), with in-memory fallback
when clipboard integration is unavailable.
"""

from __future__ import annotations

import asyncio
import logging
import os
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from textual.app import App, ComposeResult
from textual.containers import Container, Horizontal, Vertical
from textual.widgets import (
    Footer,
    Header,
    Input,
    Label,
    LoadingIndicator,
    RichLog,
    Select,
    Static,
    TextArea,
)
from textual.reactive import reactive
from rich.text import Text
from rich.panel import Panel

from .config import AgenticCliConfig
from .contracts import RunCommand, RunReceipt
from .grpc_client import GrpcHubClient
from .service import AgenticCliService
from .memory import InMemoryConversationStore

logger = logging.getLogger(__name__)

_SESSION_FALLBACK_NAMESPACE = uuid.NAMESPACE_URL
_SESSION_FALLBACK_SEED_PREFIX = "meridian://agentic-cli/session"


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
        border: tall $primary 50%;
        padding: 0 1;
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
        background: $surface-darken-1;
        color: $text-muted;
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
            pass  # compose() hasn't run yet

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


class PromptInput(Horizontal):
    """Input area at the bottom of the TUI."""

    DEFAULT_CSS = """
    PromptInput {
        dock: bottom;
        height: 3;
        background: $surface;
        border: tall $primary 50%;
        padding: 0 1;
    }
    PromptInput Input {
        width: 1fr;
    }
    """

    def compose(self) -> ComposeResult:
        yield Input(placeholder="Type your prompt... (Enter to send)", id="prompt-input")


class MeridianTUI(App):
    """Meridian Agentic CLI — interactive terminal interface."""

    CSS = """
    Screen {
        layout: horizontal;
    }
    #loading-indicator {
        display: none;
        height: 1;
    }
    #left-panel {
        width: 20;
        background: $surface-darken-1;
        border: tall $primary 30%;
        padding: 0 1;
    }
    #center-panel {
        width: 1fr;
        layout: vertical;
    }
    #right-panel {
        width: 20;
        background: $surface-darken-1;
        border: tall $primary 30%;
        padding: 0 1;
    }
    #bottom-bar {
        dock: bottom;
        height: 3;
        layout: horizontal;
    }
    #bottom-bar Input {
        width: 1fr;
    }
    #token-label {
        width: auto;
        padding: 0 1;
        content-align: center middle;
    }
    """

    BINDINGS = [
        ("ctrl+c", "quit", "Quit"),
        ("ctrl+k", "clear", "Clear"),
        ("ctrl+e", "focus_center", "Focus Output"),
        ("ctrl+shift+c", "copy_center", "Copy Output"),
    ]

    mouse_support = True

    def __init__(
        self,
        service: AgenticCliService | None = None,
        config: AgenticCliConfig | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self._config = config if config is not None else AgenticCliConfig()
        self._service = service if service is not None else AgenticCliService(config=self._config)
        self._current_session_id: str | None = None
        self._grpc_client = GrpcHubClient()
        self._hub_id: str | None = None
        self._hub_rows: dict[str, str] = {}
        self._hub_poll_seq: int = 0
        self._run_counter: int = 0
        self._copy_fallback_text: str = ""

        # Load .env
        load_dotenv(Path(__file__).parent.parent.parent / ".env")
        
        self._llm_provider = None
        provider_name = os.environ.get("LLM_PROVIDER", "local")
        if provider_name and provider_name != "local":
            try:
                from src.llm.factory import create_provider
                self._llm_provider = create_provider(
                    provider_name,
                    model=os.environ.get("LLM_MODEL"),
                )
            except Exception:
                pass  # Fall back to local backend

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield Vertical(
            RichLog(id="hub-status-log", auto_scroll=True, markup=False, highlight=False, wrap=True),
            id="left-panel",
        )
        yield Vertical(
            MessageLog(id="message-log"),
            LoadingIndicator(id="loading-indicator"),
            id="center-panel",
        )
        yield Vertical(id="right-panel")
        yield Horizontal(
            Input(placeholder="Type your prompt... (Enter to send)", id="prompt-input"),
            Label("tokens: —", id="token-label"),
            id="bottom-bar",
        )
        yield StatusBar(
            session_id="(none)",
            model_name=self._config.llm_model,
            backend_mode=self._config.backend_mode,
            id="status-bar",
        )
        yield Footer()

    async def on_mount(self) -> None:
        """Show welcome message on launch and initialise hub connection."""
        msg_log = self.query_one("#message-log", MessageLog)
        msg_log.add_message(_MessageEntry(
            role="system",
            content="Welcome to Meridian — Agentic CLI",
        ))
        actual_provider = os.environ.get("LLM_PROVIDER", "local")
        actual_model = os.environ.get("LLM_MODEL", self._config.llm_model)
        backend_label = f"LLM:{self._llm_provider.__class__.__name__}" if self._llm_provider else "local (echo)"
        msg_log.add_message(_MessageEntry(
            role="system",
            content=f"Backend: {backend_label} | Model: {actual_model} | Provider: {actual_provider}",
        ))
        msg_log.add_message(_MessageEntry(
            role="system",
            content="Type a prompt and press Enter to begin. Ctrl+K to clear history.",
        ))
        self.query_one("#prompt-input", Input).focus()

        # Attempt to create a hub; log result to the status panel
        await self._init_hub()
        self.set_interval(2.0, self._poll_hub_status)

    async def _init_hub(self) -> None:
        """Create a hub and log the result to the hub-status-log."""
        try:
            self._hub_id = await self._grpc_client.create_hub(
                workspace_id="cli",
                initiator="tui",
            )
            self._hub_rows[self._hub_id] = "connected"
            self._render_hub_rows()
        except Exception as exc:
            logger.warning("Hub creation failed: %s", exc)
            self._hub_rows["offline"] = "unavailable"
            self._render_hub_rows()

    def _build_hub_rows(self) -> list[str]:
        """Build stable, deduplicated hub status rows sorted by hub_id."""
        return [
            f"{hub_id[:8]}… | {state}"
            for hub_id, state in sorted(self._hub_rows.items(), key=lambda item: item[0])
        ]

    def _render_hub_rows(self) -> None:
        """Render hub rows from current contract state in one pass."""
        try:
            status_log = self.query_one("#hub-status-log", RichLog)
        except Exception:
            return
        status_log.clear()
        for row in self._build_hub_rows():
            status_log.write(row)

    def _apply_hub_state(self, hub_id: str, state: str, poll_seq: int) -> bool:
        """Apply a polled hub state when it is not stale."""
        if poll_seq != self._hub_poll_seq:
            return False
        self._hub_rows[hub_id] = state
        self._render_hub_rows()
        return True

    async def _poll_hub_status(self) -> None:
        """Poll hub status and update the status-log panel."""
        if self._hub_id is None:
            return
        self._hub_poll_seq += 1
        poll_seq = self._hub_poll_seq
        hub_id = self._hub_id
        try:
            state = await self._grpc_client.hub_status(hub_id)
            self._apply_hub_state(hub_id=hub_id, state=state, poll_seq=poll_seq)
        except Exception as exc:
            logger.debug("Poll hub status error: %s", exc)

    def _derive_fallback_session_id(self) -> str:
        """Derive deterministic SID from hub/local context."""
        sid_source = f"hub:{self._hub_id}" if self._hub_id else "local"
        sid_seed = f"{_SESSION_FALLBACK_SEED_PREFIX}/{sid_source}"
        return str(uuid.uuid5(_SESSION_FALLBACK_NAMESPACE, sid_seed))

    def _resolve_session_id(self, candidate_session_id: str | None) -> str:
        """Resolve and persist a non-blank session id across turns.

        Blank candidates are ignored so a previously established session id is
        preserved for follow-up prompts.
        """
        candidate = (candidate_session_id or "").strip()
        if candidate:
            self._current_session_id = candidate
        elif self._current_session_id is None:
            self._current_session_id = self._derive_fallback_session_id()
        return self._current_session_id or self._derive_fallback_session_id()

    def on_input_submitted(self, event: Input.Submitted) -> None:
        """Handle Enter key in the prompt input."""
        prompt_text = event.value.strip()
        if not prompt_text:
            return

        # Clear input
        event.input.value = ""

        # Display user message
        msg_log = self.query_one("#message-log", MessageLog)
        msg_log.add_message(_MessageEntry(role="user", content=prompt_text))

        # Run the command via worker (avoids event loop conflict)
        self.run_worker(self._run_command(prompt_text), exclusive=True)

    async def _run_command(self, prompt_text: str) -> None:
        """Execute a run command and display the result."""
        msg_log = self.query_one("#message-log", MessageLog)
        try:
            command = RunCommand(
                prompt=prompt_text,
                session_id=self._current_session_id,
            )

            loading = self.query_one("#loading-indicator", LoadingIndicator)
            if self._hub_id is not None:
                # Streaming path via gRPC AgentStream
                self._run_counter += 1
                run_id = self._run_counter
                accumulated: list[str] = []
                loading.display = True
                try:
                    async for chunk_text in self._grpc_client.stream_prompt(
                        hub_id=self._hub_id,
                        run_id=run_id,
                        prompt=prompt_text,
                    ):
                        accumulated.append(chunk_text)
                finally:
                    loading.display = False
                full_output = "".join(accumulated)
                receipt_session_id = self._current_session_id or ""
                receipt_turn_index = run_id
            elif self._llm_provider is not None:
                # Fallback streaming path via local LLM provider
                accumulated_local: list[str] = []
                loading.display = True
                try:
                    async for chunk_text in self._service.async_stream_with_llm(
                        command,
                        llm_provider=self._llm_provider,
                    ):
                        accumulated_local.append(chunk_text)
                finally:
                    loading.display = False
                full_output = "".join(accumulated_local)
                receipt_session_id = self._service._last_session_id
                receipt_turn_index = self._service._last_turn_index
            else:
                receipt = self._service.run(command)
                full_output = receipt.output
                receipt_session_id = receipt.session_id
                receipt_turn_index = receipt.turn_index

            # Track session ID for subsequent turns
            resolved_session_id = self._resolve_session_id(receipt_session_id)

            # Update status bar
            status_bar = self.query_one("#status-bar", StatusBar)
            status_bar.session_id = resolved_session_id

            # Update token label with approximate token count
            token_label = self.query_one("#token-label", Label)
            token_label.update(f"tokens: ~{len(full_output.split())}")

            # Display assistant response
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
        """Gracefully quit the application."""
        self.exit()

    def action_clear(self) -> None:
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
            self.copy_to_clipboard(selected_text)
            msg_log.add_message(_MessageEntry(role="system", content="Copied selection to clipboard."))
            self._copy_fallback_text = ""
        except Exception:
            self._copy_fallback_text = selected_text
            msg_log.add_message(_MessageEntry(
                role="system",
                content="Clipboard unavailable; selection kept in session for manual copy.",
            ))


def run_tui(
    service: AgenticCliService | None = None,
    config: AgenticCliConfig | None = None,
) -> None:
    """Launch the Meridian TUI application.

    Args:
        service: Optional pre-configured AgenticCliService instance.
        config: Optional AgenticCliConfig instance for defaults.
    """
    app = MeridianTUI(service=service, config=config)
    app.run()
