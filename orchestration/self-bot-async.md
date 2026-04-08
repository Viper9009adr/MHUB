# Self-BOT-Async orchestration

- Prompt path: chat prompt triggers `CreateHub` if no active hub exists.
- Runtime path: `AgentStream` executes ORC -> ARC -> CRT in shared context, then ORC streams final answer in the main chat stream.
- Visibility path: `OrchestratorEvents` fan-outs ordered planning/review frames (`ARC`, `CRT`) to HUB-VIEW.
- Fallback policy: local simulation fallback removed; chat requires hub orchestration flow.
- Deny-default hub meta route: `hub-state`, `report`, and `location` are resolved by `HubStatus` hub state lookup (or `no-hub` explicit stream error) and never forwarded to generic LLM orchestration.
