# Fork Divergence Detection — Technical Plan (Phase 2 Revision 3)

## 1. DDL: fork_scenarios table (pg.py pattern)

Add to `src/storage/pg.py`:

```python
CREATE_FORK_SCENARIOS_TABLE = """
CREATE TABLE IF NOT EXISTS fork_scenarios (
    fork_id TEXT PRIMARY KEY,
    parent_hub_id TEXT NOT NULL REFERENCES hubs(hub_id),
    divergence_type TEXT NOT NULL,
    divergence_reason TEXT NOT NULL,
    hal_agent_id TEXT NOT NULL,
    detected_at BIGINT NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'pending',
    user_decision TEXT,
    decided_at BIGINT,
    new_hub_id TEXT,
    divergence_payload BYTEA,
    CHECK (status IN ('pending', 'approved', 'rejected', 'resolved', 'failed')),
    CHECK (user_decision IS NULL OR user_decision IN ('approve', 'reject'))
);
CREATE INDEX IF NOT EXISTS idx_fork_scenarios_parent ON fork_scenarios(parent_hub_id);
"""
```

## 2. Proto3 definitions (proto/hub.proto additions)

Append to existing `proto/hub.proto`:

```proto
message ForkScenario {
  string fork_id = 1;
  string parent_hub_id = 2;
  string divergence_type = 3;
  string divergence_reason = 4;
  string hal_agent_id = 5;
  int64 detected_at = 6;
  string status = 7;
  bytes divergence_payload = 8;
}

message DetectForkRequest {
  string parent_hub_id = 1;
  string hal_agent_id = 2;
  string divergence_type = 3;
  string divergence_reason = 4;
  bytes divergence_payload = 5;
}

message DetectForkResponse {
  string fork_id = 1;
  string status = 2;
}

message DecideForkRequest {
  string fork_id = 1;
  string decision = 2;
}

message DecideForkResponse {
  bool ok = 1;
  string new_hub_id = 2;
}

service HubService {
  // ... existing RPCs ...
  rpc DetectFork(DetectForkRequest) returns (DetectForkResponse);
  rpc DecideFork(DecideForkRequest) returns (DecideForkResponse);
}
```

## 3. All 11 file paths with function signatures

### New files (5):
1. `src/orc/fork_handler.py` — `ForkHandler.__init__(storage, redis)`, `handle_detection(req)→ForkScenarioRecord`, `process_decision(fork_id, decision)→str|None`, `spawn_new_hub(parent_hub_id)→str`
2. `src/orc/fork_detector.py` — `ForkDetector.__init__(redis)`, `listen_for_divergence()→None`, `forward_to_orc(payload)→None`
3. `src/orc/redis_bridge.py` — `RedisBridge.__init__(url)`, `connect()→None`, `disconnect()→None`, `publish(channel, msg)→None`, `subscribe(channel)→asyncio.Queue`
4. `src/orc/fork_context.py` — `_contexts: dict[str, dict[str, str]] = {}`, `set_context(fork_id, ctx)→None`, `get_context(fork_id)→dict|None`, `clear_context(fork_id)→None`
5. `src/api/routes/fork.py` — `create_fork_router(handler)→APIRouter`

### Modified files (6):
6. `src/storage/pg.py` — add `CREATE_FORK_SCENARIOS_TABLE`, `init_tables()` adds it, new methods: `insert_fork_scenario(record)→None`, `get_fork_scenario(fork_id)→ForkScenarioRecord|None`, `update_fork_decision(fork_id, decision, new_hub_id)→bool`, `get_pending_forks()→list[ForkScenarioRecord]`
7. `src/storage/models.py` — add `ForkScenarioRecord` dataclass alongside `HubRecord`/`CheckpointRecord`
8. `src/hub/service.py` — add `DetectFork(request, context)→DetectForkResponse`, `DecideFork(request, context)→DecideForkResponse`
9. `src/orc/daemon.py` — add Redis init in `start()`, `ForkHandler`/`ForkDetector` lifecycle, pending-fork recovery on startup, `stop()` closes Redis
10. `proto/hub.proto` — add fork messages + 2 RPCs
11. `src/storage/__init__.py` — export `ForkScenarioRecord`

## 4. Step-by-step data flow

1. HAL detects reasoning divergence during hub execution
2. HAL publishes msgpack event to Redis channel `meridian:fork:detect`
3. `ForkDetector` (subscribed) receives event, validates payload
4. `ForkDetector` calls `ForkHandler.handle_detection()` **directly as a Python function** (NOT through gRPC — both are in the same process). The gRPC `DetectFork` RPC is reserved for external HAL agents in separate processes.
5. `ForkHandler.handle_detection()` stores record, sets shared context via `fork_context.set_context(fork_id, {...})`
6. `ForkHandler` serializes payload→msgpack, stores in `fork_scenarios.divergence_payload` (BYTEA) with 1-byte format marker prepended: `0x01=msgpack, 0x02=json`. On read, check first byte to determine deserialization method.
7. ORC publishes to Redis `meridian:fork:pending` for user notification (with `@with_retry` wrapper)
8. User reviews via `GET /api/v1/forks/{fork_id}`, POSTs decision to `/api/v1/forks/{fork_id}/decide`
9. `ForkHandler.process_decision()` updates status; if "approve" → `spawn_new_hub()`
10. `spawn_new_hub()` calls gRPC `CreateHub` with parent context from shared dict. **On failure**: update status to `'failed'`, publish to `meridian:fork:resolved` with error details.
11. ORC publishes to Redis `meridian:fork:resolved`; HAL receives, continues with new hub or aborts
12. `fork_context.clear_context(fork_id)` called after resolution

## 5. Shared context dict pattern (replaces ContextVar)

```python
# src/orc/fork_context.py
_contexts: dict[str, dict[str, str]] = {}

def set_context(fork_id: str, ctx: dict[str, str]) -> None:
    _contexts[fork_id] = ctx

def get_context(fork_id: str) -> dict[str, str] | None:
    return _contexts.get(fork_id)

def clear_context(fork_id: str) -> None:
    _contexts.pop(fork_id, None)
```

This avoids async task-scoping issues entirely. `fork_id` is threaded through all calls as an explicit parameter.

## 6. Redis integration

- **Library**: `redis.asyncio` (aioredis)
- **Channels**:
  - `meridian:fork:detect` — HAL→ORC divergence events (pub)
  - `meridian:fork:pending` — ORC→user notification (pub)
  - `meridian:fork:resolved` — ORC→HAL resolution (pub)
- **Connection lifecycle**: Created in `OrcDaemon.start()` via `RedisBridge.connect()`, closed in `OrcDaemon.stop()` via `disconnect()`. Single connection pool shared.
- **Pub/Sub roles**: HAL publishes to `meridian:fork:detect`; ForkDetector subscribes; ORC publishes to pending/resolved; User API subscribes to pending (via SSE).
- **Retry**: All `publish()` calls wrapped with `@with_retry` decorator (same pattern as `src/storage/retry.py`), retrying on `redis.ConnectionError` and `redis.TimeoutError`.

## 7. BYTEA serialization

- **Format**: msgpack (`msgpack` package)
- **Format marker**: Prepend 1-byte marker to serialized payload: `0x01=msgpack`, `0x02=json`. On read, check first byte to determine deserialization method.
- **Mitigation**: Wrap in try/except with JSON fallback. Validate payload < 1MB.

## 8. Sync/async strategy

gRPC servicer methods are sync (existing pattern). New RPCs `DetectFork`/`DecideFork` follow same pattern: sync method receives request, uses `asyncio.get_event_loop()` bridge (same as existing `CreateHub`). If loop running: `asyncio.ensure_future()` for fire-and-forget. If no loop: `loop.run_until_complete()` for blocking. Falls back to in-memory dict if storage unavailable.

**In-process calls**: `ForkDetector` → `ForkHandler.handle_detection()` is a direct Python call (no gRPC). Only external HAL agents use the gRPC `DetectFork` RPC.

## 9. Migration strategy

`StorageBackend.init_tables()` extended with `await conn.execute(CREATE_FORK_SCENARIOS_TABLE)`. `CREATE TABLE IF NOT EXISTS` is idempotent. Existing hubs/checkpoints unaffected. No ALTER TABLE needed.

**New dependencies in requirements.txt:**
- `redis>=5.0.0` (for redis.asyncio)
- `msgpack>=1.0.0`

## 10. HAL→ORC interface

- **Signal**: HAL publishes to Redis `meridian:fork:detect`
- **Payload** (msgpack): `{parent_hub_id, hal_agent_id, divergence_type, divergence_reason, evidence, detected_at}`
- **Contract**: HAL must provide all fields; ORC validates and rejects incomplete payloads with gRPC INVALID_ARGUMENT.

## 11. Daemon startup recovery

In `OrcDaemon.start()`, after storage and Redis are initialized:
1. Query `SELECT * FROM fork_scenarios WHERE status = 'pending'`
2. For each pending fork, re-publish to `meridian:fork:pending` so user notifications are not lost across restarts.

## 12. Test strategy

- `tests/test_fork_models.py` — ForkScenarioRecord serialization/deserialization
- `tests/test_fork_handler.py` — detection, decision, spawn flows, spawn failure handling
- `tests/test_fork_detector.py` — Redis pub/sub integration, direct call to ForkHandler
- `tests/test_fork_api.py` — FastAPI route handlers
- `tests/test_fork_storage.py` — pg.py fork_scenarios CRUD, CHECK constraint validation
- `tests/test_fork_context.py` — shared dict context isolation across forks
- `tests/test_fork_recovery.py` — daemon restart pending-fork recovery
- Scenarios: happy path, rejection, concurrent forks, Redis disconnect, BYTEA overflow, spawn failure
