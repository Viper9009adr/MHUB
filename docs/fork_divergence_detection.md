# Fork Divergence Detection

## Overview

Fork divergence detection allows a HAL agent to signal that its reasoning has diverged from the parent hub's expected path. When a divergence is detected the system creates a persistent record of the event, notifies the user via Redis, and waits for an explicit decision. If the user approves, a new hub is spawned under a fresh UUID. If the user rejects, the scenario is marked rejected and no hub is created.

The feature exists because undetected reasoning divergence silently corrupts multi-agent collaboration. Making divergence explicit and requiring a human decision before spawning a child hub preserves auditability and control.

---

## Architecture

```
HAL agent
   │
   │  msgpack-serialised DivergenceEvent
   │  published to Redis channel: meridian:fork:detect
   ▼
RedisBridge.publish()
   │
   ▼
ForkDetector._listen_loop()         ← asyncio.Task, cancelled on shutdown
   │  subscribes to meridian:fork:detect
   │  deserialises DivergenceEvent.from_msgpack()
   │  validates parent_hub_id + hal_agent_id present
   ▼
ForkHandler.handle_detection()
   │  generates fork_id (UUID4)
   │  serialises evidence → BYTEA (0x01 prefix + raw bytes)
   │  inserts ForkScenarioRecord into PostgreSQL (status="pending")
   │  sets in-memory fork context
   │  publishes to Redis channel: meridian:fork:pending
   ▼
PostgreSQL fork_scenarios table
   │
   ▼  (user polls REST or receives Redis notification)
GET /api/v1/forks/{fork_id}
POST /api/v1/forks/{fork_id}/decide   {"decision": "approve" | "reject"}
   │
   ▼
ForkHandler.process_decision()
   │  approve → spawn_new_hub() → new hub UUID returned
   │  reject  → status set to "rejected"
   │  updates fork_scenarios (status, user_decision, decided_at, new_hub_id)
   │  publishes to Redis channel: meridian:fork:resolved
   │  clears in-memory fork context
   ▼
spawn_new_hub()
   │  reads parent HubRecord from storage
   │  constructs HubRecord (inherits workspace_id + initiator; state="active")
   │  calls storage.insert_hub(new_record)
   │  returns new_hub_id
   ▼
PostgreSQL fork_scenarios table (final state)
PostgreSQL hubs table (new child hub row)
```

The same `ForkHandler` is reachable via two entry points:

1. **Redis path** — `ForkDetector` subscribes and forwards events in-process.
2. **gRPC path** — `HubService.DetectFork` and `HubService.DecideFork` RPCs delegate directly to `ForkHandler`.

Both paths produce identical `ForkScenarioRecord` rows.

---

## Components

### `src/orc/fork_context.py`

Process-wide in-memory dict keyed by `fork_id`. Stores a small string-to-string map for each live fork scenario so that `ForkHandler.spawn_new_hub()` can look up `parent_hub_id` and `hal_agent_id` without a round-trip to storage.

| Function | Purpose |
|---|---|
| `set_context(fork_id, ctx)` | Store context dict for a fork. |
| `get_context(fork_id)` | Retrieve context dict; returns `None` if absent. |
| `clear_context(fork_id)` | Remove context after decision is processed. |

Context is set by `handle_detection()` and cleared by `process_decision()` when the fork is resolved (approved or rejected).

---

### `src/orc/fork_handler.py`

Core orchestration module. Contains two public objects:

#### `DivergenceEvent` (dataclass)

Typed container for a divergence detection event. Used by both the Redis and gRPC paths.

| Field | Type | Description |
|---|---|---|
| `parent_hub_id` | `str` | Hub that spawned the diverging HAL agent. |
| `hal_agent_id` | `str` | Identifier of the reporting HAL agent. |
| `divergence_type` | `str` | Category of divergence (e.g. `"reasoning"`, `"goal"`). |
| `divergence_reason` | `str` | Human-readable explanation. |
| `evidence` | `bytes` | Raw evidence payload; empty bytes if none. |
| `detected_at` | `int` | Unix timestamp; defaults to `0` (set to `time.time()` in handler). |

Serialisation methods: `to_msgpack()` / `from_msgpack()`, `to_dict()` / `from_dict()`.

#### `ForkHandler` (class)

Constructed with a storage backend and an optional `RedisBridge`. All methods are `async`.

| Method | Description |
|---|---|
| `handle_detection(event)` | Validates, persists, sets context, publishes to `meridian:fork:pending`. Returns the `ForkScenarioRecord`. |
| `process_decision(fork_id, decision)` | Accepts `"approve"` or `"reject"`. On approve calls `spawn_new_hub()`. Persists decision, publishes to `meridian:fork:resolved`, clears context. Returns `new_hub_id` or `None`. |
| `spawn_new_hub(fork_id)` | Reads context for `parent_hub_id`, verifies the parent hub exists via storage, generates a new UUID, constructs a `HubRecord` inheriting `workspace_id` and `initiator` from the parent (with `state="active"`), and calls `storage.insert_hub()` to persist the new hub. Returns `new_hub_id` or `None` on failure. |
| `get_pending_forks()` | Returns all `ForkScenarioRecord` entries with status `pending`. Delegates to `storage.get_pending_forks()`. |
| `get_fork_scenario(fork_id)` | Delegates to `storage.get_fork_scenario()`. |
| `publish_pending(fork_id, record)` | Publishes to `meridian:fork:pending` (decorated with `@with_redis_retry()`). |
| `_serialize_payload(evidence)` | Prepends `0x01` format marker; truncates at 1 MB. Returns `None` for empty evidence. |
| `deserialize_payload(raw)` | Static method. Reads format marker byte and dispatches to msgpack (`0x01`) or JSON (`0x02`) deserialiser. |

**Redis channel names** (module-level constants):

| Constant | Channel |
|---|---|
| `CHANNEL_DETECT` | `meridian:fork:detect` |
| `CHANNEL_PENDING` | `meridian:fork:pending` |
| `CHANNEL_RESOLVED` | `meridian:fork:resolved` |

---

### `src/orc/fork_detector.py`

Subscribes to `meridian:fork:detect` via `RedisBridge` and dispatches raw messages to `ForkHandler`.

| Method | Description |
|---|---|
| `listen_for_divergence()` | Creates an `asyncio.Task` running `_listen_loop()`. Returns the task — cancel it to stop. |
| `stop()` | Cancels the listener task and awaits it. |
| `handle_divergence_event(payload)` | Public entry point for non-Redis callers (e.g. gRPC). Accepts raw msgpack bytes and delegates to `_handle_raw_event()`. |
| `_handle_raw_event(raw)` | Deserialises bytes to `DivergenceEvent`, validates required fields, then calls `ForkHandler.handle_detection()`. |

Events missing `parent_hub_id` or `hal_agent_id` are logged as warnings and dropped.

---

### `src/orc/redis_bridge.py`

Async Redis pub/sub layer built on `redis.asyncio`.

#### `with_redis_retry(config=None)` (decorator)

Wraps any `async` function with retry logic for `redis.ConnectionError` and `redis.TimeoutError`. Default configuration: 3 attempts, base delay 0.1 s, max delay 5 s, exponential back-off with jitter.

#### `RedisBridge` (class)

Manages a single `ConnectionPool` and `Redis` client. One `PubSub` object is tracked per channel.

| Method | Description |
|---|---|
| `connect()` | Creates the connection pool and client. |
| `disconnect()` | Closes all PubSub handles, then the client and pool. |
| `publish(channel, msg)` | Publishes bytes to a channel (decorated with `@with_redis_retry()`). |
| `subscribe(channel)` | Returns an `asyncio.Queue` fed by an internal reader task. Each `"message"` frame's `data` field is enqueued. |
| `unsubscribe(channel)` | Unsubscribes and closes the PubSub handle. |
| `client` | Property returning the underlying `aioredis.Redis` instance or `None`. |

---

### `src/api/routes/fork.py`

FastAPI router providing the user-facing REST interface.

**Factory function:** `create_fork_router(handler: ForkHandler) → APIRouter`

Injects a `ForkHandler` instance and registers three routes on the module-level `APIRouter` at prefix `/api/v1/forks`. Call this function exactly once per process — calling it multiple times accumulates duplicate route registrations.

See [REST API](#rest-api) below for endpoint details.

---

### `src/storage/models.py` — `ForkScenarioRecord`

Dataclass representing one row in `fork_scenarios`.

| Field | Type | Description |
|---|---|---|
| `fork_id` | `str` | Primary key (UUID4). |
| `parent_hub_id` | `str` | Foreign key to `hubs.hub_id`. |
| `divergence_type` | `str` | Category of divergence. |
| `divergence_reason` | `str` | Human-readable explanation. |
| `hal_agent_id` | `str` | Reporting HAL agent identifier. |
| `detected_at` | `int` | Unix timestamp of detection. |
| `status` | `str` | One of `pending`, `approved`, `rejected`, `resolved`, `failed`. |
| `user_decision` | `str \| None` | `"approve"` or `"reject"`, set after decision. |
| `decided_at` | `int \| None` | Unix timestamp of decision. |
| `new_hub_id` | `str \| None` | UUID of the spawned hub; `None` if not approved. |
| `divergence_payload` | `bytes \| None` | Serialised evidence; see [Payload Format](#payload-format). |

---

## gRPC Interface

Defined in `proto/hub.proto` (package `meridian.hub.v1`). The two fork-specific RPCs are part of the existing `HubService`.

### `DetectFork`

```protobuf
rpc DetectFork(DetectForkRequest) returns (DetectForkResponse);

message DetectForkRequest {
  string parent_hub_id     = 1;
  string hal_agent_id      = 2;
  string divergence_type   = 3;
  string divergence_reason = 4;
  bytes  divergence_payload = 5;
}

message DetectForkResponse {
  string fork_id = 1;
  string status  = 2;   // "pending" on success, "error" on failure
}
```

`HubService.DetectFork` builds a `DivergenceEvent` from the request fields and delegates to `ForkHandler.handle_detection()`. Returns the assigned `fork_id` and initial `status`.

### `DecideFork`

```protobuf
rpc DecideFork(DecideForkRequest) returns (DecideForkResponse);

message DecideForkRequest {
  string fork_id  = 1;
  string decision = 2;   // "approve" or "reject"
}

message DecideForkResponse {
  bool   ok         = 1;
  string new_hub_id = 2;   // populated when decision == "approve" and spawn succeeded
}
```

`HubService.DecideFork` validates that `decision` is `"approve"` or `"reject"` (returns `INVALID_ARGUMENT` otherwise) and delegates to `ForkHandler.process_decision()`.

On success, `ok=true` for both decisions. For `"approve"`, `new_hub_id` is populated when spawn succeeds. For `"reject"`, `new_hub_id` is the protobuf default empty string and storage keeps `new_hub_id=None`.

### Error codes

| gRPC status | Condition |
|---|---|
| `UNAVAILABLE` | `ForkHandler` was not injected into `HubService`. |
| `INVALID_ARGUMENT` | `decision` is not `"approve"` or `"reject"`. |
| `INTERNAL` | Unexpected exception inside the handler. |

---

## REST API

### GET /api/v1/forks

List all pending fork scenarios.

**No path or query parameters.**

**Response (200) — two pending forks:**
```json
[
  {
    "fork_id": "3fa85f64-...",
    "parent_hub_id": "hub-parent",
    "divergence_type": "reasoning",
    "divergence_reason": "Goal objective shifted without authorisation.",
    "hal_agent_id": "hal-42",
    "detected_at": 1743547200,
    "status": "pending",
    "user_decision": null,
    "decided_at": null,
    "new_hub_id": null
  },
  {
    "fork_id": "9b2e1a77-...",
    "parent_hub_id": "hub-parent",
    "divergence_type": "state_mismatch",
    "divergence_reason": "Counter drifted beyond threshold.",
    "hal_agent_id": "hal-99",
    "detected_at": 1743547800,
    "status": "pending",
    "user_decision": null,
    "decided_at": null,
    "new_hub_id": null
  }
]
```

**Response (200) — no pending forks:**
```json
[]
```

Each item in the array is identical in shape to the `GET /{fork_id}` response. Only scenarios whose `status` is `pending` are included; decided or resolved forks are intentionally excluded by design.

---

### GET /api/v1/forks/{fork_id}

Retrieve details of a fork scenario.

**Path parameter:** `fork_id` — UUID of the fork scenario.

**Response (200):**
```json
{
  "fork_id": "3fa85f64-...",
  "parent_hub_id": "...",
  "divergence_type": "reasoning",
  "divergence_reason": "Goal objective shifted without authorisation.",
  "hal_agent_id": "hal-42",
  "detected_at": 1743547200,
  "status": "pending",
  "user_decision": null,
  "decided_at": null,
  "new_hub_id": null
}
```

**Error responses:** 404 — fork not found.

---

### POST /api/v1/forks/{fork_id}/decide

Submit a user decision for a pending fork scenario.

**Path parameter:** `fork_id` — UUID of the fork scenario.

**Request body:**
```json
{ "decision": "approve" }
```
`decision` must be `"approve"` or `"reject"`.

**Response (200) — approve with successful spawn:**
```json
{
  "fork_id": "3fa85f64-...",
  "decision": "approve",
  "new_hub_id": "7c9e6679-..."
}
```

**Response (200) — reject:**
```json
{
  "fork_id": "3fa85f64-...",
  "decision": "reject",
  "new_hub_id": null
}
```

**Error responses:** 400 — `decision` is not `"approve"` or `"reject"`.

---

## Storage Schema

Table `fork_scenarios` — created automatically by `StorageBackend.init_tables()` via `CREATE TABLE IF NOT EXISTS`.

```sql
CREATE TABLE IF NOT EXISTS fork_scenarios (
    fork_id          TEXT    PRIMARY KEY,
    parent_hub_id    TEXT    NOT NULL REFERENCES hubs(hub_id),
    divergence_type  TEXT    NOT NULL,
    divergence_reason TEXT   NOT NULL,
    hal_agent_id     TEXT    NOT NULL,
    detected_at      BIGINT  NOT NULL DEFAULT 0,
    status           TEXT    NOT NULL DEFAULT 'pending',
    user_decision    TEXT,
    decided_at       BIGINT,
    new_hub_id       TEXT,
    divergence_payload BYTEA,
    CHECK (status IN ('pending', 'approved', 'rejected', 'resolved', 'failed')),
    CHECK (user_decision IS NULL OR user_decision IN ('approve', 'reject'))
);
CREATE INDEX IF NOT EXISTS idx_fork_scenarios_parent ON fork_scenarios(parent_hub_id);
```

`parent_hub_id` has a foreign-key constraint to `hubs.hub_id` and a secondary index for efficient queries per parent hub.

`StorageBackend` fork methods:

| Method | Description |
|---|---|
| `insert_fork_scenario(record)` | `INSERT … ON CONFLICT DO NOTHING`. |
| `get_fork_scenario(fork_id)` | Fetch one row by primary key; returns `None` if absent. |
| `update_fork_decision(fork_id, decision, new_hub_id, status, decided_at)` | `UPDATE … SET` four columns. Returns `True` if a row was modified. |
| `get_pending_forks()` | Fetch all rows where `status = 'pending'`. |

All methods are decorated with `@with_retry()` (3 attempts, exponential back-off, retries on `ConnectionError`, `OSError`, and `asyncpg.PostgresError`).

`NullStorageBackend.update_fork_decision()` mirrors the same decision persistence shape in memory by updating `user_decision`, `new_hub_id`, `status`, and `decided_at` on the stored `ForkScenarioRecord`. This keeps unit/integration behavior aligned with PostgreSQL for decision flows.

---

## Payload Format

Evidence is stored in `fork_scenarios.divergence_payload` (PostgreSQL `BYTEA`) with a 1-byte format marker prefix:

| Marker byte | Format |
|---|---|
| `0x01` | msgpack |
| `0x02` | JSON |

`ForkHandler._serialize_payload(evidence)` always writes `0x01` (msgpack marker) followed by the raw evidence bytes — no double-encoding. The caller passes raw bytes; the marker is added by the serialiser.

Payloads exceeding 1 MB (`MAX_PAYLOAD_SIZE = 1_048_576`) are silently truncated before the marker is prepended. Empty evidence results in `None` being stored (no BYTEA row).

`ForkHandler.deserialize_payload(raw)` is a static method that reads the first byte, dispatches to the appropriate deserialiser, and returns the reconstructed value. Unknown marker bytes are logged as warnings and return `None`.

---

## Async boundaries in `HubService`

`HubService` contains both synchronous and asynchronous RPC methods:

- Synchronous RPCs (for hub/checkpoint CRUD) may call async storage methods through `_schedule_coroutine()` when a loop is already running.
- `DetectFork` and `DecideFork` are native `async def` RPCs and directly `await` `ForkHandler.handle_detection()` and `ForkHandler.process_decision()`.

This means the fork path no longer uses an `asyncio.ensure_future` + `threading.Event` blocking bridge. Fork detection and decision handling now execute as direct awaited coroutines in the gRPC async flow.

---

## Security caveat

Current fork endpoints and RPCs do not enforce authentication or authorization. Any caller that can reach the REST/gRPC surface can submit `DetectFork` / `DecideFork` or call fork REST routes. Policy enforcement is not implemented in this slice.

---

## Tests

Fork coverage spans both unit and integration suites. Latest TST validation reports `187 passed` with warnings (no failures).

| File | What it covers |
|---|---|
| `test_fork_context.py` | `set_context`, `get_context`, `clear_context` — isolation between fork IDs, overwrite, missing key. |
| `test_divergence_event.py` | `DivergenceEvent` field defaults, `to_dict`/`from_dict` round-trip, `to_msgpack`/`from_msgpack` round-trip. |
| `test_fork_handler_serialize.py` | `ForkHandler._serialize_payload` and `deserialize_payload` — format marker, 1 MB truncation, empty evidence, unknown marker, msgpack and JSON paths. |
| `test_fork_handler_detection.py` | `ForkHandler.handle_detection` and `process_decision` — persistence via `NullStorageBackend`, context lifecycle, approve/reject branches, spawn failure. Includes `test_spawn_new_hub_creates_hub_in_storage`, which verifies that `spawn_new_hub()` inserts a retrievable `HubRecord` inheriting `workspace_id` and `initiator` from the parent hub. |
| `test_fork_handler_public.py` | `ForkHandler.get_fork_scenario` and `publish_pending` public API — mock storage, mock Redis. |
| `test_fork_detector.py` | `ForkDetector._handle_raw_event` and `handle_divergence_event` — valid dispatch, malformed msgpack, missing required fields. |
| `test_fork_routes.py` | FastAPI router (`create_fork_router`) — GET 200, GET 404, POST approve 200, POST reject 200, POST 400 invalid decision. Also covers the two new list-pending tests: `test_list_pending_forks_returns_200_empty_list` (empty array on no pending forks) and `test_list_pending_forks_returns_pending_records` (correct serialisation and count of multiple records). Uses a shared `TestClient` instance to avoid duplicate route registration. |
| `test_grpc_fork.py` | In-process gRPC fork flow: `DetectFork` + `DecideFork` approve/reject branches, child-hub creation for approve, and persisted decision fields (`status`, `user_decision`, `new_hub_id`, `decided_at`). |
