# 10 — Tracing, activity logs, and observability

## 1. What it is

Raft keeps two separate observability channels. **Activity** is product data for humans: each agent reports a small status (`online | thinking | working | error | offline`) plus a stream of "trajectory entries" (thinking, tool calls, text, status). The server keeps this in memory, mirrors it to Redis, writes it to a Postgres table, and pushes it over Socket.IO. **Tracing** is engineering data: OpenTelemetry-shaped spans produced by the server, daemon, Computer and web client. It never goes to Postgres. It goes to OTLP/ScopeDB ("Telescope") and to R2 object storage, uploaded by a separate trace-upload service. The two channels share join keys (`launchId`, `clientSeq`, `producerFactId`, `serverSeq`), so a human-visible activity row can be matched to the trace span that produced it. A third, smaller pipeline carries per-turn "agent o11y" events and Prometheus metrics. A Cloudflare Worker app, separate from the core server, handles feature flag rollout.

## 2. Key components

| Name | File path(s) | Responsibility |
|---|---|---|
| Shared tracing contract | `packages/shared/src/tracing/index.ts` | `Tracer`, `TraceSink`, `CompletedTraceSpan`, `BasicTracer`, W3C `traceparent` parse/format, scope tracers, attr-contract tracers, `noopTracer` |
| Trace family registry | `packages/shared/src/tracing/traceFamilyRegistry.ts` | Each named span family must declare who consumes it, a privacy tier, and which dimensions are filterable |
| V2 event-row schema | `packages/shared/src/tracing/eventRows.ts` | Flat typed row (`row_kind: event\|span_fact`) for `raft.trace_events_v2`. The column list and ingest SQL are generated in code and carry a SHA-256 schema fingerprint |
| Server tracer factory | `packages/server/src/tracing/serverTracer.ts` | Builds `BasicTracer` → Fanout(OTLP sink, ScopeDB event-row sink) from env vars. Returns `noopTracer` if neither is configured |
| OTLP HTTP sink | `packages/server/src/tracing/otlpHttpTraceSink.ts` | Bounded in-memory queue (4096). When full it drops the oldest item, flushes in batches over OTLP/HTTP JSON, and never blocks the caller |
| ScopeDB event-row sink | `packages/server/src/tracing/scopeDbTraceEventSink.ts` | Queues `event` and `span_fact` rows, flushes with jitter, checks the live table schema before writing, and reports drops to Prometheus |
| Request span middleware | `packages/server/src/middleware/requestObservability.ts` | One `server.http.request` span per request. Continues an incoming `traceparent`. Records HTTP metrics |
| Ambient span context | `packages/server/src/tracing/semanticTrace.ts` | `AsyncLocalStorage` carries the active span and tracer. Provides `withTraceChildSpan`, `addTraceEvent`, `tracePhase`, and a DB query tracer |
| Decision events | `packages/server/src/tracing/decisionTrace.ts` | Typed "decision" events with closed action/reason sets. Rejects attribute keys that look unsafe |
| Node trace client | `packages/trace-client/src/traceClient.ts` | `createTraceClient({source, sinks})`: MultiSink fan-out, and a `source` attr (`daemon`/`computer.cli`/`computer.menu-bar`) that callers cannot override |
| Local rotating sink | `packages/trace-client/src/localTraceSink.ts` | Appends spans as JSONL to `<machineDir>/traces/daemon-trace-*.jsonl`. Rotates at 5 MB or 5 min and keeps 8 files. Sanitizes attrs |
| Trace jitter | `packages/trace-client/src/traceJitter.ts` | Derives upload and rotation offsets for each machine from a hash of its lockId |
| Daemon uploader | `packages/daemon/src/traceBundleUpload.ts`, `packages/daemon/src/directUploadCapability.ts` | Every 5 min (+jitter) it gzips each closed JSONL file, gets a signed attestation from the server, uploads to the worker, and writes an `.uploaded.json` sidecar |
| Attestation signer | `packages/server/src/routes/internal.ts:855-990, 1624+` | `POST /internal/machine/scope-attestation` with scope `daemon-trace-bundle:create`. The server chooses `uploadId`, `objectKey`, `maxBytes` and deployment env |
| Trace upload service | `packages/trace-upload-worker/src/index.ts` (CF Worker), `node.ts`/`nodeStorage.ts` (Node/ECS + S3→R2) | Checks the attestation and hands out a PUT URL. On PUT it checks size and sha256, writes to R2, writes a ledger, then ingests to OTLP in the background and runs the V2 projector. Also takes `/api/web-traces` and feedback reports |
| V2 projector | `packages/trace-upload-worker/src/traceEventProjector.ts`, `traceAttributeAliases.ts` | Shadow write of uploaded spans into the typed `trace_events_v2` table |
| Daemon activity producer | `packages/daemon/src/agentActivityProducer.ts`, `apmStateMachine.ts` | Builds `agent:activity` messages and drops unknown or "non-fact" detail kinds before they are sent |
| Activity ingest/broadcast | `packages/server/src/services/agentOrchestrator.ts` (`case "agent:activity"` ~6669; `broadcastActivity` ~11948; `emitActivity` ~12447) | Validates and guards by launch, arbitrates the snapshot, mirrors to Redis, persists, then emits over Socket.IO |
| Activity log service | `packages/server/src/services/agentActivityLogService.ts` | Append to and read from `agent_activity_events` in Postgres |
| Activity-log REST | `packages/server/src/routes/agents.ts:1923` | `GET /agents/:id/activity-log?limit` (max 200). Only the creator or holders of `editAgents` can call it |
| External activity ingest | `packages/server/src/routes/internalAgentApi.ts:2986`, `agentOrchestrator.recordExternalAgentActivity` (~2910) | Plugin hook events (PreToolUse/PostToolUse…) sent by external agents |
| Web activity reducer | `packages/web/src/store/events/agentActivityEvents.ts`, `packages/web/src/utils/activityTrajectoryRecovery.ts` | Orders updates by `serverSeq`. Reloads the REST log after a reconnect or after a frame that arrives without entries |
| Agent o11y (turn/step) | `packages/server/src/routes/internalComputer.ts:921-1003`, `services/agentO11yValidation.ts`, `services/agentO11yScopeDbWriter.ts`, `packages/daemon/src/agentO11yClient.ts` | Computer-authenticated batch ingest of `turn/step/observation` events into ScopeDB, with payload tiers T0/T1/T2 |
| Prometheus metrics | `packages/server/src/metrics.ts` | Separate registry, `slock_*` metrics, private `/metrics` on port 9091 |
| Feature flags | `packages/server/src/services/featureFlagService.ts`, `featureFlagRolloutWriterService.ts`, `packages/shared/src/featureFlagRolloutGuardrail.ts`, `apps/feature-flag-admin/` | Flag evaluation inside core. Operators mutate flags only through a separate Worker that uses a least-privilege PG role and writes an audit row |

## 3. Data model

**Postgres (drizzle, `packages/server/src/db/schema.ts`)**

- `agent_activity_events` (line 5023): `id uuid`, `agent_id → agents.id (cascade)`, `activity` enum `online|thinking|working|error|offline`, `detail text`, `entries json TrajectoryEntry[]`, `dedupe_key text NULL`, `created_at`. Indexes: `(agent_id, created_at)`, and a partial unique index `(agent_id, dedupe_key) WHERE dedupe_key IS NOT NULL` (lines 5050-5051). The comment at 5029-5046 says `dedupe_key` is a *semantic* key (for example "this restart window"). It is never a trace or request id.
- `product_events` comment (≈5478-5490) states a boundary: runtime, lifecycle and tracing events belong in `agent_activity_events` or OTLP, not in the product analytics table.
- `feature_flags` (493): `key PK`, `enabled`, `kill_switch`, `randomization_unit user|server`, `default_enabled`, `default_variant`, `salt`.
- `feature_flag_rules` (529): `flag_key`, `stage ∈ user|platform|server|audience|lab|plan|percentage`, `priority`, `decision allow|deny`, `values jsonb`, `percentage_basis_points 0..10000`, `variant`. CHECK constraints enforce the shape of each stage.
- `feature_flag_audiences`, `feature_flag_audience_members` (508, 517).
- `feature_flag_config_versions` (559): a single `global` row with a monotonic `version` and `last_audit_event_id`.
- `feature_flag_rollout_audit_events` (576): `actor_type human|agent|system`, `request_id`, `reason`, `config_version_before/after` (unique on after), `authorization narrowing|guardrail_passed`, `operation jsonb`, `receipt_id`, `before_snapshot`, `after_snapshot`.

**Shared types (`packages/shared/src/index.ts`)**

- `AGENT_ACTIVITIES` (1309). `AGENT_ACTIVITY_DETAIL_KINDS` (1315-1367) has about 45 closed values, for example `thinking_started`, `tool_started`, `tool_end`, `runtime_crashed`, `machine_disconnected`, `subagent_activity`, `runtime_progress`.
- `TrajectoryEntry` (1480): `thinking | tool_start{toolName, toolInput} | text | slock_action{title,text} | system | compaction_started | compaction_finished | status{activity, detail, detailKind}`. Any entry may carry `producerFactId` (lineage join key) and `subagent{parentToolUseId, subagentType, taskId, phase}`.
- `ExternalAgentActivityEvent` / `ExternalAgentActivityIngestRequest` (schemas `raft-activity.v1`, `raft-agent-activity-ingest.v1`).

**Trace types (`packages/shared/src/tracing/index.ts`)**

- `CompletedTraceSpan {context{traceId(32hex), spanId(16hex), parentSpanId, traceFlags}, name, surface server|daemon|web|computer, kind, status unset|ok|error|cancelled, start/end/duration, attrs, events[]}`.
- `TraceSink.record(span)`, plus optional `recordEvent` (called as each event is emitted) and `recordSpanFact` (called when the span ends).
- Local JSONL record (`localTraceSink.ts:130`): `{type:"span", schema_version:1, trace_id, span_id, parent_span_id, name, surface, kind, status, start_time, end_time, duration_ms, attrs, events}`.
- `TraceEventRow` (`eventRows.ts:16`): about 59+ flat columns. Includes resource identity (service/deploy/ECS), span ids, promoted dimensions (`server_id, machine_id, agent_id, launch_id, route_pattern, query_name, sqlstate, previous_activity, next_activity, …`). Table name `raft.trace_events_v2`.

**Object storage (R2)**

- `trace-bundles/<serverId>/<machineId>/<uploadId>.jsonl.gz` (key chosen by the server, `internal.ts:970`).
- `trace-ledgers/<serverId>/<machineId>/<uploadId>.json`: `r2_status`, `scopedb_status pending|success|failed|skipped`, `v2_projector_status`, `spans_ingested`, `span_key_identity` (`index.ts:861-914`).

**ScopeDB agent-o11y row** (`agentO11yScopeDbWriter.ts:113`): `server_id, computer_id, machine_id` (taken from auth), plus `event_kind turn|step|observation, agent_id, turn_id, occurred_at, payload_mode hash|summary|full, turn_trigger_hash, step_input_hash, fields`.

**Redis**: `slock:agent:<id>:activity` hash `{activity, detail, detailKind, updatedAt, observedAtMs}` with a 600 s TTL (`packages/server/src/replicaRouter.ts:1152-1171`; the `observedAtMs` field is removed when absent).

## 4. Flows

### Flow A: Agent does work → human sees it live

1. **Runtime → Daemon.** The daemon's agent process manager parses runtime stream events. `trajectoryActivityProjection` maps `thinking` → (`thinking`, `thinking_started`) and `text` → (`working`, `model_response_started`) (`daemon/src/agentActivityProducer.ts:70`).
2. **Daemon (local filter).** `buildDaemonActivityMessage` drops a frame whose detailKind is not in the closed set, or is a "non-fact" kind (`none, daemon_activity, external_activity, slock_action, other`). Otherwise it builds `{type:"agent:activity", agentId, detail, detailKind, entries, launchId, daemonInstanceId, probeId, clientSeq, producerFactId, observedAtMs, isHeartbeat, runtimeError}` (`agentActivityProducer.ts:88-135`). **Note: the current daemon message carries no `activity` field.** The daemon sends only the fact (`detailKind`); the server decides the user-visible activity (step 4). Only old daemons without `detailKind` still send a declared activity. Drops are traced with `daemonActivityDropTraceAttrs`.
3. **Daemon → Server (machine websocket).** The message goes up the daemon connection.
4. **Server ingest.** `AgentOrchestrator` `case "agent:activity"` (`agentOrchestrator.ts:6669`) opens span `server.agent.activity.ingest` and checks, in order: the agent belongs to this machine and server (`validateMachineAgentMessageWithReason`); the lifecycle guard (`getLifecycleEventAcceptanceAction`), which drops `stale_launch_guard` / `legacy_lifecycle_event` frames from old launches; and whether the detailKind is known. Each drop becomes a span event `activity.ingest.dropped{reason}`. The full accept pipeline, in order (`agentOrchestrator.ts:6669-7070`):
   1. machine/server ownership check → drop reason from `validateMachineAgentMessageWithReason`
   2. launch guard → `stale_launch_guard` / `legacy_lifecycle_event`
   3. unknown detailKind → `unknown_activity_detail_kind`; known but "non-fact" → `non_fact_activity_detail_kind` (the server repeats the daemon's filter, it does not trust it)
   4. **per-producer sequence dedupe**: if `clientSeq` is present it must be strictly greater than the last one seen for the key `(agentId, launchId, daemonInstanceId)`, otherwise drop `stale_client_seq`. A new daemon process or new launch gets a fresh sequence space.
   5. **server derives the activity**: `reduceDaemonActivitySignal` (`agentLifecycleReducer.ts:441`) maps `detailKind` → activity via `CANONICAL_DAEMON_ACTIVITY_BY_DETAIL_KIND` (line 377). The producer does not get to choose the activity.
   6. lifecycle plan: `planActivitySignalAction` over a state snapshot (DB status, reset mode, reachability). An `ignore` verdict drops with `agent_stopped`, `reset_window` or `activity_plan_ignore`.
   7. Kimi circuit breaker → `kimi_activity_circuit_breaker`.
   8. `lifecycle_v2.shadow_verdict` span event: a new state kernel runs in shadow and only writes to the trace, with no effect on behavior.
   9. runtime-error preservation: if the agent currently shows a runtime error, a weak signal (heartbeat, legacy frame, non-progress kind) that would move it to online/working/thinking is accepted but does not overwrite the error (`runtime_error_state.preserved`).
   10. `applyAgentLifecycleProjectionPlan(reduceDaemonActivityLifecycle(...))`, which calls `broadcastActivity` through `lifecycleProjectionWriterDeps` (~2363).
5. **Server → in-memory snapshot.** `broadcastActivity` → `writeAgentActivitySnapshot` (~12053). Arbitration is **off by default**. It turns on only when `RAFT_ENABLE_AGENT_ACTIVITY_KERNEL_ARBITRATION` (or the `SLOCK_` alias) is set and the `..._DISABLE_...` variable is not. When it is on, `arbitrateLifecycleProjection` can reject the observation (`preserve`, `degrade_unknown`, or an `arbitrate` whose result equals the current state), using `observedAtMs` and launch generation. A rejected frame returns `kernel-preserve` right away: **nothing is written, persisted or emitted**, not even its trajectory entries. `resolve_starting` admits the frame but forces `online/idle`. When arbitration is off, or the frame is admitted, it writes `this.agentActivity.set(...)` (`agentOrchestrator.ts:12037-12230`).
6. **Server → Redis.** Fire-and-forget `replicaStateStore.setAgentActivity` (HSET + 600 s TTL) so other replicas can read the current state.
7. **Server plans the action.** `planActivityBroadcastAction` (`agentOrchestrator.ts:827`):
   - entries present → `persist-and-emit-now`
   - heartbeat → `heartbeat-refresh`; probe response → `probe-refresh`; delivery-ack overlay → `delivery-ack-refresh`. These three are emitted but never persisted.
   - status-only and terminal (`offline`/`error`, or `working` with `starting|runtime_starting|message_received|compaction_stale|stalled_recovery`) → `persist-and-emit-now` with a synthesized `status` entry (`shouldPersistStatusOnlyActivity`, 2852)
   - otherwise `debounce-only` (200 ms, `ACTIVITY_DEBOUNCE_MS`, 2110). Only the final state in the window is emitted. When the timer fires it reads the *latest in-memory snapshot*, not the frame that started it, emits without join keys, and persists nothing.
   - Both the persist path and the three refresh paths cancel any pending debounce timer for the agent before they emit (`applyActivityBroadcastAction`, ~12232-12325).
8. **Server → Postgres.** On the persist path, `persistActivityEvent` → `agentActivityLogService.appendAgentActivityEvent`, which runs `INSERT ... ON CONFLICT DO NOTHING RETURNING id`. A call with zero entries returns without inserting (the orchestrator always passes at least the synthesized `status` entry). The service keeps the *first* 100 entries, drops the rest, and coerces an invalid activity to `working` (`agentActivityLogService.ts:44-76`). The service returns a boolean. The orchestrator maps it to `applied`/`deduped`, or `error` on exception, for tracing, and does not wait for it before emitting. Kimi-runtime agents skip persistence entirely (`shouldSkipActivityLogPersistence`, ~12341). The skip returns `false`, so the trace records it as `deduped`, not as skipped.
9. **Server → Socket.IO.** `emitActivity` (~12447) bumps a per-agent `serverSeq` and emits. The counter is an in-process `Map` on each server replica and restarts at 1 after a server restart. The code comment says the client clears its own last-seen values on `socket.connect` and reloads agents, which covers the reset. Then it emits `agent:activity` to room `server:<serverId>`, and to `channel:<id>` for "joint activity projection" channels. **The public payload leaves out `entries`** (comment at ~12498: room members may not be allowed to see a private trajectory). It carries `activity, detail, detailKind, timestamp, serverSeq, launchId, clientSeq, probeId, producerFactId, isHeartbeat, isRefreshOnly`. The emit goes through a failpoint seam, `server.agentActivity.emit`. The comment calls the push "best-effort ... MUST NOT be load-bearing for correctness".
10. **Web store.** `agentActivityEvents.ts` applies the frame only if `serverSeq > lastSeq`. A frame with no `serverSeq` is applied without that check. An equal `serverSeq` can still upgrade a fallback state that lacks a detailKind. Otherwise it records `stale_server_seq` or `producer_seq_conflict` (~276-291). Timestamps are "log metadata only".
11. **Web → REST pull.** When the detail panel's activity tab is open, `registerActivityTrajectoryLiveReload` watches for frames *without* entries, skipping heartbeats and `isRefreshOnly` frames and, after a 250 ms debounce, calls `GET /agents/:id/activity-log`. `registerActivityTrajectoryReconnectReload` does the same on socket reconnect (500 ms) (`activityTrajectoryRecovery.ts`).
12. **Server → Postgres read.** `listRecentAgentTrajectory` reads the newest N rows, reverses them, and flattens them to `{timestamp, entry}` (`agentActivityLogService.ts:78-98`). It is gated by `canInspectAgentPrivateSurfaces` (`routes/agents.ts:1923-1943`).

### Flow B: Cold read / server restart hydration

1. A client asks for agent state, or the server rebuilds after a restart.
2. `resolveRecentPersistedActivity` (`agentOrchestrator.ts:3687`) calls `getLatestAgentActivityHint`. That function reads the newest `agent_activity_events` row and takes `detailKind` from the last `status` entry (`projectAgentActivityHintFromPersistedEvent`).
3. It uses the hint only when the agent is `active`, the machine is reachable (or the agent is external), the activity is transient, and the hint is ≤ 90 s old (`ACTIVITY_STALE_SEC`, 2088). Otherwise it falls back to online/offline based on reachability.

### Flow C: External (plugin) agent reports activity

1. External runtime hook (for example a Claude Code plugin) → `POST /internal/agent-api/activity` with body `raft-agent-activity-ingest.v1`. Auth is `requireAgentCapability("read")` (`internalAgentApi.ts:2986`). The comment says the least-privilege runner credential may report its *own* activity.
2. `recordExternalAgentActivity` (~2910) opens span `server.external_agent.activity.ingest`. For each event it calls `mapExternalPluginActivityEvent` and builds a lifecycle event (`eventType external_agent_signal`, `actor external`) with `correlationId = agent:<id>:externalActivity:<eventId>`. Then `reduceExternalActivityLifecycle` → `applyAgentLifecycleProjectionPlan(plan, lifecycleProjectionWriterDeps())`, which is the same `broadcastActivity` path the daemon uses (verified, `agentOrchestrator.ts:2945-2959`). The external event's `dedupeKey` is passed through to the `dedupe_key` column. Events that don't map are counted as `rejectedCount`, and the response returns `{acceptedCount, rejectedCount, droppedCount}`. None of the daemon's launch guard, clientSeq dedupe or Kimi breaker steps apply here.

### Flow D: Server-side request tracing

1. HTTP request → `requestObservabilityMiddleware` (`requestObservability.ts:186`). It parses an incoming `traceparent` header as the parent and starts `server.http.request` (kind `server`).
2. `runWithTraceSpan(span, next, tracer)` puts the span and tracer into `AsyncLocalStorage` (`semanticTrace.ts:15-23`).
3. Deep service code calls `addTraceEvent`, `tracePhase(work, onComplete)`, `withTraceChildSpan(name, …)`, `emitDecisionEvent(contract, …)`, or the DB tracer (`createTraceDbQueryTracer` emits `db.query.finished/failed` with `query_name, phase, sqlstate, timeout_bucket`). None of these need a `req` argument. Example: `scope_attestation.*` events in `internal.ts:1636-1700`.
4. On `res.finish` the middleware adds event `http.response.finished`, ends the span with `route_pattern`, `caller_kind` (human/agent/system, inferred from the `x-agent-id` header or the `/internal/agent/` prefix) and `status_bucket`, and increments `slock_http_requests_total` / duration. On `close` without finish the span ends with status `cancelled`, and metrics are skipped.
5. `BasicTracer` calls `sink.recordEvent` for each event and `recordSpanFact` + `record` at end. `FanoutTraceSink` (`serverTracer.ts:112`) sends each call to `OtlpHttpTraceSink` (queue → batch 64 / 1 s → OTLP JSON POST, 3 s timeout) and `ScopeDbTraceEventSink` (queue 8192 → batch 512 / 4 s ± 1 s jitter when built from env; the class's own defaults are 128 / 1 s → ScopeDB insert with the code-owned statement). The ScopeDB sink is created only when `RAFT_TRACE_SCOPEDB_SINK=on` and endpoint and key are set. Its `record()` is a no-op: spans reach ScopeDB only as `span_fact` rows, and OTLP stays the canonical span transport. Failures in each sink are caught and logged. Failed OTLP batches are not retried.

### Flow E: Cross-process trace propagation (server ↔ daemon)

1. Server → daemon websocket messages carry an optional `traceparent` (for example `agent:start`, `agent:deliver`, `agent:runtime_profile:*` in `shared/src/index.ts:552-834`).
2. Daemon: `parseTraceparent(msg.traceparent)` becomes the parent of a daemon span. Example: `daemon.agent.start_dispatch.receipt` (kind `consumer`) in `daemon/src/core.ts:3453-3476`. The reply includes `traceparent: formatTraceparent(span.context)`.
3. Result: one trace id covers the server dispatch and the daemon receipt, even though the two halves reach storage by different routes (live OTLP for the server, uploaded bundles for the daemon).

### Flow F: Daemon trace bundle upload

1. Daemon startup: `LocalRotatingTraceSink({machineDir, maxFileBytes 5MB, maxFileAge 5min + jitter, maxFiles 8})` and `createTraceClient({source:"daemon", sinks:[localSink]})` (`core.ts:1660-1672`).
2. Every span → `record()` → `toLocalTraceRecord` + `sanitizeAttrs` → `appendFileSync` to the current JSONL. Errors are swallowed (`localTraceSink.ts:77-86`).
3. `DaemonTraceBundleUploader.start()` waits an initial delay of 0-30 s (hash jitter), then repeats every 5 min + 0-60 s (`traceBundleUpload.ts:88-136`).
4. `findUploadCandidates`: picks `daemon-trace-*.jsonl` files that are not the current open file, have no `trace-uploads/<name>.uploaded.json` sidecar, are non-empty, and have mtime ≥ 60 s old. At most `maxFilesPerRun` per pass.
5. For each file: gzip, sha256, `bundleId = uuid`, then:
   1. Daemon → Server `POST /internal/machine/scope-attestation {scope:"daemon-trace-bundle:create", metadata:{bundleId, bundleSha256, bundleSizeBytes, deploymentEnvironment?}}` (`directUploadCapability.ts:69`).
   2. The server checks machine auth, loads the machine and server, and runs `deriveDaemonTraceBundleMetadata`. That function mints `uploadId`, sets `objectKey`, `maxBytes = 50MB`, and runs `selectDaemonTraceBundleDeploymentEnvironment`, which accepts `dev` only when the server is not production and rejects any other mismatch (`internal.ts:900-985`). It returns an HMAC-signed `scope-attestation` (aud `trace-ingest-worker`, resource `servers/<s>/machines/<m>/trace-bundles`, TTL **2 minutes**, `DAEMON_CAPABILITY_TTL_MS`, `internal.ts:853`). The endpoint also accepts a Computer attachment credential in place of the machine key. The producer's environment claim must be in the closed set `production|staging|dev|test|slockdev`. With no claim, the server uses its own environment.
   3. Daemon → Worker `POST /api/trace-bundles {bundleSha256, bundleSizeBytes, attestation}`. `createTraceBundleUpload` verifies the attestation and checks the body's sha and size against the *signed* metadata. It returns a PUT URL containing a signed `trace-upload-session` token (`index.ts:582-645`).
   4. Daemon → Worker `PUT /api/trace-bundles/<uploadId>/object?token=…`. `putTraceBundleObject` verifies the token and Content-Length, reads with a size limit, recomputes sha256, runs `TRACE_BUNDLES.put(objectKey)` to R2, writes a ledger with `scopedb_status: pending`, and calls `scheduleTraceBundleIngest` via `ctx.waitUntil` (`index.ts:647-727`).
6. The daemon writes the sidecar `.uploaded.json` and ends span `daemon.bundle.upload` (kind `producer`). That span lands in the *next* bundle. On any failure it ends the span with `error` and writes no sidecar, so the file is simply picked up again on the next tick. There is no backoff or retry counter; retries come from the sidecar being absent. Rotation is lazy: the age and size checks run only when the next span is written, so an idle daemon keeps its current file open.
7. Background ingest (`ingestTraceBundleObject`, 776): read the object back from R2 → re-check size and sha → gunzip (100 MB cap) → parse JSONL → for each 128-span batch, `postOtlpTraceBatch` (each span gets `slock.trace_ingest.span_key = serverId:machineId:bundleSha256:trace_id:span_id`) → `projectV2BestEffort` into `trace_events_v2`. The ledger is rewritten with `success` or `failed`. A failed ingest does not fail the upload, and R2 remains the source for replay. Details: (a) all of this runs only when `TRACE_INGEST_OTLP_ENDPOINT` is set, otherwise the ledger says `skipped` and nothing is ingested; (b) the worker never retries a failed ingest automatically, so replay is a manual or operator step; (c) a failure partway through leaves the earlier batches already posted, which is why a replay produces duplicates. **Dedupe happens when data is read, not when it is written**: the README says ScopeDB analysis "must dedupe on" `span_key`. A failure also sets `v2_projector_status: skipped`.
8. If the bundle carries a `feedbackReportId`, the worker also sends a webhook to feedback-admin (`index.ts:699-719`).

### Flow G: Web client traces

1. Browser → `POST /servers/:id/scope-attestation` (`routes/servers.ts:1651`, user-authenticated, gated by `canCreateScopeAttestation`; verified to be called from `webAuthTrace.ts:602`) → `POST <TRACE_URL>/api/web-traces {attestation, batchId, records, resource}` (`web/src/utils/webAuthTrace.ts:615`).
2. Worker `ingestWebTraceBatch` (`index.ts:546`) verifies the attestation, posts to OTLP, runs the V2 projection, and returns counts. Web families include `slock.state.transition`, `slock.state.violation` (coalesced) and `slock.client_error` (`web/src/utils/stateTransitionTrace.ts`, `stateViolationTrace.ts`). The trace-client README says browser trace ids are *not* continuous with server request trace ids.

### Flow H: Agent o11y turn/step events (v0 POC)

1. Daemon `agentO11yClient.ts` builds `{event_kind, agent_id, turn_id, occurred_at, payload_tier T0|T1|T2, *_hash, fields}` and runs a fast check that rejects tenant keys, `payload_mode`, and raw payload keys. I found no non-test import of this client (so it is likely not wired into the runtime yet, inferred).
2. → `POST /internal/computer/agent-o11y/events` (`internalComputer.ts:928`). `validateAgentO11yBatch` enforces max 500 events and 1 MB. It forbids tenant fields in the body (tenancy comes only from the Computer principal) and forbids `content/prompt/input/output/...` keys unless the tier is T2.
3. `verifyAgentO11yAgentsOnMachine` requires every agent_id to be on this Computer's machine, or the request gets a 403.
4. `SdkAgentO11yScopeDbWriter.writeEvents` performs a synchronous ScopeDB insert, maps T0→hash, T1→summary, T2→full, and returns `202 {accepted}`. If the writer is unconfigured or the write fails, the response is 503.

### Flow I: Feature flag evaluation and mutation

1. Evaluation (in core): `evaluateFeatureFlags(inputs[])` batch-loads flags and rules and preloads audiences, labs and billing (`featureFlagService.ts:563`). `evaluateFlagRow` (409) checks in order: missing → kill_switch → disabled → user → platform → server → audience → lab → plan → missing unit → percentage (`sha256(salt:key:unit:unitId) % 10000`, line 206) → default. It returns `{enabled, reason, variant}`.
2. Mutation (outside core): the operator logs in to the `feature-flag-admin` Worker using "Login with Raft". The Worker connects to PG through Hyperdrive as the restricted role `feature_flag_admin_operator` and writes audit rows to D1 (`apps/feature-flag-admin/DEPLOY.md`).
3. **Correction:** the live mutation path is inside the Worker (`apps/feature-flag-admin/src/worker.ts`). It takes `pg_advisory_xact_lock` (~1283), bumps `feature_flag_config_versions` (~1295), and writes audit rows to **D1** (`feature_flag_audit_events`, ~1390). DEPLOY.md says "Raft core exposes no admin API". The core module `featureFlagRolloutWriterService.ts` (global then per-flag advisory lock, `decideFeatureFlagRolloutGuardrail` from `shared/src/featureFlagRolloutGuardrail.ts:367`, compare-and-set, audit row into PG `feature_flag_rollout_audit_events` in the same transaction) exists, but **no non-test code imports it** (grep). Treat the receipt-based guardrail as designed and tested but not yet on the production path. Its rules: *narrowing* operations (kill switch on, remove server, lower percentage) are allowed as they are; *widening* needs a trusted receipt loaded by id from a server-side provider; request JSON is never accepted as evidence. Operator authorization in the Worker comes from a D1 `admin` role grant. Announcement publishing also requires a human principal, and agents fail closed there.

## 5. State machines

**Agent activity (user-visible), `AGENT_ACTIVITIES`**
States: `offline`, `online`, `thinking`, `working`, `error`.
Important: this is mostly a **stateless projection**, not a guarded state machine. The server maps each incoming `detailKind` straight to an activity through `CANONICAL_DAEMON_ACTIVITY_BY_DETAIL_KIND` (`agentLifecycleReducer.ts:377`), whatever the previous state was. The previous state only matters through the guards: the launch guard, `clientSeq` dedupe, the lifecycle plan's `ignore` (stopped or reset window), runtime-error preservation, and the kernel arbitration when it is enabled. The arrows below show typical sequences, not enforced edges.
Transitions implied by `projectTraceActivityFromFact` (`daemon/src/apmStateMachine.ts:23-46`) and the server arbitration:
- `* → online` on `idle | ready | computer_started | computer_restarted | computer_upgraded | synthetic_repair`
- `online → working` on `message_received`, `model_request_started`, `model_response_started`, `tool_started`, `runtime_progress`, and similar
- `working ↔ thinking` on `thinking_started` / anything that ends thinking (`runtimeEventEndsThinking`: text, tool_call, tool_output, compaction, review, turn_end, error)
- `* → error` on `runtime_error | runtime_stalled | computer_operation_failed`
- `* → offline` on `runtime_crashed | runtime_unavailable | stopped | runtime_interrupted | machine_disconnected`
- Arbitration (flag-gated) actions: `preserve` (reject the incoming update), `degrade_unknown`, `resolve_starting` (forces `online/idle`), `arbitrate/admit` (`agentOrchestrator.ts:12139-12230`).

**Broadcast plan per frame** (`planActivityBroadcastAction`): `persist-and-emit-now | heartbeat-refresh | probe-refresh | delivery-ack-refresh | debounce-only`, as listed in Flow A step 7.

**Web adjudication outcome** (`agentActivityEvents.ts:181`): `applied | stale_server_seq | producer_seq_conflict | invalid_activity | logged | no_op`.

**Local trace file lifecycle** (daemon disk): `open (current)` → rotated when size > 5 MB, age ≥ 5 min + jitter, or a new file is needed → `closed` → eligible when mtime ≥ 60 s → `uploaded` (sidecar exists) → `pruned` once the directory holds more than 8 files (pruning deletes the oldest regardless of upload state, see Surprises).

**Trace upload ledger** (`index.ts:861`): `r2_status: success`, then `scopedb_status: pending → success | failed` (or `skipped` when OTLP is unconfigured). `v2_projector_status: pending → success | failed | skipped`.

**Feature flag evaluation reasons**: `missing_flag, initial_allowlist, kill_switch, flag_disabled, missing_user_unit, missing_server_unit, user_rule, platform_rule, server_rule, audience_rule, lab_rule, plan_rule, percentage_rule, default` (`featureFlagService.ts:93-107`).

**Guardrail decision**: `narrowing | no_op | guardrail_passed(receiptId)` or blocked with `config_version_required | config_version_mismatch | invalid_state | invalid_intent | flag_key_mismatch | receipt_missing | receipt_invalid | receipt_stale | receipt_mismatch …`.

## 6. Design patterns worth stealing

1. **Separate "what humans see" from "what engineers debug".** Activity (`agent_activity_events` + Socket.IO) is a small closed-vocabulary product surface. Traces go to OTLP/ScopeDB. The `product_events` comment (`schema.ts:~5480`) states the boundary. *Why:* your research UI can stay stable and permission-checked while traces change freely. For Temporal, this means you should not expose Temporal history to users. Project a small activity vocabulary from workflow and activity events instead.
2. **Push to notify, pull to converge.** Socket frames are best-effort and carry no entries. The client reloads the durable log after a reconnect or after a frame without entries (`activityTrajectoryRecovery.ts`, the CC-006 comment in `agentOrchestrator.ts:~12505`). *Why:* dropped websocket messages never corrupt state, and private content never goes to a broadcast room.
3. **Two sequence numbers: one from the producer, one from the server.** The daemon's `clientSeq`, scoped to `(agentId, launchId, daemonInstanceId)`, lets the server drop replayed or reordered inbound frames. The server's `serverSeq` per agent lets the browser drop stale pushes (`emitActivity`; `agentActivityEvents.ts:276`). *Why:* ordering does not depend on wall clocks. *Caveat:* `serverSeq` is an in-memory counter on each replica and resets on restart. It protects one socket connection against reconnect races. It is not a global order, and it depends on the client resetting when it reconnects. If you copy this with Temporal, a durable per-entity counter (for example a Postgres sequence column) would give a stronger guarantee.
4. **A closed enum for status detail, and dropping unknowns at the edge.** `AGENT_ACTIVITY_DETAIL_KINDS` plus the daemon-side and server-side drop of unknown and "non-fact" kinds (`agentActivityProducer.ts:88-113`). *Why:* the UI can render every state, and producers cannot invent new states without a shared type change.
5. **Persist only facts. Debounce or refresh everything else.** `planActivityBroadcastAction` (827): heartbeats and probes are never written, status-only chatter is debounced for 200 ms, and terminal states get a synthesized row. *Why:* the activity table does not fill with liveness noise, and the "why did it stop" rows are always present.
6. **Semantic idempotency keys with a partial unique index.** `dedupe_key` + `ON CONFLICT DO NOTHING` (`schema.ts:5029-5051`). *Why:* repeated reconcile observations produce a single visible row. This is the same shape as a Temporal activity's idempotency key.
7. **Launch-generation guard on every inbound event.** `getLifecycleEventAcceptanceAction` drops frames from an old `launchId`. *Why:* a restarted agent's leftover output cannot overwrite the new run's state. For you, this is the same as tagging updates with workflow `runId` and rejecting mismatches.
8. **Write locally first, upload later in bulk.** Daemon spans go to rotating JSONL files, and a separate uploader ships closed files (`localTraceSink.ts`, `traceBundleUpload.ts`). *Why:* tracing never blocks or crashes the agent host, works offline, and survives a server outage. This fits AgentCore runtimes, where you may not want a live OTLP exporter in every session.
9. **The control plane signs and the data plane stores.** The server issues a scoped, short-lived attestation that fixes `objectKey`, `maxBytes`, sha and size. The upload worker trusts only the signed fields, and trace bytes never pass through the API server (`internal.ts:855-990`, worker `index.ts:582-727`). *Why:* the server does not carry the bandwidth, and a compromised machine can write only to its own prefix.
10. **Content-addressed, at-least-once ingest with an explicit dedupe key.** `span_key = serverId:machineId:bundleSha256:trace_id:span_id`, plus a per-upload ledger (`trace-upload-worker/README.md`, `index.ts:861`). *Why:* replays are safe, and each bundle's status can be reconciled by listing ledgers. *Nuance:* duplicates do get stored. The key lets readers dedupe; nothing stops the duplicate rows being written. Replays are not automatic either.
11. **Emitting process is set by the client and cannot be overridden.** `source` is type-forbidden in caller attrs and stripped at runtime at both start and end (`traceClient.ts:57-178`). *Why:* producers sharing one pipeline cannot mislabel each other.
12. **Privacy is enforced at the sink.** `sanitizeAttrs` drops `*_id` (except an allowlist), `prompt|content|message|path|stdout|…`, turns arrays and objects into counts, and redacts `sk_*` keys in error excerpts (`localTraceSink.ts:153-227`). Decision events reject unsafe keys (`decisionTrace.ts:27`). Agent-o11y rejects raw payload keys below tier T2. *Why:* a research system handles sensitive prompts, so tracing should hold metadata by default and content only when explicitly enabled.
13. **Ambient span context through AsyncLocalStorage.** `withTraceChildSpan` / `addTraceEvent` need no `req` argument (`semanticTrace.ts`). *Why:* instrumentation deep in the code does not change function signatures.
14. **Trace families must name a consumer.** `TRACE_FAMILY_REGISTRY` requires `consumers[{what, how, whoRuns, runbook}]`, a privacy tier, and filterable dimensions. Filtering on an undeclared dimension throws an error instead of returning an empty result (`traceFamilyRegistry.ts`). *Why:* it stops teams from collecting telemetry nobody reads.
15. **Hash-derived jitter for each machine.** `computeTraceJitter(lockId)` (`traceJitter.ts`). *Why:* after a fleet restart, uploads spread out and stay spread across restarts.
16. **Tenancy comes from auth, never from the body.** Agent-o11y forbids `server_id/computer_id/machine_id` in the payload and stamps them from the principal. It also checks that every agent belongs to the caller's machine (`internalComputer.ts:921-1003`).
17. **Asymmetric authorization for flag changes.** Narrowing is always allowed, widening needs a trusted receipt, and both use compare-and-set with an audit row in the same transaction. Flag admin runs as a separate app with a least-privilege DB role. *Why:* this is a safe way to let agents themselves (`actor_type: agent`) change rollouts. *Status:* the receipt guardrail lives in `featureFlagRolloutWriterService.ts`, which has no production caller yet. What runs today is the separate Worker with a restricted PG role, advisory locks, a config-version bump and D1 audit rows. Learn the design from the guardrail, but don't describe it as deployed.

## 7. Surprises / sharp edges

- **Socket payloads leave out trajectory entries** even though the orchestrator builds them. Watchers who lack private-surface permission see only status and detail. The full trajectory is REST-only (`agentOrchestrator.ts:~12498`, `routes/agents.ts:1923`).
- **Debounced status-only emits drop join keys** (launchId/clientSeq) on purpose, so feedback export classifies those rows as `join_key_missing` (comment in `broadcastActivity`, ~12020).
- **Persistence is fire-and-forget relative to the emit.** The socket frame can go out before the Postgres insert commits, or when the insert fails (`applyActivityBroadcastAction`, ~12235-12270). The REST pull may briefly miss a row the UI already showed.
- **Kimi runtime is special-cased.** Its activity log persistence is skipped entirely, and a per-launch circuit breaker suppresses repeated error/working signatures that previously flooded projection, Redis and Socket.IO (`agentOrchestrator.ts:12341-12436`, dated 2026-06-04 workaround).
- **Redis activity mirror has a 10-minute TTL** and is written fire-and-forget. The cold path falls back to Postgres hints, but only for transient states newer than 90 s.
- **Kernel arbitration is env-flag gated and off by default** (`RAFT_ENABLE_AGENT_ACTIVITY_KERNEL_ARBITRATION`; `RAFT_DISABLE_...` wins). `isAgentActivityKernelArbitrationEnabled` reads only the env vars. There is also a feature-flag key `agent_activity_kernel_arbitration_v0` in `featureFlagService.ts:39`, but this code path does not consult it. When arbitration rejects a frame, the frame's trajectory entries are lost too: nothing is persisted or emitted.
- **Local pruning can delete files that were never uploaded.** `pruneOldFiles` keeps the newest 8 files and ignores upload state (`localTraceSink.ts:113-123`). A daemon offline for more than about 40 min at 5 min per file will lose traces (inferred from the defaults).
- **The upload span lands in a later bundle.** `daemon.bundle.upload` is written to the current local file, so its telemetry describes the previous bundle.
- **Both server sinks drop the oldest item when full**, and neither has durability. ScopeDB rows are explicitly "decision_support" tier, not authoritative (`scopeDbTraceEventSink.ts:62`, `agentO11yScopeDbWriter.ts:97-103`).
- **Schema drift protection.** The ScopeDB sink validates the live table schema before writing and only accepts ingest statements generated in code (current or legacy 59-column). A hand-edited env statement disables the sink (`serverTracer.ts:156-162`).
- **Deployment environment is producer-claimed but server-checked.** A dev laptop daemon may label its traces `dev` against staging, never against production (`internal.ts:935-941`).
- **Browser trace ids are not continuous with server trace ids** (trace-client README). Web → server correlation needs other keys (`clientEventId`).
- **The agent-o11y writer is synchronous** and returns 503 when ScopeDB is down. There is no outbox or retry, and the daemon client appears unwired (inferred: no non-test import of `agentO11yClient.ts`).
- **`appendAgentActivityEvent` silently coerces an unknown `activity` to `working`** and truncates to 100 entries. It logs a warning and does not reject.
- **Feature-flag special case**: one flag (`THREAD_AGENT_FOLLOWER_MANAGEMENT`) has a hard-coded initial allowlist by server slug when the flag row is missing (`featureFlagService.ts:620-653`).
- **The trace-client package is deliberately limited to three consumers** (daemon, computer CLI, menu-bar). The server uses its own tracer path (`serverTracer.ts`), and web uploads directly to the worker.

## Verification log

Checked against code (packages/server, daemon, web, trace-client, trace-upload-worker, apps/feature-flag-admin):

- `planActivityBroadcastAction` order and `shouldPersistStatusOnlyActivity` kinds: confirmed.
- `broadcastActivity` / `writeAgentActivitySnapshot` / `planActivityBroadcastArbitration`: confirmed, with fixes. Arbitration is off by default. A rejected frame (preserve, degrade_unknown, or an arbitrate that resolves to the current state) is neither persisted nor emitted. Added that the debounce timer emits the latest snapshot and that the other paths cancel a pending debounce.
- `applyActivityBroadcastAction` / `appendAgentActivityEvent`: confirmed fire-and-forget persistence. Fixed: the service returns a boolean, empty entries are never inserted, truncation keeps the first 100, and the Kimi skip shows up as `deduped`.
- `emitActivity`: confirmed payload without entries, CC-006 comment and failpoint. Added that `serverSeq` is per replica, in memory, and resets on restart. Rewrote pattern 3.
- Daemon `buildDaemonActivityMessage`: **corrected**. The message has no `activity` field. The server derives the activity from detailKind (`reduceDaemonActivitySignal`, `CANONICAL_DAEMON_ACTIVITY_BY_DETAIL_KIND`).
- `case "agent:activity"` ingest: added the missed steps (server-side non-fact drop, `clientSeq` dedupe keyed by agent/launch/daemonInstance, lifecycle plan ignore reasons, Kimi breaker, shadow verdict, runtime-error preservation, projection writer).
- Section 5 activity "state machine": relabeled as a stateless detailKind → activity projection plus guards.
- Web reducer and recovery (250 ms / 500 ms, skip heartbeat or refresh-only): confirmed. Added the case of a frame with no serverSeq.
- Flow C external activity: confirmed it uses the same projection writer. Removed "inferred".
- Redis mirror: confirmed 600 s TTL and fixed the file path.
- Server sinks: confirmed OTLP 4096 queue that drops the oldest, 64 / 1 s / 3 s. ScopeDB sink needs `RAFT_TRACE_SCOPEDB_SINK=on`, its `record()` is a no-op, and there is no retry.
- Local sink: confirmed 5 MB / 5 min / 8 files, and pruning ignores upload state. Added that rotation is lazy.
- Uploader: confirmed candidate rules. Added that retries come only from a missing sidecar, with no backoff.
- Attestation: confirmed objectKey, 50 MB and the environment policy. Added the 2-minute TTL, the closed environment set, and that a Computer credential is accepted.
- Worker ingest: confirmed 128-span batches, 100 MB cap, span_key, and ledger statuses. Added that ingest needs the OTLP endpoint, is never retried automatically, can leave a partial ingest, and relies on readers to dedupe.
- Web trace attestation route: confirmed. Removed "inferred".
- Agent-o11y client: confirmed no non-test importer. Only its own test and a contract test reference it.
- Feature flags: **corrected**. The core `featureFlagRolloutWriterService` + receipt guardrail has no non-test caller. Live mutation is in the admin Worker (PG advisory lock, config-version bump, D1 audit). Updated Flow I step 3 and pattern 17.
- Not re-verified: exact line numbers in `schema.ts`, the `AGENT_ACTIVITY_DETAIL_KINDS` count, `TraceEventRow` column count, feature-flag evaluation order, Prometheus port.
