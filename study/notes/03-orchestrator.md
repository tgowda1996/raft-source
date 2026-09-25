# 03 — Agent orchestrator and agent lifecycle

All paths are relative to `/home/user/raft-source`. `AO` = `packages/server/src/services/agentOrchestrator.ts` (12,537 lines). `RED` = `packages/server/src/services/agentLifecycleReducer.ts`.

## 1. What it is

The `AgentOrchestrator` class (AO:2000) is the server-side process that owns every live WebSocket to a daemon ("machine"). It decides what happens to each message addressed to an agent: deliver it now, wake the agent first, hold it behind a gate, or drop it. It also takes in the daemon's lifecycle signals (`ready`, `agent:status`, `agent:session`, `agent:activity`, acks) and turns them into DB status writes, live "activity" pushes over Socket.IO, and Activity Log rows. Nearly every decision is a small pure `plan*` function that returns a string action. A separate `apply*` method then performs the side effects. Lifecycle semantics are being moved into a dedicated reducer (`RED`). The reducer returns a *projection plan*. A single writer (`agentLifecycleProjectionWriter.ts`) applies that plan.

## 2. Key components

| Name | File(s) | Responsibility |
|---|---|---|
| `AgentOrchestrator` | AO:2000 | Holds machine sockets, in-memory inboxes, the agent state cache, pending-ack maps, and the reset map. Runs the wake/deliver/ack/start/stop/reset flows. |
| Pure planners (`plan*`) | AO:801–1760; RED:212–310 | Tiny total functions: wake, receive, ack partition, local inbox dedupe, local delivery gate, send-to-machine, routed ownership, stop, reset, ready-reconcile, launch-guard acceptance, stale sweeps, mention-redrive HTTP status. |
| Lifecycle reducer | RED | `buildAgentLifecycleStateSnapshot` (RED:161), `planWakeAction` (RED:223), `planStatus/Session/ActivitySignalAction`, `reduce*Lifecycle` → `AgentLifecycleProjectionPlan` (RED:110), plus the arbitration kernel `arbitrateLifecycleProjection` (RED:1363). |
| Lifecycle events | `packages/server/src/services/agentLifecycleEvents.ts` | Canonical event envelope `AgentLifecycleEvent` (:107), the 16 event types (:53), reasons, and the four projection kinds (`db_status`, `wake_eligibility`, `live_activity`, `activity_log`, :35). No semantics, no I/O (header comment :5–17). |
| Legacy adapter | `packages/server/src/services/legacyAgentLifecycleAdapter.ts` (435 lines) | Translates old daemon protocol messages (`agent:status`, `agent:session`, start/stop, disconnect) into canonical events (`adaptDaemonStatusLifecycleEvent` etc., imported at AO:109–120). |
| Projection writer | `packages/server/src/services/agentLifecycleProjectionWriter.ts` | `applyAgentLifecycleProjectionPlan` (:131). This is the only place that turns a plan into DB status writes, cache updates, wake-lock release, stop-to-machine, live activity broadcast, Activity Log persistence, and trace rows. |
| Agent DB service | `packages/server/src/services/agentService.ts` | `updateAgentStatus` (:625) for explicit actions, `updateAgentStatusFromSignal` (:669) for daemon signals, which refuses to overwrite `stopped`. Also `resetAgentSession` (:1111). |
| Mention delivery ledger | `packages/server/src/services/mentionDeliveryOccurrenceService.ts`; table `mention_delivery_occurrences` | Durable per-(message, agent) record of an @mention push, with CAS state transitions and redrive. |
| Replica router / wake lock | `packages/server/src/replicaRouter.ts:1131` | Redis `SET slock:agent:{id}:waking NX EX 30` so only one replica starts an agent. Cross-replica routing of machine commands and inbox deliveries. |
| Message fan-out | `packages/server/src/services/messageService.ts:8065` `broadcastAndDeliver` | Computes the agent audience for a new message and calls `agentOrchestrator.deliverMessage` for each agent (:9227). |
| Receive / ack HTTP | `packages/server/src/routes/internal.ts:2638` (long-poll receive), `:2711` (`POST /agent/:id/receive-ack`) | Agent-side pull path and explicit ack. (Verified: `GET /agent/:id/receive` is at :2573 and requires `message:read` scope; receive-ack at :2707.) |
| Daemon side | `packages/daemon/src/core.ts:3662` | Handles `agent:deliver`, buffers it during startup, hands it to the agent manager, sends `agent:deliver:ack`. |

## 3. Data model

**Postgres (`packages/server/src/db/schema.ts`)**
- `agents` (:851): `id`, `server_id`, `name`, `status` enum `active | inactive | stopped` (default `inactive`), `session_id` (the runtime's resumable conversation id), `runtime`, `model`, `runtime_config` jsonb, `last_runtime_error` jsonb, `execution_mode` (`byoc|cloud`), `daemon_id` (column name kept from the old model; mapped to `machineId`), `deleted_at`.
- `daemons` (exported as `machines`, :3377): `server_id`, `user_id`, `api_key_hash`, `runtimes` json, `daemon_version`, `computer_version`, `last_heartbeat`.
- `agent_activity_events` (:5023): Activity Log rows. `activity` enum `online|thinking|working|error|offline`, `detail`, `entries` json (trajectory), and `dedupe_key` with a partial unique index `(agent_id, dedupe_key) WHERE dedupe_key IS NOT NULL`. One visible row per agent per incident.
- `message_mentions` (:5669): one row per mention fact. Its `id` becomes the occurrence id.
- `mention_delivery_occurrences` (:5698): PK `occurrence_id` → `message_mentions.id`. Columns: `message_id`, `server_id`, `agent_id` (unique on `(message_id, agent_id)`; index on `(machine_id_snapshot, state)` for recovery), `delivery_payload` jsonb (the exact `AgentMessage`), `state` (7 states, see §5), `delivery_path` (`unknown|immediate|busy`), identity snapshots `machine_id_snapshot / launch_id_snapshot / session_id_snapshot`, one timestamp per hop, `terminal_error_code`, `pending_coalesced_count`, `version` (bumped on every write; CAS key for redrive), `redrive_count`, `last_redrive_at`. CHECK constraints tie `state='acked'` to `acked_at` and `state='terminal_error'` to both the error timestamp and the code (:5743–5750).

**Shared types (`packages/shared/src/index.ts`)**
- `AgentStatus = "active" | "inactive" | "stopped"` (:1370).
- `AGENT_ACTIVITIES = online, thinking, working, error, offline` (:1309), plus ~40 `AGENT_ACTIVITY_DETAIL_KINDS` (:1314), e.g. `runtime_starting`, `ready`, `runtime_interrupted`, `machine_disconnected`, `stopped`, `tool_started`.
- `AgentMessage` (:100): `channel_id`, `seq` (from `messages.seq`), `message_id`, `sender_*`, `content`, `mentioned`, task fields, and so on.

**In-memory state in the orchestrator (AO:2002–2060).** None of this is durable.
- `machineConnections`: machineId → socket, daemon version, capabilities.
- `agentInboxes`: agentId → `{ inbox: AgentMessage[], pendingReceive }`. The replayable inbox lives only in memory and is capped at 1000 messages (AO:~11478).
- `agentStateCache`: `CachedAgentState` with `status`, `machineId`, `sessionId`, `expectedLaunchId`, `launchGuardMode`, `runtimeState`.
- `pendingAgentDeliveryAcks`, `pendingAgentStartAcks`: retry bookkeeping.
- `resetInProgress`: agentId → `restart|session|full`.
- `pendingMachineDisconnects`: 2 s grace timers (AO:547).

**Reducer state snapshot** `AgentLifecycleStateSnapshot` (RED:141). It is built fresh on every call from the stores above. The comment says it is "deliberately not a new persisted source of truth". It holds several independent axes:
- `dbStatus`
- `intentState` (`running_allowed | manual_stopped | deleted`)
- `runtimeState` (`not_running | starting | running_idle | working | thinking | interrupted | crashed | stalled | unknown`)
- `machineReachability`
- `controlGate` (`open | runtime_profile_migration | reset_window | zen_migrating`)
- `resetMode`, `launchId`, `sessionId`

## 4. Flows

### Flow A — A human posts a message; the server decides whether to wake the agent
1. Web/CLI → `POST` message route → `messageService.broadcastAndDeliver` (messageService.ts:8065) persists the message and gets `message.seq`.
2. `broadcastAndDeliver` builds the audience: channel agents plus mention-only agents who are not members. It drops the sender and any agent whose activity is muted, unless a mention or task assignment pierces the mute (:9081–9105). It builds one `AgentMessage` per agent (:9149).
3. Before any delivery, the server writes durable mention occurrences with `ensureAgentMentionDeliveryOccurrencesForAgents` (defined :7354, called :9197). The comment says "recorded but never delivered is recoverable while delivered but never recorded is not". If that write fails, delivery still proceeds, and a `persist_degraded` trace records it (:9193).
4. For each agent, `agentOrchestrator.deliverMessage(agentId, payload, {mentionDeliveryOccurrenceId})` runs fire-and-forget (:9227). It is not awaited by the HTTP response.
5. `AO.deliverMessage` (AO:10920):
   a. It loads the agent (`getAuthoritativeAgentForDelivery`) → `dropped/agent_unavailable` if the agent is missing.
   b. Scope gate: without `intrinsic` or `adminAuthority`, the agent needs `inbox:receive` scope (`hasPassiveDeliveryScope`), otherwise → `dropped/passive_scope_revoked`. Unless `intrinsic`, it also rechecks channel access (`canAgentAccessDeliveryTarget`) → `dropped/target_access_changed` on failure. [verified AO:10934–10940]
   c. Strict receipt mode for a remote machine: route through Redis RPC with a receipt (`routeInboxDeliveryWithReceiptCrossReplica`).
   d. External runtimes (agents Raft does not launch): push to the local inbox, emit the `external-inbox-delivered` event, publish a cross-replica wake signal → `queued/external_inbox`.
   e. `loadWakePlanInput` (AO:9547) builds the snapshot. It reads the machine-migration gate from `agentMigrationService` (→ `controlGate=zen_migrating`) and the runtime-profile gate from `agentRuntimeProfileService` (→ `runtime_profile_migration`). It also reads `resetMode` and the cached `runtimeState`. Two nuances the planner inherits: (1) `machineReachability` is left `"unknown"` (a TODO says so), so **whether the machine is online plays no part in the wake decision**; (2) if the cache has no entry (e.g. right after a server restart, before `ready`), `inferRuntimeState` assumes `active → running_idle`, so the planner picks `deliver-directly`.
   f. `planWakeAction` (RED:223) returns one of `suppress-control-gate`, `suppress-reset`, `suppress-stopped`, `attempt-wake`, `deliver-directly`. Rules, in order:
      - migration gate (`zen_migrating` or `runtime_profile_migration` only; the `reset_window` gate is *not* a control-gate suppress) → suppress
      - `inactive` → wake unless a reset is running
      - `stopped` → suppress
      - `active` but `runtimeState=not_running` → wake unless a reset is running
      - otherwise deliver directly
   g. `suppress-control-gate`: queue in the local inbox with `notifyPendingReceive:false`, so the message is released after the gate clears. Transient messages are dropped instead. The server may also send a migration nudge to the daemon. Result → `queued/control_gate_inbox`.
   h. Mention fallback (hotfix, "mention-push incident 2026-08-27"): if the message has no `message_id`, or the agent has no machine, cached `expectedLaunchId` or `sessionId`, the server CAS-abandons the occurrence (`abandonMentionDeliveryWithoutInstrumentation`: `recorded` → `terminal_error` with code `INSTRUMENT_FAILED`, only if still pre-decision) and delivers untracked. Losing the CAS → `dropped/agent_state_changed` (someone else owns the push, so no second copy). If the CAS query itself throws → `queued/replayable_inbox` and the row stays `recorded` for later recovery. Consequence: a mention to an agent whose launch guard was cleared (e.g. after a stop) leaves the tracked ledger and rides the ordinary wake path.
   i. Anything else except `deliver-directly` → `applyWakeAction` (Flow B). `deliver-directly` → `applyDirectDelivery` (Flow C).
6. The return value is a typed receipt `AgentMessageDeliveryResult` (AO:1412): `queued` with 5 reasons, or `dropped` with 9 reasons. The comment says: "`queued` never proves daemon/model consumption."

### Flow B — Wake (start) an agent because of a message
1. `applyWakeAction` (AO:11293):
   - `suppress-reset` → records a lifecycle ring-buffer event → `dropped/reset_in_progress`.
   - `suppress-stopped` → `dropped/wake_suppressed`.
   - `attempt-wake` → `startAgent(agentId, { wakeMessage })`. Tracked mentions are the exception: they are not embedded in the start message. They are re-sent later from the durable row once `agent:session` arrives. (A mention only reaches this branch still tracked if step 5h did not abandon it, i.e. the cache still held a launch id and session id — typically an `active` agent whose runtime is `not_running`.)
   - Start outcomes map to receipts: `dispatched` → `queued/wake_accepted`; `manual_stop` → `dropped/wake_suppressed`; `wake_lock_held` → message goes to the local inbox (`queued/replayable_inbox`) unless strict receipt or transient; a thrown error → `dropped/wake_failed`.
2. `startAgent` (AO:9598):
   a. `loadAgentForStart` from the DB. If the agent is `stopped` and there is a wake message → skipped `manual_stop`. External runtimes and agents with no machine → throw.
   b. Preflight the model catalog (`validateBuiltInPresetForMachine`) and take a catalog "authority" lease. The code comment says both happen before the wake lock, the cache and status writes, credential work and dispatch.
   c. `replicaStateStore.acquireWakeLock(agentId)`, which is Redis `SET NX EX 30`. If another replica holds it → `skipped/wake_lock_held`. In that case `applyWakeAction` stores the message in the local inbox (`enqueueWakeLockFallbackToLocalInbox`).
   d. Write `agentStateCache` with `status: active`, `runtimeState: starting`.
   e. Build `AgentConfig`: runtime, model, env vars, `sessionId` for resume, and `runtimeContext`.
   f. If `sessionId` exists, load `unreadSummary` (per-channel unread counts) on every start, wake included. Only when there is no `wakeMessage` and no `resumePrompt` (a plain manual start) does it also load `resumeMessages` (`messageService.getAgentResumeCatchupMessages`, bounded by the plan's history cutoff). So a message-triggered wake gets the one wake message plus a count of other unreads, not their bodies. An agent with no `sessionId` (fresh, or after a session/full reset) gets no catch-up at all. [verified AO:9775–9820]
   g. `prepareStartLaunchGuard` (AO:5468) creates a fresh `launchId` (a UUID), but only for daemons ≥ 0.30.1 (`supportsLaunchGuardForDaemonVersion`, AO:1739). It also creates `startDispatchId`.
   h. Send `agent:start` (or `agent:start:wiki`) over the socket with `sendAgentStartWithAckRetry`. This tracks `pendingAgentStartAcks` with a 5 s timeout and up to 24 attempts (AO:2106). If the send fails: roll back the cache status and launch guard, release the wake lock, and throw `RouteFailureError("daemon_offline")`.
   i. `advanceActivityIngestEpoch`, then initialise the inbox.
   j. `adaptStartLifecycleEvent` → `reduceStartLifecycle` (RED:316) → `applyAgentLifecycleProjectionPlan`. The DB write is `status=active` through the "direct" writer. The live activity emitted is `working / "Starting…" / runtime_starting`, plus an Activity Log row.
3. Daemon → `agent:start:ack` (AO:7400) marks the pending start as terminal. Later the daemon → `agent:session {sessionId, launchId}` (AO:7137). Its steps:
   - `validateMachineAgentMessage` (the agent must belong to this machine) → `shouldAcceptLifecycleEvent` (AO:3812, launch guard; wraps the pure `planLifecycleEventAcceptance`, AO:1721)
   - `acknowledgePendingAgentStartFromLifecycle`: a valid `agent:session` also settles the pending start retry, so a lost `agent:start:ack` does not cause a re-send
   - `planSessionSignalAction` (returns `ignore-and-release-wake-lock` if the agent is `stopped`, `ignore` during a reset window without an accepted launch id, else `persist-active-session`) → `reduceDaemonSessionLifecycle` (RED:943)
   - the writer persists `active` + `sessionId` via `updateAgentStatusFromSignal`, releases the wake lock, and resolves the "Starting" activity
   - Socket.IO `agent:session` goes to room `server:{serverId}` (AO:9479)
   - `recoverDurableMentionDeliveriesForAgent` re-sends any tracked mentions.

### Flow C — Direct delivery to a running agent, ack, and retry
1. `applyDirectDelivery` (AO:11124):
   a. If the message is a tracked mention: `recordMentionDeliveryServerDecision` runs a CAS `recorded → server_decided` and snapshots machine/launch/session. A null result → `dropped/agent_state_changed`.
   b. Start span `server.agent.delivery`.
   (Before a.: for a tracked mention with missing identity this returns `queued/replayable_inbox` without sending; `deliveryId` is the occurrence id for mentions, else a fresh UUID.)
   c. Make the message replayable before sending it. Local machine → `enqueueToLocalInboxIfStillActive` (re-reads the agent; `planLocalDeliveryGateAction` only allows `active` with a matching machine). Remote machine with Redis → `routeInboxDeliveryCrossReplica` (pub/sub), and on failure fall back to the local inbox. The comment explains the order: "The daemon can ack immediately from ws.send(); if the inbox does not exist yet, that ack is lost". Transient messages (e.g. reminder fires, which have no `seq`) skip the inbox entirely; they could never be acked out of it. If the enqueue is refused (agent no longer active on that machine) → `dropped/agent_state_changed` and nothing is sent.
   d. `sendAgentDeliveryWithAckRetry(machineId, {type:"agent:deliver", agentId, message, seq, deliveryId, traceparent, mentionDelivery})` (AO:9222) runs without waiting. It records `pendingAgentDeliveryAcks[key]`, keyed by `delivery:{id}` or `seq:{agent}:{seq}`.
   e. `sendToMachine` (AO:8411) → `planSendToMachineAction`: `send-locally` if a socket is open on this replica. Otherwise `reroute-then-warn` via Redis (`routeMachineCommandCrossReplica`), or `warn-offline`.
2. Daemon (`packages/daemon/src/core.ts:3662`):
   - validates the mention identity; a mismatch sends `agent:delivery:terminal_error`
   - if the agent is still starting, buffers the message in `coreStartPendingDeliveries`
   - otherwise calls `agentManager.deliverMessage`
   - on acceptance, sends `agent:deliver:ack {agentId, seq, deliveryId}`
   - tracked mentions send `agent:delivery:transition` for each stage (received, pending, drained) and ack only via `onMentionAck`.
3. Server `case "agent:deliver:ack"` (AO:7522):
   - validates that the agent belongs to the machine
   - for mentions: a `machineId` mismatch ignores the ack; a `launchId`/`sessionId` that differs from the agent's current identity terminalizes the row as `IDENTITY_DRIFT` (`recordMentionDeliveryIdentityDriftForIdentity`) and clears the retry; otherwise `recordMentionDeliveryAck` runs a guarded UPDATE that requires `daemon_drained_at` to be set and the identity snapshot to match. If that update matches no row, the ack is rejected and neither the retry entry nor the inbox is cleared.
   - `clearPendingAgentDeliveryAck` clears the retry entry, and `acknowledgeDeliveredMessages(agentId, [msg.seq])` → `partitionAcknowledgedMessages` (AO:1213) removes the message. The socket ack path passes only the seq; removal by `message_id` applies to the HTTP receive-ack path, which can send `messageIds`.
   - if something was removed, `recordDeliveryAckTurnActive` marks the agent as working. The ack is treated as evidence that a turn started.
4. Retry: the timer fires after 5 s (`AGENT_DELIVERY_ACK_TIMEOUT_MS`, AO:2104) → `retryPendingAgentDelivery` (AO:9094):
   - a parked entry ignores `ack_timeout` ticks; only `ready_reconcile` or `register` wakes it
   - after 24 attempts (about 2 minutes of unparked retrying) → give up (trace `gave_up/ack_retry_exhausted`). Giving up only deletes the retry entry; the message stays in the replayable inbox, so a later receive or reconnect can still pick it up. Parked time does not count against the 24 attempts (`attempts` increments only on an actual resend).
   - `loadPendingAgentDeliveryRetryGate` re-checks the agent row, the machine match, the mention identity, and `planWakeAction === deliver-directly`; any other answer drops the retry
   - machine offline → park the entry (timer cleared, `parked=true`)
   - otherwise resend
   - if the resend was routed to another replica, delete the local entry, because the owner replica now tracks it.
5. Replay on reconnect: daemon → `ready` (AO:6314). After reconcile the server:
   - calls `retryPendingAgentDeliveriesForMachine(machineId,"ready_reconcile")`
   - calls `retryPendingAgentStartsForMachine`
   - calls `recoverDurableMentionDeliveriesForMachine`, which lists occurrences in `server_decided | daemon_received | daemon_pending` for that machine and re-sends each one with the same `occurrenceId` as `deliveryId`.

### Flow D — Pull path (agent's CLI long-poll)
1. Agent runtime / CLI → `GET /agent/:id/receive` (routes/internal.ts:2573, scope `message:read`). If the machine's socket lives on another Fly instance, the route answers `307` with a `fly-replay` header (:2618).
2. `AO.receiveMessages(agentId, block, timeoutMs, signal)` (AO:11512). If the runtime-profile gate is on, it returns `[]` and applies `reduceRuntimeProfileControlLifecycle`. Otherwise `planReceiveAction` returns `return-buffered`, `return-empty`, or `install-waiter`.
3. `installReceiveWaiter`: only one waiter per agent; a new one supersedes the old. It times out with `[]`. The next `applyLocalInboxEnqueue` resolves it (AO:~11482).
4. The route filters out messages the agent can no longer access (`discardUndeliverableMessages`), refreshes task snapshots, renders permalinks, and returns JSON. Receiving does not remove anything from the inbox. Only an ack does.
5. `POST /agent/:id/receive-ack {seqs, messageIds}` (routes/internal.ts:2711) → `acknowledgeDeliveredMessages`. For daemons without `model_seen_boundary` capability it also moves a legacy read checkpoint (`markAgentLegacyAckCheckpoint`).

### Flow E — Daemon connects / reconnects (`ready` reconcile)
1. Daemon → `ready {runtimes, runningAgents[], daemonVersion, capabilities, ...}` (AO:6314).
2. Store version and capabilities on the connection, mirror them to a Redis hash (`setMachineMeta`), and persist runtimes with `enqueueCapabilitiesPersist` (retried with backoff).
3. For each agent assigned to the machine (`loadAgentsForReadyReconcile`), run `planReadyReconcileAction({status, running, resetMode})` (AO:1512):
   - running and (`stopped` or reset) → `force-stop-and-stay-offline` (sends `agent:stop` best-effort)
   - running → `mark-active-online` (this includes an agent the DB has as `inactive`: a running process wins over the row)
   - note: a daemon status of `sleeping` is normalized to `active` (`normalizeDaemonAgentStatus`, AO:1555)
   - not running and `active`, with no reset → `mark-wakeable-not-running`. The DB stays `active`; only `runtimeState=not_running` changes.
   - not running and `active` during a reset → `mark-inactive-offline`
   - not running and not `active` → `stay-offline`
   - an invalid persisted status fails closed and is treated as `inactive`.
4. Each action → `reduceReadyReconcileLifecycle` (RED:728) → writer. Activity dedupe keys give one Activity Log row per restart window.
5. For `mark-wakeable-not-running`, `maybeWakePendingInboxAfterReady` (AO:4384) looks at the first queued inbox message. If `planWakeAction` says `attempt-wake`, it starts the agent with that message and removes it from the inbox, because messages embedded in a start produce no deliver-ack.
6. Recover mentions, push reminder snapshots, and retry pending deliveries and starts (see Flow C.5).

### Flow F — Machine disconnect
1. Socket close, or a missed heartbeat (`MACHINE_HEARTBEAT_TIMEOUT_MS = 60_000`, cause `heartbeat_timeout`, AO:3930) → `handleMachineDisconnect` (AO:5594). Stale-socket and duplicate closes are ignored. `clearMachineConnection` runs, then a 2000 ms timer is scheduled (`MACHINE_DISCONNECT_PROJECTION_GRACE_MS`, AO:547).
2. While the timer is pending, `getMachineStatus` still reports `online` (AO:5486). A quick reconnect cancels the timer, so users do not see an "offline" flicker.
3. When the timer fires, `applyMachineDisconnectProjection` → `reduceMachineDisconnectLifecycle` (RED:1039). The DB status is not changed (`machine_disconnect_preserves_agent_status`). Wake eligibility becomes false with reason `machine_unreachable`. Live activity becomes `offline / "Machine disconnected"`. A graceful shutdown uses `reduceMachineShutdownLifecycle` instead ("Computer stopped").
4. The cache's `runtimeState` becomes `interrupted`, **not** `not_running` (RED:1043). Because `planWakeAction` only wakes on `not_running`, a new message for an `active` agent on a disconnected machine takes the `deliver-directly` path: it goes into the replayable inbox, the send fails or is rerouted, and the ack-retry entry parks until `ready`. On `ready`, reconcile then flips missing agents to `not_running`, and `maybeWakePendingInboxAfterReady` starts them with the first queued message.

### Flow G — Stop and reset
1. `stopAgent(agentId, reason)` (AO:10009) → `planStopAction`: `manual` → `persist-stopped`, otherwise `persist-inactive` (AO:1181).
2. `reduceStopLifecycle` (RED:1125) side effects: `clearInbox` (queued, un-acked messages are discarded; they survive only as unread rows in Postgres), `clearLaunchGuard`, `releaseWakeLock`, send `agent:stop` and wait for it (only if the agent has a machine), cache `runtimeState=not_running`, DB write via the direct writer, wake eligibility false only for manual stop. Live activity is `offline / "Agent stopped by user"` for manual stops and `offline / "Stopped"` otherwise, and it is emitted only when the stop was manual or the stop send failed (`emitWhen: "manual_or_stop_send_failed"`). Afterwards the orchestrator clears pending delivery acks and pending starts for the agent.
3. `resetAgent(agentId, mode)` (AO:10132) sets `resetInProgress[agentId]=mode`. That value makes `planWakeAction` return `suppress-reset` and makes daemon signals without a launch id be ignored. `planResetActions` (AO:1538) always returns `stop-internal`; it adds `clear-session` for session/full, `reset-workspace` for full with a machine, and `restart` unless told not to. `applyResetPlan` runs them in order, and the `finally` block clears the reset flag.

### Flow H — Operator redrive of a stuck mention
`POST` redrive → `AO.redriveMentionDelivery(messageId, agentId, expectedVersion)` (AO:11066). Steps:
- the row is evaluated; if it is already ACKED or TERMINAL, return that
- if the live identity is missing → `IDENTITY_UNKNOWN`
- if the snapshot differs from the live identity → `IDENTITY_DRIFT` (the row is terminalized)
- `claimMentionDeliveryRedrive` is a CAS on `version`, allowed only while `redrive_count=0` → `CAS_MISMATCH` otherwise
- on success, enqueue and send → `REDRIVE_QUEUED`.

`planMentionRedriveHttpStatus` (AO:1129) maps the result to 202 / 404 / 409. The type is derived from the method's return type (`Awaited<ReturnType<...>>`), so the mapping cannot miss a verdict.

## 5. State machines

**A. `agents.status` (persisted, 3 states).**
- `inactive` → `active`: `startAgent` (manual, wake, resume) through the reducer's direct writer. A daemon `agent:status active` or `agent:session` can also do it, through the signal writer.
- `active` → `inactive`: internal stop, reset, daemon reports inactive, or ready-reconcile `mark-inactive-offline` during a reset. (Ready-reconcile of an `active` agent that is simply missing from the daemon does **not** do this: it keeps `active` and sets `runtimeState=not_running`.)
- any state → `stopped`: manual stop only.
- `stopped` → `active`: explicit start only. `updateAgentStatusFromSignal` refuses to overwrite `stopped` at the SQL level (`ne(agents.status,"stopped")`, agentService.ts:697). `updateAgentStatus` refuses `stopped → inactive` (:640). Wake and status signals leave `stopped` alone (`suppress-stopped`, `ignore-and-release-wake-lock`).
- Machine disconnect and shutdown do **not** change status.

**B. Runtime axis `runtimeState` (in memory, RED:128).**
- `not_running` → `starting` (start) → `running_idle` (session or status active, or ready-reconcile running)
- `working` / `thinking` come from activity
- → `interrupted` (disconnect, or reset-window ready) → `not_running` (stop, shutdown, `mark-wakeable-not-running`).
- The key rule: an agent that is `active` but `not_running` gets `attempt-wake` rather than `deliver-directly` (RED:233). Every other runtime value, including `interrupted`, `crashed`, `stalled` and `unknown`, gets `deliver-directly` when the DB says `active`.
- The axis is a per-replica cache. A missing entry is inferred from the DB (`active → running_idle`, else `not_running`; RED `inferRuntimeState`).

**C. Wake plan decision table (RED:223).** The input is (controlGate, dbStatus, resetMode, runtimeState).
- migration gate → `suppress-control-gate`
- `inactive` → reset ? `suppress-reset` : `attempt-wake`
- `stopped` → `suppress-stopped`
- `active` + `not_running` → reset ? `suppress-reset` : `attempt-wake`
- else → `deliver-directly`

**D. Visible activity (`AgentActivityKind`).** The values are `online`, `thinking`, `working`, `error`, `offline`. The UI labels are Starting, Idle, Thinking, Working, Disconnected, Crashed, Stopped, Offline (manual/agent-knowledge/agent-status.md). A 90 s stale sweep (`ACTIVITY_STALE_SEC`, AO:2088) sends the daemon an `agent:activity_probe`. If there is no reply within 5 s, it falls back to setting the agent `online` (the probe fields are described at AO:2034).

**E. Arbitration kernel (RED:1363).** It resolves conflicting activity signals. The projection priority is `unknown(0) < offline < online < idle < working < thinking < stopping < error(7)` (RED:1304). The verdicts are:
- stale launch generation → `preserve`, or `resolve_starting` when a Starting spinner is showing
- `control` class → `replace`, but it does not advance the freshness clock
- `synthetic`, `replayed`, `diagnostic` classes → `preserve`
- `observed`, newer → `replace`; older → `preserve`; same timestamp → the higher priority wins.

**F. Mention delivery occurrence (`mention_delivery_occurrences.state`).**
- Seven states: `recorded`, `server_decided`, `daemon_received`, `daemon_pending`, `daemon_drained`, `acked`, `terminal_error`.
- Immediate path: `recorded` → `server_decided` (server decision, payload + identity snapshot) → `daemon_received` → `daemon_drained` (`delivery_path=immediate`) → `acked`.
- Busy path: the same, with an extra `daemon_pending` hop after `daemon_received` when the agent was mid-turn (`delivery_path=busy`; a coalesced pending increments `pending_coalesced_count`). `daemon_pending` is optional, not part of every run.
- `acked` requires `daemon_drained_at`.
- `terminal_error` can be reached from any non-terminal state. The codes are `IDENTITY_UNKNOWN`, `IDENTITY_DRIFT`, `QUOTA_LIMITED`, `DELIVERY_REJECTED`, `UNSUPPORTED_DELIVERY_PATH`, `INSTRUMENT_FAILED`. `INSTRUMENT_FAILED` comes only from `recorded` (the abandon hotfix, Flow A 5h).
- How the guards actually work (corrected): only two writes are guarded on the *current state*: `recorded → server_decided` (`WHERE state='recorded' AND server_decided_at IS NULL`) and the abandon (`WHERE state='recorded' ...`). Daemon hop writes (`recordMentionDeliveryDaemonTransition`) are guarded on the identity snapshot (occurrence, agent, machine, launch, session) plus `acked_at IS NULL AND terminal_error_at IS NULL`. Their forward-only behavior comes from a SQL `CASE` that only advances from earlier states, with `coalesce()` so hop timestamps are written once. Every write bumps `version`, but only the redrive claim is a true compare-and-swap on `version`.
- Recovery listing differs by trigger: per-machine recovery on `ready` lists `server_decided | daemon_received | daemon_pending` (:364; a `recorded` row has no machine snapshot yet); per-agent recovery on `agent:session` also includes `recorded` (:377). Note that `daemon_drained` is never re-sent: the daemon already handed it to the runtime and only the ack is missing.
- `recordMentionDeliveryServerDecision` is idempotent: if the row is already decided with the same identity and message it returns the row, so a recovery path can re-send with the same occurrence id. It returns null for `acked`/`terminal_error` (the fixed double-delivery bug in §7).

**G. Pending ack entry (in memory).** `tracked(attempts=1)`, then each 5 s timeout leads to one of four outcomes:
- the gate fails → dropped
- the machine is offline → `parked`; `ready` or `register` unparks it and resends
- the resend was routed to another replica → handed off and deleted locally
- 24 attempts reached → `gave_up`.
An ack deletes the entry.

**H. Launch guard acceptance (AO:1721).** In `guarded` mode with an expected launch id, a signal without a `launchId` is dropped (`ignore-legacy-for-guarded`). A signal with a different id is dropped (`ignore-stale-launch`). Everything else is accepted. This stops a dying old process from overwriting state for the new launch.

## 6. Design patterns worth stealing

1. **Pure planner + effectful applier.**
   - Pattern: every branch decision is a synchronous function `planX(input) → "string-action"`. The input is a small struct of booleans or enums. A separate `applyXAction(context)` method does the I/O.
   - How Raft does it: `planWakeAction`/`applyWakeAction`, `planSendToMachineAction`/`applySendToMachineAction` (AO:8411–8460), `planLocalDeliveryGateAction`/`applyLocalDeliveryGateAction`, `planResetActions`/`applyResetPlan`, `planRoutedOwnershipAction`/`applyRoutedOwnershipAction`. Each planner has its own test file of one-line asserts (e.g. `agentOrchestrator.stopPlan.test.ts` is 12 lines; `wakePlan.test.ts` enumerates the table).
   - Why it matters for you: in Temporal, this maps to deterministic workflow code (the planner) and activities (the appliers). The decision tables can be unit-tested without Temporal's test harness.
2. **State + event → projection plan → single writer.**
   - How Raft does it: `reduce*Lifecycle` returns `{dbStatus, wakeEligibility, liveActivity, activityLog, sideEffects}` as data (RED:110). `applyAgentLifecycleProjectionPlan` is the only code that performs those effects (writer header comment). The file headers of the reducer, events module, and writer each say what the file must *not* do.
   - Why it matters: one place decides "Stopped vs Interrupted vs Disconnected". Every other path produces an event. It is easy to add a new projection, such as a Postgres registry row or a Temporal signal.
3. **Separate intent from observed runtime.**
   - How Raft does it: `agents.status` stores intent (user wants it running, or stopped). `runtimeState` stores whether a process exists. `machineReachability` stores the connection. `mark-wakeable-not-running` keeps the agent `active` when the daemon restarts without it (RED:~800). The manual page states the same split for its readers: "status: active is a database-level identity flag".
   - Why it matters: a sleeping agent stays addressable, and the next message wakes it (lazy wake).
4. **Typed delivery receipts.**
   - How Raft does it: `deliverMessage` returns `queued|dropped` plus a closed set of reasons (AO:1412). Pub/sub publication does not count as `queued` when `requireQueueReceipt` is set (AO:~11200).
   - Why it matters: callers such as onboarding and notify buttons can tell "held behind a gate" apart from "agent stopped".
5. **Durability before delivery, plus a CAS ledger for high-value messages.**
   - How Raft does it: the mention occurrence row is written before fan-out (messageService.ts:9197). Every state change is a guarded `UPDATE ... WHERE <guard> RETURNING` that bumps `version`: the server decision is guarded on `state='recorded'`, and daemon hops and the ack are guarded on the identity snapshot plus "not yet acked or terminal" (see §5F). Redrive is a CAS on `version` with `redrive_count=0`. Recovery re-sends with the same occurrence id as `deliveryId`, so the daemon can de-duplicate.
   - Why it matters: this is an outbox with idempotency keys, and it works per recipient. It maps onto a Postgres table that a Temporal activity reads when it starts.
6. **Generation fencing (launch id).**
   - How Raft does it: each start gets a UUID `launchId`. Status, session, and activity signals must echo it (AO:1721, 3812). Activity de-duplication is keyed by `agent:daemonInstance:launch:clientSeq` (AO:~9504).
   - Why it matters: this is the standard fix for zombie workers. In AgentCore, use the session or invocation id as the fence.
7. **Distributed wake lock with a short TTL.**
   - How Raft does it: Redis `SET NX EX 30` per agent (replicaRouter.ts:1131). When another replica holds the lock, the message goes to the local replayable inbox instead of being lost (AO:~11330).
   - Why it matters: it prevents two starts when N servers receive N messages at once. In Temporal, a workflow id per agent gives you the same guarantee without a lock.
8. **Grace windows before showing bad news.** A 2 s disconnect grace (AO:547) and a 90 s stale-activity probe with a 5 s fallback. These keep reconnect storms from flipping the UI.
9. **Ack is evidence of turn start.** A deliver-ack that removes an inbox entry records `turn_active` (AO:~7600). No separate "started working" message is needed.
10. **Deterministic-test seams.**
    - How Raft does it: the orchestrator constructor takes `(ReplicaStateStore, Clock, Tracer)`. Tests subclass it (`DeterministicAgentOrchestrator`, `agentOrchestrator.deterministic.test.ts:657`) with `InMemoryReplicaStateStore`, a `TestClock`, and no-op persistence overrides.
    - The reducer also has property tests with named invariants, for example I2 (only the reducer decides semantics), I3 (priority only for ties), I6 (liveness must be observed) (`agentLifecycleReducer.property.test.ts:1–20`).
    - A total map `LIFECYCLE_PLAN_SHADOW_CLASS satisfies Record<AgentLifecycleEventType, ...>` (RED:1601) makes compilation fail when someone adds an event type without classifying it.

## 7. Surprises / sharp edges

- **The replayable inbox exists only in memory, and only on one replica.** `agentInboxes` is a `Map` (AO:2027), capped at 1000 with `shift()` (oldest dropped silently). A server restart loses it. Durability comes from elsewhere: `messages.seq` in Postgres (resume catch-up via `getAgentResumeCatchupMessages` in `startAgent`), and for mentions the occurrence table. The `broadcastAndDeliver` comment says "Messages are already persisted in DB, so no durability risk" (messageService.ts:9072).
- **Pending-ack retries are also in memory.** A replica crash drops them. Only tracked mentions are rebuilt, from Postgres, when the machine sends `ready` or the agent sends `agent:session` (AO:8946–8960 explicitly skips mention entries: "must not become a second recovery authority").
- **`active` does not mean running.** The reducer can keep `dbStatus=active` with `runtimeState=not_running`. `planWakeAction` then wakes instead of delivering. Readers who look only at the DB column will draw the wrong conclusion.
- **A message that would need a wake during a reset is dropped (`reset_in_progress`), not queued.** The migration gate, by contrast, queues. (An agent that is `active` and not `not_running` during a reset still gets `deliver-directly`; only the wake branches are suppressed.) The message row stays in Postgres, but resume catch-up only re-sends bodies on a plain manual start with a surviving `sessionId`, so after a `session`/`full` reset it comes back only as an unread count or when the agent reads the channel (inferred from `applyWakeAction` at AO:11300 and `startAgent` at AO:9775).
- **Wake messages embedded in `agent:start` never produce a deliver-ack.** That is why `maybeWakePendingInboxAfterReady` removes the message from the inbox by hand (AO:~4435). Tracked mentions are deliberately *not* embedded, because the session identity does not exist yet.
- **Legacy daemons (< 0.30.1) have no launch guard.** Their signals are accepted without fencing (`launchGuardMode: "legacy"`).
- **The in-memory cache can be stale across replicas.** The DB-level guard in `updateAgentStatusFromSignal` exists for exactly this reason (comment at agentService.ts:656–667).
- **Migration is half done.** Many call sites carry `TODO(lifecycle-v2/...)`, and the legacy adapter still translates old daemon messages. The arbitration kernel runs in "shadow" mode and emits trace-only verdicts (`buildLifecycleShadowVerdictAttrs`), except where `isAgentActivityKernelArbitrationEnabled` is on (AO:12121).
- **The mention-ledger code comments record bugs that were fixed.** Example: `recordMentionDeliveryServerDecision` used to return an acked row as truthy, which allowed a double delivery. The comment says the bug was hidden by a daemon-side map "that never evicts — an unbounded leak" (mentionDeliveryOccurrenceService.ts:185–197).
- **`resetAllAgentStatuses` (agentService.ts:1174) has no non-test caller.** A server restart therefore does not mark all agents inactive. The server relies on the daemon's `ready` reconcile (verified by grep; the intent is inferred).
- **Receive is fly-replayed.** The long-poll must land on the replica that holds the machine socket, because the inbox is local to that replica (routes/internal.ts:2618).
- **The file is 12.5k lines.** The class holds sockets, inboxes, activity, reminders, runtime-profile migration, skills listing, and workspace files. The planners are small, but the class is not.

## Verification log

Checked against code (adversarial pass):
- `planWakeAction` (RED:223): decision table correct. Clarified that only `zen_migrating`/`runtime_profile_migration` produce `suppress-control-gate`, and that `interrupted`/`crashed`/`unknown` still give `deliver-directly`.
- `loadWakePlanInput`: added that machine reachability is hard-coded `unknown` (online/offline plays no part in the wake decision) and that a cache miss infers `running_idle` for `active`.
- `deliverMessage` (AO:10920): confirmed the order (agent → scope → target access → strict receipt → external → wake plan → gate → mention abandon → wake/direct). Added the `target_access_changed` receipt and the precise abandon semantics (`INSTRUMENT_FAILED`, lost CAS → dropped, a CAS error → queued with the row left `recorded`).
- `applyWakeAction`: added the mapping from start outcome to receipt, and when a tracked mention can still reach the wake branch.
- `startAgent`: corrected the resume catch-up claim. `unreadSummary` is loaded on every start that has a session; message bodies (`resumeMessages`) are loaded only on plain manual starts.
- `agent:session`: added `validateMachineAgentMessage`, the settling of the pending start ack, the plan outcomes, and the correct pure function name (`planLifecycleEventAcceptance`).
- `applyDirectDelivery`: confirmed that the inbox is filled before the send, and added the transient skip and the refusal → drop.
- `agent:deliver:ack`: added the identity-drift terminalization and the rejected durable ack (the retry stays). Corrected the claim that the socket ack clears by message_id; it passes only the seq.
- Retry loop (AO:9094): the constants are confirmed (5 s, 24 attempts, 1000-message cap with `shift()`). Added that parked entries ignore ticks and do not spend attempts, and that giving up leaves the message in the inbox.
- Ready reconcile (AO:1512): confirmed. Added that a running process on an `inactive` row becomes `mark-active-online`, and the `sleeping`→`active` normalization.
- Disconnect: added the 60 s heartbeat trigger, and the cache runtime state `interrupted`, which leads to `deliver-directly` plus a parked retry rather than a wake.
- Stop: added that the inbox is cleared, the non-manual label is "Stopped", and the `emitWhen` condition.
- Mention ledger (schema :5698, service): **corrected an overstatement.** Transitions are not all state-guarded CAS. Only `recorded→server_decided` and the abandon check state; daemon hops are identity-guarded with a forward-only `CASE`; only redrive is a CAS on `version`. `daemon_pending` is an optional busy-path hop. The recovery lists differ between the machine and agent triggers. Added the missing columns (`pending_coalesced_count`, `last_redrive_at`) and the indexes.
- Confirmed as written: Redis `SET slock:agent:{id}:waking NX EX 30` (replicaRouter.ts:1134), the launch guard ≥0.30.1, the 2 s disconnect grace, the 90 s stale sweep with a 5 s probe, `resetAllAgentStatuses` having no non-test caller, fly-replay on receive, and the fire-and-forget fan-out in `broadcastAndDeliver`.
- Fixed line refs: messageService :9197/:7354, internal.ts :2573/:2707, AO redrive :11066.
