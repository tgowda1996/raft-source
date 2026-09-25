# 04 — Machine daemon, drivers and credential brokering

All paths are relative to `packages/daemon/src/` unless they start with `packages/`.

## 1. What it is

The daemon (`@botiverse/raft-daemon`, entry `index.ts` -> `DaemonCore` in `core.ts`) is a long-lived Node process on a user's machine (a "Computer"). It holds one outbound WebSocket to the Raft server (`<serverUrl>/daemon/connect`, `connection.ts:232`), authenticated with a machine API key. The server sends it commands such as `agent:start`, `agent:deliver` and `agent:stop`. The daemon turns them into real agent processes (Claude Code, Codex, Gemini CLI, Cursor, Copilot, OpenCode, Grok, Kimi, or the in-process Pi/"builtin" SDK) through a driver layer.

Agents never talk to the server directly with a real credential. Each launch gets a random local token for a loopback HTTP proxy inside the daemon, and the proxy attaches the real per-agent `sk_agent_*` key. The daemon also keeps a local inbox for each agent. It wakes the agent with a short "you have N unread" notice and serves the message bodies when the agent runs `raft message check`.

## 2. Key components

| Name | File path(s) | Responsibility |
|---|---|---|
| Entrypoint | `index.ts` | Parses CLI args, constructs `DaemonCore`, wires SIGTERM/SIGINT to `daemon.stop()`. |
| `DaemonCore` | `core.ts` (4479 lines) | Owns the connection. `handleMessage` (`core.ts:3630`) switches on every server->machine frame. Mints runner credentials (`core.ts:3337`), buffers deliveries during start, emits `ready` (`core.ts:4193`). |
| `DaemonConnection` | `connection.ts` | Raw `ws` WebSocket client (not Socket.IO). Bearer auth header, exponential reconnect from 1s to 30s max (`:152`, `:~405`), 70s inbound watchdog with a probe before a forced reconnect (`:78`). Coalesces unsent `agent:activity` per agent while disconnected and replays it on reconnect. |
| Machine lock | `machineLock.ts` | Directory lock keyed by a hash of the API key. Only one daemon per machine key and state dir (`DaemonMachineLockConflictError`). |
| `AgentProcessManager` | `agentProcessManager.ts` (8128 lines) | Per-agent state (`AgentProcess` interface `:530`), start queue, spawn, delivery routing, turn tracking, crash/stall recovery, activity broadcast and heartbeat. |
| Start queue | `agentStartCoordinator.ts`, `agentProcessManager.ts:2635-2845` | Admission control: at most 5 concurrent starts, at least 500ms between starts (`:211-212`). Can be overridden with `SLOCK_DAEMON_MAX_CONCURRENT_AGENT_STARTS` and `SLOCK_DAEMON_AGENT_START_INTERVAL_MS` (`:1279-1281`). |
| Lifecycle record | `agentLifecycleRecord.ts` | Derives one of `queued / starting / running / idle / cooldown / terminal` per agent from the manager's maps (`:20-65`). |
| Driver registry | `drivers/index.ts` | `getDriver(runtimeId)` factory map: `builtin, claude, codex, grok, antigravity (deprecated), copilot, cursor, gemini, kimi, kimi-sdk, opencode, pi`. |
| Driver contract | `drivers/types.ts` | `RuntimeDriver` (spawn, parseLine, encodeStdinMessage, buildSystemPrompt, lifecycle contract), `RuntimeSession`, and the normalized `ParsedEvent` union. |
| Child-process session | `drivers/runtimeSession.ts` | Generic wrapper: spawn via driver, split stdout into lines, `driver.parseLine` -> `runtime_event`, write stdin via `driver.encodeStdinMessage`, SIGTERM then SIGKILL. |
| Claude driver | `drivers/claude.ts`, `drivers/claudeLaunch.ts`, `drivers/claudeEventNormalizer.ts` | `claude --output-format stream-json --input-format stream-json --permission-mode bypassPermissions --append-system-prompt-file ... [--resume <sessionId>]` (`claudeLaunch.ts:59-98`). |
| Codex driver | `drivers/codex.ts` | `codex app-server --listen stdio://` over JSON-RPC (`thread/start`, `thread/resume`, `turn/start`, `turn/steer`) (`codex.ts:632-639, 809-866`). |
| Pi / builtin driver | `drivers/pi.ts` | In-process SDK session (`@earendil-works/pi-coding-agent`), `transport: "sdk"` (`pi.ts:1070`). `BuiltInDriver extends PiDriver` (`:1920`). |
| CLI transport | `drivers/cliTransport.ts` | Per-launch dir, `raft`/`slock` wrapper scripts, proxy registration, spawn env scrubbing (`prepareCliTransport`, `:554`). |
| System prompt | `drivers/systemPrompt.ts`, snapshots in `drivers/__snapshots__/systemPrompt/` | One shared prompt (`common.md`, 281 lines) for every runtime. The per-runtime patches are empty (header only) for claude, codex and the others on Linux/macOS. Differences come from the platform (`builtin.windows.patch`, `pi.windows.patch`) and from config (`configured.patch` adds a "Current Runtime Context" block with agent/server/computer identity and an optional "Runtime Profile Control" release notice). |
| Runtime input formatters | `agentRuntimeInput.ts` | Builds the per-turn text: inbox notice, concrete messages, resume prompts (`formatInboxUpdateRuntimeInput`, `:198`). |
| Agent credential proxy | `agentCredentialProxy.ts` (1727 lines) | A single loopback HTTP server on `127.0.0.1:<random port>`, with a map from per-launch token to registration. Rewrites auth, enforces origin, serves the local inbox, applies the "freshness hold". |
| Inbox state machine | `agentInboxStateMachine.ts`, `agentVisibleDeliveryLedger.ts`, `agentInboxProjection.ts` | Decides whether a `send`/`task claim`/`task update` is forwarded or held because there are unseen messages. Tracks the per-target "model-seen" boundary. |
| Managed MCP proxy | `managedMcpRuntimeProxy.ts` | Optional local MCP endpoint written into `--mcp-config` (Claude) or `-c mcp_servers...` (Codex). |
| Workspaces | `workspaces.ts`, `raftHome.ts` | `~/.slock/agents/<agentId>/` (or `$RAFT_HOME`). Seeds `MEMORY.md` and `notes/` (`workspaces.ts:12-37`). |

## 3. Data model

**Wire protocol** (`packages/shared/src/index.ts`):
- `ServerToMachineMessage` (`:545`): `machine:context`, `agent:start` / `agent:start:wiki` (carries `AgentConfig`, `wakeMessage`, `resumeMessages`, `unreadSummary`, `resumePrompt`, `launchId`, `startDispatchId`, `traceparent`), `agent:stop`, `agent:deliver` (`message`, `seq`, `deliveryId`, `transient`, `mentionDelivery`), `agent:inbox:purge`, `agent:activity_probe`, workspace list/read, skills list, reminders, migration leases, `computer:restart|upgrade`, `ping`.
- `MachineToServerMessage` (`:775`): `ready`, `agent:status` (`active`/`inactive`), `agent:activity` (with `daemonInstanceId`, `launchId`, `clientSeq`, `producerFactId`, `isHeartbeat`), `agent:session`, `agent:session:invalidate`, `agent:start:ack` (`queueState`, `queueDepth`, `queueAgeMs`), `agent:deliver:ack`, `agent:delivery:transition`, `agent:delivery:terminal_error`, workspace and diagnostic results, `machine:shutdown`, `pong`.
- `AgentConfig` (`:1111`): `name`, `model`, `runtime`, `runtimeConfig`, `reasoningEffort`, `envVars`, `sessionId` (runtime-native session to resume), `serverUrl`, `authToken`, `agentCredentialKey` (sk_agent_*, "MUST NOT be present in the spawned runtime env", `:1126-1157`), `agentCredentialId`, `runtimeContext`.
- `AgentMessage` (`:100`): `channel_id/name/type`, `sender_*`, `content`, `seq`, `message_id`, `mentioned`, thread parent fields, attachments, task fields.
- `AGENT_ACTIVITIES = ["online","thinking","working","error","offline"]` (`:1309`).

**Postgres** (`packages/server/src/db/schema.ts`):
- `agents` (`:851`): `status` enum `active|inactive|stopped`, `sessionId`, `model`, `runtime`, `runtimeConfig`, `lastRuntimeError`, `executionMode byoc|cloud`, `envVars`, `machineId` (column `daemon_id`).
- `machines` (the table is named `daemons`, `:3377`): `apiKeyHash/Prefix/Fingerprint`, `runtimes`, `hostname`, `os`, `daemonVersion`, `computerVersion`, `lastHeartbeat`.
- `computers` (`:6136`): `apiKeyHash` (sk_computer_*), `machineId`, revocation fields.
- `agent_credentials` (`:5908`): per-agent keys with `scopes[]`, `revokedAt`, `lastUsedAt`. The daemon mints one per launch through `POST /internal/computer/runners/:agentId/credentials` and revokes it with `DELETE .../credentials/:credentialId` when the process closes (`core.ts:3294`, `agentProcessManager.ts:3974-3990`, `packages/server/src/middleware/routeAuthPolicy.ts:485-497`). The revoke is fire-and-forget (a failed DELETE is only traced, not retried). Restart snapshots strip the key (`buildRestartSafeConfig` -> `stripManagedRunnerCredential`, `agentProcessManager.ts:3838`), so every restart mints a fresh one.
- `mention_delivery_occurrences` (`:5698`): the **durable** delivery record for @mentions, one row per mention occurrence. It has its own state machine: `recorded -> server_decided -> daemon_received -> daemon_pending -> daemon_drained -> acked`, or `terminal_error` with a code (`IDENTITY_UNKNOWN | IDENTITY_DRIFT | QUOTA_LIMITED | DELIVERY_REJECTED | UNSUPPORTED_DELIVERY_PATH | INSTRUMENT_FAILED`, `packages/shared/src/index.ts:532-544`). It snapshots `machineId`, `launchId` and `sessionId`, and counts redrives. Ordinary (non-mention) deliveries have no durable row; see Flow D.

**Daemon in-memory state** (nothing is durable except files on disk):
- `AgentProcess` (`agentProcessManager.ts:530-581`): `runtime`, `driver`, `inbox: AgentMessage[]`, `sessionId`, `sessionReadyForDelivery`, `launchId`, `startup`, `notifications`, `activityHeartbeat`, `compaction`, `review`, `runtimeProgress`, `exit`, `gatedSteering`, `processInstanceId`.
- Credential proxy `registrations: Map<proxyToken, {serverUrl, apiKey, agentId, launchId, activeCapabilities, inboxCoordinator, ...}>` (`agentCredentialProxy.ts:30-42, 1672-1702`).

**On-disk layout** (`$RAFT_HOME`, default `~/.slock`, `raftHome.ts`):
- `agents/<agentId>/` is the workspace and the runtime cwd. `MEMORY.md` and `notes/` are seeded on first start.
- `cli-transport/<agentId>/<launchId>/` holds the `raft` and `slock` wrappers, the Claude system prompt file and the legacy `agent-token` (`cliTransport.ts:349-351`).
- `agent-proxy-tokens/<agentId>/<launchId>.token` is mode 0600 and holds the local proxy token (`cliTransport.ts:663-667`).
- `machines/` holds the machine lock and local traces.

## 4. Flows

### Flow A — Connect and register
1. `index.ts` -> `new DaemonCore(args).start()`. The machine lock is acquired (`machineLock.ts`).
2. `DaemonConnection.doConnect()` opens `ws(s)://<server>/daemon/connect` with `Authorization: Bearer <machine key>` (`connection.ts:232-236`). A 30s connect timeout applies.
3. Server -> daemon: `machine:context {machineId, serverId}` is the first frame. It is bound by `bindAuthenticatedMachineContext` (`core.ts:3636`).
4. On `open`, the connection first flushes pending session invalidations and coalesced activity (`connection.ts:~268-270`), then calls `DaemonCore.handleConnect()` (`core.ts:4311`).
5. Daemon -> server: `ready {capabilities, runtimes, runtimeVersions, runningAgents, hostname, os, daemonVersion, computerVersion}` (`core.ts:4219-4240`). Runtimes come from each driver's `probe()`.
6. The server re-sends deliveries that were never acked to this machine (`retryPendingAgentDeliveriesForMachine`, `packages/server/src/services/agentOrchestrator.ts:~8935`).

### Flow B — Heartbeat / liveness
1. The server sends `{type:"ping"}` on a timer. The daemon replies with `pong` (`core.ts:4098-4100`).
2. Server side: if `max(lastPong, lastIngressAt)` is older than 60s, it calls `ws.terminate()` and `handleMachineDisconnect(cause:"heartbeat_timeout")` (`agentOrchestrator.ts:2103, 3886-3930`).
3. Daemon side: after 70s with no inbound traffic, the daemon sends its own `ping` probe. If nothing arrives after that, it terminates the socket and reconnects with backoff (`connection.ts:~415-450`).
4. On disconnect, agents keep running: "Lost connection — agents continue running locally" (`core.ts:4466-4474`).
5. Per-agent heartbeat: while an agent's last activity is `working` or `thinking`, a 60s `setInterval` re-sends `agent:activity {isHeartbeat:true}` so the server's stale sweep does not flip it to online (`agentProcessManager.ts:413, 5756-5800`).

### Flow C — Launch one agent (`agent:start`)
1. Server (`AgentOrchestrator`, `agentOrchestrator.ts:9880`) -> WS -> daemon: `agent:start {agentId, config, wakeMessage?, launchId, startDispatchId}`.
2. `DaemonCore.handleAgentStartMessage` (`core.ts:3507`) dedupes by `startDispatchId`. A duplicate gets the same receipt with a "duplicate" label.
3. `startAgentFromMessage` (`core.ts:3550`) adds the id to `coreStartingAgentIds`. Any `agent:deliver` that arrives now is buffered in `coreStartPendingDeliveries` (`core.ts:3697-3704`).
4. `mintRunnerCredential` (`core.ts:3337`): daemon -> server HTTP `POST /internal/computer/runners/:agentId/credentials`, with up to 3 attempts for retryable errors. There is no fallback to a machine key: "hard-fail agent:start loudly" (`core.ts:3347-3392`). Note the order: the key is minted **before** the agent enters the start queue and before `agent:start:ack` is sent. A queued agent already holds a live key. The server retries an unacked `agent:start` every 5s, up to 24 attempts (`agentOrchestrator.ts:2106-2107, 8675-8681`), and the `startDispatchId` dedupe in step 2 absorbs those retries.
5. If no wake message was given, the first buffered non-transient delivery is promoted to be the wake message (`selectWakeDeliveryIndex`, `core.ts:1458`).
6. `AgentProcessManager.startAgent` (`agentProcessManager.ts:2635`) handles the case where the agent is already running, starting or queued by *rebinding* the start rather than spawning twice. Otherwise it enqueues. The daemon immediately sends `agent:start:ack {queueState, queueDepth, queueAgeMs}` (`core.ts:3601-3611`).
7. `pumpAgentStartQueue` (`:2714`) checks the concurrency limit and minimum interval, claims a slot and calls `startAgentNow` (`:2847`).
8. `startAgentNow`:
   a. `initializeAgentWorkspace(~/.slock/agents/<id>, MEMORY.md, notes/)` (`:2880-2885`).
   b. `driver = getDriver(config.runtime)`, then `enforceRuntimeLaunchVersion` (for example, Claude Code 2.1.59 is known bad, `drivers/claude.ts:64-69`).
   c. `standingPrompt = driver.buildSystemPrompt(config)`.
   d. Choose the first-turn `prompt`, by `promptSource`: `runtime_profile_control`, `resume_prompt`, `transient_wake_message`, `runtime_profile_control_message`, `wake_thread_context`, `wake_inbox_update`, `resume_catchup_inbox`, `starting_thread_context`, `starting_inbox_update`, `resume_unread_summary`, `resume_empty`, or `cold_start` (`:2986-3080`). Several of these put **message bodies** into the first turn: `wake_thread_context` / `starting_thread_context` render the thread-join context (parent message plus recent replies) when the agent was pulled into a thread, and `resume_catchup_inbox` renders the catch-up messages concretely (`formatConcreteMessagesRuntimeInput`). A transient wake becomes a system notice. Only the `*_inbox_update` paths are content-free.
   e. Drivers with `deferSpawnUntilMessage` (OpenCode) and nothing to do do not spawn. They report `active` / "Process idle" (`:3095-3120`).
   f. `runtime = driver.createSession?.(ctx) ?? createChildProcessRuntimeSession(driver, ctx)` (`:3149`).
9. `driver.spawn(ctx)` (Claude, `drivers/claude.ts:86-155`):
   a. `prepareCliTransport` (`cliTransport.ts:554`) calls `registerAgentCredentialProxy` and receives `{proxyUrl: http://127.0.0.1:<port>, proxyToken: sap_<32 random bytes>}` (`agentCredentialProxy.ts:1685`). It writes the token file (0600), writes the `raft`/`slock` wrappers that export `SLOCK_AGENT_PROXY_URL` and `SLOCK_AGENT_PROXY_TOKEN_FILE`, and builds `spawnEnv` with the wrapper dir prepended to `PATH` (`:834`). It then deletes every raw credential variable from that env (`:836-853`).
   b. It writes the system prompt to a file and passes `--append-system-prompt-file`.
   c. `spawn("claude", args, {cwd: workspace, stdio: pipe})`, then writes the first `{"type":"user",...}` JSON line to stdin.
10. As stdout lines arrive, `driver.parseLine` produces `ParsedEvent`s and `handleParsedEvent` (`agentProcessManager.ts:7022`) handles them:
    - `session_init` -> daemon -> server `agent:session {sessionId}`. The server stores it in `agents.session_id` for resume (inferred from the server consuming `agent:session`).
    - `thinking`, `text`, `tool_call` and `tool_output` go to `broadcastActivity`, which sends `agent:activity {activity, detail, entries[] trajectory, clientSeq}` (`:5698`).
    - `turn_end` marks the session ready, flushes queued inbox via stdin if needed, sends `online/Idle` and `agent:session` again (`:7292-7357`).

### Flow D — Message reaches a running agent (`agent:deliver`)
1. Server -> daemon `agent:deliver {agentId, message, seq, deliveryId}`. The server holds a pending-ack entry and retries after 5s (`agentOrchestrator.ts:2104, 8878-8915`). Details that matter:
   - The pending-ack map is **in server memory** (`pendingAgentDeliveryAcks`). After 24 attempts the server gives up with `ack_retry_exhausted` (`AGENT_DELIVERY_ACK_MAX_ATTEMPTS`, `:2105, 9100-9123`).
   - While the machine is offline the entry is **parked** (`parkPendingAgentDeliveryAck`, `:9026-9036`), so timer retries stop. It is replayed when the machine re-registers or sends `ready` (`retryPendingAgentDeliveriesForMachine`, `:8946`).
   - @mention deliveries skip that in-memory replay. They are rebuilt from the durable `mention_delivery_occurrences` table on `ready` (`recoverDurableMentionDeliveriesForMachine`, `:8958`).
2. `DaemonCore.handleMessage` (`core.ts:3662`):
   - For a mention delivery, the daemon first checks that `machineId`, `occurrenceId` and `messageId` match. A mismatch is sent back as `agent:delivery:terminal_error` (`IDENTITY_DRIFT` / `INSTRUMENT_FAILED`, `core.ts:3681-3695`).
   - If the agent is starting, the delivery is buffered in `coreStartPendingDeliveries` and **not acked yet**. When the start finishes, the delivery promoted to wake message is acked, and the rest are replayed through `handleMessage` (`core.ts:3614-3621`).
   - Otherwise it calls `agentManager.deliverMessage`. Once the delivery is accepted it sends `agent:deliver:ack {seq, deliveryId}` (`core.ts:3727-3733`).
   - Mention deliveries are **not** acked on accept. They report `agent:delivery:transition` (`daemon_pending`, then `daemon_drained`), and the ack is sent only when the message is actually written to the runtime's stdin (`completeTrackedMentionDelivery`, `agentProcessManager.ts:4229-4243, 4688-4693, 7329`). "Acked" here means the model received it, not just the daemon.
3. `deliverMessage` (`agentProcessManager.ts:4252`) routes it:
   - Already seen by the model (seq at or below the boundary) -> dropped as a duplicate (`:4270-4285`).
   - No process and a start queued or running -> `startingInboxes.bufferDuringStart`. A *transient* delivery in this state is dropped (`transient_dropped_during_start`).
   - No process but an `idle` restart snapshot -> **auto-restart** with this message as the wake. This path uses the saved `sessionId`, so the runtime resumes (`:4380-4470`).
   - No process and `cooldown` (spawn-fail backoff) -> buffer the message and do not spawn.
   - Process idle and `supportsStdinNotification` -> push to `ap.inbox`, then write a **content-free** `[Raft inbox notice: ... pending: N message ...]` to stdin to start a new turn (`:4617-4660`, `agentRuntimeInput.ts:198-212`).
   - Process busy -> push to `ap.inbox`, bump `notifications`, and schedule a stdin notice after 3s (`STDIN_NOTIFICATION_INITIAL_DELAY_MS`, `:414`). The notice is held while the runtime is compacting (unless the driver accepts stdin then) or reviewing. If the busy process has been marked stalled (Flow F step 3), this is the point where the daemon SIGTERMs it and restarts with the queued message (`recoverStaleProcessForQueuedMessageIfNeeded`, `:4714, 6945`).
   - Exception to "content-free": when a delivery pulls the agent into a thread, `deliverInboxUpdateViaStdin` renders the thread-join context (message bodies) and adds the inbox notice only for the rest (`:7849-7869`).
   - Per-turn drivers (gemini, cursor, copilot, opencode: `supportsStdinNotification = false`) have no stdin channel. A wake spawns a new process whose first prompt is the inbox update.
4. The agent's model decides to read. It runs `raft message check`. The wrapper runs the bundled CLI, which calls `GET <proxyUrl>/internal/agent-api/events` with `Bearer sap_...`.
5. The credential proxy (`agentCredentialProxy.ts:574-587`) **answers from the daemon's local inbox first** (`localAgentApiEventsResponse`, `:1357`), records the ids as consumed, and only forwards to the server when the local inbox is empty.

### Flow E — Agent writes back (`raft message send`)
1. The agent runs `raft message send --target "#chan" <<'RAFTMSG' ...`. The CLI calls `POST <proxyUrl>/internal/agent-api/send`.
2. The proxy validates the local token (401 if unknown). It resolves the URL against the registered server and rejects any other origin with 403 `agent_proxy_origin_mismatch` (`:467-475`). It strips the caller's `Authorization` and `Host` headers, then sets `Authorization: Bearer <sk_agent>`, `X-Agent-Id`, `X-Slock-Agent-Active-Capabilities`, and a fresh `traceparent` (`:476-500`).
3. **Freshness preflight** (`prepareAgentApiSideEffectForward`, `:1480`). It covers three side effects: `send`, `task_claim` (`/tasks/claim`) and `task_update` (`/tasks/update-status`). If the target has pending messages the model has not seen, `planAgentInboxSideEffect` (`agentInboxStateMachine.ts:118`) returns a local `{state:"held", decision:"local_hold", reason:"newer_messages_available", heldMessages[latest 3], newMessageCount, omittedMessageCount, seenUpToSeq}` response. The request never reaches the server (`packages/shared/src/apmHeldFreshness.ts:15-31`).
   - The hold also **consumes** the shown messages: it moves the model-seen boundary up to `seenUpToSeq`. If the agent re-issues the same send, it is forwarded. So the hold is "read this first", not a lock.
   - `continueAnyway:true` bypasses the hold, and only for `send` (`agentCredentialProxy.ts:1513`).
   - Other decisions: `syncing_hold` (`agentInboxStateMachine.ts:409`), and a `withheld` mode for "blind seats" that returns only a count with no message bodies.
   - If there is no local pending and no known boundary, the proxy first loads recent target messages from the server to build a boundary (`loadRecentTargetMessages`, `:1517-1521`).
4. Otherwise the request is forwarded with `seenUpToSeq` through `daemonFetch`. JSON responses to send/events/history are buffered so the proxy can mark visible messages consumed (`:1547-1555`).
   - **The server enforces the same gate again.** `/internal/agent-api/send` requires `seenUpToSeq`, runs its own freshness check against the channel, and can itself return a held envelope with `heldMessages` (`packages/server/src/routes/internalAgentApi.ts:3167-3180, 3358, 3761-3962`). The daemon hold is a fast local path; the server check is authoritative, which also covers messages the daemon never received.
5. The server persists the message and fans it out. That part is covered by other notes.

### Flow F — Stop, exit and recovery
1. Server -> `agent:stop` -> `stopAgent` (`agentProcessManager.ts:4067`): cancel any queued start, `cleanupLaunchProxies` (all proxy tokens for the agent are unregistered, `launchProxyCleanup.ts`), `revokeManagedRunnerCredential` (HTTP DELETE), SIGTERM (then SIGKILL after 5s if `wait`), `agent:status inactive`, activity `offline/Stopped`.
2. The runtime `close` handler (`:3359`) classifies the exit:
   - Resume failure (the native session is missing, or the provider rejected replay) -> `agent:session:invalidate`, then an automatic cold start without `sessionId` (`:3408-3450`).
   - Clean exit -> the first leftover non-transient inbox message becomes the wake of an immediate restart (same `sessionId`, so it resumes). The rest are buffered for that start. With no leftovers, the agent becomes `idle` with a restart snapshot and is woken by the next delivery (`:3446-3525`).
   - Unclean exit, branch by branch (`:3526-3580`). The earlier version of these notes said a crash calls `recordSpawnFailure`. It does not.
     - *Recoverable runtime error*: keep a restart snapshot (the agent stays wakeable, lifecycle `idle`). If inbox messages are waiting, arm a delayed restart with the runtime-error backoff: 10s base, doubling, capped at 5 min (`armRuntimeErrorProcessRestart`, `:417-418, 1447`).
     - *Provider stream failure*: keep the snapshot, status `active`, wakeable.
     - *Startup timeout*: cache a retry config.
     - *Non-recoverable crash*: delete the snapshot, send `agent:status inactive` and activity `offline/Crashed`. The agent leaves the lifecycle map entirely and only a new `agent:start` revives it.
   - A **sticky terminal failure** (action-required auth, unsupported model, input too large, `codex_zero_evidence_turn_completed`; `:890-897`) goes through `cleanupTerminalRuntimeFailure`, which sets the `terminal` lifecycle record (`:3773-3795`). Leftover inbox messages are kept in the starting inbox.
   - A **runtime-error fingerprint fence** stops auto-retries after 3 identical runtime errors (`RUNTIME_ERROR_FINGERPRINT_FENCE_THRESHOLD`, `:419, 1611`).
   - `recordSpawnFailure` (`:1404-1426`) is only for a failed **restart attempt**: `startAgent` throws during an auto-restart from idle or a queued continuation. It covers `spawn_error` (1s base, doubling, capped at 30s) and `runner_credential_mint` (60s base, capped at 10 min, `:429-436`). That failure moves the agent into `cooldown`.
3. Stall watchdog: the check runs on each 60s heartbeat tick, only while the last activity is `working` or `thinking`. If there has been no runtime progress for 15 minutes (`RUNTIME_PROGRESS_STALE_MS`, `:442`):
   - PID alive: the daemon only records `silent_alive` / `stall.suppressed_alive` traces and does nothing else.
   - PID not alive or unknown: the agent is marked stale, the daemon broadcasts `error` activity with detailKind `runtime_stalled`, and the activity heartbeat stops (`:6883-6935`).
   - No SIGTERM is sent at this point. The recovery termination happens only when the **next message** arrives for the stale process (`recoverStaleProcessForQueuedMessageIfNeeded`, `:4714, 6945`). The close handler then restarts with that message (`restart_stall`).
   - Startup timeout is 2 minutes (`:444`). A stalled-recovery SIGTERM that hangs is surfaced after 10s (`:446`).

## 5. State machines

**Agent lifecycle record** (`agentLifecycleRecord.ts:20-65`, derivation `:300-397`):
- (absent) -> `queued` on `startAgent` enqueue.
- `queued` -> `starting` on `pumpAgentStartQueue` claiming a slot.
- `starting` -> `running` when the runtime is registered in `this.agents`.
- `running` -> `idle` after a clean exit, keeping a `restartSnapshot`. `idle` -> `queued` on the next `agent:deliver` (auto-restart).
- `idle` -> `cooldown` when an auto-restart attempt fails (`startAgent` throws: `spawn_error` or `runner_credential_mint`) and `recordSpawnFailure` starts a backoff. A running process never goes straight to `cooldown`: `running` + `cooldown` is an asserted invariant violation (`agentLifecycleRecord.ts:334`). `cooldown` -> `idle` when the backoff timer expires, and a later delivery moves it to `queued`. Deliveries during cooldown are buffered, not spawned.
- `running` -> `idle` (snapshot kept) on a recoverable runtime error. With queued messages, a delayed restart follows (10s -> 5 min backoff).
- `running` -> (absent) on a non-recoverable crash. The snapshot is deleted and status is set `inactive`.
- `running` -> `terminal` on a sticky failure (auth/action-required, unsupported model, input too large). It leaves `terminal` only on an explicit stop/start. A user message can try one recovery turn for auth-class failures (`:4555-4570`).
- Resume failure: `running` -> `queued` directly (cold start without `sessionId`, `pendingSpawnCause = restart_crash`).
- Any state -> (absent) on `agent:stop`.
- Derivation precedence when several facts exist: `terminal` > `cooldown` > `running` > `starting` > `queued` > `idle` (`agentLifecycleRecord.ts:314-390`). A record is computed from separate maps, not stored as one enum.

**Per-process turn state** (fields of `AgentProcess`):
- `startup`: `waiting` -> `ready`.
- `exit`: `live` -> `exited{code,signal}`.
- `compaction` / `review`: `none` <-> `active{startedAt, watchdog}`. Compaction is marked stale after 5 minutes and review after 10 (`:438-440`).
- APM idle/busy (`isApmIdle`, `commitApmIdleState`): idle -> busy when stdin is written. Busy -> idle on `turn_end`, unless there is pending inbox, in which case the notice is re-delivered immediately.
- `sessionReadyForDelivery`: false for a fresh Claude session until the first `turn_end` (`liveSessionReadyAt = "turn_end"`, `drivers/claude.ts:60-62`). It is true immediately for `--resume`.

**Driver lifecycle contract** (`drivers/types.ts:11-24`):
- `persistent` with `stdin: "direct"` and `inFlightWake: "steer"` (claude, codex, grok, kimi, kimi-sdk, pi/builtin): the process stays alive and messages go to stdin. A wake during a turn *steers* the current turn. No shipped driver uses `stdin: "notification"` or `inFlightWake: "queue"`, although the type allows both.
- `per_turn` with `start: immediate`, `exit: natural` and `inFlightWake: spawn_new` (gemini, cursor, copilot): a new process per turn that exits naturally.
- `per_turn` with `defer_until_concrete_message`, `terminate_on_turn_end` and `inFlightWake: coalesce_into_pending` (opencode): wakes during a turn are coalesced, and the daemon SIGTERMs the process after each turn (`:7340-7350`).

**Mention delivery (durable, server + daemon)**: `recorded -> server_decided -> daemon_received -> daemon_pending -> daemon_drained -> acked`, or `terminal_error{code}` (`packages/server/src/db/schema.ts:5698-5730`). The daemon drives `daemon_pending` (buffered in the inbox), `daemon_drained` (written to runtime stdin) and the final ack. See Flow D.

**Freshness decision** (`agentInboxStateMachine.ts`): `bypass` (continueAnyway) | `forward` (no unseen pending, or model-seen boundary known) | `local_hold` (unseen pending: show the latest 3 and consume them) | `syncing_hold`. A context mode of `inline` or `withheld` controls whether bodies are shown.

**Server-visible activity**: `online | thinking | working | error | offline` plus a `detailKind` (for example `idle`, `stopped`, `runtime_crashed`, `freshness_hold`). Agent status is `active | inactive`.

**Machine connection**: connecting -> open (`ready` sent) -> disconnected -> reconnect after a delay of 1s, doubling up to 30s. A 401 with reason `legacy_machine_key_migrated` is terminal (`connection.ts:83-85`).

## 6. Design patterns worth stealing

1. **Loopback credential broker with a per-launch capability token.** One `127.0.0.1:0` HTTP server handles every launch (`agentCredentialProxy.ts:248, 295-316`). Each launch gets a `sap_` random token that maps to the real key in daemon memory. The process env and workspace never see `sk_agent_*` (`cliTransport.ts:836-853`). The proxy pins the origin and replaces the caller's Authorization header. Why it matters: a prompt-injected agent with shell access can only reach its own Raft server as itself, and killing the launch kills the token. *For Temporal/AgentCore:* an AgentCore sidecar or gateway can hold the Bedrock or registry credentials. The agent gets a short-lived, launch-scoped token that Temporal revokes in the workflow's cleanup activity.
2. **Per-launch credentials minted just in time and revoked on exit** (`core.ts:3337`, `agentProcessManager.ts:3974`). Credentials follow the process, not the machine. A failed mint is a hard fail, with no silent fallback to broader creds.
3. **Content-free wake plus pull-based read.** stdin gets only "[Raft inbox notice: N pending in #x]". Message bodies stay in a daemon-local inbox and are pulled with `raft message check` (`agentRuntimeInput.ts:198-212`). Why: the model chooses when to read, a busy turn is not flooded, duplicate notices are suppressed by fingerprint, and "read" becomes an observable event (model-seen boundary).
4. **Freshness hold on side effects.** Before a send, task claim or task status update, the proxy checks whether the target has unseen messages. If it does, it answers "held: newer messages" with up to 3 of them and marks them seen, so the next attempt goes through (`agentInboxStateMachine.ts:118`, `apmHeldFreshness.ts`). The server repeats the check using the `seenUpToSeq` the client sends (`internalAgentApi.ts:3761+`). This is optimistic concurrency control on conversation context: the "version" is the last seq the agent saw. It prevents agents replying to stale context in a multiplayer channel, which is the main multiplayer race condition. *For Temporal:* pass `seenUpToSeq` into the activity that posts, and have the registry reject the post with a "read these first" response if the channel has moved on.
5. **One tool surface for every runtime: a CLI on PATH.** Every driver gets the same `raft` wrapper and the same system prompt (the snapshot patches are empty). Communication is always `"slock_cli"` (`drivers/types.ts:26-29`). Adding a runtime means writing spawn, parseLine and encodeStdin. The chat semantics do not change.
6. **Normalized event stream (`ParsedEvent`)** (`drivers/types.ts:88-230`). Each runtime's stdout protocol is normalized into `session_init / thinking / text / tool_call / turn_end / error / compaction_*` plus telemetry sidecars. All UI, status and recovery logic is written once against this union.
7. **Start admission queue with rebind.** Concurrency cap plus minimum interval. A duplicate `agent:start` for a queued, starting or running agent *rebinds* its config/wake instead of spawning a second process (`agentProcessManager.ts:2635-2700`). Idempotency by `startDispatchId` with a replayed receipt (`core.ts:3507-3548`).
8. **Ack-after-accept with server-side retry, in two strengths.** For ordinary deliveries, the server keeps pending acks in memory, keyed by `deliveryId`. It retries after a 5s ack timeout, up to 24 attempts, parks the entry while the machine is offline, and replays it when the machine becomes `ready`. For @mentions, the server keeps a durable per-occurrence state row, and the daemon acks only once the message reaches the model's stdin (`daemon_drained`). The daemon dedupes by seq and model-seen boundary (`deliverMessage` entry gate). The result is at-least-once delivery with daemon-side dedupe. It is durable only for mentions; a server restart loses ordinary pending acks, and those rely on the agent's pull-based `raft message check` / unread summary to catch up.
9. **Activity dedupe identity `(daemonInstanceId, launchId, clientSeq)`**, with an explicit `isHeartbeat` flag (`packages/shared/src/index.ts:790-835`). The server can drop replays and out-of-order frames without guessing.
10. **Resume-or-fresh session recovery.** The native session id is stored on the server (`agent:session`). On a resume failure the daemon sends `agent:session:invalidate` and cold-starts automatically (`agentProcessManager.ts:3408-3450`). Workspace `MEMORY.md` is the durable memory across cold starts, and the system prompt tells the agent so.

## 7. Surprises / sharp edges

- The machine link is a **raw `ws` WebSocket**, not Socket.IO (`connection.ts:3, 232`). Socket.IO is only for web clients (inferred from the package list; not verified here).
- `agentProcessManager.ts` is 8128 lines and `agentProcessManager.codex.test.ts` is 11234 lines. Most behavior is specified by tests and trace names, not by small modules.
- The legacy name "Slock" is everywhere: env vars `SLOCK_*`, `~/.slock`, the `slock` wrapper next to `raft`, and the machine table named `daemons`.
- When there is no `agentCredentialKey` (legacy daemon or legacy-machine launch only), `prepareCliTransport` falls back to writing `config.authToken || daemonApiKey` into the workspace `agent-token` file (`cliTransport.ts:668-670`). That can be the machine key, readable by the agent. New daemons never reach this path because a failed mint hard-fails, and a successful managed launch deletes any leftover legacy token (`:650`).
- The proxy token file (`agent-proxy-tokens/<agent>/<launch>.token`) is written but I found no code that deletes it. Only the in-memory registration is removed (`launchProxyCleanup.ts`), so a stale file holds a dead token. Same-uid processes are contained by "proxy capability enforcement and launch cleanup, not by file permissions" (`packages/shared/src/index.ts:1143-1145`).
- Claude runs with `--dangerously-skip-permissions --permission-mode bypassPermissions` and has its native `CronCreate` / `ScheduleWakeup` / plan-mode tools disabled (`claudeLaunch.ts:11-18, 64-75`). Scheduling must go through `raft reminder`, so wakes are visible in Raft.
- **Agents survive daemon-server disconnects.** Activity produced while offline is coalesced to the latest frame per agent, not queued in full (`connection.ts` `pendingActivityByAgent`). A daemon restart deliberately loses in-flight activity ("no durable cross-process outbox", `packages/shared/src/index.ts:806-812`).
- Model text output is **not** delivered to anyone. The prompt says "text you produce outside a `raft` command is not delivered". Text only appears as trajectory in `agent:activity`.
- For a fresh Claude session, deliveries wait for the first `turn_end` before being written to stdin. Claude reports a session id before that session can be resumed (`drivers/claude.ts:58-62`).
- The concurrency limit caps *starts in flight* (5) and start rate (500ms). It does not limit the number of running agents per machine; I found no cap on that.
- The in-process SDK drivers (pi, kimi-sdk) run tools with the daemon's `process.env`, so the wrapper script inlines `SLOCK_AGENT_ID` and `SLOCK_SERVER_URL` itself (`cliTransport.ts:684-700`).
- The wrappers include a "launch forwarding guard": an older launch's wrapper re-execs the current launch's wrapper, so a stale PATH cannot use a dead proxy token (`cliTransport.ts:58-86, 214`).

## Verification log

Checked against the code (all confirmed unless marked **changed**):
- Raw `ws` at `/daemon/connect` with Bearer auth (`connection.ts:232-236`). The 70s watchdog, 30s connect timeout, 1s->30s backoff, `legacy_machine_key_migrated` terminal reason, and `pendingActivityByAgent` coalescing are all correct.
- Server heartbeat timeout of 60s and delivery/start ack timeout of 5s (`agentOrchestrator.ts:2103-2107`) are correct. **Changed:** added the 24-attempt cap, parking while offline, replay on register/ready, that the pending-ack map lives only in memory, and that `agent:start` has the same retry.
- Start queue limits of 5 and 500ms and their env overrides are correct. `startDispatchId` dedupe and the rebind on already running/starting/queued are correct. **Changed:** the runner key is minted before queueing and before the start ack.
- Mint gets 3 attempts and hard-fails. The `sap_` + 32 random bytes token, 127.0.0.1 bind, origin-mismatch 403, header rewrite, and local inbox answer to `/events` are correct. **Changed:** noted that the revoke is fire-and-forget, restart snapshots strip the key, and the legacy fallback can write the machine key into `agent-token`.
- **Changed (missing):** the durable `mention_delivery_occurrences` table and its state machine. Mention deliveries are acked only when drained to stdin, and the daemon checks their identity. Deliveries buffered during start are not acked until the start finishes.
- **Changed:** the claim that messages are "content-free" was too broad. Thread-join context, resume catch-up and transient/system notices put bodies into runtime input. Added the missing `promptSource` values.
- Freshness hold: **changed** to cover send, task_claim and task_update. The hold consumes the shown messages, `continueAnyway` applies to send only, `syncing_hold` and `withheld` mode exist, and the **server re-checks** using `seenUpToSeq` (`internalAgentApi.ts`).
- **Changed (was wrong):** a crash does not call `recordSpawnFailure`. The close handler branches by type: recoverable (idle, 10s->5min delayed restart), provider stream, startup timeout, non-recoverable (absent/inactive), and sticky terminal. Spawn-fail cooldown applies only to failed restart attempts: spawn_error from 1s to 30s, mint failure from 60s to 10min. Added the 3-strike fingerprint fence.
- **Changed (was wrong):** the stall watchdog does not SIGTERM on detection. It marks the agent stale, broadcasts `runtime_stalled` and stops the heartbeat. The SIGTERM happens only when the next message arrives. An alive PID suppresses detection entirely.
- Lifecycle state machine: **changed**. There is no `running->cooldown` transition (it is an asserted invariant). Cooldown is entered from a failed auto-restart and expires to idle. Added `running->absent` on a non-recoverable crash, `running->queued` on resume failure, and the derivation precedence.
- Driver lifecycle contracts are verified per driver. **Changed:** added `stdin:"direct"` and `inFlightWake:"steer"` for all persistent drivers, and opencode's `coalesce_into_pending`.
- System prompt: patches are header-only on Linux. **Changed:** noted the Windows variants and the `configured.patch` runtime context and profile-control section.
- Claude flags (`--dangerously-skip-permissions`, `bypassPermissions`, disallowed `CronCreate`/`ScheduleWakeup`, `--resume`) are correct. The Postgres table names (`agents`, `daemons`, `computers`, `agent_credentials`) and the agents status enum are correct.
- Not verified: the exact line numbers in `drivers/pi.ts`, the managed MCP proxy, the workspace seeding contents, and the launch forwarding guard.
