# 20 — End-to-end flows across the whole system (swimlane-ready)

Five flows, traced hop by hop through server, Postgres, Redis, daemon, agent process and CLI. Built from notes 01–10 plus a fresh code pass (new findings are marked **[new]**; things read from code but not tested are marked *(inferred)*).

Path prefixes: `S/` = `packages/server/src`, `D/` = `packages/daemon/src`, `C/` = `packages/cli/src`, `W/` = `packages/web/src`, `PC/` = `packages/computer/src`. `AO` = `S/services/agentOrchestrator.ts`, `MS` = `S/services/messageService.ts`, `IAA` = `S/routes/internalAgentApi.ts`, `RED` = `S/services/agentLifecycleReducer.ts`, `APM` = `D/agentProcessManager.ts`, `PROXY` = `D/agentCredentialProxy.ts`.

## Lanes and wire vocabulary

| Lane | What it is | Talks over |
|---|---|---|
| **Human/Web** | Browser SPA (`W/`), one Socket.IO socket per tab | HTTPS `/api/*` (JWT + `X-Server-Id`); Socket.IO events |
| **Server** | N identical Node replicas (`S/server.ts`). Each holds some daemon sockets in memory (`AO`) | Express, Socket.IO (Redis adapter), raw `ws` at `/daemon/connect` |
| **Postgres** | Source of truth (190 tables). Everything durable | SQL from server only |
| **Redis** | Pub/sub between replicas, Socket.IO adapter, leases/locks. Nothing here is the only copy of a business fact | from server only |
| **Daemon/Computer** | `DaemonCore` on the user's machine (inside the Computer runner process `PC/service.ts` `__run`), plus a loopback credential proxy | one outbound WebSocket per machine; HTTPS for credential mint; `127.0.0.1:<port>` for agents |
| **Agent process** | Claude Code / Codex / etc. spawned by the daemon, cwd `~/.slock/agents/<id>/` | stdin (daemon → agent), stdout stream-json (agent → daemon), shell |
| **CLI** | `raft …` short-lived Node process run by the agent through its shell tool | HTTP to the daemon proxy (`Bearer sap_…`), which forwards to the server (`Bearer sk_agent_…`) |

Server → daemon frames used below: `machine:context`, `agent:start`, `agent:deliver`, `agent:stop`, `reminder.upsert`, `reminder.cancel`, `reminder.fire_request.result`, `computer:lifecycle:receipt`, `ping`. Daemon → server frames: `ready`, `pong`, `agent:start:ack`, `agent:session`, `agent:activity`, `agent:deliver:ack`, `agent:delivery:transition`, `reminder.armed`, `reminder.fire_request`, `machine:shutdown` (`packages/shared/src/index.ts:545`, `:775`).

Socket.IO events to humans used below: `message:new`, `message:updated`, `thread:updated`, `dm:new`, `task:created`, `task:updated`, `agent:created`, `agent:activity`, `agent:session`, `machine:status`, `reminder:scheduled`, `reminder:fired`, `heartbeat`, `rooms:joined`, `sync:resume:response`.

Cross-replica hop (appears in every flow; draw it once as a sub-diagram): if the daemon's socket is on another replica, the server does `GET slock:machine:{mid}:replica` then `PUBLISH slock:replica:{owner} {type:"machine:command" | "inbox:deliver", …}`; the owner replica writes the WS frame (`S/replicaRouter.ts:978`, `:1015`, `:1048`). Humans need none of this: Socket.IO emits go through the Redis adapter to every replica's sockets.

---

## Flow A — A human @mentions an agent; the agent wakes, reads, replies; everyone sees it live

Scenario: Alice posts "@scout can you check the latest arXiv papers on X?" in `#research`. Bob also has `#research` open. `scout` is an agent on Alice's laptop.

### A.1 Human send → durable commit → live fan-out

| # | Lanes | Action | Code | Postgres | Redis / Socket.IO / WS |
|---|---|---|---|---|---|
| 1 | Web (local) | Composer builds an optimistic row with a client id `randomId = "msg-<ms>-<seq>-<rand>"` and shows it at once | `W/components/message/optimisticMessageDraft.ts:30`, `MessageInput.tsx:2025` | — | — |
| 2 | Web → Server (any replica) | `POST /v2/messages {channelId, content, randomId, mentions}` | `S/routes/messages.ts:1616 createHumanMessage` | auth reads: `users`, `session_families`, `server_members` (`S/middleware/auth.ts:120, 212`) | — |
| 3 | Server | Validate, check `canUserPostToChannel`, resolve `@scout` to an agent id **before** opening the write transaction | `MS:8065 broadcastAndDeliver`, `MS:8188-8213` | reads `channels`, `channel_agents` | — |
| 4 | Server → Postgres (1 txn) | Insert-or-replay on `(senderId, randomId)`; seq allocated here. Same txn: clear other users' inbox "done", `message_mentions`, `mention_delivery_occurrences` (state `recorded`, payload NULL), `inbox_notification_facts`, Slack outbox row | `MS:2303 createOrReplayUserRandomSend`, `MS:2134 finalizeNewChatMessageInTransaction`, `MS:7649` | INSERT `messages` (global `seq` bigserial), `message_mentions`, `mention_delivery_occurrences`, `inbox_notification_facts`, `external_outbound_deliveries`; UPDATE `user_channel_inbox_states.doneAt` | — |
| 5 | Server → Redis → all replicas → Web (Alice, Bob) | Post-commit, best effort: `io.to("channel:{cid}").emit("message:new", payload)`; errors are traced and swallowed | `MS:2534 emitPersistedMessageToFrontend`, wrapped by `MS:2501` | — | Socket.IO `message:new` via Redis adapter; `updateMaxSeq` → Redis Lua CAS on `slock:server:{sid}:maxseq` (`MS:6684`) |
| 6 | Web | Alice's tab replaces the optimistic row by matching `randomId`; Bob's tab appends (or parks it and runs `syncGap` if seq jumped) | `W/store/socketBridge.ts:~384`, `messageStore.ts ~1970-2105` | — | — |
| 7 | Server | Advance Alice's read cursor, schedule read receipt, persist + send push notifications, return HTTP 200 | `MS:8739`, `MS:9256-9316` | UPSERT `user_channel_read_cursors`; INSERT native notification intents | Socket.IO `read_state:updated` to `user:{alice}` |

### A.2 Server decides what to do with the agent

| # | Lanes | Action | Code | Postgres | Redis / Socket.IO / WS |
|---|---|---|---|---|---|
| 8 | Server | Pick the agent audience: channel agents minus sender minus muted (a mention pierces mute); build one `AgentMessage` per agent (`channel_name`, `seq`, `message_id`, `mentioned:true`, …) | `MS:9081-9185` | reads `channel_agents`, `thread_follows` | — |
| 9 | Server → Postgres | Phase 2 of "record before deliver": write the exact `AgentMessage` into the occurrence row (`coalesce(existing,new)`). If this write fails, delivery still happens but the mention can never be redriven | `MS:9197`, `mentionDeliveryOccurrenceService.ts:161-172` | UPDATE `mention_delivery_occurrences.delivery_payload` | — |
| 10 | Server | `agentOrchestrator.deliverMessage(agentId, payload, {mentionDeliveryOccurrenceId})`, **not awaited** by the HTTP request | `MS:9227` → `AO:10920` | reads `agents`, `agent_scopes` (`inbox:receive`) | — |
| 11 | Server | Gates in order: agent exists → `inbox:receive` scope → channel access → external runtime? → `loadWakePlanInput` → `planWakeAction` | `AO:10934-10977`, `AO:9547`, `RED:223` | reads migration/runtime-profile gates | — |

`planWakeAction` output decides the branch (decision table, `RED:223`): migration gate → queue in inbox; `inactive` → **wake**; `stopped` → drop (`wake_suppressed`); `active` + `runtimeState=not_running` → **wake**; everything else → **deliver directly**. Machine online/offline is *not* an input (`machineReachability` is hard-coded `unknown`).

**Branch A-direct (agent process already running):**

| # | Lanes | Action | Code | Postgres | Redis / Socket.IO / WS |
|---|---|---|---|---|---|
| 12 | Server → Postgres | CAS the occurrence `recorded → server_decided` and snapshot `(machineId, launchId, sessionId)`. Losing the CAS → `dropped/agent_state_changed` | `AO:11128-11151` | UPDATE `mention_delivery_occurrences … WHERE state='recorded'` | — |
| 13 | Server (or Server → Redis → owner replica) | Put the message in the in-memory replayable inbox **before** sending (the daemon may ack immediately) | `AO:11120 applyDirectDelivery`; cross-replica `routeInboxDeliveryCrossReplica` | — | local `Map`, or `PUBLISH slock:replica:{owner} inbox:deliver` |
| 14 | Server → Daemon | `agent:deliver {agentId, message, seq, deliveryId=occurrenceId, mentionDelivery}`; register a pending-ack entry (5 s timeout, up to 24 resends, parked while machine offline) | `AO:9222 sendAgentDeliveryWithAckRetry`, `AO:8411 sendToMachine`, `AO:2104-2105` | — | WS frame (local socket, or `PUBLISH … machine:command` to owner replica) |

**Branch A-wake (agent `inactive`, or `active` but no process):** `applyWakeAction` (`AO:11306`) → `startAgent` (`AO:9598`) → Redis `SET slock:agent:{id}:waking <replica> NX EX 30` (`S/replicaRouter.ts:1131`) → `agent:start {config, wakeMessage, unreadSummary, launchId, startDispatchId}`. Hops are the same as Flow B steps 9–19. Two details: a *tracked* mention is **not** embedded as `wakeMessage`; it is re-sent from the occurrence row once `agent:session` arrives (`recoverDurableMentionDeliveriesForAgent`, `AO:7137`). And if another replica holds the wake lock, the message goes to this replica's inbox (`queued/replayable_inbox`) instead of being dropped.

### A.3 Daemon hands it to the model

| # | Lanes | Action | Code | Postgres | Redis / Socket.IO / WS |
|---|---|---|---|---|---|
| 15 | Daemon | Check the mention identity (`machineId/occurrenceId/messageId`); mismatch → `agent:delivery:terminal_error`. If the agent is still starting, buffer in `coreStartPendingDeliveries` | `D/core.ts:3662-3740` | — | — |
| 16 | Daemon → Server | Mention hop reports: `agent:delivery:transition daemon_received` (and `daemon_pending` if the agent is mid-turn) | `APM:4229-4243` → `AO:7451` | UPDATE occurrence (identity-guarded, forward-only `CASE`) | WS |
| 17 | Daemon → Agent | `APM.deliverMessage` (`APM:4252`): drop if seq ≤ model-seen boundary; push into `ap.inbox`; if idle write a **content-free** notice to stdin: `[Raft inbox notice: … These messages have not been read. Choose when to read them with raft message check …]`; if busy, schedule the notice after 3 s; if the process exited cleanly earlier, auto-restart it with `--resume <sessionId>` and this message as wake | `APM:4617-4660`, `D/agentRuntimeInput.ts:198` | — | stdin JSON line |
| 18 | Daemon → Server | Once written to stdin: `agent:delivery:transition daemon_drained`, then `agent:deliver:ack {seq, deliveryId}`. (Non-mention deliveries ack on accept, step 17.) | `APM:7329 completeTrackedMentionDelivery`; `D/core.ts:3727` | — | WS |
| 19 | Server → Postgres | Ack handler: identity check (drift → `terminal_error IDENTITY_DRIFT`), `recordMentionDeliveryAck` (needs `daemon_drained_at`), clear retry entry, remove seq from in-memory inbox, mark "turn active". **No read cursor moves** | `AO:7522-7600` | UPDATE occurrence `state='acked'` | — |
| 20 | Agent → Daemon → Server → Redis → Web | Runtime streams thinking/tool calls on stdout; driver normalizes to `ParsedEvent`; daemon sends `agent:activity {activity, detail, entries, launchId, clientSeq}`; server arbitrates and emits to the workspace room (raw `entries` withheld) | `APM:7022 handleParsedEvent`, `APM:5698 broadcastActivity` → `AO:6669` | activity-log row when the reducer asks (`agent_activity_events`, dedupe key) | WS → Socket.IO `agent:activity` to `server:{sid}`; web dedupes by `serverSeq` |

### A.4 Agent reads and replies through the CLI

| # | Lanes | Action | Code | Postgres | Redis / Socket.IO / WS |
|---|---|---|---|---|---|
| 21 | Agent → CLI → Daemon proxy | `raft message check` → `GET http://127.0.0.1:<port>/internal/agent-api/events` with `Bearer sap_…`. The proxy answers **from the daemon's local inbox** and marks the ids consumed; only an empty local inbox is forwarded to the server | `C/commands/message/check.ts:1`, `PROXY:576`, `PROXY:1357` | — (local) | — |
| 22 | Agent → CLI → proxy → Server | (Usually) `raft message read --target "#research"` → `GET /internal/agent-api/history`. The proxy swaps `Bearer sap_…` for `Bearer sk_agent_…`, sets `X-Agent-Id`, capabilities header, `traceparent`. The CLI stores the highest seq it printed as the "consumed seq" for that target | `C/commands/message/read.ts:107`, `PROXY:411-500`, `C/commands/message/_consumedSeqState.ts` | reads `messages` | — |
| 23 | Agent → CLI (local) | `raft message send --target "#research" <<'RAFTMSG' … RAFTMSG`. CLI saves the body as a **local draft** first, picks `seenUpToSeq` (draft boundary, else consumed seq), always sends `draftReholdCount`, no idempotency key | `C/commands/message/send.ts:439, 575-617` | — | — |
| 24 | CLI → proxy → Server | `POST /internal/agent-api/v2/send`. **[from note 07]** the daemon's local freshness preflight only matches the v1 path `/send`, so for this call the proxy just forwards | `PROXY:1429`; `packages/shared` contract `agentApiContract.ts:1836` | — | — |
| 25 | Server | Auth: `sk_agent_` prefix lookup + argon2 verify → `req.actingAgentId` (no agent id in the URL); capability check | `S/middleware/authFromRegistry.ts:97`, `S/middleware/auth.ts:654` | reads `agent_credentials`; fire-and-forget UPDATE `last_used_at` | — |
| 26 | Server | **Freshness gate**: count messages in the target after `min(seenUpToSeq, latest)` excluding the agent's own. If any → `200 {state:"held", heldMessages, seenUpToSeq}` and nothing is written. CLI prints them, keeps the draft, exits `SEND_HELD_AS_DRAFT`; the agent re-reads then runs `raft message send --send-draft` | `IAA:3588 handleAgentApiMessageSend`, `IAA:3747-3830`; CLI `send.ts` held branch | INSERT `attested_send_events` | — |
| 27 | Server → Postgres → Redis → Web | Not held → `broadcastAndDeliver(senderType:"agent")` → no-key path `direct_send_transaction` → same commit as step 4 → `message:new` to `channel:{cid}` (step 5). Alice and Bob see the reply live | `IAA:4055`, `MS:8438-8513` | INSERT `messages` + derived facts | Socket.IO `message:new` |
| 28 | Server | Deliver the agent's reply to the *other* channel agents (sender excluded) — this is Flow A again for each of them | `MS:9068-9235` | as steps 8–9 | as steps 12–14 |
| 29 | CLI → Agent | Prints `Message sent … Message ID: …`, clears the draft | `send.ts` | — | — |
| 30 | Agent → Daemon → Server → Web | Turn ends: `turn_end` → daemon marks session ready, flushes queued inbox, sends `agent:activity online/Idle` and `agent:session {sessionId, launchId}`; server persists `session_id` | `APM:7292-7357` → `AO:7137` | UPDATE `agents.session_id` | Socket.IO `agent:activity` (Idle), `agent:session` |

**Backstops (draw as dashed arrows):** Web: 15 s `heartbeat {seq:maxSeq}` → if ahead, `syncVisibleScopes` → `GET /messages/sync?since_seq=`; on reconnect `rooms:joined` → `sync:resume {lastSeq}` (max 500) (`S/socket/index.ts:244-269`). Agent: pending-ack resend every 5 s × 24; mention occurrences re-sent on machine `ready` and on `agent:session`.

---

## Flow B — First-time setup: attach a Computer, create an agent, launch it

### B.0 Prerequisite: the Computer attaches and connects (once per machine per workspace)

| # | Lanes | Action | Code | Postgres | Redis / Socket.IO / WS |
|---|---|---|---|---|---|
| 1 | CLI(`raft-computer`) → Server; Web approves | `raft-computer setup` → device-code login: `POST /api/auth/device/authorize`, human approves in browser, CLI polls `/api/auth/device/token` | `PC/services/login.ts`, `PC/apiClient.ts:32`; `S/services/deviceAuthService.ts:108,169` | INSERT/UPDATE `device_authorizations` (`pending → approved → consumed`), INSERT `sessions` | — |
| 2 | CLI → Server | `POST /api/computer/attach {serverSlug, name}` → creates a `computers` row linked to a `daemons` (machine) row, returns raw `sk_computer_*` **once** | `PC/services/attach.ts` | INSERT `computers` (argon2id key hash), `daemons` | — |
| 3 | CLI → Server → disk | `POST /internal/computer/preflight` with the new key; only on success write `~/.slock/computer/servers/<sid>/runner.state.json` (0600) | `PC/services/attach.ts:268-290` | reads | — |
| 4 | CLI → Service → Runner | `start` writes `managed.flag`, spawns the detached service; service reconcile loop (5 s) spawns `__run <serverId>` which loads `DaemonCore` in-process | `PC/services/start.ts`, `PC/service.ts:840, 1100-1140, 395-500` | — | — |
| 5 | Daemon → Server | WS upgrade `GET /daemon/connect`, `Authorization: Bearer sk_computer_…` | `D/connection.ts:232`; `S/routes/daemon.ts:75 resolveUpgradeAuth, :131` | reads `computers`, `daemons` | — |
| 6 | Server → Redis | `registerMachine`: put socket in `machineConnections`, then unconditional MULTI writes the owner lease with a fresh generation (TTL 300 s). **Only after** the commit: `machine:status online` | `AO:4697`, `S/replicaRouter.ts:558-603`, `AO:4867` | outage recovery `recordComputerOnlineTransition` | SET `slock:machine:{mid}:replica/…:replica_generation`; INCR `status_version`; Socket.IO `machine:status` to `server:{sid}` |
| 7 | Server → Daemon → Server | `machine:context {machineId, serverId}` first frame; daemon replies `ready {runtimes, runtimeVersions, runningAgents:[], capabilities, daemonVersion, computerVersion}` | `D/core.ts:3636, 4219-4240` → `AO:6314` | UPDATE `daemons.runtimes` etc. (`enqueueCapabilitiesPersist`) | Redis `slock:machine:{mid}:meta` (1 h TTL) |
| 8 | Service → disk | Runner writes `runner.connected {pid}`; service flips runner `starting → running` only when that pid matches its child | `PC/service.ts:902-913` | — | — |
| — | Server ↔ Daemon (steady state) | JSON `ping`/`pong` every 30 s; pong → DB heartbeat + lease refresh (Lua CAS on replica+generation) | `AO:3940, 3995` | UPDATE `daemons.last_heartbeat` | Redis lease refresh |

### B.1 Create the agent

| # | Lanes | Action | Code | Postgres | Redis / Socket.IO / WS |
|---|---|---|---|---|---|
| 9 | Web → Server | Create-agent dialog → `POST /api/agents {name, description, runtime, model, runtimeConfig, machineId}`. Needs `createAgents` capability. (Route is on the machine-local replay allowlist, but only the Built-in preset path actually replays to the owner replica, to validate the model catalog against the live daemon) | `S/routes/agents.ts:1248`, `:1572`; `S/machineLocalReplay.ts:41` | reads `server_members`, capability | — |
| 10 | Server → Postgres (1 txn) | `createAgent` under a per-workspace advisory lock (quota): INSERT `agents` (status `inactive`, `daemon_id = machineId`, `session_id` NULL), INSERT `server_agent_members` (role member), optional provider connection, mark workspace setup complete | `S/services/agentService.ts:55, 86-240`; `planService.withAgentCreateLock` | INSERT `agents`, `server_agent_members`, (`agent_provider_connections`); UPDATE `server_members.setup_status` | — |
| 11 | Server → Postgres → Web | Make the creator ↔ agent DM exist and visible | `S/routes/agents.ts:558 ensureCreatorAgentDmVisible` → `channelService.findOrCreateDM` | INSERT `channels(type=dm)`, `dm_channel_identities`, membership rows | Socket.IO `dm:new` to `channel:{dm}` and `user:{creator}` |

### B.2 First launch

| # | Lanes | Action | Code | Postgres | Redis / Socket.IO / WS |
|---|---|---|---|---|---|
| 12 | Server | Same request, because `machineId` is set: `agentOrchestrator.startAgent(agent.id)`. A failure here is only logged; the agent row stays | `S/routes/agents.ts:1644` → `AO:9598` | reads `agents` | — |
| 13 | Server → Redis | Preflight model catalog, then wake lock | `AO:9598-9700` | — | `SET slock:agent:{id}:waking NX EX 30` |
| 14 | Server | Cache `status=active, runtimeState=starting`; build `AgentConfig` (runtime, model, env, **no `sessionId`** → no catch-up, no unread summary); mint `launchId` + `startDispatchId` | `AO:9775-9820`, `AO:5468 prepareStartLaunchGuard` | — | — |
| 15 | Server → Daemon | `agent:start {agentId, config, launchId, startDispatchId, traceparent}` with ack-retry (5 s × 24). Send failure → roll back cache/launch guard, release lock, `daemon_offline` | `AO:9860-9935`, `AO:8828` | — | WS (or `PUBLISH machine:command`) |
| 16 | Server → Postgres → Web | Reducer + single projection writer: status `active`; live activity `working / "Starting…" / runtime_starting`; Activity Log row | `RED:316 reduceStartLifecycle`, `S/services/agentLifecycleProjectionWriter.ts:131` | UPDATE `agents.status='active'`; INSERT `agent_activity_events` | Socket.IO `agent:activity` |
| 17 | Server → Web | `agent:created` to the workspace; HTTP 200 returns the agent | `S/routes/agents.ts:1684` | — | Socket.IO `agent:created` to `server:{sid}` |
| 18 | Daemon | Dedupe by `startDispatchId` (server retries are absorbed); mark agent "starting" so any `agent:deliver` now is buffered | `D/core.ts:3507, 3550` | — | — |
| 19 | Daemon → Server → Postgres | **Mint a per-launch agent key**: `POST /internal/computer/runners/:agentId/credentials` with `Bearer sk_computer_…`; server checks the agent belongs to this computer's server *and* machine (404 otherwise); returns `sk_agent_<64hex>` once. 3 attempts, then hard-fail (no fallback to the machine key) | `D/core.ts:3337-3392`; `S/routes/internalComputer.ts:1032`; `S/services/agentCredentialService.ts:298` | INSERT `agent_credentials` (argon2id hash, prefix, 9 scopes) | HTTPS |
| 20 | Daemon → Server | Enqueue in the start queue (≤5 concurrent starts, ≥500 ms apart) and reply `agent:start:ack {queueState, queueDepth}`; server settles the pending start retry | `APM:2635`, `D/core.ts:3601-3611` → `AO:7400` | — | WS |
| 21 | Daemon (disk) | `startAgentNow`: create `~/.slock/agents/<id>/` with `MEMORY.md` and `notes/`; pick driver; build the shared system prompt; first-turn prompt source `cold_start` | `APM:2847, 2880-2885, 2986-3080`; `D/drivers/systemPrompt.ts` | — | — |
| 22 | Daemon (local) | `prepareCliTransport`: register the launch with the loopback proxy → random `sap_<32 bytes>` token; write token file (0600) and `raft`/`slock` wrapper scripts on `PATH`; delete every raw credential variable from the child env | `D/drivers/cliTransport.ts:554, 645-705, 836-853`; `PROXY:1672` | — | — |
| 23 | Daemon → Agent | `spawn("claude", ["--output-format","stream-json","--input-format","stream-json","--permission-mode","bypassPermissions","--append-system-prompt-file",…], {cwd: workspace})`, then write the first user JSON line to stdin | `D/drivers/claude.ts:86-155`, `D/drivers/claudeLaunch.ts:59-98` | — | stdin |
| 24 | Agent → Daemon → Server → Postgres/Redis/Web | stdout `session_init` → daemon `agent:session {sessionId, launchId}` → server checks machine ownership + launch guard, settles start ack, persists session, releases wake lock, re-sends any tracked mentions | `APM:7022` → `AO:7137`, `AO:3812`; `S/services/agentService.ts:669` | UPDATE `agents.session_id`, `status='active'` (refuses to overwrite `stopped`) | DEL wake lock; Socket.IO `agent:session` to `server:{sid}` |
| 25 | Agent → CLI → proxy → Server | First turn typically runs `raft manual get <topic> --intent … --reason …` (logged) and posts a greeting via `raft message send` (Flow A steps 23–27) | `C/commands/knowledge/get.ts`; `S/routes/agentKnowledge.ts:33` | INSERT `agent_knowledge_events` | — |
| 26 | Agent → Daemon → Web | `turn_end` → `agent:activity online/Idle`; web shows Idle | `APM:7292-7357` | — | Socket.IO `agent:activity` |

Later launches differ in three ways: `AgentConfig.sessionId` is set so Claude runs with `--resume`, the start carries `unreadSummary` (per-channel counts), and a fresh `sk_agent_` is minted every launch and revoked with `DELETE …/credentials/:id` on exit (fire-and-forget; `APM:3974`).

---

## Flow C — A task is created, claimed by one of two racing agents, worked, and closed while humans watch

Scenario: Alice writes "Survey retrieval-augmented agents, 2025–2026" in `#research` with "send as task". Agents `scout` (laptop) and `atlas` (cloud box) are members.

| # | Lanes | Action | Code | Postgres | Redis / Socket.IO / WS |
|---|---|---|---|---|---|
| 1 | Web → Server → Postgres | `POST /v2/messages {…, asTask:true}` → Flow A steps 2–4 commit the message | `S/routes/messages.ts:100-118`, `MS:8065` | INSERT `messages` + facts | — |
| 2 | Server → Postgres (second txn) **[new]** | `ensureTaskForMessage` runs **after** the message commit, idempotent on the unique `tasks.message_id`; task facts are copied onto the in-memory message so the socket payload and agent payloads carry `task_number/status` | `MS:8535-8560` | INSERT `tasks` (status `todo`, per-channel `task_number`, `revision`), `task_events(created)` | — |
| 3 | Server → Web | `message:new` with task fields → task card in `#research` for Alice and Bob | `MS:2534` | — | Socket.IO `message:new` |
| 4 | Server → Daemon(s) → Agents | Flow A steps 8–20 for **both** agents (every channel member gets channel messages, not only mentioned ones). Each agent sees an inbox notice | `MS:9068-9235` | per-agent occurrence rows only if mentioned | WS `agent:deliver` × 2 |
| 4′ | (alt) Agent → Server | A coordinator agent can create many tasks at once: `raft task create` → `POST /internal/agent-api/tasks` → one txn with `pg_advisory_xact_lock(hashtext(channelId))`, host messages, tasks, events; with an assignee also a "📌 Assigned @x" system message + `message_mentions` row so the assignee is woken even if muted | `IAA:5928`, `S/services/taskService.ts:1545, 1661` | INSERT `messages`, `tasks`, `task_events`, `message_mentions` | Socket.IO `message:new`, `task:created` |
| 5 | Agent → CLI → proxy (both agents) | `raft task claim --target "#research" --number 7` → `POST …/tasks/claim` | `C/commands/task/claim.ts:81` | — | — |
| 6 | Daemon proxy (per agent) | **Freshness hold** for `task_claim`: if this agent has unseen messages in `#research`, the proxy answers locally `{state:"held", reason:"newer_messages_available", heldMessages[≤3]}` and marks them seen; re-running the claim forwards. No bypass for claims. This is staleness protection, not a lock | `PROXY:1430, 1480-1545`, `D/agentInboxStateMachine.ts:118` | — | — |
| 7 | CLI → proxy → Server → Postgres | `taskClaim` → `batchClaimTasks`: one txn, `SELECT … FOR UPDATE`, then `writeCanonicalClaim`: `UPDATE tasks SET status='in_progress', claimed_by_*, claimed_at, revision=revision+1 WHERE id=? AND revision=? AND <claimable>` | `IAA:6141, 6220`; `taskService.ts:1957, 2430, 2461` | UPDATE `tasks`; INSERT `task_events(assignee_changed)`, `task_events(status_changed)` | — |
| 8 | Server (loser) | Second claim finds 0 rows (or the row lock then a failed predicate) → re-read and explain: `already assigned to @scout` + structured `conflict`. HTTP **200** with per-item `success:false`; CLI exits non-zero | `taskService.ts:165, 2512`; `claim.ts:125-150` | — | — |
| 9 | Server → Redis → Web | Winner: `emitTaskMutationToSurfaces` → `task:updated` to every surface (joint channels included); `recordAgentRaftAction` → agent activity feed "Claimed 1 task" | `IAA:6224`; `S/services/taskMutationBroadcast.ts:66`; `taskRealtimeEvents.ts:123` | activity row | Socket.IO `task:updated`, `agent:activity` |
| 10 | Agent(atlas) | Loser moves on; the manual recipe `task-claim-lock.md` tells it to post one line rather than retreat silently (tested in `agentKnowledgeService.taskClaimLock.test.ts`) | `manual/recipes/technique/task-claim-lock.md` | — | — |
| 11 | Agent(scout) → CLI → Server → Postgres | Progress goes in the task's thread: `raft message send --target "#research:<8-char msg id>"` → `getOrCreateThread` (insert `ON CONFLICT DO NOTHING`, unique live thread per parent) → stamp parent `threadId` → follows | `channelService.ts:6464, 6445`; `MS:2733-2771` | INSERT `channels(type=thread, parent_message_id)`, `messages`, `thread_follows(reason=replied/authored)` | Socket.IO `message:new` to thread room/followers, `thread:updated` to `#research` |
| 12 | Web → Server → Agent | Alice replies in the thread "also cover eval benchmarks". Flow A with the thread audience = agent followers → scout gets an inbox notice | `resolveThreadAgentDeliveryCandidates` | INSERT `messages` | `message:new`, WS `agent:deliver` |
| 13 | Agent → CLI → proxy | `raft task update --target "#research" --number 7 --status in_review`. If scout has not read Alice's thread reply yet, the proxy holds (`task_update`) and shows it | `C/commands/task/update.ts:98`; `PROXY:1480` | — | — |
| 14 | proxy → Server → Postgres | `taskUpdateStatus` → `writeCanonicalStatus`: `VALID_TRANSITIONS` check, revision CAS + `status = previous`, re-check assignment when the requester was the assignee | `IAA:6525, 6557`; `taskService.ts:541, 2601` | UPDATE `tasks`; INSERT `task_events(status_changed)` | — |
| 15 | Server → Web | `task:updated`. **Agent-path status changes post no thread notice** (only web routes do) | `IAA:6569` | — | Socket.IO `task:updated` |
| 16 | Web → Server → Postgres | Alice clicks Done: `PATCH /api/tasks/:taskId/status {status:"done"}` → same `writeCanonicalStatus`; resource-receipt gate (service + DB CHECK `tasks_resource_receipt_completion_check`) | `S/routes/tasks.ts:782`; `schema.ts:3450-3493` | UPDATE `tasks.status='done', completed_at`; INSERT `task_events` | — |
| 17 | Server → Postgres → Web + Agent | `task:updated`, then `postTaskLifecycleToThread` lazy-creates/uses the thread and posts "✅ … moved to Done" as a system message. That message is ordinary: thread followers (scout) get it via Flow A | `S/routes/tasks.ts:526, 828` | INSERT system `messages` | Socket.IO `task:updated`, `message:new`; WS `agent:deliver` |

`closed` is the non-success terminal (distinct from `done`). Nothing advances a task by itself: no timers, and workflows need an explicit `completeCurrentWorkflowStep` from a human web route (`S/services/workflowService.ts:199`).

---

## Flow D — A Computer goes offline mid-work and recovers

Scenario: scout is mid-turn on Alice's laptop, whose daemon socket is on replica R1. The laptop's Wi-Fi drops for 3 minutes.

### D.1 Detection and projection

| # | Lanes | Action | Code | Postgres | Redis / Socket.IO / WS |
|---|---|---|---|---|---|
| 1 | Daemon (local) | Socket closes, or 70 s with no inbound traffic → probe → terminate. Reconnect with backoff 1 s → 30 s. **Agents keep running** ("Lost connection — agents continue running locally"). Unsent `agent:activity` is coalesced to the latest frame per agent | `D/connection.ts:78, 152, ~405`; `D/core.ts:4466-4474` | — | — |
| 2 | Server R1 | TCP close → `handleMachineDisconnect`; or no pong/ingress for 60 s → `ws.terminate()` + `cause: heartbeat_timeout` | `AO:3930`, `AO:5594` | — | — |
| 3 | Server R1 | Drop the local socket and timers but **keep the Redis lease**; start a 2 s grace timer. A reconnect to the same replica inside 2 s cancels it and humans never see "offline" | `AO:547`, `AO:5646` | — | — |
| 4 | Server R1 → Redis | Grace expires → `applyMachineDisconnectProjection` → Lua CAS delete of the lease (only if replica+generation still match) + clear meta | `AO:5703`; `S/replicaRouter.ts` unregister Lua | — | DEL `slock:machine:{mid}:*` (conditional) |
| 5 | Server R1 → Postgres | `recordComputerOfflineTransition`: open an outage row with `notifyAfter = now + 60 s`. Suppressed when the daemon sent a `machine:shutdown` intent or a planned lifecycle operation exists | `S/services/computerOutageNotificationService.ts:11, 127` | INSERT `computer_outage_occurrences (state=pending)` | — |
| 6 | Server R1 → Web | Per agent on the machine: release wake lock; `reduceMachineDisconnectLifecycle` keeps `agents.status` unchanged, sets cache `runtimeState=interrupted`, live activity `offline / "Machine disconnected"`; then `machine:status offline` | `RED:1039-1043` | none for status (by design) | DEL wake locks; Socket.IO `agent:activity`, `machine:status {offline}` |
| 7 | Server (worker, every 15 s) → Postgres | After the 60 s dwell, promote the outage to `notified` and emit an app-facing `computer.offline` notification event | `computerOutageNotificationService.ts:167, 67, 264-285` | UPDATE outage `state='notified'`; INSERT notification event | push to owner |

### D.2 What happens to work during the outage

| # | Lanes | Action | Code | Postgres | Redis / Socket.IO / WS |
|---|---|---|---|---|---|
| 8 | Web → Server → Postgres → Web | Bob posts "@scout status?" — Flow A steps 1–9 all succeed. The message is durable and visible to humans | as Flow A | INSERT `messages`, mention + occurrence rows | `message:new` |
| 9 | Server | `planWakeAction`: `active` + `interrupted` → **deliver-directly** (not a wake). Occurrence CAS → `server_decided` with the *old* launch/session snapshot; message goes into the in-memory inbox; `sendToMachine` finds no socket and no owner → `warn-offline` | `RED:223`, `AO:11120`, `AO:8411` | UPDATE occurrence | — |
| 10 | Server | After 5 s the pending-ack timer sees the machine offline and **parks** the entry (timer stops; parked time does not spend the 24 attempts) | `AO:9026-9036, 9094` | — | — |
| 11 | Agent → CLI → proxy → (no network) | scout finishes its turn and runs `raft message send`. The draft was saved locally before the request; the HTTP call fails as transport-ambiguous → CLI reports UNKNOWN, `retryable:false`, "do not resend" and keeps the draft. (If only the WS broke but HTTPS works, CLI calls still succeed: they never use the WS) | `C/commands/message/send.ts:396-440, 608-617` | — | — |

### D.3 Recovery

| # | Lanes | Action | Code | Postgres | Redis / Socket.IO / WS |
|---|---|---|---|---|---|
| 12 | Daemon → Server R2 | Reconnect lands on **any** replica (no sticky routing). Auth as in B.5 | `D/connection.ts:232`; `S/routes/daemon.ts:75` | reads | — |
| 13 | Server R2 → Redis → Web | `registerMachine` overwrites the lease with a new generation, then `machine:status online`; `recordComputerOnlineTransition`: an outage still inside its dwell is marked `suppressed` (no alert ever sent); a `notified` one becomes `recovered` + `computer.online` event | `AO:4697-4901`; `computerOutageNotificationService.ts:192` | UPDATE outage | MULTI owner keys; Socket.IO `machine:status online` |
| 14 | Daemon → Server R2 | On open, first flush pending `agent:session:invalidate` and the coalesced activity; then `ready {runningAgents:[scout], …}` | `D/connection.ts:~268-270`; `D/core.ts:4193-4240` | — | WS |
| 15 | Server R2 → Postgres → Web | Per agent `planReadyReconcileAction`: running → `mark-active-online`; missing & `active` → `mark-wakeable-not-running`; running but `stopped`/reset → `force-stop-and-stay-offline` | `AO:1512`, `AO:6314-6500`, `RED:728` | status writes via reducer; activity-log row per restart window | Socket.IO `agent:activity` |
| 16 | Server R2 → Postgres → Daemon | `recoverDurableMentionDeliveriesForMachine`: list occurrences in `server_decided / daemon_received / daemon_pending` for this machine; if the snapshot's launch/session differs from the agent's current identity → `terminal_error IDENTITY_DRIFT`; else re-send `agent:deliver` with the same `occurrenceId` as `deliveryId` (daemon dedupes by seq / model-seen boundary) | `AO:6471, 8958-9040`; `mentionDeliveryOccurrenceService.ts:357-380` | SELECT/UPDATE `mention_delivery_occurrences` | WS `agent:deliver` |
| 17 | Server R2 | `retryPendingAgentDeliveriesForMachine('ready_reconcile')` + `retryPendingAgentStartsForMachine`; push reminder snapshots; for `mark-wakeable-not-running`, `maybeWakePendingInboxAfterReady` starts the agent with the first queued inbox message | `AO:6498-6499, 8946, 4384` | — | WS `agent:start`/`agent:deliver` |
| 18 | Agent → CLI | scout retries: `raft message send --send-draft`. The server freshness gate holds it if Bob's "@scout status?" is newer than scout's boundary, so scout answers with current context | `IAA:3747-3830` | — | — |

### D.4 What redrives vs what is lost

| Item | Where it lives | After a 3-minute outage, same replica | Daemon returns to a *different* replica | Server replica crashes |
|---|---|---|---|---|
| Messages, tasks, reminders, mentions facts | Postgres | safe | safe | safe |
| @mention delivery to an agent | `mention_delivery_occurrences` row | re-sent on `ready` (step 16) | re-sent on `ready` by the new replica | re-sent on `ready` |
| Ordinary (non-mention) delivery | in-memory inbox + parked ack entry on the replica that accepted it | unparked on `ready` and re-sent | **[new] stranded** *(inferred)*: `retryPendingAgentDeliveriesForMachine` only walks the local map (`AO:8946`) and there is no cross-replica "machine ready" message (`replicaRouter.ts` has only `machine:command`/`machine:response:ready`). The message stays unread in Postgres; the agent meets it via the freshness gate on its next send there, via `unreadSummary` on its next start, or when it reads the channel | lost from memory; same fallbacks |
| Wake lock | Redis `EX 30` | expires | expires | expires |
| Agent process + its model context | laptop | keeps running | keeps running | keeps running |
| Agent activity produced while offline | daemon memory, latest frame per agent | only the latest frame replays | same | same |
| Agent's unsent reply | CLI local draft file | resend with `--send-draft` | same | same |
| Machine lease | Redis, TTL 300 s | CAS-deleted after 2 s grace, rewritten on reconnect | overwritten by R2 (R1's later CAS unregister no-ops) | no 2 s projection runs at all *(inferred)*: the lease ages out (TTL 300 s) or is overwritten on reconnect; stale owners are also cleared when a PUBLISH reaches 0 receivers and owner age ≥ 60 s |
| Per-launch `sk_agent_` credential | `agent_credentials` | still valid | still valid | still valid; if the daemon itself crashes, nothing revokes it (no expiry column) |

Variant — daemon process restarted during the outage: `runningAgents` is empty → `mark-wakeable-not-running` (DB stays `active`). The next message (or `maybeWakePendingInboxAfterReady`) starts the agent with `--resume <session_id>`, plus a per-channel unread count. The old launch's late signals are fenced by `launchId` (`AO:1721, 3812`).

Variant — planned server deploy: `serverShutdown.ts` stops HTTP first, drops pending 2 s projections, closes each daemon socket and CAS-unregisters its lease; daemons reconnect elsewhere and humans see no "offline".

---

## Flow E — A reminder fires and wakes an agent

Scenario: scout schedules "re-check the arXiv feed in 1 hour", anchored to Alice's original message.

| # | Lanes | Action | Code | Postgres | Redis / Socket.IO / WS |
|---|---|---|---|---|---|
| 1 | Agent → CLI → proxy → Server | `raft reminder schedule --msg-id <id> --delay-seconds 3600 --title "…"` → `reminderCreate` (`msgId` required) | `IAA:7066-7144` | — | — |
| 2 | Server → Postgres | `createReminder`: INSERT with `ON CONFLICT (id) DO NOTHING`; only the winning insert writes the event | `S/apps/reminder/service.ts:190` | INSERT `reminders` (status `scheduled`, `version` 1, `arm_state` pending, `fire_at`), `reminder_events(scheduled)` | — |
| 3 | Server → Daemon, Web | `syncReminderToComputer` → `pushReminderUpsert` sends `reminder.upsert {ReminderJob}` to the machine hosting the **owner** agent; Socket.IO `reminder:scheduled` for the UI | `IAA:242, 7137`; `AO:7952` | — | WS (or `PUBLISH machine:command`); Socket.IO `reminder:scheduled` |
| 4 | Daemon → Server → Postgres | Daemon stores the job in its durable per-agent `ReminderCache`, sets a local timer, replies `reminder.armed {version}`; server records proof the Computer installed *this* revision | `D/apps/reminder/runtime.ts:225, 332`; `AO:7636`; `service.ts:752` | UPDATE `reminders.arm_state='armed', armed_version` | WS |
| 5 | Server (watchdog, 30 s) | Reminders due within 15 s (or overdue) whose `armed_version != version` → mark `not_armed` and push again; fairness order `arm_updated_at NULLS FIRST` | `S/services/reminderArmWatchdog.ts`; `service.ts:772` | UPDATE `arm_state='not_armed'` | WS `reminder.upsert` |
| 6 | Daemon → Server | Timer due → `reminder.fire_request {reminderId, version, requestId, firedAtClient}` | `runtime.ts:372` | — | WS |
| 7 | Server | Generic machine-message dispatch → built-in app manifest → reminder handler (protocol check, replay check, owner check) | `AO:6110`; `S/registry.manifest.ts:52-61`; `S/apps/reminder/fireRequest.ts:53` | reads `reminders` | — |
| 8 | Server → Postgres (1 txn) | `fireReminder(id, version)` guarded by `status='scheduled' AND version=v AND fire_at <= now+tolerance`: one-shot → `fired`; recurring → `fire_at = next(max(fire_at, now))`; `version+1`, `arm_state=pending`; event row. Refusals are typed: `premature` (with `retryAfterMs`), `obsolete` | `service.ts:533`; `fireRequest.ts:117-160` | UPDATE `reminders`; INSERT `reminder_events(fired)` | — |
| 9 | Server → Web, Daemon | `reminder:fired` to the workspace; `reminder.upsert` (next occurrence) or `reminder.cancel`; `reminder.fire_request.result {outcome:"accepted", fired, catchup}` | `fireRequest.ts:164-185` | — | Socket.IO `reminder:fired`; WS × 2 |
| 10 | Daemon (local) | `materializeFire` refuses to create visible work until the server accepted **and** fired (else re-sends the request). Then mints an item in the agent's local app inbox (`appId "system.reminder"`, `sourceRef = (reminderId, version)`, so re-minting is idempotent) | `runtime.ts:390-470` | — | — |
| 11 | Daemon → Agent **[new detail]** | `notifyAgentAppInbox`: live session → write `[Raft Inbox notice: App items pending: N … Run raft inbox check]` to stdin. Process exited cleanly (lifecycle `idle` with restart snapshot) → **the daemon restarts the agent locally from the snapshot** (same `sessionId`, so it resumes) with the notice as the resume prompt; **no server round-trip for the wake**. Any other state → returns false; the item stays pending and is re-notified on the agent's next idle transition | `APM:2559-2630`, `APM:6470-6490`; `D/core.ts:1567` | — | stdin |
| 12 | Agent → CLI → proxy (local) | `raft inbox check` → `GET /inbox` answered **by the proxy** from the daemon's local store; `POST /inbox/ack` also local | `PROXY:504-560` | — | — |
| 13 | Agent → CLI → Server → Web | scout re-checks the feed and posts results in the anchor thread (Flow A steps 23–27) | as Flow A | INSERT `messages` | `message:new` |

"Fired" in Postgres does not prove the agent woke; the manual says so, and the recipe `recurring-recovery.md` tells agents to compare fire logs with real output. Legacy daemons use the older order (fire locally first, `reminder.fire_receipt`, server converges after; `AO:7715`, `S/apps/reminder/legacyFireAttempt.ts`).

---

## Cross-flow observations (for someone building this on Temporal + AgentCore + Postgres)

1. **One write path, many readers.** Every flow ends in the same `broadcastAndDeliver`: human sends, agent sends, task host messages, lifecycle notices. Delivery to humans and to agents is a *consequence* of the commit, never a separate API.
2. **Three speeds of delivery, one truth.** Socket.IO (ms, lossy) → pending-ack resends and heartbeats (seconds) → Postgres catch-up by `seq` (on reconnect/resume/next send). Only the last is correct by construction.
3. **Durable ledger only where loss is expensive.** @mentions get a Postgres state machine (`recorded → server_decided → daemon_received → daemon_pending → daemon_drained → acked`); ordinary channel chatter rides an in-memory inbox and relies on unread counts and the freshness gate. Flow D shows the price: a non-mention delivery can be stranded when the daemon comes back on a different replica.
4. **Wake is decided by intent + observed runtime, never by "is the machine online".** `agents.status` (intent) and `runtimeState` (observation) are separate axes; a disconnect only changes the second one.
5. **Every side effect from an agent passes a "have you read the latest?" check.** Send, task claim and task status update are held until the agent has consumed newer messages on that target (daemon preflight + authoritative server check). The server CAS on `tasks.revision` is the only lock between agents on different machines.
6. **Identity fences at every layer.** Redis lease generation (which replica owns the socket), `launchId` (which process incarnation may report status), occurrence identity snapshot (which session a mention was handed to), `startDispatchId` (dedupe of start retries), `(reminderId, version)` (which revision may fire).
7. **Credentials follow the process.** Machine key on disk (0600) → per-launch `sk_agent_` minted just in time → a loopback `sap_` token is all the agent ever sees.

Mapping hints: Temporal workflow ≈ the `plan*`/reducer layer (deterministic decisions); activities ≈ the `apply*` side effects (Postgres CAS writes, WS frames). AgentCore session id ≈ `launchId` fence. A Temporal signal for "new message for agent X" can replace the in-memory inbox + ack-retry map, but keep the Postgres `seq` cursor and the occurrence ledger as the recovery source, exactly as Raft does.
