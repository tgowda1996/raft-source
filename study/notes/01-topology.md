# 01 — System topology & server core

All paths relative to `/home/user/raft-source`. Server code lives in `packages/server/src` (abbreviated `S/`).

## 1. What it is

Raft runs as one stateless-ish Node/Express process type (the "server", `S/server.ts`) that you can run as N replicas. Every replica shares one Postgres (source of truth, drizzle schema in `S/db/schema.ts`) and one Redis (pub/sub, ownership leases, locks, small mirrors). The process boots without Redis, but daemons cannot attach without it (owner registration fails closed). Browsers/mobile/desktop/CLI humans talk to it over HTTP (`/api/*`) and Socket.IO. Agent machines ("Computers") run a daemon (`packages/daemon`) that holds **one long-lived raw WebSocket** to **one** replica (`/daemon/connect`); agent processes on that machine reach the server through a local credential proxy in the daemon and the `/internal/*` HTTP surface. The hard problem the server core solves is: *a daemon socket is pinned to one replica, but a request or message that needs that daemon can land on any replica* — solved with a Redis "machine → owner replica" lease plus three forwarding mechanisms (pub/sub commands, HTTP replay, a reply mailbox).

Naming: a "server" in the data model is a **workspace** (Slack-like team; table `servers`, header `X-Server-Id`). The "server process" is the Node backend. The legacy product name "Slock" survives in log prefixes, Redis keys (`slock:*`), env vars (`SLOCK_*`, `SLOCKDEV_*`) and table names (`daemons`).

## 2. Key components

| Name | File path(s) | Responsibility |
|---|---|---|
| Process bootstrap | `S/server.ts` (305 lines) | Loads translation config, `initDatabase`, `initRedis`, tracer, creates Express app + `http.Server`, `AgentOrchestrator`, `initReplicaRouter` (L122), machine WS (L150), Socket.IO (L153), ~9 background workers, hourly maintenance (L166), Prometheus server (L205), graceful shutdown. |
| Express app / route layout | `S/app.ts` (665 lines) | Middleware chain (helmet, cors, raw-body webhooks before `express.json`, observability). Three route families: `/api/*` humans (JWT + `X-Server-Id`), `/internal/*` machines/agents (API keys), public (`/share`, `/api/public`, avatars). 61 route files in `S/routes/`. |
| Auth middleware | `S/middleware/auth.ts` | `requireAuth` (JWT), `requireVerified`, `requireServer` (L212: `X-Server-Id` header + `server_members` join, sets `req.serverId`), `requireMachineAuth` (L465: machine key → `req.machineId` + `req.serverId`), `requireFlexAuth`. |
| Route auth registry | `S/middleware/routeAuthPolicy.ts`, `S/middleware/authFromRegistry.ts` | Declarative table of `{method, path, principal}` for `/internal/agent-api/*` (`sk_agent_*`) and `/internal/computer/*` (`sk_computer_*`). Unregistered path under a claimed prefix → 401 `auth_policy_unregistered_path` (fail closed). |
| Machine WebSocket | `S/routes/daemon.ts` `setupMachineWebSocket` (L131) | Raw `ws` server on `upgrade` for path `/daemon/connect`, attached **before** Socket.IO. Classifies key prefix (`sk_computer_`, `sk_machine_`, `sk_daemon_`) → resolves machine+server → `orchestrator.registerMachine`. Messages → `orchestrator.handleMachineMessage`. |
| AgentOrchestrator | `S/services/agentOrchestrator.ts` (12,537 lines) | Holds per-replica `machineConnections` map, heartbeats (30s tick, 60s timeout, L2103/L3995), agent start/wake/deliver, disconnect grace (2s, L547), cross-replica routing wrappers (L8375–8408). |
| Socket.IO layer | `S/socket/index.ts`, `fanout.ts`, `accessRevocation.ts`, `platformScope.ts` | Human realtime. Redis adapter for cross-replica broadcast; JWT + membership handshake; room scheme; `sync:resume` gap fill; heartbeat with max seq; acknowledged cross-replica revocation. |
| Redis clients | `S/redis.ts` | Four ioredis connections: general, pub + sub (Socket.IO adapter), replica-sub (ReplicaRouter). Redis is optional **for booting** (`REDIS_URL` unset → `isRedisAvailable()` false, most router functions early-return, wake lock always granted). **But daemon attach needs it:** `commitMachineReplicaGeneration` throws `Machine replica owner store is unavailable` without Redis, and `registerMachine` then closes the daemon socket with `1011 replica_registration_failed` (`replicaRouter.ts` L586–588, `agentOrchestrator.ts` L4838–4845; test `agentOrchestrator.deterministic.test.ts` L3499). So "no Redis = working single node" is not true for Computers in the current code. |
| ReplicaRouter | `S/replicaRouter.ts` (1,462 lines) | `REPLICA_ID = randomUUID()` per process (L35). Per-replica channel `slock:replica:{id}`; machine ownership lease keys with generation fencing (Lua scripts); `routeMachineCommand`, `routeInboxDelivery`, `routeInboxDeliveryWithReceipt`; wake lock; agent activity/runtime-error mirrors; per-workspace max seq; Fly instance lookup. |
| MachineResponseRelay | `S/machineResponseRelay.ts` | Request/response over pub/sub: Redis TTL "mailbox" key `slock:machine-reply:{requestId}` + targeted notify + 250ms poll fallback. |
| Machine-local HTTP replay | `S/machineLocalReplay.ts` | For an allowlist of HTTP routes that need the daemon (workspace files, runtime models, computer restart…), proxies the whole HTTP request to the owning replica's private IP, HMAC-signed; or answers `fly-replay` on Fly.io. |
| Built-in app manifest | `S/registry.manifest.ts`, `S/apps/reminder/*`, `S/apps/cleaner/*` | The one place that maps app-owned machine messages (`reminder.fire_request`, `reminder.fire_attempt`) and app source acks to handlers. Apps are "RAP" apps with manifests (`hooks`, `syscalls`, `notifications`). |
| DB layer | `S/db/index.ts`, `S/db/schema.ts` (6,478 lines, 190 tables), `packages/server/drizzle/` (267 migrations `0000`–`0266`) | `pg.Pool` primary (max 50 by default), optional read replica used only for search (`getSearchDb`), PGlite (`pglite://` URL) for embedded/tests, optional RisingWave (`S/db/risingwave.ts`) streaming DB for inbox serving. |
| Deploy migration runner | `S/db/migrateDeploy.ts`, `migrationPhases.ts`, `migrationPreflight.ts` | Splits the drizzle journal into ordered phases so lock-sensitive migrations run in their own transaction after earlier ones commit. |
| Daemon | `packages/daemon/src/connection.ts` (+ many) | Opens `ws(s)://…/daemon/connect` with `Authorization: Bearer <key>` (L232–235), exponential reconnect from 1s; runs agent processes; `agentCredentialProxy.ts` local HTTP proxy for agent CLI calls. |
| Computer CLI / apps | `packages/computer` ("human/local-machine control-plane CLI (login + attach)"), `apps/raft-computer-app`, `apps/raft-desktop-electron`, `packages/desktop-contract` | Install/attach a machine to a workspace; desktop shells. |
| Trace upload worker | `packages/trace-upload-worker` (Cloudflare Worker, `wrangler.toml`, R2) | Accepts daemon trace bundles, web trace batches and feedback reports with scoped short-lived tokens; projects traces into ScopeDB. `raft-event-buffer` batches rows for export. |
| Other clients | `packages/web` (`src/api/socket.ts`), `packages/cli`, `packages/raft-sdk` (typed Agent API client for external agents) | Consumers of `/api` + Socket.IO or `/internal/agent-api`. |
| Dev orchestrator | `raftdev` (bash shim) → `scripts/dev/raftdev.ts` (3,226 lines) | Starts Docker Postgres, Redis, RustFS (S3-compatible), otelcol, optional RisingWave; tmux windows `server`, `server-2..N`, `web`, `daemon`, trace worker. `--replicas N` (cap 8, L174) runs a local cluster. |

## 3. Data model

190 `pgTable` definitions in one file, `S/db/schema.ts`. Grouped by name (table list extracted with grep; groups are my grouping):

| Group | Tables (examples) | Notes |
|---|---|---|
| Identity & sessions | `users`, `user_auth_identities`, `sessions`, `session_families`, `session_token_predecessors`, `session_refresh_rotation_receipts`, `email_verifications`, `password_resets`, `device_authorizations`, `social_auth_completions`, `user_legal_acceptances` | Refresh-token families + rotation receipts; hourly `cleanupExpiredSessions` bounds them (`S/server.ts` L166–177). |
| Workspace ("server") | `servers` (L249: `id, name, slug, kind normal|joint_storage, ownerId, plan free|founder|partner|pro, deletedAt`), `server_members` (L274: `serverId, userId, role owner|admin|member|guest`, onboarding fields), `server_agent_members`, `server_invites`, `server_join_links`, `server_membership_departures`, `server_agreements`, role audit tables | Every tenant-scoped row carries `server_id` with `onDelete: cascade`. |
| Agents & runtime | `agents` (L851: `serverId, name, status active|inactive|stopped, sessionId, model, runtime, runtimeConfig jsonb, executionMode byoc|cloud, machineId → column daemon_id`), `agent_runtime_profiles`, `agent_migrations`, `agent_migration_chunk_receipts`, `agent_activity_events`, `agent_knowledge_events`, `agent_scopes`, `agent_credentials`, `agent_bootstrap_tokens` | An agent is pinned to one machine via `agents.daemon_id`. |
| Machines & Computers | `daemons` (TS name `machines`, L3377: `serverId, userId, apiKeyHash/Prefix/Fingerprint, runtimes json, hostname, os, daemonVersion, computerVersion, lastHeartbeat`), `computers` (L6136: `serverId, apiKeyHash, machineId, revokedAt`), `computer_outage_occurrences` (L6174), `computer_lifecycle_operations`, `computer_lifecycle_dispatches`, `computer_lifecycle_operation_targets` | "Computer" is the newer credentialed attachment wrapping a legacy "machine" row. |
| Conversation | `channels` (L1445: `type channel|private|joint|dm|thread, parentMessageId`), `dm_channel_identities`, `joint_channels`, `joint_channel_servers`, `channel_agents`, `channel_humans` (both with `role` + `authorityRevision`), `channel_membership_role_events` (outbox), `messages` (L1646: `id, seq bigserial, channelId, senderType user|agent|external_projection, senderId, agentSendKey, randomId, content, threadId, searchVector tsvector`), `message_reactions`, `message_mentions`, `mention_delivery_occurrences`, `message_translations`, `thread_follows`, `user_saved` | Threads are channels (`type=thread`). `messages.seq` is a global bigserial used for gap sync. |
| Inbox / read state | `user_channel_read_cursors`, `agent_channel_read_cursors`, `read_mutations`, `read_mutation_authorities`, `read_mutation_tombstones`, `user_channel_inbox_states`, `inbox_serving_rows`, `inbox_notification_facts`, `inbox_suppression_states`, `activity_sync_*` (5 tables) | Some served via RisingWave materialized views (inferred from `S/db/risingwave.ts` naming). |
| Work | `tasks` (L3415: `channelId, taskNumber, status todo|in_progress|in_review|done|closed, claimedByType/Id, revision, messageId`), `task_events`, `workflow_templates`, `workflow_instances`, `workflow_step_instances`, `reminders` (TS name `scheduledFollowups`, L5381), `reminder_events`, `action_cards` | |
| Attachments & storage | `attachments`, `attachment_objects`, `attachment_upload_sessions`, `attachment_upload_reservations`, `attachment_storage_artifacts`, `attachment_object_gc_jobs`, inventory tables, `share_artifacts`, `server_file_upload_usage_months` | Bytes in S3-compatible storage (RustFS locally); Postgres holds metadata and GC jobs. |
| Apps, OAuth, integrations | `oauth_clients`, `oauth_grants`, `oauth_access_tokens`, `oauth_client_installs`, `rap_app_configs`, `managed_mcp_servers`, `managed_mcp_credentials`, `managed_mcp_assignments`, `provider_connections`, `provider_connection_credentials`, `agent_provider_connections` | |
| External bridges (Slack etc.) | ~27 `external_*` tables: `external_app_registrations`, `external_channel_bindings`, `external_inbound_events`, `external_outbound_deliveries`, `external_delivery_attempts`, `external_message_links`, `external_reaction_*`, `external_actor_projections` | Full inbound/outbound outbox with attempts and operator decisions. |
| Notifications & push | `notification_events`, `notification_recipients`, `notification_deliveries`, `notification_delivery_attempts`, `mobile_push_outbox`, `push_registrations`, `push_subscriptions`, `native_notification_*` | |
| Billing, flags, product | `subscriptions`, `webhook_events`, `feature_flags` (+ audiences, rules, versions, audit), `lab_definitions`, `server_lab_*`, `announcements`, `product_events`, `translation_quota_buckets` | |

Redis keys (not durable; `S/replicaRouter.ts`):
- Owner lease, TTL 300s (`MACHINE_REPLICA_TTL`, L92): `slock:machine:{id}:replica | :replica_generation | :replica_updated_at | :cohort | :request_host_class | :request_host_present | :fly | :status_version` (status_version is an INCR counter whose TTL is re-extended on each bump/refresh).
- `slock:replica:{id}:http` (replay endpoint, TTL 300s, rewritten on every register/refresh).
- `slock:machine:{id}:meta` — machine metadata mirror, **TTL 1h** (`MACHINE_META_TTL_SEC = 3600`), not 300s; cleared explicitly on disconnect.
- `slock:agent:{id}:waking` (30s NX lock), `slock:agent:{id}:activity` hash (TTL 10 min), agent runtime-error mirror (TTL 24h), `slock:server:{id}:maxseq`, `slock:machine-reply:{requestId}` (PX = caller timeout).

Lease write semantics (important, easy to get wrong): **register is an unconditional MULTI** that overwrites all owner keys with a fresh UUID generation — last writer wins. Only refresh / unregister / stale-clear are conditional Lua. `REFRESH_MACHINE_OWNER_LUA` also **re-creates** the keys if both replica and generation keys have expired (it only refuses when a *different* (replica, generation) is present), so a live socket's heartbeat resurrects a TTL-expired lease. Because register is unconditional, an older in-flight register can land after a newer one; `registerMachine` detects this (`activeConnection.ws !== ws`) and re-commits the current socket's generation via `restoreMachineReplicaGeneration` / `convergeMachineReplicaAfterStaleCommit` (bounded retries, then fail closed with 1011).

## 4. Flows

### F1. Server replica boot (`S/server.ts`)
1. Process → `initializeTranslationProviderConfig()` (fails closed if SSM config is bad).
2. Process → `initDatabase(DATABASE_URL, DATABASE_URL_READ_REPLICA)` (`S/db/index.ts` L737). `pglite://` URL → in-process PGlite with migrations.
3. Process → `initRedis(REDIS_URL)` if set (4 connections).
4. Process → `createApp(...)` → `http.createServer(app)`; `new AgentOrchestrator(...)`.
5. Process → `resolveReplicaReplayEndpoint(PORT)`: env `SLOCK_REPLICA_REPLAY_BASE_URL`, else ECS task metadata private IPv4 → `http://<ip>:<port>`.
6. Process → `initReplicaRouter(handlers…)`: subscribe `slock:replica:{REPLICA_ID}` + external-wake + principal-fence channels; write `slock:replica:{id}:http`; start 10s subscription health check.
7. `setupMachineWebSocket` then `setupSocket` (order matters: Socket.IO would otherwise swallow the `/daemon/connect` upgrade).
8. Start workers (attachment cleanup/lifecycle sweep, reminder arm watchdog, mobile push outbox, read mutation, app notification, computer outage, agent migration receipt/remediation, channel membership role outbox), metrics server, `server.listen`.

### F2. Daemon connects to a replica
1. Daemon (`packages/daemon/src/connection.ts` L232) → Server: WS upgrade `GET /daemon/connect`, `Authorization: Bearer sk_computer_…`.
2. `S/routes/daemon.ts` `resolveUpgradeAuth` (L75): prefix classify → DB lookup of computer/machine/server. Reject → raw `HTTP/1.1 401` with `Slock-Reason` header.
3. Accept → `sendAuthenticatedMachineContext(ws, {machineId, serverId})` → `orchestrator.registerMachine(...)` (`agentOrchestrator.ts` L4697).
4. Orchestrator: cancel any pending 2s disconnect projection for this machine; fence legacy key if a `computer` principal already holds the machine (checked before **and again after** the conn becomes visible, to close a race with key adoption); `clearMachineConnection(machineId, false)` (drops the old local socket **without** unregistering Redis — a same-replica reconnect is a handoff, not an ownership drop); build `MachineConnection{connectionEpochId, replicaGeneration:null, …}`, start heartbeat, put it in `machineConnections`.
5. `replicaStateStore.registerMachineReplica` → unconditional Redis MULTI sets owner keys with new random `generation`, TTL 300s (`replicaRouter.ts` L558–603). If this socket was superseded while the write was in flight, repair the successor's generation and close this one with `superseded_connection`. If the write fails → close with `1011 replica_registration_failed`, never announce online.
6. **Only after the owner key is committed**: bump `status_version`, emit Socket.IO `machine:status {status:"online", statusVersion}` to `server:{sid}`, record Computer outage recovery (`recordComputerOnlineTransition`). Comment at L4776–4787: emitting online before the Redis commit made cross-replica REST reads return offline and the web client latched offline.
7. Daemon → Server: `ready {runtimes, runtimeVersions, runningAgents, daemonVersion, computerVersion, migrationTransport, hostname, os}`. The orchestrator reconciles each agent against `runningAgents` with `planReadyReconcileAction` → one of `mark-active-online`, `mark-wakeable-not-running` (then maybe wake for pending inbox), `mark-inactive-offline`, `stay-offline`, `force-stop-and-stay-offline`, or request start (`agentOrchestrator.ts` L6314–6500).
8. Heartbeat is **application-level JSON** (`{type:"ping"}` / `{type:"pong"}` over the WS, not WS ping frames), every 30s (L3940, L3995). Pong → `lastPong`, DB `machineService.updateHeartbeat` (the `daemons.last_heartbeat` column) and `refreshMachineReplica` Lua (refreshes only if replica **and** generation match, or keys are fully absent). Any other ingress frame also counts as liveness proof and refreshes the lease, throttled to once per 15s (`INGRESS_REPLICA_REFRESH_MIN_INTERVAL_MS`). No proof (max of lastPong, lastIngressAt) for 60s → `ws.terminate()` (TCP RST, for half-open links) + `handleMachineDisconnect(cause: heartbeat_timeout)`.

### F3. Daemon disconnects
1. `ws.on("close"|"error")` → `orchestrator.handleMachineDisconnect(machineId, ws, ctx)` (L5594).
2. Ignore if `conn.ws !== ws` (stale socket) or no conn (duplicate close+error).
3. Emit internal `machine:disconnect:{id}`; `clearMachineConnection(machineId, false)` — removes the local socket and timers but **does not touch Redis yet**; dispatch computer lifecycle disconnect observation (fire-and-forget with timeout).
4. Schedule `applyMachineDisconnectProjection` after `MACHINE_DISCONNECT_PROJECTION_GRACE_MS = 2000`. A reconnect **to the same replica** within 2s calls `cancelPendingMachineDisconnect` in `registerMachine` and the UI never sees "offline". (A reconnect to a *different* replica does not cancel it; that replica simply overwrites the lease with a new generation, and this replica's later CAS unregister no-ops.)
5. When the timer fires (L5703): `commitMachineReplicaDisconnectState` → `unregisterMachineReplica(machineId, expectedGeneration)` (Lua deletes only if replica and generation still match) + `clearMachineMeta`, bounded by a timeout. If a socket reconnected meanwhile, re-register a fresh generation and stop (repair path).
6. Otherwise: `recordComputerOfflineTransition` (creates a `computer_outage_occurrences` row or `suppressed_planned` when the daemon sent a shutdown intent); for each agent on the machine release its wake lock and, if `active`, apply a lifecycle projection (disconnect vs shutdown reducer) that keeps enough state for a later lazy wake; emit `machine:status {status:"offline"}` to `server:{sid}`.

### F4. Human posts a message (realtime part)
1. Browser → any replica: `POST /api/messages` (v1) or the v2 router (both → `createHumanMessage`, `routes/messages.ts` L1752–1753; 60 msg/min/user rate limit in `app.ts` L508), JWT + `X-Server-Id`, `requireServer` checks membership.
2. `messageService.createMessage` persists row (`messages.seq` bigserial) in Postgres. **Everything after this is post-commit and best-effort**: `emitFrontendSocketBestEffort` swallows Socket.IO failures because "the canonical message is already durable… consumers recover from the persisted message stream" (`messageService.ts` L2501–2531). Realtime is an accelerator; `sync:resume` (F9) is the correctness path.
3. For DM/thread: `io.in(user:{uid}:server:{sid}).socketsJoin(channel:{cid})` grants rooms to the right sockets on **all** replicas (Redis adapter) (`messageService.ts` ~L2776–2789).
4. `updateMaxSeq(serverId, seq)` → local cache + `updateMaxSeqRedis` Lua compare-and-set.
5. `io.to("channel:{cid}").emit("message:new", payload)` — Redis adapter fans out to every replica's sockets in that room.
6. Agent delivery: `deliverMessagesToAgents` (`messageService.ts` L3535) loads `channel_agents` for the channel, drops the sender itself, drops agents that muted the channel **unless** they are @-mentioned or are the task assignee ("pierce"), renders an `AgentMessage` per agent (channel, sender, `mentioned`, content, timestamp) and hands each to the orchestrator (F5). Every agent member of the channel gets the message by default — delivery is push-to-all-members, not mention-only.

### F5. Deliver a message to an agent whose daemon is on another replica
1. Replica A orchestrator: agent's `machineId` not in `getRoutableLocalMachineIds()` → `routeInboxDelivery` or `routeInboxDeliveryWithReceipt` (`replicaRouter.ts` L1015/L1048).
2. A → Redis `GET slock:machine:{mid}:replica` → B.
3. Fire-and-forget: A → Redis `PUBLISH slock:replica:B {type:"inbox:deliver", agentId, machineId, payload}`.
   Receipt variant: A registers `pendingInboxDeliveryReceipts[requestId]` with 3s timeout, publishes `{type:"inbox:deliver:receipt", requestId, replyReplicaId:A, deliveryOptions, payload}`. PUBLISH returning 0 receivers → immediate `dropped`.
4. B `handleReplicaMessage` → `orchestrator.handleRoutedInboxDelivery[WithReceipt]` → re-checks agent active, access to target, still local → `deliverMessage(..., requireQueueReceipt)` → WS frame to daemon.
5. B → Redis `PUBLISH slock:replica:A {type:"inbox:receipt", requestId, payload:{status:"queued"|"dropped", reason}}` → A resolves the pending promise.

### F6. Agent wake/start across replicas
1. Orchestrator start path → `acquireWakeLock(agentId)`: `SET slock:agent:{id}:waking REPLICA_ID EX 30 NX` (`replicaRouter.ts` L1131).
2. Lock held elsewhere → outcome `skipped: wake_lock_held` (or throws `CrossReplicaQueueReceiptUnavailableError` if caller needs a receipt) (`agentOrchestrator.ts` ~L9703).
3. Command to daemon on another replica → `routeMachineCommandWithResult` → `PUBLISH {type:"machine:command"}`. 0 receivers → `maybeCleanupStaleMachineOwner` (only if owner age ≥ 60s) with CAS Lua.

### F7. HTTP request that must run on the daemon's replica (machine-local replay)
1. Browser → Replica A: e.g. `GET /api/servers/:sid/machines/:mid/workspaces` (`routes/servers.ts` ~L2821+). The allowlist (`machineLocalReplay.ts` L40–60, 16 entries) is **not only reads**: it includes `POST /api/agents` (create), `PATCH /api/agents/:id`, `POST /api/agents/:id/start`, `assign-machine|migrate`, `DELETE` machine, computer `restart|upgrade`, lifecycle operations, workspace delete — i.e. anything whose handler must touch the live daemon socket.
2. A → `handleMachineLocalRouting(req,res,mid, isLocal)` (`machineLocalReplay.ts` L106). Not in allowlist → `not_routed` (handler runs locally).
3. Read owner via Lua `READ_MACHINE_REPLAY_TARGET_LUA` → `{replica, endpoint, generation, updatedAt}`. If the owner is *this* replica and the socket is local → `confirmed_local`, handler runs. If the owner key says "this replica" but no local socket exists, CAS-clear that unbacked owner and (GET/HEAD only) reread once.
4. No endpoint → sleep 2s handoff window (`SLOCK_MACHINE_OWNER_HANDOFF_WAIT_MS`), reread once.
5. A → B: `fetch(endpoint + originalUrl)` with headers `x-raft-replica-replay`, `-machine`, `-timestamp`, `-signature` (HMAC-SHA256 with `SLOCK_REPLICA_REPLAY_SECRET` or `JWT_SECRET`, max age 5 min), 8s timeout (`SLOCK_REPLICA_REPLAY_TIMEOUT_MS`).
6. B verifies signature; if socket is local handles it; if not → 409 `machine_owner_not_local`.
7. A on 409/timeout/network error → CAS-clear the exact owner snapshot (replica, generation, updatedAt, endpoint). Non-idempotent methods (POST/PATCH/DELETE) → fail immediately (504 on timeout, 503 otherwise). GET/HEAD → reread owner once and replay once more **only if the new snapshot differs** (different replica/generation/version); no chains beyond one successor (L293–380).
8. On Fly.io with no endpoint: `res.set("fly-replay", "instance=<id>")` 307 → Fly's proxy re-routes.

### F8. Request/response to a daemon across replicas (MachineResponseRelay)
1. Replica A `machineResponseRelay.request({requestId, machineId, type}, timeout, send)`: `SET slock:machine-reply:{rid} <request> PX timeout NX`; then `send()` (routes command to B).
2. Daemon → B: `machine:runtime_models:result` (or workspace archive result).
3. B `forward()` → Lua `WRITE_REPLY`: identity check, first response wins, `KEEPTTL`; returns `replyReplicaId`.
4. B → `PUBLISH slock:replica:A {type:"machine:response:ready", requestId}`.
5. A `consume()` reads mailbox, validates ids, resolves. If the notify is lost, A's 250ms poll reads the mailbox anyway. Timeout → `RouteFailureError("daemon_timeout")`.

### F9. Socket.IO client connect + resume
1. Web (`packages/web/src/api/socket.ts`) → any replica: handshake `auth:{token, serverId, clientKind}`.
2. `io.use`: `verifyToken` synchronously, add to `pendingHandshakes`, `verifyActiveAccessToken` (DB), `serverService.isMember`, resolve role.
3. On connection: join `user:{uid}`, `user:{uid}:clientKind:{kind}`, `server:{sid}`, `user:{uid}:server:{sid}`, and every `channel:{id}` for listed channels + DMs; then emit `rooms:joined`.
4. Client → `sync:resume {lastSeq}` → `messageService.syncMessages(lastSeq, all channels, 500, serverId, userId)` → `sync:resume:response {messages, currentSeq, hasMore}` (`socket/index.ts` L244–269). `hasMore` just means "hit the 500 limit"; the client has to ask again. `lastSeq <= 0` is ignored.
5. Every 15s server → `heartbeat {seq: maxSeq, ts}`; the client compares seq to detect that it is behind (inferred client side). Max seq per workspace syncs from Redis every 5s, ref-counted per replica (`messageService.ts` L6628–6680).
6. Later room changes: client `join:channel {channelId}` (server re-checks `canUserAccessChannel`, fails closed) / `leave:channel`; server-side grants use `socketsJoin` (F4 step 3).

Note: `messages.seq` is **one global bigserial across all workspaces**, not per workspace or per channel. Seq values seen by one workspace have holes, so "gap" means "maxSeq > my lastSeq", not "non-contiguous". Inferred risk (not verified in code): bigserial values are assigned at insert, not at commit, so a lower seq can commit after a higher one has been observed.

### F10. Access revocation (e.g. user removed, guest policy change)
1. Service after DB commit → `revokeSocketAccess(revocation)` (`socket/accessRevocation.ts`).
2. Local listener → `evict()` closes matching sockets **including pending handshakes**; then `fanoutWithAck(io, "access:revoked", revocation)` → `io.serverSideEmitWithAck` over Redis, 10s cap, throws if publisher not `ready` (`socket/fanout.ts`).
3. Other replicas' `io.on("access:revoked")` evict and ack.

### F11. Outbox worker drain (every replica runs it)
1. `startChannelMembershipRoleOutboxWorker` interval tick every 5s (skips if previous tick still running).
2. `SELECT … WHERE delivery_status='pending' AND delivery_attempts < 5 ORDER BY created_at LIMIT 50`.
3. Claim by CAS: `UPDATE … SET delivery_attempts = n+1 WHERE id=? AND status='pending' AND delivery_attempts = n RETURNING` — losing replicas get no row. The claim *is* the attempt increment; there is no lease/claimed_by column.
4. Emit `channel:members-updated` (to `channel:{id}` for private channels, else `server:{sid}`) and `channel:authority-updated {channelRole, authorityRevision}` to the target's personal room; then mark `sent` with a CAS on (`pending`, attempts = n+1). On exception: back to `pending` with `last_delivery_error`, or `dead_letter` when attempts reached 5 (`services/channelMembershipRoleOutbox.ts` L11–85).
5. Semantics: **at-least-once**. If the process dies between emit and the `sent` update, the row stays `pending` and another replica re-emits; clients must tolerate duplicates (the `authorityRevision` makes the authority event idempotent). Other workers use `FOR UPDATE … SKIP LOCKED` instead (`readMutationSequencer.ts` L857; also attachment lifecycle, storage, external reaction/attachment workers).

### F12. Graceful shutdown (`S/serverShutdown.ts`)
1. SIGTERM/SIGINT → stop all 10 background workers (`server.ts` L214–227).
2. Start `server.close()` (stop accepting HTTP) **synchronously before** releasing machine ownership, so no new request lands after the owner key is gone.
3. In parallel (`Promise.allSettled`): `agentOrchestrator.shutdown()` + Slack bridge stop, and flush traces. Orchestrator shutdown clears timers and **drops all pending 2s disconnect projections** (so no "offline" is projected for a planned restart), then for every local machine `unregisterMachine` → close the daemon socket and CAS-unregister its lease. Daemons reconnect (exponential backoff from 1s) and land on another replica, which mints a new generation.
4. `shutdownRedis()`; `process.exit(0)`. A 5s `setTimeout(process.exit)` is armed as a backstop if `server.close()` hangs.

## 5. State machines

- **Machine ownership lease (Redis)**: `absent` → `owned(replica R, generation G)` on register (unconditional overwrite) → refreshed (TTL 300s) only by matching (R,G), or re-created by a refresh if all keys had expired → `absent` by (a) CAS unregister after the 2s disconnect grace or on shutdown, (b) CAS stale cleanup when publish has 0 receivers and owner age ≥ 60s, (c) CAS clear of the exact (R,G,updatedAt,endpoint) snapshot after failed replay/409, or of an "unbacked" owner that names this replica with no local socket, (d) TTL expiry. A new connection anywhere overwrites with (R', G'); older refresh/unregister calls then no-op. A stale register that lands late is repaired by the current owner re-committing its own G.
- **Daemon connection (per replica, orchestrator)**: `authenticating` (upgrade) → `registered_local` (in `machineConnections`, `replicaGeneration=null`, not yet request-ready) → `owner_committed` (Redis write done; `online` emitted) → (close/error/heartbeat_timeout) → `pending_disconnect` (2s grace, Redis lease still held) → `offline projected` (lease CAS-deleted, outage row, agents projected, `offline` emitted). `pending_disconnect` → back to `registered_local` if the daemon re-registers **on the same replica** within 2s. Side exits: owner commit failed → closed `1011 replica_registration_failed`; superseded by a newer socket → closed `superseded_connection`; legacy `sk_machine_` socket → `fenced` if a `computer` principal owns the machine or the legacy key was migrated.
- **Agent status** (`agents.status`): `inactive` (default) ↔ `active` ↔ `stopped`. This is the durable lifecycle column only; online/offline and working/idle are separate projections (Redis activity mirror, in-memory orchestrator state) recomputed on `ready` reconcile and on disconnect projection.
- **Machine-local routing** — function result: `not_routed` | `confirmed_local` | `handled`. Trace label (`MachineAffinityRoute`): `not_allowed` | `local` | `aws_replay` | `fly_replay` | `owner_missing` | `owner_not_local` | `timeout` | `replay_failed` (`machineLocalReplay.ts` L62).
- **Routed inbox receipt**: `pending` → `queued` | `dropped(reason)`; timeout 3s → `dropped: cross_replica_receipt_unavailable`.
- **Outbox row** (`channel_membership_role_events.delivery_status`): `pending` → `sent` | `dead_letter` (after max attempts).
- **Computer outage occurrence**: `pending` → `notified` | `recovered` | `suppressed`.
- **Computer lifecycle operation**: `status pending → completed | failed | unconfirmed | superseded | rolled_back`; `dispatch_status pending → sent`; `dispatch_mode local|server`.
- **Task** (`tasks.status`, `VALID_TRANSITIONS` in `services/taskService.ts` L541–547):
  - `todo → in_progress | closed` (no skipping straight to `done`/`in_review`)
  - `in_progress → in_review | done | closed`
  - `in_review → done | in_progress (send back) | closed`
  - `done → todo | in_progress | in_review | closed` (reopen or retroactively abandon)
  - `closed → todo | in_progress` (`closed` = non-success terminal, distinct from `done`)
  Every mutation bumps `tasks.revision`; writers pass `expectedRevision` (optimistic CAS). Claiming sets `claimedByType/Id/At`. Tasks that create external resources can require a structured `resourceReceipt` before closing.

## 6. Design patterns worth stealing

1. **Connection-owner lease with generation fencing.** `registerMachineReplica` mints a UUID generation and overwrites unconditionally; refresh/unregister/cleanup are Lua CAS on (replica, generation[, updatedAt]) (`replicaRouter.ts` L111–194, L558–640). Why: a slow async delete from an old socket can't erase a newer registration. Because the *write* is last-writer-wins, the owner also re-checks after the await and repairs if a stale register overwrote it. Plus: announce "online" only after the lease commit. For you: if an AgentCore session/Temporal worker "owns" a live agent, store owner + generation and make every mutation other than "I am the new owner" conditional.
2. **Per-replica inbox channel + typed wire messages.** Each process subscribes to `slock:replica:{uuid}`; senders look up the owner and PUBLISH a discriminated union (`machine:command`, `inbox:deliver`, `inbox:deliver:receipt`, `inbox:receipt`, `machine:response:ready`). No load balancer stickiness needed (`raftdev.ts` L151–162 says so explicitly).
3. **PUBLISH receiver count is not an ack.** Receipt-required delivery waits for the target to run its gate and answer `queued|dropped` (`replicaRouter.ts` L1041–1128). New semantics get a new wire type (`inbox:deliver:reconcile-receipt`) so an old replica ignores it and the sender times out closed instead of false-acking (test `replicaRouter.inboxReceipt.test.ts` L133).
4. **Mailbox + notify + poll for RPC over pub/sub** (`machineResponseRelay.ts`). Pub/sub is lossy; the Redis TTL key is the truth, the publish is just an accelerator, the poll is the floor. First-writer-wins in Lua.
5. **Content-free wake hints.** External-agent wake broadcast carries only `agentId`; receivers re-read durable state, so lost/duplicate signals can't break correctness; a 25s heartbeat peek is the floor (`replicaRouter.ts` L77–86).
6. **HTTP replay to the owner, signed, with a bounded discovery budget** (`machineLocalReplay.ts`). Allowlisted routes only; GET retried once, POST never replayed after ambiguity; 2s handoff wait to absorb graceful reconnects.
7. **Fail-closed route auth registry** (`routeAuthPolicy.ts`): each internal path declares its principal type; unknown paths 401. Separate key prefixes per principal (`sk_agent_`, `sk_computer_`, `sk_machine_`).
8. **Rooms as the authorization boundary for realtime**, with a composite `user:{u}:server:{s}` room because Socket.IO `in(a).in(b)` is a union, not an intersection (`socket/platformScope.ts` L27–33).
9. **Monotonic seq + resume**: global `messages.seq` bigserial, `sync:resume{lastSeq}` returns up to 500 missed, server heartbeat carries max seq so clients detect gaps.
10. **Outbox tables drained by every replica with CAS/SKIP LOCKED claims**, attempts counter, dead letter. No leader election needed. Delivery is at-least-once; payloads carry revisions so duplicates are harmless.
11. **Degrade when Redis is absent** — partially: router functions early-return (`isRedisAvailable()`), `acquireWakeLock` returns true, and the process boots on PGlite. But machine owner registration fails closed without Redis, so daemons are rejected (`1011 replica_registration_failed`). The pattern is "fail closed on the one thing that needs cross-replica truth", not "everything works single-node".
14. **Durable write first, realtime as best-effort accelerator.** Socket.IO emits after the message commit swallow errors; clients converge via `sync:resume` + heartbeat max seq (`messageService.ts` L2501–2531).
15. **Reconcile on reconnect.** The daemon's `ready` frame lists its running agents; the server diffs that against DB state and picks an explicit action per agent (`planReadyReconcileAction`). Maps directly to "Temporal/AgentCore worker comes back → report what's actually running → server reconciles".
12. **Drain HTTP before releasing ownership** on shutdown (`serverShutdown.ts`).
13. **Built-in app manifest as the only place app vocabulary appears** (`registry.manifest.ts`): generic transport asks `resolveBuiltInMachineMessageDispatch(msg)`; reminder due-time authority is on the Computer, the server only validates fire requests and holds identity.

## 7. Surprises / sharp edges

- The `machines` table is physically named `daemons`, and `agents.machineId` maps to column `daemon_id`; `reminders` table is exported as `scheduledFollowups` (`schema.ts` L3377, L870, L5381).
- The Node backend is a large monolith: `agentOrchestrator.ts` is 12.5k lines, `messageService.ts` ~9.3k, 507 files in `services/`. The orchestrator holds real in-memory state (sockets, timers), so replicas are not stateless — only their HTTP handlers are.
- Two WebSocket stacks share one HTTP server: raw `ws` for daemons, Socket.IO for humans. Registration order in `server.ts` L149–153 is load-bearing.
- Replica replay endpoint discovery depends on ECS task metadata (`ECS_CONTAINER_METADATA_URI_V4`) or an explicit env var; on Fly.io it uses `fly-replay` instead. Two hosting paths are coded side by side.
- The replay HMAC falls back to `JWT_SECRET` if `SLOCK_REPLICA_REPLAY_SECRET` is unset (`machineLocalReplay.ts` L503–505).
- `REPLICA_ID` is a fresh UUID per process start, so a restarted replica is a new identity; stale owner keys are cleaned lazily (0-receiver publish + age ≥ 60s) or by TTL (5 min).
- The fire-and-forget `routeInboxDelivery` returns `true` after PUBLISH even if the target dropped it; only the receipt variant gives a real answer.
- `fanoutWithAck` throws if the Redis publisher is not `ready` and callers "run after a DB commit", so they must surface a retryable error (`socket/fanout.ts`).
- Socket handshake sets `socket.data.userId` **before** the DB check so a revocation during validation can still catch a pending handshake (`socket/index.ts`).
- Search queries can use a separate read replica (`DATABASE_URL_READ_REPLICA`), but nothing else does.
- `unhandledRejection` is logged, not fatal (`server.ts` L3–6).
- 267 drizzle migrations; deploy runs them in phases with lock timeouts (`migrationPhases.ts`), and `drizzle.config.ts` notes that `?statement_timeout=` is silently dropped by the Neon PrivateLink pooler — the production DB is (inferred) Neon Postgres.
- `raftdev --replicas N` runs N server windows on one DB/Redis with no load balancer; the daemon always connects to replica 1 locally.

## Verification log

Checked against code (fact-check pass):
- Counts: 190 `pgTable`, 267 migrations, `server.ts` 305 / `app.ts` 665 / `replicaRouter.ts` 1,462 / `agentOrchestrator.ts` 12,537 lines, 507 service files: confirmed. `messageService.ts` is 9,318 lines, not ~6.7k: fixed.
- `REPLICA_ID = randomUUID()`, `MACHINE_REPLICA_TTL=300`, stale-cleanup min age 60s, receipt timeout 3s, subscription health 10s, wake lock `EX 30 NX`: confirmed.
- Redis-optional claim: **wrong for daemons**. `commitMachineReplicaGeneration` throws without Redis; `registerMachine` closes 1011. Fixed in section 1, Redis row, pattern 11.
- Lease semantics: register is an unconditional MULTI, not CAS; refresh Lua re-creates expired keys; stale-register repair path exists. Added. `:meta` key TTL is 1h, not 300s; added `request_host_*` keys, activity/runtime-error TTLs.
- F2: added owner-commit-before-online ordering, superseded/failed-commit exits, `ready` reconcile actions, app-level JSON ping/pong, `terminate()`, 15s ingress refresh throttle, DB `last_heartbeat` write.
- F3: Redis unregister happens **after** the 2s grace (inside the projection), not at close; grace is only cancelled by a same-replica reconnect. Added outage row, per-agent lifecycle projection, wake-lock release, `offline` emit.
- F4: realtime emit is best-effort post-commit; added agent fan-out rules (all channel agents, mute pierced by mention/task assignee), v1/v2 routes, rate limit.
- F5 receipts, reconcile-receipt wire type, 0-receiver → dropped: confirmed.
- F7: allowlist includes mutating POST/PATCH/DELETE routes; retry is GET/HEAD only, once, and only to a *different* owner snapshot; failure codes 504/503; unbacked self-owner clear. Fixed. Routing result vs trace label split.
- F8 mailbox (`PX NX`, `KEEPTTL`, 250ms poll, `daemon_timeout`): confirmed.
- F9: 15s heartbeat, 500 resume limit confirmed; added `join:channel`, global (not per-workspace) seq, commit-order caveat (inferred).
- F10 `fanoutWithAck` 10s cap / throws unless publisher `ready`; composite user:server room rationale: confirmed.
- F11: added 5s/50/5 constants, at-least-once semantics, target rooms.
- F12: added that shutdown drops pending disconnect projections and closes+CAS-unregisters each daemon; 5s is a backstop timer. Daemon backoff (1s doubling to max) confirmed in `daemon/src/connection.ts` L169, L405.
- State machines: task transitions replaced with actual `VALID_TRANSITIONS` + revision CAS; daemon connection states expanded; agent status clarified; outage and lifecycle-operation enums confirmed in `schema.ts` L6180, L6213–6218.
- Not re-verified: route auth registry details, trace worker, raftdev details other than `MAX_REPLICAS = 8` and the no-LB comment.
