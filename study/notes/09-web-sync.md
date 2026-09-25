# 09 — Web client, realtime sync and shared contracts

## 1. What it is

The web client (`packages/web`) is a React app whose state lives in module-level Zustand stores (`packages/web/src/store/*`). One Socket.IO connection per tab delivers "wake signals" and small facts (new message, read-state changed, agent activity). HTTP endpoints stay the authority: whenever the client suspects it missed something, it pulls a snapshot or a "since seq N" page. On top of that ad-hoc layer, Raft is moving domains onto `packages/sync-core`, a pure, IO-free state machine that folds frames, snapshots and difference pages per scope and outputs declarative repair requests. The same contracts are meant to be shared with a Kotlin mobile client (`packages/sync-core/contracts/activity-v1/*`, `packages/shared/src/canonicalMessageManifest.json`).

Core rule, stated in the server code: "the push is best-effort (drop/delay/reorder) and MUST NOT be load-bearing for correctness — convergence is owed to write-through + pull" (`packages/server/src/services/agentOrchestrator.ts` ~12505, contract "CC-006"). The same idea appears above the Activity routes: "Existing socket events remain wake signals; clients hydrate/repair through these exact snapshot/difference reads rather than treating socket payloads as a second source of truth" (`packages/server/src/routes/channels.ts` ~973).

## 2. Key components

| Name | File path(s) | Responsibility |
|---|---|---|
| Socket singleton | `packages/web/src/api/socket.ts` | Creates the `socket.io-client` instance (websocket only, `forceNew`), attaches `auth = {token, serverId, clientKind:"web"}`, handles token refresh on `connect_error` and before each `reconnect_attempt`. |
| Socket bridge | `packages/web/src/store/socketBridge.ts` | Installs all realtime bindings once per socket (`installSocketBridge`, idempotent per name), maps event names to store actions (a `const` list of 37 names at ~120-185, including `connect`; names only, no payload types), runs reconnect/resume/heartbeat logic and the page-visibility recovery. |
| Server socket entry | `packages/server/src/socket/index.ts` | Handshake auth, room joins (`user:`, `server:`, `channel:`), emits `rooms:joined`, answers `sync:resume`, emits `heartbeat` every 15 s, `join:channel` (re-checks `canUserAccessChannel`, fails closed) / `leave:channel`, access-revocation eviction. Redis adapter for multi-replica fan-out. |
| Message store | `packages/web/src/store/messageStore.ts` (2751 lines) | Per-channel message buckets, global `lastSeq`, unread counts, optimistic rows, `syncGap`, `sendMessage`, pending-read queue. |
| Optimistic draft | `packages/web/src/components/message/optimisticMessageDraft.ts`, `MessageInput.tsx` ~1987-2070 | Mints `optimistic-…` id and `msg-…` randomId; inserts the optimistic row; removes it on failure. |
| Read-state ledger | `packages/web/src/store/readStateSync.ts` | Versioned register per `(serverId, scopeId)`; accepts socket updates and HTTP snapshots; generation counter prevents HTTP responses from rolling back socket updates. |
| Inbox / Activity (legacy) | `packages/web/src/store/inboxStore.ts` | Loads `/channels/inbox`, applies read projections and thread replies, request-id and revision fences. |
| Activity panel on sync-core (shadow) | `packages/web/src/store/activityPanel/{gate,bootstrap,runtime,host,consumer,windowAuthority,projection}.ts` | Feature-gated (`VITE_ACTIVITY_SYNC_CORE_MODE` = off/shadow/on) consumer that runs the Activity domain of sync-core against `/channels/activity/snapshot|difference`. Off by default. |
| Sync Core | `packages/sync-core/src/{core,types}.ts` | Pure deterministic fold/gap/epoch machine; `ingestFrame / ingestSnapshot / ingestDifference / pendingRequests / violations`. Re-exported from `@botiverse/raft-shared` (`packages/shared/src/index.ts:242`). |
| Sync Core domains | `packages/sync-core/src/domains/activity.ts`, `read-state.ts`; web: `messageSyncDomain.ts`, `threadRepliesSyncDomain.ts`, `notificationPrefsSyncDomain.ts` | Domain folds: activity (contiguous), read_state / messages / thread replies / notification prefs (sparse). |
| Activity contract v1 | `packages/sync-core/contracts/activity-v1/activity-sync.tsp`, `fixtures/*.jsonl`, `runner/runBehaviorVectors.ts` | TypeSpec source generating TS binding, JSON Schema, OpenAPI; behavior vectors shared with a Kotlin canary. |
| Server Activity authority | `packages/server/src/services/activitySyncService.ts` | Reconcile-on-read: rebuilds the window from canonical inbox data, diffs, appends to a bounded change log, returns snapshot / difference / notModified / 409 snapshotRequired. |
| Canonical message manifest | `packages/shared/src/canonicalMessageManifest.{ts,json}` | Field list, wire type, nullability, merge policy and presence per producer surface for `message:*` payloads. |
| Producer registry | `packages/server/src/services/messageRealtimeProducerRegistry.ts`, `messageRealtimeEvents.ts` | Closed list of every `message:new`/`message:updated` emitter and its allowed payload keys; allowlisted socket projection. |
| Read frontier contract | `packages/shared/src/inboxScopeReadFrontier.ts` | `absent | corrupt | present` union every unread-reporting HTTP exit must use. |
| Server reset registry | `packages/web/src/store/serverResetRegistry.ts` | Every server-scoped store registers a reset; switching server fans out to all. |
| Ingress context | `packages/web/src/store/receiverPrivateIngress.ts` | Captures `{serverId, principalId, serverEpoch, generation}` before a request; late responses from an old server/user are dropped. |
| Desktop contract | `packages/desktop-contract/src/{ipc,capabilities,uri,manifest}.ts` | Typed, validated IPC (handshake, window.focus, bindServer…), `raft://v1/...` URI parser, capability ids. |
| Electron shell | `apps/raft-desktop-electron/src/app/{index,preload,computerHost}.ts` | `window.raftDesktop` bridge (badge, focus, OAuth loopback, local Computer host control). |
| Event buffer | `packages/raft-event-buffer/src/{core,http,signalDrain}.ts` | In-memory batching buffer for trace telemetry rows (`POST /internal/v1/batches`), not part of UI sync. |

## 3. Data model

**messages** (`packages/server/src/db/schema.ts:1646`)
- `seq bigserial` — server-global, one Postgres sequence across all channels and servers ("Messages with server-global seq for reliable delivery", schema.ts:1645). Indexes `(channel_id, seq)` and `(seq)`.
- `random_id` — client idempotency key; unique partial index `(sender_id, random_id) where sender_type='user'` (schema.ts:1703). Agents use `agent_send_key` with an equivalent index.
- `thread_id` is set on the thread *parent*, not on replies (comment schema.ts:1659).

**user_channel_read_cursors / agent_channel_read_cursors** (schema.ts ~3667, ~4155)
- PK `(user_id|agent_id, channel_id)`, `last_read_seq int`, `last_read_seq8 bigint` (RFC 057 shadow for int4→int8 widening), `read_state_version int`, `last_applied_authority_seq`.
- Version increments by +1 only when the seq actually moves. There are two directions: `forward` (mark read: `ON CONFLICT … DO UPDATE … WHERE EXCLUDED.last_read_seq > current`, `readMutationSequencer.ts:1270-1284`) and `backward` (mark unread, `POST /channels/:id/unread`: `WHERE EXCLUDED.last_read_seq < current`, ~1286+). So `readStateVersion` only goes up, but `maxReadSeq` can go **down**. That is why clients order read facts by version, not by seq. A write that changes nothing does no UPDATE and does not bump the version.
- Read mutations are themselves durable rows (`read_mutations`, states `admitted | executing | applied | retired_no_effect`, executing rows hold a lease `leaseExpiresAt`; a live executing predecessor blocks later admitted work for the same scope, ~760-880).

**Activity sync tables** (schema.ts 3993-4152)
- `activity_sync_principal_authorities (server_id, principal_id, row_version)` — one row-version allocator per user, shared across all filters.
- `activity_sync_row_authorities (…, row_id, last_version, active, payload_digest)`.
- `activity_sync_scopes (server_id, principal_id, filter ∈ all|unread|mentions, window_id='main', epoch, watermark, window_size=30, scope_digest, metadata)`.
- `activity_sync_rows (…, row_id, row_version, active, payload, payload_digest, tombstone_reason ∈ done|deleted|outOfWindow)`.
- `activity_sync_changes (…, seq, row_id, row_version, kind ∈ upsert|tombstone|scope, payload, tombstone_reason)` — bounded dense repair log; PK includes `seq`; CHECK constraints enforce the shape per kind.

**Sync-core types** (`packages/sync-core/src/types.ts`)
- `SyncFrame {scopeId, seq: bigint, epoch, event}`; `SyncSnapshot {scopeId, watermark, epoch, state}`; `SyncDifferenceResponse {fromSeq, toSeq, events[], partial?, snapshotRequired?}`.
- `SyncDensity = "contiguous" | "sparse"`.
- `SyncRequestDescription` = `difference{sinceSeq, epoch}` | `snapshot{reason: epoch_mismatch|snapshot_required|initial|sparse_repull}`.
- Violations: `producer_seq_conflict | cross_epoch_arrival | stale_flood | version_regression | producer_version_conflict`, kept in a ring buffer (default capacity **128**, `violations.ts:8`) with `droppedCount`. `core.ts` itself only emits `cross_epoch_arrival`, `version_regression` and `producer_version_conflict`; `stale_flood` is declared in the type and mapped by web domains/tracing but never produced by the core.

**Activity row (wire)** (`activity-sync.tsp`): common fields `rowId, rowVersion (UInt64String), latestActivitySeq, lastActivityAt, unreadCount, hasMention, firstUnreadMessageId, firstMentionMessageId, maxReadSeq, readStateVersion`; discriminated by `type: channel | dm | thread`. UInt64 values travel as canonical decimal strings (`^(0|[1-9][0-9]*)$`).

**Client stores (web)**
- `messageStore`: `channelMessages: Record<channelId, Message[]>`, `messages` (mirror of current channel), `lastSeq`, `unreadCounts`, `mentionFlags`, `hasGap/loadingGap/hasNewer`, `channelWindowMeta`; module maps `pendingGapMessages`, `pendingReadSeqs` (persisted to `sessionStorage` key `slock_pending_read_seqs`).
- `readStateSync`: `acceptedReadStates: Map<"server:scope", {maxReadSeq, readStateVersion, generation}>`.
- `agentStore.agentActivitySeq: Record<agentId, number>` for push dedup.

## 4. Flows

### F1. Connect, room join, and resume

1. `MainLayout` mounts → `useMainLayoutRealtimeBridge(driver)` → `bootstrapMainLayoutRealtimeBridge` (socketBridge.ts ~335): `reconnectSocket()` plus parallel HTTP loads (`loadUnreadCounts`, `loadChannels`, `loadDMChannels`, `loadAgents`, `loadMachines`, followed threads, inbox, saved, announcements).
2. `install()` → `attachMainLayoutSocketBridge` → `installSocketBridge(socket, "main-layout", bindings)` + `socket.onAny(markServerActivity)` + `registerTaskRealtimeHandlers`.
3. Web `connectSocket()` (`api/socket.ts`) sets `socket.auth = {token from localStorage, serverId, clientKind:"web"}` and connects over websocket only.
4. Server `io.use` middleware (`socket/index.ts` ~140): `parseSocketHandshakeAuth` → `verifyToken` (sync) → add to `pendingHandshakes` → `verifyActiveAccessToken` (DB) → `serverService.isMember` → store `serverRole`. Reject if `accessRevoked` flipped meanwhile.
5. Server `connection`: joins `user:{userId}`, client-kind room, `server:{serverId}`, user-server room; then async `listChannels` + `listDMChannels` → `socket.join("channel:{id}")` for each → emit **`rooms:joined`** (no payload). Nuances (`socket/index.ts` 219-240): this whole block runs only when the handshake carried a `serverId` (no server → no `rooms:joined`, no heartbeat, `sync:resume` ignored). The emit sits *after* the `try/catch`, so if the channel-list query throws, the server logs it and still emits `rooms:joined` with no channel rooms joined. The barrier means "room setup attempted", not "guaranteed joined"; the HTTP loads that follow are what cover that case.
6. Client `connect` handler `reconnectSnapshot`: `resetActivitySeq()`, reload sidebar order, machines, agents, servers, followed threads. It deliberately does *not* load unread/inbox (comment ~889-908; test `reconnectNoDuplicateLoads.behavior.test.ts`).
7. Client `rooms:joined` handler `roomsJoined`: if `lastSeq > 0` emit **`sync:resume {lastSeq}`**; `syncVisibleScopes()`; `loadChannels()`; `loadUnreadCounts()`; `loadInbox({reset:true, background:true})`.
8. Server `sync:resume` (ignored if `lastSeq <= 0` or no serverId) → `messageService.syncMessages(lastSeq, undefined, 500, serverId, undefined, userId)` (all channels; visibility-filtered: public channels readable server-wide, private/DM need membership, threads need follow plus a readable parent; non-member of the server → `[]`; `ORDER BY seq LIMIT 500`) → emit **`sync:resume:response {messages, currentSeq, hasMore}`**. `currentSeq` is the max seq *among the returned messages* (or the client's own `lastSeq` if none), not the server-wide max. `hasMore = messages.length >= 500`, so exactly 500 missed messages also reports `hasMore`.
9. Client `syncResumeResponse` → `batchAddMessages`; `lastSeq = max(lastSeq, currentSeq)`; if `hasMore` → `loadUnreadCounts()` + `syncVisibleScopes()` (fall back to pulls instead of paging the resume).

### F2. Send a message (optimistic + idempotent)

1. `MessageInput` → `createOptimisticMessageDraft()` → `{id:"optimistic-<ms>-<seq>-<rand>", randomId:"msg-<ms>-<seq>-<rand>"}`.
2. `addOptimisticMessage({id, randomId, senderId, content, attachments with local blob previews…})` into the channel bucket. Display order uses `optimisticDisplaySeq = max(bucketMax, lastSeq)+1` (messageStore.ts ~2144-2157).
3. `messageStore.sendMessage` → `POST /v2/messages {channelId, content, attachmentIds, randomId, mentions}`.
4. Server `createHumanMessage` → `createOrReplayUserRandomSend` (messageService.ts ~2303): in a transaction `INSERT … ON CONFLICT DO NOTHING RETURNING`. If nothing is returned, look up by `(sender_id, random_id)` and return the original row (`replayed:true`); a different channel or forward digest throws `UserRandomIdConflictError`. The replay check does **not** compare `content`: a second send with the same `randomId` and different text silently returns the first message. Replays return the mention facts frozen by the original send; on a fresh insert the same transaction also clears `doneAt` on other users' inbox states for that channel (the channel reappears in their inbox).
5. Server emits **`message:new`** to room `channel:{channelId}` via `emitPersistedMessageToFrontend` (~2534) using the allowlisted `projectMessageSocketPayload` (includes `randomId`, `seq`). The emit happens only *after* the row is durable, and a socket-emit failure is traced as `frontend_socket_emit.degraded` with `failure_policy: "continue_from_persisted_state"`: the HTTP request still succeeds and clients must converge by pull.
5a. A **replayed** send also re-emits `message:new` (so a retrying tab gets its echo), then returns early (~8733). Only a first send continues to: advance the sender's own read cursor to the new seq (`markRead` / `markAgentLegacyRead`, so your own message is never unread) + schedule a sender read receipt, then deliver to agents in the channel, then mention/inbox fan-out. The side effects therefore run exactly once per `randomId`.
5b. When is the same `randomId` actually resent? In the web client only by the axios 401 interceptor (`api/client.ts` ~78-90: refresh token, replay the identical request once) and by any network-level duplicate. A user-initiated resend after an error mints a new draft and a new `randomId`.
6. Sender tab receives the echo (usually before the HTTP response, per comment ~2261) → `messageStore.addMessage` → `findMatchingOptimisticMessage`: matches on `randomId` when both sides have one; the content/sender/time heuristic is a deprecated fallback (`isMatchingOptimisticMessage`, ~1291). Replaces at most one optimistic row; keeps blob previews (`mergeOptimisticAttachmentPreviews`).
7. HTTP response arrives → `sendMessage` removes the exact `optimisticId` too (second cleanup path), merges if the persisted id already exists.
8. On HTTP error → `removeOptimisticMessage`, restore draft text/files, show error. No automatic retry queue.

### F3. Receive a message in another channel (unread counts)

1. Server emits `message:new` to `channel:{id}`; every member socket in that room receives it (members are auto-joined in F1 step 5).
2. `socketBridge.messageNew` (~384) has three code branches: normalized V2 (`isNormalizedMessageV2FlagEnabled` → `normalizeChannelRoomMessage` then `consumeSocketMessageNewWithSyncCore`), sync-core messages (`isSyncCoreMessagesFlagEnabled` → `consumeSocketMessageNewWithSyncCore`), or legacy duplicate check. **Both flag functions read the same per-server flag `sync_core_messages_v0`** (`serverFeatureFlags.ts:24`), so in practice there are two paths: flag on → V2 normalize + sync-core fold; flag off → legacy. The middle branch is unreachable. Both flagged paths still call the legacy `addMessage` afterward (unless the core returns `duplicate_dropped`), so the legacy gap/unread logic below runs either way. The sync-core messages scope is keyed by `channelId`, is `sparse`, and uses `epoch: null`. Consequence: with the flag on, a reordered socket frame whose seq is *lower* than the last one applied for that channel is dropped (`duplicate_dropped`) and never reaches the store; only a later HTTP pull (heartbeat/syncGap, which bypass sync-core) brings it in.
3. `addMessage` (~1970-2105): non-current channel → append to bucket, `unreadCounts[ch] += 1` unless self-authored. Current channel → if `hasNewer` (viewing an older slice) and seq jumps: bump unread only, don't append. Else if seq jumps past `currentMaxSeq+1` **or `hasGap` is already set** → park in `pendingGapMessages`, set `hasGap`, schedule `syncGap` (unless `loadingGap`). Once a gap is open, *every* later live message in that channel is parked until repair finishes. Otherwise append, and if near bottom queue auto-read (`queueLiveAppendAutoReadSync`) instead of incrementing unread.
4. `applyLiveMessageActivity` → release read hold, skip if muted, `inboxStore.receiveThreadReply`, `applyMessageChannelActivity` → debounced `loadInbox({reset,background})` after 150 ms (`scheduleInboxRefresh`).

### F4. Gap repair (`syncGap`)

1. Trigger: seq gap in the current channel (F3), heartbeat with `seq > lastSeq` (F6), `sync:resume` with `hasMore`.
2. `syncGap(channelId, {sinceSeq})` (messageStore.ts ~2491): no-op if `loadingGap` is already true (single flight) or `sinceSeq <= 0`. `sinceSeq` defaults to the max seq in *that channel's bucket* (or global `lastSeq` if no channel). Sets `hasGap/loadingGap` for the current channel, captures an ingress context (a late page from a previous server/user is dropped), then loops `GET /messages/sync?since_seq=&channel_id=&limit=200` until a short or empty page.
3. Merge by id, sort by seq. Then `takeContiguousPendingGapMessages` (~1426) walks the parked list: a parked message whose id **already came back in the fetch** is discarded from the park and advances the cursor; a message with seq `cursor+1` is stitched; anything else stays parked. Because messages are durable before they are emitted, the fetch normally returns every parked message, so the park usually empties. `hasGap` stays true only while something remains parked. Then advance `lastSeq` (to the max seq seen, which is only this channel's), reapply the read-state projection, and queue auto-read if near bottom.
4. Callers: `syncVisibleScopes()` (socketBridge ~1095) runs syncGap for at most two scopes, the open thread's channel and the channel in the URL, each from its own bucket max, skipping scopes with nothing loaded. It has its own in-flight guard, so overlapping heartbeat and resume triggers coalesce.

### F5. Mark read and read-state fan-out

1. Scroll to bottom / open channel → `markCurrentChannelRead` → `queueReadSync(channelId, maxSeq)` → `rememberPendingRead` (persisted to `sessionStorage`) → `flushPendingRead` → `POST /channels/{id}/read {seq}` (one in-flight per channel; on failure the seq stays pending for the next refresh; if a larger seq arrived meanwhile, flush again).
2. Server `/:id/read` (channels.ts ~3740: 404 unless the channel is on this server and, for private/joint/DM, the user can access it; 400 if `seq` is missing) → `channelService.markRead` → read-mutation sequencer upsert (version +1 only if seq advances) → `emitReadStateUpdated`, which emits **only if `state.changed`** and the default-on kill switch `isReceiverStatePushEnabled()` allows it (a no-op read produces no push) → **`read_state:updated {serverId, scopeId, maxReadSeq, readStateVersion}`** to room `user:{userId}` (all that user's tabs and devices), plus `emitScopeReadUpdated` (**`scope_read:updated`**, read receipts for peers). Bulk variant **`read_state:updated_bulk {serverId, scopes[]}`**.
3. Client `readStateUpdated` → `normalizeReadStateUpdated` (rejects bad payloads with a reason code, never silently) → drop if `update.serverId` ≠ current server → `consumeReadStateUpdate`: accept only if `readStateVersion > previous` (equal = stale), stamping the entry with `generation = ++globalCounter` → `messageStore.applyReadStateProjection` → if `projection.complete` → inbox and thread store projections. Socket updates are ordered by version only; they do not check generations.
4. Any HTTP exit that carries read state (`/channels`, `/channels/inbox`, `/channels/unread`…) folds through `consumeReadStateSnapshot(serverId, scopeId, frontier, onCorrupt, {ledgerGenerationAtRequest})` (readStateSync.ts ~292). The caller reads the *global* generation counter before sending the request. On return, the check is *per scope*: "has this scope's entry been stamped with a newer generation since then?" If yes, the socket won and the response is `stale`. Four outcomes: `accepted`; `stale` (superseded by a socket update, or version ≤ the stored one); `cleared` (frontier `absent` → the ledger entry is deleted); `corrupt` (bad union, non-UInt64 string, or > 2^53 → alarm raised, ledger left untouched so one bad row cannot flip unread UI).
5. Mark-unread (`POST /:id/unread`) takes the `backward` sequencer path: seq goes down and the version still goes up, so the same version rule makes every tab converge on "unread".

### F6. Heartbeat and liveness

1. Server, per socket with a serverId: `setInterval(15 s)` emits **`heartbeat {seq: getMaxSeq(serverId), ts}`**. `getMaxSeq` reads an in-memory map updated on each write (`updateMaxSeq`) and refreshed from Redis every 5 s per server per replica (`startMaxSeqRedisSync`, ref-counted).
2. Client `recordHeartbeat`: if `serverSeq > lastSeq` → `syncVisibleScopes()` (only the open thread and the channel in the URL).
3. Client watchdog every 10 s: if heartbeat silent > 90 s **and** no inbound event > 120 s while `connected` → `disconnect(); connect()`.
4. Browser signals (`visibilitychange`, `focus`, `online`, `pageshow` from bfcache) → `getLiveSessionRecoveryPlan` → `ensureSocketConnected()` and/or reload unread, machines, agents, announcements. A 60 s status reconcile reloads machines when needed.

### F7. Agent activity push

1. Server `agentOrchestrator` increments an in-memory per-agent counter → emits **`agent:activity {agentId, activity, activityKind, detail, detailKind, timestamp, serverSeq, launchId?, clientSeq?, probeId?, isHeartbeat?, isRefreshOnly?}`** to `server:{serverId}` (raw `entries` are withheld from the server room for privacy; ~12500), and also fanned out to `channel:{id}` rooms of any *joint* (cross-server) channels the agent is projected into (`emitJointActivityToProjectionRooms`). The payload also carries `producerFactId` when present. `serverSeq` is a per-agent counter in a process-local `Map` on `AgentOrchestrator` (`activityServerSeq`), bumped before each emit.
2. Client `agentActivity` binding → mint a `clientEventId` for tracing → `agentStore.updateActivity(…, serverSeq)` drops pushes with `serverSeq` ≤ last applied → `liveAgentActivityStore.recordStatusActivity` → `appendTrajectory` if entries.
3. On reconnect `resetActivitySeq()` clears the dedup map, because the server counter resets on restart.

### F8. Activity panel via sync-core (gate = shadow/on)

1. `inboxStore.loadInbox` → `void observeActivityBootstrap()` (fire-and-forget, inboxStore.ts:912).
2. `bootstrap.ts` checks the gate; `off` → nothing loaded (runtime chunk is dynamically imported only when enabled).
3. `runtime.observeActivityBootstrap`: mint `requestId`, record `latestIssuedBootstrapId` *before* sending, `GET /channels/activity/snapshot?requestId&filter=all&windowId=main`.
4. Server `getActivitySnapshot` → `withReconciledScope` (REPEATABLE READ tx, retry on SQLSTATE 40001): lock principal authority and scope rows `FOR UPDATE`, read the canonical window, compute row upserts/tombstones with fresh `row_version`s, append to `activity_sync_changes`; if the log would exceed 2048 entries, bump `epoch`, reset watermark to 0, delete the log and inactive rows. Returns `{type:"snapshot", scope, epoch, watermark, activityVersion, window}`.
5. Client drops the response if a newer bootstrap was issued or the shadow generation changed → `consumer.issueRequest(scopeId, requestId)` → `consumer.acceptSnapshot` → `core.ingestSnapshot` → `host.drain()` executes `pendingRequests()` (difference requests go to `GET /channels/activity/difference?epoch&afterWatermark`).
6. Server difference: it also runs the reconcile first, so a difference read can itself append changes or roll the epoch. Epoch mismatch, or `after > watermark`, or first retained change ≠ `after+1` (the log was truncated) → **409 `{snapshotRequired:true, epoch, watermark,…}`**; `after == watermark` → `{type:"notModified"}`; otherwise the latest change per row folded into `{type:"difference", fromSeq, toSeq, rows, tombstones, …metadata, nextFromSeq:null}`. The server never returns a `partial` difference today (`nextFromSeq` is always null), so the core's partial loop is unused by this domain. After an epoch rollover the new log starts at seq 1 with only the delta from that reconcile. It is not a full image, which is why old-epoch clients must re-snapshot.
7. `publishActivityShadowVersion` notifies subscribers; `windowAuthority` returns `authority:"core"` only when gate is `on`, no repair pending, a baseline exists, and every row carries `latestActivitySeq`; otherwise `legacy`.

## 5. State machines

**Sync-core scope (per domain, per scope)** (`core.ts`)

Important framing: the code has no explicit `Repairing` state. Each scope entry is `{state, appliedSeq, epoch, acceptedFingerprint, repairPending}`, and `repairPending` is a **flag, not a gate**. `ingestFrame` never looks at it, so while a repair is outstanding a contiguous frame with seq == applied+1 is still applied, and a sparse frame still max-advances. The states below are a reading aid. Ordering of checks in `ingestFrame`: (1) epoch mismatch (only when both the entry and the frame have a non-null epoch) → (2) no baseline → (3) seq < applied → (4) seq == applied → (5) sparse → (6) contiguous next/gap. Pending repair requests live in a map keyed `difference:<domain>:<scope>` / `snapshot:<domain>:<scope>`, so repeated gaps collapse into one request. `pendingRequests()` only *reads* them. They are cleared when a snapshot or difference is ingested, not when the host sends them.

- `NoBaseline` (appliedSeq=null)
  - contiguous + frame → request snapshot(`initial`), `repairPending` → `Repairing`
  - sparse + frame → fold, adopt seq/epoch → `InSync`
  - snapshot / difference → `InSync`
- `InSync`
  - frame seq < applied → `duplicate_dropped` (+`version_regression` violation if fingerprinted)
  - frame seq == applied → duplicate if the domain has no fingerprint function or fingerprints are equal; else `producer_version_conflict` → snapshot request (`sparse_repull` for sparse, `snapshot_required` for contiguous) → `Repairing`
  - contiguous frame seq == applied+1 → apply
  - contiguous frame seq > applied+1 → *not applied*, difference request → `Repairing`
  - sparse frame seq > applied → apply (max-advance)
  - frame with different epoch → `cross_epoch_arrival`, snapshot(`epoch_mismatch`) → `Repairing`
- `Repairing`
  - difference (not partial) → sort events by seq, fold those > applied, then set `appliedSeq = max(appliedSeq, toSeq)` even if no event reached `toSeq` (the watermark jump is trusted) → `InSync`
  - difference with a different epoch → `cross_epoch_arrival` + snapshot(`epoch_mismatch`)
  - difference `partial` → fold, request next difference from new watermark → stays `Repairing`
  - difference `snapshotRequired` → snapshot request
  - snapshot same-epoch with watermark < applied (or equal, unless the domain opts in via `acceptSameWatermarkSnapshot`) → ignored as `version_regression`; else replace state, adopt the snapshot's epoch, clear both pending requests → `InSync`. A snapshot from a *different* epoch may lower the watermark: that is how a rebaseline happens. Snapshots are accepted in any state, including `InSync`.

**Activity row (per rowId)** (`domains/activity.ts`): `Absent → Live(v)` on upsert; `Live(v) → Live(v')` only if v' > v; `Live(v) → Tombstoned(v')` if v' ≥ v; `Tombstoned(t) → Live(v)` only if v > t. Tombstones are kept to block late resurrecting frames and cleared on epoch rollover.

**Server Activity scope**: `(epoch, watermark)`; each change → watermark+1; retention overflow → `epoch+1, watermark=0`, log truncated; clients holding the old epoch get 409 `snapshotRequired`.

**Activity cutover gate**: `off` (no code loaded) → `shadow` (observe, never serve) → `on` (serve if the core claims the whole window). Unknown values resolve to `off` (`gate.ts`).

**Read mutation** (`readMutationSequencer.ts`): `admitted → executing → applied | retired_no_effect`, terminal reasons `effect_applied | already_satisfied | authorization_revoked | done_frontier_beyond_latest`.

**Optimistic message**: `optimistic (id optimistic-…)` → `persisted` (socket echo or HTTP response, matched by randomId) | `removed` (HTTP error).

**Message window (current channel)**: `contiguous` → `hasGap` (seq jump; message parked; *all* further live messages parked while `hasGap`) → `loadingGap` (syncGap, single-flight) → `contiguous` (park emptied: fetched or stitched) or stays `hasGap` while parked messages remain; `hasNewer` (viewing an older slice): live messages only bump unread, not appended.

**Read-state ledger entry (client, per server:scope)**: `Absent` → `Accepted(v, gen)` on socket update or snapshot; `Accepted(v)` → `Accepted(v')` only if v' > v; an HTTP snapshot is also dropped if the entry's gen moved after the request left; `absent` frontier → `Absent` (cleared); `corrupt` → unchanged + alarm.

**Socket**: `disconnected → connecting (auth refreshed) → connected → roomsJoined → (resume)`; `connect_error` with auth failure → refresh token → retry | keep session | logout (`resolveSocketRefreshOutcome`); watchdog forces reconnect.

## 6. Design patterns worth stealing

1. **Push is a hint, pull is the truth.** Socket payloads are allowed to be dropped/reordered; every domain has an HTTP snapshot or "since" read to converge. (`agentOrchestrator.ts` CC-006 comment; `channels.ts` ~973; `socketBridge.ts` F1/F6.) For a Temporal-based system: publish workflow progress over a pub/sub channel, but let the UI re-read workflow state from Postgres or a Temporal query on any doubt.
2. **"Rooms joined" barrier before gap sync.** The server emits `rooms:joined` only after the `socket.join` loop finishes (or after the channel-list query fails, which is logged and still emitted), and the client fetches unread/inbox/resume only after it. Fetching at `connect` races the joins and loses messages (`socket/index.ts` ~220-240, comment socketBridge.ts ~889).
3. **Client idempotency key on every write.** `randomId` minted client-side, sent over HTTP, unique index `(sender_id, random_id)`, replay returns the original row, and the socket echo carries it so the optimistic row reconciles regardless of REST/socket arrival order (`messageService.ts` ~2302-2400; `messageStore.ts` ~1291; test "randomId optimistic reconcile is independent of REST and socket arrival order", `packages/web/tests/messageWindowGapRecovery.test.ts:164`). Map this to agents too: Raft uses `agent_send_key` for agent sends.
4. **Global monotonic seq + "since seq" endpoint.** One `bigserial` gives a total order; resume, gap sync and heartbeat all speak "since N" (`schema.ts:1648`, `syncMessages`).
5. **Versioned registers for mutable facts.** Read state carries `readStateVersion` bumped only on a real change (in either direction: read *or* mark-unread, so the value can go down while the version goes up); clients accept strictly newer versions only (`readMutationSequencer.ts:1270`; `readStateSync.ts consumeReadStateUpdate`). Works for any "latest value" fact such as an agent status or task state.
6. **Ledger generation fences for HTTP vs socket races.** Capture a generation counter before issuing a request; drop the response if a socket update landed meanwhile (`consumeReadStateSnapshot … ledgerGenerationAtRequest`). Same idea: `latestIssuedBootstrapId` claimed *before* the request (`runtime.ts` ~120), `captureReceiverPrivateIngressContext` (server/principal/epoch/generation).
7. **Pure sync core with declarative IO.** The core never fetches; it outputs `SyncRequestDescription`s the host drains. Injected clock, no randomness, bigint seqs, a ring buffer of violations. That makes it deterministic and testable with vectors, and portable to Kotlin (`core.ts`, `types.ts`, `core.test.ts`).
8. **Density per scope.** `contiguous` scopes stop and repair on gaps; `sparse` scopes (global-seq messages, read-state registers) just max-advance. Choosing wrong either stalls forever or hides lost events (`types.ts` SyncDensity comment).
9. **Epochs + bounded change log + explicit `snapshotRequired`.** The server keeps a dense per-scope change log up to 2048 entries, then bumps the epoch instead of pretending a gap-free difference exists (`activitySyncService.ts:768`, `getActivityDifference`).
10. **Tombstones with versions** to stop late frames resurrecting deleted rows (`domains/activity.ts applyRowsAndTombstones`).
11. **Allowlisted, registry-checked socket payloads.** Socket payloads are explicit projections, not row spreads; a closed registry lists every producer and its keys, and a manifest defines merge policy per field (`messageRealtimeEvents.ts projectMessageSocketPayload`; `messageRealtimeProducerRegistry.ts`; `canonicalMessageManifest.ts` — e.g. `shared-null-preserve` for `commentRef`: null on a shared room broadcast means "scrubbed", not "cleared").
12. **Shadow-mode cutover.** New sync engine runs against real traffic, can only ever downgrade to legacy, and costs zero bytes when off (dynamic import) (`activityPanel/host.ts`, `gate.ts`, `bootstrap.ts`).
13. **Listener installation outside React effects.** `installSocketBridge` is idempotent per `(socket, name)` so re-renders cannot double-subscribe or drop events in a teardown gap (`socketBridge.ts` header + ~110).
14. **Rooms by audience, not by broadcast.** `user:{id}` for private facts (read state, resume), `channel:{id}` for message facts (authorized membership), `server:{id}` for presence/agent activity; Redis adapter for cross-replica fan-out; access revocation evicts sockets (`socket/index.ts`).
15. **Pending writes survive reloads.** Unsent read cursors are stored in `sessionStorage` and retried on the next flush (`messageStore.ts` ~1135-1190).

## 7. Surprises / sharp edges

- **Global seq, per-channel gap detection.** `messages.seq` is one sequence for all channels, yet `addMessage` flags a gap when `message.seq > currentMaxSeq + 1` within the current channel (messageStore.ts ~2031). Another channel's writes routinely create such "gaps". The server-side `listMessagesWithCoverage` comment acknowledges this ("global sequence gaps from other channels do not look like local message gaps", messageService.ts ~6320), The sync-core messages domain is `sparse`, but its own comment gives a different reason: it is a "compat slice" and "production message delivery still owns gap repair today" (`messageSyncDomain.ts` ~137-143). Verified in code: the legacy path runs an extra `syncGap` HTTP round-trip whenever another channel's write lands between two messages of the current channel, and while that runs every new live message in the channel is parked (`hasGap` gates all appends). The earlier claim that parked messages "stay" in `pendingGapMessages` is overstated. `takeContiguousPendingGapMessages` drops any parked message the fetch returned, and since messages are committed before they are emitted, the fetch normally returns them all. Only a message not yet visible to the `/messages/sync` read stays parked.
- **Heartbeat compares a per-server max against what the user can see.** `getMaxSeq(serverId)` includes messages in channels this user is not in; the client's `lastSeq` only covers visible ones. Checked in code: `syncVisibleScopes` → `syncGap` only raises `lastSeq` to the max seq it fetched for the visible channels, so if the newest message on the server is in a channel this user can't see (or just isn't viewing), `serverSeq > lastSeq` stays true and every 15 s heartbeat triggers one or two `/messages/sync` GETs that return nothing. It costs requests but produces no wrong state. Heartbeat seq is also only as fresh as the 5 s Redis sync (warmed once when a replica starts syncing a server). The heartbeat is a per-socket `setInterval`, so N open tabs mean N timers per server process.
- **Resume caps at 500** and on `hasMore` does not page; it falls back to unread counts plus visible-scope sync (socket/index.ts `RESUME_LIMIT`).
- **`slock_lastSeq` is written on `pagehide` but never read** (`socketBridge.ts` ~1196; confirmed by a repo-wide grep: the only other references are in `packages/web/tests/channelRealtimeSync.test.ts`, which asserts the write). A reload starts at `lastSeq = 0`, so resume is skipped and the initial HTTP loads do the work.
- **No typed socket event map.** There is no `ServerToClientEvents`/`ClientToServerEvents` type anywhere in `packages/` (grep); the closest thing is a `const` list of event *names* in `socketBridge.ts`. Bindings take `unknown` and hand-normalize. The server side is untyped too: `sync:resume` destructures `{lastSeq}` straight off the payload (`readNotificationPrefsUpdate`, `normalizeReadStateUpdated`). The contracts that exist are the TypeSpec Activity contract, the canonical message manifest, and the desktop IPC validators.
- **The Activity sync-core path is dormant by default.** Gate defaults to `off`; `host.onPush` has no production caller (grep shows only its definition). Today it only ingests the bootstrap snapshot and its drained repairs. Legacy `/channels/inbox` remains what users see.
- **Reconcile-on-read on the server.** Activity snapshot/difference *write* (lock rows, append changes) on every read, under REPEATABLE READ with serialization retries. The change log is fed by reads, not by message writes.
- **Agent activity dedup counter is in memory per server process** and resets on restart; the client clears its map on every `connect` to compensate (agentOrchestrator.ts ~12470; agentStore.ts ~245). (inferred) With several replicas, ordering is only meaningful per emitting replica.
- **Message ingest paths behind flags.** `socketBridge.messageNew` has three branches (normalized V2, sync-core messages, legacy), but both flag checks read the same per-server flag `sync_core_messages_v0`, so only two run in practice. Duplicate handling differs: legacy dedups by message id in the bucket; the flagged path dedups by per-channel seq in a sparse sync-core scope, which also drops out-of-order *older* frames until an HTTP pull brings them in.
- **Idempotent replay ignores content.** `createOrReplayUserRandomSend` treats a reused `randomId` as a replay if the channel (and forward digest) match, even when the content differs. It returns the original message.
- **Read-state numbers straddle int4/int8.** `last_read_seq` is `integer` while `messages.seq` is `bigserial`; RFC 057 adds `last_read_seq8`, and the client rejects values above 2^53 as corrupt rather than truncating (`readStateSync.ts consumeReadStateSnapshot`).
- **Socket errors are mangled by the CDN.** Auth failures on reconnect can surface as "timeout", so the client refreshes the token on every `reconnect_attempt` and not only on auth-looking errors (`api/socket.ts` comment ~115).
- **Websocket-only transport** because polling sessions need sticky routing the deployment does not guarantee (`api/socket.ts` ~42).
- **`raft-event-buffer` is not a sync mechanism.** It is a memory-only telemetry batcher (`202` = queued, not committed). 429 requeues the batch at the head with exponential backoff; any other failure drops the batch (`core.ts handleExportResult`). Capped at 3 export attempts/s.
- **Two desktop shells.** `desktop-contract` describes a Tauri invoke surface ("matches actual Tauri invoke surface for Phase 1A", `ipc.ts:2`), while `apps/raft-desktop-electron` exposes its own `window.raftDesktop` preload bridge (badge, focus, OAuth loopback, and hosting the local Computer service, which keeps running agents after the app quits, `computerHost.ts` header).
- **Server switch resets everything** through `serverResetRegistry` plus the ingress `generation`/`serverEpoch`. Any late response from the previous server is dropped instead of merged.

## Verification log

Checked against code (all paths under `/home/user/raft-source/packages`):

| Claim | Source read | Verdict / change |
|---|---|---|
| Room joins then `rooms:joined`; resume limit 500; 15 s heartbeat; Redis adapter | `server/src/socket/index.ts` 18-309 | Confirmed. **Added**: the emit follows the try/catch, so it still fires if the channel-list query fails; everything is gated on `serverId`; `join:channel` re-authorizes. |
| `sync:resume` response semantics | same + `messageService.syncMessages` ~6489 | **Corrected/added**: `currentSeq` = max seq among returned rows (not server max); `hasMore` = `length >= 500`; the visibility rules. |
| Heartbeat/maxSeq mechanics | `messageService.ts` 6625-6695 | Confirmed (in-memory map, Redis mirror, ref-counted 5 s sync, warm on start). The "fires every 15 s" inference is now verified via `syncGap`'s `lastSeq` update. |
| Client connect / roomsJoined / resume handlers, watchdog 90 s/120 s/10 s | `web/src/store/socketBridge.ts` 875-1000, 1085-1225 | Confirmed. |
| `slock_lastSeq` never read | repo-wide grep | Confirmed; the only other hits are a test asserting the write. |
| randomId idempotency, unique indexes | `server/src/db/schema.ts` 1652-1705; `messageService.ts` 2303-2460 | Confirmed. **Added**: replay ignores content; replay re-emits `message:new` but skips mark-read, agent delivery and mentions (~8733); emit-after-durable, and emit failure is non-fatal. |
| Optimistic matching and HTTP-side cleanup | `messageStore.ts` 1291-1330, 2235-2315; `optimisticMessageDraft.ts` | Confirmed. **Added**: the same `randomId` is only resent by the 401-refresh replay in `api/client.ts`. |
| Three ingest paths | `socketBridge.ts` 384-408; `messageSyncFeatureFlag.ts`; `normalizedMessageV2FeatureFlag.ts`; `serverFeatureFlags.ts:24` | **Corrected**: both flags read `sync_core_messages_v0`, so there are two effective paths; the flagged path still calls legacy `addMessage`; a sparse per-channel scope drops older reordered frames. |
| Gap detection / parking / stitching | `messageStore.ts` 1394-1456, 1970-2105, 2491-2600 | **Corrected**: `hasGap` parks *all* later messages; parked messages returned by the fetch are discarded, so the "stay parked" inference was overstated. **Added**: syncGap single-flight and per-channel `sinceSeq`. |
| Messages domain sparse "because of global seq" | `web/src/store/messageSyncDomain.ts` 137-195 | **Corrected**: the code comment says it is a compat slice, with legacy still owning gap repair. |
| Read-state version monotonic; forward-only | `readMutationSequencer.ts` 40-46, 1238-1310; `routes/channels.ts` 150-225, 3740-3775, 3899 | **Corrected**: there is also a backward (mark-unread) path where seq decreases and version increases. **Added**: push only when `changed` and behind a kill switch; `read_mutations` lease states. |
| Read-state ledger generation fence | `web/src/store/readStateSync.ts` 97-400 | Confirmed. **Added**: global counter with a per-scope check; four snapshot outcomes (accepted/stale/cleared/corrupt); socket updates use versions only; other-server updates dropped (socketBridge ~418). |
| Sync-core state machine | `sync-core/src/core.ts` 100-331 | **Corrected**: "Repairing" is a non-gating flag (frames still apply during repair); the order of checks; request dedup by key; difference advances to `toSeq`; cross-epoch snapshots may lower the watermark. |
| Violation buffer default 256 | `sync-core/src/violations.ts:8` | **Corrected** to 128; `stale_flood` is never emitted by the core. |
| Activity server: 2048 retention, REPEATABLE READ + 40001 retry, 409/notModified | `server/src/services/activitySyncService.ts` 38, 755-994 | Confirmed. **Added**: difference reads also reconcile; never partial (`nextFromSeq:null`); the post-rollover log holds only a delta. |
| Activity gate default off; `host.onPush` unused | `web/src/store/activityPanel/gate.ts`, grep `onPush` | Confirmed. |
| Agent activity serverSeq, CC-006 comment | `server/src/services/agentOrchestrator.ts` 12455-12530 | Confirmed. **Added**: fan-out to joint projection channel rooms; `producerFactId`. |
| Event count "~37" | `socketBridge.ts` const list | Confirmed as 37 names (includes `connect`); reworded. |
| No typed socket event map | grep `ServerToClientEvents` | Confirmed; server handlers are untyped too. |
| Pending read queue in sessionStorage, single in-flight per channel, re-flush on larger seq | `messageStore.ts` 413, 1135-1205 | Confirmed. |

Not re-verified (taken as-is): desktop contract/Electron, raft-event-buffer details, canonical message manifest merge policies, activity TypeSpec fields, `activityPanel/windowAuthority` conditions.
