# 21 — Lessons from Raft for a multiplayer research orchestrator

Reader: you run a multi-agent research system on **Temporal** (durability), **AWS Bedrock AgentCore Runtime** (agent execution), **Bedrock + Claude** (models) and **Postgres** (registry). You want many humans and many agents working in shared spaces.

Sources: research notes 01–10 in this folder, spot-checked against `/home/user/raft-source`. `S/` = `packages/server/src`. `MS` = `S/services/messageService.ts`. `AO` = `S/services/agentOrchestrator.ts`. `IAA` = `S/routes/internalAgentApi.ts`.

A note on confidence: Temporal and AgentCore claims below are limited to well-established behaviour (workflow-ID uniqueness, signals/queries/updates, activity retries and at-least-once execution, heartbeats, child workflows, timers, schedules, continue-as-new, AgentCore runtime sessions, Memory, Gateway, Identity). Where a number matters (session length, history limits), check the current AWS/Temporal docs, because I mark those as "check".

---

## 0. The one-paragraph version

Raft's core bet is: **the conversation is a Postgres table, and everything else is a projection of it.** A message is committed together with all its derived facts (mentions, inbox rows, outbox rows) in one transaction. Every push after that (browser sockets, agent delivery, Slack) is an accelerator that is allowed to fail, because every consumer can catch up with `WHERE seq > my_cursor`. Agents are woken by content-free hints and then *pull* what they need. Before an agent does anything with side effects (post, claim a task), the server checks that the agent has actually read everything up to now, and holds the action if not. Work is split with a single conditional `UPDATE` that acts as the lock.

Most of Raft's remaining complexity (Redis owner leases, cross-replica routing, in-memory inboxes, ack-retry maps, the Computer supervisor and upgrader) exists because Raft runs agents as long-lived processes on user machines over a WebSocket. **You don't have that problem.** Temporal and AgentCore already give you durable execution, retries, single-instance-per-ID and isolated sessions. So copy Raft's *data model and protocols*, not its *transport*.

---

## 1. Mapping table: Raft component → your component

| Raft component | What it does in Raft | Your equivalent | Notes |
|---|---|---|---|
| Server process (`S/server.ts`, Express + Socket.IO) | HTTP API for humans and agents, realtime push | Your API service (e.g. on ECS/Lambda) + a websocket layer (API Gateway WebSocket, AppSync, or your own) | Keep it stateless. Raft's replicas are *not* stateless (they hold daemon sockets); yours can be. |
| Postgres `messages`, `channels`, `channel_agents`/`channel_humans` | The conversation, source of truth | Postgres `spaces`, `space_members`, `messages` (schema in §3) | Not Temporal. Never store the chat in workflow history. |
| `messages.seq` + `sync:resume` + `/messages/sync` | Catch-up cursor for everyone | Per-space `seq` column + `GET /spaces/:id/messages?after_seq=` | Per-space instead of Raft's single global bigserial (see trap in P2). |
| `AgentOrchestrator` (12.5k lines) | Wake/deliver/ack/start/stop per agent | **One Temporal entity workflow per agent** (`agent:{agentId}`) | Workflow-ID uniqueness replaces the Redis wake lock. Signals replace `agent:deliver`. |
| `agentLifecycleReducer.ts` pure planners | Decide wake vs deliver vs drop | Deterministic workflow code (plain functions called from the workflow) | Keep them as pure functions with table-driven unit tests, same as Raft. |
| Daemon (`packages/daemon`) | Runs agent processes, local inbox, credential proxy | **AgentCore Runtime** session + your agent container | A session is an execution, not an identity. Identity lives in Postgres. |
| Computer (`packages/computer`) | Supervisor, upgrader, credential store per machine | AWS itself (AgentCore manages hosts); your deploy pipeline | Don't build any of this. |
| Redis owner lease + ReplicaRouter + machine-local replay | Route to the one replica holding a daemon socket | Nothing. Temporal task queues + `InvokeAgentRuntime` by session ID | Biggest thing to *not* copy. |
| In-memory agent inbox + pending-ack retry map | Fast delivery, retried every 5 s × 24 | Temporal signal to the agent workflow + activity retry policy | Temporal gives durable retries; Raft's are in memory and lost on restart. |
| `mention_delivery_occurrences` | Durable per-(message, agent) delivery ledger | `deliveries` table driven by activities | Keep, but only for deliveries you must prove (mentions, assignments). |
| Freshness gate (`seenUpToSeq`, `IAA:3747-3830`) | Hold a send if the agent hasn't read new messages | Same check inside your `post_message` / `claim_task` API | Copy exactly. This is the core multiplayer safety feature. |
| `tasks` + `writeCanonicalClaim` CAS | Split work without double-claiming | `tasks` table + conditional `UPDATE`; child workflow per claimed task | The claim stays in Postgres even though Temporal runs the work. |
| Reminders (daemon timer + server-authoritative fire) | Agent wakes itself later | `workflow.sleep` / timers in the agent workflow; Temporal Schedules for recurring | Delete Raft's whole arm/fire/watchdog dance. |
| Action cards (`actionCardsService.ts`) | Agent proposes, human commits | Workflow waits on a human **Update** (or signal); executes with the human's identity | Temporal makes this easy and durable. |
| `raft` CLI + `/internal/agent-api/*` contract | The agent's only tool surface | A small set of tools exposed through **AgentCore Gateway** (or a CLI binary in the container) backed by one typed contract | One contract drives server, tool schema and SDK. |
| Daemon credential proxy (`sap_*` → `sk_agent_*`) | Agent never holds its real key | **AgentCore Identity** + Gateway; your API trusts the principal stamped by the gateway, never the request body | Mint per-session, revoke at session end. |
| Launch ID fencing (`AO:1721`) | Drop signals from a dead old process | Temporal `runId` + AgentCore `runtimeSessionId` stored as `current_session_id` on the agent row | Every write from a session carries it; mismatches are rejected. |
| The Manual (`raft manual get|search`, `agent_knowledge_events`) | On-demand docs for agents, logged with intent/reason | A `docs_search` tool + `agent_knowledge_events` table; protocol docs versioned in git | Cheap and high value. |
| `MEMORY.md` in agent workspace | Survives cold starts | **AgentCore Memory** keyed by actor = agent ID; plus Postgres for anything others must see | |
| Activity (`agent_activity_events`) vs traces (OTLP/ScopeDB) | Product status vs engineering telemetry | `agent_activity` table (small closed enum) vs CloudWatch/OTel (AgentCore Observability) | Never show Temporal history to users. |
| `resolveActorContext`, `serverPermissions.ts`, `canUserAccessChannel` | One permission model for humans and agents | `principals` table + one `can(principal, action, resource)` function used by REST, websocket and tool calls | |
| Outbox workers (`channelMembershipRoleOutbox.ts`, Slack outbox) | Side effects committed with the domain row | Outbox table + a relay that signals Temporal (see P1) | You still need this; Temporal does not remove the dual-write problem. |

---

## 2. The patterns (14)

Each pattern: **one line**, how Raft does it, what it looks like in your stack, what Temporal already gives you (don't copy), and the trap.

---

### P1. Commit the message and its consequences in one transaction; everything after commit is best-effort

**One line:** a message row, its mention rows, inbox facts and outbox rows are written atomically; pushes happen after commit and may fail without failing the send.

**How Raft does it**
- `broadcastAndDeliver` (`MS:8065`) → `createOrReplayUserRandomSend` (`MS:2303`) or `persistDirectSend` (`MS:8438`). Inside the transaction, `finalizeNewChatMessageInTransaction` (`MS:2134`) writes `message_mentions`, `mention_delivery_occurrences` (state `recorded`), `inbox_notification_facts` and the Slack `external_outbound_deliveries` row.
- After commit, `emitPersistedMessageToFrontend` (`MS:2534`) runs inside `emitFrontendSocketBestEffort` (`MS:2501`), which swallows errors: "the canonical message is already durable".
- Outbox drain by every replica with a CAS claim and dead-letter (`services/channelMembershipRoleOutbox.ts`); others use `FOR UPDATE SKIP LOCKED`.

**In your stack**
- `POST /spaces/:id/messages` → one Postgres transaction: `INSERT messages`, `INSERT mentions`, `INSERT outbox(kind='wake_agent', agent_id, space_id, seq)` for each agent that should hear about it.
- A relay drains `outbox` (`SELECT … FOR UPDATE SKIP LOCKED LIMIT 100`) and calls Temporal **signal-with-start** on `agent:{agentId}` with a content-free payload `{spaceId, seq}`. Mark the row sent after the signal returns.
- The websocket push to browsers is fired after commit and ignored on failure.

**Don't copy:** Raft's hand-rolled retry counters for outbox delivery can be replaced by making the relay itself a Temporal workflow (a schedule or a long-running loop with continue-as-new) whose activity does the drain. Retries, backoff and visibility come for free.

**Trap:** the dual write. "Write to Postgres, then signal Temporal" from the API handler will eventually lose a signal (crash between the two). "Signal Temporal, then the workflow writes Postgres" puts every chat message into workflow history and makes the space's workflow a hot spot. The outbox is the boring correct answer. Because the signal payload is only a hint (P4), duplicate signals from an at-least-once relay are harmless.

---

### P2. One monotonic sequence per stream; every consumer catches up with `WHERE seq > cursor`

**One line:** a single number answers "what did I miss" for browsers, agents and replays.

**How Raft does it**
- `messages.seq bigserial` (`S/db/schema.ts:1646-1648`), index `(channel_id, seq)`.
- Browsers: `sync:resume {lastSeq}` (`S/socket/index.ts:244-269`), HTTP `/messages/sync?since_seq=` (`routes/messages.ts:2089`), 15 s heartbeat carrying max seq so a lost push costs at most one tick.
- Agents: `agent_channel_read_cursors.last_read_seq`; resume catch-up `getAgentResumeCatchupMessages` (`MS:4008`); external agents drain `/internal/agent-api/events?since=` (`IAA:3204`).

**In your stack**
- `messages.seq bigint` allocated **per space** (see schema §3: `spaces.next_seq` incremented in the insert transaction with `UPDATE spaces SET next_seq = next_seq + 1 … RETURNING`). Unique `(space_id, seq)`.
- `space_cursors(principal_id, space_id, read_seq, seen_seq)` for humans and agents alike.
- The agent's `read_space` tool returns `messages WHERE space_id=$1 AND seq > $cursor ORDER BY seq LIMIT n` and advances `seen_seq` to the last seq it actually returned.

**Don't copy:** Raft's Redis max-seq mirror (`MS:6630-6691`) and the per-replica heartbeat. You can get max seq straight from `spaces.next_seq`.

**Trap:** Raft's global bigserial has two known problems you should avoid. (1) A sequence value is allocated at insert, not commit, so a transaction with a lower seq can become visible *after* a higher one; a client that jumps its cursor to `max(seen)` can skip it (notes 01, 02). A per-space counter updated with a row lock inside the same transaction serialises writers per space and removes this gap. The cost is write contention on very hot spaces; for research spaces that's fine. (2) Raft's read cursors are `int4` against a `bigint` seq and it now has a headroom tripwire (`seqHeadroomTripwire.ts`). Use `bigint` everywhere from day one.

---

### P3. Client-minted idempotency keys, enforced by a partial unique index, with insert-or-read-back

**One line:** every write carries a key the caller made up; a retry returns the original row byte-for-byte.

**How Raft does it**
- Partial unique indexes `idx_messages_user_random_id (sender_id, random_id) WHERE sender_type='user'` and `idx_messages_agent_send_key (sender_id, agent_send_key) WHERE sender_type='agent'` (`schema.ts:1700-1705`).
- `INSERT … ON CONFLICT DO NOTHING RETURNING`; if nothing returned, read the winner, compare channel/digest, return 409 on mismatch (`MS:2303-2410`). Replay returns the facts frozen at first send.
- Honest gap: the CLI never sends a key for agent sends, and a test (`agentKnowledgeService.sendIdempotency.test.ts`) stops the Manual from claiming otherwise.

**In your stack**
- `messages.idempotency_key text NOT NULL`, `UNIQUE (sender_id, idempotency_key)`.
- Temporal activities are at-least-once. An activity that posts a message must use a key that is identical across retries. Generate it **in workflow code** (deterministic) and pass it into the activity, e.g. `f"{workflow_id}:{run_id}:{step_name}"` or a UUID from Temporal's deterministic side-effect/UUID helper in your SDK. Do not generate it inside the activity.
- For agent tool calls from an AgentCore session, derive the key from `(runtimeSessionId, tool_call_id)` so a retried model tool call doesn't double-post.
- On conflict, compare `space_id` **and** a content hash. Raft's agent replay path doesn't compare anything and silently returns the old row (`agentSendReplayService.ts:120-151`); don't copy that leniency.

**Don't copy:** nothing to drop here. Temporal makes this *more* necessary, not less.

**Trap:** Raft's SDK can retry sends without a key (`shared/src/agentApiMessageClient.ts:147-215`). Make the key required in your API contract, not optional.

---

### P4. Wake with a hint, let the agent pull the content

**One line:** the thing that wakes an agent carries "space X has news up to seq N", never the messages themselves.

**How Raft does it**
- The daemon writes only `[Raft inbox notice: N pending in #x]` to the runtime's stdin; bodies stay in a local inbox and are pulled with `raft message check` (`packages/daemon/src/agentRuntimeInput.ts:198-212`).
- Cross-replica wake for external agents carries only `agentId`; receivers re-read durable state (`replicaRouter.ts:77-86`).
- Duplicate notices are suppressed by fingerprint; reading becomes an observable event (the "model-seen boundary").

**In your stack**
- Agent entity workflow (`agent:{agentId}`) receives signal `wake {spaceId, seq, reason: 'mention'|'member'|'task'|'timer'}`. It keeps only a small map `pending[spaceId] = max(seq)` in workflow state.
- When idle, the workflow starts an activity that calls `InvokeAgentRuntime` with a prompt like "You have new activity in: #lit-review (up to seq 812, you were mentioned)". The agent calls `read_space` to fetch bodies.
- Coalescing is free: ten messages while the agent is busy become one entry in `pending`.

**Don't copy:** Raft's in-memory inbox (`AO:2028`, capped at 1000 with silent `shift()`), the 5 s × 24 ack-retry map (`AO:2104-2105`), and the Redis wake lock (`SET NX EX 30`, `replicaRouter.ts:1131`). A Temporal workflow ID already guarantees one agent loop at a time; signals are durable; activity retries handle transient invoke failures.

**Trap:** signals are recorded in workflow history. An agent in 40 busy spaces can take thousands of signals a day. Keep the payload tiny, and **continue-as-new** on a threshold (Temporal exposes a "continue-as-new suggested" flag / history length in most SDKs; check yours). Carry `pending` and cursors across continue-as-new. Also: a signal only proves Temporal stored it, not that the agent saw it. Proof of reading comes from the cursor the `read_space` tool advances (P5).

---

### P5. The freshness gate: hold side effects when the agent is acting on a stale view

**One line:** a post or claim carries `seen_up_to_seq`; if the space has moved on, the server returns "held" plus the missed messages instead of committing.

**How Raft does it**
- `POST /internal/agent-api/send` with `seenUpToSeq` (`IAA:3591`, gate at `IAA:3747-3830`). Absent cursor counts as 0, so a first send into an unread channel is held (fail closed). Response is `200 {state:"held", messages, seenUpToSeq, continueAnywaySuggested after 3 re-holds}`.
- The CLI keeps a per-target "consumed seq" written only by `raft message read`, never by wake notices (`packages/cli/src/commands/message/_consumedSeqState.ts`); the draft is saved locally and resent with `--send-draft [--anyway]`.
- The daemon runs the same check locally for task claim and status update (`agentInboxStateMachine.ts`, `apmHeldFreshness.ts`).

**In your stack**
- `post_message(space_id, body, seen_up_to_seq, idempotency_key)` and `claim_task(task_id, seen_up_to_seq)`. Server logic:
  ```sql
  SELECT seq, sender_id, body FROM messages
   WHERE space_id = $1 AND seq > $seen_up_to_seq AND sender_id <> $me
   ORDER BY seq LIMIT 5;
  ```
  Non-empty → return `{status:'held', new_messages, seen_up_to_seq: <max returned>}` and store the draft in `message_drafts` keyed by `(agent_id, space_id)`. Empty → commit.
- `seen_up_to_seq` comes from `space_cursors.seen_seq`, which only `read_space` advances. The model never types the number; the tool layer fills it in from the session's cursor, so a hallucinated number can't bypass the gate.
- If the call happens inside Temporal (a workflow step that posts a synthesis), the activity passes the seq the workflow observed when it built the prompt.

**Don't copy:** the daemon-side duplicate of the gate. Raft has two gates (daemon proxy and server) and the daemon one silently doesn't match the v2 send path (`agentCredentialProxy.ts:1429` vs `/v2/send`; note 07). One server-side gate is enough.

**Trap:** agents looping forever on "held" in a busy space. Raft suggests `--anyway` after 3 re-holds. Add the same escape hatch and log every `anyway` so you can see which agents override it. Also decide which senders are exempt (system messages, the human who owns the space).

---

### P6. The claim is a conditional UPDATE, and it is the only lock

**One line:** "who is working on sub-question 7" is one `UPDATE … WHERE claimed_by IS NULL AND revision = $r RETURNING *`.

**How Raft does it**
- `writeCanonicalClaim` (`S/services/taskService.ts:2461`) with `canonicalClaimCasPredicate` (`:2430`); zero rows = you lost, and the loser's reason is built from the row read in the same statement.
- `tasks.revision` bumped on every write; writers may pass `expectedRevision`. Status table `VALID_TRANSITIONS` (`taskService.ts:541`): `todo → in_progress|closed`, `in_progress → in_review|done|closed`, `in_review → done|in_progress|closed`, `done → todo|in_progress|in_review|closed`, `closed → todo|in_progress`.
- Assign ≠ claim: assign reserves (`claimed_at` null), claim means started.
- `task_events` append-only log ordered by bigserial. DB `CHECK` that `done` requires a resource receipt when one is required.
- `tasks.message_id` unique → a task is anchored to a message and gets a thread.

**In your stack**
- `tasks` table (schema §3). The claim is an activity (or an API call from an agent tool) doing the CAS.
- When a claim succeeds, the *claimer's* workflow starts a **child workflow** with ID `task:{taskId}` to run the research. Workflow-ID uniqueness is a second fence: even if two claim paths raced past your code, only one `task:{taskId}` can be open. Use a workflow-ID conflict policy that fails the second start rather than terminating the first (check your SDK's name for this).
- Status changes are activities that do the transition check in SQL (`WHERE status = ANY($allowed_from)`), bump `revision`, and append `task_events` in the same transaction.

**Don't copy:** Raft's member-level "anyone can move anyone's task" rule (`taskService.ts:2632-2645`) and its admin force-retry that makes the transition table advisory. For research with money and compute on the line, make status changes assignee-or-owner.

**Trap:** Raft task creation has no dedupe: two agents triaging the same request can each create a task. Its fix is "claim by message id" with a unique index on `tasks.message_id`. Do the same: create tasks from a message with `UNIQUE (source_message_id)`, or give `create_task` an idempotency key.

---

### P7. Separate intent from observed runtime; one entity workflow per agent

**One line:** "the user wants this agent available" is a durable column; "a process is running right now" is a separate, disposable fact.

**How Raft does it**
- `agents.status ∈ {active, inactive, stopped}` is intent. `runtimeState` (`not_running | starting | running_idle | working | …`) is a per-replica in-memory cache (`agentLifecycleReducer.ts:128`). Machine reachability is a third axis.
- `planWakeAction` (`agentLifecycleReducer.ts:223`): migration gate → suppress; `inactive` → wake; `stopped` → suppress; `active` + `not_running` → wake; else deliver.
- Daemon `ready` frame lists running agents; server reconciles each with `planReadyReconcileAction` (`AO:6314-6500`).
- `stopped` can only be left by an explicit start; the SQL write refuses to overwrite it (`agentService.ts:697`).

**In your stack**
- `agents.desired_state ∈ {available, paused, retired}` in Postgres; this is what humans edit.
- `agent:{agentId}` Temporal workflow is the agent's life: it holds `pending` wake map, current AgentCore `runtimeSessionId` (if any), and loops: wait for signal or timer → if `paused`, just accumulate → else run a turn (activity: `InvokeAgentRuntime`). Pause/resume are **signals**; "what is it doing" is a **query**.
- AgentCore sessions are the execution. They are bounded (maximum lifetime and idle timeout; check current limits), so treat a session like Raft treats a process: disposable. Store the session ID for resumption and cold-start when it's gone, like Raft's resume-or-fresh (`agentProcessManager.ts:3408-3450`).
- Durable agent memory lives in **AgentCore Memory** (actor = agent ID) and Postgres, never only in the session.

**Don't copy:** the whole daemon-reconnect/ready-reconcile machinery, heartbeats, 2 s disconnect grace and 90 s stale-activity probe. The workflow *is* the reconcile loop: it knows whether its own activity is running.

**Trap:** don't make one workflow per *space* that also runs all its agents. It becomes a hot entity with huge history. Per-agent and per-task workflows scale; per-space workflows should, at most, hold space-level orchestration (e.g. a research plan), not message traffic.

---

### P8. Fence every write with the execution's generation ID

**One line:** a zombie session from an old launch must not be able to overwrite the current one's state.

**How Raft does it**
- Every agent start gets a UUID `launchId`; status/session/activity signals must echo it; `getLifecycleEventAcceptanceAction` (`AO:1721`) drops mismatches (`ignore-stale-launch`).
- Activity dedupe identity `(daemonInstanceId, launchId, clientSeq)` (`packages/shared/src/index.ts:790-835`).
- Machine owner lease uses a generation UUID with Lua CAS on refresh/unregister (`replicaRouter.ts:111-194`).
- Mention delivery rows snapshot `(machineId, launchId, sessionId)`; a drift marks the row `IDENTITY_DRIFT` (`mentionDeliveryOccurrenceService.ts`).

**In your stack**
- `agents.current_session_id` and `agents.session_generation bigint`. The agent workflow bumps the generation (activity) before invoking a new AgentCore session and passes `(agent_id, generation)` into the session's credentials/context.
- Every tool call from the session carries that generation (stamped by your gateway/tool layer, not by the model). The API rejects writes where `generation <> agents.session_generation`.
- Inside Temporal, activity attempts are already fenced by Temporal (a timed-out attempt's completion is rejected). The fence you need is for *AgentCore sessions that outlive the activity that started them*: e.g. the activity times out, Temporal retries and starts session B, and session A is still running tool calls.

**Don't copy:** the Redis owner lease and generation Lua. You have no sockets to own.

**Trap:** forgetting that streaming or background work inside an AgentCore session can continue after your invoke call returns or times out. Fence on the server side; don't rely on the session shutting down.

---

### P9. Record before deliver, for the deliveries you must be able to prove

**One line:** write a per-(message, recipient) ledger row before sending; "recorded but not delivered" can be recovered, "delivered but not recorded" cannot.

**How Raft does it**
- `mention_delivery_occurrences` (`schema.ts:5698-5750`): created in the send transaction (`insertMentionRowsWithOccurrenceRecords`, `MS:7649`), payload filled post-commit (`ensureAgentMentionDeliveryOccurrencesForAgents`, `MS:7354`).
- States `recorded → server_decided → daemon_received → (daemon_pending) → daemon_drained → acked | terminal_error`. Transitions are guarded `UPDATE … WHERE` with an identity snapshot; `acked` requires `daemon_drained_at` via CHECK.
- Automatic recovery on machine ready / session start re-sends every un-acked row (`AO:8958-9040`).

**In your stack**
- `deliveries(message_id, recipient_id, state, session_generation, …)` for **mentions and task assignments only**. Ordinary "new message in a space you're in" is covered by cursors (P2) and doesn't need a ledger.
- States can be simpler: `recorded → dispatched → seen | failed`. `dispatched` is set by the agent workflow's activity when it includes the item in a turn's prompt; `seen` is set when `read_space` returns that message to that session. Both are guarded updates.
- Recovery: at the start of each turn, the agent workflow runs an activity "list my `recorded|dispatched` deliveries older than N minutes" and includes them. That's Raft's automatic recovery, as a normal step.

**Don't copy:** the seven-state machine and the daemon's hop reporting. Raft needs those because the daemon, not the server, knows when stdin was written. Your tool layer sees reads directly.

**Trap:** Raft documents a "declared degradation" where the payload write fails post-commit and that mention becomes unrecoverable (`MS:9203-9222`). Write the payload inside the same transaction as the message if you can afford it.

---

### P10. The agent never holds a real credential; identity is stamped by the platform, not the request

**One line:** tools reach your API through a broker that attaches the agent's identity; the model only ever sees a short-lived, session-scoped token or none at all.

**How Raft does it**
- Daemon loopback proxy (`packages/daemon/src/agentCredentialProxy.ts:248, 295-316, 463-497`): each launch gets a `sap_*` token mapped in memory to the real `sk_agent_*`; the proxy pins the origin and overwrites `Authorization`. Revocation = delete the map entry at launch end.
- Principal-typed key prefixes (`sk_agent_`, `sk_computer_`, …) + a route registry that fails closed on unknown paths (`S/middleware/routeAuthPolicy.ts`, `authFromRegistry.ts:106-117`).
- Tenancy comes from auth, never from the body (agent-o11y strips `server_id` from payloads, `internalComputer.ts:921-1003`).

**In your stack**
- **AgentCore Identity** holds workload identity for the agent and the outbound credentials (OAuth tokens, API keys) the agent needs for third-party tools. **AgentCore Gateway** exposes your research API as MCP tools with inbound auth; the gateway (or a thin authorizer in front of your API) injects `agent_id`, `space_id` scope and `session_generation`.
- Your API ignores any `agent_id` in the tool arguments and uses the authenticated principal.
- Per-session credentials: mint at session start (activity), revoke in the agent workflow's cleanup path (a `finally`/cancellation handler that runs an activity).

**Don't copy:** the proxy token files, launch-forwarding wrapper scripts, and argon2-per-request key lookup. Those exist because Raft runs on user laptops.

**Trap:** Raft's comment claims the model never sees the proxy token, but the token file path is inlined in a readable wrapper script (note 08). The equivalent risk for you: putting a long-lived registry key in the AgentCore container environment "just for now". Any shell or code-interpreter tool can read env vars. Keep credentials behind the gateway.

---

### P11. One actor model for humans and agents, one visibility function for every transport

**One line:** a principal is a principal; permission checks ask for named capabilities, and one function decides who can see a space, whether the request came from REST, a websocket or a tool call.

**How Raft does it**
- `resolveActorContext(serverId, "user"|"agent", id)` (`S/lib/actorPermissions.ts:21`) → `{type, id, serverId, serverRole}`; all checks call `hasServerCapability(role, "editAgents")`, never `role === "admin"` (`packages/shared/src/serverPermissions.ts`, with a test pinning every role × capability cell).
- `canUserAccessChannel` used by HTTP, socket room join, replay and attachments, with mandatory `serverId` (`channelService.ts:4259-4281`). The agent version lacks `serverId` and callers must bind it themselves (a sharp edge).
- Separate visibility from delivery: members can read everything; delivery/wake goes only to members and mention/assignee pierces mute (`MS:9072-9100`). A mention of a non-member can't pull an agent into a room.
- Revocation pushes: a committed access change closes live sockets on every replica with an acked fan-out (`S/socket/accessRevocation.ts`).
- Creator authority: the human who created an agent can still manage it after being demoted (`actorPermissions.ts:82-90`).

**In your stack**
- `principals(id, kind 'human'|'agent', …)` with `agents` and `users` as 1:1 extensions; `space_members(space_id, principal_id, role)`. Every FK that says "who did this" points at `principals`.
- `can(principal, capability, resource)` in one module, used by the REST API, the websocket subscribe handler, the gateway tool handlers, **and Temporal query handlers** that return run state to a UI.
- Agent-to-agent delegation: when agent A asks agent B (via mention or task) to act, B acts as B, with B's permissions. Don't pass A's identity along.

**Don't copy:** Raft's four overlapping permission vocabularies for agents (server capabilities, channel roles, credential capabilities, grantable scopes; note 08). Start with one: role → capabilities, plus per-agent *narrowing* only.

**Trap:** Temporal namespaces are not your tenancy boundary for multiplayer. Anyone who can call your API's "describe run" endpoint gets whatever the query handler returns. Put the `can()` check in front of every query and every signal/update you expose.

---

### P12. Agent drafts, human commits

**One line:** for irreversible or structural actions, the agent prepares a typed proposal; a human's click executes it under the human's identity.

**How Raft does it**
- `action_cards` (`schema.ts:5453-5474`): `payload` frozen at prepare time, `state prepared → executed`, executed by `UPDATE … WHERE state='prepared'` so double clicks are safe, runs the same code path as the UI with the human as actor (`actionCardsService.ts:1747-1790`). The agent is woken with the result.
- Gaps: no expiry, no reject, no recall.

**In your stack**
- The agent calls `propose_action(type, payload)` → a row in `action_proposals` and a message in the space rendering it.
- The requesting workflow (agent or task) waits with a condition on a human decision delivered as a Temporal **Update** (approve/reject with a validator that checks the caller has the capability and the proposal is still `pending`) or a signal. Add a **timer** so proposals expire; Raft forgot this.
- On approve, an activity executes the action with the approving human's principal and records `executed_by`.

**Don't copy:** nothing to drop; this is simpler in Temporal than in Raft, which needs a separate wake after execute.

**Trap:** the payload must be frozen at proposal time and executed as-is. If the executing activity re-derives parameters (e.g. "spend up to the current budget"), the human approved something different from what ran. Also, an Update needs a running worker to be accepted; if your UI needs "the click was recorded" even during a worker outage, write the decision to Postgres first and signal from the outbox (P1).

---

### P13. Push is a hint, pull is the truth — for humans too

**One line:** the websocket tells browsers "something changed"; browsers converge by reading Postgres, and must survive dropped, delayed and reordered pushes.

**How Raft does it**
- Contract "CC-006": "the push is best-effort (drop/delay/reorder) and MUST NOT be load-bearing for correctness" (`AO:~12505`); Activity routes say the same (`routes/channels.ts:~973`).
- Rooms by audience: `user:{id}`, `channel:{id}`, `server:{id}`, and composite `user:{u}:server:{s}` because Socket.IO room intersection isn't a thing (`S/socket/platformScope.ts:27-33`).
- `rooms:joined` barrier before the client asks for missed messages (`socket/index.ts:~220-240`).
- Versioned registers for mutable facts (`readStateVersion`), tombstones, a closed registry of every realtime producer with allowlisted payload keys (`messageRealtimeProducerRegistry.ts`).
- Activity (product status, closed enum `online|thinking|working|error|offline`) is separate from traces (engineering telemetry); trajectory entries aren't broadcast (`AO:~12498`).

**In your stack**
- Browser subscribes to `space:{id}` over your websocket layer after the `can()` check; receives `{type:'message', space_id, seq}` or small facts. On reconnect: subscribe, *then* fetch `after_seq=lastSeq`.
- Agent progress for humans: the agent and task workflows write rows to `agent_activity(agent_id, kind, detail, session_generation, created_at)` via activities and emit a push. Do not expose Temporal event history, workflow IDs or activity names to users; project a small vocabulary.
- Mutable facts (task status, agent state) carry a `revision`; clients keep the higher one.

**Don't copy:** Socket.IO's Redis adapter and cross-replica fan-out, unless you self-host websockets across replicas. A managed websocket service or Postgres `LISTEN/NOTIFY` feeding one fan-out service is enough at the start.

**Trap:** don't use Temporal queries as the live feed for a page that many people watch. Each query hits a worker. Read Postgres projections for UI; use queries for admin/debug.

---

### P14. The agent's tool surface is a small, typed contract, and the protocol is written down, served on demand, and tested

**One line:** agents get a few general tools backed by one contract, errors that tell them the next step, and a searchable manual of coordination protocols whose wording is pinned by tests.

**How Raft does it**
- `raft` CLI is the agent's only output channel (`packages/daemon/src/drivers/systemPrompt.ts:84`). One contract table (`agentApiContract`: method, path, capability, zod schemas) drives server routes, CLI and SDK.
- Handles at the boundary (`#channel`, `dm:@peer`), UUIDs inside; resolution is the authz point.
- Errors carry `code`, `retryable`, `effect` (`draft_saved`), `fault_domain` and a literal `next_action` (`packages/cli/src/renderer.ts:79-127`).
- The Manual: `raft manual get|search` requires `intent` and `reason`; every lookup, hit or miss, goes to `agent_knowledge_events`. Recipes like `manual/recipes/technique/task-claim-lock.md`, `pattern/shard-and-merge.md`, `pattern/coordinator-synthesis.md`, `pattern/discuss-then-assign.md`. A test (`agentKnowledgeService.taskClaimLock.test.ts`) asserts specific sentences exist, because an earlier wording made an agent abandon its own lane.

**In your stack**
- Tools via AgentCore Gateway (MCP) or a CLI binary in the container: `read_space`, `post_message`, `claim_task`, `update_task`, `create_task`, `propose_action`, `docs_search`, `docs_get`, `set_reminder` (maps to a timer in the agent workflow). Generate tool schemas and API validators from one source.
- Tool results include `retryable` and `next_action`. The freshness hold (P5) is a normal result, not an error.
- `docs_search(query, intent, reason)` over a git-versioned `protocols/` folder; log to `agent_knowledge_events`. Misses tell you what your agents don't know.
- For research specifically, copy `shard-and-merge`: split by item ID, fixed output schema, per-row evidence, one merger, and a count check (input = accepted + rejected + pending + excluded).

**Don't copy:** Raft's shell-wrapper-on-PATH delivery; AgentCore Gateway does that job.

**Trap:** prompt text is part of your concurrency protocol. If the claim instructions are vague, agents will retreat from their own work or double up. Treat protocol docs like code: review, version, and test the exact sentences at the decision points.

---

## 3. Minimal Postgres schema sketch for multiplayer

Opinionated, small, and meant to be extended. Postgres 14+. Temporal owns execution state; these tables own the shared world.

```sql
-- Who can act. Humans and agents are both principals.
CREATE TABLE principals (
  id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  kind          text NOT NULL CHECK (kind IN ('human','agent','system')),
  handle        text NOT NULL,                 -- '@lit-reviewer', '@priya'
  tenant_id     uuid NOT NULL,
  created_at    timestamptz NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, handle)
);

CREATE TABLE agents (
  principal_id        uuid PRIMARY KEY REFERENCES principals(id),
  owner_principal_id  uuid NOT NULL REFERENCES principals(id),   -- creator authority
  desired_state       text NOT NULL DEFAULT 'available'
                      CHECK (desired_state IN ('available','paused','retired')),
  agentcore_runtime_arn text NOT NULL,
  model_id            text NOT NULL,
  session_generation  bigint NOT NULL DEFAULT 0,  -- fence (P8)
  current_session_id  text,                       -- AgentCore runtimeSessionId
  workflow_id         text GENERATED ALWAYS AS ('agent:' || principal_id::text) STORED
);

-- A shared room. DMs, threads and task threads are all spaces (Raft: "everything is a channel").
CREATE TABLE spaces (
  id                uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id         uuid NOT NULL,
  kind              text NOT NULL CHECK (kind IN ('space','private','dm','thread')),
  parent_message_id uuid,                        -- set for threads
  name              text,
  next_seq          bigint NOT NULL DEFAULT 0,   -- per-space sequence (P2)
  archived_at       timestamptz,
  created_at        timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX one_live_thread_per_parent
  ON spaces (parent_message_id) WHERE kind = 'thread' AND archived_at IS NULL;

CREATE TABLE space_members (
  space_id      uuid NOT NULL REFERENCES spaces(id) ON DELETE CASCADE,
  principal_id  uuid NOT NULL REFERENCES principals(id),
  role          text NOT NULL CHECK (role IN ('owner','member','observer')),
  muted         boolean NOT NULL DEFAULT false,
  revision      bigint NOT NULL DEFAULT 0,
  PRIMARY KEY (space_id, principal_id)
);

-- Messages are append-only, durable, first-class.
CREATE TABLE messages (
  id               uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  space_id         uuid NOT NULL REFERENCES spaces(id) ON DELETE CASCADE,
  seq              bigint NOT NULL,
  sender_id        uuid NOT NULL REFERENCES principals(id),
  kind             text NOT NULL DEFAULT 'chat'
                   CHECK (kind IN ('chat','system','proposal','task_event')),
  body             text NOT NULL,
  metadata         jsonb NOT NULL DEFAULT '{}',
  idempotency_key  text NOT NULL,
  content_hash     text NOT NULL,               -- compared on replay (P3)
  session_generation bigint,                    -- set for agent senders (P8)
  created_at       timestamptz NOT NULL DEFAULT now(),
  UNIQUE (space_id, seq),
  UNIQUE (sender_id, idempotency_key)
);

CREATE TABLE mentions (
  message_id    uuid NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
  target_id     uuid NOT NULL REFERENCES principals(id),
  handle_at_send text NOT NULL,                 -- frozen, like Raft
  PRIMARY KEY (message_id, target_id)
);

-- One cursor row per (principal, space). Humans and agents alike.
CREATE TABLE space_cursors (
  principal_id  uuid NOT NULL REFERENCES principals(id),
  space_id      uuid NOT NULL REFERENCES spaces(id) ON DELETE CASCADE,
  read_seq      bigint NOT NULL DEFAULT 0,      -- UI unread state
  seen_seq      bigint NOT NULL DEFAULT 0,      -- what the agent's model actually received (P5)
  PRIMARY KEY (principal_id, space_id)
);

-- Proven deliveries for mentions/assignments only (P9).
CREATE TABLE deliveries (
  message_id         uuid NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
  recipient_id       uuid NOT NULL REFERENCES principals(id),
  reason             text NOT NULL CHECK (reason IN ('mention','assignment')),
  state              text NOT NULL DEFAULT 'recorded'
                     CHECK (state IN ('recorded','dispatched','seen','failed')),
  session_generation bigint,
  dispatched_at      timestamptz,
  seen_at            timestamptz,
  error_code         text,
  PRIMARY KEY (message_id, recipient_id),
  CHECK (state <> 'seen' OR seen_at IS NOT NULL)
);

-- Work. A task is anchored to a message and gets a thread space.
CREATE TABLE tasks (
  id                 uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  space_id           uuid NOT NULL REFERENCES spaces(id),
  source_message_id  uuid NOT NULL UNIQUE REFERENCES messages(id),  -- dedupe (P6 trap)
  number             int  NOT NULL,
  title              text NOT NULL,
  status             text NOT NULL DEFAULT 'todo'
                     CHECK (status IN ('todo','in_progress','in_review','done','closed')),
  assignee_id        uuid REFERENCES principals(id),
  claimed_at         timestamptz,              -- assign != claim
  revision           bigint NOT NULL DEFAULT 0,
  workflow_id        text,                     -- 'task:<id>' once started
  output             jsonb,
  UNIQUE (space_id, number),
  CHECK (claimed_at IS NULL OR assignee_id IS NOT NULL),
  CHECK (status <> 'done' OR output IS NOT NULL)  -- a DB-level invariant, Raft-style
);

CREATE TABLE task_events (
  seq         bigserial PRIMARY KEY,
  task_id     uuid NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
  actor_id    uuid NOT NULL REFERENCES principals(id),
  event_type  text NOT NULL,                   -- created, claimed, status_changed, ...
  payload     jsonb NOT NULL DEFAULT '{}',
  created_at  timestamptz NOT NULL DEFAULT now()
);

-- Agent drafts, human commits (P12).
CREATE TABLE action_proposals (
  id               uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  message_id       uuid NOT NULL UNIQUE REFERENCES messages(id),
  requester_id     uuid NOT NULL REFERENCES principals(id),
  action_type      text NOT NULL,
  payload          jsonb NOT NULL,              -- frozen
  state            text NOT NULL DEFAULT 'pending'
                   CHECK (state IN ('pending','approved','rejected','expired','executed','failed')),
  decided_by       uuid REFERENCES principals(id),
  expires_at       timestamptz NOT NULL,
  result           jsonb
);

-- Transactional outbox: the bridge from Postgres to Temporal/websocket (P1).
CREATE TABLE outbox (
  id           bigserial PRIMARY KEY,
  kind         text NOT NULL,                  -- 'wake_agent', 'push_space', 'start_task'
  target       text NOT NULL,                  -- workflow id or room
  payload      jsonb NOT NULL,                 -- small hint, never message bodies
  attempts     int NOT NULL DEFAULT 0,
  sent_at      timestamptz,
  created_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX outbox_pending ON outbox (id) WHERE sent_at IS NULL;

-- Product-facing status, not traces (P13).
CREATE TABLE agent_activity (
  id                 bigserial PRIMARY KEY,
  agent_id           uuid NOT NULL REFERENCES agents(principal_id),
  session_generation bigint NOT NULL,
  kind               text NOT NULL CHECK (kind IN ('idle','thinking','working','waiting_human','error','paused')),
  detail             text,
  dedupe_key         text UNIQUE,
  created_at         timestamptz NOT NULL DEFAULT now()
);

-- What agents look up and why (P14).
CREATE TABLE agent_knowledge_events (
  id          bigserial PRIMARY KEY,
  agent_id    uuid NOT NULL REFERENCES agents(principal_id),
  query       text NOT NULL,
  intent      text NOT NULL,
  reason      text NOT NULL,
  hit_doc_id  text,
  created_at  timestamptz NOT NULL DEFAULT now()
);
```

Two queries that carry most of the system:

```sql
-- Post a message (inside one transaction with mentions + outbox rows)
WITH s AS (
  UPDATE spaces SET next_seq = next_seq + 1
   WHERE id = $space AND archived_at IS NULL
  RETURNING next_seq
)
INSERT INTO messages (space_id, seq, sender_id, body, idempotency_key, content_hash, session_generation)
SELECT $space, s.next_seq, $sender, $body, $key, $hash, $gen FROM s
ON CONFLICT (sender_id, idempotency_key) DO NOTHING
RETURNING *;
-- Caveat: on conflict this still consumed a seq (a harmless hole); read back the original row by (sender_id, idempotency_key) and compare space_id + content_hash.

-- Claim a task (the only lock)
UPDATE tasks
   SET assignee_id = $me, claimed_at = now(), status = 'in_progress', revision = revision + 1
 WHERE id = $task AND status = 'todo'
   AND (assignee_id IS NULL OR assignee_id = $me) AND claimed_at IS NULL
   AND revision = $expected_revision
RETURNING *;
```

---

## 4. How the flows look end to end in your stack

**Human posts "@lit-reviewer @stats-agent compare these three papers"**
1. API: `can(human, 'post', space)` → transaction: message (seq 812), two `mentions`, two `deliveries(recorded)`, two `outbox(wake_agent)`, one `outbox(push_space)`.
2. Relay (Temporal activity in a drain workflow): signal-with-start `agent:lit-reviewer` and `agent:stats-agent` with `{spaceId, seq:812, reason:'mention'}`; push `{space, seq:812}` to browsers.
3. `agent:lit-reviewer` workflow: idle → activity bumps `session_generation`, invokes AgentCore with "New activity in #papers up to 812 (you were mentioned)". Marks deliveries `dispatched`.
4. In the session, the agent calls `read_space` → gets 805..812, `seen_seq=812`, deliveries → `seen`.
5. It posts a plan with `seen_up_to_seq=812`. Meanwhile `@stats-agent` posted at 813 → **held**, returns 813. Agent adjusts ("stats-agent is taking the regression part") and posts again with 813 → commits as 814.
6. It calls `create_task` for three sub-questions and `claim_task` on one; the claim's success starts child workflow `task:{id}`.

**Agent wants to spend $400 on a dataset**
`propose_action('purchase_dataset', {...})` → proposal message → task workflow waits on Update `decide(proposal_id, approve)` with a 24 h timer → a human with the `spend` capability approves → activity executes as the human → result message in thread → workflow continues.

---

## 5. What NOT to build because Temporal/AgentCore already covers it

| Raft machinery | Why Raft has it | Why you skip it |
|---|---|---|
| Redis machine→replica owner lease, generation Lua, `routeInboxDelivery`, `machineResponseRelay`, HMAC HTTP replay (`S/replicaRouter.ts`, `S/machineLocalReplay.ts`, `S/machineResponseRelay.ts`) | A daemon socket is pinned to one server replica | You call AgentCore by session ID and Temporal routes by task queue. There is no pinned socket. |
| Redis wake lock `SET NX EX 30` | Two replicas could both start the same agent | Workflow ID `agent:{id}` is unique among running workflows. |
| In-memory inbox, 5 s × 24 ack-retry map, park-while-offline | Fast delivery to a process that might be offline | Signals are durable; activity retry policy handles invoke failures. |
| Daemon `ready` reconcile, heartbeats, 2 s disconnect grace, 90 s stale probe | Detect dead or reconnecting machines | The agent workflow knows whether its activity is running; activity heartbeat timeouts detect stuck ones. |
| Reminder arm/fire/watchdog split between Computer and server | The timer lives on the user's machine | `workflow.sleep` / timers are durable; Temporal Schedules for recurring jobs. |
| Computer supervisor, K upgrader, crash budgets, exit-code classes | Keeping processes alive on laptops | AgentCore hosts the runtime. Keep the *idea* of classifying failures (auth vs config vs transient) before retrying: set `non_retryable` error types in your activity retry policy. |
| Pure reducer with shadow-mode arbitration of conflicting status signals | Several signal sources race to set agent status | Your workflow is the single writer of agent status. |

What you **still need** even with Temporal: the outbox (P1), idempotency keys (P3), the freshness gate (P5), the claim CAS (P6), session fencing (P8) and one permission function (P11). None of these are execution problems; they are shared-state problems, and Temporal doesn't solve them.

---

## 6. Risks and traps, collected

1. **Putting chat in Temporal.** Message bodies in signals or workflow state blow up history, make replays slow and put user content in a system your permission layer doesn't guard. Hints only.
2. **Per-space workflows as the message router.** A busy space becomes one hot workflow. Route through Postgres + per-agent workflows.
3. **Unbounded agent workflow history.** Continue-as-new on a threshold; carry `pending`, cursors and current session ID forward.
4. **Treating "signal delivered" or "session invoked" as "agent saw it".** Only the `seen_seq` cursor advanced by `read_space` proves that. Raft keeps delivery ack, read cursor and model-seen boundary as three separate things for this reason (`IAA:3339-3358`).
5. **Zombie AgentCore sessions** after an activity timeout. Fence on `session_generation` server side (P8).
6. **Nondeterministic idempotency keys** generated inside activities. Generate in workflow code.
7. **Freshness-gate livelock** in hot spaces. Escape hatch after N holds, logged.
8. **Credentials in the container env.** Use Identity/Gateway; the model can read env vars through any shell or code tool.
9. **Temporal query handlers without authorization** exposed to a multiplayer UI.
10. **Proposals without expiry.** Raft's action cards never expire; add a timer.
11. **Status changes open to any member.** Raft allows it and relies on audit; with compute budgets, restrict to assignee/owner.
12. **Global sequence.** Raft's single bigserial has commit-order gaps and an int4/int8 mismatch. Per-space `bigint` seq assigned under the space row lock.
13. **Docs drift in the protocol.** Raft's own manual contradicts its code in a few places (member capabilities, edit/delete routes, a nonexistent `manageChannels` capability; notes 02 and 08). Test the sentences agents rely on.

---

## 7. Suggested build order

1. `principals`, `spaces`, `space_members`, `messages` with per-space seq and idempotency, `space_cursors`. `read_space` + `post_message` with the freshness gate. Websocket hint + `after_seq` pull.
2. Outbox + relay → signal-with-start `agent:{id}`. Agent workflow: pending map, one AgentCore invoke per turn, session generation fence, continue-as-new.
3. `tasks` + claim CAS + `task:{id}` child workflows + `task_events`.
4. `deliveries` for mentions/assignments with turn-start recovery.
5. `action_proposals` with Update + timer.
6. `agent_activity` projection for the UI; traces to AgentCore Observability/CloudWatch, kept separate.
7. `docs_search` + `agent_knowledge_events` + protocol docs (claim-lock, shard-and-merge, coordinator-synthesis) with tests on their key sentences.
