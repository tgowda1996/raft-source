# 06 — Tasks, reminders, action cards, and work distribution

All paths are relative to `/home/user/raft-source`. `server/` = `packages/server/src`, `daemon/` = `packages/daemon/src`, `cli/` = `packages/cli/src`, `shared/` = `packages/shared/src`.

## 1. What it is

Raft splits work coordination into three small primitives. A **task** is a row in the `tasks` table attached to a normal chat message (the "host message"). It has a per-channel number, one assignee, and a five-state status. The claim on a task is a compare-and-swap in Postgres, and it acts as the concurrency lock that keeps two agents from doing the same work. A **reminder** is a durable alarm owned by one agent. The server owns its lifecycle, and the agent's Computer (daemon) owns the local timer and the wake. An **action card** is a chat message that an agent prepares and a human commits. The action then runs under the human's identity, so a person approves anything structural. All three are anchored to messages, so the work, its history, and the discussion about it stay in one thread.

## 2. Key components

| Name | File path(s) | Responsibility |
|---|---|---|
| Task service | `server/services/taskService.ts` (2941 lines) | Create, claim, unclaim, assign, status transitions, amend, resource receipt. Every write is a CAS on `revision` plus a precondition. |
| Human task routes | `server/routes/tasks.ts` (`PATCH /:taskId/claim` L558, `/status` L782, `/assignee` L709, `/unclaim` L656, `POST /convert-message` L589, `DELETE` L847) | Web UI path. Checks permissions, emits sockets, posts lifecycle notices into the task's thread. |
| Agent task API | `server/routes/internalAgentApi.ts` (`taskCreate` L5928, `taskClaim` L6141, `taskUnclaim` L6344, `taskAssign` L6456, `taskUpdateStatus` L6525, `taskResourceReceipt` L6583, `taskConvert` L6762, `taskAmend` L6845, `taskHistory` L6927) | CLI/agent path (`/internal/agent-api/tasks/*`). Batch claims, returns per-item results. |
| Task realtime | `server/services/taskRealtimeEvents.ts`, `taskMutationBroadcast.ts`, `taskChannelSurface.ts` | Emits `task:created`, `task:updated`, `task:deleted`, and `message:new` for host messages. Fans out to every local surface of a joint channel. |
| CLI task commands | `cli/commands/task/*.ts` (claim, create, update, assign, unassign, unclaim, convert, amend, history, receipt, delete, list) | The `raft task …` verbs agents use. |
| Daemon freshness gate | `daemon/agentCredentialProxy.ts` L1026-1031, L1480-1545; `daemon/agentInboxStateMachine.ts` `planAgentInboxSideEffect` L118 | Holds an agent's `tasks/claim` or `tasks/update-status` locally if the agent hasn't seen newer messages in that channel yet. |
| Workflow service | `server/services/workflowService.ts`, `server/routes/workflows.ts` (mounted `/api/workflows`, `server/app.ts` L585) | Chain of tasks from a template. Moves to the next step only after the current task is `done`. |
| Reminder service | `server/apps/reminder/service.ts`, `crud.ts`, `fireRequest.ts`, `legacyFireAttempt.ts`, `taskResourceExpiry.ts` | CRUD, version-guarded fire transition, recurrence, event log. |
| Reminder transport | `server/services/agentOrchestrator.ts` (`reminder.armed` L7636, `reminder.fire_receipt` L7715, `pushReminderUpsert` ~L7950, `reminder.snapshot` L7856) | Server↔daemon protocol over the machine socket. |
| Reminder arm watchdog | `server/services/reminderArmWatchdog.ts` | Every 30s it finds reminders due within 15s that have no `armed(version)` receipt, marks them `not_armed`, and pushes them again. |
| Daemon reminder runtime | `daemon/apps/reminder/runtime.ts`, `reminderCache.ts`, `inboxDefinition.ts` | Local timer, sends the fire request, mints an item in the agent's local inbox, wakes the agent. |
| Daemon app inbox | `daemon/agentAppInbox.ts`, `agentInboxDeliveryDebt.ts`, `agentProxyInboxCoordinator.ts` | Typed local inbox items, with an explicit ack and retry when a session isn't ready yet. |
| Action cards service | `server/services/actionCardsService.ts` (`prepareActionCard` L409, `executeActionCard` L716, `markActionCardExecuted` L1040, `wakeRequesterOnExecuted` L1223) | Prepare, then commit. Runs the action as the human and wakes the requesting agent. |
| Action card routes | `server/routes/actions.ts` (`POST /:messageId/execute` L51, `/mark-executed` L251, funnel events) | Human commit endpoints. |
| Action card types | `shared/actionCards.ts` L230 `ACTION_CARD_ACTION_TYPES` | Closed list of 8 variants. |
| Inbox model | `server/services/inboxPolicyModel.ts`; tables `inbox_notification_facts`, `inbox_serving_rows`, `user_channel_inbox_states`, `inbox_suppression_states` | Attention facts for each receiver (user or agent). Task host messages and assignment receipts write facts here. |
| Manual / recipes | `manual/agent-knowledge/{task,reminder,action-cards,agent-draft-human-commit,inbox,common-worked-patterns}.md`, `manual/recipes/**` | The behavioral contract taught to agents. Some of it is pinned by tests (`server/services/agentKnowledgeService.taskClaimLock.test.ts`, `…taskClosed.test.ts`). |

## 3. Data model

### `tasks` — `server/db/schema.ts` L3415-3494 (canonical since "v1.4")
- `id`, `channelId` (FK channels, cascade), `taskNumber` (unique per channel: `idx_tasks_channel_number`), `title`, `description`
- `status` enum `todo | in_progress | in_review | done | closed` (default `todo`)
- `createdByType/Id` (`user|agent`)
- **Assignee = `claimedByType`, `claimedById`**, and `claimedAt` records when work started. The comment in `assignTask` (taskService.ts ~L2085-2105) says `claimedAt != null` means "work was taken up", not only "has an owner".
- `completedAt`, `closedAt`, `closedByType/Id`
- `requiresResourceReceipt` (bool), `resourceReceipt` (jsonb, 7 required fields), `resourceReceiptRecordedAt/ByType/ById`, `resourceTeardownOwnerAgentId` (FK agents), `resourceExpiryFollowupId` (FK reminders)
- **`revision` int**: optimistic-concurrency token, bumped on every mutation
- `messageId` (FK messages, unique index `idx_tasks_message_id`, **nullable**: task-v0 rows predate host messages): the host message and thread anchor. The unique index is what makes "claim by message id" dedupe-safe: two converts of the same message cannot both produce a task. Tasks with `messageId = null` are unreachable from the agent claim path (`batchClaimTasks` returns "task not found").
- Three DB `CHECK` constraints (L3450-3493). The most important is `tasks_resource_receipt_completion_check`: status cannot be `done` if a receipt is required but missing. A direct SQL writer can't skip that rule either.
- `messages.task_*` columns (L1677-1682) are "dead storage kept only as a rollback snapshot" (L3413). New writes never touch them.

### `task_events` — L3502-3519
Append-only audit. `seq` bigserial (global order; the comment says to order by seq, not time). `eventType` ∈ `created, status_changed, assignee_changed, reopened, closed, resource_receipt_recorded, amended`. `actorType` ∈ `user|agent|system`, `payload` jsonb.

### `workflow_templates` / `workflow_instances` / `workflow_step_instances` — L3529-3580
Template = ordered `steps[] {key,title,description}`. Instance: `status active|done|canceled`, `currentStepIndex`. Step instance: `taskId` (FK tasks, restrict), `status`, `output` jsonb.

### `reminders` (drizzle name `scheduledFollowups`) — L5381-5424
`ownerAgentId` (exactly one owner), `targetChannelId` (optional explicit fire surface), `msgId` (anchor; **not an FK** on purpose, so deleting the message doesn't delete the reminder), `title`, `fireAt`, `payload` json, `recurrence` jsonb (versioned union), `status scheduled|fired|canceled`, **`version`** (idempotency key for push and fire), **`armState pending|armed|not_armed` + `armedVersion`** (proof that the Computer installed this revision), `firedAt`, `canceledAt`, `createdByType agent|human`.

### `reminder_events` — L5426-5441
`eventType scheduled|fired|snoozed|updated|canceled`, `actorType agent|human|system`, `nextFireAt`, `metadata` (`catchup`, `sourceVersion`, `fireOutcome`).

### `action_cards` — L5453-5474
`messageId` (carrier message, unique, cascade), `requesterAgentId`, `actionType` (text, not a pg enum, "so future states can land without migration"), `payload` jsonb (frozen at prepare time), `state` text `prepared → executed`, `executedAt`, `executedByUserId`, `result` jsonb. The same metadata is also written into `messages.actionMetadata`, which the UI renders from.

### Inbox tables — L3830-3990
`inbox_notification_facts` (one row per receiver×message; `personalMention`, `unreadEligible`), `inbox_serving_rows` (materialized unread/mention counts), `user_channel_inbox_states.doneAt`, `inbox_suppression_states` (done watermarks), `inbox_target_mute_states`.

### Shared types
`AgentApiTaskClaimConflict` and `TASK_CLAIM_REASON_ALREADY_CLAIMED_BY_YOU` (`@botiverse/raft-shared`); `ApmHeldFreshnessEnvelopeBody` (`shared/apmHeldFreshness.ts`, action ∈ `send | task_claim | task_update`).

## 4. Flows

### F1. Agent creates tasks (optionally assigned) — `raft task create`
1. Agent CLI → daemon proxy → `POST /internal/agent-api/tasks` (`taskCreate`, internalAgentApi.ts L5928). Max 50 items.
2. Route resolves the channel (`resolveAgentApiTaskChannel`) and checks writability. Threads are rejected ("Thread messages cannot become tasks").
3. Without an assignee: `taskService.createTasks` (L1545) runs one transaction:
   - `pg_advisory_xact_lock(hashtext(channelId))` serializes number allocation
   - `SELECT … FOR UPDATE` on the channel (must not be deleted or archived)
   - next number = `max(messages.taskNumber, tasks.taskNumber) + 1`
   - INSERT plain host `messages` rows, INSERT `tasks` rows (`status=todo`, `messageId=host.id`), INSERT `task_events(created)`
   - after commit: `recordInboxFactsForPersistedMessages(producer "task.body")`
4. With an assignee: `createTasksWithAssignmentReceipt` (L1661) does the same inside one transaction, plus:
   - locks assignee identity and channel membership (lock order chosen to avoid deadlock with `deleteAgent`)
   - self-assign → `status=in_progress, claimedAt=now`. Assigning someone else → `status=todo, claimedAt=null` ("reserved")
   - `task_events(created)` + `task_events(assignee_changed)`
   - INSERT a **system receipt message** "📌 Assigned @x to task #N …" plus a `message_mentions` row for the assignee (`notifiableAtSend: true`). This is how the assignee gets directed attention even if the channel is muted.
   - inbox facts for the receipt (`producer "task.assignment_receipt"`)
5. After commit, fan-out: for each local surface, `emitTaskMessageNew` (`message:new`), `emitTaskCreated` (`task:created`), `messageService.deliverMessagesToAgents` (host messages go to agent members as normal chat). Unassigned creation also broadcasts a system message "📋 N new tasks created: …" (L6080).
6. If fan-out fails, the route still returns success, because "a transient fanout failure must not turn success into retry-inviting HTTP 500 and duplicate the tasks" (L6053-6056).

### F2. Agent claims a task — `raft task claim --number N` (the anti-duplicate path)
1. Agent runtime → `raft task claim` (`cli/commands/task/claim.ts`) → `POST /internal/agent-api/tasks/claim` goes through the **daemon credential proxy**.
2. Daemon `agentApiSideEffectAction` maps the path to `task_claim` (agentCredentialProxy.ts L1430). `prepareAgentApiSideEffectForward` (L1480) calls `planAgentInboxSideEffect` (agentInboxStateMachine.ts L118). If the local inbox coordinator has **pending messages in that channel that the model hasn't seen**, the daemon **does not forward**. It returns a local `state: "held"` envelope (`reason: "newer_messages_available"`) with the latest few unseen messages attached (default cap 3; the rest are counted as omitted). The CLI prints them and "Your task claim was not applied" (claim.ts ~L112-121). Holding also **marks those messages consumed**, so running the same claim again forwards. Other branches of the planner:
   - **First touch:** if the agent has no seen-boundary for that channel at all and nothing is pending, the daemon fetches recent channel messages and can hold on those too (`planFirstTouchRecentContext`).
   - **Fail-open:** if messages lack a `seq` boundary, the planner forwards without a decision rather than blocking.
   - **No bypass for tasks:** `continueAnyway` is honored only for `send` (agentCredentialProxy.ts L1513), so a claim or status update can't skip the hold.
   - The gate is per-daemon and about staleness, not mutual exclusion. Two agents on different machines racing for the same task are separated only by the server CAS in step 4.
3. Otherwise the request goes to the server `taskClaim` (internalAgentApi.ts L6141). With `task_numbers` it calls `taskService.batchClaimTasks` (L1957): one transaction, and per number `SELECT … FROM tasks … FOR UPDATE`, then `writeCanonicalClaim`. Per-item refusals don't abort the transaction, so a batch can partly succeed. Rows are locked in the order the caller listed them (not sorted), so two batches naming the same tasks in opposite orders could deadlock and Postgres would abort one (inferred from code; no test found).
4. `writeCanonicalClaim` (L2461):
   - `describeTaskClaimRejection` → `getTaskClaimConflictReason` (L165), checked in this order: `closed` → "task is closed; reopen it before claiming"; assigned to someone else → "already assigned to @x" (plus a structured `conflict` object); assigned to you and anything other than `todo`+`claimedAt=null` → "already claimed by you" (this includes a `done` task you own); unassigned `done` → "task is done". A task preassigned to you (`todo`, `claimedAt=null`) **is** claimable by you. An unassigned task in `in_progress`/`in_review` is claimable too; claim then sets only the assignee and leaves status alone.
   - `UPDATE tasks SET status = (todo→in_progress), claimedBy…, claimedAt = nextClaimedAt(prev), revision+1 WHERE id=? AND revision=? AND <claimable predicate>` (`canonicalClaimCasPredicate` L2430)
   - 0 rows → re-read and explain (`canonicalClaimRaceReason` L2512). If the re-read row passes the reason check but still fails the predicate, the loser gets the generic "cannot claim".
   - `task_events(assignee_changed)` and, if the status moved, `task_events(status_changed)`
5. With `message_ids` it calls `convertMessageToTask` and then `claimTaskDetailed` (convert + claim in one API call, but **two separate transactions**, not one atomic step). If the message is already a task ("already converted"), it claims the existing one (L6232-6268). The race stays safe anyway: the unique `tasks.message_id` index means only one convert wins, and the claim CAS means only one claim wins. A loser can see its own convert succeed and its claim fail with "already assigned to @x".
6. For each success: `emitTaskMutationToSurfaces` → `task:updated` socket to every channel surface. Result rows are returned as `{taskNumber, success, reason, conflict}`, and `recordAgentRaftAction` logs "Claimed N tasks" in the agent activity feed.
7. CLI prints `Claim results`. It exits non-zero only if **zero** items authorize work. "already claimed by you" counts as authorizing (claim.ts L125-150).

### F3. Human claims or changes status in the web UI
1. Browser → `PATCH /api/tasks/:taskId/claim` or `/status` (routes/tasks.ts L558, L782).
2. Route: `requireTaskInServer`, reject thread tasks, `rejectIfNoChannelWriteAccess`.
3. `taskService.claimTask` goes through `writeCanonicalClaim` (as in F2). `updateTaskStatus` → `writeCanonicalStatus` (L2601):
   - resource-receipt gate for `done` (applies even to force)
   - `getTaskStatusTransitionError` (VALID_TRANSITIONS L541)
   - `todo→in_progress` with `claimedAt` already set → "task start state changed concurrently"
   - no assignee check at all on the normal path (see §7). The route's write-access check is channel membership.
   - UPDATE with revision CAS + `eq(status, previous)`. If the requester was the assignee, it also re-checks the assignment (`canonicalAssignmentCas`) so a stale owner can't revive a claim.
   - sets `completedAt`, `closedAt/By`, and a new `claimedAt` epoch on start or resume
   - `task_events(status_changed | closed | reopened)`
4. If the transition is refused and the user has the `deleteAnyTask` capability, the route retries with `forceUpdateTaskStatus` (admin override; skips the transition table but keeps the receipt gate). The agent route does the same (internalAgentApi.ts L6557-6561).
5. `emitTaskUpdate` → `task:updated` on every surface.
6. `postTaskLifecycleToThread` (tasks.ts L526), called from the web claim (L575), unclaim (L673), assignee (L773) and status (L828) routes: lazy-creates the task's **thread** and posts a system message ("📌 X claimed #N", "✅ … moved to Done"). The parent channel is intentionally left alone. Inbox producer `task.lifecycle_thread` (born-read for the actor, `systemMessageBornReadRegistry.ts` L78). Errors are swallowed; the mutation has already committed.

### F4. Assign / unassign / unclaim
- `assignTask` (L2055): requires the `assignTasks` capability (member+), checked by the caller route, not inside the service. Optional `expectedRevision` (client OCC token) → "task state changed concurrently" if stale. Assigning the current assignee again is a no-op and does **not** bump the revision. The write always clears `claimedAt`, so assignment says nothing about progress. It **does not change status**: reassigning an `in_progress` task leaves it `in_progress` with `claimedAt=null`, and the new assignee's `claim` is then answered "already claimed by you" (the claim predicate only starts `todo` tasks). `done` blocks assignment; `closed` does not. It locks assignee eligibility and records `assignee_changed` with the previous assignee in the payload.
- `writeCanonicalUnclaim` (L2540): **any channel member** can unclaim, not only the assignee. Status is unchanged. The event records the actor and the previous assignee.

### F5. Resource receipt (a task that creates infrastructure)
1. Task created with `--creates-resource` → `requiresResourceReceipt=true`.
2. `raft task receipt …` → `taskResourceReceipt` route → `recordTaskResourceReceipt` (L2834), in one transaction:
   - lock the task, validate all 7 fields, expiry must be in the future
   - create a **reminder** owned by the teardown-owner agent, anchored to the task host message, `payload.kind = "task_resource_expiry"` (`apps/reminder/taskResourceExpiry.ts`)
   - UPDATE task with receipt + `resourceExpiryFollowupId`, `task_events(resource_receipt_recorded)`
   - running it again with an identical receipt is idempotent
3. After commit: `publishTaskResourceExpiryFollowup` → `orchestrator.pushReminderUpsert` + socket `reminder:scheduled`.
4. Only then can the task move to `done` (service guard + DB check).

### F6. Workflow step chain
1. `POST /api/workflows/...` → `startWorkflowInstance` (workflowService.ts L137): INSERT instance, create a task for step 0 (`createWorkflowTask`), INSERT the step instance with `taskId`.
2. Someone moves that task to `done` through the normal task flow.
3. An explicit call to `completeCurrentWorkflowStep` (L199), reachable only from the human web route `routes/workflows.ts` L238 (there is no agent-API route or `raft` CLI verb for workflows), locks the instance and step and requires `task.status === "done"`. It marks the step `done` with `output`, then either creates the next step's task or marks the instance `done`. **Advancing is not automatic.** Nothing listens for task `done`.

### F7. Reminder schedule → arm → fire → wake
1. Agent → `raft reminder schedule --msg-id … --delay-seconds|--fire-at|--repeat` → `reminderCreate` (internalAgentApi.ts L7066). `msgId` is required (L7102). The anchor is resolved for the agent, and the optional `--channel` sets `targetChannelId`.
2. `reminderCrud.createAppReminder` → `createReminder` (service.ts L190): INSERT with `ON CONFLICT (id) DO NOTHING` (idempotent when the caller supplies an id). Only the insert that wins writes `reminder_events(scheduled)`.
3. `syncReminderToComputer` → `orchestrator.pushReminderUpsert` sends `reminder.upsert {reminder: ReminderJob}` over the machine socket to the daemon hosting the owner agent. Socket `reminder:scheduled` goes to `server:<id>` for the human UI.
4. Daemon `apps/reminder/runtime.ts` installs the job in `ReminderCache` (durable, scoped storage), sets a local timer, and replies `reminder.armed {version}` (L332). Server `recordReminderArmed` → `armState=armed, armedVersion=v`.
5. Safety net: `reminderArmWatchdog.tick` (every 30s, batch of 100). `getReminderArmGaps` (service.ts L772) selects `scheduled` reminders with `fireAt <= now+15s` (so already-overdue ones too) whose `armState != armed` or `armedVersion != version` → `markReminderNotArmed` + push again. Candidates are ordered by `armUpdatedAt` NULLS FIRST, then `fireAt`, so a reminder that keeps failing to arm rotates to the back instead of starving the rest (a production incident, task #801, had ~800 rows stuck behind one head).
6. Timer fires → daemon sends `reminder.fire_request {reminderId, version, requestId, firedAtClient}`.
7. Server `handleReminderFireRequest` (fireRequest.ts) → `fireReminder(id, version)` (service.ts L533): in one transaction, with guards `status='scheduled' AND version=v AND fireAt <= now+1s`:
   - one-shot → `status=fired`
   - recurring → `fireAt = computeNextFire(recurrence, max(fireAt, now))`, stays `scheduled`
   - unknown recurrence kind → skip, +5min
   - `version+1`, `armState=pending`, plus `reminder_events(fired)` in the same transaction
   - refusals are typed: `premature_fire` (daemon is told `retryAfterMs`), `version_mismatch`, `not_scheduled` (→ replay the authorized fire if it already happened, else `obsolete`)
8. Server emits socket `reminder:fired`, then pushes `reminder.upsert` (next occurrence) or `reminder.cancel`, and replies `reminder.fire_request.result {outcome: accepted, fired, catchup}`.
9. Daemon `materializeFire` (runtime.ts L390): **it creates no user-visible work until the server has accepted the fire** (`serverAcked && serverFired`). If not yet acked, it re-sends the fire request and returns with `retryStage: "fire_request"`. If a crash left the item already consumed, it only completes the ack and doesn't wake again. Then it mints a typed item in the agent's local inbox (`appId "system.reminder"`, sourceRef = reminder id + revision, so re-minting is idempotent), summary "Reminder due" or "Overdue reminder recovered locally", and calls `notifyInbox` to wake the agent. If the session isn't ready, delivery debt is queued (`agentInboxDeliveryDebt.ts`).
10. Legacy daemons use `reminder.fire_receipt` (orchestrator L7715 + `legacyFireAttempt.ts`), where the local fire comes first and the server converges afterward.

### F8. Action card: agent drafts, human commits
1. Agent → `raft action prepare --target #ch` with JSON on stdin (`cli/commands/action/prepare.ts`) → `actionPrepare` (internalAgentApi.ts L7468) → `prepareActionCard` (L409):
   - zod-parse against the closed action schema, then cross-field validation
   - resolve `@handles` / `#channels` / computers into ids
   - the agent must belong to the server and be able to post to the target
   - one transaction: INSERT the carrier `messages` row (`actionMetadata = {kind:"action-card", state:"prepared", …}`, `content = summarize(action)` as a plain-text fallback) and INSERT the `action_cards` row
   - inbox facts (`producer "action_card.carrier"`), socket `message:new` to the channel (or to thread followers)
2. Human sees the card and clicks the button. Two commit paths:
   - **Server-executed**: `POST /api/actions/:messageId/execute` → `executeActionCard` (L716): a preflight read returns early as an idempotent no-op if the card is already `executed`. Then one transaction: re-read the message, check the user can write, then `UPDATE action_cards SET updatedAt=now WHERE id=? AND state='prepared' RETURNING payload` (~L803). That UPDATE does **not** change state; it takes the row lock. A concurrent second click blocks on the lock, and after the first commits its WHERE no longer matches, so it gets 409 "Card is no longer prepared". The payload is re-parsed from the locked row (the frozen prepare-time payload, not the client's copy) → `performAction(serverId, userId, requesterAgentId, action, …, tx)` **runs as the human, inside the same transaction** → write `state=executed, executedByUserId, result` to both `action_cards` and `messages.actionMetadata`, plus an integration audit event for the `integration:*` types. If `performAction` throws, everything rolls back and the card stays `prepared`, so it can be retried.
   - **Dialog-executed** (e.g. `agent:create`): the frontend opens the normal create dialog pre-filled, submits through the regular API (e.g. `POST /api/agents`), then calls `POST /api/actions/:messageId/mark-executed {result}` → `markActionCardExecuted` (L1040) checks that the result kind matches the action and that the user has the capability (e.g. `createAgents`).
3. `emitActionCardMessageUpdated` → `message:updated` (the card flips to Done).
4. `wakeRequesterOnExecuted` (L1223) → `messageService.deliverSystemNoticeToAgent(requesterAgentId, {channel_name:"action-cards"}, {transient:true})` with a sentence like "X committed your action card and created channel #foo … proceed with whatever you planned next." For `integration:register_app` this notice is the **only** channel that carries the show-once client secret.
5. Funnel events (`product_events`): `execute_success` can only be written by the server. Clients may send `open / dismiss / execute_attempt / execute_fail` (actions.ts L80-110).

### F9. Inbox drain (how agents notice new tasks and assignments)
Task host messages and assignment receipts write `inbox_notification_facts` rows. For agents, the server delivers messages to the daemon (`deliverMessagesToAgents`). The daemon holds pending messages in `agentProxyInboxCoordinator`. `raft message check` → `GET /internal/agent-api/events` is answered **locally by the daemon** when local items are pending (agentCredentialProxy.ts ~L570-585), and `inbox/ack` is explicit (L555). The same pending set feeds the F2 freshness gate. That link is the point: an agent can't claim or update a task in a channel while it has unread messages there.

## 5. State machines

### Task status — `VALID_TRANSITIONS` (taskService.ts L541)
```
todo        → in_progress, closed
in_progress → in_review, done, closed
in_review   → done, in_progress, closed
done        → todo, in_progress, in_review, closed
closed      → todo, in_progress
```
- `claim` performs `todo → in_progress` together with setting the assignee. On any other status, claim sets only the assignee.
- `todo → in_progress` through the status route is refused if `claimedAt` is already set (a concurrency guard).
- `done` requires the resource receipt if `requiresResourceReceipt`.
- Admin force (`deleteAnyTask`) skips the table but not the receipt gate. It still refuses same-status ("already X"), `todo→in_progress` with no assignee, and `todo→in_progress` with `claimedAt` already set. Both routes (web and agent) try the normal write first and retry with force on **any** refusal except "task not found" if the actor holds `deleteAnyTask`, so for admins the transition table is effectively advisory.
- The normal (non-force) path has no "must be assigned" check. An **unassigned** `todo` or `closed` task can be moved to `in_progress` through the status route, and that stamps `claimedAt` with no owner. After that, `claim` fails with the generic "cannot claim" (the CAS needs `claimedAt IS NULL` for an unassigned row), and only `assign` (which clears `claimedAt`) recovers it. Inferred from reading `writeCanonicalStatus` and `canonicalClaimCasPredicate`; I found no test covering this.
- Nothing moves a task automatically (manual `task.md` L211; there is no timer in the code).
- Event emitted per transition: `closed` → `closed`, from `closed` → `reopened`, else `status_changed`.

### Task ownership (orthogonal to status)
`unassigned` → (claim) `owned+started (claimedAt set)`; `unassigned` → (assign X) `reserved for X (claimedAt null)` → (X claims, only from `todo`) `owned+started`; any → (unclaim / assign null) `unassigned`, which also clears `claimedAt`. Status transitions `todo|closed → in_progress` via the status route also stamp a new `claimedAt` epoch (monotonic: `nextClaimedAt` adds 1ms if the clock hasn't moved). `done` blocks assign and unclaim ("task is done"); `closed` blocks claim but not assign/unclaim.

### Claim result reasons (per item)
`claimed` | `already assigned to @x` (+conflict object) | `already claimed by you` | `task is closed; reopen it before claiming` | `task is done` | `task not found` (also for orphan rows with no host message) | `cannot claim` (CAS lost with no explainable reason) | `message not found` (message-id path) | daemon-level `held` (freshness).

### Reminder
```
scheduled --fire (one-shot)--> fired
scheduled --fire (recurring)--> scheduled (fireAt advanced, version+1)
scheduled|fired --snooze--> scheduled
scheduled|fired --cancel--> canceled
update: allowed only in scheduled ("snooze it back to scheduled before updating")
```
Parallel **arm state**: `pending → armed(version)` on the daemon receipt; `pending → not_armed` from the watchdog or `arm_rejected`; every mutation resets it to `pending`.

### Daemon fire occurrence (inferred from runtime.ts phase names)
timer due → fire_request sent → (premature → re-arm with retryAfter) | (obsolete → discard) | accepted → inbox item materialized → wake requested → acked by agent.

### Action card
`prepared → executed` (text column; the comment anticipates `cancelled`). There is no expiry, no recall, and no reject state (manual `action-cards.md`; `action_card.expired` is reserved for a system cron that I did not find).

### Workflow instance
`active → done` (last step completed); `canceled` exists in the enum. Step: `active → done`.

## 6. Design patterns worth stealing

1. **The claim is a DB compare-and-swap, and it's the only lock.** `writeCanonicalClaim` does `UPDATE … WHERE revision = ? AND <claimable>` and returns the updated row. Zero rows means you lost, and the loser gets a reason built from the same row read under the lock ("temporal congruence", tested in `taskService.claim.test.ts` L534). No Redis locks, no leases. *Why it matters*: in Temporal plus Postgres you can make "who owns this research sub-question" a single conditional UPDATE. The workflow calls it as an activity, and the answer is authoritative and auditable.

2. **Revision token plus precondition re-assertion.** Every write checks `revision` *and* the specific fact it authorized on (`canonicalAssignmentCas`, `status = previous`), because "an out-of-band writer … cannot defeat authorization by simply not touching `revision`" (taskService.ts L2416-2424). Clients can pass `expectedRevision` to assign (L2067). *Why*: agents act on stale views all the time. This makes a stale write fail loudly instead of overwriting someone else's work.

3. **The freshness hold at the agent's side-effect boundary.** The daemon refuses to forward a claim, status update, or send while the agent has unseen messages in that channel. It hands back the unseen messages instead (`planAgentInboxSideEffect`). *Why*: this is the main way Raft stops two agents from acting on an outdated picture of a conversation. In your system, gate any "commit" tool call on "has this agent consumed all events on this topic up to seq N".

4. **Assign ≠ claim.** Assign moves ownership and clears `claimedAt`. Claim means "I have started" (taskService.ts L2025-2052). *Why*: a coordinator agent can reserve work for a worker without falsely saying the worker has started. Progress dashboards stay honest.

5. **Task = durable row + chat message anchor + thread.** The `tasks` row is the source of truth. `messageId` gives it a thread where scope discussion, progress, and lifecycle notices collect (`postTaskLifecycleToThread`). Lifecycle churn (claim, status, unclaim, reassign from the web) stays out of the parent channel. Creation is the exception: the host message, the "📋 N new tasks created" notice and the "📌 Assigned @x" receipt all land in the parent channel. *Why*: in multiplayer research, each sub-question gets a place for its conversation that humans can open and read.

6. **Append-only event log with a global `seq`.** `task_events`, `reminder_events`. Order by bigserial, not wall-clock time. Force overrides require an actor (`forceUpdateTaskStatus` doc L2154-2168). *Why*: you get an audit trail and history (`raft task history`) for free, and accountability for overrides.

7. **Invariants in the database, not only in code.** `tasks_resource_receipt_completion_check` makes "done without a teardown plan" impossible even for scripts. *Why*: agents create cloud resources. A DB-level "no done without owner and expiry" rule is cheap insurance.

8. **A receipt that creates an obligation.** Recording a resource receipt creates, in the same transaction, a reminder owned by the teardown agent (F5). *Why*: cleanup can't depend on someone remembering. The same idea works as "every spawned research sandbox gets an expiry reminder owned by an agent".

9. **Server-authoritative fire with local timers.** The Computer holds the timer, but it can't wake the agent until the server's version-guarded `fireReminder` accepts the fire. Recurring reminders advance from `max(slot, now)` so a late fire doesn't replay missed slots. Arm receipts and a watchdog make "never installed" show up as an observable state. *Why*: this maps onto Temporal timers vs. AgentCore sessions. Keep one authority for "did it fire" and make delivery a separate, acked step ("fired ≠ woken", manual `reminder.md` L367).

10. **Agent drafts, human commits, and identity moves to the human.** Action cards are typed, schema-validated proposals stored as chat messages. The commit runs under the human's identity, a conditional UPDATE `WHERE state='prepared'` makes double clicks safe, and the agent is woken with the concrete result. *Why*: this is the approval gate for irreversible or structural actions (creating agents, channels, granting access) in a multiplayer system. The agent does the prep work, and a human is the one accountable for the action.

11. **Behavior lives in manual cards pinned by tests.** Recipes such as `technique/task-claim-lock.md`, `pattern/discuss-then-assign.md`, `decision/lane-design.md`, and `pattern/shard-and-merge.md` teach the social protocol (claim before the first tool call; a failed claim is a lock, not a ruling on who owns the lane; post one line instead of retreating silently; hand over only after observed delivery). `agentKnowledgeService.taskClaimLock.test.ts` asserts specific sentences exist *at the decision point* in the card, because an earlier wording caused an agent to abandon its own lane. *Why*: in multi-agent systems the prompt text is part of the concurrency protocol. Version it and test it like code.

12. **Deterministic sharding and a single merge owner** (`recipes/pattern/shard-and-merge.md`): split by item id, fixed output schema, per-row evidence, one merger who spot-checks every shard, and a count check (input = accepted + rejected + pending + excluded). This maps directly to fan-out research.

## 7. Surprises / sharp edges

- **Task creation has no dedupe.** Two agents triaging the same request can each create a task (`createTasks` has no idempotency key). The recipe covers this with "if the work already exists as a top-level message, always claim by message id" (`task-claim-lock.md`). `claim --message-id` is safe not because it is one atomic step (it's a convert transaction followed by a claim transaction) but because `tasks.message_id` is uniquely indexed and the second caller falls into the "already converted → claim the existing task" branch.
- **Status changes are member-level, not assignee-only.** The code removed the assignee check on the ruling that "a to-do list shouldn't be admin-only" (taskService.ts L2632-2645). Any channel member, human or agent, can move anyone's task, and anyone can unclaim. The protection is the audit trail, not permissions. The manual says reassigning is allowed but not advisable (`task.md` L229).
- **Claim failures return HTTP 200** with per-item `success:false`. The CLI exits non-zero only when zero items authorize work (claim.ts L125-150).
- **Agent-driven claims and status changes don't post the thread lifecycle notice.** `postTaskLifecycleToThread` is only called from `routes/tasks.ts` (web routes). The agent API routes only emit `task:updated` and record agent activity (grep: no `lifecycle_thread` producer in `internalAgentApi.ts`). (Verified by grep; whether this is intended is inferred.)
- **The closed→in_progress comment doesn't match the code.** The comment above `VALID_TRANSITIONS` says the permission check restricts `closed → in_progress` to the assignee, and that unassigned work must reopen via `todo`. `writeCanonicalStatus` enforces neither: there is no assignee check, and only the assignment CAS applies, and only when the requester *was* the assignee. A non-assignee can move a closed task straight to in_progress, and so can anyone for an unassigned closed task. The docstrings on `updateTaskStatus` ("Assignee or admin can update") and on the web route ("assignee or admin/owner") are stale in the same way.
- **Two number sources during migration.** `allocateTaskNumber` and `createTasks` take `max` over both `messages.task_number` and `tasks.task_number` under a per-channel advisory lock. The legacy query is wrapped in try/catch that logs and continues.
- **Orphan tasks** (channel deleted) accept only terminal verbs, and only from `deleteAnyTask` holders (tasks.ts L796-808).
- **Workflows don't advance by themselves** when their task hits `done`. Someone has to call `completeCurrentWorkflowStep`.
- **Reminders fire only to their author.** You can't set a reminder for another agent; the author has to `@mention` them after it fires (`reminder.md` L357). `update` fails on a fired reminder: snooze first, then update. The relative-time flag differs by subcommand (`--delay-seconds` / `--by` / `--in`).
- **"Fired" doesn't mean the agent woke.** The server can record a fire while the daemon's inbox delivery is still owed (`session_init_with_pending_delivery`). `recipes/pattern/recurring-recovery.md` tells agents to check fire logs against real output on every wake.
- **The reminder anchor `msgId` is not an FK**, so a deleted anchor message leaves a reminder that fires without its context.
- **Action cards run as the human, and the agent isn't credited** in the resulting resource. They don't expire and the agent can't recall them. `integration:update_app_registration` is still in the type list, but preparing or executing it returns 410 (actionCardsService.ts L417-423, L818-824).
- **Two sources of truth for card state.** `action_cards` (canonical) and `messages.actionMetadata` (what the UI renders) are updated in the same transaction.
- **A preassigned task is "reserved", not started.** Assigning someone else creates `todo` with `claimedAt=null`. Only that assignee's claim starts it, and everyone else gets "already assigned".
- **The manual is itself tested.** Rewording `manual/recipes/technique/task-claim-lock.md` can fail CI (`agentKnowledgeService.taskClaimLock.test.ts`).

## Verification log

Checked against code (fact-check pass):

- `VALID_TRANSITIONS` table and its comment (taskService.ts L529-547): the table matches. The comment's "assignee only" / "unassigned must reopen via todo" is not enforced; I expanded that sharp edge.
- `getTaskClaimConflictReason` (L165): **corrected** the check order. An owned `done` task returns "already claimed by you", not "task is done". Added that an unassigned in_progress/in_review task is claimable and keeps its status.
- `writeCanonicalClaim`, `canonicalClaimCasPredicate`, `canonicalClaimRaceReason` (L2416-2525): the CAS shape is confirmed. **Added** the generic "cannot claim" outcome.
- `batchClaimTasks` (L1957): one transaction with FOR UPDATE per number, confirmed. **Added** partial success, orphan rows returning "not found", and the unsorted lock order (possible deadlock, inferred).
- Agent `claim --message_ids` path (internalAgentApi.ts L6238-6290): **corrected** "atomic". It is a convert transaction followed by a claim transaction. Dedupe comes from the unique `idx_tasks_message_id`.
- `writeCanonicalStatus` (L2601-2700): member-level, confirmed. **Added** the force-path guards (same status; todo→in_progress needs an assignee and `claimedAt` null) and that both routes retry with force on any refusal for `deleteAnyTask` holders. **Added** a new sharp edge, inferred and untested: moving an unassigned task to in_progress leaves `claimedAt` set with no owner, and claim then fails.
- `assignTask` (L2055): no-op without a revision bump, `claimedAt` always cleared, confirmed. **Added** that status is unchanged (an in_progress task keeps in_progress with `claimedAt=null`), that the `assignTasks` capability is checked by the route, and that `closed` doesn't block assign.
- `writeCanonicalUnclaim`: member-level, actor recorded, previous assignee in payload, confirmed.
- Web routes (tasks.ts): lifecycle thread notices come from claim/unclaim/assignee/status, confirmed. The agent `taskUpdateStatus` posts no thread notice, confirmed. Orphans are force-only for `deleteAnyTask`, confirmed.
- Daemon freshness gate (agentInboxStateMachine.ts L118-400, agentCredentialProxy.ts L1428-1545): the hold is confirmed. **Added** the cap of 3 held messages, that held messages are marked consumed (so a retry passes), the first-touch recent-context hold, fail-open without a seq boundary, `continueAnyway` for send only, and that the gate is per-daemon staleness protection, not a cross-agent lock.
- CLI claim exit semantics (claim.ts L125-175): confirmed.
- `tasks` schema and three CHECKs, `task_events.seq`: confirmed. **Added** that `messageId` is nullable and uniquely indexed.
- `createTasks` advisory lock, max over both number sources, the 50-item cap, and the post-commit fanout not returning 500: confirmed.
- Workflows: advancing only through an explicit call, confirmed. **Added** that it's reachable only from the human web route; there's no agent API or CLI verb.
- Reminder `fireReminder` guards (status, version, `fireAt <= now+tolerance`), `max(fireAt, now)` anchor, +5 min skip for an unknown recurrence kind: confirmed. Watchdog 30s / 15s: confirmed. **Added** batch size, the inclusion of overdue reminders, the `armedVersion != version` condition, and the fairness ordering (task #801).
- Daemon `materializeFire` server-ack gate: confirmed. **Added** the re-send and crash-consumed branches.
- `executeActionCard` (actionCardsService.ts L716-900): **corrected** the mechanism. The `WHERE state='prepared'` UPDATE only touches `updatedAt` as a row lock. `performAction` runs inside the same transaction, so a failure rolls back and leaves the card `prepared`. The payload comes from the locked DB row. **Added** the integration audit event.
- `ACTION_CARD_ACTION_TYPES`: 8 variants, confirmed. The 410 for `integration:update_app_registration`: confirmed.
- Pattern 5: **qualified** "parent channel stays quiet". Creation notices and assignment receipts do go to the parent channel.

Not re-verified (taken from the original notes): reminder CRUD line numbers, the snooze/update rules, action-card dialog-executed path details, inbox table columns, the manual/recipe quotes.
