# 05 — Raft Computer runtime & installer

All paths relative to `/home/user/raft-source`. `pc/` = `packages/computer/src/`.

## 1. What it is

A "Computer" is the host-side control plane that keeps agents running on one machine. It ships as a single-executable Node binary (`raft-computer`, SEA build) and runs as three kinds of process: a short-lived CLI (`setup`, `start`, `upgrade`...), one long-lived **service** per install root (`__service`), and one **runner** child per attached Raft server (`__run <serverId>`) (`pc/service.ts:1-20`). The runner does not reimplement the agent host: it loads `DaemonCore` from `@botiverse/raft-daemon/core` in-process and wires Computer-specific hooks into it (`pc/service.ts:395-500`). So the "daemon" is the agent-hosting engine (WebSocket to server, agent process management), and the Computer is the supervisor, identity store, lifecycle controller, and upgrader wrapped around it. The legacy path is the standalone daemon started with an `sk_machine_*` key in a terminal; the Computer path attaches with a user device-login and gets an `sk_computer_*` key per server, and runs detached from any terminal (`manual/agent-knowledge/computer.md`).

## 2. Key components

| Name | File path(s) | Responsibility |
|---|---|---|
| Bootstrap entry | `pc/index.ts` | Loads before any service module. For OS-supervised POSIX boots, captures login-shell env and applies it before importing the CLI graph; `dispatchToKResident` re-execs an old installed carrier into K's stable slot binary. |
| CLI | `pc/cli.ts` | commander tree: `login/logout/attach/setup/start/stop/restart/status/doctor/logs/runners/channel/versions/operation acknowledge/upgrade` plus hidden `__service`, `__run`, `__k-upgrade`, `__installer-converge`, `__legacy-supervisor-takeover` (`cli.ts:311-864`). |
| Service supervisor | `pc/service.ts` `runService` (l.840) | Reconciles "wanted" servers vs live runner children every 5s (`pc/serviceReconcileLoop.ts`), owns the IPC socket, mutation surface (restart/reset/upgrade-start). |
| Runner state machine | `pc/lib/runnerStateMachine.ts`, `pc/lib/state.ts` | Pure `canSpawn`, `nextRunnerStateOnExit`, trigger vocabulary. |
| Resident runner | `pc/service.ts` `runResident` (l.717), `defaultCoreFactory` (l.395) | Reads `runner.state.json`, builds `DaemonCore` with lifecycle hooks, writes connected marker on WS connect. |
| Crash budget / health | `pc/health.ts` | `CRASH_WINDOW_MS=60_000`, `DEGRADED_THRESHOLD=3` (l.34-35); fatal-config and terminal-unlinked markers in `health.json`. |
| IPC seam | `pc/serviceIpcSeam.ts`, `pc/internal/ipc-server.ts`, `pc/lib/ipc-client.ts`, `pc/lib/types.ts:90-97` | Unix socket / Windows pipe. Methods: `service-status`, `machine-attestation`, `runner-status`, `list-runners`, `restart-service`, `upgrade-start`, `reset-service`, `reset-runner`. |
| Mutation lock | `pc/concurrency.ts` | `~/.slock/computer/.lock` via proper-lockfile, ~5s acquisition, 60s stale, abort-on-compromise. |
| Durable file writer | `pc/durableFile.ts` | tmp file (unique per pid+UUID) → fsync → rename → fsync dir → read-back compare. |
| Lifecycle op outbox | `pc/lifecycleOperations.ts`, `pc/residentLifecycleBridge.ts` | Per-server `lifecycle-operations.json` of pending `shutdown`/`ready` acks, removed only on server receipt. |
| K upgrader integration | `pc/kUpgrader.ts`, `pc/kUpgradeCoordinator.ts`, `pc/kUpgradeProcess.ts`, `pc/serviceUpgradeStart.ts`, `pc/kHostAdapter.ts`, `pc/kReleaseSource.ts`, `pc/kPaths.ts` | Uses the external `@botiverse/k-carrier` (v0.1.8, `packages/computer/package.json:42`) for two-slot (stable/experiment) promote/rollback; Computer supplies `HostAdapter` and `ReleaseSource`. |
| Resident binary resolver | `pc/kResidentBinary.ts` | Chooses which on-disk bytes to spawn for `__run`/coordinator (stable slot vs experiment vs current). |
| Operation acknowledgement | `pc/kOperationAcknowledgement.ts`, `pc/kUpgradeReconcile.ts` | Manual/automatic consumption of K's terminal receipt with proof checks. |
| Installer convergence | `pc/kInstallerConvergence.ts` | `install.sh` path: verifies candidate sha256, serves it over a loopback HTTP server, and runs it through the same K upgrader. |
| Machine convergence reducer | `pc/machineConvergenceReducer.ts`, `pc/machineOperationStore.ts`, `pc/machineOperationRuntime.ts` | Pure reducer + CAS file store for a multi-process handoff record. Only referenced by `pc/legacySupervisorTakeover.ts` (see Surprises). |
| Host lifecycle (autostart) | `pc/macosLoginCarrier.ts`, `pc/osSupervisor.ts`, `pc/osSupervisorRuntime.ts`, `pc/legacyOsSupervisorMigration.ts` | macOS login LaunchAgent (current); launchd/systemd/Windows-task supervisor (legacy, being retired by installer). |
| Login / attach services | `pc/services/login.ts`, `pc/services/attach.ts`, `pc/apiClient.ts`, `pc/setup.ts` | Device-code login, `/api/computer/attach`, preflight, legacy adoption. |
| Readiness facts | `pc/machineFacts.ts`, `pc/machineReadiness.ts` | Raw evidence (pidfile, liveness, version file, connected marker) → readiness verdict. |
| Cleanup / doctor / logs | `pc/cleanup.ts`, `pc/doctor.ts`, `pc/logRotation.ts`, `pc/logs.ts` | Startup self-heal, diagnostics, rotate-on-spawn. |
| Electron menu-bar app | `apps/raft-computer-app/src/main.ts`, `actionRunner.ts`, `menuModel.ts` | Legacy desktop carrier; calls the same `createComputerApi` library (`main.ts:400`), can host `runService`/`runResident` in-process. |

## 3. Data model

### Local disk (per install root, default `~/.slock`, `pc/paths.ts`)

- `computer/user-session.json` (0600): `{userId, accessToken, refreshToken, serverUrl, email?, name?}` (`pc/services/login.ts:150-175`).
- `computer/servers/<serverId>/runner.state.json` (0600): `{kind:"computer-attachment", serverId, serverSlug, serverMachineId (=computers.id), machineId (=machines.id), apiKey (sk_computer_*), serverUrl, attachedAt}` (`pc/services/attach.ts:268-290`). Raw key never leaves this file and the runner's memory (`pc/service.ts:17-19`).
- `.../managed.flag` — presence = "service should keep this runner running". Wanted set = attached ∩ managed (`pc/service.ts:1107-1112`, `pc/serverState.ts:339`).
- `.../health.json` — crash history, fatalConfig, terminal-unlinked marker (`pc/health.ts`).
- `.../runner.pid`, `runner.log`, `runner-version.json`, `runner.connected` (connected marker written by DaemonCore `onConnect`), `lifecycle-operations.json` (`pc/paths.ts:140-203`).
- `computer/run/service.pid`, `service.log`, `service.sock`, `service.state.json`; `computer/service-version.json` (`pc/paths.ts:218-307`).
- `computer/k/` — K-owned state: slots, journal, `upgrade.lock`, operation receipt; plus Computer-owned `host-parked-set.json` and `host-runner-hold.json` (`pc/kPaths.ts`).
- `restart-pending.json` — `{requestId, originServerId, startedAt, oldServicePid, oldRunnerPids, acceptedManagedServerIds}` (`pc/restartMarker.ts`).
- `machine-operations/<operationId>.json` — `MachineOperationRecord` (`pc/machineOperationRuntime.ts:18-20`).
- `computer/.quarantine/<ts>-<serverId>/` — corrupted per-server state moved aside, never deleted (`pc/cleanup.ts:207-230`).

### Postgres (`packages/server/src/db/schema.ts`)

- `daemons` (drizzle `machines`, l.3377): `id, serverId, userId, name, apiKeyHash, apiKeyPrefix, apiKeyFingerprint, runtimes, hostname, os, daemonVersion, computerVersion, lastHeartbeat`. What the orchestrator and `agents.machineId` bind to.
- `computers` (l.~6160): `id, serverId, name, apiKeyHash (argon2id of sk_computer_*), apiKeyPrefix, attachedByUserId, revokedAt, machineId → daemons.id`. A Computer "presents as" an existing machine row so the WS/orchestrator path is unchanged (comment above `machineId`).
- `device_authorizations` (l.~6060): RFC-8628-style device grant; `status pending|approved|denied|expired|consumed`, single-consume `consumedAt`, never deleted.
- `computer_lifecycle_operations` (l.6206): user intent. `action start|stop|restart|upgrade`, `status pending|completed|failed|unconfirmed|superseded|rolled_back`, `dispatchMode local|server`, `dispatchStatus`, `dispatchLeaseAt`, `shutdownAckAt`, `readyAckAt`, `readyConnectionEpoch`, `loadedComputerVersion`, deadlines. Partial unique index `one_pending_machine` on `(serverId, machineId) WHERE terminal_at IS NULL AND parent_operation_id IS NULL` = one in-flight intent per computer.
- `computer_lifecycle_dispatches` (l.6254): the wire-level child of a user intent; `phase` enum identical to the local reducer's 12 phases, `phaseVersion`, `phaseDeadlineAt`, `observedTargetGeneration`, `currentManagedSetRevision`, `terminalEvidence`.
- `computer_lifecycle_operation_targets` (l.6297): agents affected, for projecting activity rows; no FK so deleted agents record a skip.
- `computer_outage_occurrences`: offline/online notification state per connection epoch.

### Wire types (`packages/shared/src/index.ts`)

- `ComputerLifecycleExecutionAck` (l.432): `{operationId, action, phase: shutdown|ready, loadedComputerVersion, serviceGeneration, managedSetRevision, oldProcessIdentitiesDead, deadProcessIdentities}`.
- Server→machine: `computer:restart`, `computer:upgrade`, `computer:lifecycle:receipt` (l.565-567). Machine→server: `machine:shutdown{lifecycleAcks}`, `ready{lifecycleAcks,...}`, `computer:upgrade:progress|done`, `computer:restart:done` (l.877-903).

## 4. Flows

### F1. Login (device code)
1. User → `raft-computer login` → `pc/login.ts` adapter → `pc/services/login.ts:login`.
2. CLI → Server `POST /api/auth/device/authorize` (`DeviceAuthClient.authorize("raft-computer")`, `pc/apiClient.ts:32`). Server inserts `device_authorizations` row (pending).
3. CLI emits `login.device-code {verifyUrl,userCode,expiresAt}`; user approves in browser → Server `POST /api/auth/device/approve` sets `approvedByUserId`.
4. CLI polls `POST /api/auth/device/token` every `interval` s until `approved` (single-consume sets `consumedAt`).
5. CLI → `GET /api/auth/me` (best effort) → writes `user-session.json` 0600.

### F2. Attach
1. `raft-computer attach /<slug>` → `pc/services/attach.ts:attach`.
2. If a local attachment for the slug exists: call `POST /internal/computer/preflight` with saved key; on failure, refuse (never silently mint a fresh identity) (l.110-145).
3. Else `ensureUsableUserSession` (refresh on `session_invalid`) → `POST /api/computer/attach {serverSlug,name}` → Server creates `computers` row (+ links `daemons` row), returns raw `sk_computer_*` once.
4. CLI → `POST /internal/computer/preflight` with the new key. Only if ok, write `runner.state.json` 0600 and `chmod 0600` (fail-closed: no residue on preflight failure).
5. `setup` (`pc/setup.ts`) composes login → legacy-migration picker (`pc/services/adoptLegacy.ts`, `/api/computer/adopt-legacy`) → attach → start.

### F3. Start → service → runners → WebSocket
1. `raft-computer start [server]` → `withMutationLock` → `pc/services/start.ts:start` (in-process dedupe per home, l.~395).
2. Checks: attachments exist; no terminal-unlinked target; clears `degraded` via `reset-runner` IPC (or disk) (`clearDegradedRecoveryStateForStart`).
3. Writes `managed.flag` per target (`setServerManaged`).
4. macOS: `convergeCliHostLifecycle(..."enabled")` installs login LaunchAgent `build.raft.computer.login.<hash>` (RunAtLoad, no KeepAlive) (`pc/macosLoginCarrier.ts:192-240, 880`).
5. If a service pid is alive: `assertNoServiceVersionSkew` (reads `service-version.json`, must match pid and `COMPUTER_VERSION`). Else `spawnDetachedService` with `PARENT_LOCK_HELD_ENV_VAR=1` so the child's startup cleanup won't steal the CLI's lock (`pc/service.ts:180-265`).
6. Service `runService`: startup cleanup (`runFullCleanup`), bind IPC and publish identity (bind failure is fatal → loser never supervises), rehydrate runner records from `health.json` (degraded stays degraded), start reconcile loop (every 5s + event-scheduled).
7. `reconcile()`: read K runner hold; wanted = `listManagedServerIds`; for each wanted: if `canSpawn` → `spawnChild` (rotate log, spawn `__run <id>` with resident binary from `resolveKResidentBinary`, stdio → `runner.log`). Unwanted live children get SIGTERM (`pc/service.ts:1100-1140`).
8. Runner `runResident` → `DaemonCore.start()` → WS to server with `sk_computer_*`. `onConnect` writes `runner.connected` `{pid}`.
9. While any runner is `starting`, `reconcile()` reschedules itself every 100ms (`RUNNER_READY_POLL_INTERVAL_MS`); when it sees the connected marker carrying the child's pid it moves `starting→running` and only then writes `runner.pid` (`markRunnerReadyIfConnected`, `pc/service.ts:902-913, 1037-1040`). Spawned-but-not-connected is not ready.
   - Also in each reconcile pass: if the K runner hold (`host-runner-hold.json`) is present, **no runner is spawned** and the pass re-polls every 100ms (this is how an in-flight upgrade keeps the successor service from spawning runners before `resume()`); a dead adopted `externalPid` is cleared (`running→stopped`); and on SEA builds a pending K-upgrade recovery child is spawned if one is needed (`spawnPendingKUpgradeRecovery`, `pc/service.ts:1021-1066`).
10. CLI `waitForManagedDaemonPids` polls `collectMachineFacts` → `machineReadiness` until all targets ready or 15s timeout (`start.ts` l.~165).
11. DaemonCore sends `ready{capabilities,runtimes,runningAgents,computerVersion,lifecycleAcks}` (`packages/daemon/src/core.ts:4200-4240`).

### F4. Runner exit handling
1. `child.on("exit")` → clear `runner.pid` → if `rec.stopping` → `stopped` (no restart).
2. Else read log tail since spawn offset → `classifyRunnerExit(code, signal, text)` (`pc/service.ts:557`), checked in this order: `already-running` (log regex "Another Slock daemon is already running") | 77 `unlinked-terminal` | 78 `config-error` | SIGTERM/SIGINT `graceful` | code 0 `graceful` | else `crash`.
3. crash → `recordCrash` in `health.json`; `isDegraded` (≥3 in 60s) → `degraded` (parked) else `crashed`. **Graceful exits also get the backoff**: both `crashed` and `stopped` set `backoffUntil=now+2s` and schedule one reconcile, which respawns only if the server is still wanted (`handleRunnerExitForSupervisor`, `pc/service.ts:590-750`).
4. 78 (config error, e.g. daemon dist missing so `@botiverse/raft-daemon/core` import fails): `markFatalConfig` in `health.json` → `degraded`, not counted as a crash.
5. 77: DaemonCore's `onHandshakeRejected` hook classifies the 401 reason; **both `computer_machine_unlinked` and `computer_revoked`** are terminal → `markTerminalUnlinked` then `process.exit(77)` (`pc/service.ts:431-441`, `health.test.ts:211-220`). Supervisor → `degraded`, no retry. `start`/`restart`/`reset` do **not** clear this; the user must re-run `setup`.
6. `already-running` (lock conflict) has three outcomes (`pc/runnerLockConflict.ts`): owner pid parsed from the log and alive → adopt it as `externalPid`, lifecycle `running`, write its pid to `runner.pid`; owner pid parsed but already dead → `stopped` + 2s backoff (trigger `exit-lock-owner-gone`), the supervisor never deletes `daemon.lock` itself; owner pid not parseable → `degraded` (no auto-restart).

### F5. Web-triggered upgrade (server-dispatched)
1. Admin clicks Upgrade → Server `createUserComputerLifecycleOperation` inserts `computer_lifecycle_operations` (pending, dispatchMode `server`) + `computer_lifecycle_dispatches` (phase `accepted`) in one tx (`packages/server/src/services/computerLifecycleOperationService.ts:71-130`). Unique index blocks a second in-flight intent.
2. Orchestrator `dispatchPendingComputerLifecycleOperations` (`agentOrchestrator.ts:5094-5160`) calls `claimPendingComputerLifecycleDispatches` only for machines with a live WS connection; the claim leases rows (30s lease, `dispatchAttempts+1`, conditional UPDATE, `computerLifecycleOperationService.ts:287-336`).
   - For `upgrade`, it first **re-evaluates the broadcast/release policy**: `hands_unavailable` → release the lease and retry later; policy now incompatible with the queued decision → terminalize the op as `failed` (`computer_broadcast_revalidation_*`).
   - Sends `computer:upgrade|restart {operationId}`; if the send fails the lease is released. On send, `markComputerLifecycleCommandSent` already moves the dispatch to `first_hop_observed` (ordinal 1) and the parent to `dispatchStatus=sent`.
3. Runner (DaemonCore) → `onComputerControl` (`pc/service.ts:452-478`): `enqueueLifecycleOperation(pendingPhases:[shutdown,ready])` to `lifecycle-operations.json` under a file lock; then `requestServiceUpgradeViaIpc` → IPC `upgrade-start {scope:"remote", requestId, originServerId, trigger:"web"}` (`pc/serviceControl.ts:264`).
4. Service `createServiceUpgradeStart` (`pc/serviceUpgradeStart.ts`): SEA-only; validates identity shape; single in-flight control (`claimControl`), exact replay returns the same promise; resolves target version from channel; `priorProcessIdentities = ["service:<pid>", "runner:<sid>:<pid>"...]`.
5. `inspectKUpgradeStart`: K receipt `exact` → `already-running`; another unacknowledged receipt → `K_UPGRADE_OPERATION_BLOCKED`; else spawn detached `__k-upgrade <base64 request>` (`pc/kUpgradeProcess.ts`).
6. `waitForKUpgradeStart` polls K's operation file (25ms, 10s deadline) until K durably wrote a receipt with exactly this id/target/scope/origin/prior-identities; only then IPC returns `started`. Runner streams `upgrade-progressed`/`upgrade-completed` IPC events → WS `computer:upgrade:progress|done`.
7. Coordinator `runKUpgradeCoordinator` (`pc/kUpgradeCoordinator.ts:78`): `bootstrapStable` then `upgrader.upgradeTo(target, {operation:{id, metadata}})`. K (external) downloads to experiment slot and calls the HostAdapter (`pc/kHostAdapter.ts`):
   - `quiesce()`: snapshot managed server ids + machine identities; durably write `host-parked-set.json` and `host-runner-hold.json {held:true}`; fail if identities incomplete.
   - `stop()`: graceful StopService. Old runners' DaemonCore on shutdown send `machine:shutdown{lifecycleAcks:[shutdown]}` (`packages/daemon/src/core.ts:1769`). Server bumps dispatch `first_hop_observed` again (ordinal 2), sets `shutdownAckAt` and a `readyDeadlineAt`.
   - `start(slot)`: spawn `<slot binary> __service` detached; wait for socket reachability only (30s).
   - `healthProbe()`: IPC `machine-attestation` → `{version, pid, startId=serviceGeneration}`; never from files.
   - `resume()`: delete runner hold (service reconcile may now spawn runners), poll attestation until managed set equals parked set, else `K_HOST_RESUME_DIVERGED/TIMEOUT` → K rolls back.
8. New runner connects → DaemonCore `ready` with `getComputerLifecycleReadyAcks()` → `bindKUpgradeReadyAcknowledgement` only emits the ready ack if K outcome is `promoted`, target = running version, origin server matches, and every prior pid is dead (`pc/residentLifecycleBridge.ts:50-100`).
9. Server `observeComputerLifecycleAck` (`computerLifecycleOperationService.ts:543-680`) rejects unless `loadedComputerVersion == targetVersion`, `serviceGeneration` and `managedSetRevision` are present, `oldProcessIdentitiesDead === true` and `deadProcessIdentities` is non-empty. Note the server does not re-verify the pids; it trusts the machine's attestation (the liveness check happened in step 8 on the machine). Then: dispatch → `terminal_outbox` with `terminalEvidence` → parent forced to `completed` (`machine_dispatch_converged`) → dispatch `finalized`; activity projections written; orchestrator sends `computer:lifecycle:receipt {operationId, phase}` (`agentOrchestrator.ts:5291-5362`). The receipt is also sent for `late_after_terminal` acks, so a duplicate ack still clears the machine outbox.
   - **Failure path**: if the runner reports `computer:upgrade:done {ok:false}`, the server terminalizes the op as `rolled_back` (if `rolledBack`) or `failed`, then sends **both** `shutdown` and `ready` receipts so the machine outbox drains (`agentOrchestrator.ts:6282-6300, 5387-5402`). `computer:restart:done {ok:false}` → `failed` the same way. The machine version row advances only on `upgrade:done` with `ok && !rolledBack`.
   - **Timeout path**: `sweepComputerLifecycleOperations` → `expirePendingComputerLifecycleOperations` forces `unconfirmed` with reason `shutdown_ack_timeout`, `disconnect_timeout` (stop) or `ready_timeout` (`computerLifecycleOperationService.ts:756-790`).
10. Runner `acknowledgeReceipt` → remove phase from `lifecycle-operations.json` → `acknowledgeKReadyReceipt` → K `acknowledgeOperation` (releases the next-upgrade gate).
11. Also on connect `onComputerUpgradeReconcile` → `reconcileKUpgradeOnConnect` re-emits `computer:upgrade:done` for any unacknowledged K receipt whose `originServerId` is this server (`pc/kUpgradeReconcile.ts`).

### F6. Local upgrade (CLI/tray)
Same as F5 steps 4–7 with `scope:"local"`. After `promoted`/`up-to-date` the coordinator acknowledges its own receipt; `rolled-back|held|failed` stay unacknowledged so `status` keeps showing it until `raft-computer operation acknowledge <id>` (`pc/kUpgradeCoordinator.ts:126-137`, `pc/kOperationAcknowledgement.ts`).

### F7. Remote restart
Runner `onComputerControl("restart")` → IPC `restart-service {requestId, originServerId}` → service writes `restart-pending.json` (old service pid, old runner pids, managed set) → `replacementHandoff.request()` releases the IPC listener and spawns a replacement service (`pc/service.ts:1170-1200`). New runner's `getReadyAcknowledgements` waits for restart convergence and attaches `serviceGeneration` + dead identities; `onComputerRestartReconcile` emits `computer:restart:done` and clears the marker.

### F8. Installer
`install.sh` downloads binary → runs `raft-computer __installer-converge <version> <sha>` → `convergeKInitializedInstaller`: self-version and sha256 check; if K stable already holds these exact bytes and no open receipt → just settle service; else serve the candidate on `127.0.0.1:<random>/<uuid>/raft-computer` as a `ReleaseSource` and run K upgrade through the same HostAdapter (`pc/kInstallerConvergence.ts:98-130, 297-420`). Downgrades refused without `forceDowngrade`.

## 5. State machines

**Runner lifecycle** (`pc/lib/state.ts`, `pc/lib/runnerStateMachine.ts`): states `starting, running, degraded, crashed, stopped`.
- (none)/stopped/crashed —spawn (canSpawn: wanted, no child, no externalPid, backoff elapsed)→ starting
- starting —ready (connected marker pid = child pid)→ running
- starting —spawn-failed→ crashed (+2s backoff)
- any —exit-graceful→ stopped (+2s backoff; respawned by the next reconcile only if still wanted)
- any —exit-crash→ crashed (+2s) | —exit-crash-degraded (≥3/60s)→ degraded
- any —exit-config-error (78)→ degraded; —exit-unlinked (77)→ degraded
- any —exit-already-running→ running (live incumbent pid adopted as `externalPid`) | —exit-lock-owner-gone→ stopped (+2s, retry) | —exit-already-running with no verifiable owner→ degraded
- running(externalPid) —adopted pid found dead in reconcile→ stopped (trigger emitted as `exit-graceful`)
- any —operator-stop / shutdown-stop→ stopped (no respawn)
- degraded —reset (IPC `reset-runner` or `start`)→ stopped. `applyRunnerReset` is a no-op for any other state. Exception: a terminal-unlinked runner is refused by `start` (`assertNoTerminalUnlinkedTargets`, `pc/services/start.ts:258`) and needs `setup`.
- Gate outside the pure function: while the K runner hold file exists, `reconcile()` skips `canSpawn` entirely (no spawns during an upgrade handoff).
- Boot: `rehydrateRunnerRecord` = degraded if `isDegraded` (fatalConfig set, OR ≥3 crashes whose timestamps are still within 60s of *now*) or terminal-unlinked, else stopped. Consequence: crash-budget degradation only survives a service restart that happens within 60s of the crashes; after that the rebooted service respawns the runner. `fatalConfig` and terminal-unlinked survive indefinitely (`pc/health.ts:180-200`, `pc/service.ts:1190-1200`).

**Service**: `starting, running, degraded, stopping, stopped` (`pc/lib/state.ts`) — vocabulary pinned; transitions are implicit in `runService`.

**Machine operation (reducer + server dispatch)**: `accepted → mutation_claimed → first_hop_observed → handoff_arming → handoff_armed → old_service_stop_claimed → old_service_dead → target_supervisor_live → managed_set_converged → terminal_outbox → receipt_observed → finalized` (`pc/machineConvergenceReducer.ts:47-60`, same enum in `computer_lifecycle_dispatches.phase`). Special edge: `leg_crashed(target_supervisor)` rolls back to `old_service_dead` and clears generation/outbox/receipt. Server currently only writes `accepted → first_hop_observed (on command send, ordinal 1; again on shutdown ack, ordinal 2) → terminal_outbox (ready ack) → finalized` (also `finalized` directly when the parent is terminalized as failed/rolled_back/unconfirmed). `receipt_observed` and the handoff phases are never written by the server.

**User lifecycle operation (server)**: `pending → completed | failed | unconfirmed | superseded | rolled_back`, closed by `reduceComputerLifecycleTerminal`: start needs readyAck on a new connection epoch; stop needs shutdownAck + disconnect; local restart/upgrade needs shutdown + disconnect + ready on a new epoch (+ loaded version = target); server-dispatched ones close only via the dispatch child (`computerLifecycleOperationService.ts:338-368`). Deadlines expire to `unconfirmed` (`expirePendingComputerLifecycleOperations`, verified: reasons `shutdown_ack_timeout | disconnect_timeout | ready_timeout`). `failed`/`rolled_back`/`superseded` come from `terminalizeComputerLifecycleOperation` (called on `upgrade:done`/`restart:done` with `ok:false`, or on failed policy revalidation). There is also a `legacy_k_promoted` completion mode for local upgrades (`computerLifecycleOperationService.ts:338-368`).

**Local ack outbox entry**: `{pendingPhases:[shutdown,ready]} → [ready] → removed` on each `computer:lifecycle:receipt`.

**K operation receipt** (external, observed via usage): `outcome null (active) → promoted | rolled-back | held | up-to-date | failed`; then `acknowledgedAtMs null → set`. A new upgrade is admitted only from genesis or an acknowledged terminal receipt (`pc/kUpgradeProcess.ts:inspectKUpgradeStart`).

**Device authorization**: `pending → approved → consumed`, or `denied | expired`, soft `revoked`.

## 6. Design patterns worth stealing

1. **Level-triggered reconcile loop with a pure eligibility function.** `reconcile()` recomputes desired (managed.flag files) vs actual (child records) every 5s and after events; `canSpawn(rec, wanted, now)` is pure and tested (`pc/lib/runnerStateMachine.ts`, `runnerStateMachine.test.ts:71-114`). The explicit `backoffUntil` replaced a side "restarting" set that caused double spawns (header comment). For you: Temporal workflows already give you this for orchestration, but anything that owns long-lived processes (AgentCore sessions) benefits from "desired state on disk/DB + idempotent reconciler" instead of imperative start/stop.
2. **Exit classification drives policy.** Exit codes 77/78 and log-regex classes map to distinct states, so a missing dependency or a revoked credential does not burn crash budget or spin (`pc/service.ts:330-350, 557-570`). Useful for classifying agent failures (auth vs. transient vs. config) before retrying.
3. **Durable intent before side effect, outbox + receipt for acknowledgements.** Server writes the user intent row before dispatch; the machine writes `lifecycle-operations.json` before acting; acks ride on every `ready`/`machine:shutdown` until the server sends an explicit receipt, then the local entry is deleted (`pc/lifecycleOperations.ts`, `packages/daemon/src/core.ts:4204`, `agentOrchestrator.ts:5356`). Exactly-once effect via at-least-once delivery + idempotent server observe.
4. **Evidence-bound completion.** "Done" requires proof from the live process, not a spawn return: connected marker pid equals child pid; K health probe comes from an IPC answer, never files; ready ack carries `serviceGeneration` and a list of old pids verified dead (`pc/residentLifecycleBridge.ts:50-100`, `pc/kHostAdapter.ts:14-20`). Map to: an agent task is "done" only when a receipt from the executing session says so, keyed to that session's incarnation id.
5. **Operation identity + replay/conflict semantics.** Same id + same fields = replay (return the in-flight result); same id + different fields = `OPERATION_IDENTITY_CONFLICT` (`pc/lifecycleOperations.ts:enqueueLifecycleOperation`, `pc/serviceUpgradeStart.ts`, `acceptMachineOperation`). This is the same discipline as Temporal workflow IDs; apply it to your registry writes too.
6. **Pure reducer + optimistic CAS store.** `reduceMachineConvergence(record, event)` returns `applied|replay|conflict|rejected` plus effect intents with dedupe keys; `reduceDurableMachineOperation` loads, reduces, and `compareAndSwap(phaseVersion)` with 20 retries (`pc/machineOperationRuntime.ts:50-72`). Effects are recorded as intents in the record before execution, so a crashed executor can re-derive them. The server table mirrors the same phases and `phaseVersion` column.
7. **Crash-safe file writes.** `writeDurableTextFile`: exclusive tmp name, fsync file, rename, fsync dir, read back (`pc/durableFile.ts`; concurrency test in `durableFile.test.ts`). Park snapshot and runner hold use it.
8. **Two-slot upgrades with a host adapter boundary.** K owns journal/lock/slots/rollback; Computer implements five verbs `quiesce/stop/start/healthProbe/resume`. `start` never throws for world-state failures; the probe is the only judge, which keeps rollback on one code path (`pc/kHostAdapter.ts:290-320`). `resume` checks the successor serves the *same* parked set, version-agnostic, so it also validates rollback.
9. **One install path = one upgrade path.** The installer serves its own bytes over loopback so first-install and upgrade go through the same verified pipeline (`pc/kInstallerConvergence.ts:98`).
10. **Secrets confined by process.** `sk_computer_*` lives only in `runner.state.json` (0600) and in that server's runner; service never reads it; events expose an 8-char prefix (`pc/service.ts:17-19`, `pc/services/attach.ts:8-10`). Per-tenant credential isolation by child process.
11. **Single-writer mutations through the supervisor.** `reset-runner` goes through IPC so in-memory state and disk agree (`pc/service.ts:1085-1095`).
12. **Server-side one-in-flight guard as a partial unique index** (`idx_computer_lifecycle_operations_one_pending_machine`) instead of application checks; lease-based dispatch claim with conditional UPDATE.

## 7. Surprises / sharp edges

- **The Computer embeds the daemon.** `__run` imports `@botiverse/raft-daemon/core` in-process; the daemon package is both the legacy standalone agent host and the engine inside the Computer. Agents are still spawned per turn by DaemonCore (`manual/agent-knowledge/computer.md`, "Per-turn lifecycle").
- **OS supervisor is legacy.** `osSupervisor.ts` builds launchd (`KeepAlive`), systemd (`Restart=always`), and Windows scheduled-task definitions, but `retireLegacyOsSupervisor` is an installer-only migration *away* from it (`pc/osSupervisorRuntime.ts:415-440`, `pc/legacyOsSupervisorMigration.ts`). Current model: detached self-owned service; on macOS a login LaunchAgent with `RunAtLoad` and no `KeepAlive` (`pc/macosLoginCarrier.ts:230-235`). So nothing external restarts a crashed service until next login or a manual `start` (inferred from absence of KeepAlive and no other watchdog found).
- **The machine convergence reducer is mostly unwired.** `acceptDurableMachineOperation` and `spawnLegacySupervisorTakeover` have no production callers and no tests; the only consumer is the hidden `__legacy-supervisor-takeover` worker (`cli.ts:850`) which reduces events only if a record already exists. The live upgrade path uses K + the server's dispatch table, and the server writes only 4 of the 12 phases. Treat the reducer as a spec/design, not load-bearing code.
- **Restarting from inside an agent kills the agent.** The manual warns agents are descendants of the service; `restart` on your own host terminates you mid-command (`computer.md`).
- **Readiness is not "process spawned".** A runner that loses the machine lock exits in ms with "Another Slock daemon is already running". If the incumbent pid is alive, the service adopts it as `externalPid` instead of counting a crash. If that pid already died, it retries after 2s. If no owner pid can be parsed, it parks the runner as `degraded` (`pc/runnerLockConflict.ts`, `pc/service.ts:549-555`).
- **Degraded survives restarts and upgrades**, with a caveat. It is rehydrated from `health.json`, but crash-budget degradation is recomputed against a sliding 60s window at boot. If the service restarts more than 60s after the third crash, the runner comes back as `stopped` and is respawned. `fatalConfig` (78) and terminal-unlinked (77) persist until `reset`/`setup`. Terminal-unlinked is not cleared by `start`/`reset`; only `setup` fixes it.
- **Parent-lock marker env var**: a detached service spawned while the CLI holds the mutation lock must skip stale-lock cleanup or it would steal the lock mid-upgrade (`pc/service.ts:180-205`). It is stripped from runner env (`buildRunnerChildEnv`).
- **K receipts block future upgrades** until acknowledged; a rolled-back local upgrade needs `raft-computer operation acknowledge <id>`; never delete `operation.json`/`upgrade.lock` by hand (`computer.md` Gotchas; `pc/kOperationAcknowledgement.ts` refuses while K lock is live or processes not quiescent).
- **Log rotation only at spawn** (inherited fd), by UTC date, 14-day retention, 64MB cap constant; a runner running across midnight keeps writing the old file (`pc/logRotation.ts:1-30`).
- **Env capture before import.** For launchd/systemd boots, `index.ts` captures the login-shell env with a nonce-framed `__print-env` and applies it before any module reads env at init; Windows and foreground keep inherited env (`pc/index.ts:1-25`).
- **SEA quirks**: `PI_PACKAGE_DIR` shim so the bundled pi runtime can read a package.json (`pc/service.ts:350-375`); agent CLI is re-exec `<exe> __cli` (`resolveResidentSlockCliPath`).
- **Two IDs per attachment**: `serverMachineId` = `computers.id` vs `machineId` = `daemons.id`; mixing them caused a real bug (comment in `pc/services/attach.ts:52-60`).
- **One Computer per server.** Same machine can run several attachments (one runner each) but server-side a computer row belongs to one server.

## Verification log

Checked against code (all in `/home/user/raft-source`):
- `pc/lib/runnerStateMachine.ts`, `pc/lib/state.ts`: confirmed `canSpawn` conditions, 5 runner states, and the trigger vocabulary. **Added** the `exit-lock-owner-gone` trigger and the lock-conflict branches, which the notes had missed.
- `pc/service.ts` (exit handler, `spawnChild`, `reconcile`, `runService` boot, `defaultCoreFactory`): confirmed 2s backoff, the 5s loop (`serviceReconcileLoop.ts`), the classification order, and that `onComputerControl` enqueues `[shutdown, ready]` before IPC. **Fixed:** graceful exits also arm the backoff; the 100ms ready check is a self-rescheduling reconcile, not a separate poll. **Added:** the K runner hold blocks spawns, dead `externalPid` cleanup, and the K recovery child spawn.
- `pc/health.ts`: confirmed 60s/3. **Corrected** "degraded always survives restarts": crash-degraded is recomputed against a sliding window at boot.
- `pc/runnerLockConflict.ts`: **added** the three outcomes (adopt, retry, degraded).
- `health.test.ts`: **added** `computer_revoked` as a second terminal 77 reason.
- `pc/services/start.ts`: confirmed the 15s timeout. `start` refuses terminal-unlinked targets.
- `pc/residentLifecycleBridge.ts`, `pc/lifecycleOperations.ts`: confirmed the ready ack needs K `promoted`, target == running version, origin match, and dead prior pids. Receipt removes the phase and then acks K. Also noted `retireCompletedUpgradeShutdownsFromLog` exists.
- `pc/kHostAdapter.ts`, `pc/kUpgradeCoordinator.ts`, `pc/kUpgradeProcess.ts`: confirmed the five hooks, start = reachability only, the probe goes over IPC, resume releases the hold and compares the parked set, the 25ms/10s wait, and local-only self-ack on promoted/up-to-date.
- `packages/server/.../computerLifecycleOperationService.ts`, `agentOrchestrator.ts`: **added** policy revalidation before dispatch, `first_hop_observed` written at command-send, the failure path (`rolled_back`/`failed` plus both receipts), the timeout reasons (they were marked "inferred" before), and that the server trusts machine-attested dead pids.
- Reducer usage: grep confirms `machineConvergenceReducer`/`machineOperationRuntime` are used only by `legacySupervisorTakeover.ts`, `spawnLegacySupervisorTakeover` has no callers, and there are no tests. The claim stands.
