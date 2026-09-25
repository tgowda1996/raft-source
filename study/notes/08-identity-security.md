# 08 — Identity, permissions, scopes, integrations

All paths are relative to `/home/user/raft-source`. `S=packages/server/src`, `SH=packages/shared/src`, `D=packages/daemon/src`, `C=packages/cli/src`.

## 1. What it is

Raft has two kinds of principal that can act in a workspace: humans (`users` rows) and agents (`agents` rows). Both hold a role in a server (`server_members` for humans, `server_agent_members` for agents), both hold channel memberships (`channel_humans` / `channel_agents`), and both are checked against the same 40-entry server capability matrix (`SH/serverPermissions.ts`). They authenticate very differently: humans use a JWT access token backed by rotating refresh-token "sessions" grouped into revocable "session families"; agents use long-lived `sk_agent_*` API keys that the daemon holds on the agent's behalf and never hands to the model process. On top of role and membership, agents carry two more gates that humans do not have: credential "capabilities" (coarse, set when the key is minted) and human-editable "scopes" (fine-grained, one row per agent). Third parties connect through three separate surfaces: Login-with-Raft OAuth apps (agents and humans sign into external services as themselves), a Slack bridge (Slack users appear in Raft as "external projections", not users), and Raft App Platform system apps that push items into an agent's inbox.

## 2. Key components

| Name | File path(s) | Responsibility |
|---|---|---|
| Server capability matrix | `SH/serverPermissions.ts:12-92` | 40 named capabilities; role → capability bundle (owner=all, admin=all but `manageBilling`, member=10, guest=0). |
| Role transition policy | `SH/serverPermissions.ts:105-176` | Pure functions `canChangeMemberRole`, `canTransitionServerRole` (last-owner rule, admins can't touch owners/admins). |
| Channel permission policy | `SH/channelPermissions.ts` | Channel-local admin role (closed set of 5 capabilities), guest read/join/post rules, `canAddChannelMembers`. |
| Actor context | `S/lib/actorPermissions.ts` | `resolveActorContext(serverId, "user"|"agent", id)` → one `ActorContext` shape for both principal types; `userCanActOnAgentResource` (capability OR human creator). |
| Channel actor context | `S/lib/channelActorPermissions.ts` | Joins server role + channel membership + channel role; `withLockedChannelActorCapabilities` does the check inside a `SELECT … FOR UPDATE` transaction. |
| `canUserAccessChannel` | `S/services/channelService.ts:4273` | The single "can this human see this channel" function; used by HTTP routes, socket `join:channel` (`S/socket/index.ts:284`), replay, attachments (51 call sites across 19 non-test files). |
| `canAgentAccessChannel` | `S/services/channelService.ts:5472` | Agent twin; public → yes, private/DM/joint → `channel_agents` row. |
| Human auth middleware | `S/middleware/auth.ts:66-463` | `requireAuth`, `requireServer`, `requireFlexAuth`, `requireServerMatchesParam`; `verifyActiveAccessToken` checks user not retired and session family not revoked on every request. |
| Session service | `S/services/sessionService.ts` | Create / rotate / revoke refresh sessions; session families; crash-safe rotation receipts; calls `revokeSocketAccess` after commit. |
| Socket access revocation | `S/socket/accessRevocation.ts`, `S/socket/index.ts:83-176` | In-process listener bus + Redis-adapter fanout that closes live sockets when a session, role, or channel visibility changes. |
| Route auth registry | `S/middleware/routeAuthPolicy.ts`, `S/middleware/authFromRegistry.ts` | Declarative table of which principal (`sk_agent`, `sk_computer`, `sk_machine`) may call each `/internal/agent-api/*` and `/internal/computer/*` path; unregistered path under a claimed prefix → 401. |
| Agent credential service | `S/services/agentCredentialService.ts` | Mint/lookup/revoke `sk_agent_*` keys (argon2id + prefix index); bootstrap tokens (`abtk_*`) with CAS single-consume. |
| Agent credential route | `S/routes/agentCredentials.ts` | `POST/GET/DELETE /api/agents/:id/credentials` — human session mints a key for an agent (external agents). |
| Computer runner mint | `S/routes/internalComputer.ts:1008-1200` | `POST/DELETE /internal/computer/runners/:agentId/credentials` — a Computer (`sk_computer_*`) mints/revokes a key for an agent assigned to its machine. |
| Agent capability gate | `S/routes/internalAgentApi.ts:736-778` | `requireAgentCapability(cap)`: cap must be in the credential's `scopes` column, and in the `X-Slock-Agent-Active-Capabilities` header if present (else 501). |
| Agent scopes service | `S/services/agentScopesService.ts`, `SH/agentScopes.ts` | 19 grantable + 4 intrinsic scopes; `default` vs `custom` profile; whole-set updates with revision counter. |
| Agent scope middleware | `S/middleware/agentScope.ts`, `S/middleware/agentRouteScopeCoverage.ts` | `requireAgentScope(scope)` on legacy `/internal/agent/:id/*`; a test asserts every route is scope-gated or allowlisted with a reason. |
| Daemon credential proxy | `D/agentCredentialProxy.ts`, `D/drivers/cliTransport.ts:630-700` | Holds the raw `sk_agent_*` in daemon memory; the agent's CLI talks to `127.0.0.1` with a per-launch `sap_*` token; proxy swaps in the real bearer and pins the origin. |
| CLI identity modes | `C/auth/env.ts` | Managed runner (proxy URL + token file) vs external agent (`RAFT_PROFILE` → `~/.slock/profiles/<slug>/credential.json`). |
| Device-code login | `S/services/deviceAuthService.ts`, `C/commands/agent/login.ts` | Pre-credential grant: CLI shows code, human approves in browser, CLI gets a user session, then mints `sk_agent_*`. |
| OAuth / Login with Raft | `S/services/oauthService.ts`, `C/commands/integration/*` | App registration, marketplace, installs, agent access requests, grants, access tokens. |
| Action cards | `S/services/actionCardsService.ts`, `SH/actionCards.ts` | Agent prepares a typed action; a human commits it under the human's own authority. |
| Slack bridge | `S/services/externalAppIngressService.ts`, `externalInboundWorkerService.ts`, `externalDeliveryOutboxService.ts`, `externalDeliveryWorkerService.ts`, `slackBridge*.ts` | Signed ingress → encrypted inbound queue → canonical message; outbound transactional outbox → leased delivery worker. |
| RAP system apps | `manual/agent-knowledge/app.md`, `S/services/rapAppConfigService.ts` | `system.reminder`, `system.cleaner` push items into each agent's inbox; config via `raft app config`. |

## 3. Data model

All in `S/db/schema.ts`.

**Principals**
- `users` (l.26): `id`, `email` (unique), `name` (unique handle), `passwordHash`, `emailVerified`, `retiredAt` (retired users fail `verifyActiveAccessToken`, `S/middleware/auth.ts:89`), `profileSetupCompletedAt`.
- `agents` (l.851): `id`, `serverId` (an agent belongs to exactly one server), `name` (unique per server among non-deleted), `status` (`active|inactive|stopped`), `runtime`, `model`, `executionMode` (`byoc|cloud`), `creatorType` (`user|agent`), `creatorId`, `machineId` (column `daemon_id` → which machine runs it), `deletedAt` (soft delete).
- `external_actor_projections` (l.2180): Slack people/bots. `provider`, `installId`, `workspaceId`, `externalActorId`, `displayName`, `actorKind` (`human|guest|remote|bot|unknown`), `state` (`active|tombstoned`), `projectionRevision`.
- `messages.senderType` (l.1650): `user | agent | external_projection`; `senderId` is the id in the matching table. This is the one place all three principal kinds converge.

**Membership and roles**
- `servers` (l.249): `ownerId`, `kind` (`normal|joint_storage` — `joint_storage` servers are excluded by every membership check, `S/middleware/auth.ts:228`), `hideHumansFromMembers`, `publiclyVisible`, `deletedAt`.
- `server_members` (l.274): PK (`serverId`,`userId`), `role` (`owner|admin|member|guest`), plus many per-user UI prefs.
- `server_agent_members` (l.1029): PK (`serverId`,`agentId`), `role` (`member|admin` only — no agent owner).
- `server_member_role_audit_events` (l.669): `actorUserId`, `targetUserId`, `previousRole`, `nextRole`; CHECK `previous <> next`.
- `server_membership_departures` (l.383): `reason` (`left|removed`) kept after the membership row is deleted.
- `channels` (l.1445): `type` (`channel|private|joint|dm|thread`), `guestVisible`, `guestJoinable` (CHECK joinable ⇒ visible), `parentMessageId` (threads), `archivedAt`, `deletedAt`.
- `channel_humans` (l.1605) / `channel_agents` (l.1590): PK (`channelId`, principal id), `role` (`member|admin`), `authorityRevision` (bumped when authority changes).
- `channel_membership_role_events` (l.1621): durable outbox for channel role changes; `deliveryStatus` (`pending|sent|dead_letter`), `deliveryAttempts`.

**Human sessions**
- `session_families` (l.686): `userId`, `revokedAt`, `revokedReason`, `capabilityRetainUntil`.
- `sessions` (l.700): `familyId`, `tokenHash` (sha of refresh token), `expiresAt` (30 days).
- `session_token_predecessors` (l.716): hash-only lineage of rotated tokens; "authorizes logout, never token replay".
- `session_refresh_rotation_receipts` (l.729): AES-GCM encrypted successor token so a client that crashed mid-refresh can retry with the old token exactly once.

**Agent credentials and scopes**
- `agent_scopes` (l.5825): PK `agentId`, `scopes` (jsonb array of grantable literals), `mode` (`default|custom`), `revision`, `updatedByUserId`.
- `agent_credentials` (l.5908): `agentId` (bound for life), `apiKeyHash` (argon2id), `apiKeyPrefix` (first 16 chars, indexed; partial index `WHERE revoked_at IS NULL` added in raw SQL), `scopes` text[] (the credential "max capabilities"), `createdByUserId`, `lastUsedAt/Ip/UserAgent`, `revokedAt/ByUserId/Reason`. Rows are never deleted.
- `agent_bootstrap_tokens` (l.5980): `tokenLookupHash` (HMAC-SHA256 with server pepper, bytea unique), `tokenHash` (argon2id), `targetAgentId`, `issuedByUserId`, `scopes`, `ttlExpiresAt`, `consumedAt`, `consumedCredentialId`, `revokedAt`.
- `device_authorizations` (l.6056): device-code grant (`dvc_*` device code + short human `user_code`, default TTL 10 min, `deviceAuthService.ts:35`), same lookup-hash + argon2 pattern; `status` is free text (not a pg enum) taking `pending|approved|denied|consumed`, `approvedByUserId`, `consumedAt`.

**Integrations (Login with Raft)**
- `oauth_clients` (l.1042): `serverId`, `appType` (`server_local|slock_builtin|third_party_global`), `allowedScopes`, `publishStatus` (`private|publish_requested|in_review|published|rejected|unpublish_requested`).
- `oauth_client_maintainers` (l.1098): `principalType` (`agent|human`) with CHECK exactly one of `agentId`/`userId`; `role` (`owner|rotate`).
- `oauth_client_installs` (l.1149): (`serverId`,`clientId`) unique, `status` (`active|suspended`).
- `oauth_access_requests` (l.1244), `oauth_grants` (l.1268), `oauth_access_tokens` (l.1317): per-agent (or per-human) scoped access, `status` `pending|approved|denied`.
- `integration_audit_events` (l.1287): `eventCategory`, `source` (`web|api|cli|action_card|system`), `actorType`, `subjectType`.
- `action_cards` (l.5453): `requesterAgentId`, `messageId` (carrier message, unique), `actionType`, `payload` (frozen at prepare; overwritten with the merged action if the human commits with overrides), `state` (text, default `prepared`), `executedByUserId`. The card state is also mirrored on the carrier message's `messages.action_metadata` jsonb (the comment in `SH/actionCards.ts:424` still says "no separate table", which is stale).

**Slack bridge** (l.1717–3233): `external_app_registrations` (provider fixed to `slack`), `external_app_installs` (`pending|active|reauth_required|disconnected|revoked|quarantined`), `external_channel_bindings` (`active|paused|revoked|quarantined`, `consentedByType human|agent`), `external_inbound_events` (`queued|processing|committed|duplicate|echo|quarantined|dead|revoked`, encrypted payload + erase columns, lease columns), `external_outbound_deliveries` (10 states, render snapshot + digest, lease), `external_message_links` (`firstDirection raft_outbound|provider_inbound`), `external_author_policies`, `external_delivery_operator_decisions` (l.2813: one-shot human/agent decisions `retry_in_place|skip` on a stuck delivery, with `duplicateRiskAcknowledged` / `dataLossAcknowledged` flags).

## 4. Flows

### F1. Human request to an HTTP route
1. Browser → server: `Authorization: Bearer <JWT>` + `X-Server-Id`.
2. `requireAuth` (`S/middleware/auth.ts:120`) → `verifyActiveAccessToken` (l.80): verify JWT signature and `type=access`; SELECT `users` (reject if `retiredAt`); if token carries `familyId`, SELECT `session_families` (reject if `revokedAt`). Sets `req.userId`, `req.sessionFamilyId`. Access tokens live 15 min (l.67).
3. `requireVerified` → `requireProfileSetupComplete` (l.153-206).
4. `requireServer` (l.212): SELECT `server_members ⋈ servers` where not `joint_storage` and not deleted; 403 otherwise. Sets `req.serverId`.
5. Handler: object checks, e.g. `canUserAccessChannel(channelId, userId, serverId)` (`channelService.ts:4273`) or `actorHasServerCapabilityInServer(serverId, "user", userId, cap)` (`actorPermissions.ts:64`).
Nothing is persisted by auth itself.

### F2. `canUserAccessChannel` decision (single enforcement point)
1. Caller → `getChannel(channelId)`; missing → false.
2. `channel.serverId !== serverId` → false (cross-server IDOR guard, added 2026-04-19 per comment l.4262).
3. Resolve human server role. If `guest`: threads recurse to the parent channel (joint threads denied); else `canGuestReadChannel` (`SH/channelPermissions.ts:74`) with the guest feature gate.
4. Hidden `#all` → false. `type=channel` (public) → true.
5. `joint` → `resolveChannelAccess` must pass. `thread` → recurse on parent message's channel (or joint projection's local parent).
6. `dm` → true only if user has a `channel_humans` row (agent-agent DMs are never readable through human routes, l.4336-4339).
7. `private` → `channel_humans` row required.
Used by: socket `join:channel` (`S/socket/index.ts:281-289`, silent return on deny, and a join is skipped if the socket was disconnected during the await), messages, attachments, tasks, actions, workflows, share artifacts (51 call sites in 19 non-test files).

### F3. Socket connect and live revocation
1. Client → Socket.IO handshake with `{token, serverId, clientKind}` (`S/socket/index.ts:124`). `serverId` may be null (account-level socket); then no membership check runs and no `serverRole` is stored.
2. Middleware verifies JWT synchronously, stores `userId/familyId/serverId` in `socket.data`, adds socket to `pendingHandshakes` **before** any DB await (l.133-140) so a concurrent revocation can find it.
3. `verifyActiveAccessToken`; `serverService.isMember`; store `serverRole` (used by `scope: "guests"` revocations; a socket whose role is still unresolved counts as a guest and is evicted — fail closed). Last middleware step: if `socket.data.accessRevoked` was set by a revocation that raced the awaits, reject with "Authentication changed; reconnect required".
4. On connect: remove from `pendingHandshakes` and re-check `accessRevoked` once more (an eviction can land between middleware completion and namespace registration, l.179-185); join `user:<id>` and a per-client-kind room; client later emits `join:channel` → F2 check → `socket.join("channel:<id>")`.
5. Revocation: a service commits a transaction (e.g. `revokeSession` `sessionService.ts:426`, `changeServerMemberRole` `serverService.ts:1196`, channel visibility change `channelService.ts:1889-1898`, member removal `routes/channels.ts:3617`) → `revokeSocketAccess(revocation)` → local `evict()` sets `socket.data.accessRevoked=true` and closes the transport; `fanoutWithAck(io, "access:revoked", …)` delivers to every replica via the Redis adapter, each replica runs `evict` and acks. `fanoutWithAck` (`S/socket/fanout.ts:13`) is a no-op when Redis is not configured (single instance), throws if the Redis publisher is not `ready`, and times out after 10 s; the caller's post-commit step then fails (e.g. `changeServerMemberRole` notes "authorized idempotent retries must repair a failed post-commit fanout").
6. Revocation shapes (`accessRevocation.ts:3-10`): `{userId, familyId?}`, `{serverId, scope: all|guests}`, `{serverId, scope: "non-members", channelId, memberUserIds}` (membership snapshot travels with the event so every replica decides the same way).
Client must reconnect and re-run F2 for each channel.

### F4. Refresh-token rotation
1. Client → `POST /api/auth/refresh` with refresh token.
2. `refreshSessionWithTrace` (`sessionService.ts:752`) first `validateSession`s the token; if missing it goes straight to the replay lookup (step 3). Otherwise `rotateSession` (`sessionService.ts:531`): in a transaction, lock the user row (`lockActiveUser`, returns null for retired users), `DELETE sessions WHERE tokenHash=… AND userId=… AND expiresAt>now RETURNING` (atomic consume), create a `session_families` row if the old session had none (legacy upgrade), INSERT new `sessions` row in the same family with a **fresh** 30-day expiry (sliding window), INSERT `session_token_predecessors` row for the old hash.
3. After commit: `rememberRotation` stores old→new in a local map and Redis (`slock:auth:rotated-refresh-replay:` prefix) for a 10 s grace window (`AUTH_REFRESH_ROTATED_REPLAY_GRACE_MS`, `SH/authRefreshTiming.ts:5`), so a second tab racing the same old token gets the same new token instead of a logout. Presenting an already-rotated token **after** the grace window just returns null (that client is logged out); it does **not** revoke the family, so there is no OAuth-style refresh-token-reuse theft detection.
4. A durable variant (`refreshSessionWithDurableReplay`, l.577) writes `session_refresh_rotation_receipts` with the successor encrypted, bound to `installationId` + `attemptId`.
5. Logout (`revokeSession`, l.426): the presented token is resolved via the live session, or a predecessor hash, or an unexpired durable rotation receipt (so logging out with a just-rotated token still revokes the right family); delete session, keep predecessor hash, `revokeSessionFamilyInTransaction(reason="logout")` marks `session_families.revokedAt` and deletes all family sessions, then after commit `revokeSocketAccess({userId, familyId})`. Any access token carrying that `familyId` fails on its next request (F1 step 2) even though the JWT itself has up to 15 min left.

### F5. Managed agent gets a credential (Computer path)
1. Server → daemon (over `/daemon/connect`): start agent X. (Orchestration is covered in other notes.)
2. Daemon `ensureManagedRunnerCredential` (`D/agentProcessManager.ts:3919`, up to 3 attempts for retryable errors; hard-fails the launch with `runner_credential_mint_failed` — no fallback to the legacy machine token) → `requestManagedRunnerCredentialOnce` (l.3876) → `POST /internal/computer/runners/:agentId/credentials` with `Bearer sk_computer_*` (or `sk_machine_*` alias), body scopes = all 9 capabilities, name `runner:<runtime>:<id8>`.
3. Server `authFromRegistry` (`S/middleware/authFromRegistry.ts:97`) matches registry row → `requireComputerAuth` (`auth.ts:718`) sets `req.computerId/serverId`.
4. Handler (`internalComputer.ts:1032`) checks `agent.serverId == computer's server` AND `agent.machineId == computer's machine` (404 on mismatch — no existence leak), normalizes capabilities (body omitted → all 9), calls `mintAgentCredential` (`agentCredentialService.ts:298`): generates `sk_agent_<64 hex>`, stores argon2id hash + 16-char prefix, returns raw key once.
5. Daemon keeps the raw key in memory; `cliTransport` (`D/drivers/cliTransport.ts:645-667`) deletes any legacy `agent-token` file, calls `registerAgentCredentialProxy` (`D/agentCredentialProxy.ts:1672`) which creates a random `sap_*` proxy token and registers it in an in-memory map on **one shared** proxy listener (`127.0.0.1`, random port, serves every agent on the machine, `agentCredentialProxy.ts:248,315`). The daemon writes the token to `~/.slock/agent-proxy-tokens/<agent>/<launch>.token` (dir 0700, file 0600), and writes `raft`/`slock` wrapper scripts (mode 0755, prepended to PATH) that inline-export `SLOCK_AGENT_PROXY_URL`, `SLOCK_AGENT_PROXY_TOKEN_FILE`, `SLOCK_AGENT_ACTIVE_CAPABILITIES`. These vars are deliberately **deleted** from the runtime's own spawn env (`cliTransport.ts:850-854`); they exist only inside the wrapper, so an in-process SDK driver still works.
6. On stop/exit (five call sites), daemon → fire-and-forget `DELETE /internal/computer/runners/:agentId/credentials/:credentialId` (`D/agentProcessManager.ts:3974`) → `revokeAgentCredential(reason="managed_runner_launch_ended")` (`internalComputer.ts:1219`) sets `revokedAt`. Credentials have **no expiry column**, so if the daemon crashes or the DELETE fails, the key stays valid until something else revokes it (it is only in daemon memory, so it dies with the process in practice); proxy registrations removed by `unregisterAgentCredentialProxyForLaunch`.

### F6. Agent CLI call through the proxy
1. Agent model → shell → `raft message send …` (CLI reads proxy URL + token file, `C/auth/env.ts`).
2. CLI → `http://127.0.0.1:<port>/internal/agent-api/v2/send` with `Bearer sap_*`.
3. Proxy looks up the registration by proxy token; builds target URL against registered `serverUrl`; if the origin differs → 403 `agent_proxy_origin_mismatch` (`agentCredentialProxy.ts:467`). Strips inbound `Authorization`/`Host`/hop-by-hop headers, sets `Authorization: Bearer sk_agent_*`, `X-Agent-Id`, `X-Raft-Client: cli`, and **overwrites** `X-Slock-Agent-Active-Capabilities` with the daemon's value (so the model cannot widen or drop it on the managed path), plus a fresh `traceparent` (l.477-497). The proxy does not filter paths; any path on the server origin is forwarded, and the server's route registry is what confines `sk_agent_*` to `/internal/agent-api/*`.
4. Server `authFromRegistry` → `requireAgentCredentialAuth` (`auth.ts:654`): prefix lookup on active credentials → argon2 verify → confirm server and agent not deleted → set `req.actingAgentId`, `req.agentCredentialScopes`, `req.serverId`, `req.principalKind="agent_credential"`; fire-and-forget `recordAgentCredentialUse`. Checked: credential not revoked, agent not soft-deleted, server not deleted. **Not** checked at auth time: `agents.status` (a stopped agent's live key still authenticates) or a `server_agent_members` row (a missing row just yields `serverRole=null`, i.e. zero server capabilities downstream).
5. Route: `requireAgentCapability(route.capability)` (`internalAgentApi.ts:736`) → handler → object checks (e.g. `actorHasServerCapabilityInServer(serverId,"agent",id,"joinPublicChannels")`, `canAgentAccessChannel`).
The agent identity is taken from the credential row only — there is no `:id` in agent-api paths (`routeAuthPolicy.ts:78-81`).

### F7. External agent login (device code)
1. Human creates agent in web UI (no computer). CLI on user's box → `raft agent login start` → `POST /api/auth/device/authorize` → `device_authorizations` row (`pending`), prints `user_code` + URL.
2. Human (browser, JWT) → `POST /api/auth/device/approve` → `approveDeviceAuthorization` (`deviceAuthService.ts:108`): CAS `UPDATE … WHERE status='pending'` → `approved`, `approvedByUserId`.
3. CLI polls `POST /api/auth/device/token` → `consumeDeviceAuthorization` (l.169): HMAC locate → argon2 verify → single consume → route issues a **user** session (the grant never mints `sk_*`).
4. CLI → `POST /api/agents/:id/credentials` with that user token, no `X-Server-Id` (`C/commands/agent/login.ts:373-400`). `S/routes/agentCredentials.ts`: resolve agent → caller's role in the agent's server (null → 404 `agent_missing`, same body as nonexistent); require `issueAgentCredentials` OR human creator (`userCanActOnAgentResource`) → `mintAgentCredential`.
5. CLI writes `~/.slock/profiles/<slug>/credential.json` atomically via temp dir + rename, mode 0600 (`login.ts:414-429`), verifies with `GET /internal/agent-api/` (whoami). Later calls use `RAFT_PROFILE`.
Newer "ordinary login" path accepts an existing `sk_agent_*` via hidden prompt/stdin instead (`login.ts` header comment).

### F8. Bootstrap-token exchange (gated, self-hosted runner)
1. Human → `POST /api/agents/:id/bootstrap-tokens` (only if `SLOCK_SELF_HOSTED_RUNNER_BOOTSTRAP_ENABLED=true`, `routes/agents.ts:3534`), needs `issueAgentCredentials` or creator; TTL ≤ 24h, default 30 min.
2. `issueAgentBootstrapToken` stores HMAC lookup hash + argon2 hash; returns `abtk_*` once.
3. Runner → `POST /api/agent/login` → `consumeAgentBootstrapToken` (`agentCredentialService.ts:508`): locate by HMAC → argon2 → distinct errors (`token_revoked|consumed|expired`) → **mint first** → CAS `UPDATE … SET consumedAt WHERE consumedAt IS NULL`; if 0 rows, revoke the just-minted credential with reason `bootstrap_exchange_race_lost` and return `token_consumed`.

### F9. Human edits an agent's scopes
1. Human → `PUT /api/agents/:id/scopes` `{scopes:[…]}` or `{mode:"default"}` (`routes/agents.ts:3464`).
2. Agent must be in `req.serverId`; caller must pass `canInspectAgentPrivateSurfaces` (editAgents OR human creator).
3. `updateAgentScopes` (`agentScopesService.ts:111`): `sanitizeGrantedScopes` drops unknown/intrinsic literals and sorts canonically; UPDATE (revision+1, `mode=custom`) or INSERT (revision 1).
4. Response = `AgentScopeSet`. The `agent:scope-updated` socket push is a TODO (`routes/agents.ts:3493`), so nothing is emitted.
5. Enforcement at call time: `requireAgentScope(scope)` (`S/middleware/agentScope.ts:55`) loads the row on every call for legacy `/internal/agent/:id/*`; on the agent-api surface only `action:prepare` (`internalAgentApi.ts:7472`) and wake delivery `inbox:receive` (`agentOrchestrator.ts:3779`) read `agent_scopes`.

### F10. Agent creates a channel (two gates)
- Legacy path: `POST /internal/agent/:id/channels` → `requireMachineAuth` → `requireAgentScope("channel:create")` → handler.
- Agent-api path: `POST /internal/agent-api/channels` → `requireAgentCapability("channels")` → `createChannelForAgent` (`S/routes/agentChannelCreate.ts:35`) → `actorHasServerCapabilityInServer(serverId,"agent",id,"createChannels")` → `channelService.createChannel(…, {type:"agent", id})`.
- Via action card: agent `raft action prepare` (scope `action:prepare`) → `action_cards` row + carrier message → human clicks → `actionCardsService` commit (l.1747) checks human is a server member → `createChannel` as the **human** → same socket broadcasts as `POST /api/channels`.

### F11. Agent signs into a third-party app (Login with Raft)
1. Agent → `raft integration login --service <key>` → agent-api → `oauthService.requestAgentAccess` (`oauthService.ts:3037`).
2. Resolve agent by `(server slug, agent name)`; client must be server-local, live built-in, or installed third-party.
3. If an unrevoked `oauth_grants` row covers requested scopes → insert `oauth_access_requests(status=approved)` ("reused").
4. Else if `canAutoGrantAgentClient(appType, installed)` → transaction inserts `oauth_grants` (granted by the app creator) + approved request ("created").
5. Else → insert/reuse `pending` request; for an uninstalled public app the CLI can post an owner/admin install card (`manual/agent-knowledge/integration.md`).
6. CLI consumes the one-time handoff internally (`exchangeAccessRequest`, l.3473) and stores the service session; the app later calls `/api/oauth/userinfo` and `/api/oauth/serverinfo`, both bound to the token's server.

### F12. Slack → Raft inbound
1. Slack → `POST` ingress URL → `verifyAndAdmitSlackIngress` (`externalAppIngressService.ts:502`): URL must be https and match an `external_app_ingress_endpoints` row; decrypt signing secret; verify HMAC signature with 5-min skew; `api_app_id` must equal the pinned registration.
2. `app_uninstalled` / `tokens_revoked` → `revokeInstallForLifecycleEvent`.
3. Otherwise resolve binding authority (`resolveExternalBindingAuthority`) and INSERT `external_inbound_events(status=queued)` with payload sealed (encrypted, AAD purpose `external-inbound-normalized-event`, 24h TTL); unsupported events get a discard receipt instead.
4. Worker `processExternalInboundEventOnce` (`externalInboundWorkerService.ts:688`): `SELECT … FOR UPDATE` oldest eligible (queued, or processing with expired lease), bump `leaseGeneration`, decrypt, upsert `external_actor_projections`, write a Raft message with `senderType=external_projection`.
5. Terminal status `committed`, `duplicate`, or `echo` (if `external_message_links.firstDirection='raft_outbound'`, i.e. our own outbound message coming back, l.1013); `terminalErase` wipes the payload.

### F13. Raft → Slack outbound
1. Human/agent → message send → `messageService` → inside the message transaction `maybeEnqueueOrdinaryMessageExternalDelivery({executor, …})` (`messageService.ts:2267`) inserts `external_outbound_deliveries(state=queued)` with a frozen render snapshot, digest, `reconciliationMarker`, `bindingEpoch`, `partitionPosition` (transactional outbox; same-transaction is inferred from the shared `executor` and the `transaction_pending` trace attr).
2. Worker `processExternalDeliveryPartitionHead` (`externalDeliveryWorkerService.ts:1029`) leases the head of one partition (per-binding ordering), calls Slack, records `external_delivery_attempts`.
3. Result → `accepted` (with `providerMessageId`, link row `raft_outbound`) / `retry_wait` / `outcome_unknown` (ambiguous send, reconciled via marker) / `dead` / `skipped` / `revoked` / `quarantined`.

## 5. State machines

**Session family**: `active` → `revoked` (reasons `logout | capability | revoke_all`, `sessionService.ts:498-520`). Terminal. `capabilityRetainUntil = revokedAt + 30d`.

**Session (refresh token)**: `live` → `consumed` (rotation; row deleted, hash moved to predecessors) | `expired` (deleted on validate) | `revoked` (family revoked). Consumed-within-grace → replay returns the same successor.

**Agent credential**: `active` → `revoked` (reasons in code: `user_revoked` (`routes/agentCredentials.ts:117`), `bootstrap_exchange_race_lost`, `managed_runner_launch_ended` (daemon stop)). No time-based expiry. Never deleted; never reassigned to another agent.

**Bootstrap token**: `issued` → `consumed` | `revoked` | `expired` (time; no status column, derived from `consumedAt`/`revokedAt`/`ttlExpiresAt`).

**Device authorization**: `pending → approved | denied` (CAS `WHERE status='pending'`, rejected if past 10-min TTL) → `approved → consumed` (CAS `WHERE status='approved'`, sets `consumedAt`). Expiry is checked at read time, not stored as a state.

**Agent scope profile**: `(no row, virtual default, revision 0)` → `custom` (PUT scopes) ↔ `default` (PUT mode=default). Every write bumps `revision`.

**Server role (human)**: `member ↔ admin ↔ owner`, plus `guest` behind a feature flag. Enforced by `canTransitionServerRole` inside `changeServerMemberRole` (`serverService.ts:1119`), which takes the owner count under the transaction and uses a CAS `UPDATE … WHERE role=<previous>`; post-commit `revokeSocketAccess({userId})`. Owner can do any transition except demoting the last owner; admin can only move `member|guest → admin|member|guest` for others, never self, never owners/admins (`serverPermissions.ts:146-176`). Moving to `guest` strips `#all` membership and demotes all channel-admin rows (`serverService.ts:1132-1162`). Each change writes `server_member_role_audit_events` and evicts the user's sockets.

**Agent server role**: `member ↔ admin` only (`updateAgentMemberRole`, `serverService.ts:1200`). The route (`routes/agents.ts:2028-2048`) requires `changeMemberRoles` and then uses the *older* policy `canChangeMemberRole`, not `canTransitionServerRole`: owners can do either transition, admins can only promote `member → admin` (admins cannot demote an admin agent). So human and agent role changes use two different transition functions.

**Channel role outbox event**: `pending` → `sent` | `dead_letter` (after 5 attempts, `channelMembershipRoleOutbox.ts`).

**OAuth access request**: `pending` → `approved | denied`. **Grant**: active → revoked. **Client publish**: `private → publish_requested → in_review → published | rejected`, `published → unpublish_requested`.

**Slack install**: `pending → active → reauth_required | disconnected | revoked | quarantined`. **Binding**: `active ↔ paused`, `→ revoked | quarantined`.

**Slack inbound event**: `queued → processing (leased) → committed | duplicate | echo | quarantined | dead | revoked`; expired lease returns `processing` to claimable.

**Slack outbound delivery**: `not_queued | queued → dispatching → accepted`; failure paths `retry_wait → dispatching`, `outcome_unknown → (reconcile) accepted | dead`, plus `skipped | revoked | quarantined`. A delivery stuck at the head of its partition can be unblocked by a human/agent `external_delivery_operator_decisions` row (`retry_in_place` or `skip`), each consumed once.

**Action card**: `prepared → executed` only (`ActionCardState = "prepared" | "executed"`, `SH/actionCards.ts:436`). A failed commit is a UI-only `failed` hint; the stored state stays `prepared` so the human can retry. Clicking an `executed` card is rejected (409 `STATE_MISMATCH` when `expectedState` is passed, `actionCardsService.ts:790`).

## 6. Design patterns worth stealing

1. **One actor context for humans and agents.** `resolveActorContext(serverId, "user"|"agent", id)` returns `{type, id, serverId, serverRole}` and all capability checks take that (`S/lib/actorPermissions.ts:5-37`). Why: when you make an orchestration system multiplayer, most checks should not care whether the caller is a person or an agent; a single role → capability table (`serverPermissions.ts`) keeps them from drifting.

2. **Named capabilities, not role comparisons.** Code asks `hasServerCapability(role, "editAgents")`, never `role === "admin"`. Adding a role or moving a permission is a one-line table change, and a test pins every role × capability cell (`SH/serverPermissions.test.ts`).

3. **Single enforcement function for visibility, reused by every transport.** `canUserAccessChannel` is called by HTTP, socket room join, replay, attachments. It takes a mandatory `serverId` so a UUID from another tenant cannot pass (`channelService.ts:4259-4281`). For a research system: one `canPrincipalSeeRun(runId, principal, tenantId)` used by REST, websocket, and Temporal query handlers.

4. **Revocation pushes, not just token expiry.** Session family revocation is checked on every request, and a committed authorization change closes live sockets across replicas with an acked fanout (`socket/index.ts:85-116`). Pending handshakes are registered before the first await so a race cannot slip through. Why: agents stream for hours; a 15-min token is not a revocation story.

5. **Agents never see their own key.** The daemon holds `sk_agent_*`; the model's shell gets a per-launch localhost token in a 0600 file; the proxy pins the upstream origin and overwrites `Authorization` (`D/agentCredentialProxy.ts:463-497`). A prompt-injected agent can exfiltrate at most a token that only works on 127.0.0.1 for that launch. Map to AgentCore: keep the Raft-equivalent credential in the runtime host/sidecar and give the model a scoped local capability.

6. **Principal-typed key prefixes + route registry that fails closed.** `sk_agent_`, `sk_computer_`, `sk_machine_`, `abtk_`, `sap_` make a leaked key self-identifying. `routeAuthPolicy.ts` lists every path under claimed prefixes with its principal; an unregistered sibling route returns 401 `auth_policy_unregistered_path`, and a wrong-kind key returns 401 `invalid_principal` (`authFromRegistry.ts:106-117`, `auth.ts:664-676`).

7. **Secret storage recipe.** HMAC(pepper, token) for O(1) lookup + argon2id for verification + prefix for display; raw value returned once; soft revoke, never delete (`agentCredentialService.ts:1-20`, schema l.5980+). Single-use exchange = mint then CAS claim, revoke the loser (`agentCredentialService.ts:508-605`).

8. **Agent drafts, human commits.** Agents with `action:prepare` create typed `action_cards`; the human's click executes with the human's identity and the same code path as the UI (`actionCardsService.ts:1747-1790`). Useful for any irreversible research action (spend, publish, external email).

9. **Scope coverage test.** Every `/internal/agent/:id/*` route must either carry `requireAgentScope` (detected via a tag on the middleware function) or appear in an allowlist with a written reason; stale entries also fail (`S/middleware/agentRouteScopeCoverage.ts`). A cheap way to make "forgot the permission check" a CI failure.

10. **Default-following vs pinned profiles.** `agent_scopes.mode = default` means new capabilities auto-enable; `custom` means they stay off until granted (`SH/agentScopes.ts:343-348`). No backfill needed: a missing row is a virtual default at revision 0 (`agentScopesService.ts:69-82`).

11. **Creator authority as a second path.** A human who created an agent can manage its scopes and credentials even after being demoted to member (`userCanActOnAgentResource`, `actorPermissions.ts:82-90`). Fits "my research agent" ownership in a shared team workspace.

12. **Foreign identities are projections, not users.** Slack people become `external_actor_projections` and `messages.senderType='external_projection'`; they can't log in or hold roles. Echo suppression uses a link table with `firstDirection` (`schema.ts:2980`). Inbound payloads are encrypted and erased after commit.

13. **Transactional outbox for side effects.** Channel role changes (`channel_membership_role_events`) and Slack deliveries (`external_outbound_deliveries`) are written in the same transaction as the domain change and delivered by a leased worker. You already have Temporal; the same idea applies to anything leaving your Postgres registry outside a workflow.

14. **Anti-enumeration errors.** "Agent doesn't exist" and "you aren't in its server" return byte-identical 404s (`routes/agentCredentials.ts:70-89`, `internalComputer.ts:1062-1067`).

## 7. Surprises / sharp edges

- **Four permission vocabularies for agents.** (a) server capabilities (40, via agent's server role), (b) channel capabilities/roles, (c) credential capabilities `send, read, mentions, tasks, reactions, server, channels, knowledge, mcp` (`agentCredentialService.ts:37-47`), (d) grantable scopes (19, `SH/agentScopes.ts:85-105`). (c) and (d) both appear as a field called `scopes` in different tables.
- **Human scope edits mostly don't reach the current agent CLI path.** The CLI now only talks to `/internal/agent-api/*` (`C/auth/env.ts:23-24`). That surface gates on credential capabilities (always all 9 for daemon-minted keys, `D/agentProcessManager.ts:3886`) and server role; `agent_scopes` is read only for `action:prepare` and `inbox:receive`. `requireAgentScope` guards the legacy `/internal/agent/:id/*` routes. So revoking e.g. `message:send` in the scopes API likely has no effect on a managed agent (inferred from the grep of `agentHasScope`/`loadAgentScopes` call sites; not tested end to end).
- **The "daemon scope cache" described in `SH/agentScopes.ts:28-33` does not exist.** No daemon/CLI code fetches `/scopes`, and the `agent:scope-updated` event is a TODO (`routes/agents.ts:3493`).
- **`X-Slock-Agent-Active-Capabilities` is a client-supplied header** used to narrow, never widen (`internalAgentApi.ts:762-775`); missing header means no narrowing. The daemon's default string omits `mcp` (`D/drivers/cliTransport.ts:15`), so MCP routes return 501 through the proxy unless that changes.
- **Docs and code disagree on member capabilities.** `MEMBER_SERVER_CAPABILITIES` includes `createChannels`, `addChannelMembers`, and `controlAgentRuntime` (`SH/serverPermissions.ts:77-88`, pinned by its test), while `permission-matrix.md` says members cannot create channels or start/stop agents and `server-role.md` says member agents can't create channels directly. `createChannelForAgent` checks only `createChannels`, so a member-role agent with the `channels` capability passes it (inferred from code; not run).
- **`canAgentAccessChannel(channelId, agentId)` takes no `serverId`**, unlike the human version (`channelService.ts:5472`), and for `type=channel` it returns true without checking that the agent belongs to that channel's server. Callers must bind the server themselves (the agent-api handlers do compare `channel.serverId`).
- **The manual names a capability that does not exist.** `permission-matrix.md:70-80` says channel create/add-member are gated by `manageChannels`; there is no such key in `SERVER_CAPABILITY_KEYS` (the real ones are `createChannels`, `addChannelMembers`, etc.).
- **No refresh-token reuse detection.** Replaying a rotated refresh token outside the 10 s grace window fails quietly instead of revoking the family (see F4).
- **Scope update is read-then-write without a transaction** (`agentScopesService.ts:117-143`); two concurrent PUTs can both compute the same next revision. Low impact because it is whole-set replace.
- **Access tokens outlive session revocation only if they lack `familyId`**: legacy tokens without a family are accepted until expiry (`auth.ts:78-99`).
- **`requireRetirementAuth` skips the live-session check on purpose** so a retiring user's token still works for the final receipt (`auth.ts:135-151`).
- **The `sap_*` token is reachable by the model, just not via env.** `SLOCK_AGENT_PROXY_TOKEN_FILE` is stripped from the runtime env but inlined in the 0755 `raft` wrapper script on PATH, and the token file is 0600 under the same OS user the runtime runs as. A model that `cat`s the wrapper and then the file gets a token that works only against `127.0.0.1` for that launch, and only on the agent's own server origin. The comment "never receive … the proxy token value, or a readable key-file pointer" (`cliTransport.ts:638-640`) is true of the env, not of the filesystem.
- **Agent credential lookup does an argon2 verify per request** with no cache (`agentCredentialService.ts:104-107`); prefix collisions are handled by looping candidates.
- **Channel `role=admin` cannot make a private channel visible**: channel admin grants only 5 capabilities and still requires access (`SH/channelPermissions.ts:10-22,147-163`), and a guest with a stale admin row is never elevated.
- **Action-card commit for `channel:create` checks only server membership** (`actionCardsService.ts:1747-1750`, `serverService.isMember`), relying on members already holding `createChannels`; it does not call the capability matrix, so a `guest` member clicking the card is not explicitly blocked there (inferred; guests have zero capabilities in the matrix).
- **Slack signing secrets and payloads are encrypted at rest with versioned AAD**, and endpoint/secret revisions are re-checked inside the transaction (`externalAppIngressService.ts:542-557`) so a rotated secret invalidates in-flight admissions.
- **RAP "apps" (`app.md`) are not OAuth apps.** They are system producers (`system.reminder`, `system.cleaner`) that push into the agent inbox; there is no human UI for their config.
- **`structural-enforcement.md` is not about permissions.** It is an agent-authored essay on moving rules from memory into tooling (guards, tests, generators). Its idea matches the scope coverage test and route registry above.

## Verification log

Checked against `/home/user/raft-source` (adversarial pass):

**Confirmed as written**
- 40 capability keys and the role bundles (owner=all, admin=all minus `manageBilling`, member=10, guest=0) in `SH/serverPermissions.ts`.
- `resolveActorContext` / `ActorContext` shape and `userCanActOnAgentResource` (capability OR human creator) in `S/lib/actorPermissions.ts`.
- `server_agent_members.role` enum is `member|admin` only.
- `canUserAccessChannel` flow (mandatory `serverId`, guest branch, hidden `#all`, joint, thread recursion, DM/private membership).
- `verifyActiveAccessToken` (retired user, family revocation, legacy no-family tokens accepted), 15 min access / 30 d refresh.
- `requireServer` excludes `joint_storage` and deleted servers.
- Socket `pendingHandshakes` registered before first DB await; three revocation shapes; `fanoutWithAck` over Redis.
- `requireAgentCapability`: 403 if not in credential scopes, 501 if the active-capabilities header is present and lacks it. Daemon default header omits `mcp` while the minted key has all 9.
- Route registry fails closed with 401 `auth_policy_unregistered_path`; wrong key kind → 401 `invalid_principal`.
- `agent_scopes` only read on agent-api for `action:prepare` (`internalAgentApi.ts:1385/7472`) and in the orchestrator for `inbox:receive`; the legacy bridge helper `registerAgentApiBridgeRoute` is defined but has no callers, so no agent-api route falls through to scope-gated legacy handlers. `agent:scope-updated` is still a TODO. 19 grantable scopes. Default-mode rows always resolve to the full grantable set.
- Computer mint handler: 404 `agent_missing` for both missing and cross-server/cross-machine agents.
- Manual vs code disagreement on member capabilities (`permission-matrix.md:70,79,116`).
- Slack inbound/outbound state enums and the `echo` vs `duplicate` decision (`externalInboundWorkerService.ts:1013`).
- Channel role outbox `pending → sent | dead_letter`.

**Changed / added**
- Device authorization: added `consumed` state, 10-min TTL, status is free text; split its state machine from bootstrap tokens.
- F3: `serverId` optional on socket; two extra `accessRevoked` re-checks (end of middleware, start of `connection`); fanout no-op without Redis, throws if publisher not ready, 10 s timeout.
- F4: added validate step, legacy family creation on first rotation, sliding 30 d expiry, 10 s grace value, **no reuse detection** (new sharp edge), logout resolves via predecessor or receipt.
- F5: mint retry (3 attempts) and hard-fail, one shared proxy listener, env vars live only in the wrapper (stripped from spawn env), revoke reason `managed_runner_launch_ended`, fire-and-forget revoke, no credential expiry.
- F6: proxy overwrites the active-capabilities header; no path filtering in proxy; agent auth does not check `agents.status` or agent membership row.
- Rewrote the "proxy token file is in env" sharp edge: it is in the wrapper script plus a 0600 file, not in the env.
- Agent server role changes use `canChangeMemberRole` (admins can only promote member→admin); human changes use `canTransitionServerRole`. Added to state machines.
- Action card: only `prepared|executed`; `failed` is UI-only; state is mirrored on `messages.action_metadata`.
- Added `external_delivery_operator_decisions` (`retry_in_place|skip`).
- New sharp edges: `canAgentAccessChannel` public branch has no server check; manual cites non-existent `manageChannels`; action-card commit uses only `isMember`.
- Line-number fixes: `mintAgentCredential` l.298, `consumeAgentBootstrapToken` l.508; call-site count 51 in 19 files (was "18 files").
