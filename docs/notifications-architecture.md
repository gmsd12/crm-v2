# Notifications Architecture

## Goal

Build the notifications subsystem in `crm-v2` around four priorities:

- laconic code paths
- predictable scaling
- flexible user settings
- gradual migration without breaking the current UI/API

## Current Baseline

The project already has:

- persistent in-app notifications
- per-event policies and per-user preferences
- SSE streaming for the frontend
- Celery + Redis for background delivery and scheduled checks

This is a good base, and the current refactor goal is to keep the full notification subsystem inside `apps/notifications` while preserving the existing UI and SSE behavior.

## Target Architecture

The target flow is:

`domain action -> notification outbox -> orchestrator -> in_app delivery -> user inbox`

### Layers

1. Domain publishing

- Domain code emits a notification-domain event such as `lead_assigned` or `comment_added`.
- Domain code does not build titles, bodies or recipient lists directly.
- The event is written to the notification outbox in the same DB transaction as the business change.

2. Outbox

- The outbox is the durable handoff between domain logic and notification processing.
- It prevents losing a notification event when the request succeeds but the background worker is temporarily unavailable.
- The outbox is processed asynchronously, with synchronous fallback when queue delivery is unavailable.

3. Orchestrator

- Reads outbox events.
- Dispatches them to concrete handlers.
- Applies fan-out, dedupe, retries and scheduling.
- Keeps transport-independent logic in one place.

4. Recipient resolution

- Resolves who should receive an event.
- Applies policy defaults and user preferences.
- Owns `watch_scope` semantics.
- Must become the only place where team/own/all audience logic lives.

5. Rendering

- Builds user-facing `title`, `body` and normalized payload.
- Must be separate from domain action publishing.

6. In-app delivery

- The current architecture is intentionally `in_app`-only.
- Delivery is modeled explicitly, but the code does not route through a generic channel registry.
- A future external channel can be added later, but that is not part of the current contract.

7. Inbox / user-visible notifications

- `Notification` is the inbox table and lives in `apps/notifications.models`.
- `NotificationDelivery` stores transport state separately from the inbox row.
- `NotificationDeliveryAttempt` stores per-attempt execution history.
- SSE and current API keep reading from the inbox table.

## Planned Module Layout

Phase target:

- `apps/notifications/models.py`
  - inbox model
  - policy and preference models
  - outbox model
  - delivery records, attempt logs and watch targets
- `apps/notifications/events.py`
  - notification event constants
- `apps/notifications/policies.py`
  - policy defaults
  - policy resolution
  - per-user effective settings
- `apps/notifications/registry.py`
  - domain event registry and metadata
- `apps/notifications/publishers.py`
  - stable publish API for domain code
- `apps/notifications/orchestrator.py`
  - outbox processing and dispatch
- `apps/notifications/tasks.py`
  - Celery entrypoints for outbox processing and notification delivery jobs
- `apps/notifications/runtime.py`
  - inbox emit/delivery primitives for `in_app`
  - delivery queue fallback and due-notification processing
- `apps/notifications/management/commands/`
  - operational entrypoints for due delivery and scheduled emissions
- `apps/notifications/recipients.py`
  - audience resolution
- `apps/notifications/renderers.py`
  - shared title/body formatting helpers
- `apps/notifications/handlers/lead_events.py`
  - lead assignment, unassignment, status change, deposit and comment notifications
- `apps/notifications/handlers/followups.py`
  - planned/overdue contact reminders and manager inactivity alerts
- `apps/notifications/handlers/alerts.py`
  - cross-domain alert scenarios such as partner duplicate spikes
## Rollout Plan

### Phase 1

- Add `apps/notifications`.
- Introduce `NotificationOutbox`.
- Add publisher functions.
- Route existing notification entrypoints through the outbox.
- Keep existing `Notification`, SSE and preferences unchanged.

### Phase 2

- Move recipient resolution into `apps/notifications/recipients.py`.
- Move rendering helpers into dedicated renderers.
- Move event handlers into `apps/notifications/handlers/*`.
- Move runtime and Celery delivery into `apps/notifications/runtime.py` and `apps/notifications/tasks.py`.
- Remove `apps/core/notifications.py` and `apps/core/tasks.py` from the notification execution path.

### Phase 3

- Introduce explicit team topology in IAM.
- Rebuild `watch_scope=team` on top of real team membership instead of role heuristics.

## Current Decisions

- Outbox events are stored in PostgreSQL/SQLite alongside the rest of the monolith.
- The orchestrator dispatches directly to `apps/notifications/handlers/*`.
- Handler modules use `apps/notifications/runtime.py` and `apps/notifications/recipients.py` directly.
- `Notification`, `NotificationPolicy` and `NotificationPreference` live in `apps/notifications.models`.
- `NotificationDelivery` is the single in-app transport record linked one-to-one to the inbox row.
- `NotificationDeliveryAttempt` stores the history of individual delivery executions.
- `NotificationPreference` is event-scoped: one row per `(user, event_type)`.
- `Notification.channel` is not stored because the system is currently single-channel by design.
- Celery beat and management commands point directly at `apps/notifications.*`.
- Notification API, SSE and admin live in `apps/notifications`.
- Existing user-facing notification rows remain the source for API and SSE.
- Domain code will call publisher functions directly; publishers schedule processing via `transaction.on_commit`.
- `apps/core` is no longer part of the notification subsystem beyond shared infrastructure helpers.

## In-App Contract

The final `in_app` contract is intentionally narrow:

- `Notification`
  - inbox row
  - user-facing title/body/payload
  - read/unread state
  - current visible lifecycle state: `pending`, `sent`, `failed`, `cancelled`
- `NotificationDelivery`
  - one-to-one transport state for the inbox row
  - scheduling, dedupe, aggregate attempt count, last delivery error
- `NotificationDeliveryAttempt`
  - append-only execution log for each delivery try
  - start/finish timestamps and terminal attempt status

Status synchronization rules:

- New emit creates `Notification(status=pending)` and `NotificationDelivery(status=pending)`.
- Successful delivery marks `NotificationDelivery(status=sent, sent_at=...)` and syncs the same terminal state to `Notification`.
- Failed delivery marks `NotificationDelivery(status=failed, last_error=...)` and syncs `failed` to `Notification`.
- Cancelled delivery marks `NotificationDelivery(status=cancelled)` and syncs `cancelled` to `Notification`.
- Read state belongs only to `Notification` and is never mirrored to delivery tables.

## Audience Configuration Without Full Teams

At the current stage there is still no full IAM team topology.

Because of that, `watch_scope=team` should be treated as explicit audience configuration instead of inferred hierarchy.

Current practical semantics:

- `own`: only own leads / own manager events
- `all`: all matching leads / events allowed by policy
- `team`: own leads plus explicitly configured watched users and watched roles

This allows users with elevated roles to organize notification coverage without building full teams first:

- watch only selected managers
- watch all managers by role
- watch only RET users
- mix own coverage with selected watched audiences

## Remaining Work

- Replace role-based audience approximation with explicit IAM team topology when teams actually appear.
- Expand automated coverage where needed around scheduled follow-up edge cases and long-running SSE polling.
- Keep event metadata code-driven unless product requirements justify a stronger registry contract.
