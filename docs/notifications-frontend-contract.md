# Notifications Frontend Contract

## Scope

This contract describes the current `in_app` notifications API in `crm-v2`.

Current design assumptions:

- notifications are single-channel `in_app`
- frontend works only with user inbox records
- transport internals (`NotificationDelivery`, `NotificationDeliveryAttempt`) are backend-only
- frontend must not expect a `channel` field anywhere

Base API prefix:

- `/api/v1/notifications/`

Auth:

- all endpoints require authenticated user
- user only sees their own notifications

## Notification Object

Returned by list, detail, mark-read and SSE `notifications` event.

```json
{
  "id": 123,
  "event_type": "comment_added",
  "status": "sent",
  "scheduled_for": "2026-03-11T14:00:00+01:00",
  "sent_at": "2026-03-11T14:00:00+01:00",
  "title": "Новый комментарий по лиду Ivan Petrov",
  "body": "Перезвонить после 16:00",
  "payload": {
    "lead_id": "15",
    "comment_id": "44"
  },
  "is_read": false,
  "read_at": null,
  "created_at": "2026-03-11T14:00:00+01:00",
  "actor": {
    "id": "7",
    "username": "manager_anna",
    "first_name": "Anna",
    "last_name": "Petrova",
    "role": "MANAGER"
  },
  "lead": {
    "id": "15",
    "full_name": "Ivan Petrov",
    "phone": "+79990001122"
  }
}
```

### Field Semantics

- `id`: numeric notification id
- `event_type`: stable event code
- `status`: frontend should currently expect only `sent` in list/SSE
- `scheduled_for`: planned delivery timestamp
- `sent_at`: actual sent timestamp
- `payload`: event-specific metadata, shape depends on `event_type`
- `is_read` / `read_at`: read state owned only by inbox
- `actor`: nullable, user who triggered the notification
- `lead`: nullable, lightweight lead preview

### Important

- list and SSE return only notifications with `status=sent`
- `pending`, `failed`, `cancelled` are backend lifecycle states and are not part of normal inbox UI flow
- frontend should treat unknown `payload` keys as optional

## Event Types

Current stable inbox `event_type` values:

- `next_contact_planned`
- `lead_assigned`
- `lead_unassigned`
- `lead_status_changed`
- `deposit_created`
- `manager_no_activity`
- `partner_duplicate_attempt`
- `next_contact_overdue`
- `comment_added`

## Payload Contract By Event Type

Only the keys below should be relied on from frontend.

### `lead_assigned`

Single:

```json
{
  "lead_id": "15",
  "from_manager_id": "7",
  "to_manager_id": "11"
}
```

Bulk summary:

```json
{
  "batch_id": "optional-batch-id",
  "mode": "bulk_summary",
  "lead_count": 23,
  "to_manager_id": "11",
  "from_manager_count": 2,
  "status_counts": {
    "NEW": 10,
    "IN_WORK": 13
  },
  "status_breakdown": [
    {
      "status_id": "1",
      "status_code": "NEW",
      "status_name": "New",
      "count": 10
    }
  ]
}
```

### `lead_unassigned`

Single:

```json
{
  "lead_id": "15",
  "from_manager_id": "7"
}
```

Bulk summary:

```json
{
  "batch_id": "optional-batch-id",
  "mode": "bulk_summary",
  "lead_count": 12,
  "from_manager_id": "7",
  "from_manager_ids": ["7", "8"],
  "from_manager_count": 2,
  "status_counts": {
    "NEW": 12
  },
  "status_breakdown": []
}
```

### `lead_status_changed`

Single:

```json
{
  "lead_id": "15",
  "from_status": {
    "id": "3",
    "code": "IN_WORK",
    "is_valid": true
  },
  "to_status": {
    "id": "9",
    "code": "LOST",
    "is_valid": false,
    "work_bucket": "non_working",
    "conversion_bucket": "lost"
  }
}
```

Bulk summary:

```json
{
  "batch_id": "optional-batch-id",
  "mode": "bulk_summary",
  "lead_count": 18,
  "to_status_id": "9",
  "to_status_ids": ["9"],
  "status_counts": {
    "LOST": 18
  },
  "status_breakdown": []
}
```

### `deposit_created`

```json
{
  "lead_id": "15",
  "deposit_id": "31",
  "deposit_type": 1,
  "amount": "150.00"
}
```

### `comment_added`

```json
{
  "lead_id": "15",
  "comment_id": "44"
}
```

### `next_contact_planned`

```json
{
  "notification_kind": "planned_reminder",
  "lead_id": "15",
  "next_contact_at": "2026-03-11T18:00:00+01:00",
  "remind_before_minutes": 15
}
```

### `next_contact_overdue`

```json
{
  "notification_kind": "overdue",
  "lead_id": "15",
  "next_contact_at": "2026-03-11T12:00:00+01:00",
  "slot": 2,
  "repeat_minutes": 15
}
```

### `manager_no_activity`

```json
{
  "manager_id": "11",
  "overdue_count": 7,
  "threshold": 5,
  "slot": 123456
}
```

### `partner_duplicate_attempt`

```json
{
  "attempt_id": "88",
  "partner_id": "4",
  "source": "api",
  "existing_lead_id": "15",
  "phone": "+79990001122",
  "attempts_count": 14,
  "threshold": 10,
  "window_minutes": 60,
  "threshold_block": 1
}
```

## Inbox Endpoints

### `GET /api/v1/notifications/`

Paginated DRF list.

Query params:

- `page`
- `page_size`
- `event_type`
- `status`
- `is_read`
- `lead`
- `search`
- `ordering`

Ordering fields:

- `id`
- `created_at`
- `read_at`
- `scheduled_for`
- `sent_at`
- `event_type`
- `is_read`
- `lead__id`

Search fields:

- `title`
- `body`
- `event_type`
- `lead__full_name`
- `lead__phone`

Response shape:

```json
{
  "count": 120,
  "next": "https://.../api/v1/notifications/?page=2",
  "previous": null,
  "results": []
}
```

### `GET /api/v1/notifications/unread-count/`

```json
{
  "unread_count": 5
}
```

### `POST /api/v1/notifications/{id}/mark-read/`

Marks one notification as read.

Request body:

```json
{}
```

Response:

- full `Notification` object

### `POST /api/v1/notifications/mark-all-read/`

Marks all current user unread inbox items as read.

Response:

```json
{
  "updated_count": 12
}
```

## SSE Stream

### `GET /api/v1/notifications/stream/`

Content type:

- `text/event-stream`

Query params:

- `last_id`: last processed notification id, default `0`
- `poll_interval`: 1..30 seconds, default `3`
- `once`: `1|true|yes` for one-shot response instead of long polling loop

Events:

### `unread_count`

```text
event: unread_count
data: {"unread_count":5}
```

### `notifications`

Sends only notifications with `id > last_id`.

```text
id: 123
event: notifications
data: {"items":[...Notification[]...]}
```

### heartbeat

```text
: ping
```

### Recommended frontend behavior

1. On page load call `GET /notifications/` and `GET /notifications/unread-count/`.
2. Open SSE with `last_id=<max_seen_notification_id>`.
3. On `notifications` append/merge by `id`.
4. On `unread_count` trust backend count and replace local badge value.
5. On reconnect reuse latest received SSE `id`.
6. Do not try to reconstruct unread count from local list only.

## Settings Endpoint

### `GET /api/v1/notifications/settings/me/`

Returns effective settings for current user and only for event types allowed for that role.

Response:

```json
{
  "items": [
    {
      "event_type": "comment_added",
      "policy": {
        "id": 1,
        "event_type": "comment_added",
        "enabled_by_default": true,
        "default_repeat_minutes": 15,
        "default_watch_scope": "own",
        "apply_to_teamleaders": true,
        "apply_to_admins": true,
        "apply_to_superusers": true,
        "created_at": "2026-03-11T12:00:00+01:00",
        "updated_at": "2026-03-11T12:00:00+01:00"
      },
      "preference": {
        "id": 5,
        "user": 7,
        "event_type": "comment_added",
        "enabled": true,
        "repeat_minutes": 20,
        "watch_scope": "team",
        "updated_by": 7,
        "created_at": "2026-03-11T12:00:00+01:00",
        "updated_at": "2026-03-11T12:00:00+01:00"
      },
      "watch_targets": {
        "user_ids": [11, 12],
        "users": [
          {
            "id": "11",
            "username": "manager_1",
            "first_name": "Ivan",
            "last_name": "Petrov",
            "role": "MANAGER"
          }
        ],
        "roles": ["MANAGER"]
      },
      "resolved": {
        "enabled": true,
        "repeat_minutes": 20,
        "watch_scope": "team"
      }
    }
  ]
}
```

### `PATCH /api/v1/notifications/settings/me/`

Allowed only for:

- `TEAMLEADER`
- `ADMIN`
- `SUPERUSER`

Managers receive `403`.

Request body:

```json
{
  "event_type": "comment_added",
  "enabled": true,
  "repeat_minutes": 20,
  "watch_scope": "team",
  "watched_user_ids": [11, 12],
  "watched_roles": ["MANAGER"]
}
```

All fields except `event_type` are optional partial updates.

Response:

```json
{
  "preference": {
    "id": 5,
    "user": 7,
    "event_type": "comment_added",
    "enabled": true,
    "repeat_minutes": 20,
    "watch_scope": "team",
    "updated_by": 7,
    "created_at": "2026-03-11T12:00:00+01:00",
    "updated_at": "2026-03-11T12:05:00+01:00"
  },
  "watch_targets": {
    "user_ids": [11, 12],
    "users": [],
    "roles": ["MANAGER"]
  },
  "resolved": {
    "enabled": true,
    "repeat_minutes": 20,
    "watch_scope": "team"
  }
}
```

### Watch Scope Semantics

- `own`: only own leads / own direct notifications
- `team`: own coverage plus explicitly watched users and roles
- `all`: all events allowed by policy

### Role Values For `watched_roles`

- `SUPERUSER`
- `ADMIN`
- `TEAMLEADER`
- `RET`
- `MANAGER`

### Event Availability By Role

Managers currently can configure only:

- `next_contact_planned`
- `lead_assigned`
- `comment_added`
- `next_contact_overdue`
- `lead_unassigned`

Elevated roles (`TEAMLEADER`, `ADMIN`, `SUPERUSER`) receive the full current event set.

## Policy Endpoint

### `GET /api/v1/notification-policies/`

Raw backend policy CRUD endpoint exists.

For frontend product code it should be treated as secondary/internal:

- use `/notifications/settings/me/` for user settings UI
- do not build the main notification UX around policy CRUD

## Frontend Implementation Notes

- use `event_type` as the primary renderer switch
- treat `payload.mode === "bulk_summary"` as a separate card variant
- never depend on `payload` being identical across all events
- `actor` and `lead` may be `null`
- do not expect unread items to arrive in list if they are not yet `sent`
- after `mark-read` and `mark-all-read`, update local state immediately, but keep SSE `unread_count` as source of truth
- there is no websocket contract; realtime channel is SSE only

## Non-Contract Internals

Frontend should not use or depend on:

- `NotificationDelivery`
- `NotificationDeliveryAttempt`
- outbox ids
- dedupe keys
- backend transport statuses outside inbox `status`
