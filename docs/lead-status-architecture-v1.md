# Lead Statuses v1 (Single-Instance / Single-Company)

## Контекст инстанса

- Один инстанс CRM = одна компания.
- Полноценный `tenant_id` в схеме сейчас не нужен.
- Логика остается расширяемой: при необходимости мульти-компаний позже можно добавить `org_id/tenant_id` миграциями.

## Цели

- Гибкая модель статусов под бизнес-процесс конкретной компании.
- Управление статусами как сущностью: create/update/soft-delete = `ADMIN|SUPERUSER`, hard-delete = `SUPERUSER`.
- Полный audit log изменений по лидам и по справочнику статусов.
- Настраиваемая конверсия (какие статусы считать в воронке, какие нет).

## Базовые сущности

1) `Pipeline`
- name
- is_default
- is_active

2) `LeadStatus`
- pipeline_id
- code (stable key, уникален в pipeline)
- name
- order (для UI)
- color
- is_default_for_new_leads
- is_active
- is_terminal
- counts_for_conversion (bool)
- deleted_at (soft delete)

3) `LeadStatusTransition`
- pipeline_id
- from_status_id
- to_status_id
- is_active
- requires_comment (опционально)
- unique(from_status_id, to_status_id)

4) `LeadStatusAuditLog`
- lead_id
- event_type (`status_changed`, `status_created`, `status_updated`, `status_deleted_soft`, `status_deleted_hard`)
- from_status_id (nullable)
- to_status_id (nullable)
- actor_user_id (nullable для системных действий)
- source (`api`, `admin`, `system`, `import`)
- reason/comment (nullable)
- payload_before (json, nullable)
- payload_after (json, nullable)
- created_at

## Правила доступа (согласованные)

- `SUPERUSER`:
  - полный доступ к статусам (включая hard delete)
- `ADMIN`:
  - create/update/soft-delete статусов
  - без hard delete статусов
- `TEAMLEADER`, `MANAGER`, `RET`:
  - read-only для справочника статусов (если не будет отдельного исключения бизнес-правилом)

- Изменение статуса лида как отдельное право:
  - предлагается вынести в отдельный permission (`leads.status.write`) и утвердить отдельно перед реализацией.

- Hard delete для любых сущностей:
  - только `SUPERUSER`.

## Валидация переходов

- Новый лид получает статус, где `is_default_for_new_leads=true`.
- Переход допустим только если есть активная запись в `LeadStatusTransition`.
- Для терминальных статусов (`is_terminal=true`) выход запрещен, если явно не настроен переход (например reopen).
- Если transition требует комментарий, `reason/comment` обязателен.

## Audit log (обязательно)

- Логируем каждое изменение статуса лида и каждое изменение в справочнике статусов.
- Для события фиксируем:
  - кто сделал (`actor_user_id`),
  - откуда (`source`),
  - что изменилось (`payload_before/payload_after`),
  - когда (`created_at`),
  - контекст (`lead_id`, `from_status_id`, `to_status_id`, `reason/comment`).
- Аудит не редактируется через UI/API (append-only модель).

## Визуализация для команды

- Диаграмма переходов: `docs/lead-status-workflow-v1.mmd`
- Можно открыть в Mermaid Live Editor или в IDE-плагине Mermaid.

## Пример конфига (для импорта/инициализации)

- См. `docs/lead-status-config-v1.example.yaml`

## Что масштабируется без смены подхода

- Несколько pipeline внутри компании (например B2B/B2C).
- Кастомные статусы и переходы без релиза кода.
- Гибкая метрика конверсии через `counts_for_conversion`.

## Путь к мульти-tenant (если понадобится)

- Добавить `org_id/tenant_id` в: `Pipeline`, `LeadStatus`, `LeadStatusTransition`, `LeadStatusAuditLog`, `Lead`.
- Уникальности и фильтрацию ограничить по `org_id`.
- На уровне API включить org-scope в permissions/querysets.
