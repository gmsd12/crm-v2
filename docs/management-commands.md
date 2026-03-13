# Management Commands

Ниже собраны основные management-команды проекта, которые реально используются в dev-работе.

Запускать из корня backend-проекта:

```bash
./.venv/bin/python manage.py <command>
```


## База и базовые операции

- Очистить текущую рабочую БД, не трогая миграции:

```bash
./.venv/bin/python manage.py flush --noinput
```

- Проверить проект на ошибки конфигурации:

```bash
./.venv/bin/python manage.py check
```


## Пользователи

### `bootstrap_superuser`

Создает или обновляет bootstrap-суперпользователя:

- `username`: `chtnr`
- `password`: `QwKl79$3H`

```bash
./.venv/bin/python manage.py bootstrap_superuser
```

Что делает:
- ставит `role=SUPERUSER`
- включает `is_active`
- включает `is_staff`
- включает `is_superuser`
- обновляет пароль при повторном запуске


## Лиды и статусы

### `sync_lead_statuses`

Создает или обновляет преднастроенный набор статусов лидов.

```bash
./.venv/bin/python manage.py sync_lead_statuses
```

Проверочный запуск без записи:

```bash
./.venv/bin/python manage.py sync_lead_statuses --dry-run
```

Использовать, когда нужно быстро накатить базовый справочник статусов на пустую БД.


### `seed_demo_crm`

Наполняет базу читаемыми demo-данными.

Пример:

```bash
./.venv/bin/python manage.py seed_demo_crm --leads 40 --comments 80
```

Полезные флаги:
- `--partner-code`
- `--partner-name`
- `--leads`
- `--comments`
- `--password`
- `--without-users`


### `import_legacy_data`

Импортирует данные из legacy CRM в текущую схему.

Требует настроенный `LEGACY_DATABASE_URL` в `.env`.

Базовый запуск:

```bash
./.venv/bin/python manage.py import_legacy_data
```

Сухой прогон:

```bash
./.venv/bin/python manage.py import_legacy_data --dry-run
```

Тестовый запуск только на первых 100 legacy-лидах:

```bash
./.venv/bin/python manage.py import_legacy_data --sample-leads 100
```

Только отдельные этапы:

```bash
./.venv/bin/python manage.py import_legacy_data --only users partners statuses leads
```

Полезные флаги:
- `--dry-run` — не пишет в текущую БД
- `--sample-leads N` — берет только первые `N` legacy-лидов и только связанные с ними comments/deposits/attachments
- `--batch-size`
- `--progress-every`
- `--only users partners statuses leads comments deposits attachments`
- `--user-table`
- `--database-table`
- `--status-table`
- `--lead-table`
- `--comment-table`
- `--deposit-table`
- `--record-table`
- `--no-orphan-deposit-lead` — пропускать orphan deposits без `lead_id`

Что важно знать:
- `Database` из legacy мапится в новый `Partner`
- `Status.tid` мапится в `LeadStatus.code`
- дубли по телефону не теряются: первый лид живой, остальные импортируются как soft-deleted
- orphan deposits можно складывать в технического лида
- skip/warning отчеты пишутся в `legacy/reports/`


## Партнеры

### `create_partner_token`

Создает токен партнера и один раз печатает raw token.

Пример:

```bash
./.venv/bin/python manage.py create_partner_token --partner demo-partner --name main
```

Полезные флаги:
- `--partner`
- `--name`
- `--source`


### `simulate_partner_uploads`

Симулирует залив лидов через Partner API с реальной токен-аутентификацией.

Пример:

```bash
./.venv/bin/python manage.py simulate_partner_uploads --partners 5 --leads-per-partner 5
```

Полезные флаги:
- `--partners`
- `--leads-per-partner`
- `--base-code`


## Уведомления

### `process_due_notifications`

Обрабатывает pending in-app уведомления.

```bash
./.venv/bin/python manage.py process_due_notifications
```

С лимитом:

```bash
./.venv/bin/python manage.py process_due_notifications --limit 500
```


### `inspect_pending_notifications`

Проверяет уведомления, которые застряли в pending.

```bash
./.venv/bin/python manage.py inspect_pending_notifications
```


### `emit_overdue_notifications`

Генерирует уведомления по просроченным `next_contact_at`.

```bash
./.venv/bin/python manage.py emit_overdue_notifications
```


### `emit_manager_no_activity_notifications`

Генерирует уведомления по менеджерам с большим количеством просроченных лидов.

```bash
./.venv/bin/python manage.py emit_manager_no_activity_notifications
```


## Частые dev-сценарии

### Полностью пересоздать тестовый контур и прогнать маленький импорт

```bash
./.venv/bin/python manage.py flush --noinput
./.venv/bin/python manage.py sync_lead_statuses
./.venv/bin/python manage.py bootstrap_superuser
./.venv/bin/python manage.py import_legacy_data --sample-leads 100
```


### Проверить legacy-импорт без записи

```bash
./.venv/bin/python manage.py import_legacy_data --dry-run --sample-leads 100
```


### Быстро накатить статусы и суперюзера на пустую БД

```bash
./.venv/bin/python manage.py sync_lead_statuses
./.venv/bin/python manage.py bootstrap_superuser
```
