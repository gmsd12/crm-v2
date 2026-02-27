# Partner Leads API (v1)

Ниже минимальная инструкция для партнера: как получить токен, отправить лид, проверить что лид принят.

## 1) Аутентификация

Передавай токен в одном из заголовков:

- `X-Partner-Token: <TOKEN>` (рекомендуется)
- или `Authorization: Bearer <TOKEN>`

Если токен неверный/просрочен/отозван — `401`.

## 2) Доступные endpoint'ы

- `GET /api/v1/partner/sources/` — список доступных `source_code` для токена.
- `POST /api/v1/partner/leads/` — заливка лида.
- `GET /api/v1/partner/leads/` — список лидов партнера (с пагинацией).
- `GET /api/v1/partner/leads/{id}/` — один лид партнера.

## 3) Создание лида

`POST /api/v1/partner/leads/`

### Поля запроса

- `phone` (string, обязательно) — уникальный телефон.
- `source_code` (string, опционально) — код источника.
- `geo` (string, опционально) — 2 буквы в upper-case (`US`, `DE`, `CH`).
- `full_name` (string, опционально)
- `email` (string, опционально)
- `priority` (int, опционально; по умолчанию 20)
- `custom_fields` (object, опционально)

Важно:
- если токен привязан к source, `source_code` из запроса игнорируется;
- дубликат определяется по `phone`;
- дубликат не создается как новый лид, а возвращается с флагом `duplicate_rejected=true`.

### Пример запроса

```bash
curl -X POST http://localhost:8000/api/v1/partner/leads/ \
  -H "Content-Type: application/json" \
  -H "X-Partner-Token: <TOKEN>" \
  -d '{
    "source_code": "google",
    "geo": "US",
    "full_name": "John Doe",
    "phone": "+15550000001",
    "email": "john@example.com",
    "priority": 20,
    "custom_fields": {
      "campaign": "spring_sale",
      "landing": "lp-01"
    }
  }'
```

### Успешный ответ (новый лид)

- HTTP `201`
- в теле:
  - `created: true`
  - `duplicate_rejected: false`

### Успешный ответ (дубликат)

- HTTP `200`
- в теле:
  - `created: false`
  - `duplicate_rejected: true`

## 4) Фильтры списка лидов

`GET /api/v1/partner/leads/`

Поддерживаются:

- `source=<source_code>`
- `phone=<exact_phone>`
- `received_from=<ISO_DATETIME>`
- `received_to=<ISO_DATETIME>`
- `page=<n>`
- `page_size=<n>` (ограничен настройкой сервера, по умолчанию max 200)

Пример:

```bash
curl "http://localhost:8000/api/v1/partner/leads/?source=google&page=1&page_size=50" \
  -H "X-Partner-Token: <TOKEN>"
```

## 5) Ошибки

Стандартные кейсы:

- `400` — ошибка валидации (`phone` пустой, неверный `geo`, неизвестный `source_code`).
- `401` — токен невалиден/неактивен/просрочен.
- `429` — превышен лимит запросов токена.

## 6) Быстрый локальный демо-сценарий (для команды CRM)

Команда ниже создаст 5 партнеров, токены и загрузит по 5 лидов от каждого через реальный Partner API:

```bash
.venv/bin/python manage.py simulate_partner_uploads --partners 5 --leads-per-partner 5
```

После выполнения команда выведет:

- список партнеров,
- токен каждого партнера,
- сколько лидов создано,
- сколько ушло в duplicate reject.
