# Partner Leads API

Внешняя документация для партнёра.

Токен партнёру выдаёт CRM вручную. В этой документации описано только использование уже выданного токена и работа с лидами.

## 1. Аутентификация

Передавай токен в одном из заголовков:

- `X-Partner-Token: <TOKEN>` — рекомендуемый вариант
- `Authorization: Bearer <TOKEN>`

Если токен невалиден, отключён или просрочен, API вернёт `401 Unauthorized`.

## 2. Доступные endpoint'ы

- `POST /api/v1/partner/leads/` — отправить нового лида
- `GET /api/v1/partner/leads/` — получить список своих лидов
- `GET /api/v1/partner/leads/{id}/` — получить одного своего лида



## 3. Формат лида в ответе

Поля, которые API возвращает для созданного/полученного лида:

```json
{
  "id": "123",
  "source": "google",
  "received_at": "2026-03-12T10:30:00Z",
  "geo": "US",
  "age": 27,
  "status": {
    "id": "1",
    "code": "NEW",
    "name": "Новый",
    "work_bucket": "WORKING"
  },
  "full_name": "John Doe",
  "phone": "+15550000001",
  "email": "john@example.com",
  "custom_fields": {
    "campaign": "spring_sale"
  }
}
```

Примечания:

- `priority` в ответе не возвращается
- `status` может быть `null`, если у лида нет статуса
- `custom_fields` может быть `null`

## 4. Создание лида

`POST /api/v1/partner/leads/`

### Поля запроса

- `phone` — string, обязательно
- `source` — string, опционально
- `geo` — string, опционально, 2 заглавные буквы (`US`, `DE`, `CH`)
- `age` — integer, опционально
- `full_name` — string, опционально
- `email` — string, опционально
- `priority` — integer, опционально
- `custom_fields` — object или `null`, опционально

Правила:

- если токен привязан к конкретному `source`, значение `source` из запроса игнорируется
- дубликат определяется по `phone`
- при дубликате новый лид не создаётся
- в CRM попытка дубликата сохраняется как отдельный факт
- наружу при дубликате возвращаются только данные из текущей попытки партнёра, без данных уже существующего лида

### Пример запроса

```bash
curl -X POST http://localhost:8000/api/v1/partner/leads/ \
  -H "Content-Type: application/json" \
  -H "X-Partner-Token: <TOKEN>" \
  -d '{
    "source": "google",
    "geo": "US",
    "age": 27,
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

### Успешный ответ: новый лид

HTTP `201 Created`

```json
{
  "id": "123",
  "source": "google",
  "received_at": "2026-03-12T10:30:00Z",
  "geo": "US",
  "age": 27,
  "status": {
    "id": "1",
    "code": "NEW",
    "name": "Новый",
    "work_bucket": "WORKING"
  },
  "full_name": "John Doe",
  "phone": "+15550000001",
  "email": "john@example.com",
  "custom_fields": {
    "campaign": "spring_sale",
    "landing": "lp-01"
  },
  "created": true,
  "duplicate_rejected": false
}
```

### Ответ: дубликат

HTTP `409 Conflict`

```json
{
  "source": "google",
  "geo": "US",
  "age": 27,
  "full_name": "John Doe",
  "phone": "+15550000001",
  "email": "john@example.com",
  "custom_fields": {
    "campaign": "spring_sale",
    "landing": "lp-01"
  },
  "created": false,
  "duplicate_rejected": true
}
```

Важно:

- в duplicate-ответе нет `id`
- в duplicate-ответе нет `status`
- в duplicate-ответе нет `received_at`
- в duplicate-ответе нет данных уже существующего лида

## 5. Список лидов

`GET /api/v1/partner/leads/`

Возвращает пагинированный список лидов текущего партнёра.

### Поддерживаемые фильтры

- `source=<value>`
- `phone=<exact_phone>`
- `age=<number>`
- `age_from=<number>`
- `age_to=<number>`
- `status__in=<STATUS_CODE_1,STATUS_CODE_2,...>`
- `received_from=<ISO_DATETIME>`
- `received_to=<ISO_DATETIME>`
- `page=<number>`
- `page_size=<number>`
- `ordering=<field>`

### Формат даты для `received_from` и `received_to`

Используй ISO 8601 datetime.

Подходящие примеры:

- `2026-03-12T10:30:00Z`
- `2026-03-12T10:30:00+00:00`
- `2026-03-12T13:30:00+03:00`

Рекомендуется всегда передавать timezone явно: либо `Z`, либо смещение вида `+03:00`.

Важно для query string:

- знак `+` в URL должен быть экранирован как `%2B`
- иначе многие клиенты превратят `+` в пробел, и API не сможет распарсить дату

Пример в URL:

- `received_from=2026-03-12T13:30:00%2B03:00`

### Поддерживаемая сортировка

- `id`
- `received_at`
- `age`
- `phone`
- `full_name`
- `email`
- `priority`
- `source`
- `status__code`

Для обратной сортировки используй префикс `-`, например `ordering=-received_at`.

### Пример запроса

```bash
curl "http://localhost:8000/api/v1/partner/leads/?source=google&age_from=20&age_to=35&status__in=NEW,WON&page=1&page_size=50&ordering=-received_at" \
  -H "X-Partner-Token: <TOKEN>"
```

### Пример ответа

```json
{
  "count": 2,
  "next": null,
  "previous": null,
  "results": [
    {
      "id": "124",
      "source": "google",
      "received_at": "2026-03-12T10:30:00Z",
      "geo": "US",
      "age": 27,
      "status": {
        "id": "1",
        "code": "NEW",
        "name": "Новый",
        "work_bucket": "WORKING"
      },
      "full_name": "John Doe",
      "phone": "+15550000001",
      "email": "john@example.com",
      "custom_fields": {
        "campaign": "spring_sale"
      }
    }
  ]
}
```

## 6. Один лид

`GET /api/v1/partner/leads/{id}/`

Возвращает одного лида текущего партнёра в том же формате, что и элементы в списке.

### Пример

```bash
curl http://localhost:8000/api/v1/partner/leads/124/ \
  -H "X-Partner-Token: <TOKEN>"
```

## 7. Ошибки

Типовые ответы:

- `400 Bad Request` — ошибка валидации, например пустой `phone` или неверный `geo`
- `401 Unauthorized` — токен невалиден, отключён или просрочен
- `404 Not Found` — лид с таким `id` не найден в рамках текущего партнёра
- `409 Conflict` — duplicate reject по `phone`
- `429 Too Many Requests` — превышен лимит запросов
