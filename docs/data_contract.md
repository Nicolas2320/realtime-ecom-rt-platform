# Data Contract

This document defines the **raw event contract** produced by `services/generator` and consumed by the Bronze writer from the Kafka topic:

- **Topic**: `ecom.events.raw.v1`
- **Encoding**: UTF-8 JSON
- **Event time**: `event_time` (business time)
- **Ingest time**: `ingest_time` (time the producer emitted the event)

The Silver layer enforces the validation rules in [`dq_rules.md`](dq_rules.md).

---

## Schema (Event v1)

All fields exist in the JSON payload. Some are `null` depending on `event_type`.

| Field | Type | Required | Notes |
|---|---:|:---:|---|
| `event_id` | string (UUID) | Yes | Idempotency key |
| `event_version` | int | Yes | Currently `1` |
| `event_type` | string | Yes | One of: `click`, `add_to_cart`, `checkout`, `purchase` |
| `event_time` | string (ISO-8601) | Yes | business time |
| `ingest_time` | string (ISO-8601) | Yes | time the producer emitted the event |
| `user_id` | string | Yes | e.g. `user_0001` |
| `session_id` | string (UUID) | Yes | Session identifier |
| `product_id` | string | No | Required for `click`, `add_to_cart` and `purchase` |
| `quantity` | int | No | Required for `add_to_cart` and `purchase`; must be `>= 1` |
| `price` | number | No | Required for `add_to_cart` and `purchase`; must be `>= 0` |
| `currency` | string | No | Required for `add_to_cart` and `purchase` (e.g. `EUR`) |
| `order_id` | string | No | Required for `purchase` (e.g. `ord_<id>`) |
| `source` | string | Yes | Producer identifier (e.g. `generator`) |

### Timestamp format
- Expected: ISO-8601 in UTC with `Z` suffix (the pipeline parses `yyyy-MM-dd'T'HH:mm:ss.SSSX`).

---

## Valid examples

### 1) Click
```json
{
  "event_id": "8d51b9e7-1f1b-4d20-b983-6e8f2f318d2a",
  "event_version": 1,
  "event_type": "click",
  "event_time": "2026-01-24T13:05:12.123Z",
  "ingest_time": "2026-01-24T13:05:12.456Z",
  "user_id": "user_0042",
  "session_id": "9eae3b0f-4c2a-4c2d-8c8d-0b3f0b5f7af7",
  "product_id": "sku_00123",
  "quantity": null,
  "price": null,
  "currency": null,
  "order_id": null,
  "source": "generator"
}
```

### 2) Add to cart
```json
{
  "event_id": "b2dc08f2-78f1-4b68-9d8e-20a4c4d1a930",
  "event_version": 1,
  "event_type": "add_to_cart",
  "event_time": "2026-01-24T13:06:01.005Z",
  "ingest_time": "2026-01-24T13:06:01.120Z",
  "user_id": "user_0007",
  "session_id": "bd0c4c55-3d2a-4d58-b5fa-bd9bd6f2f4c1",
  "product_id": "sku_00456",
  "quantity": 2,
  "price": 39.99,
  "currency": "EUR",
  "order_id": null,
  "source": "generator"
}
```

### 3) Checkout
```json
{
  "event_id": "a6c4f5c7-51d1-4d3e-b9e7-5f8f1d2b6d0e",
  "event_version": 1,
  "event_type": "checkout",
  "event_time": "2026-01-24T13:06:45.000Z",
  "ingest_time": "2026-01-24T13:06:45.100Z",
  "user_id": "user_0007",
  "session_id": "bd0c4c55-3d2a-4d58-b5fa-bd9bd6f2f4c1",
  "product_id": null,
  "quantity": null,
  "price": null,
  "currency": null,
  "order_id": null,
  "source": "generator"
}
```

### 4) Purchase
```json
{
  "event_id": "5f5f7c69-8d47-4b2f-9a1b-6dbf2a1aa321",
  "event_version": 1,
  "event_type": "purchase",
  "event_time": "2026-01-24T13:07:10.777Z",
  "ingest_time": "2026-01-24T13:07:10.900Z",
  "user_id": "user_0007",
  "session_id": "bd0c4c55-3d2a-4d58-b5fa-bd9bd6f2f4c1",
  "product_id": "sku_00456",
  "quantity": 1,
  "price": 39.99,
  "currency": "EUR",
  "order_id": "ord_2f9c7a1b0c",
  "source": "generator"
}
```

---

## Invalid examples (should end up in Quarantine)

### A) Invalid `event_type`
```json
{
  "event_id": "11111111-1111-1111-1111-111111111111",
  "event_version": 1,
  "event_type": "invalid_event_type",
  "event_time": "2026-01-24T13:05:12.123Z",
  "ingest_time": "2026-01-24T13:05:12.456Z",
  "user_id": "user_0001",
  "session_id": "22222222-2222-2222-2222-222222222222",
  "product_id": "sku_00001",
  "quantity": null,
  "price": null,
  "currency": null,
  "order_id": null,
  "source": "generator"
}
```

### B) Missing `product_id` for click/add_to_cart
```json
{
  "event_id": "33333333-3333-3333-3333-333333333333",
  "event_version": 1,
  "event_type": "click",
  "event_time": "2026-01-24T13:05:12.123Z",
  "ingest_time": "2026-01-24T13:05:12.456Z",
  "user_id": "user_0001",
  "session_id": "44444444-4444-4444-4444-444444444444",
  "product_id": null,
  "quantity": null,
  "price": null,
  "currency": null,
  "order_id": null,
  "source": "generator"
}
```

### C) Negative `quantity` / negative `price`
```json
{
  "event_id": "55555555-5555-5555-5555-555555555555",
  "event_version": 1,
  "event_type": "add_to_cart",
  "event_time": "2026-01-24T13:06:01.005Z",
  "ingest_time": "2026-01-24T13:06:01.120Z",
  "user_id": "user_0007",
  "session_id": "66666666-6666-6666-6666-666666666666",
  "product_id": "sku_00456",
  "quantity": -2,
  "price": -39.99,
  "currency": "EUR",
  "order_id": null,
  "source": "generator"
}
```

### D) Purchase missing `order_id`
```json
{
  "event_id": "77777777-7777-7777-7777-777777777777",
  "event_version": 1,
  "event_type": "purchase",
  "event_time": "2026-01-24T13:07:10.777Z",
  "ingest_time": "2026-01-24T13:07:10.900Z",
  "user_id": "user_0007",
  "session_id": "88888888-8888-8888-8888-888888888888",
  "product_id": "sku_00456",
  "quantity": 1,
  "price": 39.99,
  "currency": "EUR",
  "order_id": null,
  "source": "generator"
}
```
