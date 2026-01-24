# Data Quality Rules (Silver Stage)

Silver is the **first enforced contract** layer.
It reads typed Bronze parquet from:

- `s3a://lake/bronze/ecom_events/v1/`

and writes **two outputs**:

- **Silver** (valid rows): `s3a://lake/silver/ecom_events/v1/`
- **Quarantine** (invalid rows + reasons): `s3a://lake/quarantine/ecom_events/v1/`

Silver runs **two streaming sinks** from the same input stream:
- `q_silver` (valid)
- `q_quarantine` (invalid)

Each sink has its **own checkpoint**:
- Silver: `SILVER_CHECKPOINT`
- Quarantine: `QUARANTINE_CHECKPOINT`

---

## Timestamp parsing

Silver derives:
- `event_time_ts` from `event_time`
- `ingest_time_ts` from `ingest_time`

Parsing tries the generator’s format first (`yyyy-MM-dd'T'HH:mm:ss.SSSX`) and then falls back to Spark’s default.

If `event_time_ts` or `ingest_time_ts` cannot be parsed, the record is **invalid**.

---

## Validation rules

A record is **valid** if  **all** rules pass.

### Structural required fields

| Rule | Condition | Error code |
|---|---|---|
| `event_id` required | `event_id IS NOT NULL` | `missing_event_id` |
| `event_type` allowed | `event_type IN ('click','add_to_cart','checkout','purchase')` | `invalid_event_type` |
| `user_id` required | `user_id IS NOT NULL` | `missing_user_id` |
| `session_id` required | `session_id IS NOT NULL` | `missing_session_id` |
| `ingest_time` parsable | `ingest_time_ts IS NOT NULL` | `bad_ingest_time` |
| `event_time` parsable | `event_time_ts IS NOT NULL` | `bad_event_time` |

### Conditional rules (by event_type)

| Rule | Applies when | Condition | Error code |
|---|---|---|---|
| `product_id` required | `click` or `add_to_cart` | `product_id IS NOT NULL` | `product_id_required_for_click_or_cart` |
| cart fields required | `add_to_cart` | `quantity IS NOT NULL AND quantity >= 1 AND price IS NOT NULL AND price >= 0 AND currency IS NOT NULL` | `add_to_cart_requires_qty_price_currency` |
| purchase fields required | `purchase` | `order_id IS NOT NULL AND quantity IS NOT NULL AND quantity >= 1 AND price IS NOT NULL AND price >= 0 AND currency IS NOT NULL` | `purchase_requires_order_qty_price_currency` |

---

## Quarantine behavior

Invalid rows are written to **Quarantine parquet** with:

- `raw_json` (original payload)
- `validation_errors` (comma-separated error codes)
- Kafka lineage fields (`kafka_topic`, `kafka_partition`, `kafka_offset`, `kafka_key`)
- Parsed timestamps (`event_time_ts`, `ingest_time_ts`)
- `processing_time` (Spark processing timestamp)

---

## Silver output behavior

Valid rows are:

1) Enriched with event-time partitions

2) Watermarked and deduplicated:
- Watermark: `withWatermark('event_time_ts', EVENT_WATERMARK)`
- Dedupe key: `dropDuplicates(['event_id'])`

3) Written to Silver parquet in append mode.

Partitioning:
- `event_date`, `event_hour`

Path:
- `s3a://lake/silver/ecom_events/v1/event_date=YYYY-MM-DD/event_hour=H/`

### Late data semantics

With watermarking enabled, Spark will keep state only up to the watermark. Very late events (arriving later than `EVENT_WATERMARK`) may be dropped from the dedupe/stateful operators.