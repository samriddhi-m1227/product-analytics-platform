
# Event Schema Contract (v1)

This document defines the contract for product analytics events ingested by the
Product Analytics Data Platform.

All event producers (synthetic generator now; real app/stream later) must emit
events that conform to this schema.

---

## Event Format (JSON)

Each event is a single JSON object (one per line in a `.jsonl` file).

### Required Fields (all events)

| Field          | Type    | Description |
|----------------|---------|-------------|
| event_id       | string  | Unique identifier for the event (UUID). Must be globally unique. |
| event_name     | string  | Event type. Must be one of the allowed values below. |
| event_time     | string  | ISO 8601 UTC timestamp (e.g., `2026-01-06T14:29:45Z`). |
| user_id        | integer | Unique user identifier. |
| session_id     | string  | Session identifier used for sessionization. |
| platform       | string  | Platform where event occurred: `web`, `ios`, `android`. |
| schema_version | integer | Schema version for this event envelope (start at `1`). |
| properties     | object  | Event-specific attributes (flexible map). |

---

## Allowed Values

### event_name (allowed values must be from this list)
- signup
- login
- feature_use
- purchase
- logout

### platform (allowed)
- web
- ios
- android

---

## `properties` (flexible event attributes)

`properties` is intentionally flexible. Keys may vary by `event_name`.
Only include fields relevant to that event. Unknown keys are permitted.

### Example event_name: signup
```json
{
  "event_id": "b1c9e2c2-4f9c-4c63-9c5b-6c4d7f2e1a91",
  "event_name": "signup",
  "event_time": "2026-01-06T14:23:11Z",
  "user_id": 101,
  "session_id": "sess-101-1",
  "platform": "web",
  "schema_version": 1,
  "properties": {
    "method": "email"
  }
}
