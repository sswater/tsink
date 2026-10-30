# HTTP API reference

This document covers every HTTP endpoint exposed by the tsink server, including request formats, query parameters, request/response headers, and error codes.

---

## Base URL

All paths are relative to the server listen address (e.g. `http://127.0.0.1:9201`).

---

## Authentication

### Scopes

| Scope | Paths | Notes |
|---|---|---|
| **Probe** | `/healthz`, `/ready` | Always unauthenticated. |
| **Internal** | `/internal/v1/*` | mTLS peer-to-peer traffic; validated separately. |
| **Public** | All other non-admin paths | Bearer token or RBAC. |
| **Admin** | `/api/v1/admin/*` | Requires admin token or elevated RBAC role; must be enabled with `--admin-api-enabled`. |

### Bearer token

Pass credentials in the `Authorization` header:

```
Authorization: Bearer <token>
```

For public endpoints a static token can be configured via `--auth-token`. For admin endpoints a separate token can be set with `--admin-auth-token`; if only a public token is configured it is accepted for admin too.

### RBAC / OIDC

When RBAC is enabled the bearer token is validated as a service-account token or an OIDC JWT (RS256/HS256). The server attaches the resolved principal and role to the request internally; see the [Security model](security.md) for role definitions.

### Authentication error headers

All 401/403 responses include:

| Header | Values | Meaning |
|---|---|---|
| `WWW-Authenticate` | `Bearer` | Signals that a bearer token is expected (401 only). |
| `X-Tsink-Auth-Error-Code` | `auth_token_missing` | No token provided for a protected endpoint. |
| | `auth_token_invalid` | Token provided but did not match. |
| | `admin_auth_token_missing` | No token provided for an admin endpoint. |
| | `admin_auth_token_invalid` | Admin token provided but did not match. |
| | `auth_scope_denied` | Public token used on an admin-only endpoint (403). |

---

## Multi-tenancy

Pass the tenant identifier on every request using the request header:

```
x-tsink-tenant: <tenant-id>
```

If omitted, the request is attributed to the `default` tenant. Tenant IDs must be non-empty strings that do not start with `__`. See [Multi-tenancy](multi-tenancy.md) for quota and isolation details.

---

## Common response envelope

JSON responses from query and admin endpoints use one of two shapes:

**Success**

```json
{
  "status": "success",
  "data": <result>
}
```

**PromQL error**

```json
{
  "status": "error",
  "errorType": "<type>",
  "error": "<message>"
}
```

`errorType` is one of `bad_data` (request validation) or `execution` (query evaluation).

---

## Cluster response headers

When clustering is active, query responses include additional metadata headers:

| Header | Description |
|---|---|
| `X-Tsink-Read-Consistency` | Effective read consistency level used (`one`, `quorum`, `all`). |
| `X-Tsink-Read-Partial-Policy` | Active partial-response policy (`fail`, `warn`). |
| `X-Tsink-Read-Response` | `true` if the response is partial (some shards unavailable). |
| `X-Tsink-Read-Partial-Warnings` | Number of partial-response warning messages. |

The JSON body of cluster query responses also contains a `partialResponse` object:

```json
{
  "status": "success",
  "data": ...,
  "partialResponse": {
    "enabled": true,
    "policy": "warn",
    "consistency": "quorum",
    "warningCount": 1
  },
  "warnings": ["shard 3: node node-2 unreachable"]
}
```

Write responses set per-request consistency headers when clustering is active:

| Header | Description |
|---|---|
| `X-Tsink-Write-Consistency` | Effective write consistency mode used. |
| `X-Tsink-Write-Required-Acks` | Number of replica acknowledgements required. |
| `X-Tsink-Write-Acknowledged-Replicas` | Minimum replica acknowledgements actually received. |

To override the write consistency on a per-request basis, set:

```
x-tsink-write-consistency: one|quorum|all
```

To override the partial-response behaviour on reads, set:

```
x-tsink-read-partial-response: true|false
```

---

## Health & readiness probes

### `GET /healthz`

Liveness probe. Always returns `200 ok` as plain text.

### `GET /ready`

Readiness probe. Returns `200 ready` as plain text when the server is ready to serve traffic.

Both probes are unauthenticated and are suitable for Kubernetes `livenessProbe` / `readinessProbe` configuration.

---

## Self-instrumentation

### `GET /metrics`

Returns self-instrumentation counters and gauges in Prometheus text exposition format.

**Authentication:** public scope (bearer token if configured).

**Response:** `200 text/plain` — Prometheus text exposition.

Key exported metric families include `tsink_memory_*`, `tsink_series_total`, `tsink_uptime_seconds`, `tsink_wal_*`, `tsink_compaction_*`, `tsink_cluster_*`, `tsink_ingest_*`, `tsink_query_*`, and `tsink_exemplar_*`.

---

## PromQL query

### `GET|POST /api/v1/query`

Instant PromQL evaluation.

**Query parameters / POST form fields:**

| Parameter | Required | Description |
|---|---|---|
| `query` | Yes | PromQL expression string. |
| `time` | No | Evaluation timestamp. Numeric values are Unix seconds, matching the Prometheus HTTP API; RFC 3339 is also accepted. Defaults to current server time. |

**Response:** `200 application/json`

```json
{
  "status": "success",
  "data": {
    "resultType": "vector",
    "result": [
      {
        "metric": {"__name__": "cpu_usage", "host": "web-1"},
        "value": [1700000000.000, "42"]
      }
    ]
  }
}
```

---

### `GET|POST /api/v1/query_range`

Range PromQL evaluation.

**Query parameters / POST form fields:**

| Parameter | Required | Description |
|---|---|---|
| `query` | Yes | PromQL expression string. |
| `start` | Yes | Range start. Numeric values are Unix seconds, matching the Prometheus HTTP API; RFC 3339 is also accepted. |
| `end` | Yes | Range end. Must be ≥ `start`. |
| `step` | Yes | Step duration as seconds when numeric, or as a Go-style duration string (e.g. `15s`, `1m`, `1500ms`). |

**Response:** `200 application/json`

```json
{
  "status": "success",
  "data": {
    "resultType": "matrix",
    "result": [
      {
        "metric": {"__name__": "cpu_usage", "host": "web-1"},
        "values": [[1700000000.000, "42"], [1700000015.000, "43"]]
      }
    ]
  }
}
```

**Error codes:** `400` invalid parameters, `413` per-tenant range-points quota exceeded.

---

## Typed row API

The row endpoints exchange single points as type-tagged JSON instead of the Prometheus protobuf or text formats. They are the only public HTTP surface that carries `bytes` payloads end to end; PromQL and Remote Read still expose such samples as NaN.

All three endpoints share the following behaviour:

- **Uncompressed JSON only.** Request bodies are parsed as plain JSON: there is no `Content-Encoding` support (gzip, deflate and snappy bodies are not decoded), and the request `Content-Type` is not enforced.
- **Authentication and RBAC.** They use the same bearer-token / OIDC flow as the other public endpoints. `POST /api/v1/rows` requires the tenant **write** permission; `POST /api/v1/select` and `POST /api/v1/select_points` require the tenant **read** permission.
- **Multi-tenancy.** The target tenant is selected with `x-tsink-tenant` (default `default`) for both writes and reads.
- **Timestamps.** `i64` values in the units selected by `--timestamp-precision`; they are stored and returned verbatim, never rescaled.
- **Range semantics.** `[start, end)` — `end` is exclusive.
- **Cluster mode.** The endpoints are single-node only. When clustering is active all three answer `501 text/plain` with the body `row endpoints are not supported in cluster mode` instead of touching the local shard.
- **Histograms.** Native histogram values are not supported: writes and reads return `400`.

Successful responses use the standard success envelope. Query errors use the PromQL error envelope described in [Common response envelope](#common-response-envelope) — `errorType` is `bad_data` for validation failures and `execution` for backend failures. Write-endpoint validation failures are plain-text `400` responses.

### Limits

| Limit | Value |
|---|---|
| Raw size of a single value (on write; `bytes` length or `string` UTF-8 byte length) | 64 KiB (65 536 bytes) |
| Rows per `POST /api/v1/rows` request | 10 000 |
| Request body size for `POST /api/v1/rows` | 32 MiB |
| Timestamps per `POST /api/v1/select_points` request | 10 000 |
| `limit` for `POST /api/v1/select` | required, `1..=100 000` |

The 64 KiB value cap is enforced on write, and the 32 MiB body cap applies only to `POST /api/v1/rows`. The read endpoints have no handler-level body limit and are bounded only by the HTTP transport limit (64 MiB).

Base64 inflates `bytes` payloads by ~33%. The 64 KiB value cap follows from the storage shape — chunks seal at 2 048 points and have no byte dimension — so deployments that legitimately approach it should lower `with_chunk_points` (see [Data Types](data-types.md#http-transport)).

### Value encoding

Every point payload is a `{"type": <tag>, "value": <payload>}` object. The encoding is identical for writes and reads:

| `type` | `value` in JSON | Notes |
|---|---|---|
| `bytes` | base64 string (STANDARD alphabet, padded) | Opaque binary. The raw bytes `[0xCA, 0xFE]` are encoded as `"yv4="`. |
| `f64` | number | Non-finite values are exchanged as the strings `"NaN"`, `"Infinity"` and `"-Infinity"`. |
| `i64` | integer | Signed 64-bit. |
| `u64` | non-negative integer | Unsigned 64-bit. |
| `bool` | boolean | |
| `string` | string | UTF-8 text, stored losslessly. `\uXXXX` escapes are accepted, but responses always emit the raw UTF-8 characters. |

`type` and `value` may appear in either order. An unknown `type` tag, a missing `value`, or a payload of the wrong JSON kind is rejected with `400`.

A series is single-family: every point returned for one series has the same `type`. Different series in one write batch may use different types.

---

### `POST /api/v1/rows`

Typed batch write for up to 10 000 rows. `bytes` is the primary use case, but every non-histogram type is accepted.

**Request body:**

```json
{
  "rows": [
    {"metric": "device_data", "labels": [["device_id", "A"]],
     "timestamp": 1730000000000,
     "value": {"type": "bytes", "value": "yv4="}},
    {"metric": "temperature", "labels": [["device_id", "A"]],
     "timestamp": 1730000000000,
     "value": {"type": "f64", "value": 21.5}}
  ]
}
```

| Field | Required | Description |
|---|---|---|
| `rows` | Yes | Array of row objects. |
| `rows[].metric` | Yes | Non-empty metric name. |
| `rows[].labels` | No | Array of `[name, value]` string pairs; defaults to no labels. |
| `rows[].timestamp` | Yes | `i64` timestamp in the configured precision. |
| `rows[].value` | Yes | Type-tagged value object (see [Value encoding](#value-encoding)). The `type` field defaults to `bytes`, so `{"value": "yv4="}` is a valid `bytes` payload. |

- Rows are written under the tenant selected by `x-tsink-tenant`.
- Writing a value family that differs from the family already stored for a series is rejected with `400` (`ValueTypeMismatch`); the rejected batch is not persisted.
- An empty `rows` array is accepted and reports zero inserted rows.

**Response:** `200 application/json`

```json
{
  "status": "success",
  "data": {"inserted_rows": 2}
}
```

**Error codes:** `400` invalid JSON, missing or invalid fields, invalid base64, or a value/row/body limit exceeded; `413` per-tenant write quota or non-retryable admission resource limit; `429` admission backpressure (`X-Tsink-Write-Error-Code: write_overloaded`, `Retry-After: 1`); `501` in cluster mode; `500` internal failure.

---

### `POST /api/v1/select`

Range query over a single series with explicit ordering and a mandatory `limit`.

**Request body:**

```json
{
  "metric": "device_data",
  "labels": [["device_id", "A"]],
  "start": 1730000000000,
  "end": 1730086400000,
  "order": "desc",
  "limit": 100
}
```

| Field | Required | Description |
|---|---|---|
| `metric` | Yes | Non-empty metric name. |
| `labels` | No | Exact label set as `[name, value]` pairs. PromQL matchers are not supported. |
| `start` | Yes | Inclusive window start. |
| `end` | Yes | Exclusive window end; must be greater than `start`. |
| `order` | No | `asc` (default) or `desc`. |
| `limit` | Yes | Maximum number of points to return, `1..=100 000`. |
| `offset` | No | `asc` only. Number of leading points to skip for paging. |

**Ordering and paging:**

- `asc` returns oldest-first through the existing ascending pipeline. `offset` and `limit` page through a window; an offset past the end returns an empty `points` array.
- `desc` returns newest-first through the reverse tail scan. `offset` is rejected with `400`: continue paging by setting `end` to the oldest timestamp of the previous page (because `end` is exclusive, pages remain contiguous). The cost of a `desc` query is bounded by `limit`, not by the width of the window.
- `limit` is mandatory for both orders; a missing, zero, or out-of-range `limit` is rejected with `400`. Results beyond one page are fetched with `offset` (`asc`) or by narrowing the window and moving `end` (`desc`).

**Duplicate timestamps:** `desc` collapses points that share the same `(series, timestamp)` into a single point, keeping the newest stored copy. `asc` preserves the existing engine behaviour and may return several points for the same timestamp. The divergence is intentional: duplicate timestamps come from retries or crash replay, where the stored values are identical.

**Response:** `200 application/json`

```json
{
  "status": "success",
  "data": {
    "points": [
      {"timestamp": 1730086399000, "value": {"type": "bytes", "value": "yv4="}},
      {"timestamp": 1730086300000, "value": {"type": "bytes", "value": "yv8="}}
    ]
  }
}
```

A window with no matching points returns `200` with `"points": []`.

**Error codes:** `400` (`bad_data`) invalid JSON, missing or invalid fields, invalid range, missing/out-of-range `limit`, `offset` with `order: "desc"`, per-tenant read-query quota, or a histogram result; `429` (`X-Tsink-Read-Error-Code: read_overloaded`) read admission backpressure; `501` in cluster mode; `500` (`execution`) backend failure.

---

### `POST /api/v1/select_points`

Point lookup for a caller-supplied timestamp list.

**Request body:**

```json
{
  "metric": "device_data",
  "labels": [["device_id", "A"]],
  "timestamps": [1730000001000, 1730000060000, 1730000120000]
}
```

| Field | Required | Description |
|---|---|---|
| `metric` | Yes | Non-empty metric name. |
| `labels` | No | Exact label set as `[name, value]` pairs. |
| `timestamps` | Yes | Up to 10 000 `i64` timestamps. May be unsorted, duplicated, or empty. |

- There is no `limit` parameter; the length of the `timestamps` array is the bound (10 000).
- **No output-order guarantee.** The current implementation returns points in ascending timestamp order, but clients must align results by `timestamp` rather than rely on ordering.
- Duplicate input timestamps are collapsed, and timestamps with no stored point are omitted — no `null` placeholders are returned.
- When several stored points share the requested timestamp, the newest stored copy wins (the same collapse rule as `desc`).
- All requested timestamps are resolved against one series in a single call, so each touched chunk is decompressed at most once regardless of how many requested timestamps it contains.

**Response:** `200 application/json` — identical shape to [`POST /api/v1/select`](#post-apiv1select).

**Error codes:** `400` (`bad_data`) invalid JSON, missing or invalid fields, more than 10 000 timestamps, per-tenant read-query quota, or a histogram result; `429` (`X-Tsink-Read-Error-Code: read_overloaded`) read admission backpressure; `501` in cluster mode; `500` (`execution`) backend failure.

---

## Metadata queries

### `GET /api/v1/series`

Returns all time series matching one or more label selectors.

**Query parameters:**

| Parameter | Required | Description |
|---|---|---|
| `match[]` | Yes | Repeated. PromQL vector selector, e.g. `up{job="prometheus"}`. |

**Response:** `200 application/json`

```json
{
  "status": "success",
  "data": [
    {"__name__": "cpu_usage", "host": "web-1"},
    {"__name__": "cpu_usage", "host": "web-2"}
  ]
}
```

---

### `GET /api/v1/labels`

Returns a sorted list of all label names present in the store.

**Response:** `200 application/json`

```json
{"status": "success", "data": ["__name__", "host", "region"]}
```

---

### `GET /api/v1/label/{name}/values`

Returns the set of values for a single label name.

**Path parameters:**

| Parameter | Description |
|---|---|
| `{name}` | URL-encoded label name, e.g. `host`. |

**Response:** `200 application/json`

```json
{"status": "success", "data": ["web-1", "web-2", "db-1"]}
```

---

### `GET /api/v1/metadata`

Returns metric metadata (type, help, unit).

**Query parameters:**

| Parameter | Required | Default | Description |
|---|---|---|---|
| `metric` | No | — | Filter by metric name. |
| `limit` | No | `1000` | Maximum number of results (max `10000`). |

**Response:** `200 application/json`

```json
{
  "status": "success",
  "data": {
    "http_requests_total": [
      {"type": "counter", "help": "Total HTTP requests", "unit": ""}
    ]
  }
}
```

---

### `GET|POST /api/v1/query_exemplars`

Returns exemplars for the series matched by a PromQL expression.

**Query parameters / POST form fields:**

| Parameter | Required | Description |
|---|---|---|
| `query` | Yes | PromQL expression (vector or matrix selector). |
| `start` | No | Start timestamp. |
| `end` | No | End timestamp. |

**Response:** `200 application/json`

```json
{
  "status": "success",
  "data": [
    {
      "seriesLabels": {"__name__": "http_request_duration_seconds"},
      "exemplars": [
        {
          "labels": {"traceID": "abc123"},
          "value": "0.042",
          "timestamp": 1700000000.000
        }
      ]
    }
  ]
}
```

---

## Status

### `GET /api/v1/status/tsdb`

Returns a comprehensive JSON status snapshot covering memory usage, WAL state, compaction levels, cluster topology, admission guardrails, ingestion protocol status, exemplar store metrics, rules and rollup state, edge-sync state, and tenant policy.

**Authentication:** public scope, read permission.

**Response:** `200 application/json` — Large nested object; contents vary by configuration.

---

## Ingestion

### `POST /api/v1/write`

Prometheus Remote Write. Accepts snappy-compressed protobuf (`WriteRequest`).

**Request headers:**

| Header | Value |
|---|---|
| `Content-Type` | `application/x-protobuf` |
| `Content-Encoding` | `snappy` |
| `X-Prometheus-Remote-Write-Version` | `0.1.0` (optional) |

**Response:** `200` on success (empty body).

Optional response headers set when relevant features are active:

| Header | Description |
|---|---|
| `X-Tsink-Metadata-Applied` | Number of metadata updates applied. |
| `X-Tsink-Histograms-Accepted` | Number of native histogram samples accepted. |
| `X-Tsink-Exemplars-Accepted` | Number of exemplars accepted. |
| `X-Tsink-Exemplars-Dropped` | Number of exemplars dropped (over per-series limit). |

Feature flags (environment variables):

| Variable | Default | Description |
|---|---|---|
| `TSINK_REMOTE_WRITE_METADATA_ENABLED` | `true` | Accept metric metadata updates. |
| `TSINK_REMOTE_WRITE_EXEMPLARS_ENABLED` | `true` | Accept exemplars. |
| `TSINK_REMOTE_WRITE_HISTOGRAMS_ENABLED` | `true` | Accept native Prometheus histograms. |
| `TSINK_REMOTE_WRITE_MAX_METADATA_UPDATES` | `512` | Max metadata updates per request. |
| `TSINK_REMOTE_WRITE_MAX_HISTOGRAM_BUCKET_ENTRIES` | `16384` | Max total histogram bucket entries per request. |

**Error codes:** `400` invalid protobuf, `413` write quota exceeded, `422` disabled feature, `503` admission unavailable.

---

### `POST /api/v1/read`

Prometheus Remote Read. Accepts snappy-compressed protobuf (`ReadRequest`); responds with snappy-compressed protobuf (`ReadResponse`).

**Request headers:**

| Header | Value |
|---|---|
| `Content-Type` | `application/x-protobuf` |
| `Content-Encoding` | `snappy` |

**Response headers:**

| Header | Value |
|---|---|
| `Content-Type` | `application/x-protobuf` |
| `Content-Encoding` | `snappy` |
| `X-Prometheus-Remote-Read-Version` | `0.1.0` |

Only `Samples` response type is supported. Chunked streaming is not yet available.

**Error codes:** `400` invalid request, `413` queries-per-request quota exceeded, `503` admission unavailable.

---

### `POST /api/v1/import/prometheus`

Prometheus text exposition bulk import.

**Request headers:**

| Header | Value |
|---|---|
| `Content-Type` | `text/plain` |

**Request body:** Prometheus text exposition format, one sample per line:

```
http_requests_total{method="GET"} 1027 1700000000000
```

**Response:** `200` on success (empty body).

---

### `POST /v1/metrics`

OTLP HTTP metrics ingest. Accepts protobuf-encoded `ExportMetricsServiceRequest`.

**Request headers:**

| Header | Value |
|---|---|
| `Content-Type` | `application/x-protobuf` or `application/protobuf` |

Supported metric kinds: gauges, monotonic sums, histograms, summaries, and exponential histograms. Exemplars within OTLP payloads are forwarded to the exemplar store.

Feature flag: `TSINK_OTLP_METRICS_ENABLED` (default `true`).

**Response:** `200 application/json` — OTLP `ExportMetricsServiceResponse` JSON.

**Error codes:** `415` unsupported content type, `422` OTLP ingest disabled.

---

### `POST /write` and `POST /api/v2/write`

InfluxDB line protocol (v1 and v2 API paths). Accepts plain-text line protocol.

**Request body:** InfluxDB line protocol, e.g.:

```
cpu_usage,host=web-1 value=42.0 1700000000000000000
```

**Response:** `204` on success (no body), matching the InfluxDB convention.

---

## Admin API

Admin endpoints require `--admin-api-enabled` on the server. All admin requests require the admin bearer token (or a public token if no dedicated admin token is configured).

Mutating admin operations are recorded to the cluster audit log with the actor identity derived from the `Authorization` header. The actor ID can be overridden by passing `x-tsink-actor-id`.

---

### Data management

#### `POST /api/v1/admin/snapshot`

Take a local data snapshot.

**Request (query param or JSON body):**

| Field | Required | Description |
|---|---|---|
| `path` | Yes | Filesystem path to write the snapshot to. |

**Response:** `200 application/json`

```json
{"status": "success", "data": {"path": "/var/snapshots/snap-1"}}
```

---

#### `POST /api/v1/admin/restore`

Restore a local data snapshot.

**Request (query params or JSON body):**

| Field | Required | Aliases | Description |
|---|---|---|---|
| `snapshot_path` | Yes | `snapshotPath` | Path to an existing snapshot directory. |
| `data_path` | Yes | `dataPath` | Destination data directory to restore into. |

**Response:** `200 application/json`

```json
{
  "status": "success",
  "data": {"snapshotPath": "...", "dataPath": "..."}
}
```

---

#### `POST /api/v1/admin/delete_series`

Tombstone-delete series matching label selectors within an optional time range.

**Request (query params or JSON body):**

| Field | Required | Description |
|---|---|---|
| `match[]` | Yes | Repeated PromQL vector selectors. |
| `start` | No | Delete range start (Unix ms or RFC 3339). Defaults to `i64::MIN`. |
| `end` | No | Delete range end. Must be > `start`. Defaults to `i64::MAX`. |

The JSON body equivalent uses `selectors` (array of strings), `start`, and `end`.

**Response:** `200 application/json`

```json
{
  "status": "success",
  "data": {
    "tenantId": "default",
    "matchersProcessed": 1,
    "matchedSeries": 3,
    "tombstonesApplied": 3,
    "start": -9223372036854775808,
    "end": 9223372036854775807
  }
}
```

---

### Rules & rollups

#### `POST /api/v1/admin/rules/apply`

Apply a recording/alerting rules configuration.

**Request body (JSON):**

```json
{
  "groups": [
    {
      "name": "recording",
      "tenantId": "default",
      "interval": "1m",
      "rules": [
        {
          "kind": "recording",
          "record": "job:http_requests:rate5m",
          "expr": "rate(http_requests_total[5m])"
        }
      ]
    }
  ]
}
```

**Response:** `200 application/json` — rules snapshot.

---

#### `POST /api/v1/admin/rules/run`

Trigger an immediate rules evaluation cycle.

**Response:** `200 application/json` — rules snapshot. `409` if already running.

---

#### `GET /api/v1/admin/rules/status`

Return current rules scheduler state and last evaluation results.

**Response:** `200 application/json` — rules snapshot including `metrics.configuredGroups`, per-group rule results, and last-run timestamps.

---

#### `POST /api/v1/admin/rollups/apply`

Apply rollup downsampling policies.

**Request body (JSON):**

```json
{
  "policies": [
    {
      "metric": "cpu_usage",
      "resolution": "5m",
      "retention": "90d",
      "aggregations": ["avg", "max"]
    }
  ]
}
```

**Response:** `200 application/json` — rollup policies snapshot.

---

#### `POST /api/v1/admin/rollups/run`

Trigger an immediate rollup materialization run.

**Response:** `200 application/json` — rollup snapshot.

---

#### `GET /api/v1/admin/rollups/status`

Return current rollup scheduler state.

**Response:** `200 application/json` — rollup snapshot from storage observability.

---

### RBAC

#### `GET /api/v1/admin/rbac/state`

Return a full RBAC configuration snapshot (roles, service accounts, OIDC providers).

**Response:** `200 application/json`

```json
{"status": "success", "data": { ... }}
```

---

#### `GET /api/v1/admin/rbac/audit`

Query RBAC authorization audit log.

**Query parameters:**

| Parameter | Default | Description |
|---|---|---|
| `limit` | `100` | Maximum number of entries to return. |

**Response:** `200 application/json` — `{entries: [...]}`.

---

#### `POST /api/v1/admin/rbac/reload`

Hot-reload the RBAC configuration from disk.

**Response:** `200 application/json` — updated RBAC state snapshot.

---

#### `POST /api/v1/admin/rbac/service_accounts/create`

Create a new service account and return its bearer token.

**Request body (JSON):** `ServiceAccountSpec`

```json
{
  "id": "my-writer",
  "role": "writer",
  "description": "CI pipeline ingest account"
}
```

**Response:** `200 application/json`

```json
{
  "status": "success",
  "data": {
    "serviceAccount": { "id": "my-writer", "role": "writer", ... },
    "token": "<bearer-token>"
  }
}
```

---

#### `POST /api/v1/admin/rbac/service_accounts/update`

Update an existing service account (role, description).

**Request body (JSON):** `ServiceAccountSpec` with the target account `id`.

**Response:** `200 application/json` — updated service account.

---

#### `POST /api/v1/admin/rbac/service_accounts/rotate`

Rotate the bearer token of a service account.

**Request (query param or JSON body):**

| Field | Required | Description |
|---|---|---|
| `id` | Yes | Service account identifier. |

**Response:** `200 application/json` — service account and new token (same shape as create).

---

#### `POST /api/v1/admin/rbac/service_accounts/disable`

Disable a service account (token will be rejected).

**Request (query param or JSON body):** `id`

**Response:** `200 application/json`.

---

#### `POST /api/v1/admin/rbac/service_accounts/enable`

Re-enable a previously disabled service account.

**Request (query param or JSON body):** `id`

**Response:** `200 application/json`.

---

### Secrets & TLS rotation

#### `GET /api/v1/admin/secrets/state`

Return current security and secret state (TLS certificate expiry, token rotation status, mTLS materials).

**Response:** `200 application/json`

```json
{
  "status": "success",
  "data": { ... }
}
```

---

#### `POST /api/v1/admin/secrets/rotate`

Rotate a TLS certificate, bearer token, or mTLS material.

**Request body (JSON):**

| Field | Required | Description |
|---|---|---|
| `target` | Yes | Secret target identifier (e.g. `tls`, `admin_token`, `mtls_ca`). |
| `mode` | Yes | Rotation mode (`replace` or `overlap`). |
| `new_value` | No | New credential value (if not auto-generated). |
| `overlap_seconds` | No | Grace period during which the old credential remains valid. |

**Response:** `200 application/json`

```json
{
  "status": "success",
  "data": {
    "target": "admin_token",
    "mode": "overlap",
    "issuedCredential": "<new-token>",
    "state": { ... }
  }
}
```

---

### Usage accounting

#### `GET /api/v1/admin/usage/report`

Return aggregated usage report for one or all tenants.

**Query parameters:**

| Parameter | Default | Description |
|---|---|---|
| `tenant` | — | Tenant ID to filter. Omit for all tenants. |
| `start` | — | Unix milliseconds start (optional). |
| `end` | — | Unix milliseconds end (optional). |
| `bucket` | — | Time bucket granularity (`none`, `hour`, `day`). |
| `reconcile` | `false` | If `true`, reconciles counters against storage before reporting. |

**Response:** `200 application/json` — `{report, reconciliation, reconciledStorageSnapshots}`.

---

#### `GET /api/v1/admin/usage/export`

Stream raw per-request usage records as newline-delimited JSON.

**Query parameters:** Same `tenant`, `start`, `end` as the report endpoint.

**Response:** `200 application/x-ndjson` — one JSON record per line.

---

#### `POST /api/v1/admin/usage/reconcile`

Force immediate usage ledger reconciliation against live storage.

**Response:** `200 application/json` — `{journal, storageSnapshots}`.

---

### Support bundle

#### `GET /api/v1/admin/support_bundle`

Download a bounded JSON diagnostic snapshot for a tenant. Includes status, usage, RBAC state, RBAC audit, security state, cluster audit, handoff/repair/rebalance status, rules, and rollup state.

**Query parameters:**

| Parameter | Default | Description |
|---|---|---|
| `tenant` | `default` | Tenant to scope the bundle to. |

**Response:** `200 application/json` — downloaded as `tsink-support-bundle-<tenant>-<timestamp>.json`.

---

### Cluster management

All cluster endpoints require an active cluster runtime (`--cluster-enabled`). Parameters can be supplied as query params or in a JSON request body using either `snake_case` or `camelCase` field names.

#### `POST /api/v1/admin/cluster/join`

Add a node to the cluster ring.

**Parameters:** `node_id` (or `nodeId`), `endpoint`

**Response:** `200 application/json` — membership operation result.

---

#### `POST /api/v1/admin/cluster/leave`

Remove a node from the cluster ring.

**Parameters:** `node_id`

**Response:** `200 application/json`.

---

#### `POST /api/v1/admin/cluster/recommission`

Re-add a previously removed node.

**Parameters:** `node_id`, `endpoint` (optional)

**Response:** `200 application/json`.

---

#### Shard handoff

Shard handoff is the mechanism for migrating a shard between nodes.

| Method | Path | Description |
|---|---|---|
| `POST` | `/api/v1/admin/cluster/handoff/begin` | Start a handoff. Parameters: `shard`, `from_node_id`, `to_node_id`, `activation_ring_version` (optional). |
| `POST` | `/api/v1/admin/cluster/handoff/progress` | Report handoff progress. Parameters: `shard`, `phase`, `copied_rows`, `pending_rows`, `last_error`. |
| `POST` | `/api/v1/admin/cluster/handoff/complete` | Mark a handoff as complete. Parameters: `shard`, `from_node_id`, `to_node_id`. |
| `GET` | `/api/v1/admin/cluster/handoff/status` | Return handoff scheduler state. |

---

#### Digest-based repair

| Method | Path | Description |
|---|---|---|
| `POST` | `/api/v1/admin/cluster/repair/run` | Trigger an immediate repair cycle. |
| `POST` | `/api/v1/admin/cluster/repair/pause` | Pause the repair scheduler. |
| `POST` | `/api/v1/admin/cluster/repair/resume` | Resume the repair scheduler. |
| `POST` | `/api/v1/admin/cluster/repair/cancel` | Cancel an in-progress repair. |
| `GET` | `/api/v1/admin/cluster/repair/status` | Return repair scheduler state. |

---

#### Rebalance

| Method | Path | Description |
|---|---|---|
| `POST` | `/api/v1/admin/cluster/rebalance/run` | Trigger an immediate rebalance. |
| `POST` | `/api/v1/admin/cluster/rebalance/pause` | Pause rebalance. |
| `POST` | `/api/v1/admin/cluster/rebalance/resume` | Resume rebalance. |
| `GET` | `/api/v1/admin/cluster/rebalance/status` | Return rebalance state. |

---

#### Cluster audit log

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/v1/admin/cluster/audit` | Query audit entries. Query params: `limit` (default 100), `operation`, `target_kind`, `target_id`. |
| `GET` | `/api/v1/admin/cluster/audit/export` | Download audit log as newline-delimited JSON. Query params: `limit`, `format` (`jsonl`/`ndjson`). |

---

#### Cluster snapshots

| Method | Path | Description |
|---|---|---|
| `POST` | `/api/v1/admin/cluster/snapshot` | Coordinate a cluster-wide data snapshot. Parameter: `path`. |
| `POST` | `/api/v1/admin/cluster/restore` | Coordinate a cluster-wide restore. Parameters: `snapshot_path`, `data_path`. |
| `POST` | `/api/v1/admin/cluster/control/snapshot` | Snapshot the Raft control-plane log. Parameter: `path`. |
| `POST` | `/api/v1/admin/cluster/control/restore` | Restore the Raft control-plane log from snapshot. Parameters: `snapshot_path`, `data_path`. |

---

### Managed control plane

These endpoints manage deployment records in the embedded control-plane store. They are only available when the managed control-plane feature is configured.

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/v1/admin/control-plane/state` | Return full control-plane state snapshot. |
| `GET` | `/api/v1/admin/control-plane/audit` | Query control-plane audit log. |
| `POST` | `/api/v1/admin/control-plane/deployments/provision` | Provision or update a managed deployment record. |
| `POST` | `/api/v1/admin/control-plane/deployments/backup-policy` | Apply a backup policy to a managed deployment. |
| `POST` | `/api/v1/admin/control-plane/deployments/backup-run` | Record a completed backup run. |
| `POST` | `/api/v1/admin/control-plane/deployments/maintenance` | Apply a maintenance window to a deployment. |
| `POST` | `/api/v1/admin/control-plane/deployments/upgrade` | Apply an upgrade intent to a deployment. |
| `POST` | `/api/v1/admin/control-plane/tenants/apply` | Create or update a managed tenant record. |
| `POST` | `/api/v1/admin/control-plane/tenants/lifecycle` | Apply a lifecycle transition to a managed tenant. |

All control-plane mutation requests require a JSON body. Responses follow `{"status":"success","data":{"deployment"|"tenant": ...}}`.

---

## Error codes summary

| HTTP status | Meaning |
|---|---|
| `200` | Success. |
| `204` | Success with no body (InfluxDB write). |
| `400` | Invalid request parameters or body. |
| `401` | Missing or invalid authentication token. |
| `403` | Token has insufficient scope. |
| `404` | Path not found. |
| `409` | Conflict (e.g. scheduler already running, duplicate provisioning). |
| `413` | Request exceeds a configured quota (rows, queries, range points, histogram buckets). |
| `415` | Unsupported `Content-Type`. |
| `422` | Feature disabled on this node. |
| `429` | Admission backpressure; retry after the `Retry-After` interval. |
| `500` | Internal server error. |
| `501` | Operation not supported in this deployment mode (the typed row endpoints in cluster mode). |
| `503` | Required subsystem not configured or unavailable. |

On write and read admission errors the response also sets:

| Header | Description |
|---|---|
| `X-Tsink-Write-Error-Code` | Machine-readable write rejection code. |
| `X-Tsink-Read-Error-Code` | Machine-readable read rejection code. |
