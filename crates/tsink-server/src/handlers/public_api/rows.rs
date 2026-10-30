//! `POST /api/v1/rows` — typed batch write endpoint (spec §4.1).
//!
//! Request:
//!
//! ```json
//! {"rows":[{"metric":"device_data","labels":[["device_id","A"]],
//!           "timestamp":1730000000000,"value":{"type":"bytes","value":"yv4="}}]}
//! ```
//!
//! `value.type` defaults to `"bytes"` when omitted; timestamps are expressed in
//! the units selected by the server's `--timestamp-precision` configuration.

use super::super::*;
use super::typed_value::{
    self, from_tagged_json, into_tsink_value, TypedValueDto, MAX_ROWS_PER_REQUEST, MAX_VALUE_BYTES,
};
use serde::Deserialize;

/// One wire row. `value` stays untyped here so `type` can default to `bytes`
/// and so per-row errors keep the request row index.
#[derive(Debug, Deserialize)]
struct RawRow {
    metric: String,
    #[serde(default)]
    labels: Vec<(String, String)>,
    timestamp: i64,
    value: JsonValue,
}

/// Handles `POST /api/v1/rows` (spec §4.1).
pub(crate) async fn handle_rows_write(
    storage: &Arc<dyn Storage>,
    request: &HttpRequest,
    precision: TimestampPrecision,
    tenant_registry: Option<&tenant::TenantRegistry>,
    usage_accounting: Option<&UsageAccounting>,
) -> HttpResponse {
    let started = Instant::now();
    let write_admission = match admission::global_public_write_admission() {
        Ok(controller) => controller,
        Err(err) => return text_response(500, &format!("write admission unavailable: {err}")),
    };
    let tenant_id = match tenant_id_for_text_request(request) {
        Ok(tenant_id) => tenant_id,
        Err(response) => return response,
    };
    let tenant_plan = match prepare_tenant_request(
        tenant_registry,
        None,
        request,
        &tenant_id,
        tenant::TenantAccessScope::Write,
    ) {
        Ok(plan) => plan,
        Err(response) => return response,
    };

    if request.body.len() > typed_value::MAX_BODY_BYTES {
        return text_response(
            400,
            &format!(
                "request body too large: {} bytes (max {})",
                request.body.len(),
                typed_value::MAX_BODY_BYTES
            ),
        );
    }

    let rows = match parse_rows_request(&request.body, precision) {
        Ok(rows) => rows,
        Err(err) => return text_response(400, &err),
    };
    let row_count = rows.len();
    if row_count == 0 {
        return rows_write_success_response(0);
    }

    if let Err(err) = tenant::enforce_write_rows_quota(tenant_plan.policy(), row_count) {
        tenant_plan.record_rejected(
            tenant::TenantAdmissionSurface::Ingest,
            row_count,
            err.clone(),
        );
        return HttpResponse::new(413, err).with_header("Content-Type", "text/plain");
    }
    let _tenant_request = match tenant_plan.admit_with_usage(
        tenant::TenantAdmissionSurface::Ingest,
        row_count,
        usage_accounting,
    ) {
        Ok(guard) => guard,
        Err(err) => return err.to_http_response(),
    };
    let request_slot = match write_admission.acquire_request_slot().await {
        Ok(lease) => lease,
        Err(err) => {
            tenant_plan.record_throttled(
                tenant::TenantAdmissionSurface::Ingest,
                row_count,
                err.to_string(),
            );
            return write_admission_error_response(err);
        }
    };
    let rows = match tenant::scope_rows_for_tenant(rows, &tenant_id) {
        Ok(rows) => rows,
        Err(err) => return text_response(400, &err),
    };
    let _rows_lease = match write_admission.reserve_rows(request_slot, row_count).await {
        Ok(lease) => lease,
        Err(err) => return write_admission_error_response(err),
    };

    let storage = Arc::clone(storage);
    match tokio::task::spawn_blocking(move || storage.insert_rows(&rows)).await {
        Ok(Ok(())) => {}
        Ok(Err(err)) => return rows_write_error_response(&err),
        Err(err) => return text_response(500, &format!("rows write task failed: {err}")),
    }

    record_ingest_usage(
        usage_accounting,
        &tenant_id,
        "rows_write",
        request.path_without_query(),
        IngestUsageMetrics::new(
            row_count as u64,
            0,
            0,
            0,
            0,
            elapsed_nanos_since(started),
            request.body.len() as u64,
        ),
    );
    rows_write_success_response(row_count)
}

fn rows_write_success_response(inserted_rows: usize) -> HttpResponse {
    json_response(
        200,
        &json!({"status": "success", "data": { "inserted_rows": inserted_rows }}),
    )
}

fn parse_rows_request(body: &[u8], precision: TimestampPrecision) -> Result<Vec<Row>, String> {
    let payload: JsonValue =
        serde_json::from_slice(body).map_err(|err| format!("invalid JSON body: {err}"))?;
    let raw_rows = payload
        .get("rows")
        .ok_or_else(|| "request body must contain a `rows` array".to_string())?
        .as_array()
        .ok_or_else(|| "`rows` must be an array".to_string())?;
    if raw_rows.len() > MAX_ROWS_PER_REQUEST {
        return Err(format!(
            "too many rows: {} (max {MAX_ROWS_PER_REQUEST})",
            raw_rows.len()
        ));
    }
    raw_rows
        .iter()
        .enumerate()
        .map(|(index, raw_row)| parse_row(index, raw_row, precision))
        .collect()
}

fn parse_row(
    index: usize,
    raw_row: &JsonValue,
    precision: TimestampPrecision,
) -> Result<Row, String> {
    let row = RawRow::deserialize(raw_row).map_err(|err| format!("row {index}: {err}"))?;
    if row.metric.is_empty() {
        return Err(format!("row {index}: metric name is required"));
    }
    validate_timestamp(index, row.timestamp, precision)?;
    let labels = row
        .labels
        .into_iter()
        .map(|(name, value)| Label::new(name, value))
        .collect();
    let value = parse_row_value(index, &row.value)?;
    Ok(Row::with_labels(
        row.metric,
        labels,
        DataPoint::new(row.timestamp, into_tsink_value(value)),
    ))
}

/// Accepts `{"type": <tag>, "value": ...}`; an omitted `type` means `bytes`
/// (the endpoint's primary use, spec §4.1).
fn parse_row_value(index: usize, raw_value: &JsonValue) -> Result<TypedValueDto, String> {
    let value = match raw_value {
        JsonValue::Object(fields) => {
            if fields.contains_key("type") {
                serde_json::from_value::<TypedValueDto>(raw_value.clone())
                    .map_err(|err| format!("row {index}: invalid value: {err}"))?
            } else {
                let payload = fields
                    .get("value")
                    .ok_or_else(|| format!("row {index}: value must contain a `value` field"))?;
                from_tagged_json::<serde_json::Error>("bytes", payload.clone())
                    .map_err(|err| format!("row {index}: invalid bytes value: {err}"))?
            }
        }
        _ => return Err(format!("row {index}: value must be an object")),
    };

    let size = match &value {
        TypedValueDto::Bytes(bytes) => bytes.len(),
        TypedValueDto::Str(text) => text.len(),
        TypedValueDto::F64(_)
        | TypedValueDto::I64(_)
        | TypedValueDto::U64(_)
        | TypedValueDto::Bool(_) => 0,
    };
    if size > MAX_VALUE_BYTES {
        return Err(format!(
            "row {index}: value is {size} bytes (max {MAX_VALUE_BYTES})"
        ));
    }
    Ok(value)
}

/// Request timestamps are already expressed in `precision` units (spec §4) and
/// are stored verbatim; only the engine-wide sentinel needs rejecting.
fn validate_timestamp(
    index: usize,
    timestamp: i64,
    precision: TimestampPrecision,
) -> Result<(), String> {
    if timestamp == i64::MAX {
        return Err(format!(
            "row {index}: timestamp {timestamp} is out of supported range for {precision:?} precision"
        ));
    }
    Ok(())
}

/// Storage failures raised by `insert_rows`: validation and value-family
/// mismatches are client errors, everything else follows the shared write
/// error classification.
fn rows_write_error_response(err: &tsink::TsinkError) -> HttpResponse {
    match err {
        tsink::TsinkError::ValueTypeMismatch { .. }
        | tsink::TsinkError::MetricRequired
        | tsink::TsinkError::InvalidMetricName(_)
        | tsink::TsinkError::InvalidLabel(_) => text_response(400, &err.to_string()),
        _ => storage_write_error_response("rows write", err),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::http::{HttpRequest, HttpResponse};
    use base64::Engine as _;
    use serde_json::{json, Value as JsonValue};
    use std::collections::HashMap;
    use tsink::{Label, Storage, StorageBuilder, TimestampPrecision};

    fn make_storage(precision: TimestampPrecision) -> Arc<dyn Storage> {
        StorageBuilder::new()
            .with_timestamp_precision(precision)
            .with_metadata_shard_count(crate::cluster::config::DEFAULT_CLUSTER_SHARDS)
            .build()
            .expect("storage should build")
    }

    fn rows_request(body: impl Into<Vec<u8>>) -> HttpRequest {
        HttpRequest {
            method: "POST".to_string(),
            path: "/api/v1/rows".to_string(),
            headers: HashMap::from([("content-type".to_string(), "application/json".to_string())]),
            body: body.into(),
        }
    }

    async fn write_rows(
        storage: &Arc<dyn Storage>,
        precision: TimestampPrecision,
        body: impl Into<Vec<u8>>,
    ) -> HttpResponse {
        handle_rows_write(storage, &rows_request(body), precision, None, None).await
    }

    fn response_json(response: &HttpResponse) -> JsonValue {
        serde_json::from_slice(&response.body).expect("response body should decode")
    }

    fn response_text(response: &HttpResponse) -> String {
        String::from_utf8_lossy(&response.body).into_owned()
    }

    fn tenant_scoped(device_id: &str) -> Vec<Label> {
        vec![
            Label::new("device_id", device_id),
            Label::new(tenant::TENANT_LABEL, tenant::DEFAULT_TENANT_ID),
        ]
    }

    #[tokio::test]
    async fn rows_write_roundtrip_and_mixed_families() {
        let storage = make_storage(TimestampPrecision::Milliseconds);
        let body = json!({
            "rows": [
                {
                    "metric": "device_data",
                    "labels": [["device_id", "A"]],
                    "timestamp": 1_730_000_000_000_i64,
                    "value": {"type": "bytes", "value": "yv4="}
                },
                {
                    "metric": "temperature",
                    "labels": [["device_id", "A"]],
                    "timestamp": 1_730_000_000_000_i64,
                    "value": {"type": "f64", "value": 21.5}
                },
                {
                    "metric": "device_data",
                    "labels": [["device_id", "B"]],
                    "timestamp": 1_730_000_001_000_i64,
                    "value": {"value": "yv8="}
                }
            ]
        })
        .to_string();

        let response = write_rows(&storage, TimestampPrecision::Milliseconds, body).await;
        assert_eq!(response.status, 200, "{}", response_text(&response));
        let payload = response_json(&response);
        assert_eq!(payload["status"], "success");
        assert_eq!(payload["data"]["inserted_rows"], 3);

        let points = storage
            .select(
                "device_data",
                &tenant_scoped("A"),
                1_730_000_000_000,
                1_730_000_000_001,
            )
            .expect("bytes point must be persisted");
        assert_eq!(points.len(), 1);
        assert_eq!(points[0].value.as_bytes(), Some(&[0xCA, 0xFE][..]));

        let defaulted = storage
            .select(
                "device_data",
                &tenant_scoped("B"),
                1_730_000_001_000,
                1_730_000_001_001,
            )
            .expect("type-omitted bytes point must be persisted");
        assert_eq!(defaulted.len(), 1);
        assert_eq!(defaulted[0].value.as_bytes(), Some(&[0xCA, 0xFF][..]));

        let numeric = storage
            .select(
                "temperature",
                &tenant_scoped("A"),
                1_730_000_000_000,
                1_730_000_000_001,
            )
            .expect("f64 point must be persisted");
        assert_eq!(numeric.len(), 1);
        assert_eq!(numeric[0].value.as_f64(), Some(21.5));
    }

    #[tokio::test]
    async fn rows_write_same_series_mixed_family_rejected_400() {
        let storage = make_storage(TimestampPrecision::Milliseconds);
        let body = json!({
            "rows": [
                {
                    "metric": "device_data",
                    "labels": [["device_id", "A"]],
                    "timestamp": 1_730_000_000_000_i64,
                    "value": {"type": "bytes", "value": "yv4="}
                },
                {
                    "metric": "device_data",
                    "labels": [["device_id", "A"]],
                    "timestamp": 1_730_000_000_001_i64,
                    "value": {"type": "f64", "value": 21.5}
                }
            ]
        })
        .to_string();

        let response = write_rows(&storage, TimestampPrecision::Milliseconds, body).await;
        assert_eq!(response.status, 400, "{}", response_text(&response));
        assert!(
            response_text(&response).to_lowercase().contains("mismatch"),
            "expected a value-type mismatch error, got {}",
            response_text(&response)
        );
        assert!(
            storage
                .select(
                    "device_data",
                    &tenant_scoped("A"),
                    1_730_000_000_000,
                    1_730_000_000_002,
                )
                .expect("post-rejection read should succeed")
                .is_empty(),
            "a rejected batch must not persist a partial series"
        );
    }

    #[tokio::test]
    async fn rows_write_limit_rejections() {
        let storage = make_storage(TimestampPrecision::Milliseconds);

        let too_many_rows: Vec<JsonValue> = (0..=MAX_ROWS_PER_REQUEST)
            .map(|index| {
                json!({
                    "metric": "device_data",
                    "labels": [["device_id", format!("device-{index}")]],
                    "timestamp": 1_730_000_000_000_i64,
                    "value": {"type": "bytes", "value": "yv4="}
                })
            })
            .collect();
        let response = write_rows(
            &storage,
            TimestampPrecision::Milliseconds,
            json!({ "rows": too_many_rows }).to_string(),
        )
        .await;
        assert_eq!(response.status, 400, "{}", response_text(&response));
        assert!(
            response_text(&response).contains("too many rows"),
            "got {}",
            response_text(&response)
        );

        let oversized_value =
            base64::engine::general_purpose::STANDARD.encode(vec![0_u8; MAX_VALUE_BYTES + 1]);
        let response = write_rows(
            &storage,
            TimestampPrecision::Milliseconds,
            json!({
                "rows": [{
                    "metric": "device_data",
                    "labels": [["device_id", "A"]],
                    "timestamp": 1_730_000_000_000_i64,
                    "value": {"type": "bytes", "value": oversized_value}
                }]
            })
            .to_string(),
        )
        .await;
        assert_eq!(response.status, 400, "{}", response_text(&response));
        assert!(
            response_text(&response).contains("value is"),
            "got {}",
            response_text(&response)
        );

        let mut oversized_body = vec![b'x'; typed_value::MAX_BODY_BYTES + 1];
        oversized_body[0] = b'{';
        let response = write_rows(&storage, TimestampPrecision::Milliseconds, oversized_body).await;
        assert_eq!(response.status, 400, "{}", response_text(&response));
        assert!(
            response_text(&response).contains("request body too large"),
            "got {}",
            response_text(&response)
        );

        let response = write_rows(
            &storage,
            TimestampPrecision::Milliseconds,
            json!({
                "rows": [{
                    "metric": "device_data",
                    "labels": [["device_id", "A"]],
                    "timestamp": 1_730_000_000_000_i64,
                    "value": {"type": "bytes", "value": "not_base64!!"}
                }]
            })
            .to_string(),
        )
        .await;
        assert_eq!(response.status, 400, "{}", response_text(&response));
        assert!(
            response_text(&response).contains("base64"),
            "got {}",
            response_text(&response)
        );

        let response = write_rows(
            &storage,
            TimestampPrecision::Milliseconds,
            json!({
                "rows": [{
                    "metric": "device_data",
                    "labels": [["device_id", "A"]],
                    "timestamp": 1_730_000_000_000_i64,
                    "value": {"type": "histogram", "value": {}}
                }]
            })
            .to_string(),
        )
        .await;
        assert_eq!(response.status, 400, "{}", response_text(&response));
    }

    #[tokio::test]
    async fn rows_write_honors_timestamp_precision() {
        let millis_storage = make_storage(TimestampPrecision::Milliseconds);
        let response = write_rows(
            &millis_storage,
            TimestampPrecision::Milliseconds,
            json!({
                "rows": [{
                    "metric": "device_data",
                    "labels": [["device_id", "A"]],
                    "timestamp": 1_730_000_000_123_i64,
                    "value": {"type": "bytes", "value": "yv4="}
                }]
            })
            .to_string(),
        )
        .await;
        assert_eq!(response.status, 200, "{}", response_text(&response));
        let points = millis_storage
            .select(
                "device_data",
                &tenant_scoped("A"),
                1_730_000_000_123,
                1_730_000_000_124,
            )
            .expect("millisecond timestamp must be stored verbatim");
        assert_eq!(points.len(), 1);
        assert_eq!(points[0].timestamp, 1_730_000_000_123);

        let seconds_storage = make_storage(TimestampPrecision::Seconds);
        let response = write_rows(
            &seconds_storage,
            TimestampPrecision::Seconds,
            json!({
                "rows": [{
                    "metric": "device_data",
                    "labels": [["device_id", "B"]],
                    "timestamp": 1_730_000_000_i64,
                    "value": {"type": "bytes", "value": "yv4="}
                }]
            })
            .to_string(),
        )
        .await;
        assert_eq!(response.status, 200, "{}", response_text(&response));
        let points = seconds_storage
            .select(
                "device_data",
                &tenant_scoped("B"),
                1_730_000_000,
                1_730_000_001,
            )
            .expect("second timestamp must not be rescaled");
        assert_eq!(points.len(), 1);
        assert_eq!(points[0].timestamp, 1_730_000_000);
        assert!(seconds_storage
            .select(
                "device_data",
                &tenant_scoped("B"),
                1_730_000_000_000,
                1_730_000_000_001,
            )
            .expect("empty range query should succeed")
            .is_empty());
    }
}
