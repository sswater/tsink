//! `POST /api/v1/select` and `POST /api/v1/select_points` — typed read
//! endpoints (spec §4.2 / §4.3).
//!
//! `select` accepts a metric, an exact label set, the `[start, end)` window,
//! `order` (`asc` by default, `desc` for the reverse tail scan) and a required
//! `limit` (1..=100_000). Only `asc` accepts `offset`; `desc` paginates by
//! moving `end` (spec §4.2). `select_points` accepts a bounded timestamp list
//! (max 10_000) and returns only the points that exist, ascending, with
//! duplicate timestamps collapsed (spec §4.3).
//!
//! Values are exchanged through [`super::typed_value::TypedValueDto`]; values
//! of the histogram family are rejected with `400`.

use super::super::router::cluster_mode_unsupported_response;
use super::super::*;
use super::typed_value::{from_tsink_value, MAX_LIMIT, MAX_TIMESTAMPS};
use serde::Deserialize;
use tsink::QueryOptions;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
enum SelectOrder {
    #[default]
    Asc,
    Desc,
}

#[derive(Debug, Deserialize)]
struct RawSelectRequest {
    metric: String,
    #[serde(default)]
    labels: Vec<(String, String)>,
    start: i64,
    end: i64,
    #[serde(default)]
    order: SelectOrder,
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    offset: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct RawSelectPointsRequest {
    metric: String,
    #[serde(default)]
    labels: Vec<(String, String)>,
    timestamps: Vec<i64>,
}

struct SelectQuery {
    metric: String,
    labels: Vec<Label>,
    start: i64,
    end: i64,
    order: SelectOrder,
    limit: usize,
    offset: usize,
}

struct SelectPointsQuery {
    metric: String,
    labels: Vec<Label>,
    timestamps: Vec<i64>,
}

/// Handles `POST /api/v1/select` (spec §4.2).
pub(crate) async fn handle_rows_select(
    storage: &Arc<dyn Storage>,
    request: &HttpRequest,
    precision: TimestampPrecision,
    cluster_context: Option<&ClusterRequestContext>,
    public_read_context: &PublicReadContext<'_>,
) -> HttpResponse {
    // Timestamps arrive in `precision` units and are stored verbatim (spec §4);
    // the parameter is part of the pinned handler signature.
    let _ = precision;
    if cluster_context.is_some() {
        return cluster_mode_unsupported_response();
    }
    let started = Instant::now();
    let read_admission = match admission::global_public_read_admission() {
        Ok(controller) => controller,
        Err(err) => return text_response(500, &format!("read admission unavailable: {err}")),
    };
    let tenant_id = match tenant::tenant_id_for_request(request) {
        Ok(tenant_id) => tenant_id,
        Err(err) => return select_bad_request_response(&err),
    };
    let tenant_plan = match prepare_tenant_request(
        public_read_context.tenant_registry,
        public_read_context.managed_control_plane,
        request,
        &tenant_id,
        tenant::TenantAccessScope::Read,
    ) {
        Ok(plan) => plan,
        Err(response) => return response,
    };
    if let Err(err) = tenant::enforce_read_queries_quota(tenant_plan.policy(), 1) {
        tenant_plan.record_rejected(tenant::TenantAdmissionSurface::Query, 1, err.clone());
        return select_bad_request_response(&err);
    }
    let _tenant_request = match tenant_plan.admit_with_usage(
        tenant::TenantAdmissionSurface::Query,
        1,
        public_read_context.usage_accounting,
    ) {
        Ok(guard) => guard,
        Err(err) => return err.to_http_response(),
    };
    let _read_admission = match read_admission.admit_request(1).await {
        Ok(lease) => lease,
        Err(err) => {
            tenant_plan.record_throttled(tenant::TenantAdmissionSurface::Query, 1, err.to_string());
            return read_admission_error_response(err);
        }
    };
    let query = match parse_select_request(&request.body) {
        Ok(query) => query,
        Err(err) => return select_bad_request_response(&err),
    };
    if let Err(err) = tenant::enforce_query_points_quota(tenant_plan.policy(), query.limit) {
        tenant_plan.record_rejected(tenant::TenantAdmissionSurface::Query, 1, err.clone());
        return select_bad_request_response(&err);
    }

    let storage = tenant::scoped_storage(Arc::clone(storage), &tenant_id);
    let outcome = tokio::task::spawn_blocking(move || match query.order {
        SelectOrder::Asc => storage.select_with_options(
            &query.metric,
            QueryOptions::new(query.start, query.end)
                .with_labels(query.labels)
                .with_pagination(query.offset, Some(query.limit)),
        ),
        SelectOrder::Desc => storage.select_desc(
            &query.metric,
            &query.labels,
            query.start,
            query.end,
            query.limit,
        ),
    })
    .await;

    match outcome {
        Ok(Ok(points)) => {
            record_query_pressure(&tenant_id, 1, points.len());
            record_query_usage(
                public_read_context.usage_accounting,
                &tenant_id,
                "select",
                request.path_without_query(),
                QueryUsageMetrics::new(
                    1,
                    points.len() as u64,
                    elapsed_nanos_since(started),
                    request.body.len() as u64,
                ),
            );
            select_points_success_response(&points)
        }
        Ok(Err(err)) => select_storage_error_response(&err),
        Err(err) => select_execution_error_response(&format!("select task failed: {err}")),
    }
}

/// Handles `POST /api/v1/select_points` (spec §4.3).
pub(crate) async fn handle_rows_select_points(
    storage: &Arc<dyn Storage>,
    request: &HttpRequest,
    precision: TimestampPrecision,
    cluster_context: Option<&ClusterRequestContext>,
    public_read_context: &PublicReadContext<'_>,
) -> HttpResponse {
    // Timestamps arrive in `precision` units and are stored verbatim (spec §4);
    // the parameter is part of the pinned handler signature.
    let _ = precision;
    if cluster_context.is_some() {
        return cluster_mode_unsupported_response();
    }
    let started = Instant::now();
    let read_admission = match admission::global_public_read_admission() {
        Ok(controller) => controller,
        Err(err) => return text_response(500, &format!("read admission unavailable: {err}")),
    };
    let tenant_id = match tenant::tenant_id_for_request(request) {
        Ok(tenant_id) => tenant_id,
        Err(err) => return select_bad_request_response(&err),
    };
    let tenant_plan = match prepare_tenant_request(
        public_read_context.tenant_registry,
        public_read_context.managed_control_plane,
        request,
        &tenant_id,
        tenant::TenantAccessScope::Read,
    ) {
        Ok(plan) => plan,
        Err(response) => return response,
    };
    if let Err(err) = tenant::enforce_read_queries_quota(tenant_plan.policy(), 1) {
        tenant_plan.record_rejected(tenant::TenantAdmissionSurface::Query, 1, err.clone());
        return select_bad_request_response(&err);
    }
    let _tenant_request = match tenant_plan.admit_with_usage(
        tenant::TenantAdmissionSurface::Query,
        1,
        public_read_context.usage_accounting,
    ) {
        Ok(guard) => guard,
        Err(err) => return err.to_http_response(),
    };
    let _read_admission = match read_admission.admit_request(1).await {
        Ok(lease) => lease,
        Err(err) => {
            tenant_plan.record_throttled(tenant::TenantAdmissionSurface::Query, 1, err.to_string());
            return read_admission_error_response(err);
        }
    };
    let query = match parse_select_points_request(&request.body) {
        Ok(query) => query,
        Err(err) => return select_bad_request_response(&err),
    };
    if let Err(err) =
        tenant::enforce_query_points_quota(tenant_plan.policy(), query.timestamps.len())
    {
        tenant_plan.record_rejected(tenant::TenantAdmissionSurface::Query, 1, err.clone());
        return select_bad_request_response(&err);
    }

    let storage = tenant::scoped_storage(Arc::clone(storage), &tenant_id);
    let outcome = tokio::task::spawn_blocking(move || {
        storage.select_at_timestamps(&query.metric, &query.labels, &query.timestamps)
    })
    .await;

    match outcome {
        Ok(Ok(points)) => {
            record_query_pressure(&tenant_id, 1, points.len());
            record_query_usage(
                public_read_context.usage_accounting,
                &tenant_id,
                "select_points",
                request.path_without_query(),
                QueryUsageMetrics::new(
                    1,
                    points.len() as u64,
                    elapsed_nanos_since(started),
                    request.body.len() as u64,
                ),
            );
            select_points_success_response(&points)
        }
        Ok(Err(err)) => select_storage_error_response(&err),
        Err(err) => select_execution_error_response(&format!("select_points task failed: {err}")),
    }
}

fn parse_select_request(body: &[u8]) -> Result<SelectQuery, String> {
    let raw: RawSelectRequest =
        serde_json::from_slice(body).map_err(|err| format!("invalid JSON body: {err}"))?;
    if raw.metric.is_empty() {
        return Err("metric name is required".to_string());
    }
    let Some(limit) = raw.limit else {
        return Err("`limit` is required".to_string());
    };
    if limit == 0 {
        return Err("`limit` must be greater than zero".to_string());
    }
    if limit > MAX_LIMIT {
        return Err(format!("`limit` must be <= {MAX_LIMIT}, got {limit}"));
    }
    if raw.order == SelectOrder::Desc && raw.offset.is_some() {
        return Err(
            "`offset` is not supported with order \"desc\"; page by adjusting `end`".to_string(),
        );
    }
    if raw.start >= raw.end {
        return Err(format!(
            "invalid time range: start {} must be less than end {}",
            raw.start, raw.end
        ));
    }
    Ok(SelectQuery {
        metric: raw.metric,
        labels: raw
            .labels
            .into_iter()
            .map(|(name, value)| Label::new(name, value))
            .collect(),
        start: raw.start,
        end: raw.end,
        order: raw.order,
        limit,
        offset: raw.offset.unwrap_or(0),
    })
}

fn parse_select_points_request(body: &[u8]) -> Result<SelectPointsQuery, String> {
    let raw: RawSelectPointsRequest =
        serde_json::from_slice(body).map_err(|err| format!("invalid JSON body: {err}"))?;
    if raw.metric.is_empty() {
        return Err("metric name is required".to_string());
    }
    if raw.timestamps.len() > MAX_TIMESTAMPS {
        return Err(format!(
            "too many timestamps: {} (max {MAX_TIMESTAMPS})",
            raw.timestamps.len()
        ));
    }
    Ok(SelectPointsQuery {
        metric: raw.metric,
        labels: raw
            .labels
            .into_iter()
            .map(|(name, value)| Label::new(name, value))
            .collect(),
        timestamps: raw.timestamps,
    })
}

fn select_points_success_response(points: &[DataPoint]) -> HttpResponse {
    let mut out = Vec::with_capacity(points.len());
    for point in points {
        let value = match from_tsink_value(&point.value) {
            Ok(value) => value,
            Err(err) => return select_bad_request_response(err),
        };
        out.push(json!({"timestamp": point.timestamp, "value": value}));
    }
    json_response(200, &json!({"status": "success", "data": {"points": out}}))
}

fn select_bad_request_response(error: &str) -> HttpResponse {
    query_error_response(400, "bad_data", error)
}

fn select_execution_error_response(error: &str) -> HttpResponse {
    query_error_response(500, "execution", error)
}

/// Storage failures raised by the read paths: invalid ranges/identifiers are
/// client errors; backends without the read path answer 501.
fn select_storage_error_response(err: &tsink::TsinkError) -> HttpResponse {
    match err {
        tsink::TsinkError::InvalidTimeRange { .. }
        | tsink::TsinkError::MetricRequired
        | tsink::TsinkError::InvalidMetricName(_)
        | tsink::TsinkError::InvalidLabel(_) => select_bad_request_response(&err.to_string()),
        tsink::TsinkError::UnsupportedOperation { .. } => text_response(501, &err.to_string()),
        _ => select_execution_error_response(&err.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::super::super::router::route_request;
    use super::super::super::{AppContext, PublicReadContext, RequestContext};
    use super::super::typed_value::{MAX_LIMIT, MAX_TIMESTAMPS};
    use super::*;

    use crate::cluster::config::ClusterConfig;
    use crate::cluster::{ClusterRequestContext, ClusterRuntime};
    use crate::exemplar_store::ExemplarStore;
    use crate::http::{HttpRequest, HttpResponse};
    use crate::managed_control_plane::{
        DeploymentLifecycleState, ManagedControlPlane, ManagedControlPlaneActor,
        ManagedDeploymentProvisionRequest, ManagedTenantApplyRequest, TenantLifecycleState,
    };
    use crate::metadata_store::MetricMetadataStore;
    use crate::tenant;
    use serde_json::{json, Value as JsonValue};
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Instant;
    use tsink::promql::Engine;
    use tsink::{DataPoint, Label, Row, Storage, StorageBuilder, TimestampPrecision};

    const METRIC: &str = "device_data";
    const BASE_TS: i64 = 1_730_000_000_000;
    const STEP: i64 = 1_000;

    fn make_storage() -> Arc<dyn Storage> {
        StorageBuilder::new()
            .with_timestamp_precision(TimestampPrecision::Milliseconds)
            .with_metadata_shard_count(crate::cluster::config::DEFAULT_CLUSTER_SHARDS)
            .build()
            .expect("storage should build")
    }

    fn no_read_context() -> PublicReadContext<'static> {
        PublicReadContext::new(None, None, None, None)
    }

    fn device_labels(device_id: &str) -> Vec<Label> {
        vec![Label::new("device_id", device_id)]
    }

    fn scoped_labels(device_id: &str) -> Vec<Label> {
        let mut labels = device_labels(device_id);
        labels.push(Label::new(tenant::TENANT_LABEL, tenant::DEFAULT_TENANT_ID));
        labels
    }

    /// Seeds points through the default tenant's scoped storage so they land
    /// under the same tenant label the handler queries with.
    fn seed(storage: &Arc<dyn Storage>, points: &[(i64, f64)]) {
        let rows: Vec<Row> = points
            .iter()
            .map(|(timestamp, value)| {
                Row::with_labels(
                    METRIC,
                    device_labels("A"),
                    DataPoint::new(*timestamp, *value),
                )
            })
            .collect();
        tenant::scoped_storage(Arc::clone(storage), tenant::DEFAULT_TENANT_ID)
            .insert_rows(&rows)
            .expect("seed rows should insert");
    }

    fn seed_indexed(storage: &Arc<dyn Storage>, count: i64) {
        let points: Vec<(i64, f64)> = (0..count)
            .map(|index| (BASE_TS + index * STEP, index as f64))
            .collect();
        seed(storage, &points);
    }

    fn request(path: &str, body: JsonValue) -> HttpRequest {
        HttpRequest {
            method: "POST".to_string(),
            path: path.to_string(),
            headers: HashMap::from([("content-type".to_string(), "application/json".to_string())]),
            body: body.to_string().into_bytes(),
        }
    }

    fn select_body(order: Option<&str>, limit: Option<usize>, offset: Option<usize>) -> JsonValue {
        let mut body = json!({
            "metric": METRIC,
            "labels": [["device_id", "A"]],
            "start": BASE_TS,
            "end": BASE_TS + 60 * STEP,
        });
        if let Some(order) = order {
            body["order"] = JsonValue::String(order.to_string());
        }
        if let Some(limit) = limit {
            body["limit"] = json!(limit);
        }
        if let Some(offset) = offset {
            body["offset"] = json!(offset);
        }
        body
    }

    async fn run_select(storage: &Arc<dyn Storage>, body: JsonValue) -> HttpResponse {
        handle_rows_select(
            storage,
            &request("/api/v1/select", body),
            TimestampPrecision::Milliseconds,
            None,
            &no_read_context(),
        )
        .await
    }

    async fn run_select_points(storage: &Arc<dyn Storage>, body: JsonValue) -> HttpResponse {
        handle_rows_select_points(
            storage,
            &request("/api/v1/select_points", body),
            TimestampPrecision::Milliseconds,
            None,
            &no_read_context(),
        )
        .await
    }

    fn response_json(response: &HttpResponse) -> JsonValue {
        serde_json::from_slice(&response.body).expect("response body should decode")
    }

    fn response_text(response: &HttpResponse) -> String {
        String::from_utf8_lossy(&response.body).into_owned()
    }

    fn response_points(body: &JsonValue) -> &Vec<JsonValue> {
        body["data"]["points"]
            .as_array()
            .expect("response should contain a points array")
    }

    fn point_timestamps(points: &[JsonValue]) -> Vec<i64> {
        points
            .iter()
            .map(|point| {
                point["timestamp"]
                    .as_i64()
                    .expect("timestamp should be i64")
            })
            .collect()
    }

    fn point_values(points: &[JsonValue]) -> Vec<f64> {
        points
            .iter()
            .map(|point| {
                assert_eq!(point["value"]["type"], "f64");
                point["value"]["value"]
                    .as_f64()
                    .expect("value should be an f64")
            })
            .collect()
    }

    fn make_cluster_context() -> ClusterRequestContext {
        let mut config = ClusterConfig {
            enabled: true,
            node_id: Some("node-a".to_string()),
            bind: Some("127.0.0.1:9301".to_string()),
            seeds: Vec::new(),
            ..ClusterConfig::default()
        };
        if config.cluster_internal_auth_token().is_none() {
            config.internal_auth_token = Some("cluster-test-token".to_string());
        }
        let runtime = ClusterRuntime::bootstrap(&config)
            .expect("cluster runtime should build")
            .expect("cluster runtime should be enabled");
        ClusterRequestContext::from_runtime(runtime).expect("cluster context should build")
    }

    #[tokio::test]
    async fn asc_parity_with_storage_select_and_offset_paging() {
        let storage = make_storage();
        seed_indexed(&storage, 10);

        let start = BASE_TS;
        let end = BASE_TS + 60 * STEP;
        let expected = storage
            .select(METRIC, &scoped_labels("A"), start, end)
            .expect("baseline select should succeed");
        assert_eq!(expected.len(), 10);

        let mut paged_timestamps = Vec::new();
        let mut paged_values = Vec::new();
        for (offset, limit) in [(0_usize, 4_usize), (4, 4), (8, 4)] {
            // Order omitted => asc.
            let order = (offset == 4).then_some("asc");
            let response =
                run_select(&storage, select_body(order, Some(limit), Some(offset))).await;
            assert_eq!(response.status, 200, "{}", response_text(&response));
            let body = response_json(&response);
            assert_eq!(body["status"], "success");
            let points = response_points(&body);
            paged_timestamps.extend(point_timestamps(points));
            paged_values.extend(point_values(points));
        }

        let expected_timestamps: Vec<i64> = expected.iter().map(|point| point.timestamp).collect();
        let expected_values: Vec<f64> = expected
            .iter()
            .map(|point| point.value_as_f64().expect("f64 value"))
            .collect();
        assert_eq!(paged_timestamps, expected_timestamps);
        assert_eq!(paged_values, expected_values);
    }

    #[tokio::test]
    async fn desc_returns_newest_limit_descending() {
        let storage = make_storage();
        seed_indexed(&storage, 10);

        let limit = 3;
        let response = run_select(&storage, select_body(Some("desc"), Some(limit), None)).await;
        assert_eq!(response.status, 200, "{}", response_text(&response));
        let body = response_json(&response);
        let points = response_points(&body);
        let timestamps = point_timestamps(points);
        assert_eq!(
            timestamps,
            vec![BASE_TS + 9 * STEP, BASE_TS + 8 * STEP, BASE_TS + 7 * STEP]
        );
        assert!(
            timestamps.windows(2).all(|pair| pair[0] > pair[1]),
            "desc output must be strictly descending, got {timestamps:?}"
        );

        let baseline = storage
            .select_desc(
                METRIC,
                &scoped_labels("A"),
                BASE_TS,
                BASE_TS + 60 * STEP,
                limit,
            )
            .expect("baseline select_desc should succeed");
        let baseline_timestamps: Vec<i64> = baseline.iter().map(|point| point.timestamp).collect();
        assert_eq!(timestamps, baseline_timestamps);
    }

    #[tokio::test]
    async fn duplicate_ts_divergence_desc_vs_asc() {
        let storage = make_storage();
        // Two writes of the same (series, timestamp) with different values; the
        // engine does not dedupe on the ascending path.
        seed(&storage, &[(BASE_TS + STEP, 1.0)]);
        seed(&storage, &[(BASE_TS + STEP, 2.0)]);

        let asc = run_select(&storage, select_body(Some("asc"), Some(10), None)).await;
        assert_eq!(asc.status, 200, "{}", response_text(&asc));
        let asc_body = response_json(&asc);
        assert_eq!(
            response_points(&asc_body).len(),
            2,
            "asc must preserve duplicate timestamps"
        );

        let desc = run_select(&storage, select_body(Some("desc"), Some(10), None)).await;
        assert_eq!(desc.status, 200, "{}", response_text(&desc));
        let desc_body = response_json(&desc);
        assert_eq!(
            response_points(&desc_body).len(),
            1,
            "desc must collapse duplicate timestamps (spec §4.2)"
        );

        let points = run_select_points(
            &storage,
            json!({
                "metric": METRIC,
                "labels": [["device_id", "A"]],
                "timestamps": [BASE_TS + STEP, BASE_TS + STEP],
            }),
        )
        .await;
        assert_eq!(points.status, 200, "{}", response_text(&points));
        let points_body = response_json(&points);
        assert_eq!(
            response_points(&points_body).len(),
            1,
            "point queries must collapse duplicate timestamps (spec §4.3)"
        );
    }

    #[tokio::test]
    async fn limit_is_required_and_bounded() {
        let storage = make_storage();
        seed_indexed(&storage, 3);

        let missing = run_select(&storage, select_body(None, None, None)).await;
        assert_eq!(missing.status, 400, "{}", response_text(&missing));
        assert!(response_text(&missing).contains("limit"));

        let zero = run_select(&storage, select_body(None, Some(0), None)).await;
        assert_eq!(zero.status, 400, "{}", response_text(&zero));

        let too_large = run_select(&storage, select_body(None, Some(MAX_LIMIT + 1), None)).await;
        assert_eq!(too_large.status, 400, "{}", response_text(&too_large));

        let at_max = run_select(&storage, select_body(None, Some(MAX_LIMIT), None)).await;
        assert_eq!(at_max.status, 200, "{}", response_text(&at_max));

        let one = run_select(&storage, select_body(None, Some(1), None)).await;
        assert_eq!(one.status, 200, "{}", response_text(&one));
        assert_eq!(response_points(&response_json(&one)).len(), 1);
    }

    #[tokio::test]
    async fn desc_rejects_offset_with_400() {
        let storage = make_storage();
        seed_indexed(&storage, 3);

        let response = run_select(&storage, select_body(Some("desc"), Some(1), Some(1))).await;
        assert_eq!(response.status, 400, "{}", response_text(&response));
        assert!(
            response_text(&response).contains("offset"),
            "got {}",
            response_text(&response)
        );
    }

    #[tokio::test]
    async fn invalid_range_is_rejected() {
        let storage = make_storage();
        for (start, end) in [(BASE_TS, BASE_TS), (BASE_TS + STEP, BASE_TS)] {
            let response = run_select(
                &storage,
                json!({
                    "metric": METRIC,
                    "labels": [["device_id", "A"]],
                    "start": start,
                    "end": end,
                    "limit": 10,
                }),
            )
            .await;
            assert_eq!(response.status, 400, "{}", response_text(&response));
            assert!(
                response_text(&response).contains("invalid time range"),
                "got {}",
                response_text(&response)
            );
        }

        // Parse-level validation rejects the request first; assert the storage
        // error mapping (`InvalidTimeRange` -> 400) directly as well.
        let mapped = select_storage_error_response(&tsink::TsinkError::InvalidTimeRange {
            start: 5,
            end: 5,
        });
        assert_eq!(mapped.status, 400, "{}", response_text(&mapped));
        assert!(response_text(&mapped).contains("Invalid timestamp range"));
    }

    #[tokio::test]
    async fn cluster_mode_returns_501() {
        let storage = make_storage();
        let metadata_store = Arc::new(MetricMetadataStore::in_memory());
        let exemplar_store = Arc::new(ExemplarStore::in_memory());
        let engine = Engine::with_precision(Arc::clone(&storage), TimestampPrecision::Milliseconds);
        let cluster_context = make_cluster_context();

        let expected_body = "row endpoints are not supported in cluster mode";
        for path in ["/api/v1/rows", "/api/v1/select", "/api/v1/select_points"] {
            let response = route_request(
                AppContext {
                    storage: &storage,
                    metadata_store: &metadata_store,
                    exemplar_store: &exemplar_store,
                    rules_runtime: None,
                    engine: &engine,
                    cluster_context: Some(&cluster_context),
                    tenant_registry: None,
                    rbac_registry: None,
                    edge_sync_context: None,
                    security_manager: None,
                    usage_accounting: None,
                    managed_control_plane: None,
                },
                RequestContext {
                    request: request(path, json!({})),
                    server_start: Instant::now(),
                    timestamp_precision: TimestampPrecision::Milliseconds,
                    admin_api_enabled: false,
                    admin_path_prefix: None,
                    internal_api: None,
                },
            )
            .await;

            assert_eq!(
                response.status, 501,
                "{path} must fail loudly in cluster mode"
            );
            assert_eq!(
                response_text(&response),
                expected_body,
                "{path} must reuse the shared cluster-mode response verbatim"
            );
            assert!(
                response
                    .headers
                    .iter()
                    .any(|(name, value)| name.eq_ignore_ascii_case("content-type")
                        && value == "text/plain"),
                "{path} must stay text/plain"
            );
        }
    }

    #[tokio::test]
    async fn route_request_dispatches_select_endpoints() {
        let storage = make_storage();
        tenant::scoped_storage(Arc::clone(&storage), "team-a")
            .insert_rows(&[Row::with_labels(
                METRIC,
                device_labels("A"),
                DataPoint::new(BASE_TS, 7.5),
            )])
            .expect("seed write should succeed");
        let registry = tenant::TenantRegistry::from_json_str(r#"{"tenants":{"team-a":{}}}"#)
            .expect("tenant registry should parse");
        let metadata_store = Arc::new(MetricMetadataStore::in_memory());
        let exemplar_store = Arc::new(ExemplarStore::in_memory());
        let engine = Engine::with_precision(Arc::clone(&storage), TimestampPrecision::Milliseconds);

        let cases = [
            (
                "/api/v1/select",
                json!({
                    "metric": METRIC,
                    "labels": [["device_id", "A"]],
                    "start": BASE_TS,
                    "end": BASE_TS + STEP,
                    "order": "desc",
                    "limit": 10,
                }),
            ),
            (
                "/api/v1/select_points",
                json!({
                    "metric": METRIC,
                    "labels": [["device_id", "A"]],
                    "timestamps": [BASE_TS],
                }),
            ),
        ];

        for (path, body) in cases {
            let mut dispatch_request = request(path, body);
            dispatch_request
                .headers
                .insert(tenant::TENANT_HEADER.to_string(), "team-a".to_string());
            let response = route_request(
                AppContext {
                    storage: &storage,
                    metadata_store: &metadata_store,
                    exemplar_store: &exemplar_store,
                    rules_runtime: None,
                    engine: &engine,
                    cluster_context: None,
                    tenant_registry: Some(&registry),
                    rbac_registry: None,
                    edge_sync_context: None,
                    security_manager: None,
                    usage_accounting: None,
                    managed_control_plane: None,
                },
                RequestContext {
                    request: dispatch_request,
                    server_start: Instant::now(),
                    timestamp_precision: TimestampPrecision::Milliseconds,
                    admin_api_enabled: false,
                    admin_path_prefix: None,
                    internal_api: None,
                },
            )
            .await;

            assert_eq!(response.status, 200, "{path}: {}", response_text(&response));
            let body = response_json(&response);
            assert_eq!(body["status"], "success");
            let points = response_points(&body);
            assert_eq!(points.len(), 1, "{path} should return the seeded point");
            assert_eq!(points[0]["timestamp"], BASE_TS);
            assert_eq!(points[0]["value"]["value"], 7.5);
        }
    }

    #[tokio::test]
    async fn select_points_contract() {
        let storage = make_storage();
        seed(
            &storage,
            &[
                (BASE_TS, 1.0),
                (BASE_TS + STEP, 2.0),
                (BASE_TS + 2 * STEP, 3.0),
            ],
        );

        // Unsorted + duplicated input; missing timestamp omitted; output ascending.
        let response = run_select_points(
            &storage,
            json!({
                "metric": METRIC,
                "labels": [["device_id", "A"]],
                "timestamps": [
                    BASE_TS + 2 * STEP,
                    BASE_TS,
                    BASE_TS + 2 * STEP,
                    BASE_TS + 9 * STEP,
                ],
            }),
        )
        .await;
        assert_eq!(response.status, 200, "{}", response_text(&response));
        let body = response_json(&response);
        assert_eq!(body["status"], "success");
        let points = response_points(&body);
        assert_eq!(point_timestamps(points), vec![BASE_TS, BASE_TS + 2 * STEP]);
        assert_eq!(point_values(points), vec![1.0, 3.0]);
        assert_eq!(points[0]["value"]["type"], "f64");

        let empty = run_select_points(
            &storage,
            json!({
                "metric": METRIC,
                "labels": [["device_id", "A"]],
                "timestamps": [],
            }),
        )
        .await;
        assert_eq!(empty.status, 200, "{}", response_text(&empty));
        assert!(response_points(&response_json(&empty)).is_empty());

        let timestamps: Vec<i64> = (0..=MAX_TIMESTAMPS)
            .map(|index| BASE_TS + index as i64)
            .collect();
        let too_many = run_select_points(
            &storage,
            json!({
                "metric": METRIC,
                "labels": [["device_id", "A"]],
                "timestamps": timestamps,
            }),
        )
        .await;
        assert_eq!(too_many.status, 400, "{}", response_text(&too_many));
        assert!(
            response_text(&too_many).contains("timestamps"),
            "got {}",
            response_text(&too_many)
        );
    }

    #[tokio::test]
    async fn histogram_values_are_rejected_with_400() {
        let storage = make_storage();
        let histogram = tsink::NativeHistogram {
            count: None,
            sum: 1.5,
            schema: 0,
            zero_threshold: 0.0,
            zero_count: None,
            negative_spans: Vec::new(),
            negative_deltas: Vec::new(),
            negative_counts: Vec::new(),
            positive_spans: Vec::new(),
            positive_deltas: Vec::new(),
            positive_counts: Vec::new(),
            reset_hint: tsink::HistogramResetHint::Unknown,
            custom_values: Vec::new(),
        };
        storage
            .insert_rows(&[Row::with_labels(
                "hist_metric",
                scoped_labels("A"),
                DataPoint::new(BASE_TS, tsink::Value::from(histogram)),
            )])
            .expect("histogram insert should succeed");

        let range = handle_rows_select(
            &storage,
            &request(
                "/api/v1/select",
                json!({
                    "metric": "hist_metric",
                    "labels": [["device_id", "A"]],
                    "start": BASE_TS,
                    "end": BASE_TS + STEP,
                    "limit": 10,
                }),
            ),
            TimestampPrecision::Milliseconds,
            None,
            &no_read_context(),
        )
        .await;
        assert_eq!(range.status, 400, "{}", response_text(&range));
        assert!(
            response_text(&range).contains("histogram"),
            "got {}",
            response_text(&range)
        );

        let points = handle_rows_select_points(
            &storage,
            &request(
                "/api/v1/select_points",
                json!({
                    "metric": "hist_metric",
                    "labels": [["device_id", "A"]],
                    "timestamps": [BASE_TS],
                }),
            ),
            TimestampPrecision::Milliseconds,
            None,
            &no_read_context(),
        )
        .await;
        assert_eq!(points.status, 400, "{}", response_text(&points));
    }

    #[tokio::test]
    async fn suspended_managed_tenant_is_rejected_on_select_points() {
        let storage = make_storage();
        let control_plane = ManagedControlPlane::open(None).expect("control plane should open");
        let actor = ManagedControlPlaneActor {
            id: "test".to_string(),
            scope: "test".to_string(),
        };
        control_plane
            .provision_deployment(
                actor.clone(),
                ManagedDeploymentProvisionRequest {
                    deployment_id: "prod".to_string(),
                    display_name: Some("Prod".to_string()),
                    region: Some("test-region".to_string()),
                    plan: Some("test-plan".to_string()),
                    lifecycle: Some(DeploymentLifecycleState::Ready),
                    ..ManagedDeploymentProvisionRequest::default()
                },
            )
            .expect("deployment should provision");
        control_plane
            .apply_tenant(
                actor,
                ManagedTenantApplyRequest {
                    tenant_id: "team-a".to_string(),
                    deployment_id: Some("prod".to_string()),
                    lifecycle: Some(TenantLifecycleState::Suspended),
                    ..ManagedTenantApplyRequest::default()
                },
            )
            .expect("tenant should apply");
        let context = PublicReadContext::new(None, None, Some(&control_plane), None);

        for (path, body) in [
            (
                "/api/v1/select_points",
                json!({
                    "metric": METRIC,
                    "labels": [["device_id", "A"]],
                    "timestamps": [BASE_TS],
                }),
            ),
            ("/api/v1/select", select_body(Some("desc"), Some(1), None)),
        ] {
            let mut suspended_request = request(path, body);
            suspended_request
                .headers
                .insert(tenant::TENANT_HEADER.to_string(), "team-a".to_string());
            let response = if path == "/api/v1/select_points" {
                handle_rows_select_points(
                    &storage,
                    &suspended_request,
                    TimestampPrecision::Milliseconds,
                    None,
                    &context,
                )
                .await
            } else {
                handle_rows_select(
                    &storage,
                    &suspended_request,
                    TimestampPrecision::Milliseconds,
                    None,
                    &context,
                )
                .await
            };
            assert_eq!(response.status, 403, "{path}: {}", response_text(&response));
            assert!(
                response_text(&response).contains("suspended"),
                "{path} got {}",
                response_text(&response)
            );
        }
    }

    #[tokio::test]
    async fn tenant_isolation_via_scoped_storage() {
        let storage = make_storage();
        let tenant_a = tenant::scoped_storage(Arc::clone(&storage), "tenant-a");
        tenant_a
            .insert_rows(&[Row::with_labels(
                METRIC,
                device_labels("A"),
                DataPoint::new(BASE_TS, 1.5),
            )])
            .expect("tenant-a write should succeed");

        // The scoped wrapper forwards both new read paths with tenant scoping.
        let a_desc = tenant_a
            .select_desc(METRIC, &device_labels("A"), BASE_TS, BASE_TS + STEP, 10)
            .expect("tenant-a desc read should succeed");
        assert_eq!(a_desc.len(), 1);
        let a_points = tenant_a
            .select_at_timestamps(METRIC, &device_labels("A"), &[BASE_TS])
            .expect("tenant-a point read should succeed");
        assert_eq!(a_points.len(), 1);

        let tenant_b = tenant::scoped_storage(Arc::clone(&storage), "tenant-b");
        assert!(
            tenant_b
                .select_desc(METRIC, &device_labels("A"), BASE_TS, BASE_TS + STEP, 10)
                .expect("tenant-b desc read should succeed")
                .is_empty(),
            "tenant B must not see tenant A data through select_desc"
        );
        assert!(
            tenant_b
                .select_at_timestamps(METRIC, &device_labels("A"), &[BASE_TS])
                .expect("tenant-b point read should succeed")
                .is_empty(),
            "tenant B must not see tenant A data through select_at_timestamps"
        );

        // End-to-end: same storage, tenant selected by header.
        let registry =
            tenant::TenantRegistry::from_json_str(r#"{"tenants":{"tenant-a":{},"tenant-b":{}}}"#)
                .expect("tenant registry should parse");
        let context = PublicReadContext::new(None, Some(&registry), None, None);
        for (tenant_id, expected_len) in [("tenant-a", 1_usize), ("tenant-b", 0_usize)] {
            let mut select_request =
                request("/api/v1/select", select_body(Some("desc"), Some(10), None));
            select_request
                .headers
                .insert(tenant::TENANT_HEADER.to_string(), tenant_id.to_string());
            let response = handle_rows_select(
                &storage,
                &select_request,
                TimestampPrecision::Milliseconds,
                None,
                &context,
            )
            .await;
            assert_eq!(
                response.status,
                200,
                "{tenant_id}: {}",
                response_text(&response)
            );
            assert_eq!(
                response_points(&response_json(&response)).len(),
                expected_len,
                "tenant {tenant_id} select visibility mismatch"
            );

            let mut points_request = request(
                "/api/v1/select_points",
                json!({
                    "metric": METRIC,
                    "labels": [["device_id", "A"]],
                    "timestamps": [BASE_TS],
                }),
            );
            points_request
                .headers
                .insert(tenant::TENANT_HEADER.to_string(), tenant_id.to_string());
            let response = handle_rows_select_points(
                &storage,
                &points_request,
                TimestampPrecision::Milliseconds,
                None,
                &context,
            )
            .await;
            assert_eq!(
                response.status,
                200,
                "{tenant_id}: {}",
                response_text(&response)
            );
            assert_eq!(
                response_points(&response_json(&response)).len(),
                expected_len,
                "tenant {tenant_id} select_points visibility mismatch"
            );
        }
    }

    #[tokio::test]
    async fn tenant_point_quota_bounds_select_limit_and_select_points() {
        let storage = make_storage();
        let registry = tenant::TenantRegistry::from_json_str(
            r#"{
                "tenants": {
                    "team-quota": {
                        "quotas": { "maxRangePointsPerQuery": 2 }
                    }
                }
            }"#,
        )
        .expect("tenant registry should parse");
        let context = PublicReadContext::new(None, Some(&registry), None, None);

        let run = |path: &'static str, body: JsonValue| {
            let storage = Arc::clone(&storage);
            let context = &context;
            async move {
                let mut request = request(path, body);
                request
                    .headers
                    .insert(tenant::TENANT_HEADER.to_string(), "team-quota".to_string());
                match path {
                    "/api/v1/select" => {
                        handle_rows_select(
                            &storage,
                            &request,
                            TimestampPrecision::Milliseconds,
                            None,
                            context,
                        )
                        .await
                    }
                    _ => {
                        handle_rows_select_points(
                            &storage,
                            &request,
                            TimestampPrecision::Milliseconds,
                            None,
                            context,
                        )
                        .await
                    }
                }
            }
        };

        for (path, body) in [
            ("/api/v1/select", select_body(Some("desc"), Some(3), None)),
            (
                "/api/v1/select_points",
                json!({
                    "metric": METRIC,
                    "labels": [["device_id", "A"]],
                    "timestamps": [BASE_TS, BASE_TS + STEP, BASE_TS + 2 * STEP],
                }),
            ),
        ] {
            let response = run(path, body).await;
            assert_eq!(response.status, 400, "{path}: {}", response_text(&response));
            assert!(
                response_text(&response).contains("tenant range query point limit exceeded"),
                "{path} got {}",
                response_text(&response)
            );
        }

        // Within the quota both endpoints are admitted.
        let select_ok = run("/api/v1/select", select_body(Some("desc"), Some(2), None)).await;
        assert_eq!(
            select_ok.status,
            200,
            "select within quota: {}",
            response_text(&select_ok)
        );
        let points_ok = run(
            "/api/v1/select_points",
            json!({
                "metric": METRIC,
                "labels": [["device_id", "A"]],
                "timestamps": [BASE_TS, BASE_TS + STEP],
            }),
        )
        .await;
        assert_eq!(
            points_ok.status,
            200,
            "select_points within quota: {}",
            response_text(&points_ok)
        );
    }
}
