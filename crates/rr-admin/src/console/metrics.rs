use futures::StreamExt;
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::time::Duration;

pub(crate) const MAX_SCRAPE_BYTES: usize = 8 * 1024 * 1024;
pub(crate) const MAX_SERIES: usize = 100_000;

pub(crate) type MetricsResult<T> = std::result::Result<T, MetricsError>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MetricsError {
    pub code: &'static str,
    pub message: String,
}

impl MetricsError {
    fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

impl fmt::Display for MetricsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for MetricsError {}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MetricKind {
    Counter,
    Gauge,
    Histogram,
    Summary,
    Untyped,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct MetricSample {
    pub node_id: u64,
    pub name: String,
    pub labels: BTreeMap<String, String>,
    pub value: f64,
    pub timestamp_ms: Option<i64>,
    pub kind: MetricKind,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct MetricsSnapshot {
    pub node_id: u64,
    pub samples: Vec<MetricSample>,
}

#[derive(Clone)]
pub(crate) struct PrometheusScraper {
    client: reqwest::Client,
}

impl PrometheusScraper {
    pub fn new(timeout: Duration) -> MetricsResult<Self> {
        Ok(Self {
            client: build_http_client(timeout)?,
        })
    }

    pub async fn scrape(&self, node_id: u64, endpoint: &str) -> MetricsResult<MetricsSnapshot> {
        if node_id == 0 {
            return Err(MetricsError::new(
                "INVALID_NODE_ID",
                "a trusted non-zero node ID is required",
            ));
        }
        let url = parse_http_url(endpoint, "INVALID_METRICS_URL")?;
        let response = self
            .client
            .get(url)
            .send()
            .await
            .map_err(|error| request_error(error, "SCRAPE_TIMEOUT", "SCRAPE_FAILED"))?;
        if !response.status().is_success() {
            return Err(MetricsError::new(
                "SCRAPE_HTTP_ERROR",
                format!("metrics endpoint returned HTTP {}", response.status()),
            ));
        }
        let body = read_bounded_body(response, "SCRAPE_TIMEOUT", "SCRAPE_FAILED").await?;
        let text = std::str::from_utf8(&body)
            .map_err(|_| malformed_metrics("metrics response is not valid UTF-8"))?;
        parse_prometheus(node_id, text)
    }
}

pub(crate) fn parse_prometheus(node_id: u64, input: &str) -> MetricsResult<MetricsSnapshot> {
    if node_id == 0 {
        return Err(MetricsError::new(
            "INVALID_NODE_ID",
            "a trusted non-zero node ID is required",
        ));
    }
    if input.len() > MAX_SCRAPE_BYTES {
        return Err(MetricsError::new(
            "METRICS_BODY_TOO_LARGE",
            format!("metrics input exceeds {MAX_SCRAPE_BYTES} bytes"),
        ));
    }

    let mut types = BTreeMap::new();
    for (index, line) in input.lines().enumerate() {
        let line = line.trim();
        if line.starts_with("# TYPE") {
            let rest = line["# TYPE".len()..].trim();
            let fields = rest.split_whitespace().collect::<Vec<_>>();
            if fields.len() != 2 || !valid_metric_name(fields[0]) {
                return Err(malformed_metrics(format!(
                    "invalid TYPE declaration on line {}",
                    index + 1
                )));
            }
            let kind = match fields[1] {
                "counter" => MetricKind::Counter,
                "gauge" => MetricKind::Gauge,
                "histogram" => MetricKind::Histogram,
                "summary" => MetricKind::Summary,
                "untyped" => MetricKind::Untyped,
                _ => {
                    return Err(malformed_metrics(format!(
                        "unknown metric type on line {}",
                        index + 1
                    )))
                }
            };
            if types.insert(fields[0].to_string(), kind).is_some() {
                return Err(malformed_metrics(format!(
                    "duplicate TYPE declaration on line {}",
                    index + 1
                )));
            }
        }
    }

    let mut samples = Vec::new();
    for (index, line) in input.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if samples.len() == MAX_SERIES {
            return Err(MetricsError::new(
                "TOO_MANY_SERIES",
                format!("metrics input exceeds {MAX_SERIES} series"),
            ));
        }
        let mut sample = parse_sample_line(line, &types)
            .map_err(|message| malformed_metrics(format!("line {}: {message}", index + 1)))?;
        sample.node_id = node_id;
        sample.labels.remove("node_id");
        samples.push(sample);
    }
    Ok(MetricsSnapshot { node_id, samples })
}

fn parse_sample_line(
    line: &str,
    types: &BTreeMap<String, MetricKind>,
) -> std::result::Result<MetricSample, String> {
    let bytes = line.as_bytes();
    let mut cursor = 0;
    while cursor < bytes.len() && metric_name_byte(bytes[cursor]) {
        cursor += 1;
    }
    let name = &line[..cursor];
    if !valid_metric_name(name) {
        return Err("invalid metric name".to_string());
    }
    let mut labels = BTreeMap::new();
    if cursor < bytes.len() && bytes[cursor] == b'{' {
        labels = parse_labels(line, &mut cursor)?;
    }
    if cursor < bytes.len() && !matches!(bytes[cursor], b' ' | b'\t') {
        return Err("expected whitespace before the sample value".to_string());
    }
    let fields = line[cursor..].split_whitespace().collect::<Vec<_>>();
    if !(1..=2).contains(&fields.len()) {
        return Err("expected a value and optional timestamp".to_string());
    }
    let value = parse_prometheus_float(fields[0])
        .ok_or_else(|| format!("invalid sample value {:?}", fields[0]))?;
    let timestamp_ms = fields
        .get(1)
        .map(|timestamp| {
            timestamp
                .parse::<i64>()
                .map_err(|_| format!("invalid timestamp {timestamp:?}"))
        })
        .transpose()?;
    let kind = metric_kind(name, types);
    Ok(MetricSample {
        node_id: 0,
        name: name.to_string(),
        labels,
        value,
        timestamp_ms,
        kind,
    })
}

fn parse_labels(
    line: &str,
    cursor: &mut usize,
) -> std::result::Result<BTreeMap<String, String>, String> {
    let bytes = line.as_bytes();
    *cursor += 1;
    let mut labels = BTreeMap::new();
    loop {
        skip_whitespace(bytes, cursor);
        if *cursor < bytes.len() && bytes[*cursor] == b'}' {
            *cursor += 1;
            return Ok(labels);
        }
        let start = *cursor;
        while *cursor < bytes.len() && label_name_byte(bytes[*cursor]) {
            *cursor += 1;
        }
        let name = &line[start..*cursor];
        if !valid_label_name(name) {
            return Err("invalid label name".to_string());
        }
        skip_whitespace(bytes, cursor);
        if *cursor >= bytes.len() || bytes[*cursor] != b'=' {
            return Err(format!("label {name:?} is missing ="));
        }
        *cursor += 1;
        skip_whitespace(bytes, cursor);
        if *cursor >= bytes.len() || bytes[*cursor] != b'"' {
            return Err(format!("label {name:?} is missing a quoted value"));
        }
        *cursor += 1;
        let mut value = String::new();
        let mut segment_start = *cursor;
        let value = loop {
            if *cursor >= bytes.len() {
                return Err(format!("label {name:?} has an unterminated value"));
            }
            match bytes[*cursor] {
                b'"' => {
                    value.push_str(&line[segment_start..*cursor]);
                    *cursor += 1;
                    break value;
                }
                b'\\' => {
                    value.push_str(&line[segment_start..*cursor]);
                    *cursor += 1;
                    if *cursor >= bytes.len() {
                        return Err(format!("label {name:?} has an incomplete escape"));
                    }
                    match bytes[*cursor] {
                        b'n' => value.push('\n'),
                        b'"' => value.push('"'),
                        b'\\' => value.push('\\'),
                        _ => return Err(format!("label {name:?} has an invalid escape")),
                    }
                    *cursor += 1;
                    segment_start = *cursor;
                }
                _ => *cursor += 1,
            }
        };
        if labels.insert(name.to_string(), value).is_some() {
            return Err(format!("duplicate label {name:?}"));
        }
        skip_whitespace(bytes, cursor);
        if *cursor >= bytes.len() {
            return Err("unterminated label set".to_string());
        }
        match bytes[*cursor] {
            b',' => *cursor += 1,
            b'}' => {
                *cursor += 1;
                return Ok(labels);
            }
            _ => return Err("expected , or } after a label value".to_string()),
        }
    }
}

fn metric_kind(name: &str, types: &BTreeMap<String, MetricKind>) -> MetricKind {
    if let Some(kind) = types.get(name) {
        return *kind;
    }
    for (family, kind) in types {
        if matches!(kind, MetricKind::Histogram | MetricKind::Summary)
            && ["_bucket", "_sum", "_count"]
                .iter()
                .any(|suffix| name.strip_prefix(family) == Some(suffix))
        {
            return *kind;
        }
    }
    MetricKind::Untyped
}

fn valid_metric_name(name: &str) -> bool {
    let mut bytes = name.bytes();
    bytes
        .next()
        .is_some_and(|byte| byte.is_ascii_alphabetic() || matches!(byte, b'_' | b':'))
        && bytes.all(metric_name_byte)
}

fn metric_name_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b':')
}

fn valid_label_name(name: &str) -> bool {
    let mut bytes = name.bytes();
    bytes
        .next()
        .is_some_and(|byte| byte.is_ascii_alphabetic() || byte == b'_')
        && bytes.all(label_name_byte)
}

fn label_name_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

fn skip_whitespace(bytes: &[u8], cursor: &mut usize) {
    while *cursor < bytes.len() && matches!(bytes[*cursor], b' ' | b'\t') {
        *cursor += 1;
    }
}

fn parse_prometheus_float(input: &str) -> Option<f64> {
    match input {
        "+Inf" | "Inf" => Some(f64::INFINITY),
        "-Inf" => Some(f64::NEG_INFINITY),
        "NaN" => Some(f64::NAN),
        _ => input.parse().ok(),
    }
}

fn malformed_metrics(message: impl Into<String>) -> MetricsError {
    MetricsError::new("MALFORMED_METRICS", message)
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct CounterKey {
    node_id: u64,
    name: String,
    labels: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct CounterDelta {
    pub node_id: u64,
    pub name: String,
    pub labels: BTreeMap<String, String>,
    pub delta: Option<f64>,
    pub reset: bool,
}

#[derive(Default)]
pub(crate) struct CounterDeltas {
    previous: BTreeMap<CounterKey, f64>,
}

impl CounterDeltas {
    pub fn observe_node(
        &mut self,
        node_id: u64,
        samples: &[MetricSample],
    ) -> MetricsResult<Vec<CounterDelta>> {
        if node_id == 0 || samples.iter().any(|sample| sample.node_id != node_id) {
            return Err(MetricsError::new(
                "NODE_ID_MISMATCH",
                "counter samples must belong to the trusted node being observed",
            ));
        }
        let mut next = BTreeMap::new();
        let mut deltas = Vec::new();
        for sample in samples
            .iter()
            .filter(|sample| sample.kind == MetricKind::Counter)
        {
            if !sample.value.is_finite() || sample.value < 0.0 {
                return Err(MetricsError::new(
                    "INVALID_COUNTER",
                    format!("counter {} has an invalid value", sample.name),
                ));
            }
            let key = CounterKey {
                node_id,
                name: sample.name.clone(),
                labels: sample.labels.clone(),
            };
            let previous = self.previous.get(&key).copied();
            let reset = previous.is_some_and(|previous| sample.value < previous);
            let delta = previous.map(|previous| {
                if reset {
                    sample.value
                } else {
                    sample.value - previous
                }
            });
            next.insert(key.clone(), sample.value);
            deltas.push(CounterDelta {
                node_id,
                name: sample.name.clone(),
                labels: sample.labels.clone(),
                delta,
                reset,
            });
        }
        self.previous.retain(|key, _| key.node_id != node_id);
        self.previous.extend(next);
        Ok(deltas)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct HistogramBucket {
    pub upper_bound: f64,
    pub count: f64,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct AggregatedHistogram {
    pub name: String,
    pub labels: BTreeMap<String, String>,
    pub buckets: Vec<HistogramBucket>,
    pub count: f64,
    pub sum: f64,
}

impl AggregatedHistogram {
    pub fn quantile(&self, quantile: f64) -> MetricsResult<Option<f64>> {
        if !quantile.is_finite() || !(0.0..=1.0).contains(&quantile) {
            return Err(MetricsError::new(
                "INVALID_QUANTILE",
                "histogram quantile must be between zero and one",
            ));
        }
        if self.count == 0.0 {
            return Ok(None);
        }
        let rank = quantile * self.count;
        let Some(index) = self
            .buckets
            .iter()
            .position(|bucket| bucket.count > 0.0 && bucket.count >= rank)
        else {
            return Ok(None);
        };
        let bucket = &self.buckets[index];
        if bucket.upper_bound.is_infinite() {
            return Ok(self
                .buckets
                .iter()
                .rev()
                .skip(1)
                .find(|bucket| bucket.upper_bound.is_finite())
                .map(|bucket| bucket.upper_bound));
        }
        if index == 0 && bucket.upper_bound <= 0.0 {
            return Ok(Some(bucket.upper_bound));
        }
        let (lower_bound, lower_count) = index
            .checked_sub(1)
            .map(|previous| {
                (
                    self.buckets[previous].upper_bound,
                    self.buckets[previous].count,
                )
            })
            .unwrap_or((0.0, 0.0));
        let width = bucket.upper_bound - lower_bound;
        let bucket_count = bucket.count - lower_count;
        if bucket_count <= 0.0 {
            return Ok(None);
        }
        Ok(Some(
            lower_bound + width * ((rank - lower_count) / bucket_count),
        ))
    }
}

#[derive(Default)]
struct HistogramBuilder {
    buckets: Vec<(f64, f64)>,
    count: Option<f64>,
    sum: Option<f64>,
}

pub(crate) fn aggregate_histograms(
    samples: &[MetricSample],
) -> MetricsResult<Vec<AggregatedHistogram>> {
    let mut builders: BTreeMap<(String, BTreeMap<String, String>), HistogramBuilder> =
        BTreeMap::new();
    for sample in samples
        .iter()
        .filter(|sample| sample.kind == MetricKind::Histogram)
    {
        let Some((base, suffix)) = histogram_suffix(&sample.name) else {
            continue;
        };
        if !sample.value.is_finite() || (suffix != "_sum" && sample.value < 0.0) {
            return Err(malformed_histogram(format!(
                "histogram sample {} has an invalid value",
                sample.name
            )));
        }
        let mut labels = sample.labels.clone();
        labels.remove("le");
        let builder = builders.entry((base.to_string(), labels)).or_default();
        match suffix {
            "_bucket" => {
                let bound = sample
                    .labels
                    .get("le")
                    .and_then(|value| parse_prometheus_float(value))
                    .filter(|value| !value.is_nan())
                    .ok_or_else(|| {
                        malformed_histogram(format!("{} has an invalid le label", sample.name))
                    })?;
                if let Some((_, count)) = builder
                    .buckets
                    .iter_mut()
                    .find(|(upper_bound, _)| upper_bound.to_bits() == bound.to_bits())
                {
                    *count += sample.value;
                } else {
                    builder.buckets.push((bound, sample.value));
                }
            }
            "_count" => builder.count = Some(builder.count.unwrap_or(0.0) + sample.value),
            "_sum" => builder.sum = Some(builder.sum.unwrap_or(0.0) + sample.value),
            _ => unreachable!(),
        }
    }

    builders
        .into_iter()
        .map(|((name, labels), mut builder)| {
            builder.buckets.sort_by(|a, b| a.0.total_cmp(&b.0));
            let count = builder.count.ok_or_else(|| {
                malformed_histogram(format!("histogram {name} has no count series"))
            })?;
            let sum = builder.sum.ok_or_else(|| {
                malformed_histogram(format!("histogram {name} has no sum series"))
            })?;
            if builder.buckets.is_empty()
                || !builder
                    .buckets
                    .last()
                    .is_some_and(|(bound, _)| *bound == f64::INFINITY)
            {
                return Err(malformed_histogram(format!(
                    "histogram {name} has no +Inf bucket"
                )));
            }
            let mut previous = 0.0;
            for (bound, bucket_count) in &builder.buckets {
                if *bucket_count < previous {
                    return Err(malformed_histogram(format!(
                        "histogram {name} bucket counts are not cumulative"
                    )));
                }
                previous = *bucket_count;
                if bound.is_nan() {
                    return Err(malformed_histogram(format!(
                        "histogram {name} has a NaN bucket"
                    )));
                }
            }
            if (previous - count).abs() > f64::EPSILON * count.abs().max(1.0) {
                return Err(malformed_histogram(format!(
                    "histogram {name} count does not match its +Inf bucket"
                )));
            }
            Ok(AggregatedHistogram {
                name,
                labels,
                buckets: builder
                    .buckets
                    .into_iter()
                    .map(|(upper_bound, count)| HistogramBucket { upper_bound, count })
                    .collect(),
                count,
                sum,
            })
        })
        .collect()
}

fn histogram_suffix(name: &str) -> Option<(&str, &str)> {
    ["_bucket", "_count", "_sum"]
        .iter()
        .find_map(|suffix| name.strip_suffix(suffix).map(|base| (base, *suffix)))
}

fn malformed_histogram(message: impl Into<String>) -> MetricsError {
    MetricsError::new("MALFORMED_HISTOGRAM", message)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ClusterMatcher {
    pub label: String,
    pub value: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum HistoryFamily {
    ServerRequestDuration,
    ProposalDuration,
    ApplyDuration,
    AppendEntriesRtt,
    SnapshotTransferDuration,
    ServerRequests,
    AppliedEntries,
    IngestorCommands,
    SnapshotTransferBytes,
    SnapshotInstalls,
    ReplicationLag,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HistoryTemplate {
    Histogram,
    CounterRate,
    Gauge,
}

impl HistoryFamily {
    fn raw_family(self) -> &'static str {
        match self {
            Self::ServerRequestDuration => "rr_server_request_duration_seconds",
            Self::ProposalDuration => "rr_proposal_duration_seconds",
            Self::ApplyDuration => "rr_raft_apply_duration_seconds",
            Self::AppendEntriesRtt => "rr_raft_append_entries_rtt_seconds",
            Self::SnapshotTransferDuration => "rr_snapshot_transfer_duration_seconds",
            Self::ServerRequests => "rr_server_requests_total",
            Self::AppliedEntries => "rr_raft_applied_entries_total",
            Self::IngestorCommands => "rr_ingestor_commands_committed_total",
            Self::SnapshotTransferBytes => "rr_snapshot_transfer_bytes",
            Self::SnapshotInstalls => "rr_snapshot_installs_total",
            Self::ReplicationLag => "rr_raft_replication_lag",
        }
    }

    fn template(self) -> HistoryTemplate {
        match self {
            Self::ServerRequestDuration
            | Self::ProposalDuration
            | Self::ApplyDuration
            | Self::AppendEntriesRtt
            | Self::SnapshotTransferDuration => HistoryTemplate::Histogram,
            Self::ServerRequests
            | Self::AppliedEntries
            | Self::IngestorCommands
            | Self::SnapshotTransferBytes
            | Self::SnapshotInstalls => HistoryTemplate::CounterRate,
            Self::ReplicationLag => HistoryTemplate::Gauge,
        }
    }

    fn aggregation_labels(self) -> &'static [&'static str] {
        match self {
            Self::ServerRequestDuration => &["group_id", "op"],
            Self::ProposalDuration => &["group_id", "source"],
            Self::ApplyDuration => &["group_id"],
            Self::AppendEntriesRtt => &["group_id", "follower_node_id", "kind"],
            Self::SnapshotTransferDuration => &["group_id", "direction"],
            _ => &[],
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct HistoryQuery {
    pub family: HistoryFamily,
    pub start: f64,
    pub end: f64,
    pub step: Duration,
    pub rate_window: Duration,
    pub node_ids: Vec<u64>,
    pub group_id: Option<u64>,
    pub quantile: Option<f64>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct HistoryPoint {
    pub timestamp: f64,
    pub value: f64,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct HistorySeries {
    pub node_id: u64,
    pub labels: BTreeMap<String, String>,
    pub points: Vec<HistoryPoint>,
}

#[derive(Clone)]
pub(crate) struct PrometheusHistoryClient {
    client: reqwest::Client,
    base_url: reqwest::Url,
    cluster: ClusterMatcher,
}

impl PrometheusHistoryClient {
    pub fn new(base_url: &str, cluster: ClusterMatcher, timeout: Duration) -> MetricsResult<Self> {
        if !valid_label_name(&cluster.label) || cluster.value.is_empty() {
            return Err(MetricsError::new(
                "INVALID_CLUSTER_MATCHER",
                "history requires a non-empty exact Prometheus cluster label matcher",
            ));
        }
        Ok(Self {
            client: build_http_client(timeout)?,
            base_url: parse_http_url(base_url, "INVALID_PROMETHEUS_URL")?,
            cluster,
        })
    }

    pub fn validate_query(&self, query: &HistoryQuery) -> MetricsResult<()> {
        if !query.start.is_finite()
            || !query.end.is_finite()
            || query.end < query.start
            || query.step.is_zero()
            || query.rate_window.is_zero()
            || query.node_ids.contains(&0)
        {
            return Err(invalid_history_query());
        }
        match query.family.template() {
            HistoryTemplate::Histogram => {
                if query.quantile.is_none_or(|quantile| {
                    !quantile.is_finite() || !(0.0..=1.0).contains(&quantile)
                }) {
                    return Err(invalid_history_query());
                }
            }
            HistoryTemplate::CounterRate | HistoryTemplate::Gauge => {
                if query.quantile.is_some() {
                    return Err(invalid_history_query());
                }
            }
        }
        Ok(())
    }

    pub async fn query_range(&self, query: HistoryQuery) -> MetricsResult<Vec<HistorySeries>> {
        self.validate_query(&query)?;
        let promql = self.render_query(&query);
        let mut url = self.base_url.clone();
        url.set_path("/api/v1/query_range");
        url.set_query(None);
        let response = self
            .client
            .get(url)
            .query(&[
                ("query", promql),
                ("start", format_timestamp(query.start)),
                ("end", format_timestamp(query.end)),
                ("step", query.step.as_secs().to_string()),
            ])
            .send()
            .await
            .map_err(|error| request_error(error, "HISTORY_TIMEOUT", "HISTORY_FAILED"))?;
        if !response.status().is_success() {
            return Err(MetricsError::new(
                "HISTORY_HTTP_ERROR",
                format!("Prometheus returned HTTP {}", response.status()),
            ));
        }
        let body = read_bounded_body(response, "HISTORY_TIMEOUT", "HISTORY_FAILED").await?;
        let response: PrometheusResponse = serde_json::from_slice(&body).map_err(|_| {
            MetricsError::new(
                "MALFORMED_PROMETHEUS_RESPONSE",
                "Prometheus response is not a valid query_range JSON document",
            )
        })?;
        if response.status != "success" {
            return Err(MetricsError::new(
                "PROMETHEUS_ERROR",
                response
                    .error
                    .unwrap_or_else(|| "Prometheus rejected the fixed history query".to_string()),
            ));
        }
        let data = response.data.ok_or_else(|| {
            MetricsError::new(
                "MALFORMED_PROMETHEUS_RESPONSE",
                "Prometheus success response has no data",
            )
        })?;
        if data.result_type != "matrix" {
            return Err(MetricsError::new(
                "MALFORMED_PROMETHEUS_RESPONSE",
                "Prometheus query_range did not return a matrix",
            ));
        }
        if data.result.len() > MAX_SERIES {
            return Err(MetricsError::new(
                "TOO_MANY_SERIES",
                format!("Prometheus response exceeds {MAX_SERIES} series"),
            ));
        }

        let mut instances = BTreeMap::new();
        let mut series = Vec::with_capacity(data.result.len());
        for result in data.result {
            if result.metric.get(&self.cluster.label) != Some(&self.cluster.value) {
                return Err(MetricsError::new(
                    "CLUSTER_MISMATCH",
                    "Prometheus returned a series outside the configured exact cluster matcher",
                ));
            }
            let node_id = result
                .metric
                .get("node_id")
                .and_then(|node_id| node_id.parse::<u64>().ok())
                .filter(|node_id| *node_id != 0)
                .ok_or_else(|| {
                    MetricsError::new(
                        "UNMAPPED",
                        "Prometheus series has no stable non-zero node_id label",
                    )
                })?;
            if let Some(instance) = result.metric.get("instance") {
                if instances
                    .insert(node_id, instance.clone())
                    .is_some_and(|previous| previous != *instance)
                {
                    return Err(MetricsError::new(
                        "UNMAPPED",
                        format!("node_id {node_id} maps to multiple Prometheus instances"),
                    ));
                }
            }
            let points = result
                .values
                .into_iter()
                .map(|(timestamp, value)| {
                    let value = parse_prometheus_float(&value).ok_or_else(|| {
                        MetricsError::new(
                            "MALFORMED_PROMETHEUS_RESPONSE",
                            "Prometheus returned an invalid sample value",
                        )
                    })?;
                    if !timestamp.is_finite() {
                        return Err(MetricsError::new(
                            "MALFORMED_PROMETHEUS_RESPONSE",
                            "Prometheus returned an invalid sample timestamp",
                        ));
                    }
                    Ok(HistoryPoint { timestamp, value })
                })
                .collect::<MetricsResult<Vec<_>>>()?;
            series.push(HistorySeries {
                node_id,
                labels: result.metric,
                points,
            });
        }
        Ok(series)
    }

    fn render_query(&self, query: &HistoryQuery) -> String {
        let mut selectors = vec![format!(
            "{}=\"{}\"",
            self.cluster.label,
            escape_prom_label(&self.cluster.value)
        )];
        let nodes = query.node_ids.iter().copied().collect::<BTreeSet<_>>();
        if nodes.len() == 1 {
            selectors.push(format!("node_id=\"{}\"", nodes.first().unwrap()));
        } else if !nodes.is_empty() {
            let nodes = nodes
                .iter()
                .map(u64::to_string)
                .collect::<Vec<_>>()
                .join("|");
            selectors.push(format!("node_id=~\"{nodes}\""));
        }
        if let Some(group_id) = query.group_id {
            selectors.push(format!("group_id=\"{group_id}\""));
        }
        let selector = selectors.join(",");
        match query.family.template() {
            HistoryTemplate::CounterRate => format!(
                "rate({}{{{selector}}}[{}s])",
                query.family.raw_family(),
                query.rate_window.as_secs()
            ),
            HistoryTemplate::Gauge => format!("{}{{{selector}}}", query.family.raw_family()),
            HistoryTemplate::Histogram => {
                let labels = ["le", "node_id", "instance", self.cluster.label.as_str()]
                    .into_iter()
                    .chain(query.family.aggregation_labels().iter().copied())
                    .collect::<Vec<_>>()
                    .join(",");
                format!(
                    "histogram_quantile({},sum by ({labels}) (rate({}_bucket{{{selector}}}[{}s])))",
                    query.quantile.unwrap(),
                    query.family.raw_family(),
                    query.rate_window.as_secs()
                )
            }
        }
    }
}

#[derive(Deserialize)]
struct PrometheusResponse {
    status: String,
    #[serde(default)]
    data: Option<PrometheusData>,
    #[serde(default)]
    error: Option<String>,
}

#[derive(Deserialize)]
struct PrometheusData {
    #[serde(rename = "resultType")]
    result_type: String,
    result: Vec<PrometheusSeries>,
}

#[derive(Deserialize)]
struct PrometheusSeries {
    metric: BTreeMap<String, String>,
    values: Vec<(f64, String)>,
}

fn invalid_history_query() -> MetricsError {
    MetricsError::new(
        "INVALID_HISTORY_QUERY",
        "history query controls are invalid for the selected fixed template",
    )
}

fn format_timestamp(timestamp: f64) -> String {
    if timestamp.fract() == 0.0 {
        format!("{timestamp:.0}")
    } else {
        timestamp.to_string()
    }
}

fn escape_prom_label(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

fn build_http_client(timeout: Duration) -> MetricsResult<reqwest::Client> {
    if timeout.is_zero() {
        return Err(MetricsError::new(
            "INVALID_TIMEOUT",
            "HTTP timeout must be greater than zero",
        ));
    }
    reqwest::Client::builder()
        .connect_timeout(timeout)
        .timeout(timeout)
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|error| MetricsError::new("HTTP_CLIENT_ERROR", error.to_string()))
}

fn parse_http_url(input: &str, code: &'static str) -> MetricsResult<reqwest::Url> {
    let url = reqwest::Url::parse(input)
        .map_err(|_| MetricsError::new(code, "invalid HTTP endpoint URL"))?;
    if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
        return Err(MetricsError::new(
            code,
            "endpoint must be an absolute HTTP or HTTPS URL",
        ));
    }
    Ok(url)
}

async fn read_bounded_body(
    response: reqwest::Response,
    timeout_code: &'static str,
    failure_code: &'static str,
) -> MetricsResult<Vec<u8>> {
    if response
        .content_length()
        .is_some_and(|length| length > MAX_SCRAPE_BYTES as u64)
    {
        return Err(body_too_large());
    }
    let mut body = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| request_error(error, timeout_code, failure_code))?;
        if body.len().saturating_add(chunk.len()) > MAX_SCRAPE_BYTES {
            return Err(body_too_large());
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

fn body_too_large() -> MetricsError {
    MetricsError::new(
        "METRICS_BODY_TOO_LARGE",
        format!("HTTP response exceeds {MAX_SCRAPE_BYTES} bytes"),
    )
}

fn request_error(
    error: reqwest::Error,
    timeout_code: &'static str,
    failure_code: &'static str,
) -> MetricsError {
    if error.is_timeout() {
        MetricsError::new(timeout_code, "upstream HTTP request timed out")
    } else {
        MetricsError::new(failure_code, "upstream HTTP request failed")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::extract::Query;
    use axum::http::{header::CONTENT_LENGTH, StatusCode};
    use axum::response::Response;
    use axum::routing::get;
    use axum::{Json, Router};
    use serde_json::{json, Value};
    use std::collections::HashMap;
    use std::convert::Infallible;
    use std::time::Duration;
    use tokio::net::TcpListener;
    use tokio::sync::mpsc;

    async fn serve(app: Router) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{address}")
    }

    fn counter_snapshot(node_id: u64, value: u64) -> MetricsSnapshot {
        parse_prometheus(
            node_id,
            &format!(
                "# TYPE rr_server_requests_total counter\nrr_server_requests_total{{group_id=\"7\",op=\"write\"}} {value}\n"
            ),
        )
        .unwrap()
    }

    fn history_query(family: HistoryFamily) -> HistoryQuery {
        HistoryQuery {
            family,
            start: 1_700_000_000.0,
            end: 1_700_000_060.0,
            step: Duration::from_secs(15),
            rate_window: Duration::from_secs(300),
            node_ids: vec![2, 1, 2],
            group_id: Some(7),
            quantile: None,
        }
    }

    fn history_client(base_url: &str) -> PrometheusHistoryClient {
        PrometheusHistoryClient::new(
            base_url,
            ClusterMatcher {
                label: "cluster_id".to_string(),
                value: "prod-east".to_string(),
            },
            Duration::from_secs(2),
        )
        .unwrap()
    }

    #[test]
    fn parser_attaches_trusted_node_id_and_decodes_prometheus_labels() {
        let snapshot = parse_prometheus(
            7,
            r#"# TYPE rr_node_cpu_usage_ratio gauge
rr_node_cpu_usage_ratio{node_id="999",path="/a\n\"b\"\\c"} 0.75 1700000000123
# TYPE rr_server_request_duration_seconds histogram
rr_server_request_duration_seconds_bucket{group_id="7",op="write",le="0.1"} 4
rr_server_request_duration_seconds_bucket{group_id="7",op="write",le="+Inf"} 8
rr_server_request_duration_seconds_sum{group_id="7",op="write"} 2.5
rr_server_request_duration_seconds_count{group_id="7",op="write"} 8
"#,
        )
        .unwrap();

        assert_eq!(snapshot.samples.len(), 5);
        assert!(snapshot.samples.iter().all(|sample| sample.node_id == 7));
        assert!(!snapshot.samples[0].labels.contains_key("node_id"));
        assert_eq!(snapshot.samples[0].labels["path"], "/a\n\"b\"\\c");
        assert_eq!(snapshot.samples[0].timestamp_ms, Some(1_700_000_000_123));
        assert_eq!(snapshot.samples[1].kind, MetricKind::Histogram);
        assert_eq!(snapshot.samples[2].labels["le"], "+Inf");
        assert_eq!(snapshot.samples[2].value, 8.0);
    }

    #[test]
    fn parser_accepts_prometheus_special_floats() {
        let snapshot = parse_prometheus(
            1,
            "positive +Inf\nnegative -Inf\nnot_a_number NaN\nnegative_zero -0\n",
        )
        .unwrap();
        assert_eq!(snapshot.samples[0].value, f64::INFINITY);
        assert_eq!(snapshot.samples[1].value, f64::NEG_INFINITY);
        assert!(snapshot.samples[2].value.is_nan());
        assert_eq!(snapshot.samples[3].value.to_bits(), (-0.0_f64).to_bits());
    }

    #[test]
    fn malformed_text_is_rejected_instead_of_partially_accepted() {
        for input in [
            "9invalid 1\n",
            "metric{bad-label=\"x\"} 1\n",
            "metric{label=\"unterminated} 1\n",
            "metric{label=\"bad\\q\"} 1\n",
            "metric{label=\"a\",label=\"b\"} 1\n",
            "metric not-a-number\n",
            "metric 1 not-a-timestamp\n",
            "# TYPE metric not-a-type\nmetric 1\n",
        ] {
            let error = parse_prometheus(1, input).unwrap_err();
            assert_eq!(error.code, "MALFORMED_METRICS", "accepted {input:?}");
        }
    }

    #[test]
    fn input_byte_cap_is_enforced_before_parsing() {
        let input = " ".repeat(MAX_SCRAPE_BYTES + 1);
        let error = parse_prometheus(1, &input).unwrap_err();
        assert_eq!(error.code, "METRICS_BODY_TOO_LARGE");
    }

    #[test]
    fn series_cap_counts_histogram_buckets_as_input_series() {
        let input = (0..=MAX_SERIES)
            .map(|index| format!("metric_{index} 1\n"))
            .collect::<String>();
        let error = parse_prometheus(1, &input).unwrap_err();
        assert_eq!(error.code, "TOO_MANY_SERIES");
    }

    #[test]
    fn counter_deltas_start_unknown_and_treat_decreases_as_resets() {
        let mut deltas = CounterDeltas::default();
        let first = deltas
            .observe_node(1, &counter_snapshot(1, 10).samples)
            .unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].delta, None);
        assert!(!first[0].reset);

        let next = deltas
            .observe_node(1, &counter_snapshot(1, 15).samples)
            .unwrap();
        assert_eq!(next[0].delta, Some(5.0));
        assert!(!next[0].reset);

        let reset = deltas
            .observe_node(1, &counter_snapshot(1, 4).samples)
            .unwrap();
        assert_eq!(reset[0].delta, Some(4.0));
        assert!(reset[0].reset);
    }

    #[test]
    fn counter_state_is_replaced_for_a_node_after_each_complete_scrape() {
        let mut deltas = CounterDeltas::default();
        deltas
            .observe_node(1, &counter_snapshot(1, 10).samples)
            .unwrap();
        deltas.observe_node(1, &[]).unwrap();
        let first = deltas
            .observe_node(1, &counter_snapshot(1, 3).samples)
            .unwrap();
        assert_eq!(first[0].delta, None);
        assert!(!first[0].reset);
    }

    #[test]
    fn histogram_buckets_are_aggregated_before_quantiles() {
        let first = parse_prometheus(
            1,
            r#"# TYPE rr_server_request_duration_seconds histogram
rr_server_request_duration_seconds_bucket{group_id="7",op="write",le="0.1"} 4
rr_server_request_duration_seconds_bucket{group_id="7",op="write",le="0.5"} 8
rr_server_request_duration_seconds_bucket{group_id="7",op="write",le="+Inf"} 8
rr_server_request_duration_seconds_sum{group_id="7",op="write"} 4
rr_server_request_duration_seconds_count{group_id="7",op="write"} 8
"#,
        )
        .unwrap();
        let second = parse_prometheus(
            2,
            r#"# TYPE rr_server_request_duration_seconds histogram
rr_server_request_duration_seconds_bucket{group_id="7",op="write",le="0.1"} 6
rr_server_request_duration_seconds_bucket{group_id="7",op="write",le="0.5"} 12
rr_server_request_duration_seconds_bucket{group_id="7",op="write",le="+Inf"} 12
rr_server_request_duration_seconds_sum{group_id="7",op="write"} 6
rr_server_request_duration_seconds_count{group_id="7",op="write"} 12
"#,
        )
        .unwrap();
        let samples = [first.samples, second.samples].concat();
        let histograms = aggregate_histograms(&samples).unwrap();
        assert_eq!(histograms.len(), 1);
        assert_eq!(histograms[0].count, 20.0);
        assert_eq!(histograms[0].sum, 10.0);
        assert_eq!(histograms[0].buckets[0].upper_bound, 0.1);
        assert_eq!(histograms[0].buckets[0].count, 10.0);
        assert_eq!(histograms[0].quantile(0.50).unwrap(), Some(0.1));
        let p95 = histograms[0].quantile(0.95).unwrap().unwrap();
        assert!((p95 - 0.46).abs() < 1e-9, "p95={p95}");
        let p99 = histograms[0].quantile(0.99).unwrap().unwrap();
        assert!((p99 - 0.492).abs() < 1e-9, "p99={p99}");
    }

    #[test]
    fn histogram_sum_may_be_negative_when_observations_are_negative() {
        let snapshot = parse_prometheus(
            1,
            r#"# TYPE temperature_delta histogram
temperature_delta_bucket{le="-1"} 1
temperature_delta_bucket{le="+Inf"} 1
temperature_delta_sum -2
temperature_delta_count 1
"#,
        )
        .unwrap();
        let histograms = aggregate_histograms(&snapshot.samples).unwrap();
        assert_eq!(histograms[0].sum, -2.0);
        assert_eq!(histograms[0].quantile(0.5).unwrap(), Some(-1.0));
    }

    #[test]
    fn malformed_histogram_is_not_turned_into_a_quantile() {
        let snapshot = parse_prometheus(
            1,
            r#"# TYPE rr_proposal_duration_seconds histogram
rr_proposal_duration_seconds_bucket{group_id="7",le="0.1"} 9
rr_proposal_duration_seconds_bucket{group_id="7",le="0.5"} 4
rr_proposal_duration_seconds_bucket{group_id="7",le="+Inf"} 9
rr_proposal_duration_seconds_count{group_id="7"} 9
"#,
        )
        .unwrap();
        assert_eq!(
            aggregate_histograms(&snapshot.samples).unwrap_err().code,
            "MALFORMED_HISTOGRAM"
        );
    }

    #[tokio::test]
    async fn scraper_streams_a_body_and_uses_only_the_trusted_node_id() {
        let base = serve(Router::new().route(
            "/metrics",
            get(|| async { "# TYPE requests_total counter\nrequests_total{node_id=\"999\"} 12\n" }),
        ))
        .await;
        let scraper = PrometheusScraper::new(Duration::from_secs(2)).unwrap();
        let snapshot = scraper
            .scrape(42, &format!("{base}/metrics"))
            .await
            .unwrap();
        assert_eq!(snapshot.samples[0].node_id, 42);
        assert!(!snapshot.samples[0].labels.contains_key("node_id"));
    }

    #[tokio::test]
    async fn scraper_rejects_an_oversize_declared_body_without_reading_it() {
        let base = serve(Router::new().route(
            "/metrics",
            get(|| async {
                Response::builder()
                    .header(CONTENT_LENGTH, MAX_SCRAPE_BYTES + 1)
                    .body(Body::from_stream(futures::stream::pending::<
                        Result<String, Infallible>,
                    >()))
                    .unwrap()
            }),
        ))
        .await;
        let error = PrometheusScraper::new(Duration::from_secs(2))
            .unwrap()
            .scrape(1, &format!("{base}/metrics"))
            .await
            .unwrap_err();
        assert_eq!(error.code, "METRICS_BODY_TOO_LARGE");
    }

    #[tokio::test]
    async fn scraper_rejects_a_stream_that_crosses_the_byte_cap() {
        let base = serve(Router::new().route(
            "/metrics",
            get(|| async {
                let chunks = vec![
                    Ok::<_, Infallible>("x".repeat(MAX_SCRAPE_BYTES / 2 + 1)),
                    Ok("y".repeat(MAX_SCRAPE_BYTES / 2 + 1)),
                ];
                Body::from_stream(futures::stream::iter(chunks))
            }),
        ))
        .await;
        let error = PrometheusScraper::new(Duration::from_secs(2))
            .unwrap()
            .scrape(1, &format!("{base}/metrics"))
            .await
            .unwrap_err();
        assert_eq!(error.code, "METRICS_BODY_TOO_LARGE");
    }

    #[tokio::test]
    async fn scraper_rejects_non_success_and_non_http_endpoints() {
        let base = serve(Router::new().route(
            "/metrics",
            get(|| async { (StatusCode::SERVICE_UNAVAILABLE, "down") }),
        ))
        .await;
        let scraper = PrometheusScraper::new(Duration::from_secs(2)).unwrap();
        assert_eq!(
            scraper
                .scrape(1, &format!("{base}/metrics"))
                .await
                .unwrap_err()
                .code,
            "SCRAPE_HTTP_ERROR"
        );
        assert_eq!(
            scraper
                .scrape(1, "file:///tmp/metrics")
                .await
                .unwrap_err()
                .code,
            "INVALID_METRICS_URL"
        );
    }

    #[tokio::test]
    async fn history_uses_fixed_raw_counter_template_and_exact_cluster_filter() {
        let (query_tx, mut query_rx) = mpsc::channel(1);
        let base = serve(Router::new().route(
            "/api/v1/query_range",
            get(move |Query(query): Query<HashMap<String, String>>| {
                let query_tx = query_tx.clone();
                async move {
                    query_tx.send(query).await.unwrap();
                    Json(json!({
                        "status": "success",
                        "data": {
                            "resultType": "matrix",
                            "result": [{
                                "metric": {
                                    "cluster_id": "prod-east",
                                    "node_id": "1",
                                    "group_id": "7",
                                    "instance": "10.0.0.1:9091"
                                },
                                "values": [[1700000000.0, "1.5"], [1700000015.0, "2.5"]]
                            }]
                        }
                    }))
                }
            }),
        ))
        .await;
        let result = history_client(&base)
            .query_range(history_query(HistoryFamily::ServerRequests))
            .await
            .unwrap();
        let query = query_rx.recv().await.unwrap();
        let promql = query["query"].clone();
        assert!(
            promql.contains("rate(rr_server_requests_total{"),
            "{promql}"
        );
        assert!(promql.contains("cluster_id=\"prod-east\""), "{promql}");
        assert!(promql.contains("node_id=~\"1|2\""), "{promql}");
        assert!(promql.contains("group_id=\"7\""), "{promql}");
        assert!(promql.ends_with("[300s])"), "{promql}");
        assert!(!promql.contains("instance="), "{promql}");
        assert_eq!(query["start"], "1700000000");
        assert_eq!(query["end"], "1700000060");
        assert_eq!(query["step"], "15");
        assert_eq!(result[0].node_id, 1);
        assert_eq!(result[0].points[0].value, 1.5);
        assert_eq!(result[0].points[1].timestamp, 1_700_000_015.0);
    }

    #[tokio::test]
    async fn histogram_history_uses_raw_buckets_and_preserves_mapping_labels() {
        let (query_tx, mut query_rx) = mpsc::channel(1);
        let base = serve(Router::new().route(
            "/api/v1/query_range",
            get(move |Query(query): Query<HashMap<String, String>>| {
                let query_tx = query_tx.clone();
                async move {
                    query_tx.send(query).await.unwrap();
                    Json(json!({"status":"success","data":{"resultType":"matrix","result":[]}}))
                }
            }),
        ))
        .await;
        let mut query = history_query(HistoryFamily::ServerRequestDuration);
        query.quantile = Some(0.99);
        history_client(&base).query_range(query).await.unwrap();
        let promql = query_rx.recv().await.unwrap()["query"].clone();
        assert!(
            promql.starts_with("histogram_quantile(0.99,sum by ("),
            "{promql}"
        );
        assert!(
            promql.contains("rr_server_request_duration_seconds_bucket{"),
            "{promql}"
        );
        assert!(promql.contains("cluster_id"), "{promql}");
        assert!(promql.contains("node_id"), "{promql}");
        assert!(promql.contains("instance"), "{promql}");
        assert!(
            !promql.contains("rr_server_request_duration_seconds_p99"),
            "{promql}"
        );
    }

    #[tokio::test]
    async fn history_rejects_cross_cluster_or_unmapped_results() {
        for (metric, expected_code) in [
            (
                json!({"cluster_id":"other","node_id":"1"}),
                "CLUSTER_MISMATCH",
            ),
            (json!({"cluster_id":"prod-east"}), "UNMAPPED"),
            (
                json!({"cluster_id":"prod-east","node_id":"not-a-number"}),
                "UNMAPPED",
            ),
        ] {
            let base = serve(Router::new().route(
                "/api/v1/query_range",
                get(move || {
                    let metric = metric.clone();
                    async move {
                        Json(json!({
                            "status":"success",
                            "data":{"resultType":"matrix","result":[{
                                "metric":metric,
                                "values":[[1700000000.0,"1"]]
                            }]}
                        }))
                    }
                }),
            ))
            .await;
            let error = history_client(&base)
                .query_range(history_query(HistoryFamily::ServerRequests))
                .await
                .unwrap_err();
            assert_eq!(error.code, expected_code);
        }
    }

    #[tokio::test]
    async fn history_rejects_one_node_id_mapped_to_multiple_instances() {
        let base = serve(Router::new().route(
            "/api/v1/query_range",
            get(|| async {
                Json(json!({
                    "status":"success",
                    "data":{"resultType":"matrix","result":[
                        {"metric":{"cluster_id":"prod-east","node_id":"1","instance":"a:9090"},"values":[]},
                        {"metric":{"cluster_id":"prod-east","node_id":"1","instance":"b:9090"},"values":[]}
                    ]}
                }))
            }),
        ))
        .await;
        let error = history_client(&base)
            .query_range(history_query(HistoryFamily::ServerRequests))
            .await
            .unwrap_err();
        assert_eq!(error.code, "UNMAPPED");
    }

    #[tokio::test]
    async fn history_rejects_malformed_or_oversized_responses() {
        let base =
            serve(Router::new().route("/api/v1/query_range", get(|| async { Json(Value::Null) })))
                .await;
        let error = history_client(&base)
            .query_range(history_query(HistoryFamily::ServerRequests))
            .await
            .unwrap_err();
        assert_eq!(error.code, "MALFORMED_PROMETHEUS_RESPONSE");

        let base = serve(Router::new().route(
            "/api/v1/query_range",
            get(|| async {
                Response::builder()
                    .header(CONTENT_LENGTH, MAX_SCRAPE_BYTES + 1)
                    .body(Body::from_stream(futures::stream::pending::<
                        Result<String, Infallible>,
                    >()))
                    .unwrap()
            }),
        ))
        .await;
        let error = history_client(&base)
            .query_range(history_query(HistoryFamily::ServerRequests))
            .await
            .unwrap_err();
        assert_eq!(error.code, "METRICS_BODY_TOO_LARGE");
    }

    #[test]
    fn history_configuration_rejects_missing_cluster_or_invalid_controls() {
        assert!(PrometheusHistoryClient::new(
            "http://127.0.0.1:9090",
            ClusterMatcher {
                label: "cluster_id".to_string(),
                value: String::new(),
            },
            Duration::from_secs(1),
        )
        .is_err());
        let client = history_client("http://127.0.0.1:9090");
        let mut query = history_query(HistoryFamily::ServerRequestDuration);
        let error = client.validate_query(&query).unwrap_err();
        assert_eq!(error.code, "INVALID_HISTORY_QUERY");
        query.quantile = Some(f64::NAN);
        assert!(client.validate_query(&query).is_err());
        let mut query = history_query(HistoryFamily::ServerRequests);
        query.node_ids.push(0);
        assert!(client.validate_query(&query).is_err());
        let mut query = history_query(HistoryFamily::ServerRequests);
        query.step = Duration::ZERO;
        assert!(client.validate_query(&query).is_err());
    }
}
