//! Shared Prometheus recorder for metric-assertion tests in this crate.
//!
//! `metrics::set_global_recorder` succeeds only once per process, and a
//! crate's test binary is a single process — per-test recorder installs race:
//! the loser's handle renders an empty registry and its assertions flake.
//! Every metric-assertion test must instead go through the OnceLock-installed
//! recorder below and keep its series distinguishable via unique label values
//! (e.g. a per-test `group_id`), since all tests share the one registry.

use metrics_exporter_prometheus::PrometheusHandle;
use std::sync::OnceLock;

static RECORDER: OnceLock<PrometheusHandle> = OnceLock::new();

/// Install (on first call) and return the process-global Prometheus recorder
/// handle shared by all metric-assertion tests in this crate.
pub(crate) fn shared_prometheus_recorder() -> PrometheusHandle {
    RECORDER
        .get_or_init(|| {
            let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
            let handle = recorder.handle();
            let _ = metrics::set_global_recorder(recorder);
            handle
        })
        .clone()
}

/// Extract the value of the first rendered series whose line starts with
/// `metric` and contains every fragment in `label_frags` (fragments look like
/// `direction="recv"`). Returns `None` when no series matches.
pub(crate) fn series_value(rendered: &str, metric: &str, label_frags: &[&str]) -> Option<f64> {
    rendered.lines().find_map(|line| {
        if !line.starts_with(metric) || !label_frags.iter().all(|f| line.contains(f)) {
            return None;
        }
        line.rsplit_once(' ')?.1.parse().ok()
    })
}
