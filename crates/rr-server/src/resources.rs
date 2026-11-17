use std::path::{Path, PathBuf};
use std::time::Duration;

use sysinfo::{Disks, Networks, Pid, ProcessRefreshKind, ProcessesToUpdate, System};
use tokio::sync::watch;

const SAMPLE_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Debug, Default)]
struct MonotonicCounter {
    previous: Option<u64>,
}

impl MonotonicCounter {
    fn observe(&mut self, value: u64) -> u64 {
        let delta = match self.previous {
            Some(previous) if value >= previous => value - previous,
            Some(_) => value,
            None => value,
        };
        self.previous = Some(value);
        delta
    }
}

#[derive(Debug, Default)]
pub struct ResourceSample {
    process_cpu_ratio: Option<f64>,
    host_cpu_ratio: Option<f64>,
    host_cpu_cores: Option<f64>,
    cgroup_cpu_limit_cores: Option<f64>,
    process_memory_bytes: Option<u64>,
    host_memory_total_bytes: Option<u64>,
    host_memory_used_bytes: Option<u64>,
    cgroup_memory_limit_bytes: Option<u64>,
    cgroup_memory_used_bytes: Option<u64>,
    network_rx_bytes: Option<u64>,
    network_tx_bytes: Option<u64>,
    filesystem_mount: Option<String>,
    filesystem_total_bytes: Option<u64>,
    filesystem_used_bytes: Option<u64>,
}

struct ResourceSampler {
    system: System,
    networks: Networks,
    disks: Disks,
    pid: Option<Pid>,
    filesystem_path: PathBuf,
    network_rx: MonotonicCounter,
    network_tx: MonotonicCounter,
}

impl ResourceSampler {
    fn new(filesystem_path: PathBuf) -> Self {
        Self {
            system: System::new_all(),
            networks: Networks::new_with_refreshed_list(),
            disks: Disks::new_with_refreshed_list(),
            pid: sysinfo::get_current_pid().ok(),
            filesystem_path,
            network_rx: MonotonicCounter::default(),
            network_tx: MonotonicCounter::default(),
        }
    }

    fn sample(&mut self) -> ResourceSample {
        self.system.refresh_cpu_usage();
        self.system.refresh_memory();
        if let Some(pid) = self.pid {
            self.system.refresh_processes_specifics(
                ProcessesToUpdate::Some(&[pid]),
                true,
                ProcessRefreshKind::nothing().with_cpu().with_memory(),
            );
        }
        self.networks.refresh(true);
        self.disks.refresh(true);

        let process = self.pid.and_then(|pid| self.system.process(pid));
        let host_cores = self.system.cpus().len();
        let cgroup = self.system.cgroup_limits();
        let has_networks = !self.networks.is_empty();
        let mut sample = ResourceSample {
            process_cpu_ratio: process.and_then(|process| {
                (host_cores > 0).then(|| f64::from(process.cpu_usage()) / 100.0 / host_cores as f64)
            }),
            host_cpu_ratio: Some(f64::from(self.system.global_cpu_usage()) / 100.0),
            host_cpu_cores: (host_cores > 0).then_some(host_cores as f64),
            cgroup_cpu_limit_cores: cgroup_cpu_limit(),
            process_memory_bytes: process.map(|process| process.memory()),
            host_memory_total_bytes: Some(self.system.total_memory()),
            host_memory_used_bytes: Some(self.system.used_memory()),
            cgroup_memory_limit_bytes: cgroup
                .as_ref()
                .map(|limits| limits.total_memory)
                .filter(|limit| *limit > 0),
            cgroup_memory_used_bytes: cgroup.as_ref().map(|limits| limits.rss),
            network_rx_bytes: has_networks.then(|| {
                self.networks
                    .values()
                    .map(|network| network.total_received())
                    .sum()
            }),
            network_tx_bytes: has_networks.then(|| {
                self.networks
                    .values()
                    .map(|network| network.total_transmitted())
                    .sum()
            }),
            ..ResourceSample::default()
        };
        if let Some(disk) = select_disk(self.disks.list(), &self.filesystem_path) {
            sample.filesystem_mount = Some(disk.mount_point().to_string_lossy().into_owned());
            sample.filesystem_total_bytes = Some(disk.total_space());
            sample.filesystem_used_bytes =
                Some(disk.total_space().saturating_sub(disk.available_space()));
        }
        sample.process_cpu_ratio = sample.process_cpu_ratio.and_then(valid_ratio);
        sample.host_cpu_ratio = sample.host_cpu_ratio.and_then(valid_ratio);
        sample.cgroup_cpu_limit_cores = sample.cgroup_cpu_limit_cores.and_then(valid_ratio);
        sample
    }

    fn publish(&mut self) {
        let sample = self.sample();
        emit_gauges(&sample);
        if let Some(value) = sample.network_rx_bytes {
            let delta = self.network_rx.observe(value);
            metrics::counter!(
                "rr_node_network_bytes_total",
                "scope" => "host",
                "kind" => "rx",
            )
            .increment(delta);
        }
        if let Some(value) = sample.network_tx_bytes {
            let delta = self.network_tx.observe(value);
            metrics::counter!(
                "rr_node_network_bytes_total",
                "scope" => "host",
                "kind" => "tx",
            )
            .increment(delta);
        }
    }
}

fn valid_ratio(value: f64) -> Option<f64> {
    (value.is_finite() && value >= 0.0).then_some(value)
}

fn emit_gauges(sample: &ResourceSample) {
    if let Some(value) = sample.process_cpu_ratio {
        metrics::gauge!("rr_node_cpu_usage_ratio", "scope" => "process").set(value);
    }
    if let Some(value) = sample.host_cpu_ratio {
        metrics::gauge!("rr_node_cpu_usage_ratio", "scope" => "host").set(value);
    }
    if let Some(value) = sample.host_cpu_cores {
        metrics::gauge!("rr_node_cpu_cores", "scope" => "host").set(value);
    }
    if let Some(value) = sample.cgroup_cpu_limit_cores {
        metrics::gauge!("rr_node_cpu_limit_cores", "scope" => "cgroup").set(value);
    }
    for (scope, kind, value) in [
        ("process", "used", sample.process_memory_bytes),
        ("host", "total", sample.host_memory_total_bytes),
        ("host", "used", sample.host_memory_used_bytes),
        ("cgroup", "limit", sample.cgroup_memory_limit_bytes),
        ("cgroup", "used", sample.cgroup_memory_used_bytes),
    ] {
        if let Some(value) = value {
            metrics::gauge!("rr_node_memory_bytes", "scope" => scope, "kind" => kind)
                .set(value as f64);
        }
    }
    if let (Some(mount), Some(total), Some(used)) = (
        &sample.filesystem_mount,
        sample.filesystem_total_bytes,
        sample.filesystem_used_bytes,
    ) {
        for (kind, value) in [("total", total), ("used", used)] {
            metrics::gauge!(
                "rr_node_filesystem_bytes",
                "scope" => "host",
                "kind" => kind,
                "mount" => mount.clone(),
            )
            .set(value as f64);
        }
    }
}

fn select_disk<'a>(disks: &'a [sysinfo::Disk], path: &Path) -> Option<&'a sysinfo::Disk> {
    disks
        .iter()
        .filter(|disk| path.starts_with(disk.mount_point()))
        .max_by_key(|disk| disk.mount_point().as_os_str().len())
}

#[cfg(target_os = "linux")]
fn cgroup_cpu_limit() -> Option<f64> {
    if let Ok(contents) = std::fs::read_to_string("/sys/fs/cgroup/cpu.max") {
        let mut fields = contents.split_whitespace();
        let quota = fields.next()?;
        let period = fields.next()?.parse::<f64>().ok()?;
        if quota != "max" && period > 0.0 {
            return quota
                .parse::<f64>()
                .ok()
                .and_then(|quota| valid_ratio(quota / period));
        }
    }
    let quota = std::fs::read_to_string("/sys/fs/cgroup/cpu/cpu.cfs_quota_us")
        .ok()?
        .trim()
        .parse::<f64>()
        .ok()?;
    let period = std::fs::read_to_string("/sys/fs/cgroup/cpu/cpu.cfs_period_us")
        .ok()?
        .trim()
        .parse::<f64>()
        .ok()?;
    (quota > 0.0 && period > 0.0).then_some(quota / period)
}

#[cfg(not(target_os = "linux"))]
fn cgroup_cpu_limit() -> Option<f64> {
    None
}

pub fn spawn_resource_sampler(
    filesystem_path: PathBuf,
    mut shutdown: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut sampler = ResourceSampler::new(filesystem_path);
        let mut interval = tokio::time::interval(SAMPLE_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = shutdown.changed() => break,
                _ = interval.tick() => sampler.publish(),
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn monotonic_counter_handles_source_reset_without_negative_delta() {
        let mut counter = MonotonicCounter::default();
        assert_eq!(counter.observe(100), 100);
        assert_eq!(counter.observe(160), 60);
        assert_eq!(counter.observe(20), 20);
        assert_eq!(counter.observe(25), 5);
    }

    #[test]
    fn unsupported_values_are_omitted_from_prometheus_output() {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let _guard = metrics::set_default_local_recorder(&recorder);
        emit_gauges(&ResourceSample::default());
        let rendered = handle.render();
        for family in [
            "rr_node_cpu_usage_ratio",
            "rr_node_cpu_cores",
            "rr_node_cpu_limit_cores",
            "rr_node_memory_bytes",
            "rr_node_filesystem_bytes",
        ] {
            assert!(
                !rendered.contains(family),
                "unsupported {family} became a zero: {rendered}"
            );
        }
    }

    #[test]
    fn live_sampler_publishes_supported_resource_families() {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let _guard = metrics::set_default_local_recorder(&recorder);
        let temp = tempfile::tempdir().unwrap();
        let mut sampler = ResourceSampler::new(temp.path().to_path_buf());
        sampler.publish();
        let rendered = handle.render();
        for family in [
            "rr_node_cpu_usage_ratio",
            "rr_node_cpu_cores",
            "rr_node_memory_bytes",
            "rr_node_network_bytes_total",
            "rr_node_filesystem_bytes",
        ] {
            assert!(rendered.contains(family), "missing {family}: {rendered}");
        }
    }

    #[test]
    fn non_finite_and_negative_ratios_are_not_published() {
        assert_eq!(valid_ratio(f64::NAN), None);
        assert_eq!(valid_ratio(-0.1), None);
        assert_eq!(valid_ratio(0.25), Some(0.25));
    }
}
