//! The R-4 scenarios. All are gated on `--features rocks` (they spawn a
//! rocks-backed node binary); a plain `cargo test -p crash-tests` runs only
//! the skip notice below plus the pure unit tests.

#[cfg(not(feature = "rocks"))]
mod skip_notice {
    #[test]
    fn crash_scenarios_require_rocks_feature() {
        eprintln!(
            "SKIPPED: crash-recovery scenarios need the `rocks` feature and the \
             rr-crash-node binary.\n  build: cargo build -p crash-tests --features rocks\n  \
             run:   cargo test -p crash-tests --features rocks -- --test-threads=1 --nocapture"
        );
    }
}

#[cfg(feature = "rocks")]
mod rocks_scenarios {
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use anyhow::{Context, Result};
    use rand::{Rng, SeedableRng};
    use rr_client::RrClient;

    use crate::cluster::{find_bin, log_fault, port_base, ClusterConfig, CrashCluster};
    use crate::driver::Driver;
    use crate::journal::Journal;
    use crate::oracle::{replay_ack_log, OracleReport};

    /// Both scenarios spawn real processes on loopback; serialize them so
    /// port probing and failure injection never interleave across tests.
    static SCENARIO_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    /// Hard ceiling for one scenario. On expiry the test panics and the
    /// cluster's Drop guard reaps all children — no stray processes.
    /// Env-tunable (`CRASH_SCENARIO_TIMEOUT_SECS`): nightly scenario B on a
    /// slow runner (debug node binaries) needs >10 min for 10 rounds of
    /// full-log replays — the oracle replays the whole ack log every round.
    fn scenario_timeout() -> Duration {
        Duration::from_secs(u64::from(env_u32("CRASH_SCENARIO_TIMEOUT_SECS", 600)))
    }

    /// One planned kill: which node, how far into the round (wall-clock).
    /// Drawn from the seeded RNG before the round starts, so a `CRASH_SEED`
    /// replay reproduces the exact same sequence.
    #[derive(Debug, Clone, Copy)]
    struct KillPlan {
        node: u64,
        offset_ms: u64,
    }

    struct Harness {
        cluster: CrashCluster,
        driver: Driver,
        batch_lane: Option<crate::driver::BatchLane>,
        client: RrClient,
        fault_log: Journal,
        ack_path: PathBuf,
        seed: u64,
        rounds: u32,
        rng: rand::rngs::StdRng,
    }

    impl Harness {
        async fn setup(scenario: &str) -> Result<Self> {
            Self::setup_with(scenario, None).await
        }

        async fn setup_with(scenario: &str, snapshot_logs_since_last: Option<u64>) -> Result<Self> {
            let run_dir = run_dir(scenario);
            if run_dir.exists() {
                std::fs::remove_dir_all(&run_dir)
                    .with_context(|| format!("wipe run dir {}", run_dir.display()))?;
            }
            std::fs::create_dir_all(&run_dir)
                .with_context(|| format!("create run dir {}", run_dir.display()))?;

            let seed = match std::env::var("CRASH_SEED") {
                Ok(v) => v
                    .parse::<u64>()
                    .with_context(|| format!("CRASH_SEED={v} is not a valid u64"))?,
                Err(_) => rand::random::<u64>(),
            };
            let rounds = env_u32("CRASH_ROUNDS", 3);
            let write_rps = env_u32("CRASH_WRITE_RPS", 50);
            let rng = rand::rngs::StdRng::seed_from_u64(seed);

            let server_bin = find_bin("rr-crash-node", "RR_CRASH_NODE_BIN")?;
            let cluster_cfg = ClusterConfig {
                server_bin,
                run_dir: run_dir.clone(),
                port_base: port_base()?,
                // Node-side log level; `CRASH_NODE_LOG_LEVEL` allows debug
                // forensics, e.g. `openraft::core::sm=debug,openraft::engine=debug`.
                log_level: std::env::var("CRASH_NODE_LOG_LEVEL")
                    .unwrap_or_else(|_| "info".to_string()),
                snapshot_logs_since_last,
            };

            let mut fault_log = Journal::create(run_dir.join("fault-log.jsonl"))?;
            log_fault(
                &mut fault_log,
                seed,
                "plan",
                None,
                None,
                None,
                Some(format!(
                    "scenario={scenario} rounds={rounds} write_rps={write_rps}"
                )),
            )?;

            println!(
                "[crash-harness] scenario={scenario} seed={seed} rounds={rounds} \
                 (replay: CRASH_SEED={seed})"
            );
            println!("[crash-harness] run dir: {}", run_dir.display());

            let cluster = CrashCluster::start(&cluster_cfg).await?;
            let client = crate::driver::connect(&cluster.all_grpc_addrs()).await?;

            let ack_path = run_dir.join("ack-log.jsonl");
            let driver = Driver::start(client.clone(), write_rps, Journal::create(&ack_path)?);

            // Batched unkeyed write lane (client-side batching) — the crash
            // coverage for batch frames in the log. Default on;
            // CRASH_BATCH_LANE=0 disables.
            let batch_lane = if std::env::var("CRASH_BATCH_LANE").as_deref() == Ok("0") {
                None
            } else {
                Some(crate::driver::BatchLane::start(
                    cluster.all_grpc_addrs(),
                    write_rps,
                ))
            };

            Ok(Self {
                cluster,
                driver,
                batch_lane,
                client,
                fault_log,
                ack_path,
                seed,
                rounds,
                rng,
            })
        }

        /// Block until the driver has at least `CRASH_WARMUP_WRITES`
        /// (default 30) acknowledged writes — the pre-fault baseline.
        async fn warmup_barrier(&self) -> Result<()> {
            let target = u64::from(env_u32("CRASH_WARMUP_WRITES", 30));
            let deadline = Instant::now() + Duration::from_secs(60);
            while self.driver.acked() < target {
                if Instant::now() >= deadline {
                    anyhow::bail!(
                        "warmup barrier not reached: acked={} < {target}",
                        self.driver.acked()
                    );
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Ok(())
        }

        /// Ack-log length on disk — the exact set of writes the oracle must
        /// verify (the driver's in-memory counter can be one append ahead).
        fn acked_on_disk(&self) -> Result<u64> {
            Ok(
                crate::journal::Journal::read_all::<crate::journal::AckRecord>(&self.ack_path)?
                    .len() as u64,
            )
        }

        /// Replay the whole ack log; fail loudly on any loss.
        async fn run_oracle(&self, phase: &str) -> Result<OracleReport> {
            let report = replay_ack_log(&self.client, &self.ack_path).await?;
            println!(
                "[crash-harness] oracle[{phase}]: {}/{} acked writes verified in {:?}",
                report.verified, report.checked, report.elapsed
            );
            Ok(report)
        }

        async fn teardown(self, scenario: &str) {
            let stats = Arc::clone(self.driver.stats());
            let errors = self.driver.error_samples();
            self.driver.stop().await;
            println!(
                "[crash-harness] scenario={scenario} done: sent={} acked={} failed={} corrupted={}",
                stats.sent.load(std::sync::atomic::Ordering::Relaxed),
                stats.acked.load(std::sync::atomic::Ordering::Relaxed),
                stats.failed.load(std::sync::atomic::Ordering::Relaxed),
                stats.corrupted.load(std::sync::atomic::Ordering::Relaxed),
            );
            for e in &errors {
                println!("[crash-harness] driver error sample: {e}");
            }
            assert_eq!(
                stats.corrupted.load(std::sync::atomic::Ordering::Relaxed),
                0,
                "driver observed responses that did not echo their request: {errors:?}"
            );
            if let Some(lane) = self.batch_lane {
                let lane_stats = Arc::clone(lane.stats());
                let lane_errors = lane.error_samples();
                lane.stop().await;
                println!(
                    "[crash-harness] scenario={scenario} batch-lane: sent={} acked={} failed={} corrupted={}",
                    lane_stats.sent.load(std::sync::atomic::Ordering::Relaxed),
                    lane_stats.acked.load(std::sync::atomic::Ordering::Relaxed),
                    lane_stats.failed.load(std::sync::atomic::Ordering::Relaxed),
                    lane_stats.corrupted.load(std::sync::atomic::Ordering::Relaxed),
                );
                for e in &lane_errors {
                    println!("[crash-harness] batch-lane error sample: {e}");
                }
                assert_eq!(
                    lane_stats
                        .corrupted
                        .load(std::sync::atomic::Ordering::Relaxed),
                    0,
                    "batch lane observed responses that did not echo their request \
                     (batch framing regression): {lane_errors:?}"
                );
                // Count-bounds oracle for the batched lane (wired here — it
                // was dead code in oracle.rs): the applied count must cover
                // every ACKed write (keyed + batched) and never exceed what
                // the harness actually sent.
                let keyed_acked = stats.acked.load(std::sync::atomic::Ordering::Relaxed);
                let batch_acked = lane_stats.acked.load(std::sync::atomic::Ordering::Relaxed);
                let keyed_sent = stats.sent.load(std::sync::atomic::Ordering::Relaxed);
                let batch_sent = lane_stats.sent.load(std::sync::atomic::Ordering::Relaxed);
                let count = crate::oracle::verify_applied_count_bounds(
                    &self.client,
                    keyed_acked + batch_acked,
                    move || (keyed_sent, batch_sent),
                )
                .await
                .expect("batch-lane count-bounds oracle failed");
                println!(
                    "[crash-harness] scenario={scenario} count-bounds: applied={count} acked_floor={} sent_ceiling={}",
                    keyed_acked + batch_acked,
                    keyed_sent + batch_sent,
                );
            }
            // Zero-panic gate (same shape as scripts/soak.sh): a panicked
            // raft core can leave gRPC serving while apply is frozen, which
            // the oracle cannot see when quorum survives. Scans every boot
            // log (a panic precedes the restart, so evidence lives in the
            // previous boot's log).
            if let Some(path) = self.cluster.any_node_log_contains("panicked at") {
                panic!(
                    "node log contains a panic (raft-core death is never expected): {}",
                    path.display()
                );
            }
            // CrashCluster::drop SIGKILLs and reaps any surviving children.
            drop(self.cluster);
        }
    }

    fn run_dir(scenario: &str) -> PathBuf {
        if let Ok(v) = std::env::var("CRASH_RUN_DIR") {
            return PathBuf::from(v).join(scenario);
        }
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/crash-tests")
            .join(scenario)
    }

    fn env_u32(name: &str, default: u32) -> u32 {
        std::env::var(name)
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(default)
    }

    /// INV-1: an ACKed write survives any single-node crash/restart (fsync precedes ACK under Strict durability).
    /// R-4 scenario A — graceful SIGTERM. Exercises the shutdown drain path
    /// (gRPC stop → in-flight write drain → raft shutdown → batcher tail
    /// flush): every write the client saw acknowledged must survive, and the
    /// process must exit cleanly within the deadline.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn scenario_a_graceful_sigterm() {
        let _guard = SCENARIO_LOCK.lock().await;
        tokio::time::timeout(scenario_timeout(), scenario_a_body())
            .await
            .expect("scenario A exceeded its time ceiling")
            .unwrap();
    }

    async fn scenario_a_body() -> Result<()> {
        let mut h = Harness::setup("scenario-a-graceful-sigterm").await?;
        h.warmup_barrier().await?;
        // A few extra seconds of load so the shutdown window lands mid-stream.
        tokio::time::sleep(Duration::from_secs(2)).await;

        let target = h.rng.gen_range(1..=3u64);
        let frontier = h.acked_on_disk()?;
        println!(
            "[crash-harness] SIGTERM node {target} at frontier={frontier} (seed={})",
            h.seed
        );
        log_fault(
            &mut h.fault_log,
            h.seed,
            "sigterm",
            Some(target),
            Some(0),
            None,
            None,
        )?;

        // The graceful-shutdown contract: the process must exit, with code 0,
        // well inside this deadline.
        let status = h
            .cluster
            .term_node(target, Duration::from_secs(20))
            .await?
            .unwrap_or_else(|| panic!("node {target} did not exit within 20s of SIGTERM"));
        assert!(
            status.success(),
            "node {target} exited with {status} after SIGTERM — graceful shutdown failed"
        );
        let node_log = std::fs::read_to_string(h.cluster.node_log_path(target))
            .context("read terminated node's log")?;
        assert!(
            node_log.contains("rr-crash-node shut down cleanly"),
            "node {target} log lacks the clean-shutdown marker"
        );

        h.cluster.restart_node(target).await?;
        log_fault(
            &mut h.fault_log,
            h.seed,
            "restart",
            Some(target),
            Some(0),
            None,
            None,
        )?;
        let caught_up = h
            .cluster
            .wait_caught_up(target, frontier, Duration::from_secs(60))
            .await?;
        log_fault(
            &mut h.fault_log,
            h.seed,
            "rejoined",
            Some(target),
            Some(0),
            None,
            Some(format!("applied={caught_up} frontier={frontier}")),
        )?;

        // Oracle: every write acked at any point (including during the
        // shutdown window) must replay from the idempotency cache.
        let report = h.run_oracle("post-sigterm").await?;
        assert!(report.checked > 0, "scenario A acked nothing — vacuous run");
        assert_eq!(report.verified, report.checked);

        h.teardown("scenario-a-graceful-sigterm").await;
        Ok(())
    }

    /// INV-1: an ACKed write survives any single-node crash/restart (fsync precedes ACK under Strict durability).
    /// R-4 scenario B — the main loop: N rounds of seeded-random SIGKILL at
    /// random wall-clock points (under continuous write load), restart with
    /// the same data dir, catch-up, then a full ack-log replay. Zero
    /// acknowledged writes may be lost.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn scenario_b_sigkill() {
        let _guard = SCENARIO_LOCK.lock().await;
        tokio::time::timeout(scenario_timeout(), scenario_b_body())
            .await
            .expect("scenario B exceeded its time ceiling")
            .unwrap();
    }

    async fn scenario_b_body() -> Result<()> {
        let mut h = Harness::setup("scenario-b-sigkill").await?;

        // Draw the entire kill schedule up front: same seed → same schedule,
        // independent of wall-clock jitter.
        let schedule: Vec<KillPlan> = (1..=h.rounds)
            .map(|_| KillPlan {
                node: h.rng.gen_range(1..=3u64),
                offset_ms: h.rng.gen_range(800..=2500u64),
            })
            .collect();
        log_fault(
            &mut h.fault_log,
            h.seed,
            "plan",
            None,
            None,
            None,
            Some(format!(
                "schedule={:?}",
                schedule
                    .iter()
                    .map(|k| (k.node, k.offset_ms))
                    .collect::<Vec<_>>()
            )),
        )?;

        h.warmup_barrier().await?;

        for (round, plan) in schedule.iter().enumerate() {
            let round = round as u32 + 1;
            println!(
                "[crash-harness] round {round}/{}: SIGKILL node {} in {}ms (seed={})",
                h.rounds, plan.node, plan.offset_ms, h.seed
            );

            // Load continues into the kill window.
            tokio::time::sleep(Duration::from_millis(plan.offset_ms)).await;
            let frontier = h.acked_on_disk()?;

            h.cluster.kill_node(plan.node).await?;
            log_fault(
                &mut h.fault_log,
                h.seed,
                "sigkill",
                Some(plan.node),
                Some(round),
                Some(plan.offset_ms),
                Some(format!("frontier={frontier}")),
            )?;

            // The surviving pair keeps quorum and keeps serving writes.
            tokio::time::sleep(Duration::from_secs(1)).await;

            h.cluster.restart_node(plan.node).await?;
            log_fault(
                &mut h.fault_log,
                h.seed,
                "restart",
                Some(plan.node),
                Some(round),
                None,
                None,
            )?;

            let caught_up = h
                .cluster
                .wait_caught_up(plan.node, frontier, Duration::from_secs(60))
                .await?;
            log_fault(
                &mut h.fault_log,
                h.seed,
                "rejoined",
                Some(plan.node),
                Some(round),
                None,
                Some(format!("applied={caught_up} frontier={frontier}")),
            )?;

            let report = h.run_oracle(&format!("round-{round}")).await?;
            assert_eq!(
                report.verified, report.checked,
                "round {round}: oracle verified fewer writes than the ack log holds"
            );
        }

        // Final full replay after the last restart settles.
        let report = h.run_oracle("final").await?;
        assert_eq!(report.verified, report.checked);
        assert!(report.checked > 0, "scenario B acked nothing — vacuous run");

        h.teardown("scenario-b-sigkill").await;
        Ok(())
    }

    /// INV-1, INV-5: an ACKed write survives a crash landing after snapshot +
    /// log-purge cycles — recovery composes the durable SM snapshot with the
    /// un-purged log tail (T1), and a node that fell behind the purge horizon
    /// catches up via snapshot install.
    /// R-4 scenario C — snapshot/purge under crash. With
    /// `--snapshot-logs-since-last 100` the default 50 writes/s produce a
    /// snapshot + purge roughly every 2s, so the SIGKILL and the dead window
    /// straddle real snapshot cycles — the path scenarios A/B (threshold
    /// 50_000) never reach.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn scenario_c_snapshot_purge_kill() {
        let _guard = SCENARIO_LOCK.lock().await;
        tokio::time::timeout(scenario_timeout(), scenario_c_body())
            .await
            .expect("scenario C exceeded its time ceiling")
            .unwrap();
    }

    async fn scenario_c_body() -> Result<()> {
        let mut h = Harness::setup_with("scenario-c-snapshot-purge-kill", Some(100)).await?;
        h.warmup_barrier().await?;

        // Seeded-random kill plan, drawn up front (same discipline as
        // scenario B): CRASH_SEED replays the exact timings, and the draws
        // land in the fault log. Bounds are calibrated at the default
        // CRASH_WRITE_RPS=50 with snapshot threshold 100 (a snapshot+purge
        // cycle ≈ 2s of load):
        // - warmup 5–8s ⇒ 250–400 entries ⇒ a few full snapshot/purge cycles
        //   on ALL nodes before the fault lands (was a fixed 5s);
        // - dead window 12–20s ⇒ 600–1000 entries ⇒ 6–10 cycles on the
        //   surviving quorum, so the dead node falls well behind the purge
        //   horizon (was a fixed 15s).
        let target = h.rng.gen_range(1..=3u64);
        let warmup_ms = h.rng.gen_range(5_000..=8_000u64);
        let dead_window_ms = h.rng.gen_range(12_000..=20_000u64);
        log_fault(
            &mut h.fault_log,
            h.seed,
            "plan",
            Some(target),
            None,
            None,
            Some(format!("warmup_ms={warmup_ms} dead_window_ms={dead_window_ms}")),
        )?;
        tokio::time::sleep(Duration::from_millis(warmup_ms)).await;

        let frontier = h.acked_on_disk()?;
        println!(
            "[crash-harness] SIGKILL node {target} at frontier={frontier} \
             under snapshot threshold 100 (seed={}, warmup={warmup_ms}ms)",
            h.seed
        );
        log_fault(
            &mut h.fault_log,
            h.seed,
            "sigkill",
            Some(target),
            None,
            None,
            Some(format!("frontier={frontier} snapshot_threshold=100")),
        )?;
        // Capture the kill instant BEFORE the kill: the post-restart snapshot
        // check below requires an artifact written strictly after this point.
        let killed_at = std::time::SystemTime::now();
        h.cluster.kill_node(target).await?;

        // Dead window: the surviving quorum keeps writing and cycles through
        // several snapshot+purge rounds, so the dead node falls behind the
        // purge horizon — its restart must compose durable snapshot + log
        // tail, or install a snapshot outright.
        tokio::time::sleep(Duration::from_millis(dead_window_ms)).await;

        h.cluster.restart_node(target).await?;
        log_fault(
            &mut h.fault_log,
            h.seed,
            "restart",
            Some(target),
            None,
            None,
            None,
        )?;

        // Re-sample: the frontier moved while the node was dead.
        let frontier = h.acked_on_disk()?;
        let caught_up = h
            .cluster
            .wait_caught_up(target, frontier, Duration::from_secs(90))
            .await?;
        log_fault(
            &mut h.fault_log,
            h.seed,
            "rejoined",
            Some(target),
            None,
            None,
            Some(format!("applied={caught_up} frontier={frontier}")),
        )?;

        // T1 artifact, freshness-checked (2026-10-21 review): mere existence
        // of `sm-snapshot.bin` was vacuous — the node may have persisted it
        // pre-kill. Require the artifact to have been (re)written AFTER the
        // kill instant: the restarted node applies past the low threshold
        // again under continuing load, and the persist path
        // (write → fsync → rename) refreshes the file mtime. Poll with a
        // deadline — the trigger is apply-count-based.
        let snap_deadline = Instant::now() + Duration::from_secs(30);
        let snap_path = h.cluster.sm_snapshot_path(target);
        loop {
            let fresh = std::fs::metadata(&snap_path)
                .and_then(|m| m.modified())
                .map(|mtime| mtime > killed_at)
                .unwrap_or(false);
            if fresh {
                break;
            }
            if Instant::now() >= snap_deadline {
                anyhow::bail!(
                    "node {target} has no post-kill sm-snapshot.bin 30s after catch-up \
                     (path={}, killed_at={killed_at:?}) — either the T1 persist-on-build \
                     path regressed or only a pre-kill snapshot survived",
                    snap_path.display(),
                );
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }

        // Oracle: the whole ack log — including writes acked during the
        // snapshot-cycling dead window — must replay from the idempotency
        // cache on the surviving quorum.
        let report = h.run_oracle("post-snapshot-kill").await?;
        assert!(report.checked > 0, "scenario C acked nothing — vacuous run");
        assert_eq!(report.verified, report.checked);

        h.teardown("scenario-c-snapshot-purge-kill").await;
        Ok(())
    }
}
