---
sidebar_position: 2
---

# Operations Playbook / 深度探玩手册

This playbook turns the main cluster workflows into repeatable local commands. Prefer the `just` recipes shown
here over copying long shell fragments. Every recipe keeps transient PID and log files under `target/just/`.

本手册把常用集群流程整理成可重复执行的命令。优先使用文中的 `just` 命令；后台进程的 PID 和日志统一放在
`target/just/`。

## 1. Prerequisites and environment check

Install the core tools you intend to use: Rust, Python 3, Java (for the SDK), Node/npm (for docs), Helm (for
chart checks), and Docker or Colima (for real Etcd tests).

Then inspect what this machine can run:

```bash
just doctor
just --list
```

`doctor` reports required and optional tools separately. It does not install anything. For container-backed Etcd
recipes, set `RR_DOCKER_HOST` when your daemon is not the default Docker socket:

```bash
export RR_DOCKER_HOST=unix:///Users/$USER/.colima/default/docker.sock
```

Use Colima, Docker Desktop, or OrbStack — the workflow only needs a reachable Docker API and the local Etcd
image.

## 2. Fastest smoke path

Run the in-process single-node demo:

```bash
cargo run -p kv-store
```

Expected behavior: the process initializes one in-memory Raft group, runs a few writes/reads, and exits. This is
not a persistence or multi-node proof; it is the shortest API smoke path.

For regular development verification:

```bash
just build
just test
just test-one membership_guard
just check
```

`just check` covers formatting, the invariant registry, and workspace checking. Run narrower `test-one` filters
while iterating, then run the full suite before integration.

## 3. Manual three-node KV cluster

Use this path to explore raw membership behavior without the lifecycle store:

```bash
just kv-cluster-up
just kv-cluster-status
just kv-cluster-put
```

The recipe starts three local `kv-store` processes using the same `id@addr` roster, then waits for a leader.
State is in memory. Logs are in `target/just/logs/kv-node{1,2,3}.log`.

Verify the result:

- all nodes report the same `leader_node_id`;
- `last_log_index` advances on all nodes after writes;
- a follower write is redirected rather than silently accepted by a non-leader;
- the REPL prints `OK` only after the server reports success.

Stop only the processes started by the recipe:

```bash
just kv-cluster-down
```

### Port conflicts

If a node reports `code: 'Operation is not implemented or not supported'`, check whether another service already
publishes the selected ports. On a machine with Docker port forwarding, a wildcard published port can answer
before the local kv-store binds `127.0.0.1`. Use a free base port instead:

```bash
RR_KV_BASE_PORT=54101 just kv-cluster-up
```

## 4. Homogeneous Genesis cluster

Managed Genesis gives every node the same full roster and lifecycle settings; no node carries a special
`--bootstrap` role. Formation is authorized through a linearizable lifecycle record.

Prerequisites:

1. The local image `quay.io/coreos/etcd:v3.5.13` exists in the selected Docker daemon.
2. The Etcd endpoint is reachable.
3. The selected Genesis ports are free.

Start the local lifecycle backend and cluster:

```bash
just etcd-up
just genesis-up
just genesis-status
```

The lifecycle record is authoritative for formation. A Mem-backed node that restarts does not replay the seed:
if the group is already Active, it waits for replication from surviving members. A Rocks-backed node reloads its
local identity and Raft state.

Stop the processes without deleting unrelated containers:

```bash
just genesis-down
just etcd-down
```

To use non-default ports or a different cluster name:

```bash
RR_GENESIS_BASE_PORT=54201 RR_CLUSTER_NAME=rr-dev just genesis-up
```

Reusing a cluster name against an existing lifecycle record is intentional only when restarting the same cluster.
A different roster with the same name fails closed instead of silently forming a second configuration.

## 5. Legacy adoption

Use adoption only for an existing initialized cluster. The ordering matters:

1. Start initialized nodes in adopt mode first.
2. Verify the lifecycle record is `LegacyAdopted` and groups are Active.
3. Start pristine nodes.
4. Converge membership through the existing formation job or explicit `add-learner` +
   `change-membership` operations.

Adoption never initializes a group and never expands membership by itself. See
[Upgrading](../deployment/upgrading.md) for the exact Kubernetes phases. Do not clean data directories as part
of a normal adoption.

## 6. Durable Rocks restart checks

For durable restart behavior, build and test with the `rocks` feature:

```bash
cargo test -p integration --features rocks --lib homogeneous_formation -- --nocapture
```

The process-level acceptance test additionally uses real Etcd and real `kv-store` processes. Run it through the
same Docker daemon that owns the Etcd image:

```bash
DOCKER_HOST="$RR_DOCKER_HOST" RR_TEST_ETCD=1 \
  cargo test -p kv-store --features rocks --test homogeneous_process -- --include-ignored --nocapture
```

Durable mode persists Raft logs, snapshots, and framework identity. The demo business KV map still belongs to the
example state machine; do not treat the example as a production storage engine.

## 7. Failover and recovery exploration

With a three-node cluster running, stop the observed leader and watch the remaining nodes elect another leader:

```bash
just kv-cluster-status
```

Ctrl-C the leader process or use its recorded PID from `target/just/pids/`, then re-run status. Expected
behavior: committed entries remain visible through the surviving majority and a new leader appears.

For the default in-memory mode, restarting a node loses local log/vote state; surviving peers replicate from
scratch. Restarting all nodes does not preserve writes. Use the `rocks` feature for durable recovery.

## 8. Admin and observability

Each `kv-store` process serves the client/Raft gRPC port and an Admin endpoint at `port + 1000`. Useful direct
commands:

```bash
target/debug/rr-admin --addr 127.0.0.1:52101 --output json group status --group 1
target/debug/rr-admin --addr 127.0.0.1:52101 --output json group membership --group 1
```

Replace `52101` with your selected base port plus 1000. Managed formation temporarily rejects membership-changing
Admin RPCs while a group is Forming; once Active, normal membership operations are allowed.

Metrics and health behavior depend on the configured metrics port. A lifecycle-store outage makes pristine nodes
fail closed; initialized nodes can continue Raft traffic but report degraded/not-ready formation state.

## 9. Real acceptance suites

The local acceptance path is intentionally explicit:

```bash
just test-etcd
just helm-test
```

`test-etcd` requires the local Etcd image and runs both lifecycle CAS and process-level formation suites. It does
not pull images implicitly. `helm-test` requires a local Helm binary and validates Genesis/adoption rendering,
including revision-specific Job names.

These suites use real processes/containers and may leave detailed failures in `target/just/logs/` or the Cargo
test output. Do not treat skipped tests as passes.

## 10. Java SDK workflow

The Java SDK remains under `sdks/java` and uses its existing Gradle wrapper. Common commands:

```bash
just java-build
just java-test
just java-integration-test
just java-javadoc
just java-publish-local
```

Integration tests follow the SDK's `RR_TEST_DOCKER=1` convention and need a working Docker daemon. Local publish
goes to Maven Local only; it does not publish to GitHub Packages.

See the [Java SDK README](https://github.com/gardenias/rr/blob/main/sdks/java/README.md) for API-level examples, including leader redirect,
linearizable reads, idempotency, TLS and metrics.

## 11. Troubleshooting

### Lifecycle store unavailable

- Genesis/pristine node: startup fails closed.
- Initialized node: local Raft recovery may continue, but readiness stays degraded until the store is reachable.

### Different seed under the same cluster name

Formation refuses the mismatched roster. Use a new cluster name for an intentionally new deployment or return to
the original SeedSpec.

### Admin RPC returns `FailedPrecondition` during startup

The group is still Forming or lifecycle state is unavailable. Wait for Active state, then retry the membership
operation.

### Background processes remain after an interrupted command

Run `just clean-target-just` to stop processes recorded by these recipes and remove their PID state. Review
`target/just/logs/` first if you need the failure evidence.

## 12. Boundaries

This playbook does not cover dynamic replica replacement, resharding, partition split/merge, or disaster recovery
after majority state loss. Those are separate design phases. `SeedSpec` describes initial formation, not current
Raft membership; after a group is Active, current membership is the authoritative runtime state.
