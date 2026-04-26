---
title: "etcd key schema for ConfigProvider routing state"
status: proposed
created: 2026-04-27
updated: 2026-04-27
tags: [adr, config, etcd, routing, schema]
---

# A-0004 — etcd Config Schema

## Context

`rr_core::config::ConfigProvider` defines a read-only runtime surface for cluster
routing state:

- `get_routing_table`
- `watch_changes`
- `get_node_addr`

The framework needs a production-friendly backend implementation that supports
dynamic updates and operational inspectability.

## Problem

We need a stable etcd v3 key layout and value encoding for routing metadata
(nodes, partitions, and group replicas) that:

1. maps cleanly to `rr-core` types (`NodeAddr`, `PartitionConfig`, `RoutingTable`)
2. supports full-table bootstrap reads and prefix watches
3. stays human-debuggable for operators (`etcdctl get --prefix /rr/`)
4. preserves a read-only trait surface in `ConfigProvider`

## Options Considered

### Option A — JSON values under explicit typed prefixes (selected)

Key layout:

- `/rr/{cluster_id}/nodes/{node_id}` → JSON `NodeAddr` (`{"addr":"host:port"}`)
- `/rr/{cluster_id}/partitions/{partition_id}` → JSON
  `{"config": <PartitionConfig>, "group_id": <u64>}`
- `/rr/{cluster_id}/groups/{group_id}/replicas` → JSON array
  `[node_id_1, node_id_2, ...]`

Pros:

- direct mapping to `RoutingTable` and `RoutingTableChange`
- straightforward prefix scans and watches
- easy ad-hoc debugging with etcdctl + jq

Cons:

- JSON payloads are larger than binary formats

### Option B — Bincode/protobuf values under same keys

Pros:

- smaller payloads
- stricter binary schema

Cons:

- poor operability/debuggability from CLI
- harder incident triage without custom tooling

### Option C — Flatten everything into one large blob key

Pros:

- simpler writer implementation

Cons:

- coarse watch granularity
- expensive updates
- weak partial failure isolation

## Decision

Adopt **Option A** with JSON-encoded values and explicit typed prefixes.

Additional decisions:

1. **Default cluster_id** is `"default"` when constructor input is blank.
2. **Trait surface remains read-only**: no etcd write APIs are exposed from the
   `ConfigProvider` implementation. Writes are allowed only in test setup through
   raw `etcd_client::Client`.

## Consequences

### Positive

- operators can inspect live state directly with `etcdctl`
- watch translation can map key-space events to all 6
  `RoutingTableChange` variants
- schema is simple and predictable for tooling and docs

### Negative

- JSON parse/serialize overhead compared to binary formats
- malformed JSON in any key causes provider read failure until corrected

### Neutral

- key hierarchy is namespace-friendly for future cluster-specific isolation

## References

- `crates/rr-core/src/config.rs`
- `crates/rr-core/src/types.rs`
- `crates/rr-core/src/error.rs`
- `crates/rr-config-etcd/src/lib.rs`

## Changelog

| Date | Change | Trigger |
|---|---|---|
| 2026-04-27 | Initial draft | Introduce etcd-backed ConfigProvider crate |
