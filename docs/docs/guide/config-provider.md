---
sidebar_position: 4
---

# 配置提供者 / ConfigProvider

`ConfigProvider` 提供分片到 Raft 组的路由、节点地址和组副本元数据。仓库实现包括 `MemConfigProvider` 和只读 etcd 后端 `EtcdConfigProvider`。
`ConfigProvider` supplies partition-to-group routing, node addresses, and group replica metadata. Implementations include `MemConfigProvider` and the read-only etcd-backed `EtcdConfigProvider`.

## Trait 与路由表 / Trait and Routing Table

接口定义在 `rr-core`：
The interface is defined in `rr-core`:

```rust
#[async_trait]
pub trait ConfigProvider: Send + Sync {
    async fn get_routing_table(&self) -> Result<RoutingTable, ConfigError>;

    async fn routing_table_snapshot(&self) -> Result<Arc<RoutingTable>, ConfigError> {
        Ok(Arc::new(self.get_routing_table().await?))
    }

    async fn watch_changes(
        &self,
    ) -> Result<Pin<Box<dyn Stream<Item = RoutingTableChange> + Send>>, ConfigError>;

    async fn get_node_addr(&self, node_id: NodeId) -> Result<NodeAddr, ConfigError>;
}
```

每次读取都获取提供方当前的 point-in-time 快照，不在调用方引入另一份权威缓存。内存后端可用廉价 `Arc` clone 覆盖默认实现。
Each read obtains the provider's current point-in-time snapshot without introducing another caller-side authority. In-memory backends can override the default with a cheap `Arc` clone.

```rust
pub struct RoutingTable {
    pub partitions: HashMap<PartitionId, (PartitionConfig, GroupId)>,
    pub nodes: HashMap<NodeId, NodeAddr>,
    pub groups: HashMap<GroupId, GroupMeta>,
}

pub enum PartitionConfig {
    SingleId(u64),
    Range { start: u64, end: u64 },
}

pub struct GroupMeta {
    pub group_id: GroupId,
    pub replicas: Vec<NodeId>,
}
```

## 变更流语义 / Change Stream Semantics

`watch_changes` 的事件包括 `PartitionAdded`、`PartitionRemoved`、`PartitionUpdated`、`NodeAdded`、`NodeRemoved` 和 `GroupMembershipChanged`。
`watch_changes` emits `PartitionAdded`, `PartitionRemoved`, `PartitionUpdated`, `NodeAdded`, `NodeRemoved`, and `GroupMembershipChanged` events.

- 流不包含初始快照；先调用 `get_routing_table()`，再应用订阅后的事件。/ There is no initial snapshot; fetch the table first, then apply post-subscription events.
- etcd 的 nodes、partitions、groups 是独立 watcher，不保证跨域全局顺序。/ etcd uses independent watchers with no global cross-domain ordering.
- 内存后端在消费者落后时丢弃事件并记录 WARN/指标；etcd 有界通道施加背压。/ The in-memory backend drops lagged events with a warning and metric; etcd's bounded channel applies backpressure.
- 不能丢事件的消费者应定期重新读取完整表；流终止时重新读取并订阅。/ Consumers that cannot miss events should periodically refetch the table; on termination, refetch and resubscribe.

组删除当前表示为空副本集，没有独立 `GroupRemoved` 事件。内存 `remove_node` 会从副本列表删除节点但不另发 `GroupMembershipChanged`；etcd 不做这项删除。
Group deletion currently appears as an empty replica set; there is no separate `GroupRemoved`. In-memory `remove_node` strips the node from replica lists without another membership event; etcd does not strip it.

## 内存提供者 / MemConfigProvider

`MemConfigProvider` 适合测试和本地演示：
`MemConfigProvider` is intended for tests and local demos:

```rust
use rr_config::mem::MemConfigProvider;
use rr_core::types::{NodeAddr, PartitionConfig};

let config = MemConfigProvider::new();
config.add_node(1, NodeAddr {
    addr: "127.0.0.1:51001".to_string(),
}).await;
config.add_partition(
    0,
    PartitionConfig::SingleId(0),
    1,
    vec![1, 2, 3],
).await;
```

这些写入只修改本地内存表，不会自动配置其他进程。
These writes modify only the local in-memory table; they do not configure other processes.

## etcd 提供者 / EtcdConfigProvider

`rr-config-etcd` 的构造函数会立即验证连通性。`cluster_id` 用于 key 前缀隔离。以下 `localhost` 明文仅是
显式本地开发配置；生产环境使用 `new_with_options`、HTTPS、CA/TLS 和认证，见下文 rr-server 配置。
The `rr-config-etcd` constructor validates connectivity immediately. `cluster_id` isolates key prefixes. The following
`localhost` plaintext is an explicit development-only configuration; production uses `new_with_options` with HTTPS,
CA/TLS, and authentication as shown in the rr-server configuration below.

```rust
use rr_config_etcd::EtcdConfigProvider;

// LOCAL PLAINTEXT ONLY / 仅本地明文：不在生产中使用。
let provider = EtcdConfigProvider::new(
    vec!["http://localhost:2379".to_string()],
    "default".to_string(),
).await?;
```

该实现只读，Rr 不把路由写回 etcd；外部工具负责写入以下 key：
The provider is read-only; Rr never writes routing state back to etcd. External tooling provisions these keys:

```text
/rr/{cluster_id}/nodes/{node_id}
/rr/{cluster_id}/partitions/{partition_id}
/rr/{cluster_id}/groups/{group_id}/replicas
```

`routing_table_snapshot()` 默认以 serializable keys-only revision 探针检查缓存，revision 变化才全量重扫。需要 quorum 新鲜度时调用 `with_linearizable_probes(true)`，代价是每次调用一次 quorum 往返。
`routing_table_snapshot()` uses a serializable keys-only revision probe by default and rescans only when the revision changes. Use `with_linearizable_probes(true)` for quorum-fresh probes at the cost of one quorum round trip per call.

完整读取和 watch 示例见 `examples/etcd-config-quickstart`，运行前在 `localhost:2379` 启动 etcd v3。
See `examples/etcd-config-quickstart` for reading and watching; start etcd v3 at `localhost:2379` first.

## 节点目录 / NodeDirectory

`NodeDirectory` 是独立、可写、带 lease 的节点端点目录，不是 `ConfigProvider` 的扩展，也不是 managed
formation 的 `LifecycleStore`。路由/副本仍由 ConfigProvider 和 Raft 提供；目录只发布发现所需的显式地址和
位置元数据。Console 的 discovery 接入状态见 [Cluster Admin Console](../operations/cluster-admin-console.md)。
`NodeDirectory` is a separate writable, leased directory of node endpoints. It is neither a `ConfigProvider` extension
nor the managed-formation `LifecycleStore`. Routing and replicas still come from ConfigProvider and Raft; the directory
publishes only explicit discovery addresses and placement metadata. See [Cluster Admin
Console](../operations/cluster-admin-console.md) for the Console's current discovery support.

### 节点自注册 / Node Self-Registration

`rr-server` 在完整配置描述符和目录后端后自注册。任一描述符字段出现时，`advertise_addr`、
`admin_advertise_addr`、`observability_advertise_addr` 和 `environment` 都必须提供；`region`、`zone`、`rack`
可选。地址必须是非 wildcard、非零端口的 `host:port`，不能从 client 地址推导 Admin/observability 端口。
`rr-server` self-registers when given a complete descriptor and directory backend. Setting any descriptor field makes
`advertise_addr`, `admin_advertise_addr`, `observability_advertise_addr`, and `environment` mandatory; `region`,
`zone`, and `rack` are optional. Addresses must be non-wildcard `host:port` values with nonzero ports. Admin and
observability ports are never derived from the client address.

```toml
[node]
id = 1
listen_addr = "0.0.0.0:51001"
advertise_addr = "rr-1.internal:51001"
admin_advertise_addr = "rr-1.internal:52001"
observability_advertise_addr = "rr-1.internal:9100"
app_version = "1.4.0"
environment = "production"
region = "us-east-1"
zone = "us-east-1a"

[cluster]
name = "prod-rr"
directory_etcd_endpoints = ["https://etcd-1.internal:2379", "https://etcd-2.internal:2379"]
etcd_ca_path = "/etc/rr/etcd-ca.pem"
etcd_cert_path = "/etc/rr/etcd-client.pem"
etcd_key_path = "/etc/rr/etcd-client.key"
etcd_domain_name = "etcd.internal"
etcd_username = "rr-node"
etcd_password_env = "RR_ETCD_PASSWORD"
```

对应 CLI 为 `--advertise-addr`、`--admin-advertise-addr`、`--observability-advertise-addr`、`--environment` 和
`--directory-etcd-endpoints`；环境变量分别是 `RR_ADVERTISE_ADDR`、`RR_ADMIN_ADVERTISE_ADDR`、
`RR_OBSERVABILITY_ADVERTISE_ADDR`、`RR_ENVIRONMENT` 和 `RR_DIRECTORY_ETCD_ENDPOINTS`。managed formation
节点已有完整描述符且未单独配置目录端点时，复用 `[cluster.formation].etcd_endpoints`。
The corresponding flags are `--advertise-addr`, `--admin-advertise-addr`, `--observability-advertise-addr`,
`--environment`, and `--directory-etcd-endpoints`; their environment variables are `RR_ADVERTISE_ADDR`,
`RR_ADMIN_ADVERTISE_ADDR`, `RR_OBSERVABILITY_ADVERTISE_ADDR`, `RR_ENVIRONMENT`, and
`RR_DIRECTORY_ETCD_ENDPOINTS`. A managed-formation node with a complete descriptor reuses
`[cluster.formation].etcd_endpoints` when no separate directory endpoints are configured.

### rr-server etcd TLS 与认证 / rr-server etcd TLS and Authentication

上例是推荐的安全配置。`[cluster]` 的 etcd TLS/auth 同时应用于 managed formation 和 NodeDirectory 客户端；
启用任一 TLS/auth 字段后，formation 和 directory 的所有 endpoint 都必须使用 `https://`。
The example above is the recommended secure configuration. `[cluster]` etcd TLS/auth settings apply to both managed
formation and NodeDirectory clients. Enabling any TLS/auth field requires every formation and directory endpoint to use
`https://`.

CLI、TOML 与环境变量的对应关系如下；CLI 优先于 TOML：
The CLI, TOML, and environment-variable equivalents are listed below; CLI values override TOML:

- `--etcd-ca` ↔ `[cluster].etcd_ca_path` ↔ `RR_ETCD_CA`：CA bundle。/ CA bundle.
- `--etcd-cert` ↔ `[cluster].etcd_cert_path` ↔ `RR_ETCD_CERT`：客户端证书。/ Client certificate.
- `--etcd-key` ↔ `[cluster].etcd_key_path` ↔ `RR_ETCD_KEY`：客户端私钥。/ Client private key.
- `--etcd-domain` ↔ `[cluster].etcd_domain_name` ↔ `RR_ETCD_DOMAIN`：SNI/证书名称覆盖。/ SNI/certificate
  name override.
- `--etcd-username` ↔ `[cluster].etcd_username` ↔ `RR_ETCD_USERNAME`：认证用户名。/ Authentication username.
- `--etcd-password-env` ↔ `[cluster].etcd_password_env` ↔ `RR_ETCD_PASSWORD_ENV`：保存密码的环境变量名。/
  Name of the environment variable containing the password.

客户端 cert/key 必须成对；username/password-env 必须成对，且引用的环境变量必须已设置且非空。TOML 和 debug
输出不保存明文密码，密码不能放入 argv。生产环境推荐同时配置 CA、mTLS 和用户名/密码，并按职责授予最小
etcd 权限。
The client certificate and key are a pair. Username and password-env are also a pair, and the referenced variable must
be set and non-empty. TOML and debug output do not contain the plaintext password, and passwords never go in argv.
Production should combine CA verification, mTLS, and username/password authentication with least-privilege etcd roles.

本地明文是显式开发配置：使用 `http://127.0.0.1` 并完全省略上述 etcd TLS/auth 字段。rr-server 没有
`--etcd-allow-insecure`；不要把 `[node].allow_insecure` 误认为 etcd 安全开关。
Local plaintext is an explicit development configuration: use `http://127.0.0.1` and omit every etcd TLS/auth field
above. rr-server has no `--etcd-allow-insecure`; `[node].allow_insecure` is not an etcd security setting.

```toml
# LOCAL PLAINTEXT ONLY / 仅本地明文：生产环境请使用上例 HTTPS + TLS/auth。
[cluster]
name = "rr-local"
directory_etcd_endpoints = ["http://127.0.0.1:2379"]
# Intentionally no etcd_* TLS/auth fields / 有意不配置任何 etcd_* 安全字段。
```

### 外部 provisioning / External Provisioning

部署系统可以写入相同 schema，但应优先复用 `EtcdNodeDirectory::register` 和返回的 registration 来管理 lease。
key 和 JSON value 如下；`revision` 与 `lease_id` 是 etcd 元数据，不属于 JSON value。
A provisioner can write the same schema, but should prefer `EtcdNodeDirectory::register` and the returned registration
for lease management. The key and JSON value are shown below; `revision` and `lease_id` are etcd metadata, not JSON
fields.

```text
/rr/{cluster_name}/directory/nodes/{node_id}
```

```json
{
  "node_id": 1,
  "client_addr": "rr-1.internal:51001",
  "admin_addr": "rr-1.internal:52001",
  "observability_addr": "rr-1.internal:9100",
  "advertise_addr": "rr-1.internal:51001",
  "environment": "production",
  "region": "us-east-1",
  "zone": "us-east-1a",
  "rack": null,
  "boot_id": "01JBOOT-ID-FROM-THE-NODE"
}
```

- schema 拒绝未知字段、key/value node ID 不一致、空 environment/boot ID 和非法地址。/ The schema rejects
  unknown fields, key/value node-ID mismatches, empty environment/boot IDs, and invalid addresses.
- 默认 lease TTL 为 15 秒；节点每 5 秒 heartbeat，优雅关闭时 revoke。heartbeat 只续租，不重写 descriptor JSON，
  因此不应推进 `mod_revision`。/ The default lease TTL is 15 seconds; nodes heartbeat every five seconds and revoke
  on graceful shutdown. Heartbeats only extend the lease and do not rewrite descriptor JSON or advance `mod_revision`.
- 注册使用 create-if-absent，已有同 node ID 记录会发生所有权冲突，不能无条件覆盖。过期 lease 必须由新
  registration 替换。/ Registration is create-if-absent: an existing record for the same node ID is an ownership
  conflict, not an overwrite opportunity. An expired lease requires a new registration.
- 外部 provisioner 若直接写 etcd，必须自行创建并续租 lease、处理冲突/revoke，并保证记录来自受信写入路径；
  不要写无 lease 的永久地址记录。/ A provisioner writing etcd directly must create and renew leases, handle
  conflicts/revocation, and protect the write path. Do not create permanent directory records without leases.
- `list` 默认每页 50、最大 200，cursor 固定目录 revision；watch compaction 会触发全量对账。/ `list` defaults to
  50 records per page with a maximum of 200, cursors pin the directory revision, and watch compaction triggers a full
  reconciliation.

### 崩溃重启与租约重试 / Crash-Restart Lease Retry

异常终止无法执行 revoke，旧 lease 可能仍在。重启遇到 `AlreadyExists` 时，节点每秒重试注册，等待旧 lease
自然过期；单次注册 deadline 为 10 秒，整个启动等待上限为 30 秒。旧记录过期后，以新 boot ID 创建新 lease，
不会覆盖仍属于其他存活 boot 的记录；持续冲突超过上限会使节点启动失败，而不是无限重试或静默跳过目录。
An abnormal stop cannot revoke its lease, so the old record may remain. On `AlreadyExists` during restart, the node
retries registration once per second while waiting for natural lease expiry. Each registration has a ten-second
deadline and total startup waiting is capped at 30 seconds. After expiry, the node creates a new lease with its new boot
ID instead of overwriting another live boot's record. A persistent conflict beyond the cap fails startup rather than
retrying forever or silently skipping the directory.

启动后的 heartbeat 失败会撤销/清理旧 registration、将目录 revision 归零，并在后续 5 秒周期尝试重新注册；
优雅关闭会 revoke。Console 侧的信任到期与凭证暂停语义见
[Cluster Admin Console](../operations/cluster-admin-console.md#发现与信任--discovery-and-trust)。
After startup, heartbeat failure revokes/cleans the old registration, resets directory revision to zero, and retries
registration on the five-second reconciliation cycle. Graceful shutdown revokes the lease. See [Cluster Admin
Console](../operations/cluster-admin-console.md#发现与信任--discovery-and-trust) for Console trust expiry and credential
pausing.

## 自定义提供者 / Custom Providers

自定义后端需实现 trait，将后端通知转换为变更流，并明确初始快照、断线、顺序和 lag 行为。`RrNode` 启动时读取表并运行 watch loop。
A custom backend implements the trait, converts backend notifications into the change stream, and defines snapshot, disconnect, ordering, and lag behavior. `RrNode` reads the table at startup and runs a watch loop.

## 托管 SeedSpec 与生命周期 / Managed SeedSpec and Lifecycle

配置 `[cluster.formation]` 后，节点从相同 SeedSpec 构造静态路由，并使用独立 `LifecycleStore` 授权 Genesis、恢复和旧集群收养。`[cluster.formation].etcd_endpoints` 属于生命周期存储，不会让 `EtcdConfigProvider` 变成可写成员表。
With `[cluster.formation]`, nodes build static routing from an identical SeedSpec and use a separate `LifecycleStore` to authorize Genesis, recovery, and legacy adoption. Formation endpoints belong to the lifecycle store; they do not make `EtcdConfigProvider` a writable membership authority.

- SeedSpec 的 groups/replicas 只描述初始路由和 Genesis 授权；当前 voter/learner 以 Raft membership 为准。/ Seed groups and replicas describe initial routing and Genesis authorization; current Raft membership is authoritative.
- managed 节点禁用旧 watch loop 的成员副作用；Active 记录下的空节点等待复制。/ Managed nodes suppress legacy watch-loop membership side effects; pristine nodes with an Active record wait for replication.
- 修改 SeedSpec 会改变 seed identity，并被现有生命周期记录拒绝；该路径不支持动态扩缩容或 resharding。/ Changing SeedSpec changes seed identity and is rejected by the existing record; this path does not support dynamic scaling or resharding.
- 存储中断不会清空路由：已初始化节点可继续 Raft 流量但 readiness degraded，空节点不初始化且不就绪。/ A store outage does not clear routing: initialized nodes may continue Raft traffic with degraded readiness, while pristine nodes neither initialize nor become ready.

## 旧模式动态行为 / Legacy Dynamic Behavior

以下副作用仅适用于未配置 `[cluster.formation]` 的旧模式节点：新分片可在本节点副本列表包含本节点时启动组；成员事件可调整或停止本地组；节点事件更新路由地址。
The following side effects apply only to legacy nodes without `[cluster.formation]`: new partitions can start groups when the local node is a replica; membership events can adjust or stop local groups; node events update routing addresses.

不要把这些 watch 副作用当作当前 Raft membership 的权威来源，也不要据此假定所有扩缩容操作都已自动完成。
Do not treat these watch side effects as the authority for current Raft membership or assume that every scaling operation completes automatically.
