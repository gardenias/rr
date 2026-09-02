---
title: "TCP raft transport security gate"
status: implemented
created: 2026-08-28
updated: 2026-08-29
tags: [adr, security, transport, tcp, startup-validation]
---

# A-0014 — TCP Raft Transport Security Gate

> 工作项:roadmap WS-5(`.sisyphus/plans/rr-v0.2.0-release-roadmap.md`)。
> 依据:全量 review P0 #4 + Oracle Q2(`.sisyphus/evidence/rr-full-review-2026-08-16.md`,HEAD=`ba69cc8`)。

## Context

rr 的 inter-node Raft RPC(AppendEntries/Vote)有两种 transport:

- **gRPC(默认)**:由 `RrNode` 内嵌的 tonic server 承载 `RaftService`(`crates/rr-server/src/node.rs:432`),支持 TLS/mTLS;
- **TCP(实验性)**:`--raft-transport tcp`(`crates/rr-server/src/cli.rs:76-81`)启用轻量二进制 framing(`RRAF` magic + version,`crates/rr-network/src/tcp_raft_server.rs:13-20`),由 `TcpRaftServer` 承载。`InstallSnapshot` 不走 TCP,始终回退 gRPC(`crates/rr-raft/src/tcp_transport.rs:1-4` 模块注释),因此 TCP 模式下 gRPC `RaftService` 仍必须在线。

启动安全校验是 `validate_security_invariants`(`node.rs:36-108`),在 `RrNode::new` 中执行(`node.rs:162`),共 5 条 gate(raft 无 TLS、raft 有 TLS 但无 mTLS、bearer token 无 TLS、admin 无 TLS、admin 无 token),`allow_insecure` 在 `node.rs:37-39` 短路全部检查。

关键结构事实:

- TCP server 在 **rr-server binary 的 `start_node` 里** bind(`crates/rr-server/src/main.rs:210-232`),晚于 `node.start()`,完全在 `RrNode` 生命周期之外;
- bind 地址由 `replace_port(cfg.listen_addr, raft_tcp_port)` 派生(`main.rs:214`,host 沿用 `listen_addr`;`raft_tcp_port` 缺省 = gRPC port + 10000,`crates/rr-server/src/config_file.rs:304-315`);
- `NodeConfig`(`crates/rr-server/src/config.rs:18-104`)**没有任何字段描述 raft transport**,因此 `validate_security_invariants` 无从知道 TCP 栈的存在;
- `TcpRaftServer` 本身无 TLS、无 auth、无限流(全文 `tcp_raft_server.rs:1-171`,frame 协议没有任何身份字段)。

## Problem

5-gate 只校验 gRPC 面,TCP raft transport 完全在 gate 之外,形成两条明文路径:

1. **`--raft-transport tcp` + gRPC TLS/mTLS 齐全**:5 条 gate 全过,operator 有充分理由相信节点间 Raft 流量受 mTLS 保护——但实际的 AppendEntries/Vote 走**明文 TCP 端口**,且没有任何 peer 身份校验。任何能到达该端口的人都可以注入 Vote/AppendEntries frame,干扰选主与复制。这是"配置看起来安全、实际不安全"的最坏组合。
2. **`--raft-transport tcp` + `--allow-insecure`**:gate 全短路,TCP 可以 bind 在 `0.0.0.0` 等任意非 loopback 地址,零检查、零告警。

例外说明:`--raft-transport tcp` 且 gRPC 无 TLS 且不 allow_insecure 的场景,今天会被 gate 1 挡住(`raft_lookup` 恒为 `Some`,`main.rs:160-173`)——但挡住的理由是 gRPC 面,不是 TCP 面;一旦 gRPC 面合规(情形 1),TCP 面就裸奔。

这正是 review 元根因的又一实例:不变量("节点间共识流量必须有传输层身份保护")存在,执法者(5-gate)只盯着其中一层。

## Options

### Option A(采纳):gate 泛化为 transport-aware 的 config 级校验

`NodeConfig` 增加 raft transport 描述字段,`validate_security_invariants` 针对 TCP 增加两条规则:无 TLS 能力 ⇒ 要求显式 `allow_insecure`;bind 必须是 loopback(不可绕过)。即 Oracle Q2 的判定。

- 优点:执法点在 `RrNode::new`——与现有 5 条 gate 同一 choke point,binary 与 library 嵌入者同受约束;config 级校验在 bind 之前失败,错误信息可读;改动面小(config.rs + node.rs + main.rs 穿线)。
- 缺点:`TcpRaftServer` 作为 rr-network 的 public API 仍可被手工 bind 绕过(见 Decision §4,与现状 5-gate 对裸 tonic server 的性质相同,可接受)。

### Option B(明确否决):给 TCP transport 加 TLS

**0.2.0 不做,0.2.1+ 再议。** 否决理由:

1. **范围与风险**:自定义二进制 framing 上加 TLS 需要握手设计、证书管线、与 `TcpConnectionPool`(`main.rs:129`)的连接复用语义重做,属于发布前不可接受的风险;roadmap 明确"不给 TCP 加 TLS(0.2.1+ 再议,避免范围蔓延)"。
2. **不能消除 gRPC 依赖**:InstallSnapshot 始终走 gRPC 回退(`tcp_transport.rs:1-4`),gRPC TLS 栈反正要配——TCP 加 TLS 在 0.2.0 的边际安全收益低于其工程成本。
3. **TCP transport 的价值主张是性能对照**(benches / rr-perf-bench 的对比基线),loopback 限制不影响该用途;生产需求与 frame 级 peer auth 设计是 0.2.1+ 的准入条件,届时重开 ADR 评估。

### Option C(否决):只在 binary 的 `main.rs` 里校验

`TcpRaftServer` 恰好 bind 在 binary 层,顺手在那里检查最省事。否决:把 rr 当库嵌入的用户(`examples/kv-store` 等 fork 模式,`main.rs:1-6` 模块注释明示的推荐用法)完全绕过;且破坏"所有启动安全不变量集中在 `RrNode::new`"的单一 choke point 架构——5-gate 的错误聚合与 `allow_insecure` 语义都在那一处。

### Option D(否决):删除 TCP transport 或退回 feature-gate 实验品

TCP transport 已随 CLI ship(`--raft-transport`,cli.rs:76)且是性能对比工具;删除损失 benchmark 基线,gate 化成本远低于删除。它的问题不是存在,而是无守卫。

### Option E(否决):为非 loopback bind 提供额外逃生 flag(如 `--allow-insecure-tcp`)

对一个**零认证**的协议开逃生口,等于把漏洞戴上安全帽:任何拼错/抄来的部署配置都能一键回到裸奔。0.2.0 拒绝提供;待 TCP 有 auth/TLS 后该问题自然消解。

### Option F(否决):仅文档警示

文档不执法——这正是本次 review 的元根因("不变量存在但没有执法者"),不再重复这个错误。

## Decision

采纳 Option A。

### 1. `NodeConfig` 增加 transport 描述

新增字段(默认 gRPC,保持向后兼容):

```rust
pub enum RaftTransport {
    Grpc,
    Tcp { bind_addr: String },  // TCP raft server 的计划 bind 地址
}
// NodeConfig 新增: pub raft_transport: RaftTransport  // 默认 RaftTransport::Grpc
// builder: with_raft_transport(...)
```

`main.rs::start_node` 在构造 `NodeConfig` 时穿线:`RaftTransportKind::Tcp` 时填入 `replace_port(&cfg.listen_addr, tcp_port)`(与现有 `main.rs:214` 同一派生逻辑,单一事实源)。

### 2. 新增两条 gate(语义,非代码)

- **G6(TCP 必须显式 insecure)**:`raft_transport = Tcp` 时,无论 gRPC TLS/mTLS 是否齐全,都必须显式 `allow_insecure`,否则拒绝启动。错误信息需说明:TCP frame 协议没有 TLS 也没有任何形式的 peer 认证,0.2.0 中它是 dev/benchmark-only 特性。这条 gate 堵住 Problem 情形 1(gRPC 合规掩盖 TCP 裸奔)。
- **G7(TCP 只许 loopback)**:`raft_transport = Tcp` 时,`bind_addr` 解析为 `SocketAddr` 后 `ip().is_loopback()` 必须为真(IPv4 `127.0.0.0/8` 与 IPv6 `::1` 均覆盖;`0.0.0.0`/`::` 是 unspecified 不是 loopback,拒绝)。`listen_addr` 在 `config.rs:209-214` 已强制可解析为 `SocketAddr`,派生地址同样可解析,不会出现无法判定的情况。**G7 在 0.2.0 中不被 `allow_insecure` 绕过**——这是对 "allow_insecure 短路一切" 模式(`node.rs:37-39`)的首次有意的例外,理由是 TCP 面没有任何认证(见 Option E)。

实现形态:`validate_security_invariants` 重构为两段——先评估 G7(硬性,任何模式都执行),再按现有逻辑处理 `allow_insecure` 短路与其余 gate(含 G6 并入违规聚合列表,保持 "一次性列出全部违规" 的现有风格,`node.rs:46-105`)。`allow_insecure=true` 的启动 warn(`node.rs:164-179`)与 `rr_security_insecure_mode` gauge(`node.rs:187`)语义不变。

### 3. 现有 gRPC gate 行为不变

TCP 模式下 gRPC `RaftService` 仍在线(InstallSnapshot 回退需要),gate 1/2 对 gRPC 面的校验照常——G6/G7 是**增量**而非替代。`raft_transport = Grpc`(默认)的校验结果与现状逐字节一致。

### 4. 已知残余缺口(记录在案)

`TcpRaftServer::bind` 是 rr-network 的 public API,library 用户可以不经 `NodeConfig` 手工 bind——本 ADR 强制的不变量精确表述为:**"`rr-server` binary 与标准 `NodeConfig`/`RrNode` 路径,无法在非 loopback 地址上以明文 TCP 跑 Raft"**。与现有 5-gate 对"手工起裸 tonic server"的性质相同,可接受;`TcpRaftServer` rustdoc 补充安全警示(实施时)。

### 5. 与 WS-2 的协调

WS-2 的 K8s/Helm 修复为无 TLS 场景提供显式 `RR_ALLOW_INSECURE=true` 开关;那些制品默认 `raft_transport = grpc`,不受 G7 影响。若未来部署制品要暴露 TCP transport,必须同时把容器 bind 约束在 loopback——这在 K8s 里几乎等价于"不可用",再次印证 TCP 在 0.2.0 是 dev/benchmark 特性。

## Consequences

### Positive

- 不变量"节点间共识流量必须有传输层身份保护"对两种 transport 都有执法者;"gRPC 合规掩盖 TCP 裸奔"的配置组合从结构上消失。
- 检查在 bind 之前失败,错误信息一次性列出,运维一遍修完(沿用现有聚合风格)。
- TCP transport 的 dev/benchmark 用途(loopback 单机/CI)不受任何影响。
- 为 0.2.1+ 的 "TCP TLS/auth" 讨论留下干净的基线:届时 G6/G7 随能力上线而放宽,而不是拆除临时补丁。

### Negative

- `RrNode::new` 的 gate 逻辑出现第一个 "allow_insecure 不可绕过" 的分支,`allow_insecure` 语义从"短路一切"变为"短路除 G7 外的一切"——必须在 `NodeConfig::allow_insecure` rustdoc(`config.rs:77-103`)与错误信息中写明白,避免 operator 困惑。
- `NodeConfig` 与 `validate_security_invariants` 的复杂度略增(一个 enum + 两条规则)。

### Neutral / 对 v0.2.0 的 breaking 判定

- **行为 breaking(窄面)**:以下两类当前可启动的配置,0.2.0 起被拒绝:
  1. `--raft-transport tcp` + gRPC TLS/mTLS 齐全、无 `allow_insecure`(被 G6 拒);
  2. `--raft-transport tcp` + `allow_insecure` + 非 loopback bind(被 G7 拒)。
  可接受:0.2.0 尚未发布(crates 仍为 0.1.0,API freeze 在 WS-3),TCP 是实验性 transport,且拒绝理由为安全修复;必须进 CHANGELOG 0.2.0 breaking 清单与 release notes。
- **wire / API 兼容**:不动任何 proto、不动公开 trait;`NodeConfig` 是 rr-server 的内部配置 struct(非公开 API 面),新字段有默认值,library 嵌入者源码兼容。
- gRPC 默认路径行为零变化(回归测试保证)。

### 配套测试方案

1. **单元测试(rr-server::node,沿用 `node.rs:993-1111` 的现有 gate 测试模式)**:
   - TCP + gRPC TLS/mTLS 齐全 + 无 `allow_insecure` → 拒绝,错误信息含 TCP 与 `allow_insecure` 指引(核心回归:证明 gRPC 合规不再掩盖 TCP);
   - TCP + `allow_insecure` + loopback bind(`127.0.0.1` / `::1`)→ 通过;
   - TCP + `allow_insecure` + `0.0.0.0` / 非 loopback → **仍拒绝**,且错误信息明示 `allow_insecure` 不能绕过 loopback 规则;
   - TCP + 无 `allow_insecure` + 无 TLS → 拒绝(与 gate 1 叠加,聚合信息含两条);
   - 现有 8 个 gate 测试(`validate_security_*`)在 Grpc 默认下逐个字节不回归。
2. **config 解析测试(rr-server::config_file,沿用 `:840-871` 模式)**:TOML `[cluster].raft_transport = "tcp"` + `[node].raft_tcp_port` 经 `ResolvedConfig::resolve` 后,bind 地址正确派生并被 transport 描述携带。
3. **启动测试(验收标准)**:构造 `NodeConfig` + `RaftTransport::Tcp { bind_addr: "0.0.0.0:<port>" }`、无 `allow_insecure`,`RrNode::new` 返回 `Err`,错误信息可读(指明 TCP 无认证能力、需要 `--allow-insecure` 且 bind 必须是 loopback);同配置加 `with_allow_insecure(true)` 仍 `Err`(G7 不可绕过);改 `127.0.0.1` 后 `Ok`。若实施时评估成本可接受,可加一条 binary 级冒烟:`rr-server serve --raft-transport tcp --listen-addr 0.0.0.0:...` 进程非零退出且 stderr 含指引文本。
4. **TCP 正路径不回归**:loopback + `allow_insecure` 下 TCP raft 集群(现有 tcp transport 测试)保持绿。

## Changelog

| Date | Change |
|------|--------|
| 2026-08-28 | Initial: `validate_security_invariants` 泛化为 transport-aware——`NodeConfig` 新增 `RaftTransport` 描述;G6(TCP 必须显式 `allow_insecure`,不论 gRPC TLS 状态)+ G7(TCP bind 必须 loopback,`allow_insecure` 不可绕过);**明确否决给 TCP 加 TLS**(0.2.1+ 再议,理由三条);否决 binary-only 校验、删除 transport、额外逃生 flag、纯文档方案;判定对 v0.2.0 为窄面行为 breaking 并划定配套测试。 |
| 2026-08-29 | Implemented(WS-5):按 Option A 落地。G7 硬 gate 在 `crates/rr-server/src/node.rs:46-69`,前置于 `allow_insecure` 短路(`node.rs:71-73`)——不可绕过,解析失败的 bind 地址同样 fail-closed;G6 在 `node.rs:127-143`,并入违规聚合列表保持"一次性列出全部违规"风格。`RaftTransport` 描述字段在 `crates/rr-server/src/config.rs:29`(默认 `Grpc`,行为逐字节不变);`main.rs:161-176` hoist `replace_port` 派生为单一事实源,同时喂给 `NodeConfig::raft_transport`(门控)与 `TcpRaftServer::bind`(实际绑定)。测试:6 条 TCP gate 单测(`node.rs:1163-1282`,含 gRPC TLS 合规不掩盖 TCP、`0.0.0.0`/不可解析地址 fail-closed、聚合信息)+ 配置解析三层测试(`config_file.rs:838-934`,CLI/TOML/优先级与 bind 派生);现有 9 条 gate 测试零回归。 |
