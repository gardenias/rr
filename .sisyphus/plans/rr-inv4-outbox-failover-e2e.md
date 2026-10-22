# INV-4 集群级 outbox 换主续投 e2e 设计

> **状态**:**landed / closed**(2026-10-22 落地,O-1 ~ O-5 全部完成,默认特性全套件绿)
> **依据**:`docs/docs/reference/invariants.md` INV-4 已知缺口(:530-533)与台账行(:910);
> ADR `.sisyphus/adr/A-0007-output-outbox-mechanism.md`;`formal/tla/OutputWatermark.tla`;
> 可靠性专项 `.sisyphus/plans/rr-reliability-program.md`(R-4 / R-8)
> **格式参考**:`.sisyphus/plans/rr-reliability-program.md`
> **基线**:HEAD=`33938e0`;文中所有 file:line 引用均已对照该基线核实。

## 目的与原则

闭合 INV-4 的最后一个覆盖空白:outbox 在 leader 切换后的续投语义(不重不漏)从未在集群级
被端到端验证过——现有覆盖是逐环单测(`output_runner.rs` / `service_adapter.rs` 内)加 TLA 小模型
(`OutputWatermark.tla`,280k 态零反例,且模型把切换窗口的重复投递**显式建模为合法**)。本设计把
"换主续投"从设计断言变成可执行回归门禁。

原则(沿用可靠性专项):

1. **最小侵入**:全部控制面复用现有生产注入点(`KafkaProducer` trait、`StateMachineFactory`、
   `attach_output_runner`、`raft.trigger().snapshot()`),生产代码零改动;mock/捕获实现只进
   测试 crate。
2. **确定性优先**:时序窗口用机制保证(水线不经 raft 复制 ⇒ 窗口天然成立),不用固定 sleep;
   一切等待走轮询 + 超时。
3. **每个断言可证非空转**:重投臂必须同时断言"重复确实发生"与"重复被吸收",避免测试在
   机制失效时假绿。
4. **一套联动更新**:代码、invariants.md(现有测试/已知缺口/台账)、CHANGELOG、
   `scripts/check-invariants.sh` 同 PR 闭环。

## 关键机制事实(设计前提,已逐条核实)

- **水线不复制**:`advance_output_watermark` 是纯内存操作(group_manager.rs:1083-1100 →
  service_adapter.rs:169-179),只随快照传播(invariants.md:489-493)。因此健康 3 节点进程内集群中,
  follower 内存水线恒为 0,kill leader 后新 leader **必然从 seq 1 起重投全部 pending**——
  "已投递未快照"窗口不需要任何快照操控,确定性极强。
- **pending/next_seq 每副本一致**:`append_outputs` 在 apply 路径对每个节点确定性执行
  (service_adapter.rs:229-242),seq 从 1 起;重放的 output 幂等键与首次完全一致。
- **runner 是孤儿 API**:`attach_output_runner`(group_manager.rs:668-694)全仓无调用方,
  `RrNode::start` 不 attach(node.rs:409-410)。测试必须**逐节点显式 attach**;本测试同时成为该
  API 的首个真实调用方与回归锚。
- **Service 覆盖陷阱**:apply 路径实际调用 `handle_with_outputs_ctx`(service_adapter.rs:393-398),
  其默认实现委托 `handle_with_ctx` → `handle`,**不**转发 `handle_with_outputs`(service.rs:184)。
  测试 Service 必须 override `handle_with_outputs_ctx`,否则真实集群上 outputs 恒为空。
- **kill = 优雅 shutdown**:用 `leader_node.shutdown().await`(leader_redirect_failover.rs:65 先例),
  它经 `stop_group` 停掉老 leader 的 output runner(group_manager.rs:480-482),杜绝幽灵投递。
  **不可用** `FaultInjector::isolate` 模拟 kill——mock producer 不走 fault-aware pool,被隔离的
  老 leader 会继续投递,破坏断言前提。
- **快照后水线连续性在进程内测不到**:快照不主动复制给健康 follower,follower 自拍的快照水线
  也是 0;要让新 leader 带旧水线恢复需 InstallSnapshot(lag + purge)或磁盘重启。该臂归入
  O-4(rocks 重启),不在 O-2/O-3 强行构造。

## O-1 测试 harness:集群构造 + emitting service + 捕获 sink

**里程碑:0.2.1|工作量:1.5 人日|依赖:无**

### 为什么

`ClusterBuilder` 硬编码 `echo_factory()`(lib.rs:235/331),EchoSM 无 `outbox_control`,runner 会
报 `OutboxUnsupported` 永久 park(output_runner.rs:192-203)——builder 路线走不通。同时全仓没有
发 Output 的 Service 先例(examples 的 KvService/CounterService 都只实现 `handle`),也没有
`attach_output_runner` 的调用先例。这一层全部要新建,是后续两臂的地基。

### 做什么

新测试文件 `tests/integration/src/outbox_failover.rs`(注册 `#[cfg(test)] mod outbox_failover;`
进 lib.rs:22-39 模块列表),内含三件可复用设施:

1. `build_outbox_cluster(node_count, group_count, factory)`:照 `client_batching.rs:60-127`
   `build_service_cluster` 的独立文件模板(注册 partition/node addr、每节点
   `RaftGroupManager::new_with_network` + `ShardRouter` + `RaftGroupLookupImpl` +
   `ClientDispatchImpl` + `NodeConfig::with_group_manager(..).with_allow_insecure(true)`),
   factory 闭包按 `group_id` 分派 service。文件头注释标注骨架来源,防第三份拷贝漂移。
2. `OutboxEchoService`:实现 `Service` 四必选方法(snapshot/restore 用 EchoSM 式 8 字节计数即可),
   override `handle_with_outputs_ctx`:每条请求产出 1 条 output,`idempotency_key` 由请求幂等键
   确定性派生(`format!("{req_key}:0")`,与 output.rs:42-43 约定一致),target 由构造参数决定
   (KafkaPublish 或 RaftWrite)。**禁止**只 override `handle_with_outputs`(见机制事实)。
3. `CapturingKafka`:照 `output_runner.rs:421-468` 模板——
   `Arc<Mutex<Vec<(String, i32, Vec<u8>, Vec<u8>, String)>>>` 记录
   `(topic, partition, key, payload, idempotency_key)`,附 `fail_first_n` 脚本化失败;
   **3 节点共享同一 `Arc`**,attach 时逐节点 `Arc::clone`——只有 leader 的 runner 真正 publish,
   换主后新 leader 的记录进同一 Vec,天然汇总。

oracle 期望集由"幂等键由请求确定性派生"直接算得,无需 service 侧日志。

### 怎么做

- 每节点 `node.start().await?` 之后:
  `node.group_manager().attach_output_runner(1, rr_client, Some(Arc::clone(&kafka))).await?`
  (三节点都 attach;非 leader 自动 park,output_runner.rs:131-144)。
- 快照阈值不动(默认 `LogsSinceLast(50_000)`,rr-raft/src/lib.rs:11,测试规模永不自动触发);
  需要手动快照时 `node.group_manager().get_raft(1).await.unwrap().trigger().snapshot().await`
  + 轮询 `fetch_snapshot_bytes(1)` 确认物化(lib.rs:915-943 两段式先例;trigger 不等待完成,
  vendor/openraft/src/raft/trigger.rs:82-89)。
- 清理:`shutdown_all(&survivors)`(lib.rs:377),已单独 shutdown 的节点排除。

### 验收标准

- [x] 三节点 real-network 集群带 `OutboxEchoService` 起组,client 写 N 条后捕获 sink 收齐 N 条,
  `(topic/partition/key/payload/idempotency_key)` 五元组逐字段正确。
- [x] `mgr.fetch_snapshot_bytes(1)` + `FramedSnapshot::decode` 中间断言:leader 内存水线 == N,
  任一 follower 持久化水线 == 0(钉住"水线不复制"的机制前提,测试非空转的第一道证据)。
- [x] `cargo test -p integration outbox_failover` 本地 10 次连跑零 flaky。

## O-2 KafkaPublish 臂:换主重投 e2e(核心断言)

**里程碑:0.2.1|工作量:1 人日|依赖:O-1**

### 为什么

这是台账 :910 行点名的主场景。TLA 模型断言"切换落在 SendOk↔Advance 窗口 ⇒ 重复投递合法",
本臂在真实 3 节点系统里双向验证:**重复确实发生**(窗口真实存在)且**无一丢失**(at-least-once
成立)、**下游按键去重后恰好一次**(承诺语义兑现)。

### 做什么 / 怎么做

`test_inv4_outbox_redelivers_after_leader_failover_kafka`(命名/标注遵循 R-1 约定
`test_inv<n>_<desc>`,fn 首行 `/// INV-4: <不变量陈述>`,check-invariants.sh 解析规则见
scripts/check-invariants.sh:199):

1. O-1 集群(group 1 = OutboxEchoService,KafkaPublish target),`wait_for_cluster_ready` 得 leader L。
2. client 写 N=5 条(首写用 `write_when_lease_ready`,lib.rs:475,避 lease race)。
3. 轮询捕获 sink 至 ≥N 条(超时 5s)——老 leader 投递确认;此时**不触发任何快照**。
4. `nodes[L].shutdown().await`(优雅停机,runner 随 stop_group 停止)。
5. `wait_for_new_leader(&survivors, 1, L, 12_000)`,按 leader_redirect_failover.rs:78-91 模式
   对每个 survivor 单独确认。
6. 轮询 sink 至:唯一键集合 == 期望集 且 总条数 ≥ N+1(实际因 follower 水线为 0,会全量重投 N 条,
   总数 → 2N;断言写下界即可,不锁死上限)。
7. 断言:
   - **不漏**:`set(published.idempotency_key) ⊇ 期望集`;
   - **重投确实发生**:`∃ key: count(key) ≥ 2`(机制证据;全量重投时实际 ∀key 成立);
   - **不重(有效一次)**:按键去重后的 payload 集合与期望逐字节一致;
   - runner 时序预算:选举 150-300ms + idle poll 50ms + 首轮退避 100ms ⇒ 重投亚秒级发生,
     断言超时放宽到 10s 档。
8. 负面对照(同文件第二个测试,可选但强烈建议):新 leader 所在节点**故意不 attach** runner,
   断言 settle 窗口内 sink 无新增——证明 O-2 的正断言依赖 runner 而非空转。

### 验收标准

- [x] 正断言三件套(不漏/重投发生/有效一次)全绿;负面对照红→绿方向正确。
- [x] 全程无固定 sleep;`cargo test -p integration` 20 次连跑零 flaky(R-3 验收模式)。

## O-3 RaftWrite 臂:双组幂等吸收 e2e

**里程碑:0.2.1|工作量:1.5 人日|依赖:O-1(与 O-2 可同 PR 或紧随)**

### 为什么

Kafka 臂的"下游吸收"是 test double 模拟的;RaftWrite 臂验证的是 INV-4 声明的**框架级**去重:
重投经 `send_request_idempotent` + `rr.output.g<N>` 命名空间幂等键(output_runner.rs:382-391),
在目标组幂等层 decide → Replay,用户 Service 不再被调(service_adapter.rs:506-513)。这也是
review P0③(幂等键跨源组碰撞)修复的唯一 e2e 防线。注意 `OutputTarget::RaftWrite.addrs` 字段
被忽略(output_runner.rs:341),寻址由 attach 时传入的 `RrClient` bootstrap 决定。

### 做什么 / 怎么做

`test_inv4_outbox_raft_write_duplicates_absorbed_by_target_group`:

1. O-1 双组集群(同一 3 节点,group_count=2):group 1 = OutboxEchoService(RaftWrite{group_id:2}),
   group 2 = CountingService(lib.rs:510-553 模式,共享 `Arc<Mutex<Vec<Vec<u8>>>>` 记录 apply)。
2. 每节点 `attach_output_runner(1, Some(Arc::new(client)), None)`;client 由路由表 bootstrap,
   `RetryConfig { max_retries: 10, retry_delay_ms: 100 }`(覆盖双组同时重选主)。
3. 写 N=3 到 group 1 → 轮询目标组 apply 计数至 3N=9(每条 entry 在目标组 3 副本各 apply 一次,
   基线见 lib.rs:2812-2817 既有断言)。
4. kill 源组 leader(连带杀掉目标组一个副本,两组都重选主——`wait_for_new_leader` 对两组各等一次)。
5. **重投发生的观测口**:轮询新 leader `mgr.pending_outputs(1)`(group_manager.rs:1065,pub)——
   新 leader 初始 pending = 全量 N(水线 0),pending 只能靠投递成功推进水线来清空;
   `pending → 空` 即重投完成的证明,替代 runner 侧无返回值的盲区。
6. 断言:settle 后目标组 apply 计数**恒为 3N**(Replay 吸收,不再调 `handle`);
   计数中 payload 集合 == 期望集(不漏)。
7. 规模刻意小(N=3):默认幂等 LRU 10k 下 `Expired` 不可达,规避"重复变永久卡死"陷阱
   (rr-client IdempotencyExpired → runner 无限退避;该负向变体出范围,入风险登记)。

### 验收标准

- [x] 换主前后目标组 apply 计数差为 0,且 pending 先满后空(重投证据链完整)。
- [x] 与 O-2 同 PR 或紧随 PR 落地;20 次连跑零 flaky。

## O-4 rocks 重启臂:快照恢复水线连续性(rocks-gated)

**里程碑:0.2.1(可与 R-4 crash harness 合并验收)|工作量:1 人日|依赖:O-1;`integration/rocks` feature**

### 为什么

O-2/O-3 覆盖"换主续投",但"快照恢复后水线连续"这一半(已知缺口原文:"经快照恢复、续投不重不漏")
在进程内健康集群里构造不出来(见关键机制事实最后一条)。用 rocks 持久化 + 同 data_dir 重建
模拟节点重启,验证:**快照点之前已推进的水线不丢**(不重投 ≤ 水线的 seq)、快照点之后的 pending
仍被续投。

### 做什么 / 怎么做

`test_inv4_outbox_watermark_survives_snapshot_restart`(rocks-gated):

1. 单节点组,照 `test_restart_persistence`(lib.rs:1207)/ raft_log_durability.rs:48-81 `TestNode`
   模式:`RaftGroupManager::new_with_rocks(node_id, factory, base_path)`,attach runner + CapturingKafka。
2. 写 N 条 → 投递确认(sink ≥ N)→ `trigger().snapshot()` + 轮询 `fetch_snapshot_bytes` 物化
   → 再写 M 条 → 投递确认(sink ≥ N+M;**二次写后不快照**)。
3. shutdown,同 data_dir 重建 manager,重新 attach runner,等自选为 leader。
4. 断言:sink 总条数 = N+M + 至多 M 条重投——**seq ≤ N 的键零重投**(水线经快照存活),
   seq ∈ (N, N+M] 允许重投(快照点之后,at-least-once 窗口);去重后全集 == N+M(不漏)。
5. rocks 门控按 raft_log_durability.rs:10-14 内部门控模式(`#[cfg(all(test, feature = "rocks"))]`
   包内部 mod),避免非 test 构建的 dead-code 被 workspace clippy gate 打掉。

### 验收标准

- [x] `cargo test --workspace --features rr-storage/rocks,integration/rocks` 下绿;
  非 rocks 默认跑不受影响。
- [x] seq ≤ 快照水线零重投、快照点后允许重投、去重不重不漏,三断言齐备。

## O-5 文档闭环(与代码同 PR)

**里程碑:0.2.1|工作量:0.5 人日|依赖:O-2/O-3(O-4 可独立后续)**

### 做什么

1. `docs/docs/reference/invariants.md`:
   - `### 现有测试`(:506-526)追加条目,格式必须可被 check-invariants.sh 解析
     (`` `tests/integration/src/outbox_failover.rs` `test_inv4_...` `` 或 `` `name`(:NNN) ``;
     解析规则 :103-145,名字须含 `inv4`/`INV-4` 或 fn 正上方注释块带 marker);
   - `### 已知缺口` 第一条子条按既有样式改写 `**✅ 已闭环(2026-10-2X)**:集群级换主续投 +
     rocks 重启续投 e2e 落地`;第二子条(pending_outputs warn-only)**保持 open**;
   - 台账 :910 行加闭环注记(参照 :897-916 各行写法)。
2. `CHANGELOG.md` `## [Unreleased]` 新增 `### Added — <date> INV-4 outbox failover e2e`,
   条目注明 `(test_inv4_…, INV-4)`、"Registered in the INV-4 test list (check-invariants.sh
   enforced)"、负面对照写 "negative control verified"(范例 :598-605、:867-869)。
3. 本地跑 `scripts/check-invariants.sh` 验证 5 项检查全绿。
4. 台账 :910 行"设计文档"列回填本文件路径。

### 验收标准

- [x] check-invariants.sh 绿;invariants.md 三处更新与代码同 PR;CHANGELOG 条目格式符合约定。

## 汇总表

| ID | 项目 | 里程碑 | 工作量(人日) | 依赖 |
|---|---|---|---|---|
| O-1 | harness:集群构造 + emitting service + 捕获 sink | 0.2.1 | 1.5 | 无 |
| O-2 | KafkaPublish 臂换主重投 e2e(核心) | 0.2.1 | 1 | O-1 |
| O-3 | RaftWrite 臂双组幂等吸收 e2e | 0.2.1 | 1.5 | O-1 |
| O-4 | rocks 重启臂(快照恢复水线连续性) | 0.2.1 | 1 | O-1,rocks feature |
| O-5 | 文档闭环(invariants/CHANGELOG/lint) | 0.2.1 | 0.5 | O-2/O-3 |

**合计:约 5.5 人日**。执行顺序:O-1 → O-2(核心断言先行)→ O-3 → O-5(随 O-2/O-3 同 PR)→ O-4。

## 出范围(明确声明)

- **SIGKILL 崩溃语义**:本设计用优雅 shutdown;kill -9 随机点 + ack-log oracle 归 R-4 crash
  harness(tests/crash),本测试的 oracle 组件(捕获 sink / 去重校验)设计为可被 R-4 复用。
- **InstallSnapshot 携带水线的跨 follower 恢复**(lag + purge 构造):归 R-4/R-8 soak 故障循环,
  不在集成层强行构造。
- **Expired 锁死负向变体**(大量 output 冲掉幂等 LRU → 重投变永久卡死):是另一个独立的
  已知行为,建议单列 backlog,不在本测试混入。
- **rr-server 生产侧 producer 配置入口**:`attach_output_runner` 的孤儿 API 状态由本测试首次
  锚定;node bootstrap 暴露 producer 配置是真实部署需求,建议另立 backlog 项,不阻塞本设计。

## 风险登记

| 风险 | 影响 | 缓解 |
|---|---|---|
| 时序 flaky(选举 150-300ms + runner 50ms idle poll + 退避) | 测试噪声反噬信任 | 全程轮询 + 10-12s 超时;20× CI 连跑零 flaky 才转正;失败时 seed/日志可复现 |
| 只 override `handle_with_outputs` 导致 outputs 恒为空 | 测试假绿(零 output 也"不重不漏") | O-1 验收强制"sink 收齐 N 条"+ leader/follower 水线中间断言;代码评审 checklist 点名该陷阱 |
| `attach_output_runner` 孤儿 API 语义漂移 | 生产永远无人接线而不自知 | 本测试成为首个调用方即回归锚;登记出范围 backlog(生产 bootstrap 配置入口) |
| 双组同节点 kill 的连带重选主时序 | O-3 投递延迟超预期 | RetryConfig 10 次/100ms;两组分别 wait_for_new_leader;pending 轮询而非固定等待 |
| 快照 trigger 异步不等待 | O-4 窗口误判 | trigger 后一律 `fetch_snapshot_bytes` 轮询物化(lib.rs:915-943 两段式),不假设时序 |
| rocks 门控测试在默认跑被跳过 | O-4 形同虚设 | 沿用 lib.rs:6-16 既定 rocks 跑法;夜间/soak 入口包含 rocks feature 跑批 |
| 新 builder 骨架是第三份拷贝 | 三处漂移 | 文件头标注来源(client_batching.rs:60-127);后续提取公共 builder 列入技术债,本次不做 |

## 落地记录(2026-10-22)

O-1 ~ O-5 全部落地。默认特性 `cargo test -p integration` 全套件绿(83 passed / 0 failed,
含全部非门控 INV-4 臂);`scripts/check-invariants.sh` 绿(8 个 INV id、125 条
registry↔test 链接,含本次新增 5 条);`cargo fmt --check -p integration` 干净。

### 实际文件布局(相对设计的拆分)

设计 O-1 的单一 `outbox_failover.rs` 实际拆为共享 harness + 三个臂文件:

- `tests/integration/src/outbox_support.rs` —— 共享 harness:`build_outbox_cluster`
  (照 client_batching.rs:60-127 独立文件模板)、`OutboxEchoService`(override
  `handle_with_outputs_ctx`)/`outbox_echo_factory`、`CountingService`/`counting_factory`、
  三节点共享 Arc 的 `CapturingKafka`、`fetch_framed_snapshot`、`poll_until`、
  `kafka_echo_target` 等;三臂共同 import。
- `tests/integration/src/outbox_failover_kafka.rs` —— O-1 冒烟 + O-2 核心臂 + 负面对照
  三测试(:151/:279/:369)。
- `tests/integration/src/outbox_failover_raft_write.rs` —— O-3 臂(:216)。
- `tests/integration/src/outbox_restart_rocks.rs` —— O-4 臂(:192),按
  raft_log_durability.rs 模式 `#[cfg(all(test, feature = "rocks"))]` 内部门控。

### 两处有文档记录的设计偏离

- **O-3 证据链(outbox_failover_raft_write.rs:199-214 测试内注释)**:设计步骤 5
  "轮询新 leader `pending_outputs(1)` 先满后空"只在被 kill 节点兼任目标组 leader 时
  可直接观测;常见情形(被 kill 者只是目标组 follower)下目标组 leader 完好,runner 在
  选举后数毫秒内排空全部 N 条,快于测试线程的 50ms 轮询,"满"态永远看不到。因此"满"
  前提改为 kill 前在两个 follower 上确定性钉住(follower 水线恒 0 ⇒ pending 恒为全量 N,
  正是被测机制事实),"空"仍在换主后的新 leader 上观测。重投的第二重证明:目标组 leader
  `last_log_index` 增长 ≥ N(除 runner 外无人写 group 2)。
- **O-4 断言强度(outbox_restart_rocks.rs:326-333 测试内注释)**:设计原文"总数 =
  N+M + 至多 M 条重投"的不确定上界,落地为精确等式 `records.len() == N + 2*M`(sink
  无失败注入 + 每键计数互证,恰好一次重投 per post-snapshot 键);快照物化轮询因
  `outbox_support::fetch_framed_snapshot` 是 `RrNode`-bound 而本臂为裸
  `RaftGroupManager`(NoopNetwork、无 node),按 lib.rs:915-943 两段式先例在臂文件内
  以本地 helper 实现。

### 验证证据

- 默认特性全套件:`cargo test -p integration` → 83 passed / 0 failed
  (outbox_failover_kafka 三测、outbox_failover_raft_write 一测全绿;
  outbox_restart_rocks 在非 rocks 默认跑中按门控跳过)。
- rocks 门控臂:`cargo test -p integration --features integration/rocks
  test_inv4_outbox_watermark_survives_snapshot_restart` → 1 passed / 0 failed。
- 执法:`scripts/check-invariants.sh` → `invariants-lint OK`。
- 文档:invariants.md 现有测试 / 已知缺口 / 台账三处已更新;CHANGELOG 新增
  `### Added — 2026-10-22 INV-4 outbox failover e2e`。
