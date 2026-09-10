# rr 可靠性强化专项计划(R-1 ~ R-8)

> **状态**:**全部关闭**(2026-08-28 制定;R-1~R-8 于 2026-09-05~10 全部落地,各批次验收记录见 CONTEXT.md;时间累积型验收——R-3 CI 20× 零 flaky、R-8 2h+ soak 绿、fuzz 首次有网 nightly——由 nightly CI 承载)
> **依据**:`.sisyphus/evidence/rr-full-review-2026-08-16.md`(11 个 P0、P1/P2 清单、Oracle 5 问)+ `.sisyphus/plans/rr-v0.2.0-release-roadmap.md`(Phase 1 索引表)
> **格式参考**:`.sisyphus/plans/rr-wal-impl.md`(分阶段 checkbox + 验收标准 + 风险登记)
> **基线**:HEAD=`ba69cc8`;文中所有 file:line 引用均已对照该基线核实。

## 目的与原则

本计划是 v0.2.0 路线图 **Phase 1 — 可靠性专项** 的实施文档。路线图确立的编排逻辑不变:**先让 CI 说真话(Phase 0),再修正确性三连,RC 期间并行建可靠性体系**。本专项解决 Oracle 判定的元根因——"不变量 X 存在,但 layer Y 没强制;功能在涨,横切不变量没有执法者"。

原则(与路线图一致,并作为本专项内取舍依据):

1. **正确性 > 性能 > 功能**。性能类项目(R-7)的价值在于给正确性改动(WAL、WS-4 batcher 改造)提供回归判据,而非追求数字本身。
2. **每项可靠性机制必须自带可执行验证**:不变量配测试、形式化配 model-check 运行方式、注入配 oracle。不允许"文档宣称但没有强制点"的新增条目。
3. **非阻塞先行**:TLA+、fuzz、性能门禁、soak 均以非阻塞 CI 形态起步,积累可信度后再晋升为门禁,避免噪声反噬开发节奏。
4. **最小侵入**:能复用现有抽象(`ChannelPoolLike`、`StorageEngine`、criterion、rr-perf-bench)就不新建框架。

与 8-16 review 的关系:P0/P1 的**点状修复**归 Phase 0(WS-1~WS-7)与 RC 并行项(WS-8 等);本专项是把同类问题**制度化防复发**的部分。与 WS-4 的衔接:R-4 复用 `.sisyphus/plans/rr-durability-hardening.md`(另一工作流编写)的 `close()` 协议,本文件只做引用、不重复设计。R-1 的交付物 `docs/docs/reference/invariants.md` 由 WS-8 工作流编写,本文件只设计其**执法机制**。

---

## R-1 不变量目录执法机制

**里程碑:RC|工作量:2~3 人日|依赖:WS-8(invariants.md v1 落地)**

### 为什么

review 元根因:"安全 gate 只管 gRPC、durability 靠隐式 drop-drain、exactly-once 链条某环静默回退"——11 个 P0 里至少 5 个(P0#3/#4/#5/#6/#7)是"不变量存在但无强制点"模式。WS-8 交付 `invariants.md`(每条:不变量陈述 → 强制点 file:line → 现有测试 → 已知缺口),但文档本身不防漂移:v0.1.0 时代 docs 系统性漂移(review P1)已证明"写了文档没人维护"是常态。R-1 要让**新增对外属性必须先登记不变量、每条不变量必须有显式关联测试**成为流程硬约束。

### 做什么

- PR 模板与 review 清单中增加不变量检查项;
- 建立不变量编号(`INV-n`)与测试代码的双向链接约定;
- 加一个轻量 CI lint(脚本级,非新框架),校验双向链接完整。

### 怎么做

1. **PR 模板检查项**。在 `.github/pull_request_template.md` 的 Checklist(现有 8 项,:42-50)追加两条:
   - `[ ] 本 PR 若新增/变更对外宣称的属性(可靠性、一致性、durability、安全 gate、兼容承诺),已在 docs/docs/reference/invariants.md 登记或更新对应 INV 条目(强制点 + 关联测试);若纯内部重构,勾选 N/A 并说明`
   - `[ ] 本 PR 新增/修改的不变量相关测试,测试名或 doc comment 首行含对应 INV-n 编号`
   同时在 Testing Performed 段落(:25-33)的"protocol/persistence changes"分支下补一行:`invariants.md 中受影响的 INV 条目编号:____`。
2. **code review 清单**。在 `CONTRIBUTING.md` 的 reviewer 段新增小节"Invariant enforcement review",列 4 条审查问题:(a) 该改动是否改变了任何对外可观察保证?(b) 若改变,invariants.md 是否同步?(c) 声称的强制点(file:line)是否真实存在(抽查)?(d) 关联测试是否真的会在 CI 跑(对照 WS-1 教训:feature 未传播=测试假绿)?
3. **测试命名/标签约定**。Rust 无原生测试标签机制,采用**命名约定 + doc comment**双轨:
   - 测试函数名:`test_inv<n>_<short_desc>`(如 `test_inv3_acked_writes_survive_abrupt_drop`)。存量测试不强制改名,在 doc comment 首行加 `INV-n: <不变量陈述>` 即可计入关联。
   - invariants.md 每条 INV 必须列出至少一个关联测试名;反之,任何含 `INV-n` 标记的测试必须能在 invariants.md 找到条目。
4. **CI 校验脚本** `scripts/check-invariants.sh`(新文件,挂入 CI 的 `fmt` job 或独立 `invariants-lint` job,均为阻塞式,因为纯文本检查零噪声):
   - 从 `docs/docs/reference/invariants.md` 提取全部 `INV-\d+` 编号与其声明的测试名;
   - `grep -rn "INV-\d+" --include="*.rs" crates/ tests/` 反向提取代码侧引用;
   - 双向 diff:有 INV 无测试引用 → fail;代码引用了不存在的 INV 编号 → fail;INV 条目中列出的测试名在代码中 grep 不到 → fail(防改名漂移)。
5. **登记触发规则**(写入 invariants.md 头部,作为 living document 的元规则):凡 CHANGELOG `### Added` 中出现能力性宣称、README/docs 中出现 guarantee/must/never 级措辞、新增 pub API 带语义承诺,三者任一命中即触发登记义务。

### 验收标准

- [ ] PR 模板与 CONTRIBUTING.md 更新合入;
- [ ] `scripts/check-invariants.sh` 入 CI 且对当前 invariants.md v1 全绿;
- [ ] 抽查实验:故意删掉某 INV 的测试引用,CI 变红(用一次临时分支验证后丢弃);
- [ ] invariants.md v1 的全部条目(≥8 条 advertised 属性)均有关联测试且测试在 CI 实际执行(对照 job 日志确认,不依赖"应该有")。

---

## R-2 TLA+ 形式化(rr 自有扩展协议)

**里程碑:规格 1 随 WS-6 之后启动(RC 末),其余跨 0.2.1→0.3.0|工作量:6~8 人日|依赖:R-1(invariants.md 条目作为 spec 输入)**

### 为什么

review 的 P0#7(change-membership 全链路无 quorum guard)与 P0#6(offset→request_id 静默回 0 击穿 exactly-once)属于**协议级**缺陷:单测能覆盖"某条路径对",覆盖不了"所有交错下都对"。openraft 核心已有上游验证(Verdi 谱系 + 自身测试),不重复;rr 的自有扩展层(membership guard、路由表切换、output watermark、offset 链)恰好是无形式化保护的增量,也是多步跨层协议,最适合 TLA+ 的性价比区间。

### 做什么

对 4 个 rr 自有扩展协议各写一份 TLA+ spec + TLC model,明确验证的安全/活性性质。**不做**:openraft 核心、gRPC wire 编解码、RocksDB 内部。

### 怎么做

1. **存放位置**:仓库根新目录 `formal/tla/`(与 `fuzz/` 平级,均为质量设施且不进 workspace 编译):
   ```
   formal/tla/
   ├── README.md                  # 如何跑 TLC(tla2tools.jar / docker)、state constraint 约定
   ├── MembershipGuard.tla / .cfg
   ├── OffsetExactlyOnce.tla / .cfg
   ├── OutputWatermark.tla / .cfg
   └── RoutingTableSwitch.tla / .cfg
   ```
   每份 spec 头部注释双向链接:对应 invariants.md 的 `INV-n` + 实现 file:line。
2. **规格编写顺序**(按"正确性风险 × 与在研代码的耦合度"排序,先写正在被改的,能直接反哺 WS 验收):
   - **① `MembershipGuard.tla`**(对应 WS-6 / ADR A-0012)。Model:3~5 节点、并发 `ChangeMembersip` 请求、guard 允许/拒绝逻辑、`force` 旁路。Safety:`\A t: 任一时间点,已提交的 voter 集合满足 quorum 存在性`(除非 force 且 audit 已记录);活活性:合法变更最终可完成。重点抓"两个并发变更各自通过 guard、叠加后丢 quorum"这类交错。
   - **② `OffsetExactlyOnce.tla`**(对应 review P0#6,`rr-ingestor-kafka/src/lib.rs:74-78` + `rr-raft/src/idempotency.rs` 的 watermark/LRU 语义)。Model:offset 流(含乱序/重复/非法)、`offset_to_request_id` 映射、idempotency watermark 推进、LRU 驱逐。Safety:同一 `(request_type, request_id)` 不产生两次不同 apply 结果;非法输入不产生"回退到 0 导致误中既有 key"的状态。这份 spec 应与 WS-4 把 `kafka_offset_to_request_id` 改为返回 `Result` 的改造同步定稿。
   - **③ `OutputWatermark.tla`**(对应 A-0007;实现点 `crates/rr-raft/src/service_adapter.rs:146-171` 的 `SharedOutboxControl` 与 `output_runner.rs:171-221` 的发送循环)。Model:output 产生(seq 分配)、leader 切换、发送成功/失败、watermark 推进。Safety:watermark 单调;`seq <= watermark` 的 output 至多已被投递(允许重复——at-least-once 是承诺语义,spec 要把"允许重复、不允许静默丢失"显式写成性质而非隐含假设)。Liveness:持续成功下 watermark 最终推进到 next_seq。
   - **④ `RoutingTableSwitch.tla`**(对应 T32/T33 语义,`rr-router/src/router.rs:34` 的表查询 + ConfigProvider watch 传播)。Model:路由表版本切换、各节点在不同版本上的窗口期、客户端请求路由。Safety:任一请求在其生效的版本上恰好路由到一个 group(不双写、不丢失);切换收敛后无请求落到旧 group。
3. **state constraint**:每个 spec 配小型 .cfg(节点 ≤3、请求数 ≤5、版本 ≤2),保证 TLC 在 CI 10 分钟内跑完;完整状态空间留本地手动跑。
4. **CI 接入(非阻塞)**:新 workflow(如 `.github/workflows/formal.yml`),`schedule` 每周 + `workflow_dispatch`,`continue-on-error: true`。runner 用 ubuntu + 下载 `tla2tools.jar` 跑 `tlc2.TLC`。失败时上传 TLC 输出 artifact;由人工分类为"spec 滞后于实现"(更新 spec)或"实现 bug"(开 issue 转 P 级)。
5. **漂移防护**:R-1 的 PR 检查项加一句——改动上述 4 个协议实现文件的 PR,reviewer 需确认对应 spec 是否仍成立(抽查,不强制每次改 spec)。

### 验收标准

- [ ] 4 份 spec 全部 TLC 无反例(小状态空间);
- [ ] 每份 spec 与 invariants.md 条目、实现 file:line 双向链接齐全;
- [ ] formal workflow 在 CI 以非阻塞形态运行 ≥2 周无系统性误报;
- [ ] 至少一次"spec 抓到实现/设计问题"或"故意植入反例被 TLC 抓到"的实证记录(写入 spec 目录 README)。

---

## R-3 确定性仿真测试(turmoil 评估试点)

**里程碑:0.2.1(spike 时间盒 2 人日;通过判据后推广 2~3 人日;转替代方案 1~2 人日)|依赖:无硬依赖,与 WS-4 无冲突**

### 为什么

review P1 指出集成测试"38 处 sleep + 固定端口(51000+node_id@`tests/integration/src/lib.rs:175`、19204@:1023),flaky 风险"。已核实:`build_real_network` 主路径已用 `127.0.0.1:0` 动态端口(:214-221),但 sleep 等待(选举 100ms、收敛 2000ms 等)仍是墙钟语义——CI 慢机器上超时即 flaky,失败不可复现。chaos 测试(E2a~E3c、E5,lib.rs:1611-2088)基于 `FaultAwareChannelPool`(`tests/integration/src/fault_injection.rs`),故障注入靠 `rand::random`,同样是不可回放的一次性随机。确定性的价值:**失败可用同一 seed 逐字节回放、无 flaky、模拟时钟让秒级选举在毫秒级跑完**(可大幅提高故障场景覆盖密度)。

### 做什么

评估 [turmoil](https://crates.io/crates/turmoil)(tokio 确定性网络仿真)对现有 harness 的适配路径;跑一个最小试点;若成本超预期,落地"确定性 fault schedule 记录/回放"替代方案。

### 怎么做

**适配面分析(已核实的结构事实)**:

- 天然抽象点存在:`rr-raft/src/network_factory.rs:248` 的 `ChannelPoolLike` trait,`GrpcNetworkFactory::new(Arc<dyn ChannelPoolLike>, group_id)`(:373),`RaftGroupManager::new_with_network`(:116)——`FaultAwareChannelPool` 正是经此注入的。
- **核心矛盾**:`ChannelPoolLike::get_or_connect` 返回 `tonic::transport::Channel`,turmoil 不提供 tonic Channel;turmoil 的 `net::TcpStream` 需要经自定义 connector 接入 tonic(`Endpoint::connect_with_connector` 类 API)。抽象需从"给 Channel"下沉到"给 stream"。
- turmoil 的 host 模型要求每个节点一个 `sim.host(...)`;现有 `ClusterBuilder` 是单进程多 `RrNode` 共享一个 runtime,需重构为 per-host 启动。
- tokio 时间在 sim 内被劫持为模拟时钟——选举/heartbeat/sleep 全部确定性化。

**试点设计(时间盒 2 人日)**:

1. **移植目标**:`test_follower_isolation_and_catchup`(lib.rs:1648)。选它因为:逻辑最简单(isolate → 写 10 条 → restore → 断言追平)、不经过 client SDK(直接 `raft.client_write`,避开 `RrClient` 的 tonic client 侧适配)、断言基于 raft metrics 而非 sleep 计数。
2. **需要抽象出的 trait**(最小切口):在 rr-raft 增加一个 `StreamConnector` 层——`async fn connect(&self, node_id: NodeId, addr: &str) -> io::Result<Box<dyn AsyncReadWrite>>`,生产实现走 tokio `TcpStream`+TLS,仿真实现走 turmoil stream;`ChannelPool`/`GrpcRaftNetwork` 改为经 connector 建 Channel。**技术前提验证点**(spike 第一天必须回答):tonic 0.14 是否仍暴露 `connect_with_connector`(或等价 service-based connect);若否,适配成本立即超预算,直接转替代方案。
3. **通过判据**(全部满足才推广):(a) 试点测试在 sim 内跑通;(b) 同一 seed 两次运行事件序列逐字节一致;(c) 模拟时钟下运行时长 < 墙钟版的 1/10;(d) 改造不破坏现有 `ChannelPoolLike` 全部调用方(`cargo test --workspace` 全绿)。
4. **推广(2~3 人日)**:按 E2a → E3b(split-brain 防护,价值最高)→ E5 顺序移植;sleep 等待统一替换为 sim 内的条件轮询。

**替代方案(若 spike 失败,1~2 人日)——确定性 fault schedule 记录/回放**:

- `FaultInjector` 增加 schedule 模式:测试启动时用**显式 seed** 的 RNG 生成 `Vec<(offset_ms, FaultEvent)>`(isolate/restore/packet_loss/latency),测试全程把实际生效的事件序列写 JSONL;
- 失败时 CI 上传 JSONL artifact;本地 `RR_FAULT_REPLAY=<file>` 精确重放同一序列;
- 配合把 38 处固定 sleep 逐步替换为已有先例的条件轮询(`wait_for_cluster_ready` 等,lib.rs:352)。
- 理由:不动网络栈与 runtime,成本约为 turmoil 全量适配的 1/5,拿到"可回放 + 无随机 flaky"80% 的价值;代价是无模拟时钟,测试时长不降。

### 验收标准

- [ ] spike 报告(`.sisyphus/evidence/` 下,记录 tonic connector 验证结论、工作量实测、通过判据逐项结果);
- [ ] 二选一落地:试点 chaos 测试 turmoil 化且 seed 回放一致;**或** FaultInjector 的 schedule 记录/回放上线,至少 E3b/E5 两个测试接入;
- [ ] 移植/接入后的测试在 CI 连续 20 次运行零 flaky(用 CI 重复触发验证)。

---

## R-4 crash-recovery harness + 存储故障注入

**里程碑:0.2.1(L1 wrapper 可在 RC 后先行)|工作量:4~5 人日|依赖:WS-1(rocks 测试在 CI 真正运行)、WS-4 的 `close()` 协议(场景 A 需要)**

### 为什么

review P0#5("shutdown durability 靠隐式 drop-drain,不可证明")与 Oracle Q1("侥幸安全不成立")。现有 `test_crash_during_write_preserves_acked_entries`(lib.rs:1924-1988)是 in-process drop 模拟 kill -9——**已核实的局限**:同一进程内 drop 无法模拟 OS 级崩溃的任意指令点中断;单轮、单节点;oracle 只查 `last_log_index >= max_acked`,不验证状态机层 exactly-once;且因 WS-1 的 feature 传播问题在 CI 从未运行。此外整条存储路径(fsync 失败、torn write、ENOSPC、慢盘)**零故障注入覆盖**。

### 做什么

- 真进程级 crash harness:SIGKILL 随机点 + 重启 + "已 ACK 必存活" oracle;
- `StorageEngine` 故障注入包装层,被单元/集成测试复用;
- 与 WS-4 close 协议衔接,区分优雅关闭与 kill 两类契约场景。

### 怎么做

**1. crash-recovery harness(新 crate `tests/crash/`,workspace member,`publish = false`)**:

- **进程编排**:spawn `rr-server` 子进程(3 节点,`--storage rocks --durability strict`,per-node `TempDir` data-dir)。直接复用 `tools/rr-perf-bench/src/cluster.rs` 已验证的模式:端口布局函数(:20-29)、`spawn_server`(:280)、`wait_tcp` 探活(:362)。为 harness 增加 `kill_node(id, SIGKILL)` 与 `restart_node(id)`(restart 复用同一 data-dir)。
- **client driver**:独立任务以固定速率发 keyed 幂等写(`FramedEntry` + `IdempotencyKey`,沿用 lib.rs:2112 的既有用法);每条 `Ok` 立即向 **ack log**(append-only JSONL,`(request_id, payload_hash)`;ack log 自身 fdatasync)追加。ack log 即 oracle 数据来源——"ACK 边界"由 client 侧观测定义,与 WS-4 的"fsync 后才 ACK"契约精确对齐。
- **killer**:seed 化 RNG 在随机 wall-clock 点(含"正在大批量写入"窗口)对随机节点发 **SIGKILL**;每轮记录 seed 到测试日志,失败可复现(与 R-3 替代方案的回放设施同构)。
- **oracle(重启后)**:等节点 rejoin 并追平,回放 ack log 中每条 `(request_id, payload_hash)`:用相同 key 重放写,必须返回 `IdempotencyDecision::Replay` 且 payload 哈希一致。**这比查 log index 强**:同时验证日志存活、状态机不重复 apply、幂等层跨重启有效,即 exactly-once 的 durability 面。附注:vote 持久性沿用现有断言方式(lib.rs:1980-1987)。
- **与现有测试的关系**:保留 `test_crash_during_write_preserves_acked_entries` 作快速冒烟;harness 是其严格升级版(真 SIGKILL、多轮 kill/restart、状态机级 oracle),不重复替代。
- **两类场景**:
  - 场景 A(SIGTERM 优雅关闭):依赖 WS-4 的 `IoBatcher::close()`(drain→final fsync→应答 pending→退出)与 `stop_group` await——oracle 升级为"**已提交到 batcher 的 pending 也全存活**"(强于 kill 场景);
  - 场景 B(SIGKILL):只承诺"已 ACK 必存活",pending 未 ACK 可丢。两场景都跑,契约差异即 WS-4 交付的验证。

**2. 存储故障注入包装层**:

- **包装哪个 trait**:`rr_core::storage::StorageEngine`(`crates/rr-core/src/storage.rs:27`,7 个方法:`put/get/delete/scan/write_batch/flush/create_cf`)。实现 `FaultInjectingStorage<S: StorageEngine>` newtype,支持四种故障(均可按概率/延迟/触发次数编程,seed 化):
  - `flush()` 返回 `StorageError::Io`(fail fsync);
  - `write_batch` 只应用 ops 前缀后返回 Err(torn write——注意 `MemStorage::write_batch` 恰好有"部分应用不回滚"语义,review P1 `mem.rs:86-103`,wrapper 将其显式化为可配置故障而非依赖 bug);
  - 写路径返回 ENOSPC 类 `StorageError::Io`;
  - 全部方法前注入可配延迟(慢盘)。
- **放置**:`crates/rr-core/src/storage_fault.rs`,挂在新 feature `test-utils` 下(参照现有 `defensive` feature 的先例);tests/integration 与 crates 单测经 dev-dependency + feature 复用。
- **已知 gap(必须写明)**:`IoBatcher::new(storage: Arc<RocksStorage>, ...)`(`crates/rr-raft/src/io_batcher.rs:114`)持有**具体类型**而非 `Arc<dyn StorageEngine>`,wrapper 插不进 group-commit 路径。两个选项:(a) 推荐——WS-4 改造 batcher 生命周期时顺手泛化为 `Arc<dyn StorageEngine>`,wrapper 即覆盖最热写路径;(b) 兜底——harness 层用真磁盘故障(`fallocate` 预填 data-dir 所在文件系统模拟 ENOSPC;cgroup/io.max 限速模拟慢盘)。本计划按 (a) 优先排期,(b) 保留为 harness 场景 C。
- **分级**:L1 = wrapper(StorageEngine 级,单元/集成测试,CI 每 PR 跑);L2 = 进程级 SIGKILL + ENOSPC(harness,nightly);L3 = FUSE/dm-delay(可选,本地手册,不进 CI)。

### 验收标准

- [ ] `tests/crash/` 在 CI(先 nightly,后随 WS-1 修复进常规 job)跑通:单轮 3 节点、≥10 次随机 SIGKILL/restart 循环,oracle 零失败;
- [ ] 场景 A 能证明 WS-4 close 协议:SIGTERM 后 batcher pending 零丢失(该测试同时是 WS-4 的验收证据之一);
- [ ] `FaultInjectingStorage` 四种故障各有 ≥1 个消费它的测试(fail fsync 后重启恢复、torn write 后恢复不产出半条 entry、ENOSPC 下错误向上传播而非 panic、慢盘下无超时误判为数据丢失);
- [ ] 失败复现:同一 seed 重跑得到同一 kill 序列。

---

## R-5 线性一致检查器(porcupine 风格)

**里程碑:0.2.1|工作量:3~4 人日|依赖:无(消费 R-3 的 chaos 场景,但不阻塞)**

### 为什么

现有 `test_concurrent_writes_under_chaos_linearizability`(lib.rs:2009-2088)注释自述是 "pragmatic Jepsen-style stress test rather than a full linearizability checker"——它只断言 `last_applied >= ack_count`(不丢已 ACK 写),**查不出**:读到旧值(stale read)、读-修改-写交错下的寄存器语义违反、重复 apply 后的值错乱。这些正是 linearizable read(A-0009)上线后对外宣称的属性,需要真 checker。

### 做什么

实现 porcupine 风格的 Rust 线性一致检查器,模型为 kv-store register;历史从集成测试 client 侧采集;接入 CI。

### 怎么做

1. **模型**:`examples/kv-store` 作为载体,对单 partition 单 key 建模 register:op ∈ {`write(v)`, `read()→v`}。register 的串行 spec 极简(read 返回最近一次 write 的值),checker 实现复杂度可控。
2. **历史采集点(client 侧插桩)**:在 tests/integration 内实现 `RecordingClient`(decorator 包装 `RrClient::send_request*` 及 A-0009 的 `read_linearizable`):每次调用记录 `{invoke_us, return_us(或失败标记), op, key, input, output}`。时间基准用**单进程 `Instant`**(集成测试 client 与被测集群同进程,区间内单调钟语义安全;不跨进程比时钟,避免时钟漂移误报)。invocation/response 以区间语义处理(响应未知时记 `?`,按 porcupine 惯例参与部分序)。
3. **checker 算法**:标准 porcupine 流程——按 response 时间排序得到部分序 → 回溯搜索线性化点(剪枝:寄存器状态确定时跳过不可能分支) → 任何完整线性化存在即合法。历史规模 ≤10k ops 时秒级完成;超阈值降采样。输出:违规时给出最小冲突子历史(两个操作 + 中间链),直接可读。
4. **放置**:`tests/integration/src/linearizability.rs`(新 module,不进发布面);若后续 sdk 多语言需要共用,再抽 crate。
5. **喂数据的测试**:
   - 改造 `test_concurrent_writes_under_chaos_linearizability`:5 writer × 20 ops 的计数断言**保留**(回归兜底),同时采集历史喂 checker;
   - 新增 `test_kv_register_linearizable_under_partition`:kv-store register 模型 +  follower 隔离轮转 + `read_linearizable` 与 stale `read_local` 混合负载——stale read 的历史**不**喂 checker(或按 session-guarantee 单独建模),linearizable read 必须过。
6. **CI 位置**:随默认 test job 运行(秒级);失败时上传历史 JSONL artifact,可用 checker 单测离线重放。
7. **备选评估——"直接用 Jepsen + Java SDK"**:**不采纳,理由**:Jepsen 需要 Clojure/JVM 工具链 + SSH 多机编排,与现有 in-process `FaultInjector` 设施重复投资;`sdks/java` 尚浅(review P2:Docker 集成测试在 CI 被 env gate 跳过),checker 本身(knossos)虽是成熟资产,但接入成本(≥8 人日)远超 Rust 原生 register checker(3~4 人日);且本项的核心缺口是"没有 checker"而非"checker 不够强"。**触发重评的条件**:0.3.0 后若 Java SDK 成为一等公民、或需要跨语言一致性声明,再立项。

### 验收标准

- [ ] checker 对人工构造的 3 类非法历史(stale read、lost write、重复 apply 值错乱)全部判非法,对合法历史判合法(单测);
- [ ] 两个集成测试采集真实历史并通过检查;
- [ ] 故意植入一次 stale-read 回归(临时分支),checker 抓到并输出可读的反例历史。

---

## R-6 proptest / fuzz 扩展

**里程碑:0.2.1|工作量:3~4 人日|依赖:WS-1(bench/CI 真实性修复的经验复用)**

### 为什么

review P1:fuzz 未接 CI,4 个 target 仅 decode 层且无 corpus(已核实:`fuzz/fuzz_targets/` 下 `fuzz_framed_entry_decode` / `fuzz_framed_snapshot_decode` / `fuzz_apply_outcome_decode` / `fuzz_client_request_proto`,target 体均为单行 `let _ = X::decode(data)`;`fuzz/Cargo.toml` 有意不进 workspace)。review P2:bincode 未精确锁定、缺 framed 双向兼容测试——而 CHANGELOG 已对外承诺 framing 的 magic+version 前瞻兼容(FramedSnapshot/FramedEntry),承诺没有测试兜底。proptest 已是 workspace 依赖(根 `Cargo.toml:50`,rr-core/rr-storage 已用),扩展边际成本低。

### 做什么

按"字节输入 → fuzz;结构化状态机序列 → proptest"的分工扩展;corpus 入仓;fuzz 接 nightly CI。

### 怎么做

**proptest 新增(4 项)**:

1. **路由表变迁**(`rr-router`):随机操作序列(add_partition / remove / reassign group),每个中间态断言 `ShardRouter::route`(`crates/rr-router/src/router.rs:34`)对任一 partition 恰好返回一个 group 或明确 `RrError`,无 panic、无"同 partition 同版本表路由到两个 group";语义锚点 = T32/T33(lib.rs:2463/2509)。
2. **幂等 LRU/watermark**(`rr-raft/src/idempotency.rs`):model-based——对照一个无容量上限的 `HashMap` oracle,随机 `decide/record` 序列 + capacity 压力,断言:watermark 单调、`Replay` 返回值与原值一致、`Expired` 只在 `request_id <= watermark 且 cache miss` 时发生(对照 `rr-core/src/idempotency.rs:106-114` 的契约文档)。
3. **IoBatcher 交错**(`rr-raft/src/io_batcher.rs`):随机 submit 并发度 × window × max_entries,断言:每个 submission 的 callback **恰好触发一次**、批内顺序按 big-endian log-index(与 :34-36 的 ordering 文档一致)、写错误传播给该批全部 caller、无 hang(空 append 场景沿用既有测试 `rocks_log_storage.rs` 的 `test_rocks_empty_entries_no_hang`)。
4. **framed 双向兼容矩阵**(`rr-core`):FramedEntry/FramedSnapshot 的 version 字段 ∈ {0, 1, current, current+1, u8::MAX} × 随机 payload 矩阵:低版本 bytes → 当前代码必须可 decode(前瞻兼容承诺);高于当前版本必须**明确 Err 而非 panic/误读**。配套 **golden corpus 文件测试**:若干固定 bytes 样本入仓(放 `crates/rr-core/tests/golden/` 或 fuzz corpus 复用),防 bincode 升级静默改格式(review P2 "bincode 未精确锁定"的缓解)。

**fuzz 扩展**:

- 新增 2 个 target:`fuzz_framed_entry_roundtrip`(decode→encode→decode 幂等性,能抓"decode 接受但语义漂移"类 bug,比纯 decode 深一层)、`fuzz_apply_outcome_roundtrip`(同理)。decode 层 4 个现有 target 保留。
- router/idempotency/batcher **不**进 fuzz(结构化序列输入,proptest 是正确工具)。

**corpus 与 CI**:

- corpus 入仓:`fuzz/corpus/<target>/`(cargo-fuzz 默认约定路径),seed corpus 取自 golden 兼容矩阵样本 + 现有单测的固定 fixture;
- CI 接入:新 workflow(如 `fuzz-nightly.yml`,与 R-2 的 formal workflow 同模式):`schedule` 每日 + `workflow_dispatch`,nightly toolchain,`cargo fuzz run <target> -- -max_total_time=900`(每 target 15 分钟);crash 时 `fuzz tmin` 最小化后 `upload-artifact` 归档 + 开 issue(按 backtrace hash 去重,周汇总防爆量);
- PR 级只做 smoke:`cargo fuzz run -- -runs=10000`(约 2 分钟,非阻塞,可后续视噪声决定是否保留)。

### 验收标准

- [ ] 4 个 proptest 模块入仓并随默认 test job 运行;
- [ ] 兼容矩阵覆盖 version 边界,高版本输入产生明确 Err(断言错误类型而非"不 panic");
- [ ] `fuzz/corpus/` 入仓且 nightly fuzz workflow 跑通 ≥1 周,crash artifact 可下载复现;
- [ ] 故意回退一次 bincode 行为变更(临时分支),golden corpus 测试变红。

---

## R-7 性能基线与回归门禁

**里程碑:RC|工作量:2~3 人日|依赖:WS-1(rocks benches 修复,见下)**

### 为什么

双重动机:(1) ADR A-0010(WAL)的**前置条件 1** 明确要求 release-build 基线(`.sisyphus/plans/rr-wal-impl.md:9`:"Run `cargo build --release` … Capture TPS / p50 / p99 / p999"),没有它 WAL 永远不该开工;(2) WS-4 要动写热路径(batcher 生命周期、append 直返 receiver),没有基线就没有"性能不退步"的判据。review P2 亦指出"bench 无回归门槛"。

### 做什么

跑一次正式 release-build 基线并存档;CI 增加 criterion baseline compare 门禁(先非阻塞);rocks benches 的 `required-features` 问题归 WS-1,仅引用。

### 怎么做

1. **基线采集**(RC 期间一次,此后每个 minor release 刷新):
   - `cargo build --release -p rr-perf-bench -p rr-server -p rr-admin`;
   - S1:`rr-perf-bench --scenario s1 --storage rocks --durability strict`;
   - S2:`--scenario s2 --storage rocks --durability replicated_only --concurrency 100`(按 wal-impl 前置条件原文;可同时跑全量 sweep `1,10,50,100,200,500` 丰富基线);
   - 对照组:`--storage mem`。指标直接用 `report.rs` 已输出的 TPS/p50/p95/p99/p999(µs);
   - 记录环境:CPU/内存/磁盘类型、OS、rustc 版本、commit SHA。
2. **存档位置**:`docs/perf/baselines/v0.2.0-baseline.md`(叙述 + 表格)+ 原始数据 `baseline-v0.2.0.json`。为此给 rr-perf-bench 加 `--json-out <path>` 小扩展(`MetricsSnapshot` → serde JSON,`metrics.rs` 已有结构,半天工作量)。
3. **rr-perf-bench 顺带小修**(与基线可信度直接相关,review P2):`Cluster::drop` 只 `start_kill` 不 wait(`cluster.rs:87-93`,panic 路径泄漏子进程);固定 `port_base 47000`(`cluster.rs:51-52`,并行跑冲突)——改为 drop 时 best-effort wait + 文档注明 port_base 参数。
4. **CI 回归门禁(criterion baseline compare)**:
   - 机制:criterion 0.5 原生支持 `--save-baseline <name>` / `--baseline <name>`。main 分支 push 时在 bench job 末尾 `--save-baseline main` 并把 `target/criterion` 存 artifact/cache;PR 的 bench job 下载后 `--baseline main`,criterion 输出每个 bench 的 change 估计与显著性。
   - 阈值(分层):统计显著 **且** 均值回归 >15% → fail;5~15% → 在 PR 留 warning 注解。阈值依据:criterion 微基准在共享 runner 的典型抖动为 ±5~10%,15% 给出安全边际,运行两周后按实测噪声收紧。
   - **失败策略:先非阻塞**——GitHub hosted runner 性能噪声不可控,门禁以 `continue-on-error` + PR comment 运行 ≥2 周,确认误报率后再转 blocking(或等有专用 runner)。此点明确写入 CI 配置注释,避免把噪声门禁当成可靠性成果自欺欺人。
   - job 位置:扩展现有 `bench` job(`ci.yml:180-198`,当前 `cargo bench --workspace -- --test` 只编译不比较)。
5. **rocks benches 修复**:`benches/Cargo.toml` 的 3 个 bench(`log_append_rocks`/`log_append_batched`/`log_tps`,:26-54)带 `required-features = ["rocks"]`,CI bench job 未开 feature 故从未运行——**属 WS-1 范围**(路线图 WS-1 第 4 条),本项只在验收时引用其结果,不重复设计。

### 验收标准

- [ ] `docs/perf/baselines/v0.2.0-baseline.md` 存档,含 S1/S2 两场景 rocks + mem 对照,环境元数据齐全;
- [ ] A-0010 前置条件 1 标记完成(在 wal-impl plan 勾选),并回答前置条件 2 的瓶颈归属问题;
- [ ] PR bench job 输出 baseline compare 报告(非阻塞);连续两周噪声观测记录;
- [ ] rocks 相关 bench 在 CI 日志可见实际执行(WS-1 验收的旁证)。

---

## R-8 soak 自动化

**里程碑:0.2.1(nightly 2h);0.3.0 起 24h 报告成为 release 门禁|工作量:3~4 人日|依赖:R-4 的 ack-log/oracle 组件、R-7 的 rr-perf-bench `--json-out`**

### 为什么

wal-impl 的 Phase 12 已把 "24h soak、零数据丢失零 panic" 列为 WAL 换默认后端的前置;更一般地,rr 宣称的是长稳服务能力(leader flapping、磁盘满、网络分区都有 runbook,`docs/operations/runbooks/`),但**没有任何长跑验证**——短时 chaos 测不出慢泄漏、lag 不收敛、metric/日志膨胀。review 元根因的同构问题:advertised 的运维属性没有强制点。

### 做什么

24h+ soak harness:docker-compose 3 节点 + rr-perf-bench 负载 + seed 化故障注入循环 + 三件套 oracle(无数据丢失 / 无 panic / lag 收敛);报告存档;渐进成为 release 门禁。

### 怎么做

1. **拓扑**:复用仓库根 `docker-compose.yml` + `deploy/compose/config/node{1,2,3}.toml` 的 3 节点形态,storage=rocks、durability=strict。compose 文件按需补 healthcheck(与 WS-2 的 compose 修复对齐)。
2. **负载发生器**:rr-perf-bench 扩展 `--scenario s3-soak`:keyed 幂等写(固定速率,如 200 ops/s)+ 周期性 `read_linearizable` 验证 + ack log 落盘(复用 R-4 的 ack-log 组件)+ 每 5 分钟快照一次 `MetricsSnapshot` 追加到 `metrics.jsonl`。避免另起负载工具。
3. **故障注入循环**(独立脚本,seed 化):事件 ∈ {`docker kill -9`(随机节点)、`docker network disconnect/connect`(分区)、`fallocate` 填满某节点挂载卷后清理(ENOSPC)、`docker pause/unpause`(进程挂起)};间隔按指数分布(mean 10~30 min);每个事件追加到 `fault-log.jsonl`(时间、事件、目标、seed)——与 R-3/R-4 的回放设施同构,失败可复现。
4. **oracle(soak 结束后自动检查)**:
   - **无数据丢失**:回放 ack log,100% `Replay` 且 payload 一致(同 R-4 oracle,组件复用);
   - **无 panic**:聚合 3 节点日志,`panicked at` 零匹配;进程重启次数 = 故障注入次数(无意外 OOM/崩溃);
   - **lag 收敛**:以 `/metrics` 的 replication lag / `last_applied` 差值为准,每次故障事件平息后 60s 内 lag → 0;全程 p99 收敛时间入报告。
   - 可用性:非故障窗口 client 错误率 < 1%。
5. **报告格式**:`docs/perf/soak/soak-report-<date>.md`(运行时长、总 ops、ack 数、丢失数=0、panic 数=0、故障事件表、lag 收敛统计、资源占用曲线)+ 原始 `metrics.jsonl` / `fault-log.jsonl` 归档 CI artifact。
6. **运行分层**(GitHub hosted runner 单 job ≤6h 的硬约束):
   - **nightly smoke soak 2h**:`schedule` 每日,hosted runner,完整跑 harness 但时长压缩;报告标 `smoke`;
   - **release 前 24h soak**:`workflow_dispatch` 手动触发,自托管 runner 或维护者机器(脚本同源,`SOAK_DURATION=24h`);报告标 `release`。
7. **成为门禁的时机**:0.2.1 期间 soak 纯信息性(攒报告、调 oracle 阈值);**0.3.0 起**,cut minor release tag 前必须存在该 HEAD(或等价 RC commit)的 green 24h soak 报告,写入 release checklist。

### 验收标准

- [ ] harness 一键运行(`scripts/soak.sh` 或等价入口),本地 2h smoke 全绿;
- [ ] nightly smoke soak 在 CI 连续运行 ≥1 周,oracle 无误报(或误报已修阈值);
- [ ] 至少一份 24h release 级报告存档(0.3.0 门禁启用前必须完成);
- [ ] 故意植入"kill 后丢一条已 ACK 写"(临时分支 + 旧版本镜像),oracle 抓到并定位到故障事件。

---

## 汇总表

| ID | 项目 | 里程碑 | 工作量(人日) | 依赖 |
|---|---|---|---|---|
| R-1 | 不变量目录执法机制 | RC | 2~3 | WS-8(invariants.md v1) |
| R-2 | TLA+ 形式化(4 spec) | ①随 WS-6 后启动,其余 0.2.1→0.3.0 | 6~8 | R-1 |
| R-3 | 确定性仿真(turmoil 试点) | 0.2.1 | spike 2;推广 2~3 / 替代 1~2 | 无硬依赖 |
| R-4 | crash harness + 存储故障注入 | 0.2.1(L1 wrapper 可 RC 后先行) | 4~5 | WS-1、WS-4(场景 A) |
| R-5 | 线性一致检查器 | 0.2.1 | 3~4 | 无 |
| R-6 | proptest/fuzz 扩展 | 0.2.1 | 3~4 | 无(rocks 部分 WS-1) |
| R-7 | 性能基线与回归门禁 | RC | 2~3 | WS-1(rocks benches,引用) |
| R-8 | soak 自动化 | 0.2.1(nightly 2h);0.3.0 起 release 门禁 | 3~4 | R-4、R-7 |

**合计:约 26~34 人日**(R-3 按推广路径计)。与路线图 Phase 1 粗估 20~30 人日的差异主要来自两处细化:R-2 拆成 4 份 spec(6~8),R-4 拆成 harness + wrapper 两层(4~5)。RC 期并行落地的是 R-1 + R-7(4~6 人日);0.2.1 主体是 R-3/R-4/R-5/R-6/R-8(约 14~19 人日);R-2 跨 0.2.1~0.3.0。

执行顺序建议:R-7(基线先行,解锁 WS-4/A-0010 判据)→ R-1(执法机制,成本最低收益最广)→ R-4(wrapper 部分,与 WS-4 同窗口)→ R-6 → R-5 → R-3 spike → R-8 → R-2 逐 spec。

## 风险登记

| 风险 | 影响 | 缓解 |
|---|---|---|
| turmoil 适配成本超预期(tonic 0.14 connector API 变动 / host 模型重构面大) | R-3 超支 | spike 时间盒 2 人日;第一天验证 `connect_with_connector` 技术前提,失败立即转"确定性 fault schedule 记录/回放"替代方案(1~2 人日,价值保留 80%) |
| TLA+ spec 与实现漂移(实现改了 spec 没改) | 形式化沦为摆设 | spec 双向链接 invariants.md `INV-n`;R-1 的 review 清单含"动协议实现须核对 spec"抽查项;CI 周跑使漂移尽快暴露 |
| CI 性能门禁噪声误判(hosted runner 抖动) | 误阻塞 PR、团队对门禁失去信任 | 先非阻塞 ≥2 周攒噪声数据;阈值分层(>15% 才 fail);长期迁专用 runner;此妥协明确写入 CI 注释 |
| 24h soak 超 hosted runner 6h 上限 | R-8 无法全量在 CI | 分层:nightly 2h smoke(hosted)+ release 前 24h(自托管/本地,同一脚本);门禁只要求报告存在,不要求在 GitHub 跑 |
| `IoBatcher` 持 `Arc<RocksStorage>` 具体类型,wrapper 插不进 group-commit 路径(io_batcher.rs:114) | 故障注入覆盖不到最热写路径 | 首选 WS-4 顺手泛化为 `Arc<dyn StorageEngine>`;兜底 harness 场景 C 用真磁盘故障(fallocate/cgroup io.max) |
| checker 时间语义误报(跨进程时钟、响应超时) | R-5 报告不可信 | 单进程 `Instant` + 区间语义;未知响应记 `?` 按部分序处理;stale read 历史不喂 register checker |
| fuzz nightly 发现噪声化(重复 crash issue 爆量) | 维护负担 | backtrace hash 去重 + 周汇总;PR 级仅 10k runs smoke 且非阻塞 |
| R-4 依赖 WS-4 未落地(close 协议延期) | 场景 A 无法验收 | wrapper(L1)与 kill 场景 B 不依赖 close 协议,可先行;场景 A 测试随 WS-4 合入再启用 |
| 可靠性专项被 P0 修复挤压(路线图已列同名风险) | 专项无限推迟 | R-1/R-7 绑定 RC exit criteria;R-3/R-5/R-6 绑定 0.2.1 里程碑而非"有空再做";R-8 绑定 0.3.0 门禁 |
| golden corpus / fuzz corpus 体积膨胀 | 仓库膨胀 | corpus 定期 `cargo fuzz cmin` 最小化;golden 样本控制在 KB 级 |
