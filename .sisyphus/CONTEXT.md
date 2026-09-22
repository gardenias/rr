# rr — Project Context

> Top-level session-handoff / onboarding file. Each session should UPDATE
> the sections below rather than rewrite. Per-phase deep dives live in
> `.sisyphus/notepads/<phase>/{decisions,learnings,problems}.md`.
> Forward-looking design decisions live in `.sisyphus/adr/A-NNNN-*.md`.

**Last updated**: 2026-09-22（**100k roadmap 首轮执行：M1 TCP 量化(+5.6% 非杠杆)、M2 热路径 metrics 零分配化 + MemConfigProvider Arc 快照、mimalloc A/B 否决(macOS)；路由缓存提案因 INV-6 明文否决、改 provider 内快照形态**）
> 前情 2026-09-21（**W9 INV-1 全链路闭环:根因定点 + 三修复落地 + perf-lab smoke ×3 / 宿主机 oracle 5000/5000 双路实证;既有选举 flake 根治;A-0012 watch 守卫闭环**）
> 前情 2026-09-17（**perf-lab 加固批次：soak lanes open-loop 化（134→2000/s 实测达标）、RaftService gRPC 4MiB 解码上限修复（16KiB 档 wedge 真凶）、探针取证加固；W9 罕见 INV-1 flake 立案观察**）
> 前情 2026-09-13（**M4 完成：活性缺陷链全部定点修复；首份生产基线 17/18 绿 + W9 全长复核绿（oracle 5000/5000）——18/18 达成；判据表已回填，A-0010 已重判维持 deferred；上游 issue 草稿已备**）
**Current phase**: **v0.2.0 已发布**；下一里程碑 v0.3.0 规划见 `rr-phase4-v030.md`，WAL（A-0010）维持 deferred
**Active plan**: `.sisyphus/plans/rr-prod-perf.md`（**M0–M4 全部完成**，基线 `docs/perf/prod/20260912-234352-9fa8347/`，M5=Linux 宿主正式基线）；v0.3.0 候选 `.sisyphus/plans/rr-phase4-v030.md`、`rr-wal-impl.md`（deferred，重判结论+解锁路径已回写）
**Latest tags**: **`v0.2.0`(final,2026-09-10)** + `v0.2.0-rc1` + `v0.2.0-rc2`
**Main branch HEAD**: 见下文 Git 状态段
**Latest full review**: 2026-08-16 (`.sisyphus/evidence/rr-full-review-2026-08-16.md`,11 P0);架构归档 2026-08-28 (`.sisyphus/evidence/rr-architecture-analysis-2026-08-28.md`)
**Latest Oracle review**: 2026-04-26 (`bg_1d8162f6`);Oracle 判断已并入 8-16 review
**Latest Momus review**: 2026-05-13 (`bg_a0e5a6d2`) on rr-wal-impl.md — pending

---

## 项目一句话 / Project One-Liner

rr 是 Rust 写的分布式 Multi-Raft 共识框架：openraft `= 0.10.0-alpha.34` + tonic gRPC + RocksDB，
**11 个 crate**（core / storage / config / config-etcd / raft / network / router / server / client / admin /
**ingestor-kafka**），**5 个 example**（counter, kv-store, kv-store-kafka, otel-client, etcd-config-quickstart），
完整的集成测试 + chaos framework + Docusaurus 文档站 + drop-in Prometheus/Grafana 套件。

### Session 2026-09-22 — 100k roadmap 首轮:TCP 量化 + 热路径零分配 + 分配器 A/B 否决

> 目标锚点:`docs/perf/roadmap-100k.md`(G2 单组 100k ops/s)。本 session = 全项目
> 对照 review + M1/M2 高置信项落地。**改动未 commit**(门禁全绿:fmt/clippy
> `-D warnings`/rr-config+rr-router+rr-server+rr-raft(166)/integration(81)/
> invariants-lint 70 links)。bench 原始 JSON 归档 `docs/perf/baselines/
> raw-20260922-s2-*-mem.json`(未 commit)。

- **Review 结论(符号级,采样锚定 roadmap 热点表)**:
  ① client 写路径 `gRPC decode → dispatch → route → client_write` 本身已相当干净
  (unkeyed payload 全程移动语义,无冗余拷贝);**真正的逐请求浪费集中在 metrics
  标签 String 化与路由表 clone**。
  ② `Service` trait 按值收 `Vec<u8>` 强制 unkeyed apply 路径每条 entry 一次
  `to_vec()` —— 消除需破坏性 API(Bytes 化),记 v0.3.0 候选。
  ③ `rocks_log_storage` overlay `entry.clone()` 是 openraft owned-Vec 返回契约的
  固有成本,判定不可消除(roadmap 已回写理由)。
- **M1 TCP 量化(勾选)**:S2 k=200 mem,`--use-tcp-transport` 31.9k vs gRPC 30.2k
  (+5.6%),p99 -23%。**复制面 TCP 是温和正收益而非 3× 杠杆**;不推广到 client
  路径;M1 主杠杆改为 client 面请求级批处理/h2 调参。
- **M2 落地(零分配化)**:
  - metrics 标签 intern:dispatch.rs(每写 3 次 `to_string()`)、
    network_factory.rs(每 AppendEntries RPC 3 次,含心跳)、service_adapter.rs
    (每 applied entry 2 次)全部改进程内 `&'static str` intern(每 id 泄漏一次,
    有界)。
  - `ConfigProvider::routing_table_snapshot()` 新增带默认实现(trait 方法,
    semver-minor);`MemConfigProvider` 改 copy-on-write `Arc<RoutingTable>`,
    `route()` 热路径 = 读锁 + Arc incref,零表拷贝。**INV-6 合规形态**:路由仍逐
    调用取 provider 当前快照,无第二份缓存视图(router 侧缓存提案被 INV-6 明文
    否决,改 provider 内快照)。
  - **遗留(prod 地雷记录)**:etcd provider `get_routing_table` 仍是逐调用全前缀
    扫描 —— etcd 后端部署下每个 client 请求打一次 etcd。默认方法保持行为等价;
    revision 感知缓存列为 follow-up。
- **mimalloc A/B(勾选,结论否决)**:feature `rr-server/mimalloc`、
  `rr-perf-bench/mimalloc` 落地(默认关)。macOS arm64 两轮 25.4k/23.6k vs 系统
  分配器 30.1k/30.2k,稳定 **-20%** —— macOS 否决;Linux 对照留到 M5 重测。
- **前后宏基准**:HEAD 30.2k → 全改动 28.4k/29.1k/29.3k(median 29.1k,-3.6%,
  在本机 ±5% 噪声带内;bench-compare 硬门禁阈值 15%)。收益主要来自"构造上消除
  分配/锁"(采样占比小但确定),宏基准分辨不出;etcd 路径的 prod 收益是结构性的。
- **detect_changes**:40 changed symbols / risk=critical(纯计数阈值, affected
  processes 全部为单测流程且全绿);语义上 = 低风险(加性 trait 方法 + 内部表示
  替换 + 标签 intern,无行为变更)。
- **下一步候选**(按 roadmap):client 面请求级批处理评估(M1 主杠杆)→ tonic/h2
  调参扫描 → M3 worker/亲和性扫描;`Service` Bytes 化并入 v0.3.0 规划;
  etcd provider revision 缓存单独立项。



### Session 2026-09-21 — W9 INV-1 闭环 + 既有 flake 根治 + A-0012 watch 守卫 + 两个遗留问题结案

> 承接 09-17 的 W9 立案。 commits:`f3aad0f`(fmt 漂移)→ `6eb02d6`(字节预算 +
> save_vote 屏障)→ `bd698d2`(vendor patch #3 + try_submit + issue 草稿)→
> `eb1119e`(flake 根治)→ `bc641e1`(watch 守卫)。

- **W9 INV-1 根因定点并修复**(细节见上文 🔴 开放项节的"已定点并修复"改写):
  `log_progress.flushed()` 光标的两条无 IO 膨胀路径——become-leader 的
  `UpdateIOProgress{when:None}`(vendor patch #3 改挂 `Condition::IOFlushed{
  prev_accepted}`,伴随 `run_update_io_progress` 的 `submit→try_submit` 防光标回退)
  与 `save_vote` 直写越过 batcher(rr 侧改 `flush_barrier` 先行)。IoBatcher 字节预算
  (默认 64 MiB)断"无界积压→OOM 重启波"链,INV-1 强制点 #7。验证:rr-raft 166、
  integration 80/80、crash harness 10/10(SIGKILL 场景含)、clippy/fmt/invariants-lint
  (68 links)全绿。**perf-lab W9 smoke 复跑阻塞**:本机 Docker 缺 `rust:1.98-bookworm`
  /`debian:bookworm-slim` 基础镜像且 registry 不可达(按环境约定未拉取)——有网后
  连跑 3-4 轮 `--smoke` 实证(flake 率 ~1/4)。
- **既有 flake 根治**(非 W9 引入,归因实验:基线 ×12 轮败 4 次):openraft alpha.34
  起 lease 门控提案快败 `ForwardToLeader::empty()`,而 `wait_for_*` 只看
  `current_leader` → 选主后一次性写撞租约竞态。新增 `write_when_lease_ready`
  (ForwardToLeader 重试至租约建立),4 处站点替换;修复后 ×15 轮全绿。
- **A-0012 follow-up 闭环**:watch-loop `NodeRemoved` 自动摘除接入同一
  `membership_change_is_safe` 谓词,目标 voter < 2 拒绝 + warn 留痕;自动路径刻意
  无 force(刻意拆除走 `--force`/`node-reset`);维持 Option B(不下沉 manager 层)。
  测试 `test_node_removed_quorum_guard` 含负面对照(摘守卫即变红)。
- **遗留查明结案 ×2**:
  ① **快照安装防回退(β,09-17)——无需补丁**:openraft `install_full_snapshot`
  已有接收侧守卫(`snap_last_log <= local_committed` 直接跳过,含矛盾断言);
  空快照(last_log=None)在已有 committed 的节点上天然被拒;发送侧空快照投毒已由
  T1(rocks 路径快照持久化)闭合;mem 路径按设计全量易失。α/β/γ/δ 四方向全部闭环。
  ② **step-down 在途 client_write(09-10)——已查明,行为可接受**:openraft 在
  step-down(`become_following`/`CloseReplicationStreams`)时**不主动应答**在途
  responder;它们在 (a) 被新 leader AppendEntries 截断/快照 purge 时收
  `ForwardToLeader` 错误(`run_truncate_log`/`run_purge_log` 的 drain),(b) 分区
  失联场景则悬挂至客户端超时——客户端侧止血即 R-2 的 `with_request_timeout`
  (映射为可重试 `NetworkError::Timeout`)。无需服务端改动。
- **归档清理**:`docs/perf/prod/` 31 → 4 目录(1.7G → 265M),保留两个入库基线 +
  W9 活跃取证对。
- **上游**:issue 草稿补 Issue 3(become-leader 膨胀对一切"延迟完成 append"的
  存储后端都成立;含 try_submit 伴随修复说明)。提交动作待有网 + 用户决策。

### Session 2026-09-21(午后)— 宿主机 W9 近似验证(Docker 不可用的替代路径)

> perf-lab 重建被双重阻塞(registry 不可达 + 本地无基础镜像),改走宿主机路径实证
> W9 修复三件套。**结果:oracle 5000/5000 全绿**。

- **形态**:release 构建(kv-store `--features rocks` + rr-perf-bench);3 节点
  `--data-dir`(durable log + T1 持久快照)+ `--snapshot-logs-since-last 2000`
  (快照搅动比 perf-lab 的 5000 更密);soak 2000/s × **16KiB**(=32MB/s,W5-16384
  的积压形态)180s;两次 kill 注入:T+60s SIGKILL node1、T+120s 同时 SIGKILL
  node2+node3,5s 后重启;oracle 前还有一次全集群冷重启(从数据目录拉起)。
- **结果**:soak sent=133,766 / acked=132,251 / failed=1,515(1.1%,全部集中在
  双杀+重建窗口,op_timeout 止血——无悬挂);**oracle 5000/5000**(replay 375 +
  survival-only 4625,跨 LRU 窗记录走线性一致读存活证明);零 panic;node2 重启经
  T1 路径从持久快照恢复(快照点 T1-N1.50005,replay 尾部 [50006..98579))。
- **观察(非阻塞项)**:① 重启+全量 RocksDB open + 快照恢复在本机 ~31s(快照
  300MB 级);② kill 窗口内 term 搅到 T148(有 pre-vote 仍多轮竞选,收敛正常);
  ③ 探针在失 quorum 窗口报 "KvResponse decode failed" ×9——**响亮失败而非静默
  陈旧读**,可接受;④ RSS 峰值 6.3GB = 16KiB×133k 条目的内存 KV 本体 + 快照
  序列化拷贝 + RocksDB 缓存(kv-store 演示形态使然),非 batcher 积压(字节预算
  上限 64MB/组)。⑤ **踩坑**:首轮跑进了 OrbStack 对 51001/51002 的转发(my-biz
  的 cex-node 容器占用),客户端打到了别的协议——宿主机复现实验务必避开已发布
  端口(本轮用 53001-3)。编排脚本在 `target/w9-host/w9-host-repro.sh`(一次性,
  未入库);证据目录 `target/w9-host/run-20260921-101053/`。
- **结论**:字节预算 + save_vote 屏障 + vendor patch #3 在"快照 crunch + 双杀
  重启波 + 全集群冷重启"下保持 INV-1。~~perf-lab `--smoke` 复跑(容器 OOM 维度)
  仍待有网后补。~~

- **✅ perf-lab 实证(同日下午,网络恢复后)**:补齐 `rust:1.98-bookworm` /
  `debian:bookworm-slim` / redpanda 镜像后,`prod-perf.sh --smoke` **连跑 3 轮
  MATRIX PASS**(归档 `docs/perf/prod/20260921-{104153,110559,112441}-985b594`,
  未入库):W9 档 soak 60s + TransferLeader 全部 0 失败(如 119,999/119,999)、
  oracle 干净;W5-16384(原 wedge/积压档)、W7 快照压力全绿。W9 INV-1 flake
  (~1/4 率)三连绿 + 宿主机强验证 = 关闭观察。残留观察项:上游 issue(3 个)
  待提;`target/w9-host/w9-host-repro.sh` 保留为无 Docker 环境的复跑入口。

### Session 2026-09-05/06 — 可靠性专项批次(R-1 执法 + R-6 性质测试 + R-7 基线)

> 执行目标(用户原话):"生产级别的稳定性和可靠性,简洁的集成使用体验,极高的吞吐和极低的延迟"。
> 按 `rr-reliability-program.md` 既定顺序落地 R-7 → R-1 → R-6;**本 session 全部改动未 commit**。

- **Batch 0(RC 退出验证)**:6 处遗留 rustfmt 漂移修复(`cargo fmt --check` 全绿);workspace clippy `-D warnings` + 全量测试(含 rocks)全绿;3 个 rocks 持久化测试逐个单独运行实测通过;docs build 通过。helm/docker 动态验证因外网中断(Docker Hub / get.helm.sh 不可达)只做静态复核——制品自 WS-2 验证批次以来零变更(f3b98e0 后无改动)。cargo-semver-checks 本机未装,CI 有 semver-checks.yml 常驻。
- **R-7 基线与门禁**:`rr-perf-bench --json-out`(meta + S1/S2/S3 快照,兼容外接集群);`scripts/perf-baseline.sh` 一键矩阵采集;`docs/perf/baselines/v0.2.0-baseline.md` + 5 份 raw JSON 存档(M3 Pro:~34k TPS 饱和,strict≈test_only≈mem <2%,饱和点单进程 ~3.6 核)。**A-0010 判据:前置 1 ✅,前置 2 ❌(CPU 绑定,非存储)→ WAL 维持 deferred,IoBatcher 调优同判无收益**,已回写 `rr-wal-impl.md`。CI bench job:main 存 criterion baseline、PR 对比、`scripts/bench-compare.sh`(>15% 显著回归报错/5~15% 警告),`continue-on-error` 观察期。`Cluster::drop` 修复(start_kill 后 best-effort reaping)。
- **R-1 执法机制**:`scripts/check-invariants.sh`(5 项双向检查,故意删反链实测变红);CI `invariants-lint`(阻塞式);PR 模板 2 检查项 + INV 编号栏;CONTRIBUTING 评审四问;INV-1~8 的 52 条既有测试补 `INV-n` 反链;invariants.md 新增维护规则 5(登记触发元规则)。
- **R-6 性质测试**:proptest ×4(router 变迁 INV-6 / 幂等 model-parity INV-3 / io_batcher 交错 INV-1(rocks-gated)/ framed 版本矩阵 ×7),新增 ~0.85s;golden corpus 6 样本(rr-core/tests/golden/,字节级重编码一致,bincode 漂移绊线)。fuzz:2 个 roundtrip target + 43 个入仓种子 + `fuzz-nightly.yml`(每日 03:23 UTC,6 target × 15min,crash→tmin→artifact);顺带修复 `fuzz/Cargo.toml` 的 `prost = { workspace = true }` 潜在损坏(独立 crate 无法解析,改钉 "0.14.3")。fuzz 端到端受网络所限未跑(shim 编译 + 全种子回放已验),**首次有网 nightly 是真验收**。
- **下一步候选**(按专项顺序):R-4 crash harness + 存储故障注入(先决:IoBatcher 泛化 `Arc<dyn StorageEngine>` 的 impact 评估)→ R-5 线性一致检查器 → R-3 DST spike → R-8 nightly soak → R-2 TLA+。`v0.2.0` tag 是否 cut 仍待用户决策(RC exit 已满足)。

### Session 2026-09-12(深夜→13 凌晨)— M4 收官:活性缺陷链全灭 + 首份生产基线 17/18 绿

> 承接白天场:继续深挖"修复后仍不自愈"。一天内四个独立缺陷全部定点修复并矩阵级验证;
> 超 knee 行为从"melt+永不自愈"翻转为"优雅降级+完全自愈"。

- **缺陷 #2(openraft,vendor patch #2 ✅)**:`engine_impl.rs::handle_vote_req` 的 log recency
  早退**不采纳更高 term**(标准 raft 必须采纳)——term 跑飞的节点永远无人跟随,集群 term
  永不收敛(T856 vs T113 实证)。补丁:recency 拒票时仍本地采纳高 vote(响应仍 granted=false,
  recency 分支先行故无法被重放成真授权)。实证:补丁后三节点 term 收敛到同一值。
- **缺陷 #3(rr 框架,A-0016 缺口 ✅)**:durable log + 非持久 SM 快照下,重启只能 replay 未
  purge 尾部 → initialize 时的 membership 条目丢失 → openraft 经 `sm_mem` 回退重建**空
  membership** → 0 voter → 无选举/无心跳/lease 死 → client_write 一律 `ForwardToLeader::
  empty()`(客户端"not leader None"),而 metrics 仍报 persisted vote 的假 is_leader。
  W7 滚动重建 2/2 复现即此。修复:`sm-membership.bin` sidecar(committed membership 的
  bincode,apply/install 时写,启动时读;刻意不存 last_applied——SM 数据非持久,replay 定位
  必须留在 openraft)。+2 单测;W7 场景实测重建后立即可写(0% err)。
- **矩阵级终验**:`20260912-234352-9fa8347`,**17/18 绿**:W1×3 全阶梯(4096B 上轮全灭,
  本轮 16000/s 档 0 err)、W2–W5、**W7 修复实证**、W6/W11/W8 全绿;零 panic。
  16k/s×600s 实测:ok 2.58M/5.9% err(原 82–100%),负载停 30s 后 0% err p99 22ms。
  判据表(plan §9.1)回填:knee ≈4000–8000/s@1KiB;**安全税 ≈0**(TLS p50 == 明文 p50);
  rocks-log vs mem-log 固定 +7~8ms p50 延迟底(归因 IoBatcher 窗口 vs OrbStack 磁盘未定);
  lin-read +25% p99;无快照毛刺。**A-0010 重判:维持 deferred**,解锁路径=Linux 宿主 W11
  A/B → IoBatcher 窗口调优 → 仍热才谈 WAL(已回写 rr-wal-impl.md)。
- **W9(复核关闭,改判绿)**:首轮红(读探针 flake)经三次隔离复现阴性 + 全长单体
  重跑(20260913-100629-65fc548,2h)全绿推翻:908,258 写 0 失败、探针 719/719、
  TransferLeader 成功、oracle 5000/5000(replay 147 + survival 4853)。脚本语义已修:
  oracle 永远执行且为一致性唯一裁决源;load 腿失败+oracle 干净时标注疑似探针问题。
- **遗留**:① 【已查明并推翻】16KiB 档 wedge 真凶 = RaftService tonic 4MiB 解码上限(批量 AppendEntries 超限),非内存悬崖;2026-09-17 修复;
  ② soak 闭环 pacer 实际只发出 ~134/s(标称 2000/s)——open-loop 化是 tool backlog;
  ③ 上游 issue 待提(两个 vendor patch 的对应位置);④ A-0015 跟踪"上游修复后摘 vendor"。
- **commits**:`e59eba6`(full_snapshot+pre-vote)→ `b709cc7`(tooling 门禁+日志流)→
  `9134d87`(vendor panic 补丁)→ `ddf7790`(CONTEXT)→ `9fa8347`(vote-term 采纳 +
  membership sidecar)→ `66d8b57`(基线归档+判据回填+A-0010 回写+门禁重试)。

### Session 2026-09-12 — M4 首轮全矩阵：P0 活性 bug 全解剖 + 部分修复落地

> 首轮非 smoke 全矩阵（docs/perf/prod/20260912-000736-9d6c1d0/）。矩阵的价值在首轮即兑现：
> 抓到"流量尖峰 = 集群永久死亡"级缺陷链。根因全部钉到行；修复分"已落地"与"待拍板"两组。

- **P0 主根因（openraft bug，已 vendor 补丁修复 ✅）**：`openraft-0.10.0-alpha.34/src/core/raft_core.rs:2422`，
  `run_save_committed_and_apply` 中 `get_log_id(already_applied.next_index()).unwrap()`——
  SaveCommittedAndApply 命令入队后、执行前，snapshot purge 恰好越过该 index → None → unwrap panic
  → RaftCore 死亡 → 2/3 节点 panic 后 quorum 永久丢失（负载停止也不恢复）。16k/s×1KiB 下 ~2.5min
  必现（两轮各 panic 2/3 节点，位置完全一致）。**上游 main 至今未修，alpha.34 已是最新发布版**。
  **已走 B 路径落地**：`vendor/openraft/` + `[patch.crates-io]`；guard 语义——`already.next` 被 purge
  时前跳到首个现存 log id（已 purge 前缀被 snapshot 覆盖，安全）；整段 purge 则 warn+跳过；
  `next > last_log_id` 保留原 panic（真不变量破坏）。`deploy/perf/Dockerfile` 补 `COPY vendor`。
  验证：16k/s×600s 快照全开**首次零 panic**（二进制内含 patch 字符串实证）。
  待办：给上游提 issue；A-0015 跟踪"上游修复后摘除 vendor"。
- **已落地修复（3 文件未 commit，均验证）**：
  ① `network_factory.rs::full_snapshot` 三缺陷——`_cancel`/`_option` 原样丢弃（无超时无取消，
  卡死接收方 = transmitter 无限重试且每次重读全量 SM 快照+全量 chunk 拷贝）+ 全量物化 Vec；
  改为 select 取消 + `option.soft_ttl()` 整体 deadline（超时映射 `Unreachable` 让 transmitter
  退避而非热重试）+ 惰性流式分块。② `raft_node.rs` 两处 Config：`enable_pre_vote: Some(true)`
  （修复孤立节点 term 通胀 T910，实测生效）+ `install_snapshot_timeout: 60_000`（默认 200ms 对
  百 MB 快照不可用）。③ `prod-perf.sh`：tier 后健康门禁（5s 写探针，不可写→记 fail+硬停矩阵，
  sec/w8 豁免——首轮 W2~W7 在死集群上白跑 70min 的浪费不再发生）+ `docker logs -f` 流式落盘
  （OrbStack 日志轮转冲掉 panic 原点的问题根治，本轮 panic 即靠流式抓到）。
  验证：cargo test -p rr-raft 148 绿 + clippy 绿 + bash -n/shellcheck 绿。
- **实验已回退**：选举参数 500/1500/3000 vs 50/150/300 在 16k/s（超 knee）差异 marginal
  （804k vs 742k ok）；近 knee 影响未测，保留 50/150/300 待策略拍板。
- **矩阵关键读数**：W1-128B 全阶梯 0 err（32k/s 档 905k ok——128B knee 未到）；W1-1024 ≤4k/s
  干净、8k/s 起 err（**容量 knee：1024B 在 4k–8k/s 之间**，2 核容器+mTLS）；W8 insecure 32k/s
  0 err；W11 mem-log 8k/s 0 err；W6 kafka commit 2005/s 对齐 producer 2000/s、lag max 100。
  16k/s 超 knee 后所有修复下仍 82%+ err——超 knee 行为是 melt 而非 degrade（后续议题）。
- **🔴 更深的第二根因（战略决策点，待 Louis 拍板）**：panic 修复后集群在 16k/s 仍 melt，
  且**永不自愈**（负载停 5min+ 实测）。终态解剖：kv-3 稳定 leader(T62，与 kv-1 同步）但
  **无法 commit 任何新条目**（复制静默 wedge，无 panic——client_write 永远"未确立领导权"）；
  kv-1 租约外不知 leader → 回 None；kv-2 落后 31k 且 pre-vote 下仍 term 通胀至 T917 孤立竞选。
  客户端四轮尝试全收 "not leader None"。**这是 alpha.34 复制核当天第三个独立活性缺陷**
  （panic 竞态/禁 snapshot 轮的 responder wedge/本轮的复制静默）——whack-a-mole 式 vendor
  补丁的维护成本正在失控。选项：①接受现状：matrix 限定 knee 内 + knee 边缘，M4 基线仍有意义，
  超 knee melt 记 known-issue（A-0015 跟踪）；②继续挖 wedge（vendor patch #2，工作量未知）；
  ③openraft 版本变更（0.9.25 stable，port 成本高，A-0015 正式立项）；④产品级入口背压/拒载
  （独立于 raft 版本的应有防御，不替代 raft 健壮性）。**建议 ①+④ 短期、③ 中期**。
- **开放项**：kv-2/kv-3 容器负载下重启（疑似 OOM，未坐实;16KiB wedge 的内存悬崖假说已被
  2026-09-17 的 gRPC 4MiB 解码上限发现推翻——重启原因待另行确认）。
- **下一步**：Louis 拍板战略方向 → commit（修复 4 文件 + vendor + CONTEXT.md，等确认）→
  按拍板结果重跑全矩阵 → 基线归档入 git → 判据表回填（plan §9）+ A-0010 用 W11 数据重判。

### Session 2026-09-10(深夜三)— M3 落地:一键矩阵 + Prom 窗口采集 + 三个修复的引擎缺陷

> 生产性能方案 M3 收官;全量 smoke 全档绿(sec + W1–W9 + W11)。三轮 smoke 各暴露一个
> 真问题并当场修复——perf-lab 的验收价值在工具自身落地前就已兑现。

- **M3-6(Prom 对齐采集)**:rr-perf-bench 新 `prom.rs`——`--prom-url` 在 mix/wladder/
  soak 的 JSON 报告嵌入**同墙钟窗口**的服务端汇总(server/proposal/apply/append-RTT
  p50/p99 + 复制 lag max + `rr_ingestor_commands_committed_total` 提交速率;原始直方图/
  计数器族 instant 查询,observability recording rules 零触碰);`started_unix_secs`
  入 ScenarioReport/SoakSummary;curl 子进程零新依赖(仓内 cmd_stdout 同模式);
  采集失败降级 null + note,**永不拖垮 run**。5 个解析单测。
- **M3-7(一键矩阵)**:`scripts/prod-perf.sh`(新;sec 负面对照硬门禁 + W1–W9/W11
  全档,档位全 env 可覆盖,--smoke 缩时,单层失败不中断、ledger 记账、退出码聚合)+
  `scripts/prod-perf-report.py`(新;从 raw JSON 渲染 report.md,knee/安全税/raft-log
  成本 A/B 表自动算,禁止手抄数字)。归档 `docs/perf/prod/<date>-<sha>/`(raw-*.
  json + ts-*.jsonl + ledger + docker-stats + meta;**本轮 smoke 目录不入 git**,
  首份基线归 M4)。
- **overlays**:`docker-compose.kafka.yml`(W6:redpanda + kv `--kafka-brokers perf_` +
  profile 化 producer;topic-init 重试环 + awk 精确匹配幂等)、`docker-compose.insecure.
  yml`(W8 对照:明文无 token,kv-store dev 模式;token 插值用空字面量中和)、
  `docker-compose.memlog.yml`(W11 对照:去 `--data-dir` 回退 M0 前形态;报告注明
  mem-SM 混杂因子,R-7 实测 <2%)。基座 compose 增 `SNAPSHOT_LOGS_SINCE_LAST` env
  旋钮(默认 50000 行为不变,W7 用)。W10(Toxiproxy WAN)按计划"可选"标记 deferred。
- **rr-admin TLS/token**(W9 TransferLeader 前置,也补齐"perf-lab 安全集群此前无法
  运维"的口子):`--tls-ca/--tls-cert/--tls-key` 全有或全无 + `--tls-domain` +
  `--token-env`/`--client-token-env`(REPL kv 命令用);单 Interceptor 类型统一所有
  调用点(InterceptedService<Channel, TokenInterceptor>,token 缺省即透传),
  decommission.rs 三签名改别名;70 单测绿。
- **kv-store**:`--snapshot-logs-since-last` 透传(W7;NodeConfig.with_* 既有旋钮)。
- **引擎缺陷修复(全部冒烟实测发现)**:
  ① open-loop 调度器爬速缺陷:驱动循环单轮 >50µs 时计划时刻按"上次计划+间隔"爬行,
  永不追赶,实测 20k/s×16KiB 只发出 8%(且 lag 不可见——正是本引擎要消灭的偏差);
  重构为**落后即不 yield 的追发**(ahead 才 sleep_until;acquire 与窗口结束 select),
  payload 编码移入 op 任务并行化。同缺陷②:`max_in_flight` 满时 firing loop 停在
  acquire 上 → join! 永不返回(W5-16KiB hang 15min 实证);③ 新增 `--op-timeout-secs`
  (默认 30s,HDR 上限;tonic 无默认 RPC deadline,**挂死 RPC 会无限悬挂**——soak 在
  TransferLeader 瞬间实证;soak 场景同旋钮接入,失败计数不中断);④ soak oracle 对
  **超 LRU 窗(10k)记录**原样判死刑 → 2h 级 soak 必败(R-8 验收第 2/3 条"待时间"
  的潜在雷);现 Expired 记录跳过 replay 探针(会 re-apply 污染 key)、仅线性一致 Get
  证存活,单列 `survival_only` 计数并醒目 eprintln;空 ack 日志从"0/0 假绿"改为
  fail。⑤ W9 的 TransferLeader 必须打**目标节点** admin API(服务端前提错误实证);
  ⑥ prod-perf.sh 函数在 `||` 语境下 `set -e` 失效导致 rc 丢失 → 显式 rc 检查 + 失败
  自动抓节点日志取证。⑦ compose 每阶段 `--build` 重解析 base 镜像清单撞外网 →
  preflight 单次构建。
- **验证**:全量 `--smoke` 末轮全档绿(sec 明文全拒;W1×3 payload、W2×2、W3×3、W4×2、
  W5×4 含 16384 档有界收尾;W9 60s soak 16,765 acked 0 失败 + TransferLeader 成功 +
  oracle 5000/5000 其中 survival 2322;W7 快照压力;W6 producer 20k 消息 0 误 +
  Prom 提交速率实测;W11/W8 对照档;selfcheck 8/8 复测绿;workspace clippy
  `-D warnings`/fmt/22+70 单测/docs build/invariants-lint 65 links 全绿;
  detect_changes 53 符号 CRITICAL 但全在 rr-admin/kv-store/rr-perf-bench 三个工具
  二进制内,库 crate 零触碰——已复核)。
- **遗留发现(供后续专项)**:rr-client 单次 RPC 尝试无 deadline,leader 切换瞬间在途
  写可无限悬挂(客户端侧 op_timeout 已止血;服务端在 step-down 时是否主动结束在途
  client_write 待查);macOS Docker Desktop 下 bench 驱动 lag-p99 ~25ms 为环境烙印
  (lag 指标的意义就是把它暴露出来)。
- **下一步**:M4——首轮完整矩阵实测(非 smoke 时长)+ 首份生产基线归档 +
  判据表回填 + **A-0010 前置判据 2 以 W11 数据重判并回写 `rr-wal-impl.md`**。

### Session 2026-09-10(深夜二)— M2 落地:rr-perf-bench open-loop 引擎 + TLS client(§5.2 之 1–5)

> 生产性能方案 M2 收官;本地 kv-store 与 perf-lab mTLS 双路冒烟绿;S1/S2/S3 schema 回归干净。

- **openloop.rs(新引擎,~700 行含 12 单测)**:计划时刻调度(恒定网格 /
  `--poisson` 指数到达)+ 协调遗漏修正三计量(`lat`=计划→完成、`svc`=实际→完成、
  `lag`=计划→实际,驱动跟不上计划时 lag 如实暴露);`--mix write:90,local:5,lin:5`
  加权 op 混合;`--key-space`/`--key-dist uniform|zipfian`(`--zipf-theta` 0.99,CDF
  二分采样,>16M keyspace 拒绝防 OOM);`--client-id`(JSON meta 聚合标签);
  `--timeseries-out` 1s 桶 JSONL(t/op/count/err/p50/p99,wladder 带 rung);
  `--max-in-flight` 信号量背压(滑落计入 lag,不静默丢载);wladder = 单连续
  分段定速调度(rung 间队列溢出按 scheduled 时刻正确归属,不 drain 不中断)。
  全随机源 = seed 化 SplitMix64 三流(到达/op/key;`0x5EED5EED5EED5EED` 默认),
  与 rr-core storage_fault 同族(私有故按仓内不提取惯例复制 + 参考向量钉死)。
- **场景**:`mix`(定速)/`wladder`(`--ladder-rates`,默认 100→32000 八档);
  不进 `All`(all 行为不变);KV 线协议 bincode(KvRequest 与 soak.rs 单源,
  pub(crate) 可见性共享,序位测试双向防漂移)。
- **关键设计决策——写走 plain `send_request`**:冒烟实测发现 keyed 写在 open-loop
  并发下必然触发幂等层 watermark 过期(并发在途 ID 乱序 apply:26 先于 25 落盘 →
  25 判 Expired;框架契约本就要求"namespace 内单调",串行生产者为假设)。首跑
  8.4% 写错误全为此类。幂等 keyed 路径由 soak(W9)与 Kafka ingestor(W6)持续覆盖,
  open-loop 引擎测其下的写包线。已写入模块文档与 CHANGELOG。
- **TLS client(M2-5)**:`--tls-ca/--tls-cert/--tls-key` 全有或全无(kv-store 同约定)+
  `--tls-domain`(SNI 覆盖)+ `--token-env VAR`(env 读 token,fail-fast,密钥不上
  argv);`connect_client` 统一两个连接点,全场景(含 soak)生效;明文默认不变。
  rr-perf-bench 新增 rr-network path 依赖(TlsConfig;rr-client 无 re-export)。
- **perf-lab 接线**:bench 容器默认命令换 wladder 小阶梯 + 挂 certs + 注
  RR_CLIENT_TOKEN(M1 注释承诺的 M2 flags 落地);README 增 "Running load (M2)" 段。
- **回归证据**:workload/metrics/cluster 三文件 vs HEAD 零 diff;report.rs 纯加法;
  soak.rs 仅可见性;JSON schema 与 v0.2.0 逐键一致(mix/wladder/client_id 未跑即缺省);
  S1 同 flag 冒烟正常;`git diff HEAD -- scripts/ deploy/lab/ .github/` 为空;
  invariants-lint 65 links 绿;`--open-loop` 等 flag 误用于 S 场景有 eprintln 提示。
- **验证**:fmt/clippy `-D warnings`/17 单测绿;本地单节点 kv-store:mix(500/s
  5s zipfian 混合)0 err、wladder(100/500/2000 × 3s poisson)offered 精确跟随速率;
  **perf-lab 实测:三节点 mTLS + token 下 wladder 100→2000 四档 36,000 写 0 err**,
  明文负面对照 100/100 全拒;栈 `down -v` 收净。
- **下一步**:M3(§5.2 之 6–7:Prom 对齐采集 + 报告渲染 + `scripts/prod-perf.sh`
  一键矩阵 + W6/W10 profile)。

### Session 2026-09-10(午后)— M1 落地:perf-lab 生产形态基准栈(deploy/perf/)+ kv-store TLS 参数面(M1-a)

> 生产性能方案 M1 收官;全栈 selfcheck 实测 8/8 绿;改动未 commit(等用户决策)。

- **M1-a(kv-store TLS/mTLS/token 参数面)**:`--tls-cert/--tls-key/--tls-ca`
  全有或全无 + `--tls-domain`(拨 peer 的 SNI 覆盖);token 固定走
  `RR_ADMIN_TOKEN`/`RR_CLIENT_TOKEN` env(secure 模式必需,fail-fast;与
  rr-server `auth.*_token_env` 同约定,密钥不上 argv)。secure 模式 = 全
  G1–G7(不再 `allow_insecure`):mTLS 强制(无 opt-out knob——G2 下
  RaftService 唯一身份面),`ChannelPool::with_tls` 承载
  AppendEntries/Vote/snapshot;无 TLS flag = 历史明文 dev 模式
  (allow_insecure + warn)。main 顶端装 rustls ring provider(kv-store 新增
  rustls 依赖)。impact:run_node 上游仅 main(LOW,exact)。
- **deploy/perf/**:`Dockerfile`(kv-store `--features rdkafka,rocks`;runtime
  补 libstdc++6——rocksdb C++ 链接;镜像 rr-perf:dev);compose 栈 `rrperf`
  (kv-1/2/3:durable log `--data-dir /var/lib/rr/data` + named volumes +
  cpuset(默认 0-1/2-3/4-5,env 可覆盖)+ mem_limit 2g + 证书 ro 挂载 +
  token `:?` 注入;bench/probe 两个 profile 容器网内运行;Prom/Grafana 复用
  deploy/observability,宿主端口 9190/3100;**节点端口一律不发布宿主**——
  网内负载原则);`gen-certs.sh`(kv-N SAN + client.pem;certs/ gitignored;
  key 0644 因容器 uid 10001 需读——本地专用,头部警告;**scripts/
  gen-dev-certs.sh 冻结不动**,SAN 不匹配故 perf 自带生成器);`.env.example`;
  `selfcheck.sh`(8 项:S1×2 ALLOW_INSECURE 零赋值(源码 `[=:]` 锚定防注释
  误伤)+ 渲染配置零出现;S2 token 注入;C1 三节点 /ready;C2 单 leader
  (mTLS 复制实证);N1 明文拒;N2 无证书拒;P1 mTLS 正对照;默认 down -v,
  `--keep` 保留);README(拓扑/安全模型/配额矩阵/macOS 失真警告)。
- **验证**:kv-store fmt/clippy `-D warnings`/12 测试(rocks)绿;本地单节点
  secure 冒烟六项全过(全有或全无 fail-fast、token env fail-fast、mTLS
  bootstrap + is_leader=1、明文拒、无证书拒、client cert 收);**全栈实测
  selfcheck 8/8 绿**(首构建 ~7.5min;三节点 mTLS 选举成功;
  `raft-log/1` SST/CURRENT 落卷实证);invariants-lint 65 links 绿;
  detect_changes 11 符号全在 kv-store main.rs,无跨模块爆炸半径。
  **踩坑:不要编辑正在运行的 bash 脚本**(bash 按偏移续读,编辑 → 假语法
  错误;selfcheck 首跑因此中断,重跑即绿)。
- **下一步**:M2(rr-perf-bench 加法扩展 §5.2 之 1–5:open-loop 调度、
  `--mix`、zipfian、`--client-id`、`--timeseries-out`、TLS client)+
  S1/S2/S3 同 flag 回归。

### Session 2026-09-10(深夜)— M0 落地:durable raft log 生产接线(ADR A-0016 → implemented)

> G8 修复,生产性能方案前置阻塞解除;全部改动本 session 完成。

- **rr-raft**:`RaftGroupManager::with_rocks_base_path`(cfg `rocks`);`start_group`
  统一 log 选择(base path 已设 = `RocksLogStorage` at `{base}/{group_id}`,否则内存),
  两个维度正交收敛至 `build_node<NF>`;`start_group_rocks` 退化为兼容 shim(断言 +
  委托统一入口,`new_with_rocks` 调用方无可观察变化);`build_node` 内
  `create_dir_all(base)` 防御两级新目录。
- **rr-server**:`--storage rocks` 时新 helper `with_raft_log_dir` 接线
  `{data_dir}/raft-log/{group_id}`(gRPC/TCP 两分支,match 重构为先生成 manager 再
  Arc);mem 后端行为不变。
- **kv-store**:新增 `rocks` cargo feature(`rr-raft/rocks`)+ `--data-dir` flag
  (非 rocks 构建 fail fast 并提示重编译,与 `--kafka-brokers` 同模式);doc header 同步。
  **计划修正**:不设 `--storage`/`--durability`(`NodeConfig.storage_engine` 无生产
  消费者,假 knob 不加;log 存储固定 Strict)。
- **测试**:rr-raft 单测 ×2(`test_start_group_persists_log_with_rocks_base_path`、
  `test_start_group_rocks_shim_still_persists`;直开存储字节级 + `Raft::new` 恢复双验证。
  踩坑:metrics `last_log_index` 启动恢复竞态 → 轮询;`RocksLogStorage` 必须显式
  close+drop 才释放 DB LOCK);integration e2e
  `test_rocks_raft_log_rolling_restart_preserves_acked_writes`(3 节点双开,50 条已
  ACK,逐节点滚动重启 quorum 保持、复制收敛、恢复节点 lin-read、直开 log DB 字节级)。
- **文档**:ADR A-0016(implemented)+ 索引;CHANGELOG `[Unreleased]`(Added + Changed);
  storage-engines.md 增"Raft 日志持久化"双语段;invariants.md INV-1 强制点 #6 +
  3 条新测试登记(lint 65 links)。
- **验证**:rr-raft rocks 146 全绿(含 2 新)、e2e 一次过、invariants-lint 65 links、
  fmt/clippy `-D warnings`/workspace 全量测试(rocks)全绿。
- **下一步**:M1(perf-lab stack:deploy/perf/ + 证书/token + kv-store TLS 参数面)。

### Session 2026-09-10(晚)— 生产性能测试方案定稿 + G8 关键发现(raft log 未接线)

> 用户批准方案 A(rr-perf-bench 纯加法扩展)与 kv-store 改动;动工前用户要求核实
> "raft 集群间协调同步是否开启",查实一个生产正确性缺口。

- **方案**:`.sisyphus/plans/rr-prod-perf.md`——perf-lab(`deploy/perf/`,kv-store ×3
  rocks/strict + TLS/mTLS/token + cpuset 隔离 + 网内负载)、W1–W11 负载矩阵、
  open-loop 无协调遗漏度量、M0–M5 里程碑(~8.5 人日核心)。局部脚本
  (`scripts/perf-baseline.sh`/`soak.sh`/`deploy/lab/`)零改动为硬性验收。
- **G8 发现(三层证据)**:
  ① 复制(AppendEntries)**已开启**:stock rr-server(`main.rs:127`)与 kv-store
  (`main.rs:315`)均 `RaftGroupManager::new_with_network` → `GrpcNetworkFactory`;
  ② **raft log 持久化未开启**:`start_group` → `RaftNode::new_with_factory` →
  `RrLogStorage` 内存 log(`raft_node.rs:85`);`start_group_rocks`/`RocksLogStorage`
  生产路径零调用,且硬编码 `NoopNetworkFactory`(`group_manager.rs:1081`)——
  "rocks log + 复制"组合在生产代码中不存在,唯一双开组装在 `tests/crash`;
  ③ 影响:R-7 基线写路径最热的一段(raft log append + IoBatcher + fsync)不在测量
  回路内,A-0010 前置判据 2("存储非约束")证据不充分,**须在生产基线后重判**;
  且 Helm/K8s 默认部署形态全集群断电即丢未快照日志——strict 耐久契约在默认形态
  不成立(正确性缺口,crash harness 当时已以 rr-crash-node 绕过)。
- **M0(前置阻塞,已入计划)**:group_manager log storage × network factory 正交化 +
  RrNode 3 处调用点统一入口 + rr-server/kv-store 接线 + 双开 e2e(重启已 ACK 不丢)。
  impact 分析:`start_group` 图分析 9 个直接调用全为 rr-raft 单测;RrNode 3 处
  (node.rs:334/363/459)经文本核实(field-receiver 调用图未收录)。落地前补 ADR。
- **A-0010 重判据**:W11(mem-log vs rocks-log A/B)为直接证据档;若 rocks-log 显著
  劣化且 fsync 转热 → WAL 重判为具备开工条件,结论回写 `rr-wal-impl.md`。

### Session 2026-09-10(傍晚)— v0.2.0 final 发布(用户决策 cut tag)

> RC exit criteria 已于先前 session 满足(Phase 0 全 WS + RC 并行项关闭,专项 R-1~R-8 收官);
> 本 session 执行发布收尾并打 tag。

- **CHANGELOG 定版**:`[Unreleased]` 全部内容按类别合并入 `[0.2.0] — 2026-09-10`
  (Added/Changed/Fixed/Removed/Notes;182→182 条 bullet 零丢失,5 条为刻意合并/状态更新改写);
  补 online backup 条目(`AdminService.OnlineBackup` 流式 RPC + `rr-admin online-backup`,
  post-rc2 落地但此前无 changelog 记录);`semver-checks.yml` 条目更新为阻塞门禁表述。
- **release-notes/v0.2.0.md 定版**:发布日期 2026-09-10;Summary/Highlights 补 Phase 4 特性、
  发布加固批次(A-0011~A-0014)、R-1~R-8 与 A-0015;Breaking Changes 补 post-rc2 批次
  (`GroupMeta.leader` 删除、`OffsetToRequestId` Result 化、错误 enum `#[non_exhaustive]`、
  openraft alpha.19→alpha.34、`DurabilityPolicy` rename、行为收紧集);MSRV 叙述修正为
  1.82→1.98(原文件停留在 1.88);升级路径同步加别名迁移/non_exhaustive/GroupMeta 三步。
- **freeze checklist 签字**:F 节末项(发布后转阻塞门禁)记为既定 post-tag 动作,随 tag 后
  首个 commit 落地;release owner 签字 2026-09-10。路线图与 rr-reliability-program.md
  状态行同步置为已闭环/全部关闭。
- **发布前验证**:`cargo fmt --all -- --check`、workspace clippy `-D warnings`
  (`--all-targets --features rocks`)、workspace 全量测试(`--features
  rr-storage/rocks,integration/rocks`)、`scripts/check-invariants.sh`(63 links)全绿。
- **tag**:`v0.2.0`(annotated)打在 release commit 上;紧随一个 commit 把
  `semver-checks.yml` 转为阻塞门禁(baseline 滚动 v0.2.0-rc2 → v0.2.0,去掉
  `continue-on-error`),即 freeze checklist F 节末项。
- **待用户动作**:`git push` main + tag(本地领先 origin/main 8 个 commit + 1 个新 tag);
  push tag 后 `release.yml` 自动发布 Docker 镜像与 Helm chart(OCI);GitHub Release
  描述直接粘贴 `release-notes/v0.2.0.md` 正文。

### Session 2026-09-10 — R-2 TLA+ 形式化落地(专项收官)

> 用户指示:"从理论上正确才有可能正确"。4 份 spec 全部实测 TLC 零反例;专项 R-1~R-8 全部关闭。

- **`formal/tla/` 新目录**(与 fuzz/ 平级,不进 workspace 编译):4 份 spec + 5 个 cfg +
  README(运行手册 + 性质↔实现映射 + 实证记录)+ `bugs/` 两个植入反例。
- **MembershipGuard.tla**(INV-7/A-0012):并发变更任意交错下 n→0 无条件拒绝、低于下限必经
  force+先审计、审计完备;11,545 态零反例。**实证 1**:`bugs/MembershipGuardBuggy.tla`
  (移除拒绝路径=P0#7 修复前形态)被 TLC 3 步抓到 `QuorumLossOnlyWithAudit` 违反。
- **OffsetExactlyOnce.tla**(INV-3/A-0003+A-0011):exactly-once apply、缓存响应归属、
  watermark=最大已 apply、无虚假 Expired、fail-closed 终止;558 态零反例。**实证 2**:
  `bugs/OffsetClampBug.tla`(哨兵 clamp-to-0=P0#6 原始缺陷)被抓 `ResponseBelongsToGenuine`。
- **OutputWatermark.tla**(INV-4/A-0007):无幻影推进、保序游标、Failover 落在 SendOk↔Advance
  窗口 ⇒ 重复投递显式合法(承诺语义);280,478 态零反例。
- **RoutingTableSwitch.tla**(INV-6):单归属、收敛后无旧路由、watch 最终收敛;
  Mem 整表快照 / etcd 按 key 增量双 cfg 分别 1,411 / 4,731 态零反例。
- **CI**:`.github/workflows/formal.yml`(每周日 05:29 UTC + dispatch,非阻塞,TLC 输出
  失败时上传 artifact;tla2tools v1.7.4 钉版)。
- **双链**:invariants.md 的 INV-3/4/6/7 各增"形式化(R-2)"条目(lint 63 links 仍绿);
  spec 头部注释含 spec↔实现 file↔INV 三向链接。
- 本机工具:tla2tools.jar v1.7.4 装于 `~/.kimi-code/tools/tla2tools/`(已按环境约定登记
  INSTALLED-SOFTWARE.md)。

### Session 2026-09-09/10 — 可靠性专项批次三(R-3 DST spike + 替代方案;R-8 soak 自动化)

> 按专项顺序执行 R-3 → R-8。

- **R-3 spike(turmoil)**:承重前提全绿——tonic 0.14.5 `connect_with_connector` 存在且
  实证可用;`serve_with_incoming` 经 ~45 行 `SimIo` newtype(tonic `Connected` 无 blanket
  impl;`TokioIo` 方向相反)承载 turmoil listener;turmoil 0.7.2 与本仓 tokio/hyper 栈兼容。
  证据测试 `tests/integration/src/turmoil_spike.rs`(3 次 echo 往返,模拟时钟墙钟 0.00s),
  报告 `.sisyphus/evidence/rr-r3-turmoil-spike-2026-09-09.md`。**按计划决策规则转替代方案**:
  完整试点需 per-host ClusterBuilder + RrNode serving seam,3~4 人日超时间盒。
- **R-3 替代方案落地**:`tests/integration/src/fault_schedule.rs`——seed 化 schedule 生成
  (StdRng,轮转覆盖 pool)+ JSONL 故障日志(fdatasync/条,schema 与 crash harness/R-8 同族)
  + `RR_FAULT_SEED` 重生成 / `RR_FAULT_REPLAY` 逐条回放。E5(`test_concurrent_writes_under_
  chaos_linearizability`)与 E3b(`test_minority_partition_blocks_writes`)接入并双模式实测
  通过;6 个 schedule 单测。CI 20× 零 flaky 判据待 nightly 累积。
- **R-8 soak 自动化**:`rr-perf-bench --scenario soak`(定率 keyed 幂等写 + 周期性
  `read_linearizable` 探针 + fdatasync ack log + metrics.jsonl 计数快照;`--oracle-only`
  独立回放模式);oracle = 探针 Put(幂等缓存路径)+ linearizable Get 读回原始值,
  大日志按均匀步长采样(首尾各 100 条必含,采样有醒目告警)。`scripts/soak.sh` 一键
  harness:lab 3 节点 + **网内**负载(redirect 广告 kv-N:51001,宿主直连必挂——
  smoke 实测踩出)+ python3 seed 化故障循环(SIGKILL/pause/分区,指数
  间隔;ENOSPC 有意不进 smoke 集——docker volume 无容量上限,填宿主盘不可接受,L1 已由
  FaultInjectingStorage 覆盖)+ 三件套 oracle(无丢失/无 panic/lag 60s 收敛)+ 报告入
  `docs/perf/soak/`。`soak-nightly.yml` 每日 04:17 UTC 2h smoke,信息性。
  **实测**:60s/90s smoke 全绿(oracle 587/587、582/582);负面对照 ×2 实测命中
  (伪造未写记录 → INV-1 VIOLATION 并定位 request_id;篡改 value_hash → hash-mismatch)。
  lab 栈 kv-store 为 MemStorage——kill 覆盖的是 snapshot 恢复路径;rocks/strict 日志级
  崩溃恢复仍归 `tests/crash`。2h+ 绿由 nightly 累积(R-8 验收第 2/3 条待时间)。

### Session 2026-09-06 — 可靠性专项批次二(R-4 crash harness + 故障注入 + R-5 线性一致检查器;**A-0015 关键正确性修复**)

> 承接批次一,执行 R-4 → R-5。harness 首跑即抓到真实正确性 bug 并当批次闭环。

- **R-4 crash harness(新 crate `tests/crash`,workspace member)**:真进程级 3 节点
  rocks/strict 集群(`rr-crash-node` bin:生产组件组装——`RaftNode::new_with_rocks_factory` +
  `GrpcNetworkFactory` + 生产 gRPC impl + `ServiceStateMachineAdapter` 幂等;选择自搭二进制
  因为 stock `rr-server` 有两个已核实缺口:内建 EchoSM 不走幂等 adapter、`RrNode` 的
  `start_group` 硬编码内存 log 使 `--storage rocks` 不接 raft 日志路径)。ack-log
  (fdatasync)为 oracle 真源;seed 化 kill 计划(`CRASH_SEED` 复现);oracle 探针回放
  (同键异负载:Replay 返回原负载=存活证明,强于 log-index 对比)。scenario A
  (SIGTERM 优雅关闭契约)+ scenario B(N 轮 SIGKILL)。`crash-nightly.yml` 每日
  02:41 UTC 10 轮,信息性,失败传 artifacts。
- **R-4 故障注入**:`rr-core` feature `test-utils` 下 `FaultInjectingStorage`
  (fail_flush / torn_write 前缀 / ENOSPC StorageFull / latency;SplitMix64 seed 化;
  组合顺序 enospc>torn);`IoBatcher` 泛化 `Arc<dyn StorageEngine>`(计划选项 a 落地);
  wrapper 8 单测 + `storage_fault_tests` 4 个组提交消费者测试(rocks-gated)。
- **★ A-0015(关键,新增 ADR)**:harness 首跑抓到——异步 append 违反 openraft
  "submitted ⇒ visible" 契约(apply/复制以 submit 为门控,`engine_impl.rs`
  `next_progress_driven_command`)。debug 下 sm worker / 复制流断言杀任务
  (`sm/worker.rs:211`、`stream_state.rs:105`),节点假死;release 下**静默跳 apply**
  (状态机分叉)。修复:pending overlay(append 先 stage;读路径**克隆区间快照 →
  扫描 → 合并**,快照序对"迭代器创建即钉视图 × hook 摘除"竞态完备——次轮仪器化实证
  `lo=923→first=924` 后引入);`IoBatcher` post-flush hook(对错路径都摘除)+
  `flush_barrier()`;`truncate_after`/`purge` 先屏障后直连删(消 entry 复活 race);
  Phase 3 窗口闸门改按 submission 计数(hook 双 completion 不致窗口失效)。
  回归:rocks_log_storage +4、io_batcher +5;终证 scenario A/B 全绿(10 轮 SIGKILL,
  3266/3266,零 panic 零 head-gap)。
- **R-5 线性一致检查器**(`tests/integration/src/linearizability.rs`):porcupine 风格
  register checker(区间语义、`?` 未知响应、read-match 剪枝、失败态记忆化、违规
  贪心最小化反例);`RecordingClient` 双历史(受检:写 + `read_linearizable`;
  session:`read_local` 不送检);`KvRegisterService` + `build_register_cluster`;
  E5 既有测试升级为计数 + checker 双轨。**校准测试 model bug 修复**:
  `checker_perf_concurrent_burst` 原构造 63/64 读注定不可满足(语义错 + 指数穷举陷阱),
  改为读写区间交叠。模块 16/16 绿(含真集群 `test_kv_register_linearizable_under_partition`)。
- **验收对照**:R-4 验收 4 条全达(CI nightly 承载、场景 A 证 close 协议、四种故障各
  ≥1 消费测试、seed 复现);R-5 验收 3 条中前 2 条达(校准判非法/判合法 + 两个集成测试
  真实历史过检),第 3 条(临时分支植入 stale-read 回归)由校准测试等价覆盖、未单独做。
- **验证**:fmt / clippy `-D warnings` / invariants-lint(63 links)/ workspace 全量
  tests(rocks) 全绿;crash scenario A+B 复绿;linearizability 16/16。
- **下一步候选**:R-3 DST spike → R-8 nightly soak(依赖 R-4 ack-log/oracle 组件复用)→
  R-2 TLA+;`v0.2.0` tag 决策仍待用户。`tests/crash` 运行产物在 `target/crash-tests/`。

### Session 2026-09-05 — 服务端延迟预算可观测性批次落地

- **新指标(代码)**:`rr_server_requests_total{group_id,op,outcome}` + `rr_server_request_duration_seconds{group_id,op}`(dispatch 客户端协议边界;op=write|read|lin_read,outcome=success|redirect|error|idempotency_expired;leader redirect 不计入 raft 段);`rr_raft_apply_duration_seconds` + `rr_raft_applied_entries_total`(SM adapter,精确 applied TPS 源,无 snapshot 阶跃/leader 切换空洞);`rr_raft_append_entries_rtt_seconds{kind=heartbeat|append}`(leader→follower RTT,replication lag 的时间维度伴生指标);`rr_ingestor_commands_committed_total`(Kafka 路径 raft 已提交命令数,与生产端速率/soak 计数交叉核对);`rr_proposal_duration_seconds` 加 `source=client|batch` 标签(两者形状不同,勿混算均值)。闭环:server ≈ routing + replication + apply。
- **关键修复**:`rr-server/src/node.rs` recorder 安装处配显式 histogram buckets(`*_seconds` 统一延迟 bucket 组 + `rr_replication_batch_entries` 专用组)——否则 histogram 导出为 Prometheus summary,所有 `histogram_quantile(..._bucket)` recording rule/panel 全部无数据。
- **配套**:recording rules 按延迟预算分层重写;Grafana cluster-overview dashboard 扩充对应 panel;metrics-reference.md 全量同步(含使用注记)。
- **验证**:新增 `apply_emits_prometheus_metrics`(进程级全局 recorder 一次性安装,rr-raft 内无冲突);rr-raft 105 + rr-server 110 全绿;workspace check/clippy `-D warnings` 零告警;docusaurus build 通过;recording rules 引用指标与代码逐一对账无缺。
- **遗留**:仓库存在 6 个先前遗留的 rustfmt 漂移点(非本批次引入:`ingestor_runner.rs:501`、builtin_service/config_file/rocks/examples×2/tests/rr-lab-producer),本次仅修本批次引入的 `service_adapter.rs` 一处;`.idea/`、`.playwright-mcp/` 为本地未跟踪目录,未入库。

### Session 2026-08-29 — Phase 0 发布阻塞项全部落地(WS-1~WS-7 实施)

- **WS-1 CI 真实性**:test-rocks 传 `rr-storage/rocks,integration/rocks` + 修复 3 个 rocks 持久化测试的 bit-rot(从未编译所致);chaos job 正名 `external-broker`;bench job 启用 rocks benches;grafana 幽灵 panel(`rr_auth_rejects_total`)删除;clippy job 补 rocks 覆盖(连带修掉 rr-perf-bench 10 个存量 lint 与 integration rocks-gated 代码 4 个);coverage job 同步修 feature 传播。3 个持久化测试本地实测真跑真绿。
- **WS-2 部署制品**:Dockerfile rust 1.88 + HEALTHCHECK(/ready)+ 补 `COPY tools`(latent 构建阻断);K8s 默认 TLS Secret + 必需 admin_token + pod anti-affinity,头部注释给本地 insecure 开法;Helm `security.allowInsecure`(默认 false,与 `tls.enabled=false` 组合时模板 fail)+ anti-affinity;compose healthcheck + `service_healthy`,insecure opt-in 单一来源上移到 compose env。helm/kubectl/compose 全部实测验证。
- **WS-3 版本/API**:`workspace.package.version = "0.2.0"` 全仓单一来源(20 个 manifest);7 个错误 enum 加 `#[non_exhaustive]`(`BatchOp`/`ApplyOutcome` 评审后有意不加);新建 `semver-checks.yml`(baseline rc2,v0.2.0 前非阻塞);`.sisyphus/checklists/v0.2.0-api-freeze.md` 落地。
- **WS-4 durability(ADR A-0011 → implemented)**:`try_create_snapshot_builder(force)` 错误通道(poisoned builder)+ `rr_snapshot_build_failures_total`;`kafka_offset_to_request_id`/`OffsetToRequestId` Result 化 + ingestor supervisor fail-closed + `rr_ingestor_offset_map_failures_total`;`IoBatcher` tracked spawn + 显式 `close()`(drain→final fsync→应答 pending→join,幂等 30s deadline);删 per-append relay spawn;`stop_group` await flush(签名不变);新测试 T1–T6(核心 T4 `test_stop_group_flush_preserves_acked_and_pending`)。rr-raft rocks 130/130 绿。
- **WS-5 TCP gate(ADR A-0014 → implemented)**:`RaftTransport{Grpc,Tcp{bind_addr}}` 入 `NodeConfig`;G6(TCP 必须显式 allow_insecure)入聚合;G7(bind 必须 loopback)为硬 gate、**allow_insecure 不可绕过**(短路前置);main.rs `replace_port` 单一事实源穿线;6 条 gate 单测全绿。
- **WS-6 quorum guard(ADR A-0012 → implemented)**:守卫落 `admin.rs:537-638`(存在性→空集合→quorum;目标 voter<2 拒,n→0 永不放行);`force` proto(wire-additive)+ CLI `--force` + warn 级 `force_accepted` audit;`rr_core::membership::MIN_VOTERS_AFTER_REMOVAL=2` 单一定义;`UnsafeMembershipChange` → failed_precondition;e2e `membership_guard.rs`(3→1 拒/3→2 成/2→1 force 成)绿。遗留:watch-loop NodeRemoved 路径无守卫(A-0012 follow-up)。
- **WS-7 删 GroupMeta.leader(ADR A-0013 → implemented)**:字段/`get_leader*`/`NoLeader`/metric 全删,全部调用点清理;serde 防线测试(旧 leader 键静默忽略);新 e2e `leader_redirect_failover.rs`(kill leader → client redirect 收敛)绿;`dispatch.rs` 的 `leader_node_id:0` 占位加约束注释。
- **治理**:CHANGELOG `[Unreleased]` 增收"发布加固批次";invariants.md 闭环标注(WS-4/5/6/7);CONTEXT.md Wave 8 不实记载已修正(quorum-loss check 当时并不存在)。
- **新发现(pre-existing,已立项建议)**:`rr-client` `send_request_inner` 在 server 回 `success=false` 且无 `leader_info` 时静默返回 Ok(空 payload)——静默假成功,待修;`dynamic-*.md`/`leader-flapping.md` 仍描述虚构的 `update_leader`/`LeaderChanged` 模型,并入 RC docs 漂移清单。
- **验证**:`cargo check/clippy/test --workspace --all-targets --features rocks` 全绿(clippy `-D warnings` 零告警);未做任何 git 变更,GitNexus 索引待 commit 后 `npx gitnexus analyze` 刷新。

### Session 2026-08-28 — v0.2.0 发布加固规划(设计先行,未改代码)

- **输入**:2026-08-16 全量 review(11 个 P0,`.sisyphus/evidence/rr-full-review-2026-08-16.md`,基线 HEAD=`ba69cc8` 与当前一致)+ 2026-08-28 架构全景归档(`.sisyphus/evidence/rr-architecture-analysis-2026-08-28.md`)。
- **主路线图** `.sisyphus/plans/rr-v0.2.0-release-roadmap.md`:Phase 0 发布阻塞 WS-1(CI 真实性)/WS-2(部署制品)/WS-3(版本对齐+API freeze)/WS-4(正确性三连+durability 契约)/WS-5(TCP gate)/WS-6(quorum guard)/WS-7(删 `GroupMeta.leader`);Phase RC:WS-8 invariants.md + audit 配对 + docs 漂移;Phase 1:可靠性专项。关键路径 WS-1 → WS-4 → RC。
- **新增 ADR(均 proposed)**:A-0011 shutdown durability 契约(IoBatcher tracked spawn + 显式 close;snapshot 经 `try_create_snapshot_builder` 错误通道;offset→request_id Result 化;含 openraft 钉版源码核实的否决备选)/ A-0012 membership quorum guard(目标 voter<2 拒绝,n→0 不可 force;`force` wire-additive + audit;`UnsafeMembershipChange` 错误)/ A-0013 删除 `GroupMeta.leader`(核实结论:etcd 从不持久化该 struct,存量数据零迁移;redirect 缓存机制已存在)/ A-0014 TCP transport 安全 gate(G6 显式 allow_insecure + G7 强制 loopback 且不可绕过;明确不给 TCP 加 TLS)。
- **新增 plan**:`.sisyphus/plans/rr-durability-hardening.md`(6 阶段,T4 stop_group flush 重启测试为核心)+ `.sisyphus/plans/rr-reliability-program.md`(R-1~R-8,合计 26~34 人日,跨 RC/0.2.1/0.3.0)。
- **新增 docs**:`docs/docs/reference/invariants.md` v1——8 条 advertised 属性的不变量链 + 强制点(file:line 已核实)+ 现有测试 + 缺口→工作流对照表;定位为 living document。
- **修正义务登记**:CONTEXT.md 原 Wave 8 "服务端 quorum-loss check" 记载与代码不符,已在 A-0012 Decision §8 登记为实施期修正项。
- 本 session 纯文档产出,未修改任何源码;下一步:按路线图 WS-1/WS-2/WS-3 并行开工。

### Session 2026-05-13 — A-0005 + A-0009 implemented; A-0010 WAL ADR accepted

- **A-0005 per-caller rate limiting wire-up**: code path (`PerCallerLimiterMap`, `RateLimitConfig::per_caller_*`, `RateLimitInterceptor::with_per_caller`, 13 unit tests) was already shipped in `crates/rr-network/src/rate_limit.rs`. This session adds the missing config surface: 3 CLI flags (`--per-caller-admin-rps` / `--per-caller-client-rps` / `--per-caller-burst`) + matching `RR_PER_CALLER_*` env vars + `[rate_limit].per_caller_*` TOML fields + `ResolvedRateLimit` plumbing + `main.rs` mapping to `RateLimitConfig`. 3 new config_file tests. ADR `accepted` → `implemented`.
- **A-0009 linearizable read wire-up**: stale-read separation (`Request` + `ReadLocal` RPCs) and locking model (`Arc<TokioRwLock<S>>`) were already shipped. This session adds the linearizable variant end-to-end: new `ClientService.ReadLinearizable` RPC, `ClientDispatch::submit_linearizable_read` trait method (returns `LinearizableReadResult { payload, leader_node_id, leader_addr }`), production impl in `ClientDispatchImpl` calling `RaftGroupManager::linearizable_read` (openraft `ReadPolicy::ReadIndex` + `await_ready`), SDK methods `RrClient::read_linearizable` / `read_linearizable_blocking` / `read_linearizable_reactive`. **Oracle-flagged correctness fix**: initial implementation dropped openraft `LinearizableReadError::ForwardToLeader` — would error on followers instead of redirect. Fixed by extending `RaftError::NotLeader` with `leader_addr`, having `RaftGroupManager::linearizable_read` surface `ForwardToLeader` as the structured `NotLeader { leader, leader_addr }`, adding `optional LeaderInfo leader_info` to `ReadLocalResponse` proto (wire-additive), and teaching `RrClient::read_linearizable` to honour `leader_info` redirects (dial the leader on next retry). 3 new unit tests including `test_read_linearizable_surfaces_leader_redirect_to_proto`. ADR `accepted` → `implemented` (3-node integration test gap tracked as follow-up). The write-path `client_write_many` was already extracting `leader_node.addr`; updated to populate the extended `NotLeader.leader_addr` field for consistency.
- **A-0010 WAL log-storage backend** (`accepted`): librarian research (`bg_345e4dcc`) compared `raft-wal` (1★, openraft 0.9-only), TiKV `raft-engine` (proven but `raft-rs` protobuf API mismatch), sled / fjall (rejected — fsync issues or wrong abstraction), redb / heed (rejected — B-tree latency), and custom segmented log on `std::fs` (chosen). Decision: build custom because openraft 0.10-alpha fit + same effort as forking `raft-wal` + no supply-chain risk. **Implementation deferred** until release-build profile confirms storage is the binding constraint. Companion plan `.sisyphus/plans/rr-wal-impl.md` (12 phases, 16 ported tests + 3 WAL-specific recovery tests, ≥ 2× RocksDB acceptance bar, 24 h soak before default flip) sent to Momus (`bg_a0e5a6d2`).
- **Tests passing**: rr-network 70 / rr-raft 101 / rr-server 95 / rr-client 23 — zero regressions. Clippy `-D warnings` clean across affected crates.

### Session 2026-05-12 (late) — A-0006/A-0007/A-0008 implemented

- **A-0008 配置化**：`rr_raft::DEFAULT_SNAPSHOT_LOGS_SINCE_LAST` (50_000) + `RaftNode::new_with_factory` / `new_with_rocks_factory` 接受 `snapshot_logs_since_last: u64` 参数；`RaftGroupManager::with_snapshot_logs_since_last` builder；`NodeConfig` 新字段 + builder；CLI `--snapshot-logs-since-last` + env `RR_SNAPSHOT_LOGS_SINCE_LAST` + TOML `[node].snapshot_logs_since_last`；零值拒绝；4 个 config_file 测试 + 1 个 raft_node 测试。
- **A-0007 OutputRunner 真实实现**：替换 42 行 no-op placeholder 为完整 send loop。新增 `rr_core::output::{KafkaProducer, NoopKafkaProducer}` trait。Output runner 监听 raft 元数据通道实现 leader-only 执行，drain pending_outputs，按 OutputTarget 分发（RaftWrite via RrClient::send_request_idempotent / KafkaPublish via KafkaProducer），成功 advance_output_watermark，失败 exponential backoff（100ms → 30s cap）。`attach_output_runner` 扩展接受 kafka producer 参数。新增 metrics：`rr_output_send_total{group_id,target}` / `rr_output_send_failures_total` / `rr_output_send_duration_seconds` / `rr_output_pending`。5 个新单元测试。
- **A-0006 状态收尾**：代码自 2026-05-07 已实现且测试齐备，本 session 仅做 ADR status → `implemented` + CHANGELOG 收录 + README banner 更新。
- **ADR README banner 更新**：从 "proposed designs ... not yet implemented" 改为按 ADR 索引各自状态。A-0001/2/3/4/6/7/8 全部 `implemented`；A-0005/9 仍 `accepted`。
- **Pre-existing clippy warning fixes**：`tcp_raft_server.rs` 未用 import / `config_file.rs` + `health.rs` uninlined_format_args / `main.rs` field_reassign_with_default。`cargo clippy -p rr-core -p rr-raft -p rr-server --features rocks --all-targets -- -D warnings` clean。
- **测试通过**：rr-raft (122) / rr-server (92) / rr-core (57) / rr-storage (44) 全绿。

---

## 已完成的工作 / Completed Work

### Session 2026-05-12 — 性能分析 + IoBatcher + SDK reactive/blocking + rr-perf-bench

**代码质量 — REPL 可测试化**（`crates/rr-admin/src/main.rs`）：
- 从 `run_repl` 提取 `parse_repl_line(&str) -> ReplCommand` 和 `decode_kv_response(&[u8]) -> Result<Option<String>>`
- +19 个单元测试（69/69 全绿）
- 覆盖率：rr-admin REPL 从 ~15% 提升

**IoBatcher — Raft log group commit 引擎**（`crates/rr-raft/src/io_batcher.rs`，新文件）：
- 实现 per-group 异步 IO batcher，多提案共享单次 write_batch + fdatasync
- 三阶段积累：Phase2 try_recv 零等待 drain / Phase3 500µs 窗口 / Phase4 单次写入
- `RocksLogStorage::append()` 改为真正异步（channel 投递立即返回，callback 异步触发）
- 开放 API：`IoBatcherConfig::default/no_batching/new()`，`RocksLogStorage::open_with_config()`
- 新增 5 个 batcher 专项测试 + `log_append_batched` benchmark（26% 提升实测）
- 修复预存 bug：`crates/rr-server/src/node.rs` `PrometheusBuilder::build()` → `build_recorder()`
  （原实现会启动内置 HTTP server，多进程并发启动时端口冲突导致 node 2/3 崩溃）

**新 benchmark 文件**（`benches/benches/`，4 个新文件）：
- `framing_roundtrip.rs`：FramedEntry encode/decode 各 payload size
- `idempotency_throughput.rs`：IdempotencyLayer decide+record 吞吐
- `log_append_batched.rs`：group commit vs sequential 对比
- `log_tps.rs`：TPS 直接测量（serial blocking / reactive pipeline）

**Rust SDK — 阻塞 + 响应式接口**（`crates/rr-client/src/lib.rs`）：
- `#[derive(Clone)]` to `RrClient`（所有字段 Arc-backed，clone 共享 leader cache）
- 阻塞接口：`send_request_blocking / send_request_idempotent_blocking / read_blocking`
  → `tokio::task::block_in_place` + `block_on`，需多线程 runtime
- 响应式接口：`send_request_reactive / send_request_idempotent_reactive / read_reactive`
  → `tokio::spawn` 内部 await，callback 在 tokio worker thread 触发，调用方立刻返回
- 新增 10 个测试（23 总，全绿）：Clone 共享状态 / 阻塞失败路径 / reactive 立即返回 / 多 callback 全触发

**Java SDK — 真响应式重构**（`sdks/java/src/main/java/.../RrClient.java`）：
- 原实现：`CompletableFuture.supplyAsync(() -> blockingCall)` = 假异步（线程被占用）
- 新实现：`ClientServiceGrpc.newFutureStub()` + `ListenableFuture` → `CompletableFuture`
  → Netty event loop 驱动 I/O，无线程在 gRPC 等待期间被占用
- 异步 retry 链：递归 `sendRequestAsync` via `thenCompose` + `exceptionallyCompose`
- 非阻塞 backoff：`CompletableFuture.delayedExecutor()`（不用 `Thread.sleep`）
- 公开阻塞接口：`sendRequestSync / readSync / getClusterInfoSync` → `blockOn(future.get())`
- 新增 `sdks/java/src/test/.../RrClientTest.java`：11 个测试（21 总，全绿）
  验证：阻塞接口抛 RrClientException / reactive 立即返回 / callback 触发 / 调用线程不阻塞

**rr-perf-bench — 3 进程集成性能测试工具**（`tools/rr-perf-bench/`，新 crate）：
- 4 个源文件：`cluster.rs / metrics.rs / workload.rs / report.rs / main.rs`
- 启动 3 个 rr-server 进程，bootstrap 3-node cluster，用 rr-admin CLI 注册节点
- S1：`send_request_blocking()` — 串行阻塞，测最低延迟基线
- S2：`send_request_reactive()` + Semaphore — pipeline_depth 个请求始终 in-flight，响应驱动
- HDR Histogram 延迟采集，table 输出 TPS / p50 / p95 / p99 / p999 / Errors
- CLI：`--scenario / --storage / --durability / --payload / --concurrency / --warmup-secs / --duration-secs`

**实测性能数据**（debug build，3 进程 localhost，RocksDB）：
- S1 串行（Strict）：~568 TPS，p50=1.4ms（bottleneck：debug build CPU + gRPC RTT，非 fsync）
- S2 reactive（ReplicatedOnly，depth=100）：~485 TPS，p99=720ms（server 侧饱和）
- 三种 durability 几乎相同（< 2% 差异）→ debug build 下 CPU 是瓶颈，非 storage
- release build 预估：2k-5k TPS（serial）；高并发 IoBatcher 后 50k+ TPS

**关键发现（写入链路分析）**：
- `append()` 之前 blocking await：RaftCore task 被卡，openraft pipeline 失效
- `IoBatcher` 修复后：append 异步，多提案可同时 in-flight，group commit 生效
- Java SDK 原 "假响应式"根因：`newBlockingV2Stub` 线程占用全程
- 真响应式必须用 `newFutureStub` 把 I/O 交给 Netty

---

### Session 2026-05-08 — Framework Review + Quality Hardening + openraft 0.10 + Follower Read

**Framework Deep Review**（`.sisyphus/evidence/rr-framework-review-2026-05-08.md`）：
- 三轮并行 explore agent 分析：错误处理、API 完整性、性能/并发
- 产出完整的能力/限制/缺陷/边界报告

**openraft 0.9.21 → 0.10.0-alpha.19 迁移**（`c19d258`）：
- 移除 storage-v2 feature（0.10 已合并）
- RrTypeConfig 新增 Term/LeaderId/Vote/Responder 关联类型
- RaftStateMachine::apply() 从 IntoIterator → Stream<EntryResponder>
- RaftLogStorage：truncate → truncate_after，移除 read_vote，全 io::Error
- RaftNetwork → RaftNetworkV2：full_snapshot，stream_append/transfer_leader
- Raft<C> → Raft<C, SM>，RaftMetrics 类型参数统一

**Follower Read 特性**（`c742a98`）：
- 两种一致性：stale（本地直读）+ linearizable（ReadIndex via openraft 0.10）
- Proto: ClientRequest 新增 read_only=4, linearizable=5
- SDK: RrClient::read() + read_linearizable()
- ServiceStateMachineAdapter 改为 Arc<RwLock<S>> 支持并发读

**Quality Hardening P0-P3**（`7d183d8`, `35943e7`, `069f24c`）：

P0（已修）：
- Lock poisoning：service_adapter/rate_limit 5 处 .expect() → .unwrap_or_else(|p| p.into_inner())
- RetryConfig 溢出：5 处 2u64.pow(attempt) → saturating_mul/pow(min(20))

P1（已修）：
- Ingestor batch write：write_batch() via client_write_many()，吞吐 ~1k → ~10k+ msgs/sec/group
- Service lifecycle hooks：on_leader_elected/on_step_down/shutdown，自动 via metrics_ticker

P2（已修）：
- GroupManager 7 RwLocks → 1 DashMap<GroupId, Arc<Mutex<GroupState>>>
- Admin proto 错误码 gRPC-idiomatic：5 RPC 改用 Status 替代 bool success+message

P3（已修）：
- Rate limiter 后台 GC task（Weak Arc，60s 间隔）
- Backup/restore SHA-256 checksum（data_sha256 in metadata.json，verify on restore）

**ADR 新增**（同 session）：
- A-0005: Per-caller rate limiting design
- A-0006: RocksDB CF-level tuning design

**新增 contract tests**（同 session）：
- test_idempotency_dedup_survives_leader_switch (P0/C-005)
- test_committed_entry_applies_exactly_once_after_leader_crash (T-005)
- test_snapshot_state_digest_consistent_across_nodes (T-002)
- test_ingestor_survives_leader_switch_no_message_loss (T-003)

### Session 2026-04-27 — multi-language SDK + etcd contrib + dashboard polish（4 个并行交付，未 commit）

用户指示：做"java&rust sdk + etcd ConfigProvider + dashboard 持续打磨"，跳过 P2-6 reset-ingestor-offset CLI。Phase A 串行（先 review Rust SDK 找 gap），Phase B/C/D 并行委派给 deep-类目 subagent。

**Phase A — rr-client (Rust SDK) 审查 + 优化** (`crates/rr-client/`, 自己做)
- 审查报告：`.sisyphus/evidence/rr-client-sdk-review-2026-04-27.md`。结论：核心 API（retry / leader cache / idempotency / metrics / tracing）合格保持原状，但 3 个 P0 阻塞 + 1 个 P2 polish 必修。
- **F1 修复 — TLS/mTLS**：新 `RrClientBuilder::with_tls(TlsConfig)`，`connect_to_addr` 按 `tls_config` 选 `https`/`http` scheme，wire `endpoint.tls_config(tls.build_client_tls()?)`。
- **F2 修复 — bearer token**：`with_bearer_token(String)` + 内部 `build_request<T>` helper 调 `rr_network::auth::attach_bearer_token` 给所有 outbound（`send_request_inner` + `get_cluster_info`）。
- **F3 修复 — Cargo.toml metadata**：description / license=MIT / repository / homepage / documentation / readme / keywords / categories 全补齐，`rust-version.workspace = true`。`cargo publish --dry-run` 仅卡 rr-core/rr-network path-dep version（workspace-wide 问题，与"暂不做版本管理"对齐，不阻塞 SDK 改动）。
- **F4 修复 — README**：`crates/rr-client/README.md`（quick start dev / production TLS+auth / idempotency / cluster discovery / metrics / security notes / license / links），~140 行。
- **安全语义**：TLS-less bearer 默认 `tracing::warn!`（与 server `allow_insecure` opt-in 对齐）；`with_strict_security(true)` 时 build 拒绝（`NetworkError::ConnectionFailed`）。
- **向后兼容**：`RrClient::connect` / `connect_with_config` 保留为 builder 薄包装，无 `#[deprecated]`。
- **测试**：8 原有 + 7 新增 = 15 unit tests 全绿（builder defaults / with_bearer_token_stores_token / strict_security_rejects_token_without_tls / with_retry_config_overrides / empty_addrs_rejected / build_request_attaches_bearer_token / build_request_omits_metadata_when_no_token）。
- **L1 验证**：cargo check workspace clean / clippy `-D warnings` clean / cargo doc clean。
- **版本**：仍 `0.1.0`（用户决定保持，等 workspace 统一发版时再 bump）。

**Phase B — Java SDK** (`sdks/java/`, 委派 `bg_af216421` 21min, 已独立验证)
- 完整 Gradle Kotlin DSL 项目骨架：`build.gradle.kts`, `settings.gradle.kts`, `gradle.properties`, Gradle wrapper, JDK 17 toolchain。
- Maven groupId `io.github.gardenias`，Java root package `io.github.gardenias.rr.client`。
- protos 通过 `protobuf-gradle-plugin` 直接消费 `crates/rr-network/proto/*.proto` —— **不复制 proto 到 Java 树，single source of truth**。
- 公开类：`RrClient` / `RrClientBuilder` / `RetryConfig` / `TlsConfig` / `IdempotencyKey` / `LeaderCache` / `BearerTokenInterceptor` + 异常族（`RrClientException` → `NetworkException` → `RpcFailedException` + `IdempotencyExpiredException`）。
- 公开方法用 Java 命名：`sendRequest` / `sendRequestIdempotent` / `sendRequestWithPartition` / `getClusterInfo`，全部返 `CompletableFuture`（async idiom 对齐 Rust `async fn`）。
- Builder 方法：`newBuilder(List<String>)`（Java keyword `new` 不能作方法名，README 已说明） / `withTls(TlsConfig)` / `withBearerToken(String)` / `withRetryConfig(RetryConfig)` / `withStrictSecurity(boolean)` / `build() -> CompletableFuture<RrClient>`。
- 依赖（轻量）：grpc-netty-shaded 1.66 + grpc-protobuf + grpc-stub + protobuf-java + Micrometer 1.13（必选）+ slf4j-api 2.0（必选）。**无** OpenTelemetry / Resilience4j / Lombok / Guava / Apache Commons / Spring。
- **bearer auth**：`io.grpc.ClientInterceptor` 实现，`Metadata.Key.of("authorization", ASCII)` 注入 `"Bearer " + token`，与 Rust SDK 同 wire shape。
- **TLS**：`NettyChannelBuilder.useTransportSecurity()` + `GrpcSslContexts.forClient().keyManager().trustManager().build()`，`domainName` 切 SNI override authority。
- **strict security**：`build()` 时 token+tls=null+strict 抛 `IllegalStateException`，与 Rust SDK 同语义；warn-only mode 用 slf4j WARN。
- **Metrics**：Micrometer `MeterRegistry`（默认 `Metrics.globalRegistry`，可 builder 覆盖）。Timer `rr.client.request.duration`{group_id, outcome} + Counter `rr.client.retries` / `rr.client.leader_cache.hits|misses`。Prometheus 导出后是 `rr_client_*`，与 Rust SDK 指标 surface 对齐。
- **测试**：9/9 unit tests pass（`BearerTokenInterceptorTest` 2 + `LeaderCacheTest` 3 + `RrClientBuilderTest` 4），0 failures/errors/skipped。
- **集成测试**：`RrClientIntegrationTest` 在独立 source set，gated by `RR_TEST_DOCKER=1`，本环境 ghcr.io 镜像不可达 → fallback 到 `InProcessChannelBuilder` 假 service（happy path 验证）。
- **CI**：`.github/workflows/sdks-java.yml` —— `actions/setup-java@v4 (temurin 17)` + `gradle test build`，tag publish 到 GitHub Packages（不发 Maven Central）。
- **GH Packages publish**：wired in `build.gradle.kts` via `maven-publish`，仅配置不真发。
- **Verification**：`./gradlew test build check javadoc --warning-mode all` 全绿，`build/libs/rr-client-0.1.0.jar` 产出。
- 主分支独立 re-run `./gradlew test --rerun-tasks`：BUILD SUCCESSFUL 9 tasks executed。

**Phase C — etcd ConfigProvider** (`crates/rr-config-etcd/`, 委派 `bg_f2d7a2de` 14min, 已独立验证)
- 新 workspace crate `rr-config-etcd v0.1.0`，依赖 `etcd-client 0.14`（加到 `[workspace.dependencies]`）+ `tokio-stream 0.1`。
- `EtcdConfigProvider::new(endpoints: Vec<String>, cluster_id: String)` async constructor。
- 实现 `ConfigProvider` trait 三方法：`get_routing_table`（prefix 扫 + 装配 RoutingTable）、`watch_changes`（3 个 etcd watcher → mpsc → Stream<RoutingTableChange>）、`get_node_addr`（直 Get）。
- **read-only**：trait surface 不暴露写入。测试 setup 用 raw `etcd_client::Client` 写 key（非公共 API）。
- **ETCD KEY SCHEMA**（`.sisyphus/adr/A-0004-etcd-config-schema.md` 文档化）：
  - `/rr/{cluster_id}/nodes/{node_id}` → JSON `NodeAddr { addr }`
  - `/rr/{cluster_id}/partitions/{partition_id}` → JSON `{ config, group_id }`
  - `/rr/{cluster_id}/groups/{group_id}/replicas` → JSON `[node_id, ...]`
  - cluster_id 默认 `"default"`，构造器可改
  - 用 JSON 而非 bincode：操作员可 `etcdctl get --prefix /rr/` 直接看
- **Cargo metadata**：description / license=MIT / repository / keywords / categories 完整。
- **测试**：9/9 unit tests pass（serde wire shapes / key path generation / 5 个 watch event variants 解析 / etcd 错误映射），1 个 integration test ignored（gated `RR_TEST_ETCD=1` + Docker，跟 rr-ingestor-kafka 的 `RR_TEST_KAFKA_BOOTSTRAP` 同 pattern）。
- **示例 crate**：`examples/etcd-config-quickstart/`（连 localhost:2379, fetch table, await one watch event）。
- **L1 验证**：`cargo check -p rr-config-etcd` clean / `cargo clippy --all-targets -D warnings` clean / `cargo doc` clean / `cargo build -p etcd-config-quickstart` clean / **`cargo check --workspace` 全绿**。
- **ADR A-0004**：`proposed` 状态，登记到 `.sisyphus/adr/README.md`。

**Phase D — Prometheus / Grafana 持续打磨** (`deploy/observability/`, 委派 `bg_bbc98e7c` 6min, 已独立验证)
- **Dashboard `rr-cluster-overview.json`**：6 → 12 panels（保留原 6，新增 6）：
  1. `rr_raft_replication_lag` heatmap（`{{group_id}}/{{follower_node_id}}`）+ 强制 `rr_raft_is_leader == 1` predicate（panel 描述强调 follower lag=0 是误读）
  2. Idempotency rates（replays/collisions/expired）3 条 rate
  3. `rr_idempotency_cache_size` gauge stat（按 `${IDEMPOTENCY_CACHE_MAX}` 模板变量归一化，阈值 50/90）
  4. Ingestor health（step_down_drops + seed_failures + seek_below/above_watermark）stacked timeseries
  5. `rr_framing_decode_failures_total{kind}` 按 kind stacked
  6. Learner promotion outcomes（success/failure）bargauge
- **新增模板变量**：`group_id` / `follower_node_id` / `decode_kind`（multi-value）+ `IDEMPOTENCY_CACHE_MAX`（textbox，默认 50000）。
- **Recording rules**：6 → 10：`cluster:rr_replication_lag:max_per_group`, `rr:idempotency_replay_rate_5m_raw`（旧名占用，后缀 `_raw`）, `rr:ingestor_offset_skew_total`, `rr:framing_decode_failure_rate_5m`。
- **Alerts**：6 → 9：`RrIdempotencyCacheNearFull`(warn) / `RrIngestorOffsetDrift`(warn) / `RrFramingDecodeFailures`(critical)。
- **Datasource**：所有 panel 用 `${DS_PROMETHEUS}` 变量，不硬编码 UID。
- **指标存在性 grep 验证**：12 个引用的 metric 全部 `grep -r "rr_<name>" crates/` 命中代码；列表见 `bg_bbc98e7c` task output。
- **README 更新**：`deploy/observability/README.md` Panels/Recording rules/Alerts 三段全更新 + 文末 `Last updated: 2026-04-27`。
- **未拆 dashboard**：12 panels 在 14 阈值下，未创建独立 `rr-ingestor.json`。
- **JSON/YAML 校验**：`json.load` + `yaml.safe_load` 双 parse 通过；总计 12 panels / 10 rules / 9 alerts (rr_correctness=6 + rr_availability=3)。

**未 commit**：四个 phase 全部工作树挂着，按用户"暂不做版本管理"约束。

**关键决策记录**：
- 跳过 P2-6 `rr-admin reset-ingestor-offset` CLI（用户明确 deprioritize）
- Java SDK groupId `io.github.gardenias`（GitHub-hosted org 标准 reverse-DNS）
- Java 发 GitHub Packages 不发 Maven Central（避免 GPG / Sonatype 流程）
- etcd Provider read-only（仅 ConfigProvider trait）
- rr-client 保持 0.1.0（不与 framework v0.3 同步 bump）
- 所有"轻量"约束严格执行（Java 无 OpenTelemetry/Resilience4j/Lombok）

---

### Continuation session (2026-04-26 later, 11 commits, Phase 3.1 + Oracle follow-ups + test hardening)

完成所有 Phase 3.1 Theme B 运维自动化、Oracle review P1/P2 跟进、Phase 3.1 Theme C 测试加固。

**Wave 8 — admin add_learner + change_membership** (`89abdf3`)
- proto: AddLearner / ChangeMembership / GetGroupMembership RPCs + types
- rr-server: AdminServiceImpl 实现，含空 voter 拒绝(~~quorum-loss check~~ 修正 2026-08-29:当时**并未**实现 server 端 quorum 守卫,此行为不实记载;守卫由 ADR A-0012 于 2026-08-29 实际落地,见 `admin.rs::change_membership`)
- rr-admin: `group add-learner / change-membership / membership` 命令
- 4 unit + 3 CLI parse tests

**Wave 9 — T0.3 rr-admin node-reset** (`0ab0918`)
- 新模块 `crates/rr-admin/src/node_reset.rs`：3 步前置检查（路径存在 + 看着像 rr 数据
  目录 + RocksDB LOCK 未被持有）+ 必须 `--force`，防止 `--data-dir /` 类拍脑袋
- CLI `rr-admin node-reset --data-dir <path> [--force]`，输出后续 add-learner +
  change-membership 步骤说明
- 8 unit + 2 CLI parse tests

**Wave 10 — B.2 auto-promote learner** (`917c113`)
- 新模块 `crates/rr-raft/src/learner_promoter.rs`：tokio task 监听
  `raft.metrics()`，仅 leader 时扫 membership，per-learner 跟踪 stability ticks
  （连续 3 次 `lag <= max_lag=50` → promote）
- 步下台清空 stability counter；可在运行时 hot-reload config
- audit-log 配对 started/succeeded|failed 事件，新指标
  `rr_learner_promotions_total{group_id}` + `rr_learner_promotion_failures_total`
- `RaftGroupManager::start_learner_promoter` / `stop_learner_promoter` 公开 API

**Wave 11 — B.1 online backup streaming RPC** (`2f03e4a`)
- proto: `OnlineBackup(OnlineBackupRequest) returns (stream OnlineBackupChunk)`
  with `OnlineBackupMetadata { group_id, last_log_index, last_log_term, size_bytes,
  rr_version }`
- rr-server: tokio_stream + 64 KiB chunks，匹配 InstallSnapshot 的 chunk 大小；
  `force_fresh_snapshot=true` 触发新 snapshot；wait-up-to-60s 让 snapshot
  materialize；client-disconnect 在 audit log 中可见
- rr-admin: `group online-backup --group <ID> --output <FILE> [--force-fresh]`，
  byte-count 校验 + JSON/human 输出

**Wave 12 — B.3 DecommissionNode workflow** (`7d42363`)
- 新模块 `crates/rr-admin/src/decommission.rs`：5-step orchestrator（inspect →
  refuse on quorum loss → drain via TransferLeader → ChangeMembership → confirm）
- `MIN_VOTERS_AFTER_REMOVAL = 2`：拒绝把任何 group 留到 < 2 voters；操作员需先
  add 一个新 voter 或用 node-reset
- CLI `rr-admin node decommission --node-id <ID> [--dry-run]`，per-group action
  enum 区分 Skipped/AbortedQuorumLoss/WouldRemove/DrainedAndRemoved/Removed/Failed
- 8 unit + 2 CLI parse tests

**Wave 13 — Oracle P1/P2 review 跟进** (`d97f2b3`)
- **P0-1**（降级 perf）：`refresh_snapshot_if_stale` 在 seed_offset 前触发
  `raft.trigger().snapshot()` if `snapshot_index < last_applied`，bound
  leader-promotion re-fetch 浪费到 ≤ 30s 而非"全部 post-snapshot 条目"
- **P1-3**：`rr_framing_decode_failures_total{kind}` 在 FramedSnapshot /
  FramedEntry / ApplyOutcome decode 错误路径
- **P1-4**：Kafka seek 用 `Consumer::fetch_watermarks` 校验 [low, high]，
  out-of-range 发 `rr_ingestor_seek_below_watermark_total` /
  `rr_ingestor_seek_above_watermark_total`
- **P2-1**：runner 调用 `commit_applied_bytes` 让 Kafka `__consumer_offsets`
  反映 rr 进度（仅 observability，rr 自身不依赖）
- **P2-2**：`IdempotencyLayer::restore` 改 replace（不 merge）→ 双重 restore
  幂等
- **P2-3**：`ApplyOutcome` 加 `[0xAA][0x01]` magic+version header，防止 raw
  user bytes 被误 decode 成 Success(garbage)
- **P2-4**：`attach_ingestor` TOCTOU fix（先取 ingestor_handles 写锁，再
  check get_raft，与 stop_group 的取锁顺序一致）
- **P2-5**：`IdempotencyState.reserved` → `data` 重命名，`#[serde(alias =
  "reserved")]` 保后向兼容（bincode v2 标准配置是 positional 编码，wire 不变）
- P2-6（reset-ingestor-offset CLI）defer 到 follow-up — 罕见用例，需独立设计

**Wave 14 — Theme C.1 TLS+mTLS+auth e2e** (`c1d0d41`)
- `tests/integration/src/tls_auth_e2e.rs`：`rcgen 0.13` 生成内存中 CA + server
  cert + client cert，spawn 真实 tonic Server（mTLS required + bearer auth），
  客户端 4 种凭据状态全测：valid / wrong-token / no-token / no-client-cert
- 一个 async test 函数承载 4 个 scenario，cert+server 开销摊销

**Wave 15 — Theme C.2 cargo-fuzz** (`a4c8fe6`)
- 新 `fuzz/` sub-crate（独立 workspace，避免 panic=abort + nightly 污染主 workspace）
- 4 个 fuzz target：FramedSnapshot / FramedEntry / ApplyOutcome decode +
  ClientRequest proto decode
- `fuzz/README.md` 含 nightly + cargo-fuzz install + run-all snippets +
  CI workflow 模板（建议作为 nightly cron 而非 per-PR）

**Wave 16 — Theme C.3 网络分区病理 case** (`b9f53ef`)
- 3 个新 chaos test（用现有 `FaultAwareChannelPool` 而非真 iptables — 双向隔离
  在 channel 层等价于 kernel packet drop）：
  - **E3a flapping**：5 次快速 isolate/restore leader，断言无 election storm
  - **E3b minority partition**：5 节点中隔离 leader+1 follower，断言 3-node
    majority 选新 leader 且 minority NOT 接受 writes（防 split-brain）
  - **E3c double-follower isolation**：3 节点中隔离 2 followers，断言
    leader-of-minority NOT commit；restore 一个后 quorum 恢复

**Wave 17 — Theme C.4 crash-during-write 持久性** (`eea3988`)
- `test_crash_during_write_preserves_acked_entries`（`rocks` feature gated）
- Phase 1 写 20 条记录每个 ack 的 last_log_index，Drop manager **不调用
  stop_group**（最近似 in-process kill -9）
- 500ms settle delay 让 RocksDB process-global LOCK 状态释放
- Phase 2 用 fresh `RocksLogStorage::open` 读，断言 `last_log_id.index >=
  max(acked_indices)` + vote 持久化

**Wave 18 — Theme C.5 linearizability stress** (`e27a8f8`)
- `test_concurrent_writes_under_chaos_linearizability`：3 节点，5 并发 writers ×
  20 writes（100 总）+ 6 cycles of (isolate-follower 150ms + restore 100ms)
  叠加进行
- 断言 3 不变量：`last_applied >= ack_count`（无 acked-but-lost）+ `acked >= 1`
  （非 trivial）+ stable leader after chaos clears
- Pragmatic Jepsen-style stress（不引入 Maelstrom / porcupine-rs 重依赖），
  catches lost ack / divergent SM / election storm 三大类 bug

### Phase 4 v0.3.0 base (2026-04-25 ~ 2026-04-26 早段, 14 commits)

**Wave 1 — Security hardening** (`7db26ec`)
- F1/F2 完整：audit module, ServiceLabel, BearerInterceptor + RateLimitInterceptor reject 事件,
  sanitized errors, `validate_security_invariants`（5-gate startup check：raft+TLS, raft+mTLS,
  auth+TLS, admin+TLS, admin+token）
- `NodeConfig::allow_insecure` opt-in（CLI/env/TOML），fail-closed `*_token_env`
- `MAX_INSTALL_SNAPSHOT_BYTES = 4 GiB`, `rr_snapshot_installs_total{group_id}` label
- `ServiceStateMachineAdapter::new(group_id, service)` — **breaking** signature change
- `docs/deployment/upgrading.md` + Dev compose 设置 `allow_insecure=true`
- Oracle bg_1b5e149d 审查：7 处必修全部 applied

**Wave 2 — Tier 0 docs patches** (`02bfbe2`)
- T0.1 Service + StateMachine Safety Invariants rustdoc（per ADR A-0002 §D1）
- T0.2 `DurabilityPolicy` rename: `Balanced→ReplicatedOnly`, `Fast→TestOnly`,
  `#[deprecated]` aliases, `is_strict()`, `canonical_name()`, runtime warn 非 Strict 时

**Wave 3 — Tier 1 FramedSnapshot** (`c341bd5`)
- `rr_core::framed_snapshot::FramedSnapshot` (8-byte magic `\0rrSNAP1` + bincode body, version 1)
- `decode()`：3 状态返回（Some/None/Err）防止 silent corruption
- 自动迁移 v0.2.x raw bytes
- `FrameworkState { idempotency, ingestor }` schema locked

**Wave 4a — Tier 2/3 shared types** (`7ff6526`)
- `rr_core::idempotency::{IdempotencyKey, IdempotencyDecision, IdempotencyError, ApplyOutcome}`
- `rr_core::framed_entry::FramedEntry` (magic `\0rrENT01`, version field added in 4d)
- `rr_core::ingestor::{CommandIngestor, DynCommandIngestor}` 双 trait（typed + erased）解决 object-safety
- `RrError::IdempotencyExpired` variant + proto3 `optional` 字段（back-compat）

**Wave 4b — IdempotencyLayer + 写路径** (`4852448`)
- `rr_raft::idempotency::IdempotencyLayer`：watermarks HashMap + LRU cache, decide/record/capture/restore
- 5 位 client（rr-client, rr-server dispatch, rr-network ClientServiceImpl, rr-server node,
  rr-raft service_adapter）线性穿透 IdempotencyKey
- `RrClient::send_request_idempotent(group_id, payload, key)` 公开 API
- `outcome="idempotency_expired"` 直方图标签 + `RrError::IdempotencyExpired` 客户端错误

**Wave 4c.1+4c.2 — Ingestor lifecycle** (`8938114`)
- `ServiceStateMachineAdapter` 跟踪 `latest_ingestor_offset`（仅成功 apply 时推进，失败不推进）
- `snapshot()` 写入 `FrameworkState.ingestor.last_applied_offset`（非空时才写）
- `restore()` 把 offset 重新水合到 adapter
- `latest_ingestor_offset()` 公开访问器
- 新模块 `rr_raft::ingestor_runner`（~370 行）：`spawn(mgr, group_id, ingestor, auto_idem)`,
  `IngestorHandle::stop()`, 监听 raft.metrics()，leader → seek+poll，step-down → park
- 错误处理：poll 错误 expo backoff（cap 30s，绝不下台 leader），write 错误 → 退出 poll loop（park 等下次 leadership）
- `RaftGroupManager::{attach_ingestor, attach_ingestor_with_auto_idempotency, stop_ingestor,
  fetch_snapshot_bytes}` 公开 API；`stop_group` 先停 supervisor 再 stop raft

**Wave 4c.3 — rr-ingestor-kafka crate** (`bfa0d37`)
- 新 workspace crate `crates/rr-ingestor-kafka/`：
  - `config`: `KafkaIngestorConfig`, `KafkaSecurityConfig`（env-resolved 密码 per F2 模式），
    `SecurityProtocol` enum
  - `mock`: `MockKafkaIngestor` + `MockKafkaHandle`（与真实 backend 同 `Offset = i64`，便于框架 e2e 测试）
  - `backend`（`rdkafka` feature gate）：`KafkaIngestor` 包 `StreamConsumer`，单 partition `assign()`
    避开 consumer-group rebalance；`seek` 用 `offset+1` resume after persisted；`poll` deadline-bounded
    `tokio::select!`；async commit
- 默认特性 = `[]`，工作区构建无需 cmake；`--features rdkafka` 拉 librdkafka via cmake-build
- e2e 测试 `tests/e2e_real_broker.rs` gated 在 `RR_TEST_KAFKA_BOOTSTRAP` env var

**Wave 4 verify — 跨层集成测试** (`6949356`)
- `tests/integration/src/cross_tier_ingestor.rs`：3 个测试证明 Tier 1 + Tier 3 端到端协作
  - `ingestor_pushed_commands_apply_through_full_stack` — 5 个 mock kafka 消息流过 supervisor → raft
    → adapter → user Service，断言 exactly-once apply 顺序
  - `snapshot_after_ingestor_apply_carries_offset_into_framework_state` — apply offset 100..103，
    手动触发 snapshot，断言 `framework_state.ingestor.last_applied_offset` = 102
  - `restored_adapter_seeds_new_ingestor_with_persisted_offset` — fresh adapter 从 snapshot 恢复，
    bytes 经 `DynCommandIngestor::seek_bytes` 回到新 ingestor 实例

**Wave 4d — Oracle ship-blocker fixes** (`e0a8d1e`)
Oracle review (`bg_1d8162f6`) 找到 3 P0 + 5 P1。本 wave 修了 P0-2/P0-3/P1-1/P1-2 + 1 个发现的 latent watermark bug：
- **P0-2** `seed_offset` 失败现在 fatal — supervisor 退出而非 fall through 到 free-running poll
  （之前会触发 `auto.offset.reset=earliest` 导致 mass replay）
- **P0-3** Auto-idempotency on ingestor write path：新 `AutoIdempotency { request_type,
  offset_to_request_id }` config + `RaftGroupManager::attach_ingestor_with_auto_idempotency`
  方法。Runner 派生 `IdempotencyKey` per command；re-fetched messages 由 idempotency 层 dedup，
  不再 double-apply。`rr-ingestor-kafka::kafka_offset_to_request_id` 是 Kafka i64 的标准转换器
  （负 sentinel clamp 到 0 防 watermark 跳到 u64::MAX）
- **P1-1** `FramedEntry` 添加 `version: u8` 字段 + `FRAMED_ENTRY_VERSION = 1` 常量；decode 拒绝
  newer-than-supported；与 `FramedSnapshot` 的 versioning 对等
- **P1-2** `IdempotencyKey` 手写 `PartialEq` / `Eq` / `Hash` 只比较 `(request_type, request_id)` ——
  对应 layer 的 cache key + `client_id` 是 diagnostic-only 的文档承诺
- **Latent watermark bug 修复（Wave 4d 集成测试发现）**：`decide()` 旧逻辑 `cache miss + req_id <=
  watermarks.get().unwrap_or(0)` 会让首次 id=0 被错判为 Expired。新逻辑用 HashMap entry 的"存在
  与否"作为 watermark presence 信号，Kafka offset 0 第一条消息正确路由到 New
- **P1-5** observability：`rr_ingestor_step_down_drops_total{group_id}` 计数 step-down 时
  丢弃的批尾命令，pair 上 `rr_ingestor_seed_failures_total` 让操作员看到所有"沉默"故障
- 新增端到端测试 `auto_idempotency_dedups_replayed_kafka_offsets` 证明 P0-3 关闭

**Wave 5 — Tier 4 docs + example** (`e0a0337`)
- `docs/docs/guide/idempotency.md` (T4.2)：完整指南——身份模型（带 4d hand-impl 语义）、watermark、
  4-way decisions、客户端 opt-in、auto-idempotency、持久化、metrics、何时不该用
- `docs/docs/guide/kafka-input.md` (T4.2)：rr-ingestor-kafka 指南——topic 布局、生命周期、build
  prereq（cmake）、wiring example、TLS/SASL config、自定义 ingestor 实现、metrics、failure
  modes、v0.3.0 limitations 包括 P0-1 deferred 性能 issue
- `docs/docs/deployment/v0.2-to-v0.3-migration.md` (T4.4)：操作员升级指南——breaking change 清单、
  wire-format 自动迁移、rolling upgrade procedure、optional-feature adoption、downgrade procedure
- `examples/kv-store-kafka/` (T4.3)：参考示例，默认特性用 MockKafkaIngestor 所以无 cmake 即可编译；
  演示 attach_ingestor_with_auto_idempotency + replay-with-dedup
- `docs/sidebars.ts` 注册三页；`npm run build` 全清

**Wave 6.1 — Theme A.1 + A.2 observability** (`e5fdb48`)
- `crates/rr-raft/src/metrics_ticker.rs`：每组定时器，每 5s 读 `RaftMetrics.replication` 发射
  `rr_raft_replication_lag{group_id, follower_node_id}` = `leader.last_log_index - follower.matched.index`。
  **仅 leader 发射**——follower 不发射（避免 Prometheus 误读为"lag=0"）。告警时必须 predicate 在
  `rr_raft_is_leader == 1`，模块 rustdoc + metrics-reference.md 都强调
- `RaftGroupManager::start_metrics_ticker(self: &Arc<Self>, group_id)` 公开 API；`RrNode::start()`
  在每个 `start_group` 后自动调用（含三条 watch 路径：bootstrap, PartitionAdded,
  GroupMembershipChanged）
- 旧 metrics（`rr_raft_is_leader`, `rr_raft_current_term`, `rr_raft_last_log_index`）也由 ticker
  在 idle 时持续更新——以前只在 write 时更新
- `BearerInterceptor::call` reject 事件加 `caller_token_fp` 字段——非密码学 8-hex siphash
  fingerprint，让操作员关联同一 caller 的多次试探，不暴露 token
- `pub fn token_fingerprint` 公开 helper，rustdoc 大写"NOT a security primitive"防止误用
- `docs/docs/deployment/audit-log.md` 增"调用者身份"章节，更新 metrics-reference

**Wave 6.2 — Theme A.3 + A.4 ops + OTEL** (`b9776cc`)
- `deploy/observability/prometheus/rr-recording-rules.yml`：6 条预计算 PromQL（leaders observed,
  leader churn rate, p99 proposal latency, idempotency replay/collision rates, max replication lag）
- `deploy/observability/prometheus/rr-alerts.yml`：6 条默认 alert（RrInsecureModeInProduction,
  RrIdempotencyCollisionsObserved, RrIngestorSeedFailures, RrLeaderFlapping, RrNoLeader,
  RrReplicationLagHigh）
- `deploy/observability/grafana/rr-cluster-overview.json`：6-panel 仪表盘
- `deploy/observability/README.md`：wiring 说明 + 阈值理由 + 交叉引用
- `examples/otel-client/`：tracing-opentelemetry 0.27 + opentelemetry 0.26 + opentelemetry-stdout
  exporter，演示 W3C traceparent 从 client span 提取 + 注入到 outbound rr-client request 的完整 pattern
- 验证：`cargo run -p otel-client` 真实导出 traceparent + span via stdout

**Wave 7 — Theme B.4 /ready JSON body** (`582cc6d`)
- `/ready` 替换为 JSON 体：`{"ready": bool, "reason": str, "since_unix_secs": u64}`
- `reason` 自由文本（"starting", "ready", "shutting_down", "degraded:lost_quorum", ...）
- `ReadinessFlag::set_with_reason(bool, reason)` 新方法；`set(bool)` 保留 backwards-compat
- 内部存储 `Arc<AtomicBool>` → `Arc<std::sync::RwLock<ReadinessState>>`（非热路径，开销可忽略）
- K8s probe 仍只看 status code；ops tooling 可 parse body
- 7 个新单测 + 现有 `test_health_endpoints_full_lifecycle` 集成测试更新为 JSON 解析

### Phase 2 基线 (v0.1.0, 2026-04 完成)
- Multi-Raft 共识（单节点承载多个独立 Raft group）
- gRPC networking with ChannelPool
- 可插拔存储：MemStorage + RocksStorage (feature-gated)
- Dynamic ConfigProvider trait（MemConfigProvider 实现）
- Client SDK 带自动 leader-redirect + 指数退避重试
- Snapshot streaming
- 24 个集成测试 + fault-injection 框架（但 4 chaos tests 被 `#[ignore]` 禁用）
- CI：13 个 job，cargo-audit + llvm-cov + chaos + macOS

### Phase 3 核心交付 (2026-04-22~23, v0.2.0-rc1 + rc2 候选)

**Wave A — Quick wins & correctness**:
- LICENSE (MIT), CHANGELOG, CONTRIBUTING, CODE_OF_CONDUCT, PR template
- `DurabilityPolicy { Strict, Balanced, Fast }` — **修复了严重 bug**：RocksDB
  之前既没调 `set_sync(true)` 也没 `set_use_fsync(true)`，Raft 持久性合约被
  破坏。现默认 `Strict`。
- `RaftNode::new_*` 返回 `Result<Self, RrError>`，不再 panic；新增
  `RaftError::InitializationFailed`。
- **Chaos tests 启用**：重构 `FaultAwareChannelPool` 为**双向隔离**
  （per-node pool sharing FaultState）。之前只阻断入站连接，leader
  "被隔离"后仍能发 heartbeat，所以 `test_leader_isolation_and_recovery`
  永远超时。修复后 4 个 `#[ignore]` chaos tests 全部通过。
- C4 架构图套件（13 个 Mermaid 文件：Context/Containers/4 × Components/
  Deployment/5 × Dynamic flows）。
- Metrics 文档补齐 14 个（之前只有 6 个）。

**Wave B — Deployability**:
- **rr-server 二进制**（替换 4 行 stub）：clap CLI + TOML config + env var
  overrides + SIGTERM/SIGINT graceful shutdown + built-in EchoStateMachine。
- 3 个示例 config（single-node, 3node-cluster-node{1,2,3}）。
- Dockerfile（multi-stage，非 root UID 10001）+ docker-compose + K8s
  StatefulSet + Helm chart → `ghcr.io/gardenias/rr`。
- `.github/workflows/release.yml` 在 tag push 时推 Docker + Helm OCI。

**Wave C — Security**:
- TLS via rustls (`tonic tls-ring` feature) + optional mTLS。NodeConfig
  带 `tls_config`；rr-raft/ChannelPool 本地重复定义 `TlsPaths` 避免循环
  依赖。
- `scripts/gen-dev-certs.sh` — CA + per-node 自签名证书，SAN 覆盖
  `localhost`, `127.0.0.1`, `rr-N`, `nodeN.rr.local`。
- `docs/docs/deployment/tls.md` — 完整 TLS + 故障排查指南。
- Bearer-token auth（`rr-network::auth`）：AdminService 独立 token，
  ClientService 可选，RaftService 只靠 mTLS。Constant-time compare
  防止 timing attack。Token 通过 env var 引用 —— 不存在 config 文件。
- **Rate limiting**（T18）：`tokio::Semaphore` 自实现 token bucket（零新依赖），
  `RateLimitInterceptor` + `ChainedInterceptor<Auth, RateLimit>` 合成。
  **RaftService 永远不限流**（代码结构上禁止）—— throttling Raft 会
  破坏选举。耗尽返回 `RESOURCE_EXHAUSTED`。
- **Audit log** via tracing target `rr::audit`：每个 AdminService RPC
  发 structured event（op/peer/node_id/result）。Mutation RPC 发 2 个
  event 做取证配对。`docs/docs/deployment/audit-log.md` 讲 schema + 路由。

**Wave D — Operations**:
- **Backup/restore**（offline）：`rr-admin backup/restore/inspect-backup` —
  `tar.gz(metadata.json + data/)` 格式。Node 必须停机。有 `format_version`
  做版本兼容性。6 个 unit tests + e2e roundtrip 验过。
- **5 个运维 runbook** 在 `docs/docs/operations/runbooks/`：disk-full,
  leader-flapping, network-partition, snapshot-corruption,
  configprovider-unreachable。统一 Symptom→Detection→Mitigation→RCA→
  Recovery→Prevention 格式。

**Wave E — Observability + Release**:
- **HTTP /health /ready /metrics**（`rr-server::health`）— hyper 单 server
  共用 `metrics_port`。`/ready` 由 `ReadinessFlag` 驱动（start() 置 true，
  shutdown() 置 false）。K8s probe 从 TCP 改成 HTTP。
- **W3C traceparent**（`rr-network::tracing`）— 轻量 header 透传，不含
  OpenTelemetry SDK（~12MB deps）。用户 bring-your-own。
- **rr-client 客户端 metrics** — request_duration_seconds / retries_total
  / leader_cache_hits_total / _misses_total，全部 `group_id` 标签。
- `#[instrument]` 覆盖 ClientDispatchImpl / RaftGroupLookupImpl /
  RrClient::send_request*。
- **Semver policy doc**（`docs/docs/reference/compatibility.md`）— 分
  surface 的稳定性承诺、MSRV 策略、deprecation 流程。

### 本次 session 最后做的（2026-04-23 午后）

- **系统代码覆盖率报告** — `cargo-llvm-cov 0.8.5` 全工作区：
  - **76.6% 行 / 79.3% region / 77.9% 函数**
  - 最强：rr-router 97.5%, rr-config 95.9%, rr-core 95.8%, rr-client 94.4%
  - 最弱：rr-admin 39.3%（main.rs + REPL 没测）, rr-server 68.6%
    （main.rs + health.rs 没测）
  - HTML + LCOV + JSON 报告在 `target/coverage/`
  - Nightly `--branch` 触发 LLVM SIGSEGV，已知 bug；stable 的 region
    coverage 实际等价于分支覆盖率（region 粒度包含条件分支）

### 2026-04-24 session — Mermaid→PlantUML 转换 + I/O 设计 ADR 三连

**文档层（docs/）**:

- **Mermaid → C4-PlantUML 全量转换**：`docs/docs/` 下 18 个 markdown 文件、
  26 个 mermaid 代码块 100% 替换。7 个 C4 图（Context/Container/4×
  Components/Deployment）用 `!include <C4/C4_*.puml>`；5 个 C4Dynamic 用
  `C4_Dynamic`；7 个 sequenceDiagram 用 PlantUML 原生 sequence；6 个
  graph/flowchart 视情况用 C4-PlantUML 或 PlantUML component/package 图。
- **移除 `@docusaurus/theme-mermaid`**：`package.json` + `docusaurus.config.ts`
  + `package-lock.json` 同步；npm install 通过，`npx tsc --noEmit` 零错误。
- **MDX 解析错误修复**：`disk-full.md` + `metrics-reference.md` 里 `<N%` 被
  MDX v3 当成 JSX 标签，用反引号 `` `<N%` `` 包成 inline code 解决。
- **Broken links 清零**：~30 处 `/docs/intro` 改 `/`（`routeBasePath:'/'` +
  `slug:/`）；`compatibility.md` 引用 `../../../CHANGELOG.md` 改 GitHub 绝对
  链接（Docusaurus 不解析 docs 目录外的相对链接）。
- **Broken anchor 修复**：`dynamic-snapshot-transfer.md#snapshot-handling` →
  `#快照处理--snapshot-handling`（Docusaurus 为双语 heading 生成含中文的
  slug）。
- **验证**：`npm run build` 最终零警告，26 个 plantuml 代码块，0 个 mermaid。

**框架能力调研（5 个并行 explore agent）**:

研究了 rr 框架的 5 个维度并向用户报告：
- 入口方法（gRPC 三服务 + HTTP 观测 + SDK + CLI；**无 Kafka/MQ**，全 grep 零命中）
- Raft log 持久化与恢复（RocksDB 3 CF；Strict/Balanced/Fast durability；openraft 自动恢复）
- 快照机制（`Service::snapshot/restore` trait + 64KB chunked gRPC 流式传输 + 自动 restore-on-start）
- Req-resp 模式（纯同步 blocking；无 request-id；断链 = 状态已变更+响应丢失+无法恢复）
- 业务输出（metrics + 结构化日志 + tracing + admin status + topology + config watch + health；**无 CDC/watch stream/state callback**）

**设计讨论 → 三份 ADR（`.sisyphus/adr/`，英文，`proposed` 状态）**:

- **A-0001 CommandIngestor abstraction** — trait 放 rr-core（无 MQ 依赖），
  `rr-ingestor-kafka` 独立 crate（拒绝 NATS/Pulsar 当前范围）。Per-group 独立
  topic（`input_<G>`），leader-exclusive 消费，offset 进每个 Raft log entry，
  snapshot 记 `last_applied_offset`。
- **A-0002 Snapshot consistency + recovery** — 通过 **librarian 拉 openraft
  源码（`databendlabs/openraft @ 30dc1f0c`）**权威核对 3 条关键契约，纠正了
  讨论中几个不准确的说法（见下方"常见陷阱"新增条目）。计划改动：
  `DurabilityPolicy` rename + deprecate、`Service` safety invariants 文档化、
  `rr-admin node-reset` 单节点灾难恢复命令、6 步 recovery flow 表。
- **A-0003 Framework idempotency** — per `(request_type, request_id)` 去重，
  `client_id` 仅做诊断字段；opt-in per request（不带 key 走原路径，兼容业务
  自己做多字段复合幂等）；watermarks + bounded LRU，随框架 snapshot 自动
  持久化；新增 `IdempotencyExpired` error。

**Phase 4 执行计划**:

- `.sisyphus/plans/rr-phase4-v030.md` — Tier 0 (docs patches) → Tier 1
  (FramedSnapshot 基础) → Tier 2 (幂等) + Tier 3 (Kafka) 并行 → Tier 4
  (协同+文档) → Tier 5 (release)，约 3-4 周到 v0.3.0。
- **Momus 两轮审阅**（session `ses_2404bbf16ffeKOS5yqjsWAPjYa`）：
  第 1 轮 NEEDS-CHANGES（3 个 issue：T3.1-3 object-safety、T2.3 文件路径错+
  缺 dispatch、T4.4 QA 不可重复）→ 修复后第 2 轮 OKAY。
- 主要修复：`CommandIngestor` 用双 trait（implementer 面 associated type +
  framework 面 `DynCommandIngestor` 类型擦除 `Vec<u8>` offset）解决
  object-safety；T2.3 补全 gRPC → dispatch → apply 三段链路；T4.4 换成 6 步
  可重复 QA 清单。

---

## 🔴 重磅新发现(2026-09-17 晚):重启+purge 形态的第二条数据丢失路径——SM 级

> 与 LOG 级(IoBatcher 未 flush 尾部)并列且更凶:**重启节点向同伴投毒空快照,级联
> 抹掉整个集群的状态机**。

- **实锤日志**(crunch 实验后集群,kv-1):`get_snapshot: sending back snapshot:
  meta: {last_log:None, ...}`;kv-2:`install_full_snapshot ... data_len=14`(空 HashMap)
  反复安装。
- **机制**:重启 → in-mem SM 全新(last_applied=None)→ 同伴(落后)来要快照 →
  openraft 无条件向 SM 要 → **把"刚重启的空状态"当全量快照发出去** → 安装方
  `restore(空)` 且 last_applied=None → **健康节点被抹空** → 级联 → 集群僵尸
  (leader 在、term 在涨、写全部超时)。
- **第二个同源面**:重启后 SM 只能 replay 未 purge 尾部(~1000 条)——**SM 永久
  部分化**(99% 的 key 消失),因为 kv-store 的快照从不持久化。LOG 完好也没用。
- **根因归属**:A-0016(durable log)形状下,SM 快照不持久 + openraft 不防
  "回退式快照安装"(install 不校验 snapshot.last_log ≥ 本地 applied)。
- **修复方向(待拍板)**:α. 发送侧拒绝:last_applied 未追上日志尖峰时不发快照
  (sm worker get_snapshot guard);β. 接收侧防回退:install 时 snapshot.last_log
  < 本地 applied 则拒(vendor patch #4 候选);γ. **治本:kv-store 持久化 SM 快照**
  (data-dir 写盘,重启 restore——A-0016 形状的必要补全);δ. 背压(原 C)依然需要,
  断积压→OOM→重启波触发链。

## ✅ 已闭环(2026-09-21):选举安全级 INV-1——重启降级 → 陈旧多数派 → 已提交数据被毁

> 原为 🔴 开放项(2026-09-17 深挖立案);2026-09-21 定点 + 修复 + 双路实证闭环,
> 细节保留如下。修复:IoBatcher 字节预算 + save_vote 屏障 + vendor patch #3
> (+try_submit);实证:perf-lab `--smoke` ×3 全绿(W9 含)+ 宿主机近似
> oracle 5000/5000(双杀重启波 + 全集群冷重启)。

**复现配方**:`scripts/prod-perf.sh --smoke`(全档;~1/4 率,归档 20260917-143506-ed6e9f9)。
**已实证的失败解剖**(全部来自流式日志 + 取证 dump):
1. soak 中三节点于 ~45s 内相继重启(node1/node2 于 06:43:00 同一波,node3 于 06:43:36;
  死因疑似 OOM 未坐实)——重启前日志戛然而止于 snapshot build(06:42:52)。
2. node1(时任 T27 leader)重启恢复:`get_initial_state ... last_log_id: T27-N1.400039`
  ——**持久日志比客户端已 ACK 的位置(400040,即 write#1)短一条**;node2 同波恢复同为
  400039。即 **acked/committed 写(400040)在任何节点都不持久**(IoBatcher 异步
  尾部在死亡前未落盘;为何落后 ~8s 未 flush:疑似 snapshot build+2000/s 下 batcher
  被饿死,fsync 队列积压无界——无背压)。
3. node3(last_log 400039,落后 1 条)于 06:42:56 竞选 T28:重启后的 node1(400039)
  因 recency 相等而授予,node3+node1 = 2/3 **陈旧多数派当选**(node2 拒,无效)。
4. node3 的空白条目(T28)覆盖 400040 → **已提交写在最后的持有处被截断** → oracle
  INV-1(读回 None)。
5. **membership sidecar 经受住实战**:重启恢复 membership 完整(index 0 已被 purge,
  sidecar 仍给出 {1,2,3})——上轮修复在此闭环验证。

**性质**:标准 raft 假设"重启节点不会比它 ACK 过的位置更短";IoBatcher 的
ack-先于-flush 尾部打破该假设 → 重启降级 → 选举安全失效。**这不是选举代码的 bug,
是存储层 durability 契约的 bug。**
**对照实验(2026-09-17)**:SIGKILL-under-backlog(16KiB×2000/s=32MB/s,kill leader
12s 处,oracle 5000/5000 绿)——**裸 kill 不丢数据,io 门控表面健康**。但这不能洗清
门控嫌疑:该实验无 backlog/无 snapshot crunch,flush 即时,"submitted 门控"与
"flushed 门控"行为不可区分。原始失败的三要素(W5-16384 49MB/s 造积压 → 三节点
06:42:52 同刻 snapshot build(~300MB 全量序列化,2 核 CPU 压死+内存翻倍 → 疑似
OOM 重启波)→ 积压+重启叠加)缺一不可。

**✅ 已定点并修复(2026-09-21,vendor patch #3)**:原两个候选代码点(① leader
self-match 经 `update_local_progress`;② follower `Condition::IOFlushed` 捕获的
io_id)在常规路径上**都是正确的 flush 门控**——但二者共享的判据
`log_progress.flushed()` 存在一条**无 IO 的膨胀路径**:become-leader 时
`vote_handler` 无条件(`when: None`)发 `Command::UpdateIOProgress`,而
`run_update_io_progress` 在 **submit 时刻**就发 `Notification::LocalIO`,
`handle_local_io` 一律按"已 flush"处理(`try_flush` + leader `update_local_progress`)。
同步存储下无害(accepted==durable),但 rr 的 A-0015 pending overlay 让
`last_log_id` 包含未落盘尾部 → **带积压当选的节点 flushed 光标瞬间越过未 fsync
的条目**:作为 leader 自匹配未落盘尾部(quorum 只差一个真 flush 即可 commit),
下台后作为 follower 时 AppendEntries 响应门控同样被膨胀光标放行。IOId 排序
vote 优先,每换任期重新放大一次。完整链条:重启波 → 选举频繁 + snapshot crunch
积压 → 新 leader 自匹配未落盘尾部 → commit/ACK 时条目仅单点持久 → 第二波重启 →
陈旧多数派(node1+node3 @400039,node2 @400040 拒票无效)→ T28 空白条目截断
最后的持有处 → INV-1 违反。修复:`UpdateIOProgress` 改挂
`when: Condition::IOFlushed{prev_accepted}`(`accept_log_io` 返回的前置 accepted;
与 follower 重复追加路径的既有守卫同构;fresh start 为 None 保持原行为——重启
恢复的盘上日志本就持久)。**伴随修复**:带门控的 UpdateIOProgress 可能被推迟到
新任 leader 空白条目的 append 之后执行,`run_update_io_progress` 里原样的
`submit()` 会把 submitted 光标回退(debug 下直接 panic)——改用上游预留的
`try_submit()`(只在落后时推进;有序命令通道下"光标之后必已提交存储"成立)。
**注意**:IoBatcher 字节预算(同一批落地)只是缩小
暴露窗口,真正闭环是本补丁。回归承载:W9 smoke 复跑 + crash-nightly。

**修复方向终态**:① IoBatcher 字节预算背压 ✅(2026-09-21 落地,默认 64 MiB,
INV-1 强制点 #7);② ack 门控修复 = vendor patch #3 + **`save_vote` 屏障**(rr 侧,
同日):vote 直写绕过 batcher,快速完成会把 vote 主导的 flushed 光标推过仍在排队的
日志尾,放行 `Condition::IOFlushed` 门控的 AppendEntries 响应——现已改为
`flush_barrier()` 先行,"vote flushed ⟹ 此前日志已 flushed"重新成立(回归测试
`test_save_vote_waits_for_pending_appends`,INV-1 登记);③ 容器重启根因(OOM
嫌疑)——①落地后 16KiB 档在途字节有界,待 W9 复跑实证。

**⚠️ 既有 flake 登记(2026-09-21,与本次修复无关)→ 已根治(同日)**:
integration 套件在并行全量跑下,选举/恢复类测试偶发失败——
`test_leader_isolation_and_recovery` / `test_minority_partition_blocks_writes` /
`test_3node_leader_failover` 各见过(症状:恢复后写撞到
`ForwardToLeader{leader:None}`,即选主尚未收敛)。**归因实验**:vendor patch #3
摘掉的基线 ×12 轮复现 4 次,带补丁 ×15 轮 2 次——补丁前后同族同率,判为既有
flake。**根因**:openraft alpha.34 起"leader-lease 门控提案"(`ForwardToLeader::empty()`
快败,见 CHANGELOG 0.2.0 迁移注记),而 `wait_for_*` 只看 `current_leader`——
一次性写在新 leader 提交空白条目(租约建立)前必撞竞态。修复:测试 harness 新增
`write_when_lease_ready`(对 ForwardToLeader 重试至租约建立),替换 4 处一次性写
(isolation-recovery / 3node-failover / minority-partition 多数侧 / flapping)。
**修复后 ×15 轮全绿**(基线失败率 ~1/3)。

## 未完成 / 需要重做 / Open Work

### 框架功能开发：**全部完成** ✓

Phase 4 (v0.3 核心) + Phase 3.1 (Theme A 可观测性 / Theme B 运维自动化 /
Theme C 测试加固) + Oracle review P0/P1/P2 跟进 — 全部 implemented，仅以下小残留：

- **P2-6** `rr-admin reset-ingestor-offset` CLI — Oracle review 项中唯一未做。
  **罕见用例**（操作员手动修复 corrupt 持久 offset）+ 需独立 admin RPC 设计；
  defer 到后续 minor version。所有现实用例可由 Wave 9 的 `node-reset` 替代。

### 跨 session 的非紧迫待办（来自原 Phase 3 计划）

- ✅ **多语言 client SDK** — Java SDK done (`sdks/java/` 2026-04-27)；Rust SDK polished (`crates/rr-client/` 同期)。Go / Python 仍 deferred 待社区驱动。
- ✅ **etcd ConfigProvider** — done (`crates/rr-config-etcd/` 2026-04-27, ADR A-0004)。
- ✅ **Prometheus recording rules / dashboard 持续打磨** — done (2026-04-27, 6 → 12 panels / 6 → 10 rules / 6 → 9 alerts)。
- **Per-caller rate limiting**（依赖 caller identity）—— Wave 6.1 的
  `caller_token_fp` 是基础。
- **Raft LogStorage 的 RocksDB CF 单独调优**——分离 raft_log / state_machine 的
  block_cache_size / write_buffer。
- **rr-client / rr-core / rr-network 的 workspace 级 publish metadata 补齐** —— 当前 rr-client 自身 metadata 已齐，但 path-deps 缺 version，`cargo publish` 阻塞。等用户决定整体发版时一起做。

### 覆盖率维度的后续（2026-05-12 更新）

- ✅ **删除 vestigial stubs** — `rr-network/src/client.rs` + `server.rs` 文件不存在（已在早期 session 删除）
- ✅ **health.rs 集成测试** — 已有 17 个测试含 6 个真实 HTTP server 测试（见 health.rs）
- ✅ **CI coverage `--features rr-storage/rocks`** — ci.yml line 171 已有
- ✅ **rr-admin REPL 测试** — 2026-05-12 提取 `parse_repl_line` + `decode_kv_response`，新增 19 个测试，覆盖率从 15% 提升

### 已知跨 session 的待办（非本 session 引入）

- ✅ Online backup via snapshot-fetch RPC（已 done — Wave 11 `OnlineBackup` streaming RPC）
- ✅ ChangeMembership RPC 自动化（已 done — Wave 8 `change_membership` + Wave 10 auto-promote learner）
- ✅ 多语言 client SDK — Java SDK + Rust SDK polish done（2026-04-27）；Go/Python 仍 deferred
- Jepsen-style linearizability testing（pragmatic stress 已 done Wave 18，full porcupine-rs / Maelstrom 仍 deferred）

---

## 下一步工作 / Next Session's Candidate Work

**2026-05-12 更新**：本 session 专注于性能分析、IoBatcher group commit、SDK 响应式/阻塞接口、rr-perf-bench。所有变更在工作树（未 commit）。

**性能方向（按优先级）**:
1. **release build 压测** — 当前数字全是 debug build（CPU 瓶颈），release 会有 3-5× 提升
2. **IoBatcher 实际效果验证** — 需要真正并发提案才能体现 group commit（可用 rr-perf-bench S2 reactive 模式）
3. **WAL log storage** 替换 RocksDB log（raft-wal 或自实现顺序文件，去掉 LSM overhead）
4. **ReplicatedOnly 生产建议** — 3 副本下去掉 per-write fsync，100k+ TPS 可达

**SDK 方向**:
- Java SDK 测试：`./gradlew test` 21 个全绿；reactive 接口用 `newFutureStub` 真异步（已实现）
- 可选：Rust SDK `send_request_with_partition` 等其余方法补 blocking/reactive 变体

**代码质量**:
- 所有变更未 commit（工作树状态），需要用户触发 commit
- rr-admin REPL 测试 ✅ / 4 个新 benchmark 文件 ✅ / IoBatcher ✅ / SDK 两侧 ✅

**可选 follow-up（原有，未变化）**:
- P2-6 `rr-admin reset-ingestor-offset` CLI（defer）
- 多语言 client SDK Go / Python（Java + Rust 已完成）
- workspace 级发版准备

---

**Line A 摘要（原 Phase 3.1 计划）**:

摘要 / Summary:

**Tier 0 — v0.2.0 Final 前必做（~3-4 天）**:
1. 验证 `v0.2.0-rc1` release workflow 在 GitHub Actions 跑通（GHCR 权限检查）
2. Three-in-one cleanup commit：删 vestigial stubs + CI coverage 加 rocks feature + 修 32 个 `io_other_error` clippy warning
3. `rr-server/src/health.rs` 集成测试（24% → 90%）
4. F1-F4 independent reviews via oracle agent
5. 打 `v0.2.0-rc2`（Phase 3 continuation 8 个 commit 进镜像）
6. 切 `v0.2.0` final + crates.io 首发
7. Release notes + announcement

**Tier 1 — Phase 3.1 Hardening（~6-8 周，三条线并行）**:
- Theme A 可观测：`rr_raft_replication_lag` 指标、caller identity → audit log、
  Grafana dashboard + Prometheus alerts、OpenTelemetry 全栈示例
- Theme B 运维自动化：online backup via snapshot-fetch RPC、
  auto-promote learner → voter、`DecommissionNode` workflow、
  `/ready` 返回详细 JSON body
- Theme C 测试加固：TLS+auth e2e 集成测试、proto fuzzing、
  iptables 级真网络分区 chaos、crash-during-write 耐久性测试、
  Jepsen-style linearizability

**Tier 2 — Post-v1.0 愿景**:
多语言 SDK（Go/Python/Java）、etcd ConfigProvider、
secret rotation、cosign 镜像签名、SBOM、alt storage backends、
cross-partition transactions、partition split/merge 等。

---

## 常见陷阱 / Common Pitfalls (Lessons Learned This Phase)

### 性能 / SDK 陷阱（2026-05-12）

- **`PrometheusBuilder::build()` 启动内置 HTTP server（默认端口 9000）**：`rr-server/node.rs` 原来用 `build()` 而非 `build_recorder()`，导致多进程并发启动时端口冲突，第 2/3 个节点崩溃。修复：改用 `build_recorder()`（只创建 recorder，不启动 HTTP）。
- **`RocksLogStorage::append()` blocking await 让 openraft pipeline 失效**：原实现 `self.storage.write_batch(ops).await?` 阻塞等 fsync，RaftCore task 被卡住，无法处理下一个提案。修复：IoBatcher channel + 异步 callback，RaftCore 立即返回。
- **Java CompletableFuture.supplyAsync(() -> blockingCall) 是假响应式**：只是把阻塞移到 ForkJoinPool 线程，线程仍然被 I/O 占满。真响应式必须用 `newFutureStub()` 让 Netty event loop 处理 I/O。
- **debug build 下 fsync 不是瓶颈**：三种 durability（Strict/ReplicatedOnly/TestOnly）TPS 差异 < 2%，因为 CPU/序列化开销远大于 fsync。真实生产数字必须用 release build 测量。
- **rr-perf-bench 进程泄漏 → 端口冲突**：`Cluster::start()` 失败时 `RocksLogStorage` drop 不杀子进程。修复：实现 `Drop for Cluster`，调用 `child.start_kill()` 清理所有子进程。
- **RrClient 阻塞方法需 multi-thread runtime**：`send_request_blocking` 用 `block_in_place`，在 `current_thread` runtime 下会 panic。测试必须用 `#[tokio::test(flavor = "multi_thread")]`。

### OpenRaft 契约纠错（2026-04-24 sourcecheck，commit `30dc1f0c`）

- **OpenRaft 不保证 snapshot 原子性**——`build_snapshot()` 在 spawned task
  里跑，和 `apply()` 并发。实现者（`Service::snapshot()`）**必须**自己用
  lock 或 immutable view 保证原子。证据：`openraft/src/core/sm/worker.rs#L233-L237`。
  → 已写入 ADR A-0002 §Contract 1 + `Service` trait 计划中的 Safety
  Invariants doc。
- **`applied_state > log_state` 不是错误**——这是 crash-after-snapshot-install
  的正常恢复路径。OpenRaft 会 purge logs to last_applied。证据：
  `openraft/src/storage/helper.rs#L153-L165`。→ 已写入 ADR A-0002 §Contract 3。
- **真正的不可恢复场景**：`last_applied < committed` AND 所需 log 已经被
  purge（`start < last_purged_log_id+1`）→ `StorageError: read_log_at_index`。
  这才是需要 `rr-admin node-reset` 的场景。
- **OpenRaft 允许三种 apply 策略**：persistent SM（apply 时持久化）、
  persistent snapshot only（靠 replay）、transient SM（in-memory + 必须实现
  `save_committed()`）。用户 `Service` 属于哪种影响恢复行为，要文档化。

### Docusaurus / MDX 陷阱

- **MDX v3 把 `<N%` 当 JSX 标签**，build 报 "Unexpected character"。用
  反引号包成 inline code：`` `<20%` `` 而不是 `<20%`。
- **Docusaurus 不解析 `docs/docs/` 目录外的相对链接**（例如
  `../../../CHANGELOG.md`）。引用仓库根文件要用 GitHub 绝对 URL。
- **双语 heading 的 slug 含中文**：`## 快照处理 / Snapshot Handling` 生成
  `#快照处理--snapshot-handling`，不是 `#snapshot-handling`。引用 anchor
  要用完整 slug。
- **`routeBasePath: '/'` + `slug: /` 的 docs 不能用 `/docs/intro` 引用**，
  用 `/` 或 `/intro`（看 slug 定义）。

### Rust trait design 陷阱

- **Trait with associated type 不是 object-safe**——`Box<dyn Trait>` 编不过
  如果 associated type 出现在方法签名里。解法是双 trait 模式：公共 trait 保
  implementer 的 typed 体验；framework-internal 类型擦除 trait（例如用
  `Vec<u8>` 代替 `Self::Assoc`）提供 object-safety。ADR A-0001 的
  `CommandIngestor` + `DynCommandIngestor` 是例子。

### 测试基础设施陷阱（Phase 3.1 Theme C waves）

- **RocksDB process-global LOCK 不在 Drop 时即时释放**。in-process 测试
  drop manager → reopen 同 path 的 RocksDB 时遇 "lock hold by current
  process"。需要 sleep ~500ms 让 RocksDB 内部 lock 状态清空。Wave 17
  `test_crash_during_write_preserves_acked_entries` 学到。
- **`rcgen 0.13` 默认 features 不含 crypto**。`KeyPair::generate()` 需要
  `features = ["crypto", "ring"]`。新版 rcgen 把所有加密 backend 设成 opt-in。
- **`tonic` 0.14 的 `Channel::connect()` 可能不在 TLS 失败时立即报错**——
  握手延迟到第一个 RPC。所以 mTLS 缺 client cert 的测试要在 `connect_result`
  成功时也尝试 issue 一个 RPC，确保 fallback 检查触发。
- **`cargo-fuzz` 必须是独立 workspace**——它要 `panic = "abort"` profile +
  nightly toolchain + libfuzzer-sys（链 C++ runtime），跟主 workspace 的
  stable/unwind 配置冲突。`fuzz/Cargo.toml` 用 `[workspace]` 自包含。
- **clippy `single_match` 在测试 panic 路径上很烦**——`match Result { Ok(_)
  => panic!(...), _ => {} }` 必须改 `if let Ok(_) = result { panic!(...) }`。
  虽然语义等价，clippy 死磕。
- **clippy `uninlined_format_args` 触发于 assert! 的 message 里 `{:?}`** —
  修：`assert!(..., "got {:?}", err)` → `assert!(..., "got {err:?}")`。

### 原有陷阱（Phase 3）

- **RocksDB fsync 不配置 = Raft 持久性被破坏**。默认行为是 WAL 不 fsync，
  节点 crash 可能丢已 commit 的条目。永远用 `DurabilityPolicy::Strict`
  除非你真的理解后果（replicated 数据 + 定期备份）。
- **`cargo-llvm-cov --branch` 在 nightly 上 crash LLVM**（v0.8.5 +
  nightly 1.97）。目前不能依赖。Stable 的 region coverage 是替代。
- **tonic 0.14 的 TLS 需要显式 `features = ["tls-ring"]`**，而且 rustls
  0.23 要求 `install_default()` crypto provider，否则 first handshake
  panic。rr-server 在 `main()` 最早处调用了。
- **rr-raft 和 rr-network 有本地的 ChannelPool 重复定义** — 故意打破循环
  依赖。改一边要记得改另一边（包括 `TlsPaths` 新字段）。
- **`FaultAwareChannelPool` isolate 语义必须是双向的** — 单向（只拦入站）
  不能触发 leader 的 step-down，所有 leader-isolation 相关 chaos test 都
  会失败。
- **tonic interceptor 不能 Clone 是个坑** — `AsciiMetadataValue::parse`
  在某些 ASCII-like unicode（比如 💩）上不会 fail，测试要用 `\n\r` 这类
  strict invalid chars。
- **`main.rs` 的 `clap` derive 里的 `///` 会变成 `--help` 输出** — 不
  是纯 dev 文档，任何 CLI flag 的文档字符串都是用户界面。
- **gitnexus 索引会被 commit 使其陈旧**，运行 `npx gitnexus analyze`
  刷新（不带 embeddings，否则会删除）。

---

## 未来想法记录 / Future Ideas

### 正式 ADR（`.sisyphus/adr/`）

2026-04-24 的 design session 产出了三份 ADR，`proposed` 状态，尚未排期：

- **[A-0001](./adr/A-0001-command-ingestor-abstraction.md)** —
  `CommandIngestor` trait in rr-core + `rr-ingestor-kafka` 独立 crate。支持
  Kafka topic 作为命令源（每 Raft group 对应独立 topic `input_<G>`，单
  partition，leader-exclusive 消费）。offset 作为字段进入每个 Raft log
  entry，snapshot 记 `last_applied_offset`。明确拒绝：不做 feature flag
  方案（拒绝 librdkafka C 依赖污染 rr-core）；暂不做 NATS / Pulsar /
  RocketMQ 支持。
- **[A-0002](./adr/A-0002-snapshot-consistency-and-recovery.md)** —
  Snapshot 一致性契约 + 灾难恢复路径。包含对 OpenRaft 契约的**权威核对**
  （librarian task `bg_e072f2fd` 拉 openraft `30dc1f0c` 源码验证），纠正了
  几条之前文档里不准确的说法：
  1. OpenRaft **不**保证 snapshot 原子性——是实现者（`Service::snapshot()`）
     的责任。
  2. Recovery 流程分 6 步（install snapshot → replay logs → purge hole）。
  3. `applied > log_state` 是**正常恢复路径**，不是错误。
  4. `DurabilityPolicy::{Balanced, Fast}` 违反 OpenRaft durability 契约，
     计划 rename + deprecate。
  5. 新增 `rr-admin node-reset` 单节点灾难恢复命令。
- **[A-0003](./adr/A-0003-framework-idempotency-layer.md)** —
  框架级幂等：`(request_type, request_id)` 去重，`client_id` 仅作诊断字段。
  Opt-in per request（`ClientRequest.idem_key` optional），不带 key 的请求
  照常走原路径。watermarks + bounded LRU response cache，随框架 snapshot
  自动持久化。新增 `IdempotencyExpired` error。和 A-0001 天然协同——Kafka
  at-least-once + 框架 dedup = exactly-once apply。

三份 ADR 关系：**A-0002 是 A-0001/A-0003 的前置**（后两者依赖框架 state
machine wrapper 对 `Service::snapshot()` 原子性的假设）。A-0001 + A-0003
**强烈建议一起发布**（共用 `FramedSnapshot` 格式，避免多次 snapshot 迁移）。

### 未成 ADR 的零散想法

- **Per-caller rate limiting**（现在只有 per-service）：需要知道 caller
  identity，依赖 auth module 的 subject claim。和 audit log 的 caller
  identity 工作可以合并。
- **`rr-client` OpenTelemetry 集成示例** — 在 examples/otel-client/ 里
  演示用 tracing-opentelemetry 把 trace context 传到 rr 集群。
- **etcd-backed ConfigProvider** 放在 `contrib/rr-config-etcd` 独立
  crate（避免 rr-config 绑定到 etcd client 依赖）。
- **Raft LogStorage 的 RocksDB column family 单独调优** — 分离
  raft_log 和 state_machine 的 block_cache_size / write_buffer。
- **Prometheus recording rules** — 发布一组推荐的 query（leader 稳定
  性、proposal p99、replication lag 等），让 ops 直接 import。
- **`rr-admin change-membership` 自动化** — 当前 `add_learner` 后必须
  手动调 `change_membership` 提升为 voter；应该支持 "auto-promote after
  caught-up" 模式。

---

## 关键文件索引 / Key File Index

### 架构文档
- `docs/docs/architecture/c4/README.md` — C4 图套件入口（13 个图）
- `docs/docs/architecture/{overview,crate-map,multi-raft,networking,storage,data-flow}.md` — 散文版

### 部署
- `docs/docs/deployment/deployment.md` — 通用部署
- `docs/docs/deployment/tls.md` — TLS/mTLS 完整指南
- `docs/docs/deployment/audit-log.md` — 审计日志架构
- `docs/docs/deployment/kubernetes.md` — K8s + Helm
- `docs/docs/operations/runbooks/*.md` — 5 个运维手册

### 参考
- `docs/docs/reference/metrics-reference.md` — 14 个 server-side + 4 个 client-side metric
- `docs/docs/reference/compatibility.md` — semver / API stability policy

### 源码关键入口
- `crates/rr-server/src/main.rs` — 生产二进制
- `crates/rr-server/src/node.rs` — RrNode 生命周期
- `crates/rr-raft/src/group_manager.rs` — Multi-Raft 核心
- `crates/rr-network/src/{auth,tls,rate_limit,tracing}.rs` — 安全/可观测性中间件
- `crates/rr-admin/src/backup.rs` — 备份/恢复
- `crates/rr-client/src/lib.rs` — Rust client SDK（blocking + reactive 接口，Clone，23 tests，2026-05-12）
- `crates/rr-raft/src/io_batcher.rs` — IoBatcher group commit 引擎（2026-05-12 新增）
- `crates/rr-raft/src/rocks_log_storage.rs` — 真异步 append + IoBatcherConfig（2026-05-12）
- `crates/rr-admin/src/main.rs` — REPL 可测试化：parse_repl_line / decode_kv_response（2026-05-12）
- `crates/rr-config-etcd/src/lib.rs` — etcd-backed ConfigProvider, read-only, ADR A-0004 schema (2026-04-27)
- `sdks/java/src/main/java/io/github/gardenias/rr/client/RrClient.java` — 真响应式（newFutureStub）+ blocking API，21 tests（2026-05-12）
- `tools/rr-perf-bench/src/` — 3 进程集成性能测试工具（2026-05-12 新增）
- `deploy/observability/{grafana,prometheus}/` — 12 panels / 10 rules / 9 alerts (2026-04-27 polish)

### 性能 benchmark 文件（2026-05-12 新增）
- `benches/benches/framing_roundtrip.rs` — FramedEntry encode/decode
- `benches/benches/idempotency_throughput.rs` — IdempotencyLayer 吞吐
- `benches/benches/log_append_batched.rs` — group commit vs sequential
- `benches/benches/log_tps.rs` — TPS 直接测量

### 本次 session 的计划
- `.sisyphus/plans/rr-phase3-prodready.md` — Phase 3 完整 roadmap（~30 任务 × 5 wave）

### 前瞻设计 ADR
- `.sisyphus/adr/README.md` — ADR 索引 + 状态图
- `.sisyphus/adr/A-0001-command-ingestor-abstraction.md` — Kafka 输入
- `.sisyphus/adr/A-0002-snapshot-consistency-and-recovery.md` — 快照契约 + 灾难恢复
- `.sisyphus/adr/A-0003-framework-idempotency-layer.md` — 框架幂等
- `.sisyphus/adr/A-0004-etcd-config-schema.md` — etcd ConfigProvider 的 key 布局 + JSON wire shape（2026-04-27）
- `.sisyphus/adr/A-0015-log-visibility-contract.md` — Raft 日志可见性契约:submitted⇒visible(pending overlay + flush barrier;R-4 harness 发现,2026-09-06)

### 可靠性专项产物(2026-09-05/06)
- `tests/crash/` — R-4 进程级 crash harness(`cargo test -p crash-tests --features rocks`)
- `crates/rr-core/src/storage_fault.rs` — R-4 故障注入 wrapper(feature `test-utils`)
- `crates/rr-raft/src/storage_fault_tests.rs` — 组提交路径故障消费测试(rocks-gated)
- `tests/integration/src/linearizability.rs` — R-5 porcupine 检查器 + register 集群 harness
- `scripts/check-invariants.sh` / `scripts/perf-baseline.sh` / `scripts/bench-compare.sh` — R-1/R-7 门禁
- `docs/perf/baselines/v0.2.0-baseline.md` + 5 份 raw JSON — R-7 首份基线
- `.github/workflows/{crash-nightly,fuzz-nightly}.yml` — R-4/R-6 nightly 承载

### 本次 session 评审
- `.sisyphus/evidence/rr-client-sdk-review-2026-04-27.md` — rr-client SDK 审查报告（F1/F2/F3 P0 阻塞 + F4 polish，全部已修）

### Phase 4 执行计划
- `.sisyphus/plans/rr-phase4-v030.md` — v0.3.0 I/O enhancements 执行计划
  （Tier 0 docs patches + Tier 1 framework wrapper + Tier 2 idempotency +
  Tier 3 Kafka + Tier 4 synergies + Tier 5 release，~3 周）。
  **Momus 审阅通过**（session `ses_2404bbf16ffeKOS5yqjsWAPjYa`，两轮审阅，
  最终 OKAY）。

### 覆盖率报告
- `target/coverage/html/html/index.html` — HTML 可点击报告
- `target/coverage/lcov.info` — Codecov 兼容格式
- `target/coverage/coverage.json` — 可编程消费的 summary

---

## Git 状态

- **`iter/r0-persistence` 已 ff 合并进 `main`(2026-09-21)**:16 个 commit——iter 迭代
  (T1 持久快照 / R-1 ApplyContext / R-2 deferred / T6 评估)+ W9 INV-1 三修复
  (字节预算 / save_vote 屏障 / vendor patch #3+try_submit)+ flake 根治 +
  A-0012 watch 守卫 + vendor fmt(成员化后续)+ 并行会话的 vendor 入 members /
  patch#2 测试钉 / T5 AC-1 契约测试。HEAD = `d1c75d4`(合并 16 commit + vendor fmt + 本注记)。
- **Push 被网络阻塞**:github.com 经本地代理(198.18.0.38)拒连(2026-09-21 午后,
  Docker Hub 正常而 GitHub 被掐;重试 3 次无效)。已挂 cron `01M32183P8KKFC531588A75P8V`
  (每 7 分钟)自动重试:网络恢复即 `git push origin main iter/r0-persistence` +
  两个 tag(`v0.2.0` 触发 release.yml 发布 Docker+Helm;`v0.2.0-rc3` 供 my-biz
  [patch] git 依赖)。合并前验证:fmt / invariants-lint(68 links)/ rr-raft 166 /
  integration 81 / rr-server 112 / openraft 551 / crash 10/10 全绿。
- 工作树干净(除未跟踪的本地证据目录 docs/perf/prod/2026091* 与 target/w9-host/)。

---

## 项目不变量 / Project Invariants (NEVER break)

这些是已经确立的决定，后续 session 不应违反：

- **`rr-core` 必须是 DAG root**，零依赖其他 `rr-*` crate。
- **Library crate 不用 `anyhow`**（binary / example / test 可以）。
- **Library crate 不用 `println!` / `eprintln!`**，只用 `tracing`。
- **Hot path 不用 `.unwrap()` / `.expect()`**，init 代码可以。
- **所有 `tokio::spawn` 必须有 tracked JoinHandle**，不要 fire-and-forget。
- **RocksDB 必须走 `spawn_blocking`**，不要在 tokio 主 runtime 里直接 `db.put()`。
- **没有全局可变状态 / static singleton** — state 归 `RrNode` 拥有。
- **`RaftService` 永远不限流**（T18 文档+代码双重强调）。
- **不要 `rm -rf` 任何 RocksDB 文件**（即使是 `.log`）— RocksDB 依赖完整文件布局。
- **遵循 semver**（`docs/docs/reference/compatibility.md`）— 0.x.y 里 x 是 breaking。
