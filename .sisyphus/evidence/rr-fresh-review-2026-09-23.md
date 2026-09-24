# rr 全项目全新 Review — 2026-09-23

> 触发:用户指令"根据我们的目标再一次 review 整个项目,进一步的优化改进;忘记之前的分析结果,全新分析和探索"。
> 方法:9 个并行审查 agent(6 × code-reviewer 符号级 + 3 × explore 门禁/性能/文档),全部发现要求 file:line 实证;
> 6 个 P0 全部由主会话二次逐行复核确认。HEAD = `5b26d9e`(v0.2.0 + 未发布批次:client 批处理 / etcd revision 缓存 / Arc 路由快照 / 字节信封)。
> 目标锚:① 生产级稳定性可靠性(响亮失败、不丢数据)② 简洁集成 UX ③ 极高吞吐/极低延迟(G2: 单组 100k ops/s)。
> **本评审零代码改动**;落地分批见文末,待拍板。

## 总评

代码基本面健康:信封解码纪律(magic+version+金色语料+fuzz)、SnapshotStore 原子持久化、IoBatcher close 协议、
G1–G7 启动门禁、错误脱敏带泄漏断言测试、INV 登记执法均为同侪高水位,INV-1 链条经 09-21 修复后有双路实证。
本轮新发现集中在**交界面上**:客户端字节直通 apply 的解释器、TCP 实验传输的多路复用、outbox 幂等键命名空间、
运维工具的防护开关失效、etcd 配置面的可用性封顶。性能上 09-22 采样结论(传输+分配主导)得到复核确认,
并新增一个强假设:**单条 h2 连接是 30k 平台与多组 26k 持平的共同串行点**。

---

## P0 — 正确性/数据安全(6 项,全部实证)

### P0-1 日志投毒 → apply 崩溃循环(客户端可达,无需开启批处理)
- 链:`dispatch.rs:90-91` unkeyed payload 原样 propose → `service_adapter.rs:256` `FramedEntry::decode(entry)?` /
  `:278` `rr_core::batch::decode_batch(&payload)?` decode 失败向上传 → `sm_adapter.rs:326` 转 `io::Error` →
  sm worker 循环第一个错误即死(`vendor/openraft/src/core/sm/worker.rs:97`)→ RaftCore `Fatal::StorageError` 形态
  (`raft_core.rs:2092-2094`)→ 组永久 Shutdown,无任何 rr 侧看守重启;rocks 日志下重启重放毒条目 → 再次 Fatal(崩溃循环)。
- 放大面:批拆分对一切 unkeyed 字节无条件生效——任意客户端发 `\0rrBAT01`+垃圾即触发;ingestor 来的 framed entry
  同样会被拆批(reinterpret 或杀组,`service_adapter.rs:271-288` 不区分来源)。
- 修复方向:① 入口校验——dispatch/write 路径对以任一 house magic 开头的原始 payload 先做完整 decode,畸形/未来版本
  在 propose 前以 `InvalidArgument` 拒回客户端(确定性,全节点一致);② 批拆分 gate 到 `offset.is_none()`(ingestor
  字节永不重解);③ 中期(设计决策,建议 ADR):apply 期 decode/业务错误转为"已应用的错误响应"而非组级 Fatal,
  或配 Fatal watchdog + poison-skip runbook;文档化 `Service::handle` 错误=组级致命。

### P0-2 TCP 传输 request_id 跨组碰撞 → 响应错投 → false matched 推进(raft 安全)
- `tcp_transport.rs:344` `request_seq: Arc::new(AtomicU64::new(1))` 每个 network 实例从 1 重启;`:370`
  `slot = request_id % max_per_node`;`:217` 连接级 `pending.insert(request_id, tx)` 仅按 request_id 路由(`:171`),
  帧内 group_id 解析后被丢弃(`:48`)。连接池全节点跨组共享(`group_manager.rs:155-160`)→ 两个组打同一 target 的同一
  slot 必然撞号:insert 覆盖前一个 waiter,响应错投给另一组的流;`stream_append_sequential` 按请求区间构造结果
  → 组 B 可把从未发给 follower 的条目标记为已复制(failover 时=数据丢失)。
- 修复:request id 改池级/连接级原子发号;`pending` 键改 `(group_id, request_id)` 并校验响应帧 group;insert 撞号
  响亮报错。补多组并发 RPC 不错投的回归测试。

### P0-3 `OutputTarget::RaftWrite` 幂等键跨源组碰撞 → 静默丢输出
- `output_runner.rs:274-278`:`IdempotencyKey::with_client_id("rr.output", seq_to_request_id(output.seq), …)`——
  request_type 为常量、request_id 为**源组局部** outbox seq(每组从 0 起)。两个源组写同一目标组必撞:
  后者收 Collision/缓存响应而永不 apply;任一源推进 watermark 后,重启的源组低 seq 全判 Expired 永久锁死。
- 修复:以 `output.idempotency_key`(已唯一)派生 request_id,或 request_type 带源组命名空间 `rr.output.{src_group}`;
  删除 `seq_to_request_id` 恒等包装。

### P0-4 `rr-admin node-reset` 的活节点防护是死代码 → `--force` 可抹掉运行中节点的数据目录
- `node_reset.rs:168-178` `rocks_lock_held` 所有路径返回 `false`:以 write 打开 LOCK 文件并不检测 RocksDB 的
  fcntl/flock 持有。模块文档与 main.rs:278 宣称的"拒绝持有活锁的路径"从未生效。
- 修复:非阻塞排他 advisory lock 探测(`fs2::try_lock_exclusive` 或 fcntl),失败即视为持有;补"持锁即拒"测试。

### P0-5 Kafka `poll()` 错误路径丢弃已消费的部分批 → 当前 leader 任期内静默跳命令
- `backend.rs:327-334`:`Some(Err(e))` 直接 `return Err`,已入 batch 的消息(consumer position 已推进)被丢弃;
  supervisor 错误路径 `ingestor_runner.rs:429-431` 仅 `sleep(backoff); continue;`——不重 seek。常规 broker 抖动
  即造成"稳定 leader 上这批命令永不 propose",等下次换主才恢复。
- 修复:`Some(Err)` 分支先 `if !batch.is_empty() { return Ok(batch) }`(错误下次 poll 仍会浮现并触发 backoff)。

### P0-6 etcd watch 一次性死亡 → 节点配置面永久失明且不可观测
- `rr-config-etcd/src/lib.rs:248-267`:三个出口(流结束/错误/`canceled()`)全部 `break`,无重连、无
  `with_start_revision` 恢复、无 compaction 处理、无指标。部分死亡(仅 groups watcher 死)时 mpsc 不关,消费端
  无信号;全部死亡时 `node.rs:522` 只 `info!` 一行且不重订阅。另:`spawn_watcher` 丢弃 JoinHandle(`:491-493`),
  违反 tracked-spawn 不变量,也使死亡不可探测。
- 修复:watch 任务内重连环 + 从 last_seen_rev+1 恢复 + compaction→全量对账信号;返回 JoinHandle 注册表;
  `rr_config_etcd_watch_reconnects_total` 指标。

---

## P1 — 可靠性/性能/运维(按主题,全部 file:line 实证)

### 耐久性与存储
- `sm_adapter.rs:178` **membership sidecar 裸 `std::fs::write`**:非原子、无 fsync、无目录 fsync,且 fail-open
  (`:138-142` 损坏即空 membership 启动)——撕写即复现它要防的零 voter 楔形。与隔壁 SnapshotStore(tmp→fsync→
  rename→dir fsync,fail-closed)同风险类不同待遇。→ 抽共享 `atomic_write_enveloped` helper。
- `group_manager.rs:502-504` `stop_group` 在 `raft.shutdown()` 报错时 `?` 提前返回,**跳过契约性
  `log_storage.close()`**(:509-524)——恰在 RaftCore Fatal 的最坏场景漏掉最终 flush。→ close 无条件执行后再返回错误。
- `rocks_log_storage.rs:348-354` `get_log_state` 启动时 `scan` 物化整个 log CF 只为取最后一条——50k 条目 × KiB 级
  负载=每 `Raft::new` 数百 MB 瞬时。→ StorageEngine 加反向 seek/last-key 方法。
- `snapshot_store.rs:238` `save_if_newer` 每次快照 build 全量读+bincode 解码旧快照只为比 `covered_index`,且全部
  store IO(读/写/2×fsync)同步跑在 sm worker 上,apply 全线 stall(默认 50k 条目策略下 ~2 次/s)。→ 元数据小
  sidecar/进程内原子 + `spawn_blocking`。
- `snapshot_store.rs:224-228` rename 后目录 fsync 双吞(open 失败 + sync 失败均静默)——Strict 耐久承诺静默失效。
  → 至少 `warn!`,宜上传错误(调用方本就 fail-closed)。
- `rocks.rs:307-313` 组提交任务 `flush_wal(false)` 实为 no-op(manual_wal_flush 默认 false)且双吞错误;
  `group_commit_interval_ms` 在 Strict/TestOnly 下静默忽略。→ `flush_wal(true)` + warn/counter + open() 时告知忽略。
- `rocks.rs:162` 文档称 block cache "shared",实际每 CF 各建一个(`:272/:285`)+ DB 级又一个(`:360`)+ 无名 CF 落
  8MiB 默认(`:371`)——实测上限 ≈3× 文档值;`:268-275` 与 `:284-289` 重复死代码。→ open() 建单 `Arc<Cache>` 传入。
- `rocks.rs` 全部错误映射 `io::Error::other(...)` 丢 `ErrorKind`——真 ENOSPC 到上游变 `Other`,与故障注入器刻意发的
  `StorageFull`(storage_fault.rs:347)不可区分。→ 模式匹配映射 StorageFull。

### etcd provider(09-23 刚落地,新角度)
- `lib.rs:507` `route()` 每请求一次 etcd probe RTT 且 probe 失败硬错(手握有效缓存也拒服务)——可用性/延迟被 etcd
  封顶,与 100k 目标不兼容;无 single-flight(并发 miss=并发全扫)。→ probe 失败服务陈旧缓存+warn+指标;
  probe 合并/短 TTL。
- `lib.rs:412` 缓存键用**集群全局** revision:共享 etcd 上无关写入即失效(命中率归零=每写 probe+全扫 2 RTT);
  serializable probe vs linearizable scan 的成员偏斜可致永不命中或长期陈旧。→ 以前缀 max `mod_revision` 为水位。
- `lib.rs:365` etcd 客户端 `Client::connect(.., None)`——无 TLS/认证可言,公开 API 无法传 `ConnectOptions`。
  → `new_with_options`/builder。
- 整 crate 零 metrics(probe 延迟/命中率/rescan/watch 死亡不可观测)。
- `lib.rs:435/442/451` 一个畸形 key 使全集群路由读取全灭;孤儿 partition 静默编造空 replicas 组(`:459-464`)。
- `cluster_id` 空串静默落 `"default"` 且不校验字符集(含 `/` 即破 key schema)。

### server / network
- `dispatch.rs:70-72`(同 :182/:210)路由失败把 provider 内部错误原文+`NotFound` 码发给客户端——绕过 F2.5 脱敏,
  且 provider 宕机被错标 NotFound 破坏重试语义。→ 仅 PartitionNotFound→not_found,其余脱敏 `unavailable`。
- `node.rs:572-596/689` gRPC bind 失败只在分离任务里 `warn!`,`/ready` 仍 200、health 仍 SERVING——K8s 向死监听
  导流。→ `start()` 内同步 bind(serve_with_incoming)失败即上传;admin 同。
- `tcp_raft_server.rs:85-93` 帧读 `vec![0u8; payload_len]` 无上限(u32 可达 4GiB),accept 循环每连接一个无跟踪
  spawn(`:131-138`)。→ 上限对齐 RAFT_MAX_DECODING_MESSAGE_SIZE;JoinSet 管理。
- `network_factory.rs:444` / `tcp_transport.rs:401` Append/Vote 忽略 `RPCOption`(openraft 明文把 soft_ttl 执法委托
  给传输层)——半开连接静默楔死复制,心跳有超时作掩护。→ 传输调用包 `timeout(option.soft_ttl())`(full_snapshot
  已正确,:598)。
- `raft_service.rs:131-173` install_snapshot 双缓冲(~2×4GiB 瞬时)、流无总 deadline、`done` 非终止未执法。
- 无跟踪 spawn 残留:health.rs:171(每 HTTP 连接)/tcp_raft_server.rs:133/admin.rs:851/rate_limit.rs:153,225。
- `node.rs:606-607/674-675` admin/metrics 端口硬编码 `0.0.0.0`;metrics 无认证且暴露内部直方图。→ bind host 可配,
  默认 loopback。
- `rate_limit.rs:148-166` refill 每 tick 1 permit:rps≳1000 时 timer 粒度击穿→忙转吃核,配置与实际不符;per-caller
  模式 `limiter: None` 时无认证请求完全不限流(`:363-374`,文档承诺的全局兜底桶不存在)。→ elapsed×rps 批量补发
  + 兜底桶/配置校验。
- `admin.rs:948-952` `GetMetricsSummary` 恒返回零;`:218` `snapshot_in_progress` 恒 false——静默错误答案 API。
- `client_service.rs:147` 等 6 处每请求 `to_string()` 提取 traceparent 仅为通常关闭的 debug! 服务。→ `enabled!` 门控。

### rr-client / 批处理语义(默认化前必须闭合)
- `batch.rs:233` 批帧无幂等键,`send_request_inner` 重试会把整批 N 条双写放大(文档未提)。→ rustdoc+guide 响亮
  标注"批重试=双写放大,适合业务幂等负载",考虑批 RPC `max_retries:0` 或自动生成幂等帧。
- `batch.rs:179` 同 partition 两个 flush 任务可并发→跨批乱序。→ per-partition in-flight=1 串行化(或文档化)。
- `batch.rs:151-159` `max_buffered` 按条数不按字节——大负载下客户端内存无界。→ 增字节维度上限。
- `batch.rs:37-39` `linger=ZERO` 禁用定时 flush(与 Kafka 语义相反)且单独写可永久悬挂;`batch_submit` future 被
  drop 后负载仍会落盘(取消幻觉)。→ 文档或改语义。
- `batch.rs:181-206` 旧 linger 定时器可提前冲掉新窗口(代际计数修复);`fail_all` 把 Timeout 等错误塌缩为字符串;
  响应帧条目多于 waiter 时静默截断(应严格相等响亮报错)。
- 杂项:`lib.rs:185` 亚毫秒 timeout 截断为 0;reactive API 无 runtime preflight(blocking 已有);`BatchConfig`
  校验错用 `RpcFailed` 分类;`channel_pool` 字段存而不用。

### rr-admin / examples
- `main.rs:1308/1326` REPL `put`/`delete` 不查 `success`/`leader_info`,对 follower 也打印 OK(假成功回归);
  单一 `--addr` 永远只能服务 Admin/Client 其一(默认打 client 端口,`status`/`members` 恒 Unimplemented)。
- `main.rs:1068-1089` 失败的 admin mutation 退出码仍为 0(自动化误判成功)。
- `decommission.rs:119-120` 枚举的是**被联系节点**的组而非目标节点的组——多组集群下静默漏 drain。
- `backup.rs:103` `sha256_of_dir` 整文件读入内存(GB 级 SST);`:219-229` restore 有 tar-slip 面(`..` 未拒);
  `:165-166` 备份无 fsync 且 gzip flush 错误被 drop 吞。
- kv-store 手搓参数解析:缺值/畸形 panic,`--bootsrap` 拼错只 warn 不拒;`port+1000` u16 溢出;`Increment`
  i64 溢出 debug panic/release 回绕。

### rr-raft 编排
- `group_manager.rs:618-630`(attach_ingestor/attach_output_runner/start_metrics_ticker/start_learner_promoter 同形)
  取 Arc 在锁外,`stop_group` 先移除后锁——attach 可在组已停后spawn supervisor 并返回 Ok。→ 锁内复查
  `contains_key`。ingestor parked 臂对 sender-drop 热自旋(`ingestor_runner.rs:237-243`)。
- `output_runner.rs:171-176` leader 期间 `pending_outputs` 报错→ supervise 无 backoff 立即重进→CPU 自旋+日志洪水。
- `ingestor_runner.rs:352-369` / `output_runner.rs:248` 不可取消的 30s 级等待堵 `stop_group`。→ select 取消臂。
- `learner_promoter.rs:214-215` 同 tick 两个 learner 达标→第二个的 change_membership 用旧 voter 集→把刚提升的
  第一个降回 learner(membership 拍振)。→ 每次成功后重读成员或一次性批量提升。
- `sm_adapter.rs:79-83/339` `pending_deferred` 无界且撞 correlation_id 静默覆盖(丢首个 responder)。→ 上限+gauge+
  覆盖 warn。
- `idempotency.rs:87` watermarks 按客户端控制的 `request_type` 无界增长并进每份快照。→ 类型基数上限/响亮拒绝。
- `group_manager.rs:776-779` `write()` 把 NotLeader 字符串化(write_batch/linearizable_read 保留了结构化重定向)
  ——单写热路径反而拿不到 leader_info。→ 同样提取 `forward_to_leader()`。

---

## P2 快单(择要,全部有 file:line 于主报告原始输出)

- `rr-core/batch.rs:102` 32 位目标 cursor+len 回绕可 panic;`encode_batch` 文档声称 panic 实为静默截断(`:61-63`)。
- `MemStorage`:`Default` 产生零 CF 实例(mem.rs:19)、`write_batch` 中途错=前缀部分应用(与 trait 原子承诺不符)、
  `scan(start>end)` panic。
- `RocksStorage::flush()` 只 flush 默认 CF(rocks.rs:553-561)。
- `DurabilityPolicy` 弃用别名承诺 v0.3.0 移除但仍在(rocks.rs:80-91)——下个 tag 即 0.3.0 窗口,要么删要么改注。
- framed_entry.rs:38-43 "同版本可加字段"只对旧 reader/新 writer 单向成立,反向=apply 期 UnexpectedEnd(=P0-1 链),
  需文档化两阶段 rollout 要求。
- `log_storage.rs:37` `is_defensive()` 每次读/写都查 env(热路径);`intern_id_label` 全局泄漏单例(network_factory.rs:303)
  与"无全局状态"字面冲突(有界,宜文档化)。
- proto  hygiene:无 reserved 段;`ClientResponse.leader_info` 缺 `optional` 与 ReadLocalResponse 不一致;
  ClientService 默认 4MiB 解码上限与 `BatchConfig.max_bytes>4MiB` 的组合无人文档化。
- `local_read` 对未知组误报"不支持本地读"(group_manager.rs:871);重复 start_group 报 RocksDB LOCK 而非干净的
  "already exists"。
- ingestor 编码失败静默丢该消息(ingestor_runner.rs:488-493,应 fail-closed);`offset+1` 未 checked(backend.rs:287
  等,i64::MAX→debug panic/release 回绕成负哨兵)。
- `router.rs:38` `#[instrument]` INFO span 每请求一次(热路径);`router.route.not_found` 命名破 `rr_*_total` 约定。
- rr-admin 无 connect timeout;node-reset 文档宣称的 y/N 交互不存在。
- Mem provider broadcast Lagged 静默丢事件(mem.rs:154,dev 后端但消费端从不 reconcile);`remove_partition` 留陈旧
  group、`add_partition` 冲突静默保旧 replicas、`remove_node` 留悬引用。

---

## 测试 / CI / 门禁结论

**可信(merge 级)**:fmt / clippy `-D warnings` / 默认+rocks+defensive+macOS 测试矩阵 / doc-test / 阻塞式
invariants-lint(70 links,实测绿)/ semver-checks(基线 v0.2.0)/ bench-compare(已转阻塞)。批处理三件套有真实
单测+3 个 PR CI 内 e2e(client_batching.rs:161/194/222);金色语料字节绊线在案。

**不可信(缺口)**:
1. **批处理在 crash/soak/fuzz 下零覆盖**(项目自定"默认化前必须过"的门):crash 驱动只发幂等写(driver.rs:59/115),
   无批处理旋钮;soak 只有 keyed 写(soak.rs:338/385),`--client-batching` 加上去是**静默空转**(main.rs:876-881
   无条件应用、无组合校验)——需 soak 增 unkeyed 道或独立 batched oracle + 非法组合硬错。
2. **`external-broker` CI job 绿得有名无实**:不设 `RR_TEST_ETCD`/`RR_TEST_KAFKA_BOOTSTRAP` 也无 services
   (ci.yml:362-380)——etcd revision 缓存的唯一 e2e(e2e_real_etcd.rs:155,`#[ignore]`+env gate)从不运行。
3. **fuzz 无 `decode_batch` / PST / MBR target**,6 target 清单停在旧格式(fuzz/Cargo.toml:37-77);43 种子无新格式,
   gen_corpus.rs 自定的"格式变更即重生成"未被遵守也无 CI 执法。
4. invariants.md 的行号引用已漂移(如 :105、:533-535、:251-252),lint 只校验测试名/标记,看不到漂移。
5. CI 重job 无 timeout(默认 6h 上限);四个 nightly 失败仅传 artifact、无人收信;ubuntu 三 job 共享同一 cargo
   cache key 跨 feature 集;fuzz-nightly 的 cargo-fuzz 未钉版;bench-compare.sh 头部注释仍写 continue-on-error
   (9-22 已转阻塞);crash-nightly 注释仍说"two scenarios"(已 3 个)。
6. 集成测试固定 sleep 仍普遍(flaky 温床);client_batching.rs:70-72 有 port-0 TOCTOU(注释已自知)。

## 性能 — 新发现与杠杆排序(详表见主报告 agent-13 输出)

- **新假设(最值得先证)**:`rr-client` 每 addr 单条 h2 连接(lib.rs:99/357-360)+ bench 单 `Arc<RrClient>` +
  stock 拓扑所有组 leader 同节点 ⇒ 200 流挤一条 conn 的 server 侧单 conn task——与 35% h2/syscall 采样桶、
  多组 26k 平顶同时吻合。**L1:client 连接扇出 N∈{1,2,4,8} 实验(小改动,预期大杠杆,零安全风险)**;
  L2:`max_concurrent_streams`(hyper 默认 200,恰=bench k)+ `max_frame_size` 提升(一行级)。
- 其余杠杆:L3 client 微分配(`lib.rs:649` payload 每次 attempt 全克隆/`:584` group_id.to_string());L4 Bytes 端到端
  (=A-0017,v0.3.0 窗口);L5 每写 5 次全局 recorder 记录批量化;L6 worker 数/P 核亲和扫描;L7 热路径
  `#[instrument]` 降 DEBUG;L8 IoBatcherConfig 接 config_file;L9 rocks 读路径 spawn_blocking 跳数;L10 etcd prod
  改 watch 驱动失效(与 P1 etcd 节合并);L11 每写 per-group Mutex 改 DashMap 读。
- bench 自身注意:S2 客户端每 op 一次 spawn + payload clone,100k+ 时 bench 进程可能先饱和——归因前先验客户端余量。

## 文档 / 部署漂移(择要)

- **高**:根 `Dockerfile` 与 `deploy/lab/Dockerfile` 缺 `COPY vendor`(vendor/openraft 已是 workspace member+
  [patch] 源)——HEAD 无法构建,release.yml 镜像 job 与本地 compose 栈同坏(perf Dockerfile 已于 9134d87 修,
  这两处漏修)。
- 中:metrics-reference.md 6 处仍标 "v0.2.0 (unreleased)";约 20 个已发指标未收录(含 idempotency/learner/
  framing/ingestor/output 全家 + 新批处理指标);compatibility.md:122-127 引用已不存在的 "known issue" 段;
  README:202 误称 kv-store 无 admin 端口;client-sdk.md 批处理文档缺"未发布"标记;**Java SDK 缺
  readLinearizable**(proto 已有, Rust v0.2.0 已发)。
- 低:semver-checks.yml 注释基线写 rc2(实为 v0.2.0);observability README panel 数/默认端口过时;
  release.yml cargo publish `|| echo` 吞错且 crates.io 发布会静默丢 vendor patch(需说明/重试);
  k8s manifest 门禁数措辞不一(6 vs 5)。
- **已核验一致**:README quickstart 命令/输出、helm values↔config_file 字段(k8s env/flags↔cli)、perf 栈全部
  flag、grafana/rules/alerts↔代码指标(幽灵 panel 类已清零)、compatibility.md 字节格式表↔七个 magic、
  Docker 1.98 钉版三处一致、semver 基线 v0.2.0。

## 优势(明确保持)

信封解码+金色语料+fuzz 的格式纪律;A-0015 pending overlay 的论证+常驻 tripwire;IoBatcher close 协议与字节预算;
SnapshotStore fail-closed 全件套;G1–G7 门禁(含 G7 不可绕过);错误脱敏+泄漏断言测试;v0.2.0 假成功修复及其
回归测试;INV 登记执法链路;router INV-6 无回潮;`kafka_offset_to_request_id` fail-closed 范式。

---

## 建议落地批次(按目标权重排序,待拍板)

- **批次 A(P0,小改动为主)**:A1 入口 magic 校验+ingestor 批拆 gate(P0-1 ①②);A2 TCP request_id 池级化+组感知
  demux(P0-2);A3 RaftWrite 幂等键重定名(P0-3);A4 node-reset 真 flock 探测(P0-4);A5 kafka poll 部分批先交付
  (P0-5);A6 etcd watch 重连环+JoinHandle+指标(P0-6)。每项配回归测试;P0-1③ 的 apply-错误契约另起 ADR。
- **批次 B(P1 耐久/可靠快赢)**:membership sidecar 原子写 helper;stop_group 错误路径照常 close;get_log_state
  反向 seek;save_if_newer O(1) 守卫+spawn_blocking;目录 fsync 响亮化;flush_wal(true)+错误上报;dispatch 路由
  错误脱敏+改码;/ready 诚实 bind;TCP 帧上限;rate limiter elapsed 批量补发+兜底桶。
- **批次 C(性能 M3 实验)**:L1 连接扇出 × L2 流上限先行实测,同场 L6 worker/亲和;随后 L3/L5/L7 清理;
  Bytes 化留 A-0017 窗口;L8/L9/L10 rocks/etcd prod 线另行测量。
- **批次 D(门禁补全)**:fuzz target(batch/PST/MBR)+corpus 重生成+新鲜度 CI;soak unkeyed 道+批处理组合硬错;
  external-broker 补 env/services;重 job timeout;nightly 失败自动开 issue;invariants 行号策略(删号或 lint 校验)。
- **批次 E(文档/部署)**:两个 Dockerfile 补 COPY vendor;metrics-reference 补全+去 "(unreleased)";Java
  readLinearizable;README/client-sdk/compatibility 三处措辞;release.yml publish 加固说明。

---

# 附录 — 落地实施记录（2026-09-23 深夜场，用户:"全部开工吧"）

9 个 coder 工作流并行实施;中途撞 5h 配额（5 个工作流标记 failed,但检查工作树发现其编辑大部分已落地,
仅死在自验证阶段）;集成者完成收尾、跨工作流冲突修复与全部门禁。总计 89 文件 +9130/−1342,未 commit。

## P0 落地状态（6/6,全部带回归测试）

| # | 修复 | 关键落点 |
|---|---|---|
| P0-1 | dispatch 入口 `validate_client_payload`（propose 前拒畸形/未来版本帧）+ apply 批拆分 gate 到 raw client bytes（framed/ingestor verbatim）+ ADR A-0018 | `rr-core/src/ingress.rs`(新), `dispatch.rs`, `service_adapter.rs` |
| P0-2 | 池级 request_id + `PendingKey(group_id, request_id)` demux + 撞号响亮化 | `tcp_transport.rs` + 跨组并发回归测试 |
| P0-3 | `request_type = rr.output.g{source_group}` 命名空间化,删恒等包装 | `output_runner.rs` + 单测 |
| P0-4 | fcntl(F_SETLK) + flock(LOCK_EX\|LOCK_NB) 双命名空间探测,探测错误 fail-closed | `node_reset.rs` + 持锁拒绝回归 |
| P0-5 | poll 错误臂 `if !batch.is_empty() { return Ok(batch) }` 先交付;Mock 注入 seam | `backend.rs`, `mock.rs` + 回归 ×2 |
| P0-6 | watch 重连环 + `with_start_revision` 恢复 + compaction 全量对账(diff_tables) + `WatchStreamGuard` abort-on-drop + 指标 | `rr-config-etcd/lib.rs` |

## 集成期自抓自修（跨工作流交互,swarm 结构上不可能预见）

1. **INV-7 守卫被绕过**:`MemConfigProvider::remove_node` 新行为（预剥离 replicas）使 watch 处理器
   `new_members.len() < meta.replicas.len()` 恒假 → 守卫失效,`test_node_removed_quorum_guard` 红。
   修为以 raft membership 为真相源（node.rs）。**教训:守卫的参照系必须是强一方。**
2. **output_runner 新测试前提写反**:`stop_group` 返回时 metrics 已是 Shutdown,断言 GroupGone 必败;
   改为 Parked|GroupGone 诚实断言（GroupGone 臂留给真竞态）。
3. **部署制品连锁**:新 `metrics_bind_host`/`admin_bind_host` 默认 loopback(v0.3.0 破坏窗口)会使
   k8s/helm/compose 跨 Pod 抓取失联 → 5 处制品补显式 `0.0.0.0`。

## 门禁终态

- fmt / clippy `--workspace --all-targets --features rocks -D warnings` / 全量测试
  `--features rr-storage/rocks,integration/rocks`:全绿(含 integration 84、rr-server 126、rr-raft 200+)。
- `check-invariants.sh` 70 links 绿;fuzz crate 编译绿;docs `npm run build` 绿;
  workflow YAML 全部解析通过;`./gradlew test` 31 绿。
- GitNexus 重索引(7,797 nodes / 19,637 edges);`detect_changes`:773 symbols / risk=critical
  (纯计数阈值;受影响 processes 全部为测试流,与 09-22 同形态)。

## 遗留（下轮）

- invariants.md 登记本波新测试(ingress 守卫 / TCP demux / poll 部分批 / sidecar 原子写等 → INV 映射逐条核)。
- `admin_listen_host`/`metrics_listen_host` 已落地为 `admin_bind_host`/`metrics_bind_host`(默认 loopback,
  破坏窗口内)——运营侧注意跨主机抓取需显式配置(制品已处理)。
- Java `sdkVersion` bump 留发版治理;`ConnectOptions` TLS 需 etcd-client `tls` feature(未启用,文档已注)。
- 批次 C 实测:见 `docs/perf/baselines/`(扇出矩阵)。
