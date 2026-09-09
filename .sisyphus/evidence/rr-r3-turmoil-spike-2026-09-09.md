# R-3 spike 报告:turmoil 确定性仿真适配评估

> 工作项:`.sisyphus/plans/rr-reliability-program.md` R-3(时间盒 2 人日)。
> 日期:2026-09-09 · HEAD:`6eb03b7` · 证据测试:`tests/integration/src/turmoil_spike.rs`

## TL;DR

**两个承重技术前提全部通过**(tonic-over-turmoil ping-pong 实证),
但完整试点的剩余工作量(per-host harness 重构 + server 监听 seam)超出
2 人日时间盒。**按计划决策规则转入替代方案**:FaultInjector schedule
记录/回放(本 session 落地,E3b/E5 接入)。turmoil 路径保留为重评选项,
重评触发条件见末节。

## 前提验证(计划指定,spike 第一天必须回答)

| 前提 | 结论 | 证据 |
|---|---|---|
| tonic 0.14 是否暴露 `connect_with_connector`(或等价 service-based connect)? | ✅ 是。`tonic 0.14.5` `Endpoint::connect_with_connector`(endpoint.rs:502)+ `connect_with_connector_lazy`(:527)均在,约束 `C: Service<Uri>`,`C::Response: hyper::rt::Read + Write + Send + Unpin` | registry 源码 + spike 测试 |
| turmoil 能否承载 tonic gRPC 双向流量? | ✅ 能。client 侧:`TurmoilConnector`(Service<Uri>)拨 `turmoil::net::TcpStream` 包 `hyper_util::rt::TokioIo` 后交给 `connect_with_connector`;server 侧:`serve_with_incoming` 接 `turmoil::net::TcpListener`,需 ~45 行 `SimIo` newtype(tonic `Connected` 无 blanket impl;`TokioIo` 方向相反,不能用)。3 次 echo 往返全对,模拟时钟下墙钟 0.00s | `turmoil_spike::tonic_echo_over_turmoil_sim`(绿) |
| turmoil crate 可获取? | ✅ 0.7.2(sparse index 可达,与本仓 tokio 1.x / hyper 1.x 栈兼容) | `tests/integration/Cargo.toml` dev-dep |

## 完整试点的剩余工作量实测(基于 spike 实测 + 代码核实)

1. **client 侧 connector seam**(rr-raft):`ChannelPool::get_or_connect`
   经 `Endpoint::connect()` 直连;改为可注入 `StreamConnector`(
   `connect(node_id, addr) -> io::Result<Box<dyn AsyncRead+Write>>`),
   生产实现走 TcpStream(+TLS),sim 实现走 turmoil stream + TokioIo。
   ~120 行 + 调用点适配。风险低。
2. **server 侧 listener seam**(rr-server):`RrNode::start` 走
   `serve_with_shutdown(addr)`(node.rs:551,admin :631),无 listener 注入点;
   需加 `serve_with_incoming` 路径 + `SimIo` 包装(spike 已验证模式)。
   触及生产 serving 代码与 WS-5 安全 gate 路径。风险中。
3. **per-host ClusterBuilder**(tests/integration):`sim.host(name, closure)`
   要求每节点一个 host 闭包,节点内全部 tokio::time/spawn 进入模拟时钟域;
   现有单 runtime 多 RrNode 结构需重构。Arc 共享 config provider 跨 host 可用
   (同进程)。**注意**:turmoil 的故障模型(partition/latency/loss)与现有
   `FaultAwareChannelPool` 是两套——chaos 测试需改写为 turmoil 故障 API,
   而非复用 FaultInjector。风险中。
4. **通过判据成本**:判据 (b)(同 seed 逐字节一致)需 openraft 内部无
   非确定性源(HashMap 迭代序、墙钟泄漏);(c)(10× 加速)需 chaos 测试的
   sleep 全部进模拟时钟。E2a→E3b→E5 每个移植估 0.5~1 人日。

**合计:试点本身 ~1.5 人日(略超时间盒边缘),达到"判据全绿 + E3b/E5 移植"
合计 3~4 人日,显著超过 2 人日 spike 盒。**

## 决策

按 R-3 计划的决策规则("若成本超预期,落地确定性 fault schedule 记录/回放
替代方案"):本 session 落地替代方案——`FaultInjector` schedule 模式
(显式 seed 生成事件计划 + 全程记录实际生效事件 JSONL + `RR_FAULT_REPLAY`
精确重放),E3b/E5 接入。该方案拿到"可回放 + 无随机 flaky"的核心价值,
不动生产 serving 代码,且其 fault-log schema 与 R-4/R-8 的
fault-log.jsonl 同构(同一回放设施族)。

## turmoil 重评触发条件

- 某 chaos 场景需要模拟时钟压缩(如 24h 内核对决 soak 的 in-process 版);
- 或出现 wall-clock flaky 事故且 replay 设施不足以定位;
- 或 openraft 升级后其内部计时可注入。
触发时以本报告的已验证前提为起点,直接进入第 1~3 步重构,无需再 spike。

## 本报告的可执行工件

- `tests/integration/src/turmoil_spike.rs`(`tonic_echo_over_turmoil_sim`,
  随默认 test job 运行——若 tonic/turmoil 任一升级破坏该路径,CI 即知,
  重评信号自动浮现);
- `tests/integration/Cargo.toml` dev-deps:turmoil 0.7、async-stream、
  hyper-util、tower-service、http、prost、tonic-prost。
