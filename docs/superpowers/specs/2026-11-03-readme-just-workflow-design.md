# README 深度手册与 just 工作流设计

- 日期：2026-11-03
- 状态：已与需求方确认文档结构，待实施
- 范围：`README.md`、`docs/docs/guide/`、根目录 `justfile`、Java SDK 命令入口

## 目标

让新用户、开发者和运维者分别能快速找到正确的下一步：

1. README 保留项目价值、架构和最小成功路径。
2. 深度手册覆盖真实三节点、同构 Genesis、旧集群 adoption、Rocks 持久化、故障转移、Admin 运维、真实 Etcd/Helm 验收、Java SDK 和安全/观测边界。
3. `just` 统一常用命令组合，减少 README 中的长命令和易错步骤。
4. Java SDK 的构建、单测、集成测试、Javadoc、本地发布纳入同一命令面。

## 方案取舍

### 方案 A：只扩充 README

优点是单文件可见；缺点是 README 会膨胀，复杂场景与快速入口混在一起，难以维护。

### 方案 B：README 入口 + 独立 playbook + justfile（采用）

README 保持轻量；playbook 能系统展开；justfile 提供可执行入口。代价是需要保证三者同步，实施时用冒烟命令验证。

### 方案 C：只依赖 Docusaurus 文档站

对终端开发不友好，且不能把命令组合变成可执行入口。

## 文档结构

- `README.md`：新增 Operations Playbook 和 just 快速入口，不承载完整手册。
- `docs/docs/guide/playbook.md`：按任务组织，按“前置条件 → 命令 → 验证 → 失败处理”展开。
- 文档侧栏如存在手工导航则加入 playbook；否则只链接。

## just 命令边界

命令分组：

- 常规：`just doctor`、`just build`、`just test`、`just fmt`、`just check`。
- KV 手工集群：`just kv-cluster-up/down/status/put`。
- Managed Genesis：`just etcd-up/down`、`just genesis-up/down/status`，命令使用可覆盖的地址和 cluster name。
- 外部验收：`just test-etcd`、`just helm-test`。
- 文档：`just docs-build`、`just docs-serve`。
- Java：`just java-build`、`just java-test`、`just java-integration-test`、`just java-javadoc`、`just java-publish-local`。

所有长时间运行进程使用 `target/just/` 下的 PID/log；down/status 命令不得删除用户数据目录。外部镜像、Docker socket、Helm、Node 和 Java 缺失时必须 fail fast 并给出安装/配置提示。

## 验收

- README 新增入口和示例与 playbook/justfile 一致。
- `just --list` 能展示分组命令。
- `just doctor` 在当前环境能识别 cargo、java、node、helm、docker/colima。
- 无 Docker socket 时 `just test-etcd` fail fast，不产生后台残留。
- Java wrapper 本地改动不属于本次内容；只调用它，不修改。
- Markdown 行宽检查与 `cargo test` 保持通过。
