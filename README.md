# influx-sync

InfluxDB 跨正向隔离同步系统（ISFP 协议，V1.5.0）。

**功能**：安全区 I 的 InfluxDB 1.x 数据 → 正向隔离装置（TCP 映射）→ 安全区 III 的 InfluxDB 1.x，
单向、有序、At-Least-Once、断点续传同步。实测 **20 万点/s 实时零丢失**（64 核/124G，麒麟 V10）。

## 能力概览

| 能力 | 说明 |
|---|---|
| 数据获取 | 时间窗口轮询（正确性基座）+ 订阅 fast-path 透传（V1.5，实时性加速，游标追平自动启用）+ 订阅信号（不丢弃、延迟触发），游标 + WAL 缓冲，支持历史回填 |
| 传输 | ISFP：20B 头 + gzip(Line Protocol) + CRC32；停等协议（适配隔离装置单字节 ACK）；滑窗（实验项，需装置验证） |
| 可靠性 | WAL group commit + 停等 ACK + last_seq 连续前缀去重 + 断点续传 + DLQ 毒丸隔离 + 中继 DLQ + 反压三级水位 + WAL 撕裂尾自恢复 |
| 实时性（V1.5） | fast-path：订阅推送直接透传，端到端延迟 watermark+~2.2s → flush 间隔+传输（<1s）；快路径一切退化方向都是重复/回退轮询，零丢失不变 |
| 性能（V1.4） | 组帧 60x CPU（544ns/点）、边界去重零全窗口 map、checkpoint 节流、并发写库+按序 ACK、schema single-flight |
| 中继（V1.3） | Receiver 落盘同时转发下一跳（relay 配置，复用 Sender，At-Least-Once） |
| 监控 | /metrics（Prometheus：游标/端到端延迟/积压/重试/在途帧/DLQ/反压） |
| 带宽 | gzip 约 19x：5 万点/s 实测 2.5Mbps（对比明文方案 47.8Mbps） |

## 版本记录

| 版本 | 内容 |
|---|---|
| v1.0 | 基线：窗口轮询 + 停等发送 + WAL + seq 去重 + DLQ + 反压 |
| v1.1 | Poller 多窗口并行查询 + 并行组帧（`poller_parallel`） |
| v1.2 | 订阅信号事件驱动（`signal_listen`，仅信号不解析） |
| v1.2.1 | watermark 解锁自唤醒（消除 ticker 闲置等待） |
| v1.2.2 | 解压上限 16MB 独立于压缩帧 1MB（大帧毒丸根治） |
| v1.2.3 | 并行查询段边界 +1ns 重叠 + 归并去重（高压漏行防御） |
| v1.3 | Receiver 中继：落盘同时转发下一跳 |
| v1.3.1 | 代码审计修复：DLQ 偏移/seq 跳跃死锁/拆批卡死 + NaN/转义/精度等 13 项 |
| v1.4.0 | 性能审计修复：组帧重构（60x CPU）+ 边界去重 + checkpoint 节流 + group commit + WriteRaw + 传输调优 + single-flight schema + Receiver 流水线按序 ACK + 滑窗（实验项） + P0 修复（撕裂尾恢复/中继 DLQ/LRU 吞重试）+ e2e 延迟指标 |
| v1.4.1 | 滑窗缺陷复审修复（N1-N5）：陈旧 ACK 错位提交（P0，连接级复位） + seqTracker 恒开 + 中段损坏跳单帧重同步 + 溢出不跳越 + schema 降级负缓存 + 6 个小项 |
| v1.4.2 | 复审修复（N6-N8）：永久缺口发送方权威闭合（去告警刷屏/恢复去重） + seq 从 1 开始文档化 + schema 降级复用历史成功条目（防类型冲突毒丸） |
| v1.4.3 | 收尾加固：inflightSeq 引用计数（双保险不低估在途） + gapWarned 闭合/时间窗复位（Error 级可观测性不丢失） |
| v1.5.0 | A4 订阅 fast-path 透传：推送批直进 WAL（同 gzip 管线），游标追平自动启用（auto/on/off 三态 + 迟滞），秒级分区去重集抑制轮询重复转发，WAL 并发追加 API，订阅改造成 ops 步骤（见 docs/a4-fast-path.md） |

## 目录

```
cmd/sender        发送端入口（I 区）
cmd/receiver      接收端入口（III 区）
internal/protocol ISFP 帧编解码
internal/transport TCP 停等客户端 / 流水线服务端（并发写库 + 按序 ACK）
internal/wal      分段 WAL（group commit + checkpoint 节流 + 撕裂尾恢复）
internal/sender   Poller（查询/信号/组帧/去重过滤）+ Sender（停等/滑窗发送）+ SignalListener + FastPath（A4）
internal/receiver 帧处理（连续前缀去重/缺口闭合/写库/中继）
internal/influx   InfluxDB 1.x HTTP 客户端（schema 自适应 + ns 精度）
internal/monitor  Prometheus 指标
internal/model    Point / Line Protocol 序列化与解析（键序缓存 + strconv + LP 行解析）
bench/loadgen     压测工具（telemetry / -hx 两种格式）
bench/poison      毒丸注入工具
```

## 快速开始

```bash
# 构建（麒麟 V10 需静态编译，glibc 2.28；先 commit+tag 再构建，溯源戳才干净）
CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o bin/sender ./cmd/sender
CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o bin/receiver ./cmd/receiver

# 测试
go test ./...
go test -race ./internal/...
```

## 文档

| 文档 | 内容 |
|---|---|
| [docs/architecture.md](docs/architecture.md) | 程序设计：架构/模块/数据结构/并发模型/核心机制/可靠性论证 |
| [docs/protocol.md](docs/protocol.md) | ISFP 线协议规范：帧格式/ACK 语义/seq 规则/连接生命周期/兼容性 |
| [docs/configuration.md](docs/configuration.md) | 参数配置详解 + 默认值总表 + 生产环境示例 |
| [docs/deployment.md](docs/deployment.md) | 部署：安装包/systemd/升级回滚/防火墙/上架清单 |
| [docs/operations.md](docs/operations.md) | 运维：日志/指标/排障/备份/例行检查 |
| [docs/relay.md](docs/relay.md) | 中继功能（V1.3） |
| [docs/a4-fast-path.md](docs/a4-fast-path.md) | A4 订阅 fast-path 透传设计（V1.5：衔接/门控/零丢失论证/部署步骤） |
| [docs/pipeline-validation.md](docs/pipeline-validation.md) | 滑窗隔离装置兼容性验证方案（未验证前保持默认关闭） |
| [docs/audit-fixes-2026-08.md](docs/audit-fixes-2026-08.md) | 三轮审计修复记录（V1.3.1 → V1.4.3） |
| [docs/AGENT.md](docs/AGENT.md) | 开发环境与上下文（设备/部署拓扑/决策/约束） |
