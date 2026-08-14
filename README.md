# influx-sync

InfluxDB 跨正向隔离同步系统（ISFP 协议，V1.4.2）。

**功能**：安全区 I 的 InfluxDB 1.x 数据 → 正向隔离装置（TCP 映射）→ 安全区 III 的 InfluxDB 1.x，
单向、有序、At-Least-Once、断点续传同步。实测 **20 万点/s 实时零丢失**（64 核/124G，麒麟 V10）。

## 能力概览

| 能力 | 说明 |
|---|---|
| 数据获取 | 时间窗口轮询 + 订阅信号事件驱动（可选），游标 + WAL 缓冲，支持历史回填 |
| 传输 | ISFP：20B 头 + gzip(Line Protocol) + CRC32；停等协议（适配隔离装置单字节 ACK 0xff/0x00） |
| 可靠性 | WAL 落盘 + 停等 ACK + seq 去重 + 断点续传 + DLQ 毒丸隔离 + 反压保护（三级水位） |
| 中继（V1.3） | Receiver 落盘同时转发下一跳（relay 配置，复用 Sender，At-Least-Once） |
| 监控 | /metrics（Prometheus 格式：游标/延迟/积压/重试/DLQ/反压） |
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

## 目录

```
cmd/sender        发送端入口（I 区）
cmd/receiver      接收端入口（III 区）
internal/protocol ISFP 帧编解码
internal/transport TCP 停等客户端/服务端
internal/wal      分段 WAL（64MB/段 + checkpoint + DLQ）
internal/sender    Poller（查询/信号/组帧）+ Sender（停等发送）+ SignalListener
internal/receiver 帧处理（去重/写库/中继）
internal/influx   InfluxDB 1.x HTTP 客户端（schema 自适应 + ns 精度）
internal/monitor  Prometheus 指标
internal/model    Point / Line Protocol 序列化
bench/loadgen     压测工具（telemetry / -hx 两种格式）
bench/poison      毒丸注入工具
```

## 快速开始

```bash
# 构建（麒麟 V10 需静态编译，glibc 2.28）
CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o bin/sender ./cmd/sender
CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o bin/receiver ./cmd/receiver

# 测试
go test ./...
go test -race ./internal/...
```

## 文档

| 文档 | 内容 |
|---|---|
| [docs/architecture.md](docs/architecture.md) | 功能设计：架构/数据流/协议/可靠性机制 |
| [docs/configuration.md](docs/configuration.md) | 参数配置详解 + 生产环境示例 |
| [docs/deployment.md](docs/deployment.md) | 部署：安装/systemd/双实例/防火墙/升级 |
| [docs/operations.md](docs/operations.md) | 运维：日志/指标/排障/备份/已知问题 |
| [docs/relay.md](docs/relay.md) | 中继功能（V1.3） |
| [docs/audit-fixes-2026-08.md](docs/audit-fixes-2026-08.md) | 代码审计修复记录（V1.3.1 → V1.4.0） |
| [docs/AGENT.md](docs/AGENT.md) | 开发环境与上下文（设备/部署拓扑/决策/约束） |
