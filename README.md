# influx-sync

InfluxDB 跨正向隔离同步系统（ISFP 协议）。

**功能**：安全区 I 的 InfluxDB 1.x 数据 → 正向隔离装置（TCP 映射）→ 安全区 III 的 InfluxDB 1.x，单向、有序、At-Least-Once 同步。

## 版本记录

| 版本 | 内容 |
|---|---|
| v1.0 | 原始基线：窗口轮询 + 停等发送 + WAL + seq 去重 + DLQ + 反压 |
| v1.1 | Poller 多窗口并行查询 + 并行组帧（gzip 移出 WAL 锁），`sync.poller_parallel` 配置 |
| v1.2 | 订阅信号事件驱动（`sync.signal_listen`），SignalListener 仅作触发信号，不解析内容 |
| v1.2.1 | watermark 解锁自唤醒（消除 ticker 闲置等待） |
| v1.2.2 | 协议层修复：解压上限 16MB 独立于压缩帧 1MB（大帧毒丸根治） |
| v1.2.3 | 并行查询段边界 +1ns 重叠 + 归并去重（高压漏行防御） |
| v1.3 | **Receiver 中继**：落盘同时转发下一跳（`relay` 配置），复用标准 Sender 保证 At-Least-Once |

## 组件

- `cmd/sender`：I 区发送端（轮询/信号查询 → WAL → 停等发送）
- `cmd/receiver`：III 区接收端（接收 → 去重 → 写库 → ACK；可配置中继转发）
- `bench/cmd/loadgen`：压测工具（支持 telemetry/hx 两种格式）
- `bench/cmd/poison`：毒丸注入工具

## 快速开始

```bash
# 构建（麒麟 V10 需静态编译）
CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o bin/sender ./cmd/sender
CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o bin/receiver ./cmd/receiver
```

配置与部署详见 `docs/`。

## 文档

- `docs/relay.md`：中继功能说明（V1.3）
- `docs/部署文档.md`、`docs/配置修改指南.md`：部署与配置（完整版见交付介质）
