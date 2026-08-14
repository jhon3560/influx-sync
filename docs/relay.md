# 中继功能（V1.3）

## 1. 用途

三机场景：A（源）→ B（目的）→ C（目的），A 与 C 之间网络不通（B 有隔离/网关）。
B 的 receiver 在**落盘的同时**把数据转发给 C，C 直接落盘。

```
A ──(ISFP)──> B[receiver] ──┬── 写 B 库（现有逻辑）
                            └── 转发 WAL ──> 中继 Sender ──> C[receiver] 写 C 库
```

## 2. 配置（Receiver 端，可选）

```yaml
# /opt/influx-sync/conf/receiver.yaml
relay:
  addr: "198.51.100.172:28103"   # 下一跳 Receiver 地址；空 = 不启用中继
  wal_dir: "/opt/influx-sync/data/relay-wal"  # 转发缓冲 WAL（重启不丢，必须配置）
  timeout: 10s                     # 转发读写超时
```

- 不配置 `relay` 段 = 原有行为，完全向后兼容。
- 下一跳（C）是标准 receiver，**无需任何改动**。

## 3. 设计要点

| 项 | 说明 |
|---|---|
| 转发内容 | 写库成功后的原始 Line Protocol（独立 seq 重新编号） |
| 可靠性 | 转发走**独立 WAL**，复用标准 Sender（停等 ACK / 断点续传 / At-Least-Once） |
| ACK 语义 | B 写库成功即 ACK 上游（对 A 快）；转发异步，失败由转发 WAL 缓冲重试 |
| 转发 append 失败 | V1.4/C2：raw lines 落中继专用 DLQ（`relay.dlq_dir`，默认 `<wal_dir>/../relay_dlq`）+ `relay_dlq_total` 指标——**不再存在静默丢失路径**。注意降级边界：中继 DLQ **落盘本身**再失败（磁盘满等）仍只能记 Error 日志，该帧无法恢复（一级防护：转发 WAL 的 fsync 失败场景极小） |
| 毒丸 | 写库失败进 DLQ 的帧**不转发**（避免下游同样落 DLQ） |
| 吞吐 | B 同时承受写库+转发（带宽约为单链路 2 倍） |

## 4. 验证结果（2026-08-11 实测）

### 4.1 正常中继

拓扑：103 sender → 171 receiver-174（B，relay → 127.0.0.1:28103）→ 171 receiver-c（C 模拟）

| 指标 | 结果 |
|---|---|
| 5万点/s × 30s 推送 | 1,500,000 点 |
| B 库（HXScada.telemetry） | 1,500,000（零丢失） |
| C 库（HXScadaC.telemetry） | 1,500,000（零丢失） |
| 转发 WAL 积压 | 0（实时转发完成） |
| relay wal append 失败 | 0 |

### 4.2 断连恢复（C 宕机 → 恢复）

| 阶段 | B 库 | C 库 | 转发 WAL |
|---|---|---|---|
| 压测中停 C（5万点/s×60s 的第 41s 起） | 完整（3,000,000） | 1,517,922（缺 148万） | 积压 11MB |
| 恢复 C 后自动补发 | 3,000,000 | **3,000,000（追平）** | **清空** |

验证结论：
- B 落盘不受 C 断连影响（解耦）；
- 断连数据全量缓冲于转发 WAL，无丢失；
- 恢复后自动补发，C 与 B 精确一致（At-Least-Once，零人工干预）。

## 5. 部署示例（B 机）

```bash
# B 机 receiver 配置 relay 后重启
install -m 0640 conf/receiver.yaml /opt/influx-sync/conf/receiver.yaml
systemctl restart influx-sync-receiver

# C 机：标准 receiver（独立配置/端口/库）
systemctl start influx-sync-receiver
```

## 6. 注意

- C 的可用性依赖 B：B 宕机期间 C 断供，恢复后转发 WAL 补发（数据不丢）
- 中继链路总带宽 = A→B + B→C 两份（如 20万点/s 需约 2×194Mbps）
- B 需能出站连接 C（隔离装置方向需放行 B→C）
