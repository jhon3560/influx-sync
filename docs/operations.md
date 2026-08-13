# 运维手册

## 1. 日志

| 位置 | 内容 |
|---|---|
| /opt/influx-sync/logs/sender-*.log | sender 全流程（内置轮转 100MB×10） |
| /opt/influx-sync/logs/receiver-*.log | receiver 全流程 |
| journald（influxdb） | 目标库服务（生产已配置落文件 /var/log/influxdb/ + logrotate） |

关键日志行：
- `poll window start=... end=... points=N` —— 每轮查询
- `ack success seq=N` / `retry frame seq=N` —— 发送进度/重试
- `poison packet isolated to DLQ` —— 毒丸隔离（含 path/http_status）
- `frame written seq=N lines=N` —— 落库成功
- `backpressure: poller paused/degraded` —— 反压触发

## 2. 监控指标（/metrics，Prometheus 格式）

| 指标 | 含义 |
|---|---|
| sync_delay_seconds | now-cursor（端到端滞后，正常=watermark+处理时间） |
| wal_pending / wal_size_bytes | 积压帧数/字节（持续增长=对端断连或处理瓶颈） |
| send_total / ack_success / ack_fail | 发送与 ACK 计数 |
| retry_total | 重试总数（>0 持续=链路不稳） |
| dlq_total / influx_sync_poison_packet_count | 毒丸隔离计数（非零需查 DLQ 文件） |
| recv_total / write_ok / write_fail / dup_total | 接收端落库与去重 |
| influx_sync_wal_disk_usage_ratio | WAL 盘占用率（反压依据） |
| influx_sync_backpressure_status | 0 正常/1 降速/2 挂起 |

## 3. 常用运维操作

```bash
systemctl status influx-sync-sender-174
systemctl restart influx-sync-sender-174
curl -s http://127.0.0.1:28080/metrics

# 数据验证（务必用 influx CLI；高压下 curl count 偶发返回错误值如 1970）
influx -host 127.0.0.1 -port 11911 -username root -password 'SECRET_REDACTED' \
  -database HXScada -execute 'SELECT count(*) FROM hisdb'

# 链路延迟采样
# 源库: SELECT last(*) FROM hisdb  vs  目标库同查询 → 时间差
```

171 机房运维脚本集：`/home/hexin/ops/`（01_health 总检 / 03_freshness 新鲜度 /
04_writerate 速率 / 05_link 链路 / 06_logs / 07_disk / 08_slowq / 10_sample /
90_restart；`./99_help.sh` 清单）。首次运行提示输入 sudo 密码。

## 4. 故障排查

| 现象 | 排查 |
|---|---|
| 目标库无新数据 | ①源库有无写入 ②sender 日志 poll/ack ③wal_pending 是否增长 ④receiver 日志/进程 ⑤防火墙/虚地址连通 |
| wal_pending 持续增长 | 对端不可达（receiver 停机/隔离装置/网络）→ 恢复后自动追平 |
| sync_delay_seconds 持续大 | ①写入量 > 能力（20 万点/s 上限）②watermark 大 ③源库查询慢（查 slow query） |
| 毒丸计数增长 | 看 receiver 日志 http_status + DLQ 文件内容（400=字段/表结构问题，查目标库 schema） |
| 重启后重复数据 | 幂等覆盖，count 不重复；无需处理 |
| 重启后链路不动 | 确认 last_seq 与 WAL 完好（数据目录勿删）；seq 跳跃会接受处理（V1.3.1 不死锁） |

## 5. 备份与恢复

- **备份**：conf/*.yaml（配置）、data/（WAL+last_seq+DLQ，随服务停机快照）；
  InfluxDB 数据由 InfluxDB 自身备份工具负责（influxd backup）
- **恢复**：停服务 → 还原 conf 与 data → 起服务；WAL 未确认帧自动重发追平
- **清库重建注意**：DROP DATABASE 会连带删除该库的 SUBSCRIPTION，重建库后需重新
  `CREATE SUBSCRIPTION`（signal_listen 依赖它）

## 6. 已知问题与边界

| 项 | 说明 |
|---|---|
| InfluxDB 1.12.4 压力期查询异常 | 高压写入期间 curl count 偶发 1970/database not found/慢查询>5s；压力过后恢复，数据无损；验证统一用 CLI，监控查询加重试 |
| 吞吐上限 | 实测 20 万点/s 实时（环境相关）；超限表现=延迟增大（不丢数据） |
| batch_points 压缩超限 | 压缩后 >1MB 自动减半拆批（V1.3.1），不会卡死；建议按实际数据压缩率选值 |
| DLQ 容量 | 1GB 上限，超限时退回 0x00 重试（不丢主链路，但毒丸会反复重试） |
| 单实例数据目录 | WAL/last_seq/DLQ 目录不可多实例共用（flock 会阻止，属于保护行为） |

## 7. 例行检查建议

- 每日：/metrics 扫一遍（delay/pending/retry/dlq）
- 每周：磁盘（/opt/influx-sync/data、InfluxDB 数据目录）、日志轮转正常
- 每月：DLQ 目录清理（分析后）、系统重启演练（验证断点续传）
