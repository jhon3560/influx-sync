# 运维手册

> V1.4.3。设计见 [architecture.md](architecture.md)；线协议见 [protocol.md](protocol.md)。

## 1. 日志

| 位置 | 内容 |
|---|---|
| /opt/influx-sync/logs/sender-*.log | sender 全流程（内置轮转 100MB×10） |
| /opt/influx-sync/logs/receiver-*.log | receiver 全流程 |
| journald（influxdb） | 目标库服务（生产已配置落文件 /var/log/influxdb/ + logrotate） |

### 1.1 关键日志行（正常流）

| 日志 | 含义 |
|---|---|
| `poll window start=... end=... points=N` | 每轮查询窗口 |
| `ack success seq=N` / `pipeline ack success seq=N` | 帧确认（停等/滑窗） |
| `frame written seq=N lines=N` | 落库成功 |
| `sender started` / `receiver started` | 进程启动完成 |
| `receiver restored last_seq seq=N` | last_seq 恢复 |

### 1.2 关键日志行（事件，需要关注）

| 日志 | 级别 | 含义与处理 |
|---|---|---|
| `permanent seq gap closed via sender wal head` | Info | 重启后缺口正常闭合（N6），无需处理；高频出现=连接频繁重建 |
| `seq jump` / `seq jump too large...` | Warn/Error | 首条告警（5 分钟内同缺口只 Debug）：确认是否为重启/WAL 重置所致 |
| `poison packet isolated to DLQ` | Error | 毒丸隔离（含 path/http_status）：查 DLQ 文件定位字段/表结构问题 |
| `relay wal append failed, saving to relay dlq` | Error | 中继 WAL 失败已转存：查转发 WAL 磁盘/权限 |
| `wal: truncated torn tail record` | Error | WAL 撕裂尾自恢复（游标回退重查补数据）；频繁出现=异常掉电 |
| `wal: skipping corrupt record in segment` | Warn | WAL 中段坏帧跳帧重同步（丢一帧）：磁盘位反转，检查存储健康 |
| `point too large, skipped (would deadlock cursor)` | Error | 单点超 16MB 上限被跳过（point_skip_total 计数） |
| `frame too large, splitting batch` | Warn | 帧压缩超 1MB 自动拆批（正常自适应） |
| `backpressure: poller paused/degraded/resumed` | Warn/Info | 反压状态切换 |
| `influx: schema discovery failed...` | Warn | schema 发现失败：复用旧 schema 或类型推断兜底（30s 重试） |
| `retry frame seq=N` / `frame keep retrying` | Info/Error | 重试进度；后者=连续 0x00 达到阈值（仍不丢弃，排查链路/目标库） |

## 2. 监控指标（/metrics，Prometheus 格式）

| 指标 | 类型 | 含义 |
|---|---|---|
| sync_cursor_ns | gauge | 逻辑游标（已进入 WAL 的最大数据时间） |
| sync_delay_seconds | gauge | now-cursor（正常=watermark+处理时间） |
| sync_e2e_delay_seconds | gauge | **端到端延迟**：now-目标库最后写入点时间（0=未知；A5） |
| wal_pending / wal_size_bytes | gauge | 积压帧数/字节（持续增长=对端断连或瓶颈） |
| send_total / ack_success / ack_fail | counter | 发送与 ACK 计数 |
| retry_total | counter | 重试总数（>0 持续=链路不稳） |
| heartbeat_total | counter | 心跳计数 |
| poll_skip | counter | 反压跳过轮询数 |
| point_skip_total | counter | 超限跳过的单点（病理数据） |
| dlq_total / influx_sync_dlq_generated_total | counter | 毒丸隔离计数（非零需查 DLQ 文件） |
| influx_sync_poison_packet_count | counter | 毒丸帧数 |
| relay_dlq_total | counter | 中继转发失败转存计数 |
| recv_total / write_ok / write_fail / dup_total | counter | 接收端收帧/落库/去重 |
| recv_inflight | gauge | **在途处理帧数**（正常 ≤ max_inflight×连接数） |
| influx_sync_wal_disk_usage_ratio | gauge | WAL 盘占用率（反压依据，0~1） |
| influx_sync_backpressure_status | gauge | 0 正常/1 降速/2 挂起 |
| influx_sync_poller_paused_seconds_total | counter | Poller 挂起累计秒数 |

**告警阈值建议**：sync_e2e_delay_seconds > watermark+30s；wal_pending 持续增长；
retry_total 分钟级增量 >0；dlq_total/relay_dlq_total/point_skip_total 任何增量；
recv_inflight 持续 > max_inflight×4。

## 3. 常用运维操作

```bash
systemctl status influx-sync-sender-174
systemctl restart influx-sync-sender-174
curl -s http://127.0.0.1:28080/metrics | grep -E 'sync_delay|sync_e2e|wal_pending|retry'

# 数据验证（务必用 influx CLI；高压下 curl count 偶发返回错误值如 1970）
influx -host 127.0.0.1 -port 11911 -username root -password 'SECRET_REDACTED' \
  -database HXScada -execute 'SELECT count(*) FROM hisdb'

# 链路延迟采样：源库 SELECT last(*) 与目标库同查询 → 时间差
# 二进制溯源（版本核对）
go version -m /opt/influx-sync/bin/sender | grep vcs
```

171 机房运维脚本集：`/home/hexin/ops/`（01_health 总检 / 03_freshness 新鲜度 /
04_writerate 速率 / 05_link 链路 / 06_logs / 07_disk / 08_slowq / 10_sample /
90_restart；`./99_help.sh` 清单）。首次运行提示输入 sudo 密码。

## 4. 故障排查

| 现象 | 排查 |
|---|---|
| 目标库无新数据 | ①源库有无写入 ②sender 日志 poll/ack ③wal_pending 是否增长 ④receiver 日志/进程 ⑤防火墙/虚地址连通 |
| wal_pending 持续增长 | 对端不可达（receiver 停机/隔离装置/网络）→ 恢复后自动追平 |
| sync_delay / sync_e2e 持续大 | ①写入量 > 能力 ②watermark 大 ③源库查询慢（查 slow query）④目标库写慢（查 recv_inflight 是否打满） |
| 毒丸计数增长 | 看 receiver 日志 http_status + DLQ 文件内容（400=字段/表结构问题；类型冲突常见于 schema 降级后恢复，V1.4.2+ 已复用旧 schema 预防） |
| relay_dlq_total 增长 | 转发 WAL 盘满/权限 → 处理后重放 relay_dlq 目录即可 |
| 重启后重复数据 | 幂等覆盖，count 不重复；无需处理 |
| 重启后链路不动 | 确认 last_seq 与 WAL 完好（数据目录勿删）；首帧缺口自动闭合（`gap closed` 日志） |
| seq jump 告警刷屏 | V1.4.2+ 已消除（缺口闭合+节流）；仍刷屏=缺口无法闭合（多 sender 误配？）查 recv_inflight 与连接数 |
| 日志 `torn tail` / `corrupt record` | 掉电/存储异常：确认数据追平（游标重查），检查磁盘与供电 |
| 滑窗模式 `pipeline send/ack wait failed` 频繁 | 未做装置兼容性验证就开启？回退 pipeline_window=1；已验证则查网络质量 |

## 5. 备份与恢复

- **备份**：conf/*.yaml（配置）、data/（WAL+last_seq+DLQ，随服务停机快照）；
  InfluxDB 数据由 InfluxDB 自身备份工具负责（influxd backup）
- **恢复**：停服务 → 还原 conf 与 data → 起服务；WAL 未确认帧自动重发追平
- **清库重建注意**：DROP DATABASE 会连带删除该库的 SUBSCRIPTION，重建库后需重新
  `CREATE SUBSCRIPTION`（signal_listen 依赖它）
- **禁止操作**：删除/清空 WAL 目录（=丢数据源）；多实例共用数据目录；
  手工编辑 checkpoint/last_seq 文件

## 6. 已知问题与边界

| 项 | 说明 |
|---|---|
| InfluxDB 1.12.4 压力期查询异常 | 高压写入期间 curl count 偶发 1970/database not found/慢查询>5s；压力过后恢复，数据无损；验证统一用 CLI，监控查询加重试 |
| 吞吐上限 | 实测 20 万点/s 实时（环境相关）；超限表现=延迟增大（不丢数据） |
| batch_points 压缩超限 | 压缩后 >1MB 自动减半拆批；batch=1 仍超限跳过该点（point_skip_total） |
| DLQ 容量 | sender 侧 WAL DLQ 1GB 上限，超限退回 0x00 重试；receiver 侧毒丸 DLQ 无上限（建议月度清理） |
| 中继 DLQ 二级失败 | 中继 DLQ 落盘本身失败（磁盘满）只能记日志，该帧无法恢复（一级防护失效场景） |
| 单实例数据目录 | WAL/last_seq/DLQ 目录不可多实例共用（flock 保护） |
| schema 降级 | 元查询失败时复用旧 schema / 类型推断兜底，30s 重试；显式 tag_columns 可绕开 |
| 滑窗 | pipeline_window>1 需先过装置兼容性验证（pipeline-validation.md）；默认关闭 |

## 7. 例行检查建议

- 每日：/metrics 扫一遍（delay/e2e_delay/pending/retry/dlq/inflight）
- 每周：磁盘（/opt/influx-sync/data、InfluxDB 数据目录）、日志轮转正常
- 每月：DLQ 目录清理（分析后）、系统重启演练（验证断点续传）
- 每季度：WAL torn tail / corrupt record 日志回顾（存储健康信号）
