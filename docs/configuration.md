# 参数配置详解

配置文件为 YAML。环境变量可覆盖部分参数（`INFLUXSYNC_` 前缀，见 §4）。

## 1. Sender 配置（cmd/sender，-config 指定）

```yaml
source:
  url: http://192.0.2.174:11911   # 源 InfluxDB（必填）
  database: hisdb                    # 源库名（必填）
  username: root                     # 认证（InfluxDB 开认证时必填）
  password: "SECRET_REDACTED"
  timeout: 10s                       # HTTP 超时（查询/写）

sync:
  interval: 1s          # 轮询周期（兜底 ticker；信号模式下也保底）
  window: 5s            # 查询窗口大小
  watermark: 2s         # 水位延迟：窗口终点 = now-watermark
                        # 实测延迟 = watermark + ~2.2s 处理
                        # 越小越实时；若源端写入延迟>watermark 有漏数风险（建议 ≥2s）
  max_window: 30s       # 单轮查询窗口上限（防时间跳变/积压一次拉爆）
  batch_points: 30000   # 单帧点数（原始 30000 点≈2.4MB；压缩后须 ≤1MB）
                        # 若压缩后超限：自动减半拆批；单点仍超限则跳过该点并计数
  query_limit: 500000   # 单次查询 LIMIT（越大分页越少、扫描开销越低）
  poller_parallel: 4    # 并行查询/组帧 worker 数（0/1=串行）
  signal_listen: ":18098"  # 订阅信号监听（源库 SUBSCRIPTION 推送目标）；空=纯轮询
  signal_min_interval: 200ms  # 信号最小查询间隔（V1.4：忙时信号延迟触发而非丢弃）
  backfill: 0s          # 首次启动回填时长（游标=now-watermark-backfill）
  tag_columns: []       # 显式 tag 列（空=自动 SHOW TAG KEYS 发现）
  measurements: []      # 同步的 measurement（空=全部 /.*/）

wal:
  path: /opt/influx-sync/data/wal-174   # WAL 目录（必填；勿与其他实例共用）
  segment_size: 64MB    # 段大小

tcp:
  addr: 192.0.2.131:28101  # receiver 地址（必填；经隔离装置映射的虚地址）
  timeout: 10s          # 读写超时
  dial_timeout: 10s     # 连接超时

sender:
  max_retry: 10         # 连续失败告警阈值（不丢弃，见 architecture §4.3）
  backoff_base: 1s      # 重试退避基数（1s→2s→4s…）
  backoff_max: 60s      # 退避封顶
  heartbeat_interval: 30s  # 空闲心跳（维持隔离通道活性；心跳 seq 固定 0，不占数据 seq）
  pipeline_window: 1    # 滑窗大小（A1 实验项）：1=停等（默认）。>1 时同连接多帧在途
                        # （吞吐 ≈ W×batch/RTT）；开启前必须先与隔离装置确认允许
                        # 同连接多请求在途，否则链路不通

monitor:
  addr: :28080          # /metrics 端口（每实例独立）
  username: ""          # 监控认证（空=不启用）
  password: ""

log:
  level: info           # debug/info/warn/error
  file: /opt/influx-sync/logs/sender-174.log
  max_mb: 100           # 轮转大小（内置轮转）
  max_backups: 10       # 保留份数
```

## 2. Receiver 配置（cmd/receiver）

```yaml
target:
  url: http://127.0.0.1:11911   # 目标 InfluxDB（必填；本机）
  database: HXScada             # 目标库（必填）
  username: root
  password: "SECRET_REDACTED"
  timeout: 30s                  # HTTP 超时

tcp:
  listen: :28101                # 监听地址（必填；0.0.0.0=所有网卡，被动接收）
  read_timeout: 60s             # 单帧读取超时（> sender 心跳间隔 30s）
  max_inflight: 8               # 每连接在途帧上限（A2 并发写库+按序 ACK 流水线）
  max_conns: 0                  # 最大并发连接（0=不限制）

dedup:
  cap: 10000                    # LRU 去重容量（in-flight 窗口）
  last_seq_file: /opt/influx-sync/data/last_seq-174  # 已处理最大 seq 持久化

dlq:
  dir: /opt/influx-sync/data/dlq-174  # 毒丸死信目录（空=禁用 DLQ，退回重试）

relay:                          # 中继（V1.3，可选；不配置=无中继）
  addr: "198.51.100.x:28103"   # 下一跳 receiver 地址
  wal_dir: /opt/influx-sync/data/relay-wal  # 转发 WAL（必配，重启不丢）
  timeout: 10s                  # 转发读写超时（V1.4：真正生效，原先被硬编码覆盖）
  dlq_dir: ""                   # 转发失败转存目录（V1.4/C2）；空=默认 <wal_dir>/../relay_dlq

monitor:  # 同 sender
  addr: :28080
log:      # 同 sender
  file: /opt/influx-sync/logs/receiver-174.log
```

## 3. 生产环境实际配置（生产主站，2026-08）

### 103（隔离前，发送侧）双实例

| 实例 | 源 | 订阅信号 | TP202 目标（隔离装置前侧虚地址） |
|---|---|---|---|
| sender-174 | `http://192.0.2.174:11911` hisdb | :18098 | `192.0.2.131:28101` |
| sender-175 | `http://192.0.2.175:11911` hisdb | :18098 | `192.0.2.131:28102` |

关键参数：watermark=2s（174）/10s（175）、batch_points=30000（174）/10000（175）、
query_limit=500000、poller_parallel=4、WAL 64MB、monitor :28080。

### 171（隔离后，接收侧）双实例（被动 server，监听本机即可）

| 实例 | 目标库 | 监听 |
|---|---|---|
| receiver-174 | `http://127.0.0.1:11911` HXScada | :28101 |
| receiver-175 | `http://127.0.0.1:11911` HXScada | :28102 |

**注意**：三区 171 是纯被动 server，**不需要配虚地址**（198.51.100.180 是隔离装置侧
地址，由装置转发到本机端口）；103 的 tcp.addr 才需要配虚地址（192.0.2.131）。

## 4. 环境变量覆盖

| 变量 | 覆盖项 |
|---|---|
| INFLUXSYNC_SOURCE_URL / _DATABASE | sender 源库 |
| INFLUXSYNC_TCP_ADDR | sender 目标地址 |
| INFLUXSYNC_WAL_PATH | sender WAL 目录 |
| INFLUXSYNC_TARGET_URL / _DATABASE | receiver 目标库 |
| INFLUXSYNC_TCP_LISTEN | receiver 监听 |
| INFLUXSYNC_MONITOR_ADDR / INFLUXSYNC_LOG_LEVEL | 通用 |

## 5. 参数选择建议

- **延迟敏感**：watermark=2s（延迟 ~4.2s）；**安全优先**：watermark=5s+（~7s）
- **吞吐**：batch_points 越大每帧点越多（上限受"压缩后 ≤1MB"约束，超限自动拆批）；
  query_limit 大→分页少；poller_parallel 4 适合 20 万点/s
- **多实例**：每实例独立 WAL 目录 / last_seq / DLQ / monitor 端口 / 日志文件——
  共用会损坏数据
- **InfluxDB 侧**（128G 内存环境）：tsi1 索引、max-values-per-tag=0、cache-max-memory=20g、
  snapshot=1g、压缩并发 16、wal-fsync-delay=1s
