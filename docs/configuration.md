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
  fast_path:            # A4 快路径（V1.5）：订阅推送直接透传，端到端延迟降到 <1s
    listen: ":18097"    #   推送监听地址；空=禁用（退化为纯轮询）
    mode: auto          #   off=仅信号（=旧 signal_listen 行为）/ auto=游标追平自动启用（默认）/ on=强制转发
    activate_age: 5s    #   auto：cursor 年龄 ≤ 此值启用透传（默认 watermark+3s）
    deactivate_age: 30s #   auto：年龄 > 此值退回仅信号（迟滞防抖）
    dedup_window: 15s   #   去重集保留窗口（默认 watermark+5s；驱逐/重启只会造成重复转发，不丢）
  backfill: 0s          # 首次启动回填时长（游标=now-watermark-backfill）
  tag_columns: []       # 显式 tag 列（空=自动 SHOW TAG KEYS 发现）。
                        # 推荐显式配置：完全绕开 schema 降级期类型漂移风险（N8）；
                        # 显式配置时不再查询 SHOW TAG KEYS（仍查 SHOW FIELD KEYS 取类型）
  measurements: []      # 同步的 measurement（空=全部 /.*/）；快路径逐行按此过滤

wal:
  path: /opt/influx-sync/data/wal-174   # WAL 目录（必填；勿与其他实例共用）
  segment_size: 64MB    # 段大小

tcp:
  addr: 192.0.2.131:28101  # receiver 地址（必填；经隔离装置映射的虚地址）
  timeout: 10s          # 读写超时
  dial_timeout: 10s     # 连接超时
  compression: zstd     # V1.6 帧压缩算法：zstd（默认，更快+压缩率更高）/ gzip（兼容旧接收端）
                        # 注意：zstd 需两端同版本（同一安装包满足）；混合版本升级期设 gzip

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
  # V1.4.1：LRU 去重已移除（写死废状态），last_seq 连续前缀推进 + 幂等写入兜底；
  # 在途帧数通过 recv_inflight 指标观测
  last_seq_file: /opt/influx-sync/data/last_seq-174  # 已处理最大连续 seq 持久化

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

## 5. 参数选择建议（含 V1.5/V1.6 本机实测数据）

### 5.1 batch_points：帧大小是吞吐/带宽/延迟的公共旋钮

**协议约束**（不可违反，超限自动处理）：

- 压缩后单帧 ≤1MB（隔离装置单包上限）；解压后 ≤16MB。超限时自动减半拆批
  （不卡死），单点仍超限则跳过该点并计数（`point_skip_total`）。

**经验公式**：实测（真实测点形态 LP，zstd L1）压缩率约 **10x**，行均 ~90B →
单帧点数上限 ≈ `10 × 1MB / 行字节数` ≈ 11 万点——远大于常用 batch_points。
**所以 batch_points 实际由实时性/内存/重传粒度决定，而不是压缩上限。**

| 场景 | batch_points | 帧大小（原始→zstd） | 理由 |
|---|---|---|---|
| 高吞吐（≥10 万点/s） | 30000 | ~2.4MB → ~240KB | 帧数最少，停等 RTT 摊销最好 |
| 均衡（默认） | 10000 | ~800KB → ~80KB | 吞吐/重传粒度/内存均衡 |
| 低写入率/低延迟敏感 | 1000~5000 | 80~400KB → 8~40KB | 每帧处理快；小帧压缩率略降但绝对流量小 |

- **吞吐公式**：停等链路吞吐 = `batch_points / RTT`。隔离装置 RTT 大时，
  **优先加大 batch_points**（减半 = 吞吐减半）；V1.5 起另有
  `pipeline_window` 滑窗（需装置验证后开启）。
- **快路径不受 batch_points 影响**：订阅帧大小由源库 `[subscriber]`
  `write-buffer-size` 与 `flush-interval` 决定（见 5.3）。

### 5.2 压缩算法（V1.6）

- `tcp.compression: zstd`（默认）/ `gzip`。本机实测链路带宽（5 万点/s）：
  **zstd 1.2Mbps vs gzip 2.6Mbps**；10 万点/s：zstd 1.4~2.0Mbps vs gzip 2.5~3.2Mbps
  ——zstd 约为 gzip 的 1/2~1/3，且发送端 CPU 更低。
- zstd 需两端同版本（同一安装包部署即满足）；**混合版本升级期把两端都设回 gzip**。
- **字典训练：实测不建议**。对 ≥500 点的帧，16KB 训练字典使输出反而 **大 12~14%**
  （LP 格式规整，zstd 帧内前 ~1KB 即自学习收敛，静态字典反而与自适应窗口竞争）；
  仅 <1KB 小帧有 ~16% 增益，绝对收益可忽略。压缩瓶颈已不在链路，不必引入
  字典分发/版本复杂度。

### 5.3 订阅侧参数（源库 influxdb.conf `[subscriber]`，快路径相关）

| 参数 | 默认 | 建议 | 说明 |
|---|---|---|---|
| `write-buffer-size` | 1000 | 1000~5000 | 推送批次点数上限 |
| `flush-interval` | 1s | 100~200ms | **快路径延迟地板**；越小越实时 |
| `http-timeout` | 30s | 30s | 推送 HTTP 超时 |

推送帧大小 ≈ `min(buffer, 写入率 × flush-interval)`：

- 写入率高（≥ buffer/flush-interval）→ 按 buffer 触发，帧 = buffer 点数（压缩率正常）；
- 写入率低 → 按 flush-interval 触发，产生小帧（压缩率低，但绝对流量小，无碍）。
- 例：flush-interval=200ms + buffer=1000 → 写入率 ≥5000 点/s 时帧恒为 1000 点。

### 5.4 其余参数

- **延迟敏感**：轮询路径 watermark=2s（e2e ~2.5s）；**安全优先**：watermark=5s+；
  要亚秒级请启用快路径（`fast_path.listen`，见 docs/a4-fast-path.md）——
  实测快路径所有档位 e2e 0~1s，负载越高相对优势越大。
- **吞吐**：query_limit 大→分页少；poller_parallel 4 适合 20 万点/s；
  源库写入能力是 15 万+ 的常见瓶颈（实测容器环境 ~8~12 万点/s）。
- **多实例**：每实例独立 WAL 目录 / last_seq / DLQ / monitor 端口 / 日志文件——
  共用会损坏数据
- **InfluxDB 侧**（128G 内存环境）：tsi1 索引、max-values-per-tag=0、cache-max-memory=20g、
  snapshot=1g、压缩并发 16、wal-fsync-delay=1s

## 6. 默认值总表

| 配置项 | 默认值 | 生效位置 |
|---|---|---|
| source/target.timeout | 10s | influx HTTP 兜底（查询按窗口动态 30s+2×窗口，10min 封顶；写库 10s+1ms/行，120s 封顶） |
| sync.interval | 1s | 轮询周期 |
| sync.window | 5s | 查询窗口 |
| sync.watermark | 10s | 水位延迟 |
| sync.max_window | 30s | 单轮窗口上限 |
| sync.batch_points | 10000 | 单帧点数 |
| sync.query_limit | 100000 | 单次查询 LIMIT |
| sync.poller_parallel | 4（≤1 视为 4） | 并行 worker 数 |
| sync.signal_min_interval | 200ms | 信号最小间隔 |
| sync.fast_path.listen | 空（禁用） | A4 快路径监听（V1.5） |
| sync.fast_path.mode | auto | off=仅信号 / auto=游标追平自动启用 / on=强制 |
| sync.fast_path.activate_age / deactivate_age | watermark+3s / 30s | 自动启用/退避阈值（迟滞） |
| sync.fast_path.dedup_window | watermark+5s | 快路径→轮询去重集保留窗口 |
| sync.backfill | 0 | 首次回填 |
| wal.segment_size | 64MB | 段大小 |
| tcp.timeout / dial_timeout | 10s | sender TCP 读写/拨号 |
| tcp.compression | zstd | 帧压缩：zstd（V1.6 默认）/ gzip（兼容旧接收端） |
| tcp.read_timeout | 60s | receiver 读帧超时 |
| tcp.max_inflight | 8（≤0 视为 8） | receiver 并发写库窗口 |
| tcp.max_conns | 0（不限制） | receiver 最大连接数 |
| sender.max_retry | 10 | 连续失败告警阈值（不丢弃） |
| sender.backoff_base / backoff_max | 1s / 60s | 退避 |
| sender.heartbeat_interval | 30s | 空闲心跳 |
| sender.pipeline_window | 1（停等） | 滑窗（实验项，需装置验证） |
| sender.idle_sleep | 200ms（内部常量） | 空闲兜底轮询（WAL 通知失效时） |
| log.level | info | 日志级别 |
| log.max_mb / max_backups | 100 / 10 | 轮转 |
| relay.timeout | 10s | 中继读写超时 |
| relay.dlq_dir | `<wal_dir>/../relay_dlq` | 中继失败转存 |

运行时常量（不可配）：WAL checkpoint 节流 1s；last_seq 持久化节流 1s；DLQ 容量 1GB
（sender 侧）；帧压缩上限 1MB / 解压上限 16MB；seq 大跳跃阈值 100000；schema 缓存
1h（降级 30s）；心跳 seq=0；数据 seq 从 1 开始。
