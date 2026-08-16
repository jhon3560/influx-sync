# 实测部署记录：隔离装置真机同步（2026-08-16）

> 状态：**进行中**。本机 203.0.113.100 → 隔离装置 → 对端 203.0.113.240。
> 产品：influx-sync V1.7.7（tag v1.7.7，部署期修复 5 个缺陷 N12~N15 等，二进制 SHA256 见 SHA256SUMS）。
> 文档归属：docs/（AGENT.md §6 交接面），由双 AI 互审。

## 1. 环境

| 项 | 本机（发送侧） | 对端（接收侧） |
|---|---|---|
| 系统 | Windows + WSL（wjl） | 银河麒麟 V10 SP1 x86_64 |
| 地址 | 203.0.113.100 | 203.0.113.240（装置后侧 203.0.113.101 映射） |
| InfluxDB | docker influxdb:1.8（`:18086`，无认证） | 系统预装 influxd（`:8086`，认证 root/SECRET_REDACTED） |
| 端口 | monitor :18080 | monitor :28180，tcp.listen :28101 |

- 隔离装置对 `203.0.113.100 → 203.0.113.101` 多端口透明转发（28101/8086/80 实测连通）。
- 对端原库（**不得破坏，测完恢复**）：`_internal, mydb, equdb, pmudb, mhdb`。

## 2. 决策（用户确认）

1. **只同步 mhdb_main**（用户："选1，我们只需要一个库就够了"）。
2. **目标库改名 `mhdb_main_sync`**（用户授权自定）——与对端原库物理隔离，
   测完 `DROP DATABASE mhdb_main_sync` 即恢复对方原状态。
3. 对端妨碍测试的服务可临时停（用户授权）；本次未停任何服务，仅复用空闲端口。

## 3. 部署步骤

### 3.1 本机：恢复备份（portable 格式）

```bash
docker volume create sync-src-data
docker run -d --name sync-src -p 18086:8086 \
  -v sync-src-data:/var/lib/influxdb \
  -v "/mnt/f/DATA/bak20260314/influxbak":/backup:ro influxdb:1.8
docker exec sync-src influxd restore -portable -db mhdb_main /backup
```

**坑 1**：`influxd restore -portable -db` 单库恢复**要求 influxd 守护进程运行中**
（经 `127.0.0.1:8088` meta RPC 更新元数据再落 shard），与"先停服务再恢复"的旧认知相反。
一次性容器里跑会报 `error updating meta: dial tcp [::1]:8088: connection refused`。
**坑 2**：restore 容器必须同时挂数据卷与备份目录（首次忘挂 /backup）。
**坑 3**（V1.7.3 缺陷，已修 V1.7.4）：`backfill: all` 被 `Validate()` 误入通用 duration
校验表报 `bad duration "all"`——文档默认值无法通过配置校验，专门处理 all/0 的代码成死代码。
**坑 4**：receiver **不会自动建目标库**，须先在目标 influx 手动 `CREATE DATABASE`。
**坑 5**（V1.7.3 缺陷，已修 V1.7.5，N12）：`ProbeOldestData` 按 7 列假设取
`SHOW SHARD GROUPS` 的 `row[4]`，但 InfluxDB 1.8 真实为 6 列（无 shard_group 列），
`row[4]` 实为 **end_time**：
- 单分片组库（当前组未闭合）→ 游标=end_time 落在**未来** → 完全不发数据；
- 多分片组库 → 游标=最老组结束时间 → **最老一段数据静默丢失**（mhdb_main 会丢 2025-12-15→12-22 一周）。
冒烟测试实测捕获（smoketest 游标 08-17T00:00Z 未来值）。单测 mock 编码了错误的
7 列布局故未拦截，已改真实布局回归。**部署全部改用 V1.7.5 二进制**。

### 3.2 对端：receiver（V1.7.3 二进制，协议与 V1.7.5 一致，测试后统一升级）

```bash
mkdir -p /opt/influx-sync-test/{bin,logs,data}
scp bin/receiver root@203.0.113.240:/opt/influx-sync-test/bin/
scp ../../influx-sync-test/receiver.yaml root@203.0.113.240:/opt/influx-sync-test/
nohup /opt/influx-sync-test/bin/receiver -config /opt/influx-sync-test/receiver.yaml &
```

关键配置：`target.database: mhdb_main_sync`、`tcp.listen: :28101`、
`dedup.last_seq_file: /opt/influx-sync-test/data/last_seq`、认证 root/SECRET_REDACTED。

### 3.3 本机：sender（V1.7.5 二进制）

```bash
bin/sender -config /home/USER/influx-sync-test/sender.yaml
```

关键配置：`source=http://127.0.0.1:18086, database=mhdb_main`、
`tcp.addr=203.0.113.101:28101`、`backfill=all`、`compression=zstd`（默认）。

### 3.4 冒烟测试（28102 副链路，先行验证）

小库 `smoketest`（loadgen 2 万点）→ receiver-smoke → 对端 `mhdb_sync_smoke`。
目的：在大库回填前验证协议/装置层全链路。过程中捕获坑 3/坑 5 两个缺陷并修复。

## 4. 验证计划

| 项 | 方法 | 状态 |
|---|---|---|
| receiver 监听 | `ss -tlnp \| grep 28101` | ✅ pid 85564 |
| 链路连通 | `nc -vz 203.0.113.101 28101` | ✅ succeeded |
| 备份恢复 | SHOW DATABASES / COUNT | ✅ 21GB，13 分片组（2025-12-15→2026-03-14），measurement：hdb/hdb_test/yctp |
| 冒烟（28102 副链路） | 2 万点全链路 | ✅ **20000/20000 精确一致**（zstd+停等 ACK 穿装置正常） |
| 全量回填 | 对端 mhdb_main_sync 计数趋近源 | 🔄 V1.7.6/N14 提速后 1.5 天/分钟，ETA ~50 分钟 |
| 实时模拟（快路径） | loadgen 3000 点/s × 5min + SUBSCRIPTION→:18097 | ✅ **端到端 <1s**（对端最新点时间戳=写入时刻 ±1ms；93k 点/31s） |
| 订阅清洗 | 恢复备份携带原厂死订阅 | ✅ 已 DROP `mhdb_main_to_redis`（指向 203.0.113.1 原网络） |

### 4.1 关键实测数据（快路径）

- 订阅：`CREATE SUBSCRIPTION sync_fast ON mhdb_main.autogen DESTINATIONS ALL 'http://172.17.0.1:18097'`
  （docker 源库 → WSL 宿主 sender 快路径；[subscriber] flush-interval 默认 1s = 延迟地板）
- 实测：loadgen 写入 3000 点/s，对端 `SELECT LAST(value)` 时间戳与 UTC 当前时刻差 **<1s**。
- 回填与快路径并行互不阻塞：游标在 2025-12 历史区时，telemetry 实时点已在对端出现。

### 4.2 坑 6（备份元数据携带订阅）

portable 恢复会把**原厂的 SUBSCRIPTION 元数据一并带过来**（mhdb_main_to_redis →
http://203.0.113.1:80/apis/influx/subscription），恢复后的 influxd 会向原网络推送
（本环境不可达，重试刷日志）。恢复后务必 `SHOW SUBSCRIPTIONS` 检查清理。

### 4.3 性能观察（V1.8.0/N16 修复后）

- **坑 13（V1.7.7 瓶颈，已修 V1.8.0，N16）**：4 路采样定位——源库查询
  ~1.8s/轮 与窗口处理串行，发送端 0.96 fps 等米下锅（发送能力实测 6.6 fps，
  装置通道实测 57 Mbit/s 远未打满）。修复：① **查询预取流水线**：处理本轮
  结果时下一窗口查询已在途（消费轮采用槽边界，游标不符才丢弃重查=零丢失）；
  ② **window_target 解耦**：帧调小对准协议上限（937）后欠满判定不再被帧大小
  锁死，窗口仍按 30000 点目标翻倍。实测：0.41→**0.71 天/分钟（+73%）**、
  0.96→**8.19 fps（×8.5）**。稠密区收益预计更大（窗口从 5s 锁死解放）。
- 配置最终态：batch_points=937 + window_target=30000 + query_limit=40000 +
  poller_parallel=8 + pipeline_window=8 + max_window=3600s。
- **坑 14（源库调优）**：WSL docker 源库恢复后 8 路压缩合并与查询争抢 IO。
  已调：`max-concurrent-compactions=2`（让 IO 给 SELECT）、`cache-max-memory-size=4GB`
  （分页重扫 TSM 少读盘）、`cache-snapshot-memory-size=256MB`、`wal-fsync-delay=100ms`。
  调优后 0.71→0.76 天/分钟；裸查询回到 ~170ms 低位。生产裸机 influxd 无需
  （默认合并并发不影响）；**回填类任务建议调低合并并发**。
- 源库（WSL docker）剩余压缩合并结束速率还会自愈。
- 停等/滑窗链路：本装置允许多帧在途（pipeline 8 全程 ack_fail=0）。

- **坑 7（V1.7.5 缺陷，已修 V1.7.6，N14）**：恢复的 mhdb_main 是**稀疏库**
  （~7 行/s，5s 窗口仅 ~33 点），而"空窗翻倍"只在窗口**完全为空**时触发 →
  回填速率被基础窗口封顶在 ~5× 实时（实测 1.5 小时/分钟，83 天历史需 16 天）。
  修复：**欠满窗口翻倍**——空窗或点数 < batch_points 均翻倍（5s→…→1h），
  翻倍窗口按 max_window 切片处理（空片跳过/稀疏片继续/稠密片复位），内存有界不变。
  部署同时调大 `max_window: 3600s`（稀疏数据切片内实际行数少，安全）→
  实测 **1.5 天/分钟**（~60× 提速），全量回填 ETA ~1 小时。稠密数据命中
  batch_points 自动复位，实时段窗口终点仍被 watermark 截断，延迟不受影响。
- **坑 8（环境级）**：hdb 行宽 ~8KB（400+ 字段），batch_points=30000 帧原始 ~240MB
  超协议上限 → 自动减半拆批（820+ 次，无丢无卡，C3 机制生效）。拆批后实际帧
  ~500-2000 点（协议上限 16MB 原始 / 1MB zstd）。
- **坑 10（停等链路 RTT 实测）**：隔离装置往返 **~1.5s/帧**，吞吐 = 帧点数/RTT。
  实测 batch_points=1000（免拆批）时速率 **0.04 天/分钟**，batch_points=30000
  （拆批后帧少）时 **0.63 天/分钟**——小 batch 反慢 15 倍。巨行库应保持大 batch
  让拆批机制自动定帧。
- **坑 11（滑窗实测，A1 实验项落地）**：本装置**允许多帧在途**（pipeline_window=8
  全程 ack_fail=0）。速率：停等 0.63 → 滑窗 4 0.95 → 滑窗 8 0.92 天/分钟
  （4 以上持平，瓶颈转为源查询/发送侧）。生产部署可开 pipeline_window=4~8，
  但**须先在本装置实测确认**（旧装置约束：同连接多请求在途可能不通）。
- **坑 9（环境级）**：大窗口回填查询 + 订阅推送 + loadgen 写入并发时，WSL docker
  源库过载（load 11+），loadgen 出现过一次 30s 写超时（仅影响模拟，不影响同步正确性）。
- **坑 12（V1.7.6 缺陷，已修 V1.7.7，N15）**：稠密区 42 分钟窗口 JSON 响应 >64MB，
  客户端 LimitReader **静默截断** → `parse query response: unexpected EOF` →
  同窗口无限重试，回填停滞 10+ 分钟。修复：响应上限 64MB→512MB + 截断显式
  报错（提示调低 query_limit/max_window）+ 查询失败复位窗口增长（下轮回基础
  窗口自愈）。部署同时调低 `query_limit: 10000`（胖行 400+ 字段/行，10k 行≈10MB/页）。
- 链路吞吐（batch 30000 + 滑窗 8）：稀疏区 ~0.9 天/分钟，稠密区 ~0.2 天/分钟
  （受帧发送与查询共同限制）。

### 4.4 已同步区间终验（游标 2026-01-15 时执行，区间 2025-12-20→2026-01-10）

带时间范围轻量查询（避免全库扫压两端），源/目标**逐位一致**：

| 查询 | 源 | 对端 |
|---|---|---|
| hdb.DC2C1B1DMTag01 COUNT | 1,106,751 | 1,106,751 ✅ |
| hdb.DC2C1B1DMTag01 SUM | 893,627 | 893,627 ✅ |
| yctp.Q100 COUNT | 1,457 | 1,457 ✅ |
| yctp.Q100 SUM | -123.64999999999944 | -123.64999999999944 ✅（浮点位级） |
| telemetry COUNT（实时段） | 810,000 | 810,000 ✅ |

**结论：WAL+seq+幂等+zstd+滑窗链路零丢失，浮点值位级保真。**

## 5. 收尾/恢复对端（测试完成后执行）

1. 对端：`pkill -f influx-sync-test/bin/receiver`；`rm -rf /opt/influx-sync-test`
2. 对端：`influx -username root -password SECRET_REDACTED -execute "DROP DATABASE mhdb_main_sync"`
   （原库 _internal/mydb/equdb/pmudb/mhdb 全程未动）
3. 本机：停 sender；`docker rm -f sync-src && docker volume rm sync-src-data`（视需保留）
