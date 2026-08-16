# 实测部署记录：隔离装置真机同步（2026-08-16）

> 状态：**进行中**。本机 203.0.113.100 → 隔离装置 → 对端 203.0.113.240。
> 产品：influx-sync V1.7.5（tag v1.7.5，部署期修复 2 个缺陷，二进制 SHA256 见 SHA256SUMS）。
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

### 4.3 性能观察（V1.7.6/N14 修复后）

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
- 链路吞吐（batch 30000 + 滑窗 8）：~0.9 天/分钟（受发送侧与查询共同限制）。

## 5. 收尾/恢复对端（测试完成后执行）

1. 对端：`pkill -f influx-sync-test/bin/receiver`；`rm -rf /opt/influx-sync-test`
2. 对端：`influx -username root -password SECRET_REDACTED -execute "DROP DATABASE mhdb_main_sync"`
   （原库 _internal/mydb/equdb/pmudb/mhdb 全程未动）
3. 本机：停 sender；`docker rm -f sync-src && docker volume rm sync-src-data`（视需保留）
