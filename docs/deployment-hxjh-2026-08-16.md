# 实测部署记录：隔离装置真机同步（2026-08-16）

> 状态：**进行中**。本机 203.0.113.100 → 隔离装置 → 对端 203.0.113.240。
> 产品：influx-sync V1.7.3（tag v1.7.3，二进制 SHA256 见 SHA256SUMS）。
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
修复见 commit（V1.7.4），部署改用 V1.7.4 二进制。
**坑 4**：receiver **不会自动建目标库**，须先在目标 influx 手动 `CREATE DATABASE`。

### 3.2 对端：receiver

```bash
mkdir -p /opt/influx-sync-test/{bin,logs,data}
scp bin/receiver root@203.0.113.240:/opt/influx-sync-test/bin/
scp ../../influx-sync-test/receiver.yaml root@203.0.113.240:/opt/influx-sync-test/
nohup /opt/influx-sync-test/bin/receiver -config /opt/influx-sync-test/receiver.yaml &
```

关键配置：`target.database: mhdb_main_sync`、`tcp.listen: :28101`、
`dedup.last_seq_file: /opt/influx-sync-test/data/last_seq`、认证 root/SECRET_REDACTED。

### 3.3 本机：sender

```bash
bin/sender -config /home/USER/influx-sync-test/sender.yaml
```

关键配置：`source=http://127.0.0.1:18086, database=mhdb_main`、
`tcp.addr=203.0.113.101:28101`、`backfill=all`、`compression=zstd`（默认）。

## 4. 验证计划

| 项 | 方法 | 状态 |
|---|---|---|
| receiver 监听 | `ss -tlnp \| grep 28101` | ✅ pid 85564 |
| 链路连通 | `nc -vz 203.0.113.101 28101` | ✅ succeeded |
| 备份恢复 | SHOW DATABASES / COUNT | 进行中（20GB） |
| 全量回填 | 对端 mhdb_main_sync 计数趋近源 | 待 |
| 实时模拟 | loadgen 写入 → 观察 sender/receiver 指标 | 待 |
| （可选）快路径 | 源库 SUBSCRIPTION → sender :18097 | 待 |

## 5. 收尾/恢复对端（测试完成后执行）

1. 对端：`pkill -f influx-sync-test/bin/receiver`；`rm -rf /opt/influx-sync-test`
2. 对端：`influx -username root -password SECRET_REDACTED -execute "DROP DATABASE mhdb_main_sync"`
   （原库 _internal/mydb/equdb/pmudb/mhdb 全程未动）
3. 本机：停 sender；`docker rm -f sync-src && docker volume rm sync-src-data`（视需保留）
