# 部署手册

> V1.6.0（A4 订阅快路径 + zstd 压缩）。安装包：`influx-sync-v1.6.0-linux-amd64.tar.gz`。
> 配置说明见 [configuration.md](configuration.md)；协议见 [protocol.md](protocol.md)。

## 1. 安装布局

```
/opt/influx-sync/
├── bin/
│   ├── sender            # V1.6.0 静态二进制（CGO_ENABLED=0，兼容麒麟 V10 glibc 2.28）
│   ├── receiver
│   ├── loadgen           # 压测工具（可选）
│   └── *.bak             # 历史版本备份（升级前保留；含 <新版本>.bak 双份）
├── conf/
│   ├── sender-174.yaml / sender-175.yaml
│   └── receiver-174.yaml / receiver-175.yaml
├── data/                 # WAL / last_seq / DLQ / relay-wal（按实例分子目录）
└── logs/                 # 程序日志（内置轮转 100MB×10）
```

专用系统用户：`influxsync`（useradd -r -M -s /sbin/nologin influxsync），
`chown -R influxsync:influxsync /opt/influx-sync`。

## 2. 首次安装（安装包）

```bash
# 1. 解包并安装
tar xzf influx-sync-v1.6.0-linux-amd64.tar.gz -C /tmp
cd /tmp/influx-sync-v1.6.0
./upgrade.sh --no-restart          # 只安装二进制（首次无服务可重启）

# 2. 目录与服务账号
mkdir -p /opt/influx-sync/{conf,data,logs}
useradd -r -M -s /sbin/nologin influxsync || true
chown -R influxsync:influxsync /opt/influx-sync

# 3. 配置：按实例从示例复制改参
cp conf/sender.example.yaml   /opt/influx-sync/conf/sender-174.yaml
cp conf/receiver.example.yaml /opt/influx-sync/conf/receiver-174.yaml
vi /opt/influx-sync/conf/*.yaml        # 必填项见 configuration.md

# 4. systemd（包内模板，多实例用 %i 区分）
cp systemd/influx-sync-sender@.service   /etc/systemd/system/
cp systemd/influx-sync-receiver@.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now influx-sync-sender@174

# 5. 验证
tail -f /opt/influx-sync/logs/sender-174.log     # "sender started"
curl -s http://127.0.0.1:28080/metrics | head
go version -m /opt/influx-sync/bin/sender | grep vcs.revision   # 溯源戳=v1.6.0
```

## 3. 升级

```bash
# 安装包脚本升级（推荐）：备份 .bak → 装新二进制 → 重启全部 influx-sync-* 服务
tar xzf influx-sync-v1.6.0-linux-amd64.tar.gz -C /tmp
cd /tmp/influx-sync-v1.6.0 && ./upgrade.sh
```

手动升级（等价操作）：

```bash
cp /opt/influx-sync/bin/sender /opt/influx-sync/bin/sender.v<旧版本>.bak
install -m 0755 sender /opt/influx-sync/bin/sender
chown influxsync:influxsync /opt/influx-sync/bin/sender
systemctl restart influx-sync-sender-174
# 验证：日志 "sender started"、/metrics、WAL 积压追平
```

**回滚**：`cp bin/sender.bak bin/sender && systemctl restart influx-sync-sender-174`。

升级向后兼容：协议 Version=1 不变；WAL/checkpoint 格式兼容；新配置项均有默认值。
两侧（sender/receiver）不要求同步升级，但建议配套升级以取得正确性修复。

## 4. 源码构建（开发机）

```bash
# 麒麟 V10 glibc 2.28 必须静态编译；**先 commit+tag 再构建**，溯源戳才干净
git tag v1.6.0 && git status --porcelain   # 确保工作树干净
mkdir -p /tmp/out && CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /tmp/out/sender ./cmd/sender
CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /tmp/out/receiver ./cmd/receiver
CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /tmp/out/loadgen ./bench/loadgen
go version -m /tmp/out/sender | grep vcs   # vcs.modified 必须为 false
```

## 5. systemd 单元（包内模板，等价示例）

```ini
# /etc/systemd/system/influx-sync-sender@.service
[Unit]
Description=Influx Sync Sender - %i
After=network.target

[Service]
User=influxsync
ExecStart=/opt/influx-sync/bin/sender -config /opt/influx-sync/conf/sender-%i.yaml
Restart=always
RestartSec=5
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
```

receiver 同构。`systemctl enable --now influx-sync-sender@174` 即实例化 174。

## 6. 生产拓扑（生产主站）

```
174(源Influx) ──订阅──> 103 sender-174 (18098信号) ──TP202--> 192.0.2.131:28101 ──隔离──> 171 receiver-174 (:28101) --> HXScada
175(源Influx) ──订阅──> 103 sender-175 (18098信号) ──TP202--> 192.0.2.131:28102 ──隔离──> 171 receiver-175 (:28102) --> HXScada
```

- 103 双 sender 实例（/opt/influx-sync 单目录双 yaml）；171 双 receiver 实例
- 源库订阅：`CREATE SUBSCRIPTION hx_sync ON hisdb.autogen DESTINATIONS ALL 'http://<103生产IP>:18098'`
  （signal_listen 与 hx_migrate 共用 18098 亦可——信号只是"有数据了"的提示，不解析内容）
- **虚地址**：103 tcp.addr 配 192.0.2.131（隔离装置前侧）；171 只监听本机
  （0.0.0.0），**不需要配置 198.51.100.180**
- **运维地址 198.51.100.x 不应出现在任何配置里**（测试临时网段）
- 中继部署：B 机 receiver 加 `relay` 段（addr=下一跳、wal_dir 必配）；C 机标准
  receiver 无改动。详见 [relay.md](relay.md)

## 7. 防火墙

```bash
# 103（订阅信号入站）
firewall-cmd --permanent --add-port=18098/tcp
# 171（ISFP 入站）
firewall-cmd --permanent --add-port=28101/tcp --add-port=28102/tcp
firewall-cmd --reload
# 出站默认放行（sender 连对端不需规则）；严格模式需另加出站 rich rule
```

## 8. 上架检查清单

1. `ip link` 生产网卡 state UP；`ip route` 默认路由走生产网关
2. 103→虚地址 `ping`/`nc -z 192.0.2.131 28101` 通
3. 订阅创建且 `SHOW SUBSCRIPTIONS` 可见
4. 二进制溯源 `go version -m bin/sender | grep vcs` 与版本一致
5. 启动 sender → 日志 `sender started` + `/metrics` 可查
6. 写入 1 条测试数据 → 171 库 count 验证（用 influx CLI，勿用 curl count 高压 bug）
7. `sync_delay_seconds` 稳定在 watermark+处理时间；`sync_e2e_delay_seconds` 同步收敛
8. 滑窗如需开启：先完成 [pipeline-validation.md](pipeline-validation.md) 装置验证

## 9. 参数调优要点（V1.5/V1.6 本机实测，上线前必读）

完整推导见 [configuration.md](configuration.md) §5。生产四步走：

1. **压缩**：`tcp.compression: zstd`（默认）。实测链路带宽 zstd ≈ gzip 的 1/2~1/3
   （5 万点/s：1.2 vs 2.6Mbps），且 CPU 更低。**不要做字典训练**（≥500 点帧实测
   反而大 12~14%）。zstd 需两端同版本——**升级时 sender/receiver 一起升**；
   滚动升级窗口期两端设 gzip。
2. **batch_points**：高吞吐档位用 30000（帧 ~240KB，停等 RTT 摊销最好；吞吐 =
   batch/RTT，减半即吞吐减半）；均衡 10000；低写入率 1000~5000。压缩上限
   （1MB/16MB）实测远够用，超限自动拆批不卡死。
3. **订阅侧**（源库 `influxdb.conf [subscriber]`）：`flush-interval=100~200ms`
   （快路径延迟地板，默认 1s 太大）、`write-buffer-size=1000~5000`（推送帧点数；
   写入率 × flush-interval ≥ buffer 时帧恒为 buffer 点数，压缩率正常）。
   快路径推送帧大小只受这两个参数影响，**与 batch_points 无关**。
4. **延迟预期**：轮询路径 e2e ≈ watermark + 1~2s（高压下查询滞后会再涨，
   实测 10 万/s 时 12~17s）；快路径 e2e 0~1s（所有档位），压测数据见
   [bench-2026-08.md](bench-2026-08.md)。

防火墙补充：启用快路径时源库订阅需追加 fast_path 目的地（103 的 18097），
与 signal_listen 同一订阅多 DESTINATIONS（每库仅 1 个 subscription，见
[a4-fast-path.md](a4-fast-path.md) §6）。
