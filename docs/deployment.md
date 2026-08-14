# 部署手册

## 1. 安装布局

```
/opt/influx-sync/
├── bin/
│   ├── sender            # V1.4.2 静态二进制（CGO_ENABLED=0，兼容麒麟 V10 glibc 2.28）
│   ├── receiver
│   └── *.bak             # 历史版本备份（升级前 cp 保留）
├── conf/
│   ├── sender-174.yaml / sender-175.yaml
│   └── receiver-174.yaml / receiver-175.yaml
├── data/                 # WAL / last_seq / DLQ / relay-wal（按实例分子目录）
└── logs/                 # 程序日志（内置轮转 100MB×10）
```

专用系统用户：`influxsync`（useradd -r -M -s /sbin/nologin influxsync），
`chown -R influxsync:influxsync /opt/influx-sync`。

## 2. 构建与升级

```bash
# 构建（开发机 WSL 或任意 Go 1.22 环境）
CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o bin/sender ./cmd/sender
CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o bin/receiver ./cmd/receiver

# 升级流程（每实例）
cp /opt/influx-sync/bin/sender /opt/influx-sync/bin/sender.v<旧版本>.bak
install -m 0755 sender /opt/influx-sync/bin/sender
chown influxsync:influxsync /opt/influx-sync/bin/sender
systemctl restart influx-sync-sender-174
# 验证：日志 "sender started"、/metrics、WAL 积压追平
```

升级向后兼容：协议 Version=1 不变；WAL/checkpoint 格式兼容；新配置项均有默认值。

## 3. systemd 单元（示例）

```ini
# /etc/systemd/system/influx-sync-sender-174.service
[Unit]
Description=Influx Sync Sender - 174
After=network.target

[Service]
User=influxsync
ExecStart=/opt/influx-sync/bin/sender -config /opt/influx-sync/conf/sender-174.yaml
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

receiver 同构（ExecStart 换 receiver + 对应 yaml）。多实例=多个单元文件。
`systemctl enable --now` 开机自启。

## 4. 生产拓扑（生产主站）

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

## 5. 防火墙

```bash
# 103（订阅信号入站）
firewall-cmd --permanent --add-port=18098/tcp
# 171（ISFP 入站）
firewall-cmd --permanent --add-port=28101/tcp --add-port=28102/tcp
firewall-cmd --reload
# 出站默认放行（sender 连对端不需规则）；严格模式需另加出站 rich rule
```

## 6. 中继部署（可选，V1.3）

B 机 receiver 加 `relay` 段（addr=下一跳、wal_dir 必配）；C 机标准 receiver 无改动。
B 需能出站连 C。详见 [relay.md](relay.md)。

## 7. 上架检查清单

1. `ip link` 生产网卡 state UP；`ip route` 默认路由走生产网关
2. 103→虚地址 `ping`/`nc -z 192.0.2.131 28101` 通
3. 订阅创建且 `SHOW SUBSCRIPTIONS` 可见
4. 启动 sender → 日志 `sender started` + `/metrics` 可查
5. 写入 1 条测试数据 → 171 库 count 验证（用 influx CLI，勿用 curl count 高压 bug）
6. `sync_delay_seconds` 稳定在 watermark+处理时间
