# AGENT 开发环境与上下文（给下次 AI 开发会话）

> 本文档固化开发/部署环境的全部关键上下文，新会话先读此文件，无需再依赖历史对话。

## 1. 开发环境

| 项 | 值 |
|---|---|
| 源码 | WSL `~/influx-sync-src`（git 管理） |
| 当前版本 | V1.5.0（tag v1.5.0，A4 订阅 fast-path 透传；V1.4.3 分支为 audit-fixes 收尾） |
| 版本体系 | v1.0(master) → v1.1(feature/parallel) → v1.2~v1.2.3(feature/signal-trigger) → v1.3+中继 → v1.3.1(审计修复) → v1.4.0(性能审计修复) → v1.4.1(滑窗缺陷复审 N1-N5) → v1.4.2(复审 N6-N8) → v1.4.3(收尾加固)；每版本打 tag，二进制保留 .bak |
| 构建 | `CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o bin/sender ./cmd/sender`（麒麟 V10 glibc 2.28 必须静态） |
| 发布流程 | **先 commit+tag，确认工作树干净后再构建**（在仓库外目录构建再拷回 bin/），保证 `go version -m` 的 vcs.revision=tag 且 vcs.modified=false；发布包需含 SHA256SUMS |
| 测试 | `go test ./...`；`go test -race ./internal/...`；交付前两者都过 |
| 压测工具 | bench/loadgen：`-hx` 格式(hisdb 表) 与 telemetry 格式；`-rate/-duration/-workers/-batch/-url/-db/-user/-pass` |

## 2. 设备（测试期直连；上架后为隔离拓扑）

| 设备 | 角色 | 连接 | 认证 |
|---|---|---|---|
| 103 | 隔离前（sender 侧） | 198.51.100.103:10022 | hexin / SECRET_REDACTED；root / SECRET_REDACTED |
| 171 | 隔离后（receiver 侧） | 198.51.100.171:10022 | 同上 |

- **连接方式**：pi 的 MCP（ssh-agent-mcp 装在 Windows，mcp SDK<2.0）；连接配置在
  Windows `C:\Users\USER\ssh_config.json`（s103/s171，密码 SECRET_REDACTED）；WSL 备份
  `~/shanxi/ssh_config.json`。MCP 连接 idle 会断，用 `ssh_connect_by_name` 重连。
- **命令限制**：MCP 每条命令 timeout≤60s（长任务用 nohup 后台+轮询）；含 `&` 的命令
  必须写成 `echo '<pw>' | sudo -S bash -c 'nohup ... &'` 形式；pkill/pgrep 的 -f 模式
  会匹配自身命令行导致自杀，用 `[a]` 字符类技巧。
- **sudo**：`echo 'SECRET_REDACTED' | sudo -S <cmd>`（每会话首次）。**注意**：pam_faillock
  deny=5/unlock_time=600——连续密码错误会锁定账号 10 分钟，`faillock --user hexin --reset` 解锁。
- 传文件：Windows `pscp/plink`（`C:\Program Files\PuTTY\`），-P 10022 -pw 'SECRET_REDACTED'。

## 3. 设备当前部署状态（2026-08 上线准备完成）

### hx_migrate（现有方案，已按生产虚地址配置）
- 103：双实例（/opt/hx_migrate=174 链、/opt/hx_migrate_175=175 链），订阅监听 18099/18199，
  TP202 client → 192.0.2.131:18898/18899；watch_dog 守护
- 171：双实例（18898/18899 监听本机，写 HXScada）；db.conf 已注释 8 个无效引用
- 虚地址：103 侧 192.0.2.131（隔离前）、171 侧 198.51.100.180（仅 sender-171 占位；
  **171 被动 server 不配虚地址**）
- hx_migrate 弱点：写库单线程停等（上限 ~10-12 万点/s）；client 断连不自动重连（对端
  重启必须重启本端）；对端批量断开偶发 segfault（watch_dog 自动拉起）；表过滤靠
  channel table_name（写错静默丢弃）
- 文档：docs/hx_migrate配置说明_订阅同步.md

### influx-sync（主方案，保留部署、未启用）
- 103：sender-174/175（源 192.0.2.174/175:11911 → 192.0.2.131:28101/28102，
  signal_listen 18098）；171：receiver-174/175（:28101/28102 → HXScada，relay 已注释）
- systemd 单元已建（influx-sync-sender/receiver-*.service，disabled）

### 系统配置（两台）
- 密码：root SECRET_REDACTED / hexin SECRET_REDACTED（sudo 用）
- 网卡全部静态（BOOTPROTO=none）；运维网卡 DEFROUTE=no（192.168.137 段无默认路由，
  重启不恢复）；未用网卡禁 DHCP
- 防火墙：白名单已加（103: 18099-18199、10022；171: 18898-18899、28101/28102、11911、
  10022 等；target=default 未启用 DROP）
- 171 InfluxDB：auth-enabled=true，root/SECRET_REDACTED 全库权限；日志落
  /var/log/influxdb/（logrotate daily×30）；journald 限制 1G/128M/512M
- 171 机房运维脚本：/home/hexin/ops/（01_health 等 12 个，见 operations.md）
- 生产路由：103 `192.0.2.0/24 via 192.0.2.254`（rc.local）；171 `198.51.100.0/24
  via 198.51.100.253`；生产网卡 linkdown（上架接线后 UP）

### 上线待办（174/175 上线后）
1. 174/175 建订阅：`CREATE SUBSCRIPTION hx_sub ON HXScada.autogen DESTINATIONS ALL
   'http://192.0.2.176:18099'`（175 用 18199）——hx_migrate 用
2. influx-sync 信号订阅（如需启用）：推送到 103 的 18098
3. **V1.5 A4 fast-path**：每库仅允许 1 个 subscription——需把 fast-path 地址
   （103 的 18097）追加为同一订阅的第二个 DESTINATION（DROP+CREATE，几秒推送
   中断，无数据影响）；建议 `[subscriber] flush-interval=100~200ms`。详见
   docs/a4-fast-path.md §6/§9
4. 确认源库表名（channel table_name 当前 hisdb 是占位值）

## 4. 关键决策记录

| 决策 | 结论 |
|---|---|
| 方案选择 | hx_migrate 与 influx-sync **并存**；influx-sync 为主方案（完整性/带宽/延迟优），hx_migrate ≤10 万点/s 安全区 |
| 报告原则 | 文档不体现源码对比、不体现版本迭代过程、无倾向性，只列测试数据；交付 Word 版 |
| 测试规范 | 不模拟数据库重启（只测正常工况）；每档清库独立验证；压测中观察（延迟/读取错误率每 5s 采样）；验证用 influx CLI（curl 高压有 count bug） |
| 兼容红线 | At-Least-Once 永不丢弃数据帧（AckFail 只重试）；协议 Version=1 不变 |
| 滑窗（pipeline_window） | 默认 1=停等；开启前必须过 docs/pipeline-validation.md 装置验证 |
| seq 语义 | 数据帧从 1 开始（0 保留给心跳）；last_seq 连续前缀推进；重启缺口由新连接首帧闭合 |
| 文档维护 | 改动机制/配置/指标后同步更新 docs/（architecture/protocol/configuration/operations 四件套） |

## 5. 用户工作偏好（务必遵守）

- **缺信息直接问，不要乱猜**（参数、端口、表名等关键值）
- 设备操作**不要换 yum 源**（保持麒麟官方）；缺工具手动装 rpm
- 测试完**不要着急还原配置**，等用户命令
- 改代码前说明方案获批准；做好版本管理（git tag）+ 设备二进制 .bak 备份
- 用户常给中文指令；回复简明、用表格、给文件路径
