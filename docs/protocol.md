# ISFP 协议规范（Influx Sync Frame Protocol）

> 版本：Version=1（V1.4.3 现行）。本规范是 sender ↔ receiver 之间的线协议，
> 经正向隔离装置 TCP 映射传输。任何改动必须保持向后兼容（见 §7）。

## 1. 设计约束（由隔离装置决定）

| 约束 | 说明 |
|---|---|
| TCP 单通 | 仅 sender 侧可发起连接（装置映射方向固定） |
| 响应单字节 | 每个请求帧只允许回 **1 字节**应答：0xff / 0x00 |
| 单包上限 | 压缩后帧 ≤1MB（装置单包限制） |
| 一问一答（默认） | 装置对"同连接多请求在途"的兼容性**未验证**——滑窗需按
[docs/pipeline-validation.md](pipeline-validation.md) 实测后方可开启 |

## 2. 帧格式

```
Header 固定 20 字节，Big Endian：
| Magic(2)=0x5057 | Version(1)=0x01 | Type(1) | Seq(8) | Length(4) | CRC32(4) |
| <----------- Payload: gzip(Line Protocol, BestSpeed) ------------> |
```

| 字段 | 含义 | 规则 |
|---|---|---|
| Magic | 0x5057（"PW"） | 不符即断连接 |
| Version | 0x01 | 不符即断连接（未来升级需协商） |
| Type | 帧类型 | 0x01 数据 / 0x02 心跳 / 0x03 控制（预留）/ 0xFF 错误（预留） |
| Seq | 帧序号 | **从 1 开始**（见 §4）；sender 内严格递增 |
| Length | Payload 字节数 | HeaderSize+Length ≤ 1MB |
| CRC32 | IEEE，仅覆盖 Payload | 不符回 0x00（重传兜底） |

- 心跳帧：Payload 为空（Length=0），不压缩、CRC=0。
- 数据帧 Payload = gzip(原始 Line Protocol，BestSpeed 级别)。
  解压上限 16MB，**独立于**压缩上限——压缩后 ≤1MB 不保证解压后 ≤1MB
  （30000 点 ≈2.4MB 原始），解压超限报错拒绝而非截断（截断会破坏末行完整性）。

## 3. ACK 语义

| 字节 | 含义 | 触发 |
|---|---|---|
| 0xff | 成功：**该帧数据已落库**（或毒丸已隔离进 DLQ、或旧 seq 去重命中） | 写库成功 / DLQ 隔离成功 / `seq ≤ last_seq` |
| 0x00 | 失败（可重试） | 解压失败、CRC 不符、写库瞬时失败（5xx/超时/网络）、DLQ 落盘失败 |

- **"0xff = 已落库"** 是 At-Least-Once 的基石：sender 只在收到 0xff 后才 Commit
  删除 WAL 帧；0x00/超时一律重发，**永不丢弃**。
- 0x00 一律可重试：毒丸（HTTP 4xx 等永久错误）由 receiver 隔离进 DLQ 后回 0xff
  解卡，不占用 0x00 重试通道。
- 应答顺序：receiver 并发写库（A2 流水线）但 **ACK 按帧到达顺序写回**——
  帧 k 写库完成且 0..k-1 已回才回它的 ACK，wire 与停等模式逐字节一致。

## 4. Seq 语义（V1.4.2 定稿）

| 规则 | 说明 |
|---|---|
| 数据帧 seq 从 **1** 开始 | seq=0 保留给心跳（N7 文档化）：data seq=0 会被 receiver 首帧去重（`seq ≤ last_seq=0`）吞掉 |
| 心跳 seq 恒为 0 | 不消耗数据 seq 空间；receiver 按 Type 先于去重检查处理心跳 |
| sender 侧严格递增 | WAL NextSeq 单调递增；越界 append 直接报错（顺序铁律） |
| receiver 侧连续前缀推进 | last_seq 只推进连续前缀（并发乱序完成安全，N2）；大跳跃（>100000）直接越过（幂等覆盖安全） |
| 缺口闭合 | 新连接首帧 = sender WAL 头，首帧 seq>last+1 时闭合 [last+1, seq-1]（N6）——该区间在 sender 侧已 Commit，"0xff=已落库" 保证已完成 |

## 5. 连接生命周期

```
sender:  Dial → [发送帧 → 读 1B ACK]×N（停等）→ 空闲心跳（30s 周期）
         ACK 超时/读错/写错 → 关连接 → 退避重连 → 从 WAL 头重发
receiver: Accept → 读帧（read_timeout 60s）→ 并发 handler → 按序回 ACK
          bad magic/version → 回 0x00 并关连接（sender 重连重发）
```

- 连接断开时 receiver 未完成的写库仍会完成（幂等），sender 重发后由
  last_seq/幂等覆盖去重。
- 滑窗模式（pipeline_window>1，实验项）：0x00/非法 ACK/发送失败视为**连接级失败**，
  关连接重连后从失败帧起重发（go-back-N）——重连清空陈旧 ACK 流，杜绝错位提交（N1）。

## 6. 帧大小与超时

| 项 | 值 |
|---|---|
| 压缩帧上限 | 1MB（含 Header；装置单包约束） |
| 解压上限 | 16MB（防解压炸弹，独立于压缩上限） |
| sender TCP 读写超时 | 默认 10s（tcp.timeout） |
| receiver 读帧超时 | 60s（tcp.read_timeout，需 > 心跳间隔） |
| receiver 写库超时 | 10s + 1ms/行，封顶 120s（按批大小动态） |
| ACK 写回超时 | 5s（写失败视为连接故障） |
| 心跳间隔 | 30s（sender.heartbeat_interval） |

## 7. 兼容性规则

- **Version=1 不变**：任何协议级改动（新 Type、字段重定义）需升级 Version 并协商，
  禁止静默变更语义。
- WAL/checkpoint/DLQ 均为本地持久化格式，与线协议解耦——线协议不变的版本
  升级不要求两侧同步升级（但仍建议配套升级以取得正确性修复）。
- 滑窗（A1）为**线协议兼容**的同连接多请求在途扩展：每个帧仍对应一个响应字节、
  顺序不变；兼容性瓶颈在装置而非协议。
