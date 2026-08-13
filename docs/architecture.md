# 功能设计（Architecture）

## 1. 目标

把安全区 I（隔离装置前）InfluxDB 1.x 的测点数据，经正向隔离装置（TCP 单通、响应仅允许单字节
0xff/0x00）同步到安全区 III 的 InfluxDB 1.x。要求：单向、有序、At-Least-Once、断点续传、
20 万点/s 级吞吐、目标库时间戳与源一致。

## 2. 总体架构

```
[I 区] 源 InfluxDB(174/175)
        │  轮询查询 + 订阅信号
        ▼
[sender] ──┬─ Poller：窗口轮询([cursor, now-watermark)) → 并行查询/组帧 → WAL → 推进游标
           ├─ SignalListener：订阅推送信号 → Notify → Poller 立即查询（事件驱动）
           └─ Sender：WAL.Peek → 停等发送(帧 → 单字节ACK) → Commit / 重试 / DLQ
        │  ISFP over TCP（隔离装置映射）
        ▼
[receiver] ──┬─ Server：逐连接逐帧读 → HandleFrame → 回 ACK
             ├─ 去重（last_seq + LRU）→ 解压 → 写目标 Influx
             ├─ 写库失败：永久(400) → DLQ 隔离 + 欺骗 ACK 0xff（解卡主链路）
             │             瞬时 → 回 0x00（sender 重发）
             └─ Relay(V1.3)：写库成功后原始行写入转发 WAL → 中继 Sender → 下一跳
        ▼
[III 区] 目标 InfluxDB(HXScada)
```

## 3. ISFP 协议

```
Header 固定 20B（Big Endian）：
| Magic(2)=0x5057 | Version(1)=1 | Type(1) | Seq(8) | Length(4) | CRC32(4) |
Payload = gzip(Line Protocol, BestSpeed)，CRC32 只算 Payload。

Type：0x01 数据帧 / 0x02 心跳帧（无 Payload）/ 0x03 控制 / 0xFF 错误
ACK：0xff 成功（已落库）/ 0x00 失败（可重试）

限制：压缩帧 ≤1MB（隔离装置单包约束）；解压上限 16MB（独立，防截断毒丸）。
```

## 4. 数据流与关键机制

### 4.1 Poller（源端查询）

- 游标 cursor（WAL 持久化）→ 查询窗口 [cursor, min(cursor+window, now-watermark))
- watermark 防"写入尚未完成可见"；首次启动游标 = now-watermark-backfill（可回填历史）
- 订阅信号（源库 SUBSCRIPTION → signal_listen）触发立即查询；ticker 兜底保不漏
- 多窗口并行查询（poller_parallel），段边界 +1ns 重叠 + Key 去重（防高压漏行）
- **顺序铁律**：先 WAL 落盘成功，后推进游标——违反会漏数据

### 4.2 WAL（落盘缓冲）

- 分段追加（默认 64MB/段），记录格式 `[u32 len][frame]`，每帧 fsync
- checkpoint 原子持久化（tmp+rename）：cursor、next_seq、段确认位置
- 目录 flock 防多实例并发损坏；重启扫描段重建索引，段全确认后整段删除
- DLQ：毒丸帧转存 `dlq/seq-*.frame|txt`，上限 1GB

### 4.3 Sender（停等发送）

- WAL.Peek → TCP 发送 → 读 1 字节 ACK（隔离装置只允许单字节返回）
- 0xff：Commit（WAL 段确认推进）；0x00/超时：指数退避重发（1s→60s 封顶），**永不丢弃**
  （At-Least-Once 红线；毒丸由 receiver 侧 DLQ 隔离后回 0xff 解卡）
- 空闲心跳（30s）维持通道活性；断连自动重连（对端恢复后重发积压）

### 4.4 Receiver（落库与 ACK）

- 去重：`seq ≤ last_seq` 直接 0xff；LRU（in-flight 窗口）防乱序重入
- seq 跳跃：接受处理（幂等覆盖安全）——拒绝会造成停等死锁（V1.3.1）
- 写库失败分类：HTTP 4xx 永久 → DLQ 隔离 + 0xff（不丢主通道）；其他 → 0x00 重试
- **ACK 语义：0xff = 已落库**；last_seq 持久化每秒节流（崩溃窗口由重发+幂等兜底）

### 4.5 反压（Poller 侧）

WAL 所在盘占用率三级水位（迟滞）：<60% 全速；60~80% 降速（批减半+间隔 5s）；≥80%
挂起（游标冻结），降至 60% 以下恢复。

### 4.6 中继（V1.3）

receiver 写库成功后，把原始 Line Protocol 追加到独立转发 WAL（独立 seq），复用标准
Sender 发往下一跳 receiver（无需改动）。B 写库成功即 ACK 上游；转发异步缓冲（重启不丢）；
毒丸不转发。详见 [relay.md](relay.md)。

## 5. 可靠性验证（实测）

| 场景 | 结果 |
|---|---|
| 20 万点/s × 60s | 1200 万点实时零丢失、零重试 |
| 目标 InfluxDB 重启 | 自动恢复补传，不丢 |
| 链路中断恢复 | 断点续传自动追平 |
| 毒丸注入 | DLQ 隔离，主通道不阻塞，其余数据零丢失 |
| 中继正常 + C 断连恢复 | 双库各 150/300 万零丢失，恢复自动补发 |
| 批量同时间戳（dense ts） | OFFSET 分页防卡死 |

## 6. 与 hx_migrate 的对比（实测数据）

| 维度 | hx_migrate | influx-sync |
|---|---|---|
| 吞吐上限 | ~10-12 万点/s（写库单线程停等） | 20 万点/s 实时 |
| 带宽（5 万点/s） | 47.8 Mbps（明文+TP202 开销） | 2.5 Mbps（gzip） |
| 断链恢复 | client 不自动重连，对端重启需重启本端 | 自动重连断点续传 |
| 数据获取 | 订阅推送（只实时，表名过滤） | 轮询+信号，可回填历史 |
| 结论 | 保留使用（现有链路）；≤10 万点/s 安全区 | 主方案 |

## 7. 关键设计约束

- 隔离装置限制：TCP 单通、响应单字节 0xff/0x00 → 停等协议是唯一可靠方式
- 目标库时间戳与源一致（±0ns，V1.3.1 起 json.Number 消除 ±256ns 误差）
- 不做跨库事务、不做源端写回、同步过程不修改任何数据字段
