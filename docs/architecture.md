# 程序设计（Architecture & Design）

> V1.4.3。线协议规范见 [protocol.md](protocol.md)；配置见 [configuration.md](configuration.md)；
> 部署见 [deployment.md](deployment.md)；运维见 [operations.md](operations.md)。

## 1. 目标与红线

把安全区 I（隔离装置前）InfluxDB 1.x 的测点数据，经正向隔离装置（TCP 单通、响应仅
允许单字节 0xff/0x00）同步到安全区 III 的 InfluxDB 1.x。

| 要求 | 实现方式 |
|---|---|
| 单向 | 协议只有 sender→帧、receiver→单字节 ACK 两个方向 |
| 有序 | 帧按 seq 顺序发送/落库（接收端并发写库但按序 ACK） |
| At-Least-Once | WAL 持久化 + 0xff 才 Commit + 幂等落库 + 重试不丢弃（**永不丢帧红线**） |
| 断点续传 | WAL 游标 + last_seq + 重启扫描重建 |
| 吞吐 | 20 万点/s 级（V1.4 组帧优化后 CPU 不再是瓶颈） |
| 时标一致 | 目标库时间戳与源一致（±0ns，json.Number 解析） |

## 2. 总体架构

```
[I 区] 源 InfluxDB(174/175)
        │  轮询查询 + 订阅推送（信号 + V1.5 fast-path 透传）
        ▼
[sender] ──┬─ Poller：窗口轮询 [cursor, now-watermark) → 并行查询/组帧 → 快路径去重过滤 → WAL → 推进游标
           ├─ SignalListener：订阅推送信号 → Notify（pending-flag+延迟触发，不丢弃）
           ├─ FastPath(V1.5)：订阅推送批解析/过滤 → 同一 WAL（gzip 管线）；游标追平自动启用（三态+迟滞）
           └─ Sender：WAL.Peek → 停等发送（帧 → 单字节 ACK）→ Commit / 重试 / DLQ
        │  ISFP over TCP（经隔离装置映射）
        ▼
[receiver] ──┬─ Server：逐连接逐帧读 → 并发 handler（写库）→ 按序 ACK
             ├─ 去重（last_seq 连续前缀）→ 解压 → WriteRaw 直写目标 Influx
             ├─ 写库失败：永久(4xx) → DLQ 隔离 + 0xff（解卡）；瞬时 → 0x00（重发）
             └─ Relay(V1.3)：写库成功后原始行写入转发 WAL → 中继 Sender → 下一跳
        ▼
[III 区] 目标 InfluxDB(HXScada)
```

> A4 快路径说明：历史同步由 Poller 全权负责（订阅只推送新写入）；快路径仅在游标追平
> （cursor 年龄 ≤ activate_age）后透传，与轮询窗口重叠区由秒级分区去重集抑制重复转发；
> 快路径只加速、不改变正确性基座（详见 docs/a4-fast-path.md）。

## 3. 模块划分（包职责）

| 包 | 职责 | 关键点 |
|---|---|---|
| internal/protocol | ISFP 帧编解码 | 20B Header + gzip Payload + CRC32；解压上限独立校验 |
| internal/model | Point / Line Protocol 序列化与解析 | 缓存键序（schema 级一次排序）+ 转义循环 + strconv（60x CPU）；LP 行解析（V1.5 fast-path 用） |
| internal/influx | InfluxDB 1.x HTTP 客户端 | 查询/WriteRaw、schema 自适应（single-flight+降级复用）、动态超时 |
| internal/wal | 分段 WAL | group commit、checkpoint 节流、撕裂尾截断恢复、中段坏帧跳帧重同步 |
| internal/sender | Poller / Sender / SignalListener / FastPath | 反压状态机、边界去重、拆批/跳点、滑窗（实验）、WAL 通知唤醒、快路径透传+去重集（V1.5） |
| internal/receiver | 帧处理 | last_seq 连续前缀 + 缺口闭合、毒丸 DLQ、中继、e2e 延迟指标 |
| internal/transport | TCP 客户端/服务端 | 停等客户端；服务端流水线（并发 handler + 按序 ACK + 在途窗口） |
| internal/monitor | Prometheus 指标 | 全 atomic，无锁 |
| internal/config | YAML 配置 | 环境变量覆盖、严格校验（负时长拒绝） |

## 4. 关键数据结构与持久化格式

### 4.1 WAL

- 目录：`<wal_dir>/seg-%06d.log`（64MB/段，追加写）+ `checkpoint` + `.lock`
- 段内记录：`[u32 len][frame bytes]`，len = 20 + 解压后 payload 长（与帧头 Length 一致）
- 内存索引：启动扫描重建（seg/offset/length/seq/type），仅含未确认帧
- checkpoint（tmp+fsync+rename+目录 fsync 原子替换）：

```json
{"cursor_ns": <逻辑游标>, "next_seq": <下一帧序号>,
 "seg_start": <最老未删段>, "acked_bytes": <该段已确认偏移>}
```

- 恢复语义：checkpoint 落后于实际（节流）时由扫描重建兜底；确认帧重发由幂等覆盖；
  段删除后 checkpoint 未及时持久化也不丢（扫描容忍缺失段）
- 损坏处理：尾部撕裂 → 截断恢复（Error 日志，游标回退重查补数据）；中段坏帧 →
  跳单帧重同步（Warn 日志，丢一帧而非一尾）

### 4.2 DLQ

- 毒丸：`<dlq_dir>/dlq_<seq>_<秒>_<纳秒>.json`（Payload 以 gzip Base64 存）
- sender 侧 WAL DLQ：`<wal>/../dlq/seq-*.frame|txt`，容量 1GB 上限
- 中继转发失败：`relay.dlq_dir`（默认 `<wal_dir>/../relay_dlq`），格式同毒丸
  （降级边界：中继 DLQ 落盘本身失败只能记日志，relay.md 已明示）

### 4.3 last_seq

- 单文件原子写（tmp+rename），**每秒节流**；崩溃窗口由重发+幂等兜底
- 语义：已成功处理的**最大连续** seq（连续前缀推进，见 §6.2）

## 5. 并发模型

| 组件 | 模型 |
|---|---|
| WAL | 单 Mutex 串行全部 IO；append 通知通道（cap=1）唤醒空闲 Sender |
| Poller | 单 goroutine 主循环（信号/ticker 驱动）；查询/组帧 worker 池（poller_parallel） |
| Sender | 单 goroutine 停等循环；滑窗模式单连接 W 帧在途 |
| Receiver | 每连接一读循环 + 每帧一 handler goroutine（并发写库，幂等）；ACK 写回协程按序输出（cond 同步）；last_seq 推进器独立锁 |
| Metrics | 全 atomic，无锁读取 |

反压状态机（Poller，WAL 盘占用率三级水位，迟滞）：<60% 全速 → 60~80% 降速
（批减半+间隔 5s）→ ≥80% 挂起（游标冻结）→ <60% 恢复。

## 6. 核心机制

### 6.1 数据获取（Poller）

1. 游标 cursor（WAL 持久化）→ 窗口 [cursor, min(cursor+window, now-watermark))
2. watermark 防"写入尚未完全可见"；窗口上限 max_window 防时间跳变一次拉爆
3. 多段并行查询（poller_parallel），段边界 +1ns 重叠防高压漏行；**只对 N-1 个
   边界时间戳去重**（小集合零分配比较），分页边界同理
4. 订阅信号：pending-flag + 延迟触发（不丢弃，尾部延迟 ≤ signal_min_interval）；
   ticker 兜底保不漏；watermark 截断时自唤醒
5. **顺序铁律**：先 WAL 落盘成功，后推进游标——违反会漏数据

### 6.2 落库与 ACK（Receiver）

- 去重：`seq ≤ last_seq` 直接 0xff。**last_seq 只按连续前缀推进**（seqTracker）：
  乱序完成的帧记入 pending，绝不越过在途帧——否则该帧失败后的重传会被吞掉（N2）
- 缺口闭合（N6）：新连接首帧 = sender WAL 头（服务端读序分配 frameIdx，确定性）；
  首帧 seq > last+1 时闭合缺口（该区间 sender 已 Commit、0xff 已保证落库），
  双保险：缺口区间必须全部不在途（引用计数），防多 sender 误配
- 大跳跃（>100000）：接受处理（幂等覆盖安全）；拒绝会造成停等死锁
- 写库：解压后 raw 直接 WriteRaw（不拆行）；超时按批大小动态（10s+1ms/行，120s 封顶）
- 失败分类：HTTP 4xx 永久 → DLQ 隔离 + 0xff；其余 → 0x00 重试
- 心跳（seq=0）：按类型先于去重处理，不写库不推进 last_seq

### 6.3 落盘与确认（WAL + Sender）

- **group commit**：每轮 poll 全部帧一次 fsync（先 WAL 后游标不变）
- checkpoint **节流 1s/次**（Commit 路径）；SetCursor/段删除/Close 立即持久化
- 发送：停等（默认）；0x00/超时指数退避（1s→60s）**永不丢弃**；毒丸由 receiver
  DLQ 解卡；空闲心跳 30s；空闲等待由 WAL append 通知唤醒（替代 200ms 轮询）
- 滑窗（pipeline_window>1，实验项）：0x00 视为连接级失败→重连→go-back-N 重发
  （N1 修复，杜绝陈旧 ACK 错位提交）

### 6.4 组帧（性能关键路径，V1.4）

- schema/series 级**键序缓存**（注入 Point，序列化零排序）
- 包级转义循环 + strconv.Append* + []byte 拼装（替代 NewReplacer×4/Sprintf×3）
- 实测：32900ns/85 分配 → **544ns/2 分配**（~60x CPU）
- schema 发现：按 measurement single-flight + 成功缓存 1h；失败复用历史成功条目
  + 30s 负缓存（类型不漂移）；显式 tag_columns 可完全绕开发现

### 6.5 中继（V1.3）

receiver 写库成功后，原始 Line Protocol 追加独立转发 WAL（独立 seq），复用标准
Sender 发往下一跳。B 写库成功即 ACK 上游；转发异步缓冲（重启不丢）；毒丸不转发；
转发 WAL append 失败落中继 DLQ。详见 [relay.md](relay.md)。

## 7. 可靠性论证（At-Least-Once 提纲）

1. **发送侧不丢**：Poller 查询成功后帧必先落 WAL（fsync）才推进游标；
   崩溃后游标 ≤ 已落盘数据 → 窗口重查补回。
2. **传输侧不丢**：sender 只在 0xff 后 Commit；0x00/超时重发；重试上限只告警不丢弃。
3. **接收侧不丢**：写库成功才 0xff 并推进 last_seq；瞬时失败不推进不登记去重
   （V1.4 修复 LRU 吞重试 P0）；并发乱序完成由连续前缀推进保护；缺口闭合仅
   覆盖 sender 已 Commit 区间。
4. **重复无害**：Influx 按 (measurement,tags,field,timestamp) 幂等覆盖；
   DLQ 隔离帧重放同路径幂等。
5. **已知边界**（文档明示，均不破坏红线）：
   - sender WAL 目录被删除 = 数据源不可恢复（禁止操作）；
   - 多 sender 共用一个 receiver 端口 = 误配（seq 空间冲突）；
   - 中继 DLQ 落盘本身失败（磁盘满）= 该帧无法恢复（一级防护失效场景）；
   - 位反转破坏 WAL 中段 = 跳一帧（日志可见，其余帧完好）。

## 8. 实测与边界

| 场景 | 结果 |
|---|---|
| 20 万点/s × 60s | 1200 万点实时零丢失、零重试 |
| 目标 InfluxDB 重启 / 链路中断恢复 | 自动重连断点续传追平 |
| 毒丸注入 | DLQ 隔离，主通道不阻塞 |
| 中继正常 + C 断连恢复 | 双库零丢失，恢复自动补发 |
| 批量同时间戳（dense ts） | OFFSET 分页防卡死 |
| WAL 撕裂尾 / 中段坏帧 | 截断恢复 / 跳帧重同步（日志可见） |

与 hx_migrate 对比：吞吐 20 万 vs ~10-12 万点/s；带宽 2.5 vs 47.8 Mbps（5 万点/s）；
断链自动重连 vs 需手动重启；可回填历史 vs 仅实时订阅。
