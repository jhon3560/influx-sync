# 代码审计与修复记录（2026-08）

基于 V1.3 代码的全面审计，共发现 3 个严重问题、4 个中等问题、6 个建议改进与若干小项，
全部在 V1.3.1 修复（含回归测试）。

## 严重问题（P0）

| # | 问题 | 修复 |
|---|---|---|
| 1 | WAL.MoveToDLQ 帧读取偏移漏加 recordHeadLen（4 字节），DLQ 内容错位不可解析 | 偏移修正 + 测试补强（DLQ 帧可完整 Decode 且 payload 一致） |
| 2 | Receiver 对超大 seq 跳跃回 AckFail → Sender 停等重发同一帧 → 链路永久卡死（last_seq 文件丢失场景） | 改为接受处理（Influx 幂等覆盖保证安全），仅告警；测试同步更新 |
| 3 | appendFrames 组帧编码失败（压缩后 >1MB / 原始 >16MB）无拆批 → 游标永久卡死 | appendBatch 对超限帧减半拆批重试；isFrameTooLarge 判定 + 测试 |

## 中等问题（P1）

| # | 问题 | 修复 |
|---|---|---|
| 4 | Decompress 用 LimitReader 静默截断（>16MB 帧返回截断数据，末行损坏） | LimitedReader 多读 1 字节检测超限，报错拒绝 + 测试 |
| 5 | NaN/Inf 字段值 `%v` 输出 → InfluxDB 400 → 整帧毒丸 | 跳过 NaN/Inf 字段；全 NaN 点整点跳过 + 测试 |
| 6 | 字符串字段只转义 `"` 不转义 `\` → 行解析破坏 → 整帧毒丸 | 同时转义反斜杠与引号 + 测试 |
| 7 | 写库固定 30s 超时，大 batch 高压下假失败重发 | 按批大小动态超时（10s + 每行 1ms，封顶 120s） |

## 建议改进（P2）

| # | 项 | 修复 |
|---|---|---|
| 8 | epoch=ns 时间戳经 float64 有 ±256ns 精度损失 | json.Number 解析（UseNumber + tsToInt64），时间戳保真 |
| 9 | Poller wakeupScheduled 数据竞争 | atomic.Bool |
| 10 | queryParallel 注释称"按段序"实际乱序归并 | 收集后按 idx 排序，输出时间升序 |
| 11 | SendFrame 未处理部分写 | 循环写全量 |
| 12 | last_seq 每帧持久化（高频文件 IO） | 每秒最多一次节流；崩溃窗口由重发+幂等兜底 |
| 13 | 黄色降速 Sleep(5s) 阻塞主循环 | time.After + select（可响应退出） |

## 小项

- metrics.mu 未使用字段删除
- Encode 先校验 payload 大小再建 gzip writer（顺序优化）
- sender/receiver 的 metricsSrv.Shutdown 增加 5s 超时

## 验证

- `go build ./...` / `go vet ./...` 通过
- `go test ./...` 全部通过（新增回归测试：DLQ 偏移、解压超限、NaN/转义、拆批、seq 跳跃新语义）
- `go test -race ./internal/...` 全部通过

---

## V1.4 修复记录（2026-08-14，外部审计报告 + 修复实施）

审计报告覆盖 3709 行代码 + 全套文档，实测热点路径（组帧 329ms/万点、747MB 分配）。
全部问题经代码逐项确认属实后修复；另发现审计未覆盖的 1 个 P0 数据丢失缺陷。

### 审计报告之外发现的 P0（实测复现）

| # | 问题 | 修复 |
|---|---|---|
| X1 | Receiver 的 LRU 去重在**写库之前**登记 seq：瞬时失败（500）回 0x00 → Sender 重发同 seq → 被 LRU 吞掉直接回 0xff → 上游删除 WAL → 数据永久丢失（实测：writes=1, ack=ff） | 去重登记移到写库成功之后；`TestTransientFailureRetryNotSwallowed` 回归测试 |

### 审计报告 P0 项

| # | 报告项 | 修复 |
|---|---|---|
| C1 | WAL 撕裂尾部（头完整+帧体撕裂）→ Open 失败进程起不来 | indexSegment 首个无效记录视为撕裂尾：截断 + Error 日志 + 继续（`TestTornTailRecovery`/`TestTornHeadRecovery`） |
| C2 | 中继 WAL append 失败仅记日志仍回 0xff → 对下一跳永久丢失 | 失败时 raw lines 落中继专用 DLQ（relay.dlq_dir，默认 <wal_dir>/../relay_dlq）+ relay_dlq_total 指标（`TestRelayAppendFailureGoesToDLQ`） |

### 审计报告 P1 性能项（实测基准）

| # | 报告项 | 修复 | 实测收益 |
|---|---|---|---|
| P1 | LineProtocol sort×2 + NewReplacer×4 + Sprintf×3 | 包级转义循环 + strconv.Append* + 缓存键序 + []byte 拼装 | 32900ns/85 分配 → **544ns/2 分配**（~60x CPU / ~40x 分配） |
| P2 | 每点 Key()（684ns/7 分配）且查询路径调用两次 + 双份全窗口 map | Key 零分配化（352ns/2 分配）+ QueryRange/queryParallel 改为仅边界 ts 去重（小集合线性比较，零分配） | 全窗口 map 与双份 Key() 消除 |
| P3 | checkpoint 每次 ACK 全量持久化（2 fsync+rename+目录 fsync，5.4ms/次） | Commit 路径节流 1s/次；段删除/SetCursor/Close 立即持久化（`TestCheckpointThrottled`） | 省 ~110ms/s 盘 IO |
| P4 | 每帧 fsync（3.3ms/帧）且持锁阻塞 Peek | AppendBatch group commit：每轮 poll 一次 fsync；先 WAL 后游标不变（`TestAppendBatchGroupCommit`） | 帧率翻倍不再是瓶颈 |
| P5 | 解压→拆行→拼回往返（1.3ms/帧 + 1.85MB 分配/帧） | influx.WriteRaw 直接整体写入；splitLines 仅失败/DLQ 路径使用 | 热路径零拆拼 |
| P6 | 默认 http.Client（MaxIdleConnsPerHost=2 < poller_parallel=4）+ 固定 10s 全局超时 | Transport 调优（16/主机）；查询按窗口动态 ctx 超时（30s+2×窗口，10min 封顶）；写库保留动态超时 | 大窗口回填不再假失败 |
| P7 | ensureSchema 持全局锁做网络请求 + 失败缓存空 schema 1h | 按 measurement single-flight；失败不缓存并报错（游标保持重试） | 并发 worker 不再互阻；类型不再错 1 小时 |

### 架构/实时性项

| # | 报告项 | 修复 |
|---|---|---|
| A1 | 停等协议吞吐上限（batch/RTT） | 滑窗实现（sender.pipeline_window，默认 1=停等；go-back-N）；**开启前须与隔离装置确认同连接多请求在途**（`TestSenderPipeline`/`TestSenderPipelineGoBackN`） |
| A2 | Receiver ACK 路径全串行，写库 RTT 计入链路 RTT | 每连接流水线：并发 handler + 按序 ACK（0xff=已落库语义不变，wire 兼容停等 sender）（`TestServerOrderedAckUnderConcurrency`）；OrderedSeq 模式 last_seq 只推进连续前缀，防跳序吞重试 |
| A3 | 订阅信号忙时丢弃 + 空闲 Sleep(200ms) 首帧延迟 | pending-flag + MinSignalInterval 延迟触发；WAL append 通知通道唤醒 Sender 空闲等待 |
| A5 | 无端到端延迟指标 | receiver 提取帧末点时间戳 → sync_e2e_delay_seconds |

### 其余修复

| # | 项 | 修复 |
|---|---|---|
| C3 | classifyWriteError 解析错误文案 | influx.WriteHTTPError typed error + errors.As（字符串解析仅兼容路径） |
| C4 | batch=1 单点超限 → 游标永久卡死 | 跳过该点 + point_skip_total 计数（`TestAppendBatchSplitsOnTooLarge`） |
| C5 | relay.timeout 配置从未生效 | 硬编码 10s 改为 cfg.RelayTimeout() |
| 🟢 | 心跳复用数据 seq 空间 | 心跳 seq 固定 0 |
| 🟢 | SignalListener 不读 body | io.Copy(io.Discard, LimitReader 1MB) 后再 204 |
| 🟢 | monitor HTTP 无超时 | ReadTimeout 15s / ReadHeaderTimeout 5s |
| 🟢 | tcp_server 无连接上限 / 每帧 2 次 make | max_conns 配置 + 单次分配 Header+Payload |
| 🟢 | config.dur 静默回退 / 负 interval 不校验 | Validate 严格校验（解析失败/负值报错） |
| 🟢 | MinSignalInterval 未从 YAML 传入 | signal_min_interval 接线 + 测试 |
| 🟢 | Sender 重试每次 Peek 重读盘 + 分配 1MB | 最后 Peek 帧缓存（同 seq 复用） |
| 🟢 | 组帧 benchmark 回归守卫 | BenchmarkLineProtocol/LinesToProtocolBytes/PointKey/PointsEqual |

### 基准实测（Ryzen 4800H，V1.4 修复后）

```
BenchmarkLineProtocol-16          544.4 ns/op    256 B/op   2 allocs/op   （修复前 ~33µs/85 allocs）
BenchmarkPointKey-16              351.6 ns/op    144 B/op   2 allocs/op   （修复前 684ns/7 allocs）
BenchmarkPointsEqual-16            14.0 ns/op      0 B/op   0 allocs/op
BenchmarkLinesToProtocolBytes-16  5.84 ms/万点    5.2MB/万点
```

全部测试（含 -race）通过；新增回归测试 20+ 个。

---

## V1.4.1 修复记录（2026-08-14，滑窗缺陷复审 N1-N5）

V1.4.0 交付后复审发现滑窗路径 1 个 P0 + 2 个 P1 + 2 个 P2 + 6 个小项，全部修复并补回归测试。

### N1（P0，实测复现）：滑窗 go-back-N 陈旧 ACK 错位 → 提交从未写库的帧

第 1 轮 i+1..W-1 帧的陈旧 0xff 残留在线上，0x00 后重发被误读为"重发帧的 ACK"，
f1 未写库却被提交删除（At-Least-Once 被破坏），随后退化为 commit out-of-order 死循环。

**修复（方案 2）**：0x00/非法 ACK/发送失败一律视为**连接级失败**——关闭连接重连后
从 nackAt 起重发尾窗（重连天然清空陈旧 ACK 流；receiver 幂等写入 + 连续前缀
lastSeq 去重保证不重复计数）。

回归测试：`TestSenderPipelineNackFrameNeverCommitted`（seq=1 永远 0x00 → pending 恒
保持 4、ack_ok 恒为 0、seq1 nack ≥3）。修复前该测试 pending 会掉到 0。

### N2（P1）：OrderedSeq 门控与服务端实际窗口不一致

`OrderedSeq = max_inflight > 1` 用原始配置值（默认 0→false），而服务端把 0 默认成 8
→ 默认部署是"流水线服务端 + 非按序推进"，存在跨连接乱序完成吞重传帧的丢失路径。

**修复**：seqTracker **恒开**（删除 OrderedSeq 配置项）。停等 sender 下前缀必然完整
推进、行为不变；小缺口不再推进 last_seq，正确性由幂等重写兜底
（`TestSeqSmallJumpAllowed` 更新为连续推进语义）。

### N3（P1）：bit-rot 中段损坏截断整个尾部

**修复**：indexSegment 遇无效记录先**向前重同步**找下一个合法帧头（记录长必须与
帧头 Length 一致，`[u32 len] = HeaderSize + payload`）——跳过单帧而非整尾；重同步
失败（真撕裂尾）才截断。`TestMidSegmentCorruptionSkipsSingleRecord`：坏帧跳过、
前后帧完好、文件未被截断。

### N4（P2）：seqTracker 溢出跳越的吞帧地雷

**修复**：pending 超限时**只推进连续前缀** + 清空 pending（响亮 Error 日志），绝不
跳越 last_seq；被清空的帧重传时幂等重写，正确性不受影响。

### N5（P2）：ensureSchema 严格化可致同步永久停摆

**修复**：元查询失败**不传播、不停摆**——降级为类型推断兜底（v1.3.1 行为）+ 30s
负缓存短 TTL 后自动重试发现（成功条目仍 1h）。
`TestSchemaDegradeOnMetaFailure` / `TestSchemaRecoversAfterMetaHeals`。

### 小项

| 项 | 修复 |
|---|---|
| dedup.CheckAndAdd 返回值被丢弃（LRU 废状态） | LRU 删除；新增 recv_inflight 在途帧指标 |
| DLQ 文件名同 seq 秒内重试互相覆盖 | 文件名加 UnixNano 后缀 |
| tcp_server ackDone 死代码 | 删除 |
| "零分配"注释与实测不符 | 修正为"低分配"（Key 实测 2 allocs） |
| runPipeline 每轮 go-back-N 重读盘 W×1MB | 窗口内只 PeekBatch 一次；nack 后尾窗复用不 refill |
| 中继 DLQ 二级失败仅记日志 | relay.md 明示降级边界 |

全部测试（含 -race）通过。

---

## V1.4.2 修复记录（2026-08-14，复审 N6-N8）

### N6（P1，N2 引入的操作级回归）：永久小缺口冻结 last_seq + 逐帧告警刷屏

last_seq 文件按 1s 节流持久化，每次正常重启都会产生 ≤ 上次节流点的永久缺口；
seqTracker 连续前缀推进下 last_seq 永久冻结、后续每帧命中 seq jump Warn 分支
（~7 行/秒，58 万行/天）、"seq≤lastSeq" 去重完全失效（正确性由幂等重写兜底）。

**修复（发送方权威闭合）**：新连接首帧 = sender WAL 头（tcp_server 读帧循环按序
分配 frameIdx，心跳不计入，确定性免竞态）；若 seq > last+1，[last+1, seq-1] 在
sender 侧已 Commit（"0xff=已落库"保证它们完成于旧进程，不可能在途）→ 安全
markDone 闭合，last_seq 恢复连续推进、去重恢复、告警刷屏消失。
双保险：缺口区间必须全部不在途才闭合（防多 sender 误配病态场景）。
重复 seq jump 告警降级为 Debug（首条保留）。

回归测试：TestGapClosedOnRestartWithStaleFile / TestGapNotClosedMidConnection /
TestGapNotClosedWhileInFlight（阻塞式假库钉住"在途"状态）。

### N7（P2，历史遗留）：新 WAL 首帧 seq=1 而非 0

确认属实（NextSeq 空索引时被顶升到 1）。**不改**：data seq=0 会被 receiver 首帧
去重（seq≤lastSeq=0）直接吞掉，且 seq=0 已被心跳占用（按类型先于去重检查）。
已文档化：seq 从 1 开始是有意行为。

### N8（P2，N5 残余）：降级期 tag 列写成 string field → 恢复后类型冲突毒丸

**修复**：元查询失败时若存在该 measurement 的上一份成功 schema（即使已过期），
**复用旧 schema**（类型不漂移、不产生毒丸），degraded 短 TTL 30s 后重试发现；
无历史时才退到类型推断。文档同步：tag_columns 显式配置列为推荐项（完全绕开
此路径）。
回归测试：TestSchemaReuseLastGoodOnMetaFailure。

全部测试（含 -race）通过。
