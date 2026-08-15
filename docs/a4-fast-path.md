# A4：订阅 fast-path 透传（实时性增强）设计方案

> 状态：设计评审稿（V1.5.0 实施）
> 目标：在**保持 WAL 轮询同步为零丢失正确性基座**的前提下，利用源库 SUBSCRIPTION 推送把
> 端到端延迟从 `watermark + ~2.2s`（当前 ~4.2s）降到 `订阅 flush 间隔 + 链路传输`
> （约 0.1s~1s），解决"WAL 同步不够实时"的既有短板。

## 1. 动机与量化目标

| 指标 | 现状（V1.4.3，纯轮询） | A4 目标 |
|---|---|---|
| 端到端延迟（写入源库 → 目标库落库） | watermark(2s) + 处理 ~2.2s ≈ **4.2s** | **0.1~1s**（订阅 flush 间隔 + 传输 + 写库） |
| 零丢失 | 已保证（轮询 + 游标 + WAL） | **不变**（快路径只加速，正确性仍由轮询兜底） |
| 吞吐 | 20 万点/s | 不低于现状；快路径批次直透传，省一次源库查询 |

## 2. 核心架构：双路径重叠 + 单一 WAL 基座

```
[源 InfluxDB] ── SUBSCRIPTION 推送(原始 LP 批次) ──▶ [FastPath 转发器] ─┐
       │                                                              │
       │ 轮询查询(epoch=ns)                                            ▼
       └──────────▶ [Poller: 窗口查询 → 边界去重 → 快路径去重过滤] ──▶ [同一 WAL]
                                                                         │
                                                        seq 连续、停等/滑窗发送
                                                                         ▼
                                                    [Receiver: 去重 → WriteRaw → ACK]（零改动）
```

- **历史同步**：由 Poller 全权负责（快路径永远不碰历史——订阅只推送**新写入**）。
- **实时同步**：由 FastPath 负责最近 `[cursor, now]` 的窗口，直接透传订阅批次。
- **衔接点**：两路径共享**同一条 WAL 与同一 seq 空间**——目标侧看到的帧流始终 seq 连续，
  不存在"切换点"；目标库为时序库，按时间戳乱序/覆盖写入天然安全（此前"有序"红线指
  链路帧序，快路径不破坏帧序）。
- **快路径不推进游标**：cursor 仍只由 Poller 推进（先 WAL 后游标铁律不变），快路径只往
  WAL 追加帧。

## 3. 启用时机门控（历史→实时衔接的核心，对应评审意见）

**原则：只有 WAL 同步追平到接近实时后，快路径才开始转发数据。** 在此之前快路径仅扮演
信号角色（等同于现有 signal_listen，且可完全替代它）。

### 3.1 状态机（自动 + 迟滞 + 手动强制）

```
                cursor 年龄 ≤ activate_age（默认 watermark+3s）
   ┌────────┐ ───────────────────────────────────────────────▶ ┌────────┐
   │ WAITING │                                                  │ ACTIVE │
   │(仅信号) │ ◀─────────────────────────────────────────────── │(透传)  │
   └────────┘     cursor 年龄 > deactivate_age（默认 watermark+30s，迟滞防抖）└────────┘
```

- `mode`：`off`（永不转发，纯信号，= 今天的行为）/ `auto`（默认，按上表门控）/ `on`（强制转发，供人工确认后使用）。
- 判定依据为**数据时间戳游标**的年龄（`now - cursor`），由 Poller 每轮 `pollOnce` 用最新
  cursor 更新；迟滞区间防止游标在阈值附近抖动导致开关翻转。
- WAITING 状态下推送批只触发 `notify`（唤醒 Poller 立即查询）+ 计数 `fast_path_signal_only`，
  **不写 WAL、不登记去重**——因此回填阶段零影响。
- 典型时序：首次部署 backfill=0 → cursor 初始化在 now-watermark-backfill → 立即满足
  activate 条件 → 启动即 ACTIVE（本来就是近实时起点）。带历史回填（backfill=7d）→ 快路径
  保持 WAITING，Polling 逐窗追平，**追到近实时时自动切入 ACTIVE**，无需人工干预。

### 3.2 衔接正确性论证（切换全程零丢失）

- 快路径**只在 WAL AppendBatch 成功之后**才把点登记进去重集 → "被 Poller 跳过 ⟹ 已实际
  转发"恒成立。
- 订阅推送不可靠（fire-and-forget，失败即丢）：**丢掉的推送从不登记** → Poller 必然
  补发 → 零丢失。
- WAITING 期推送不转发也不登记 → 该时段数据全部由 Poller 转发 → 零丢失。
- ACTIVE 期两路径窗口重叠（快路径覆盖 `[cursor, now]`，Poller 覆盖
  `[cursor, now-watermark)`）→ 重叠区由去重集抑制二次转发；集因驱逐/重启丢失条目只会
  造成**重复转发**（目标库幂等覆盖，计数不重复）——快路径的一切退化方向都是"退化回
  轮询"或"重复"，**不存在丢数据方向**。

## 4. 关键设计决策

| 决策 | 内容 | 理由 |
|---|---|---|
| 订阅事实约束 | InfluxDB 1.x **每库仅允许 1 个 subscription**；A4 复用现有订阅、把 fast_path 地址加为**第二个 DESTINATION**（`DESTINATIONS ALL 'signal','fastpath'`），需 ops DROP+CREATE（几秒推送中断，无数据影响） | 不能新建第二个订阅（会报 "subscription already exists"） |
| 精度 | 快路径只接受 **ns 精度**批次（每行 ts 需落在 `[1e15, 5e18)`）；其余跳过整行由 Poller 兜底 | ns/µs 数值域重叠（1.75e15 既是 ns-1970 也是 µs-2025），无法可靠自动判别；本部署写入方统一 ns |
| 批大小 | body 上限 = `MaxDecompressedLen`(16MB)；压缩后超 1MB 帧限 → 整批跳过由 Poller 兜底 | 复用协议现有限额 |
| 行级容错 | 批内**逐行**解析：坏行/非 ns 行/非目标 measurement 行跳过，其余行仍转发；跳过的行由 Poller 兜底 | 单行坏数据不拖累整批实时性 |
| measurement 过滤 | 复用 `sync.measurements` 配置，逐行过滤 | 与轮询路径语义一致 |
| 反压联动 | 快路径每批检查 WAL 盘占用：≥ 黄线(60%) 即丢批（由 Poller 兜底，延迟退化回轮询） | 防止快路径在链路故障期把 WAL 打满 |
| WAL 并发追加 | `AppendBatch` 改为**内部分配 seq**（重写帧头 seq 字段，CRC 只覆盖 payload 不受影响），返回分配 seq | 消除 Poller 与 FastPath 并发追加时 NextSeq 读-用间隙的 TOCTOU；Poller 组帧逻辑同步简化 |
| 去重集 | 秒级分区 + series 紧凑 ID（`series map[string]uint64`，条目 = `id<<30 | ts%1e9` 精确键）+ cursor 驱逐（分区秒 < cursor 即删） | 零碰撞零丢失；内存 = 保留窗口（默认 watermark+5s）内的点数，20 万点/s 下 ≈ 100 万条目 ≈ 32~48MB |
| 去重登记时序 | WAL 成功后才登记 | 见 §3.2 证明 |
| 快路径批即信号 | 每批处理后调用现有 `Poller.Notify()`（cap=1 合并语义） | 保留 Poller 事件驱动能力；`signal_listen` 可保留兼容，但被 fast_path 取代后建议下线 |
| Receiver/中继 | **零改动** | 快路径帧与轮询帧完全同构（协议 Type=1 数据帧、同一 seq 空间） |
| 时钟 | 激活判定依赖 sender 时钟与数据 ts 的对齐，文档要求 NTP；阈值可配 | 避免时钟偏移误判 |

## 5. 组件与改动清单

| 组件 | 改动 |
|---|---|
| `internal/wal` | `AppendBatch(typ, frames [][]byte) ([]uint64, error)` 内部分配 seq；`AppendEncoded` 不变（relay 用） |
| `internal/model/lineparse.go`（新增） | `ParseLine(line []byte) (meas string, tags [][2]string, ts int64, ok bool)` 零分配倾向解析（转义 `\,` `\=` `\ `、引号字符串字段、末 token 时间戳）；`SeriesKey(meas, tags) string` 规范化键（与 `Point.Key()` 前缀同构，供两路径共用） |
| `internal/sender/fastpath.go`（新增） | FastPath 转发器 + 秒级分区去重集 + WAITING/ACTIVE 状态机 + 反压门 |
| `internal/sender/poller.go` | `pollOnce` 过滤点集（查去重集，命中跳过）；每轮更新快路径 cursor/状态 |
| `internal/config` | `sync.fast_path: {listen, mode, activate_age, deactivate_age, dedup_window}` |
| `cmd/sender` | 启动 FastPath HTTP server（listen 非空时）；注入 notify/WAL/metrics |
| `internal/monitor` | 新增指标（见 §7） |
| 文档 | 本文档 + configuration.md + README 版本记录 + architecture.md 数据流 + 部署订阅改造步骤（AGENT.md 上线待办同步） |

## 6. 配置示例

```yaml
sync:
  fast_path:
    listen: ":18097"        # 订阅推送地址（空=禁用，退化为纯轮询+旧 signal_listen）
    mode: auto              # off=仅信号 / auto=追平自动启用（默认） / on=强制转发
    activate_age: 5s        # cursor 年龄 ≤ 此值才启用（默认 watermark+3s）
    deactivate_age: 30s     # 年龄 > 此值退回仅信号（迟滞，防抖）
    dedup_window: 15s       # 去重集保留窗口（默认 watermark+5s）
```

源库订阅改造（ops，一次性）：

```sql
-- 现有订阅上追加 fast_path 目的地（Influx 每库仅一个订阅，必须重建）
DROP SUBSCRIPTION hx_sub ON HXScada.autogen
CREATE SUBSCRIPTION hx_sub ON HXScada.autogen DESTINATIONS ALL
  'http://<103>:18098',   -- 原 signal_listen（可保留）
  'http://<103>:18097'    -- A4 fast_path
-- [subscriber] flush-interval 调小以换取更低延迟（源库配置，默认 1s，建议 100~200ms）
```

## 7. 指标

`fast_path_state`（0=off/1=waiting/2=active）、`fast_path_batches_total`、
`fast_path_points_total`、`fast_path_signal_only_total`（WAITING 期）、
`fast_path_dropped_oversize|precision|backpressure_total`、`fast_path_line_skipped_total`、
`fast_path_dedup_hits_total`（Poller 侧命中）、`fast_path_dedup_entries`（集大小）。

## 8. 测试计划

- 单元：LP 解析（转义/引号串/缺 ts/非 ns 数值）、SeriesKey 与 Point.Key 一致性、
  去重集（分区驱逐/打包键）、WAL AppendBatch（并发追加 seq 唯一连续、帧可解码）、
  状态机（activate/deactivate 迟滞/mode 三态）。
- e2e：假订阅推送 + 假源查询 + 假目标——① ACTIVE 下推送批落库且 Poller 窗口去重后不再
  重发（目标写计数=1）；② WAITING（游标落后）下推送不转发、Poller 照常补发零丢失；
  ③ 订阅丢弃（推一半断连）→ Poller 补齐零丢失；④ 反压丢批 → Poller 补齐。
- `go test -race` 全绿；Benchmark 守卫（解析 + 去重集吞吐 ≥ 20 万点/s 单核）。

## 9. 上线步骤（灰度）

1. 源库 DROP+CREATE 订阅加目的地、调 flush-interval；
2. 部署 sender（`fast_path.listen` 打开，mode=auto）；观察 `fast_path_state`：回填期应为
   waiting，追平后自动 active；
3. 对比 `sync_e2e_delay_seconds` 从 ~4.2s 降到 <1s；`dup_total`/`fast_path_dedup_hits`
   验证去重生效；
4. 回滚：`mode: off` 或清空 listen 即退回纯轮询，无数据面影响。

## 10. 带宽与压缩分析（FAQ）

**快路径不牺牲压缩**：推送批次在 sender 侧同样经过 `protocol.Encode` 编码后才进入
WAL/隔离链路，与轮询路径压缩管线完全一致。

**V1.6 起支持 zstd**：帧类型即压缩算法标识（TypeData=0x01 gzip / TypeDataZstd=0x04
zstd，Version 仍=1、帧布局不变），接收端按类型自动解压、中继按类型透传，零协商成本。
`tcp.compression: zstd|gzip`（默认 zstd）——zstd(SpeedFastest) 压缩/解压显著快于
gzip(BestSpeed)，且对 LP 类高重复文本压缩率更高（本机 5 万点/s 实测：链路 0.6Mbps
vs gzip 2.4Mbps）。注意：zstd 帧需两端同版本（同包部署满足）；混合版本升级期
把 `tcp.compression` 设回 gzip 即可。

| 段 | 承载 | 压缩 | 影响 |
|---|---|---|---|
| 源库 → sender（订阅推送，I 区内部 LAN） | 原始 LP 明文 | 无 | 不占隔离链路；与现有 signal_listen 收取并丢弃的 body 相同，只是开始利用内容 |
| sender → 隔离装置 → receiver | gzip/zstd 帧 | ~19x(gzip)/更高(zstd) | 与轮询路径一致；重叠窗口由去重集抑制二次转发 |
| 快路径组帧粒度 | 客户端写批次（如 5000 点 ≈ 400KB） | 压缩率与轮询大帧基本持平 | 可忽略 |

订阅推送的源库复制开销（每个写请求推一份 body）是 SUBSCRIPTION 机制固有成本；可调大
`[subscriber] write-buffer-size` 减少推送频次（以 flush 延迟为代价）。

## 11. 不做的事（二期范围外）

- 不解析订阅 payload 做对账以外的任何业务处理；不做快路径独立 seq 空间/独立链路；
- 不把游标推进权交给快路径；不支持 ns 以外精度（Poller 兜底）。
