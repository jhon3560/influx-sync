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
