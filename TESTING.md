# 测试与验证（TESTING）

本文档汇总本项目的测试依据、测试方法与实测结果。全部证据材料在 `docs/` 目录
（审计报告 audit-*.md、基准报告 bench-2026-08.md、真机实测记录 deployment-hxjh-2026-08-16.md）。

## 1. 测试体系

| 层 | 内容 | 位置 |
|---|---|---|
| 单元测试 | 11 个包全量单元/集成测试,含竞态检测(`go test -race`) | `internal/*/` |
| 端到端测试 | 完整链路 + 断连恢复 + 重启恢复 + 全量回填 + 同值不回拨 + 重爬幂等 | `internal/integration/e2e_test.go` |
| 基准守卫 | 组帧/Key/去重基准,防止性能回退 | `internal/model/bench_test.go` |
| 审计 | 四轮交叉审计(N9-N16、R1-R3),缺陷+修复+回归测试闭环 | `docs/audit-*.md` |
| 真机验证 | 隔离装置实机全链路(见 §4) | `docs/deployment-hxjh-2026-08-16.md` |

## 2. 运行测试

```bash
go test ./...                  # 全量单元+集成
go test -race ./internal/...   # 竞态检测
go test ./internal/model/ -bench . -benchmem   # 基准
```

## 3. 单元测试结果(最近一轮全量)

```
ok  influx-sync/internal/config       0.019s
ok  influx-sync/internal/influx       0.040s
ok  influx-sync/internal/integration  3.572s
ok  influx-sync/internal/logger       0.011s
ok  influx-sync/internal/model        0.007s
ok  influx-sync/internal/monitor      0.014s
ok  influx-sync/internal/protocol     0.179s
ok  influx-sync/internal/receiver     0.115s
ok  influx-sync/internal/sender       29.820s
ok  influx-sync/internal/transport    0.318s
ok  influx-sync/internal/wal          1.365s
```

`go test -race ./...` 全绿(11 包)。

## 4. 真机隔离装置验证(V1.8.0)

环境:源 InfluxDB 1.8(恢复的 21GB 生产库)→ 正向隔离装置(多端口透明转发)→
目标 InfluxDB(麒麟 V10)。详见 docs/deployment-hxjh-2026-08-16.md。

| 验证项 | 结果 |
|---|---|
| 全链路一致性 | 冒烟 20000/20000 点逐位一致;已同步区间 COUNT/SUM(含浮点位级)源/目标完全一致 |
| 快路径实时性 | 订阅透传端到端 <1s |
| 滑窗装置兼容性 | 本装置支持多帧在途:pipeline_window=8 全程 ack_fail=0;速率停等 0.63 → 滑窗 8 0.92 天/分钟 |
| 停等 RTT | ~1.5s/帧实测 |
| 断点续传 | 回填中断后从 WAL checkpoint 续传,无需重新导入 |
| 环境恢复 | 测试后对端数据库/端口/目录 100% 还原 |

## 5. 性能基准(实测)

| 项 | 结果 |
|---|---|
| 组帧 LineProtocol | 544 ns/点、2 次分配(优化前 32900 ns/85 次分配,~60×) |
| Point.Key() | 352 ns/点(优化前 684 ns) |
| 带宽 | gzip/zstd 压缩约 19×:5 万点/s 实测 ~2.5 Mbps |
| 回填吞吐 | 真机 0.41→0.76 天/分钟(N16 预取流水线);瓶颈=源库胖行查询,链路余量 ~12× |

## 6. 正确性保障设计(审计闭环)

At-Least-Once 红线:WAL 先落盘后推进游标、0xff 才 Commit、last_seq 连续前缀推进、
缺口由新连接首帧闭合、快路径一切退化方向只造成重复转发或退回轮询(幂等覆盖兜底)。
四轮审计发现的全部缺陷(含 2 个 P0、3 个 OOM、1 个静默丢点)均已修复并配回归测试。
