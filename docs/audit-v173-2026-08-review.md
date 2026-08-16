# V1.7.3 复审报告（2026-08-16，对 audit-v173 R3a/R3b 修复的复核）

> 审计人：副审计员（reviewer）。对象：主实现方对 R3a/R3b 的修复（tag v1.7.3，
> commit adf6f06 + release 二进制 a4d8fe7）。
> 基线：全量 `go test -count=1 ./...`（11 包）+ `go test -race` 全绿；
> 二进制 `vcs.revision=adf6f06, modified=false` 指向 tag 本体；
> 包 SHA256 `af284600d569e6c071c842589a11c0a3199ecf48987c855e1f36ff00025c0bf3`。

## 结论

**R3a/R3b 修复正确，本轮闭环成立。** 我用自己的取证测试独立复验：

| 验证 | 结果 |
|---|---|
| R3a：同 (series, ts) 重复登记后 `refs==1`（不再双计） | ✅ 实测 `refs after duplicate add: 1 (want 1)` |
| R3a：全驱逐后 series 名归零（无泄漏） | ✅ |
| R3b：20 万 unique series 全驱逐后 `series=0 idRefs=0 idName=0` 三映射全归零 | ✅ 实测 |

修复方式与我在 audit-v172 §4 的建议逐字一致（极小改动方案），回归测试
（DuplicateRegistrationNoLeak / HighCardinalityFullyBounded / SeriesBounded 增补）
把我上轮的取证用例固化为永久测试，交接质量好。

## 深度复核（未发现新问题）

- **R3a 的 `packed` 计算**：`id<<30 | offset`，id 上限 1<<34 → id<<30 < 2^64 无溢出 ✓；
- **R3b map 化后的清理对称性**：Add 建键（series/idName 同步）、evict 删键（series/
  idName/idRefs 同步删），无残留路径 ✓；
- **`nextID` 单调不回收**：uint64 空间下 200k/s 可用 ~290 万年，非问题 ✓；
- **1<<34 拒绝登记兜底**保留，退化方向=重复转发（零丢失语义不变）✓。

## 至此 V1.7 系列四轮审计全部闭环

N9（点条目驱逐）→ R1（series 名清理）→ R3b（逆向索引 map 化）→ R3a（引用计数
精确化）：fastDedup 四层映射（秒分区/名称/逆向/引用）全部有界或随驱逐收敛，
病态高基数场景的 OOM 与泄漏路径全部封死。部署包可直接用于升级验证。

（本报告依 AGENT.md §6 协作约定入 docs/ 目录提交，供下轮接手直接读取。）
