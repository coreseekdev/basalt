# AGENTS.md —— Agent 工作规则（强约束，任何 Agent 会话必须遵守）

> 本文件是 coding agent 在本仓库工作的最高优先级流程约束。
> 违反本文件的"完成"声明无效。账本：[docs/VERIFICATION.md](docs/VERIFICATION.md)。
> 架构三约束：云原生（OSS + OSS 优化存储）、高性能（架构优化 + 测试）、
> 高可靠（形式化验证 + 模型检查）——所有工作不得违背。
> **I/O 实现无关（ADR-14）**：禁止代码依赖缓冲 I/O 特有行为——持久化契约经
> DiskIo::append（写边界）/ sync_file（持久边界）定义，对缓冲写与 O_DIRECT 同构。

## 1. 三类 Review Agent（定期 + 事件触发，必须启动）

| Agent | 触发条件 | 职责 |
|---|---|---|
| **Code Review** | 每轮存储/服务端/协议的实质性变更合并前 | 实现与理想的差距、隐藏 bug。**必须探针实证**（写临时测试跑通后删除），不接受纯读码结论 |
| **形式化验证 Review** | spec/*.tla 变更后必须；平时定期 | 不变式合理性/完整性（对照 docs/11-testing-strategy.md §1 七条验收不变式）、环境模型合理性、活性缺口；识别"构造恒真"的空洞不变式并降级标注 |
| **性能 Review** | 定期 + 热路径变更后 | 分配/拷贝/syscall/索引/锁五个维度，结论须含验证方法（基准/strace/perf）与 A/B 方案 |

Review 结论必须落盘入库（docs/review-*.md），作为后续工作的任务书。

## 2. 顺序约束（重要：Code Review 先于形式化验证）

```
code review agent（发现缺陷/语义澄清）
  → 修复 + 回归测试（红转绿）
  → 形式化验证跟进：不变式补充 / harness 扩展 / 规约 vN+1
  → 不变式 review agent 校验
```

**禁止并行启动 code review 与形式化验证评审**：code review 的结论
（缺陷模式、边界语义、故障模型修正）是形式化验证的输入——并行会使
规约与实现各自演化、精化桥断裂。本规则的依据：opfuzz/code review
发现的 5 个存储缺陷中 3 个（批对齐、log_start 持久化、空段封存）
直接改变了 truncate/delete 的形式化规格需求。

形式化验证内部的 review（不变式评审）可与 code review 的**修复实现**
并行，但规约修订必须在 code review 结论入库之后。

## 3. 缺陷 → 检测机制纪律（docs/VERIFICATION.md §12 矩阵）

**每个已发生的缺陷必须落一个永久检测机制**，并在账本 §12 矩阵登记
错误类别。机制分层：Kani（不可信输入边界）→ opfuzz（状态机交互采样）
→ 一致性/边界表/属性测试（确定性边界与表示不变式）→ TLC（协议语义）
→ Verus（算术与包含性）。只修不加机制 = 未完成。

## 4. WIP 纪律

- 未全绿的验证测试标 `#[ignore]` + WIP 注释（复现序列、疑点、修法方向
  必须写入），**不计入账本已验证集合**；
- 禁止 delete-to-pass：不得通过删除断言/弱化规约使测试变绿；
- 账本状态必须与事实一致（✅/🚧/⬜），诚实标注证据边界。

## 5. 工具与版本锁定

- tla2tools.jar 已入库（spec/tools/）；Verus/Kani 版本记录在各自 README；
- 升级验证工具必须同步账本工具版本并全量重跑对应验证。

## 6. 提交纪律

- 原子提交：一个逻辑变更一笔（缺陷修复 / 机制新增 / 文档 分开）；
- 提交前：涉及 crate 的全部测试 + verify.sh 全链路绿；
- 中文 conventional commits；工作区保持干净。

## 7. 参考索引

- 验证总纲与账本：docs/VERIFICATION.md（§12 缺陷→机制矩阵）
- 规约验证指南（人类可读）：spec/VERIFICATION-GUIDE.md
- 评审任务书范例：docs/review-storage-c7-20260909.md
- opfuzz WIP 复现序列：storage/tests/opfuzz.rs 注释
