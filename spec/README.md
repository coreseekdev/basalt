# spec/ —— TLA+ 规约（T-Q.2）

- `BasaltDataPlane.tla`：ADR-10 数据面协议 v0.1（PlusCal），不变式与三个实验开关见文件头。
- `docs/VERIFICATION.md` §7 是验证账本；§5 是精化桥协议（本目录与 Verus 规约的对应表随 M2 落地）。

## 运行

```sh
jar 已版本锁定入库（spec/tools/）；升级需同步账本工具版本
make check                # 名义协议：全部不变式应通过
make demo-eager           # EagerLeader=TRUE：应检出 InvLeaderHasCommitted 反例
make splitbrain           # 控制器 fencing 失效 + 提交不校验视图：实验性
make splitbrain-cepoch    # 控制器 fencing 失效 + 提交校验视图：实验性
```

设计变更时重跑全部四个场景；TLC 输出的 .bin/.st 文件由 `-cleanup` 清理。

## 继任规则对应表（精化桥 v0）

| TLA+ / 本文件 | 实现（T-M2.3） | Verus 规约（M2 后） |
|---|---|---|
| `SuccessorOf(q, j)` | OffsetForLeaderEpoch + 继任者预计算 | ghost 函数 `spec_successor` |
| `Push`（原子 fetch+truncate+epoch 确认） | follower fetch 循环 + truncate | `Push` ghost 步骤 |
| `CommitAdvance` 多数派内容校验 | HW 推进的 ack 计数 | `spec_commit_invariant` |
| `InvLeaderHasCommitted` | e2e 断言器 acked⊆leader 前缀 | 不变式 `acked_prefix_of_leader` |
