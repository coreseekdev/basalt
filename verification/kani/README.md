# verification/kani —— L1 无 panic 门禁（Kani）

## 状态：✅ 2 harness 全部验证通过（2026-09-10）

```
Verification Time: 1.3s
Complete - 5 successfully verified harnesses, 0 failures, 5 total.
```

| Harness | 性质 |
|---|---|
| `zigzag_roundtrip_no_panic` | 全部 i64：put/read 往返 == Some(v)，pos 恰好走完 |
| `read_zigzag_arbitrary_bytes_no_panic` | 任意 3 字节输入：解码不 panic |

| `validate_append_no_panic_any_input` | append 校验守卫块任意输入无 panic（C13 族 append 入口） | 全称定理 |
| `validate_append_guard_complete` | Ok ⇒ total ≥ 61 ∧ rest.len() ≥ total（守卫完备性） | 全称定理 |

## 工具

| 工具 | 版本 | 安装 |
|---|---|---|
| Kani | 0.67.0 | `cargo install --locked kani-verifier && cargo kani setup`（二进制在 `~/.kani/kani-0.67.0/bin`） |

运行：`PATH=$HOME/.kani/kani-0.67.0/bin:$PATH kani record_l1.rs`

## TCB 声明（诚实边界）

standalone 模式不拉 cargo 依赖，故本文件是 `record/src/lib.rs`
`put_zigzag`/`read_zigzag` 的**同源副本**（逐行一致）。真实现变更时必须
同步本副本（PR 模板勾选项），否则证明失效。长期方案：改用
`cargo kani -p basalt-record`（需先解决 zstd-sys 的 C 依赖链接），
或将 zigzag 拆为无依赖子 crate 使证明直接指向真实现。

## 已知边界

- `read_zigzag` 的 overlong 输入在 crate 实现中静默丢弃高位
  （`shift < 64` 守卫），本副本同语义——canonical 编码不受影响；
- BatchHeader::parse 的 61B 边界读取为常量 bounds，下一批 harness 覆盖。
