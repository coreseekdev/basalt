# verification/verus —— record 核心证明切片

## 状态：✅ 全部验证通过（2026-09-09）

```
verification results:: 18 verified, 0 errors
```

## 工具版本（账本要求锁定）

| 工具 | 版本 | 获取 |
|---|---|---|
| Verus | 0.2026.09.09.f42e59f (rolling) | `https://github.com/verus-lang/verus/releases`（x86-linux.zip，自带 rustc 包装器） |
| 依赖工具链 | rustup toolchain `1.98.1-x86_64-unknown-linux-gnu` | `rustup toolchain install 1.98.1-x86_64-unknown-linux-gnu --profile minimal` |
| 求解器 | Z3（Verus 内置分发） | — |

运行：

```sh
rustup toolchain install 1.98.1-x86_64-unknown-linux-gnu --profile minimal
verus --crate-type=lib record_core.rs        # 替换为解压后的 verus 二进制路径
```

## 已证明的 Claim（对应 docs/VERIFICATION.md §7）

| Claim | 内容 | 性质 |
|---|---|---|
| C5 | `zig`/`unzig`（算术实现）与 `zig_spec`/`unzig_spec` 逐点相等；`unzig_spec(zig_spec(v)) == v` 对**全部 i64** | 全称定理 |
| C5' | **crate 位运算原型**：`((v<<1) ^ (v>>63)) as u64` 与算术规约逐点相等（`zig_bits_matches_math`，补码恒等式经 bit_vector 编码）；`((z>>1) as i64) ^ -((z&1) as i64)` 与规约逐点相等（`unzig_crate`） | 全称定理 |
| C5'' | `roundtrip_crate(v) == v` 对全部 i64 —— record/src/lib.rs 中 `zigzag_roundtrip` proptest 的全称化升级 | 全称定理 |
| C6a | `Compression::from_bits(bits) == comp_of(low3(bits))`（全 i16）；`from_bits(bits(x)) == Some(x)` 对全部枚举值 | 全称定理 |

对应实现：`basalt/record/src/lib.rs` 的 `put_zigzag`/`read_zigzag`（核心两行）与 `Compression`。
规约即类型：这些函数的实现改动必须同步通过本切片的 `ensures`，PR 门禁据此把关。

## 已知的会话摩擦（供后续切片参考）

1. **spec 层算术拓宽到 `int`**：u64/i64 参与运算即变 int，需在边界显式 `as` 收拢；
2. **`by (bit_vector)` 只看目标表达式本身**：let 别名、外部上下文不进入编码，断言必须写成参数上的纯表达式；spec 函数调用不可出现在 bv 块内；
3. **spec 层 `<<` 依赖 vstd bits 扩展且拓宽为 int**：桥接时用加法等价式（`x << 1` ↔ `x + x`）更稳；
4. 枚举 cast 到 int 需要 `#[derive(Copy)]` + `#[repr(i16)]`；
5. **`by (nonlinear_arith)` 块内只能出现算术原子**：spec 函数调用（如 pow128(k)）在 nl
   块内是未解释符号，Z3 直接放弃——必须先 `let p: int = pow128(k);` 绑定为原子，
   且前提（如 `p1 == 128 * p`）要内联进蕴含式；div-mod 恒等式用 plain SMT 即可，
   放进 nl 反而丢失；
6. **跨循环体的事实只存活于不变式**：循环前证明引理建立的边界事实（如 pow128 具体值、
   顶层守卫）在循环体内不可见，需提升为不变式或在体内重取。

## 下一步（按 docs/VERIFICATION.md §8 顺序）

1. **varint（LEB128）循环机器（进行中）**：引理骨架与循环不变式组已写在
   `varint_wip.rs`（**未完成，不计入账本**），剩余 9 个验证义务与入手点见该文件头部注释；
2. 批头 `BatchHeader::parse` 的无 panic + 域内正确性（bounds 全部是常量，适合 Kani 而非 Verus）;
3. Creusot 对照（切换协议 §4.1）：同一 zigzag 切片用 `cargo creusot prove` 复现，采集摩擦数据。
