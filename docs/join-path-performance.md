> 历史报告：后续短路径与大 fanout 优化及配对验证见 [新报告](fanout-performance.md)。

# reactive-db 短路径与 join 汇合优化

日期：2026-10-26。直接基线 `39804b1`。本轮只修改 reactive-db 和任务验证目录，未改动 livequery、tanstack-db-rs。继续使用纯安全 Rust、零运行时依赖，公共 API 不变。

## 结论与边界

主要收益来自单行 join 索引补丁：万节点汇合在定向长采样中降低约 **39.2%**，完整调度器对照中降低约 **43.5%**。单个 join、fanout=1 约降低 **14.3%**。批量撤回基本持平；fanout=1024 约慢 **2.0%**，不宣称所有 join 形状都变快。

短路径增加空队列/单目标快速出口，并缩小出队返回值。主测试中万节点无关写入降低约 **27 ns**，剪枝路径降低约 **33 ns**；但第一组长采样平均只降低约 3 ns / 15 ns，运行区间重叠。不能认定此前 20–30 ns 的退化已在所有运行条件下消除。

原有多层宽图紧凑队列、受影响节点传播和 empty-delta 截断仍保留。万节点两层图主测试约快 2.1%，第一组长采样约慢 6.5%；后续 ABBA 复核约慢 0.3%，范围重叠，未复现持续的 6.5% 退化。短路径复核中无关写入约慢 5.2%（约 14 ns），剪枝约慢 0.6%；因此短路径收益仍不稳定。本轮没有重新计时全图扫描原版，不能用历史不同实验绝对时间来宣称已超过它。

## 定位与实现

`src/scheduler.rs` 的出队原先返回 `(rank, NodeId)`，调用者丢弃 rank。现在只返回 `NodeId`，rank 继续在队列内部用于拓扑排序和去重。新增小型内联入口，仅处理空队列和唯一剩余目标；一般合并仍在独立函数中。没有将整个合并逻辑强制内联，也没有恢复全图扫描。

早期 ABI 对照检查了 Apple Silicon release 汇编：原版通用出队使用 240 字节栈帧和返回结果缓冲；仅缩小返回类型即可去掉该缓冲写入，但其通用函数仍有同样大小的栈帧。最终小入口使空/单目标无需进入通用合并分支。汇编只作为结构证据，不能换算成固定纳秒收益。

join 汇合的瓶颈不只是队列。即使输入只有一个变化，`src/join/index.rs` 过去仍为 rows、added、removed 构建临时 BTreeMap/BTreeSet：已有行替换时，每侧最多涉及五个临时树节点的分配。现在内部 `Patch::One` 直接拥有 ID、缓存旧 key 和新 entry；多行和空输入沿用 `Patch::Many`。候选配对集合仍是原 BTreeSet，不引入新的通用容器。

`src/join/runtime.rs` 通过统一的 patch ID 迭代器访问两种形式。Overlay 的最终 bucket 缓存、旧成员撤回和工作计费保持原规则。新值 selector 在准备期执行一次；删除继续使用缓存 key。同 key 替换仍执行 remove/add，避免改变具有资源所有权的相等 key 的释放语义。此优化去掉临时索引树分配，不代表整次 write 零分配。

曾尝试提前消费重复队首、单个 join 候选内联集合；未观察到稳定额外收益，最终撤回。最终保留的队列单目标 heap 与上一版一致。

## 语义与回归

没有修改 prepare→commit 发布边界、退休节点回收、订阅缓冲或预算 API。所有传播边和算子工作仍按原规则计费；单行 patch 为当次 write 所有，失败时随 stage 释放。预算仍是逻辑工作/输入/输出计数，不是字节或墙钟预算。用户回调或比较/克隆 panic 仍遵循已有 fail-stop/poison 契约。

新增 5 项回归，总计 **95 项**（82 unit/integration、13 rustdoc，其中 10 compile-fail）通过：

- 最大 NodeId、零 ID 与最大 rank 不混淆或截断；不同长度汇合序列正确去重并可继续入队。
- 单行 overlay 与独立 BTreeMap 全重算模型比较：同 key 替换、改 key、删除、新增；提交前已发布索引不变，提交后成员和行匹配模型。
- 丢弃单行 patch 后通过 Weak 引用确认 payload 被释放。
- 32 个共享不等深度父节点的 join：少一个 work 的预算拒绝，源、视图、epoch 和所有订阅都不变；精确预算重试成功，统计相同，所有订阅收到相同 epoch 的一次变化。

既有独立全重算 join 模型、双边同批次改 key、自连接、left join 未匹配维护、跨集合原子发布、稀疏传播、退休回收等测试全部继续通过。

标准 Cargo 的 check（all-targets/all-features）、clippy（同范围且 -D warnings）、test、release test、fmt check、rustdoc（-D warnings）通过；最低支持版本 Rust 1.88 的 check/test 也通过。采用工具链绝对路径并清空调用级 RUSTC_WRAPPER/RUSTC_WORKSPACE_WRAPPER，确保最终验证未经过 Blaze。没有修改全局配置。

内存 benchmark 连续 10 轮、每轮 10,000 行、每个 payload 256 字节，所有轮次 owner 和外部 snapshot 释放后 live payload 都为 0。RSS 从约 30.1 MiB 到 30.6 MiB 后接近平台；分配器可保留页，RSS 不作为无泄漏的证明。

## 测量方法

Apple M2 Pro、16 GiB、macOS 27.0.1、Rust 1.99.0，release Criterion。基线导出源逐文件对照 `39804b1`（25 个 src 文件），使用相同 manifest、lockfile 和 harness。计时与构建、测试分开，无其他本任务 benchmark 并行运行。

主测试 22 个 scheduler 场景，ABBA（before→after→after-repeat→before-repeat），30 样本、150 ms 预热、500 ms measurement。join_shapes 5 场景同样 ABBA，20 样本、100 ms 预热、400 ms measurement。

第一组定向对照 before→queue-only→after→after-repeat→queue-only-repeat→before-repeat，50 样本、1 秒预热、3 秒 measurement。queue-only 是最终队列代码加原版 join index/runtime，验证 join 收益来源。后续复核也使用 50 样本、1 秒预热、3 秒 measurement，ABBA。

表中范围是**两次运行的中位数最小/最大值**，不是置信区间；变化率为两次中位数平均值之比。CSV/JSON 另存每次运行 median/mean 的 95% CI。主测试若出现很宽范围（例如 affected/10000、layered/1000），说明跨运行扰动明显，对应大百分比不应作为稳定收益。empty_write 在进入调度前返回，数纳秒差异不归因于本次代码。


## 分步消融对照

| 万节点场景 | 原版 µs | 仅队列优化 µs | 队列＋单行索引补丁 µs |
|---|---:|---:|---:|
| unaffected/10000 | 0.260–0.285 | 0.257–0.293 | 0.253–0.285 |
| pruned/10000 | 0.317–0.331 | 0.277–0.330 | 0.290–0.327 |
| layered/10000 | 8259.906–8294.799 | 8385.570–8560.057 | 8609.073–9018.049 |
| converging/10000 | 15194.187–15469.479 | 15218.942–16590.246 | 9150.826–9486.887 |

仅队列优化没有稳定降低 join 汇合耗时；显著收益出现在单行索引补丁加入后。不同测试组不能直接混合计算改进率。

## 完整调度器对照

| 场景 | 优化前 µs | 优化后 µs | 平均中位数变化 |
|---|---:|---:|---:|
| affected/0 | 0.248–0.254 | 0.248–0.352 | +19.4% |
| affected/100 | 25.160–25.331 | 24.757–25.748 | +0.0% |
| affected/1000 | 261.120–269.386 | 250.061–265.229 | -2.9% |
| affected/10000 | 3308.062–6245.118 | 3287.824–3433.301 | -29.6% |
| converging/100 | 102.841–176.719 | 64.156–67.157 | -53.0% |
| converging/1000 | 1067.847–1088.841 | 663.777–668.229 | -38.2% |
| converging/10000 | 13847.188–14724.458 | 8030.993–8109.951 | -43.5% |
| empty_write/0 | 0.015–0.016 | 0.016–0.016 | +2.3% |
| empty_write/100 | 0.016–0.016 | 0.016–0.017 | +6.4% |
| empty_write/1000 | 0.015–0.016 | 0.016–0.017 | +4.8% |
| empty_write/10000 | 0.015–0.016 | 0.016–0.016 | +3.6% |
| layered/100 | 55.031–100.116 | 53.787–54.943 | -29.9% |
| layered/1000 | 595.840–1328.139 | 591.230–649.119 | -35.5% |
| layered/10000 | 6378.431–7149.417 | 6456.583–6789.299 | -2.1% |
| pruned/0 | 0.254–0.270 | 0.249–0.273 | -0.4% |
| pruned/100 | 0.279–0.298 | 0.263–0.263 | -8.8% |
| pruned/1000 | 0.290–0.323 | 0.267–0.279 | -10.8% |
| pruned/10000 | 0.295–0.334 | 0.277–0.287 | -10.4% |
| unaffected/0 | 0.247–0.271 | 0.247–0.263 | -1.6% |
| unaffected/100 | 0.251–0.252 | 0.245–0.288 | +5.8% |
| unaffected/1000 | 0.266–0.268 | 0.246–0.272 | -3.0% |
| unaffected/10000 | 0.269–0.288 | 0.250–0.252 | -9.8% |

## 第一组长采样

| 场景 | 优化前 µs | 优化后 µs | 平均中位数变化 |
|---|---:|---:|---:|
| converging/10000 | 15194.187–15469.479 | 9150.826–9486.887 | -39.2% |
| layered/10000 | 8259.906–8294.799 | 8609.073–9018.049 | +6.5% |
| pruned/10000 | 0.317–0.331 | 0.290–0.327 | -4.6% |
| unaffected/10000 | 0.260–0.285 | 0.253–0.285 | -1.2% |

## 长采样复核

| 场景 | 优化前 µs | 优化后 µs | 平均中位数变化 |
|---|---:|---:|---:|
| layered/10000 | 8178.034–8419.690 | 8253.799–8387.766 | +0.3% |
| pruned/10000 | 0.290–0.321 | 0.303–0.311 | +0.6% |
| unaffected/10000 | 0.277–0.277 | 0.277–0.306 | +5.2% |

## Join 形状对照

| 场景 | 优化前 µs | 优化后 µs | 平均中位数变化 |
|---|---:|---:|---:|
| fanout/1 | 0.817–0.824 | 0.700–0.707 | -14.3% |
| fanout/1024 | 225.962–225.987 | 230.290–230.855 | +2.0% |
| fanout/64 | 7.085–7.191 | 6.888–6.910 | -3.4% |
| retract_bucket/200 | 113.387–113.656 | 112.304–113.432 | -0.6% |
| retract_bucket/2000 | 853.823–858.449 | 860.282–863.746 | +0.7% |

## 复现与后续边界

基线 checkout 为 `39804b1`，两侧使用当前相同 benches、Cargo.toml 和 Cargo.lock。标准命令：

```sh
cargo bench --bench scheduler --locked --offline -- --noplot --save-baseline before
cargo bench --bench scheduler --locked --offline -- 'scheduler/(unaffected|pruned|layered|converging)/10000$' --noplot --sample-size 50 --warm-up-time 1 --measurement-time 3 --save-baseline focused
cargo bench --bench core --locked --offline -- 'join_shapes/' --noplot --save-baseline join-before
cargo bench --bench memory --locked --offline
```

切换基线和候选时分别保存名称，再反序重复。实际运行使用 Rust 1.99.0 工具链绝对路径、对应 RUSTC/RUSTDOC，并清空两个 wrapper 环境变量；因本机 rust-objcopy 的 LLVM 版本不匹配，两侧统一设置调用级 `CARGO_PROFILE_BENCH_STRIP=none` 和 `CARGO_PROFILE_RELEASE_STRIP=none`。未改项目或全局配置。

最终代码仍保留有序运行时节点表、BTreeSet 候选和原版多行索引补丁。后续若继续压缩短路径，需要更受控的 CPU 频率/温度与分配计数实验；现有测量不足以证明固定 20–30 ns 的净收益。大 fanout 配对与订阅数据成本仍随受影响输出增长，未声称改为常数时间。
