# 批次索引候选：硬件计数器与同实例对照

2026-10-26。产品基线 d8d87a269d6a9ab001530c44277950b2642f9952。

## 结论与交付

本轮不合入运行时优化。保留原有稀疏传播、短路径、宽图、大 fanout 和 join 汇合实现；src/、Cargo.toml、Cargo.lock 与基线一致。新增严格同实例 benchmark 和键身份/更新 delta/值释放回归。原 livequery、tanstack-db-rs 仓库未修改；没有 JS、FFI、依赖或分配器替换。

硬件计数器把此前“大 fanout 约 7–9% 退化”的线索缩小到了输入索引 BTreeMap 查找的条件分支预测行为：三轮候选的条件分支误预测/周期均增加，PMI 额外样本集中在相同指令偏移。L1D load miss 比例反而略低，指令缓存 miss 没有一致方向。同实例改变分配路径没有重现该规模退化。因此不能把退化直接归因于批次索引多了分配或一致的缓存 miss 增加。

这是对分支预测相关性的直接证据，不是对具体预测器地址冲突、BTB 别名或单一因果机制的证明。不同布局还可能改变路径历史、指令翻译和缓存行为。未找到在全部保护场景都稳定获益的实现；拒绝以局部短路径收益换取无法排除的 fanout/宽图/join 回退。

## 硬件计数器方法

Apple M2 Pro（t6020），macOS 27.0.1，Xcode 27.1；使用已安装的 xctrace CPU Counters，无提权、额外安装或进程内 FFI。测量上一轮冻结的 baseline/candidate Criterion 二进制，命令过滤 join_shapes/fanout/1024，--profile-time 12，录制限时 6s。采样期间不并发编译、测试或其他本任务 benchmark。

三组事件：l1d_metrics、instruction_address_translation_metrics、discarded_sampling。各做三次 baseline/candidate，顺序 AB/BA/AB，第二轮倒转事件组顺序，共 18 份记录。每组独立录制，不能当作同时采集的 IPC 或每操作成本。配置保持 EL0 默认引导计数，具体 JSON、命令脚本、导出表和结果均随证据包保存。

解析 Instruments MetricTable 的线程归属指标，不直接相减跨核累计原始 PMC。仅分析主线程 2.0–5.5s 内完整且不超过 20ms 的有效片段，并按 S/E 核拆分；下表报告 S 性能核。大部分记录覆盖 3.435–3.495s，一份 baseline 分支记录覆盖 3.062s；观察周期/时间约 3.275–3.336GHz，未锁核/锁频。完整覆盖率在 counter-summary.json。计数以周期归一化，不是每次写入计数，也不是误预测概率。

18 次录制命令均返回 54，日志显示记录保存，时间限制结束后目标被 SIGKILL；没有把这些标成退出 0。每份导出成功，验证非空表（约 3.7–7.5 万行）、时间窗、主线程与核归属后才纳入分析。原始日志、TOC、导出表保留，便于独立核查；不推断未文档化退出码含义。

指标定义来自本机 RecountDT.framework 的 Analysis/metrics.json、bottleneck.json、characteristics.json。discarded_cond_branch 对应 BRANCH_COND_MISPRED_NONSPEC；保留 Instruments 的语义，不把 speculative load uop 与 retired branch 混为同一分母。

| 性能核指标（基线 → 候选） | 第 1 次 | 第 2 次 | 第 3 次 |
|---|---:|---:|---:|
| 条件分支误预测 / 百万周期 | 12334 → 14308 | 12516 → 14262 | 12747 → 14498 |
| L1D load miss / 千次 load uop | 37.350 → 36.743 | 36.745 → 36.409 | 37.188 → 36.211 |
| 指令缓存 demand miss / 百万周期 | 20.662 → 20.865 | 19.379 → 18.769 | 21.888 → 31.118 |

条件误预测增加约 13.7–16.0%。L2 指令 TLB miss 也偏高（候选约 87/108/100、基线 58/79/82 每百万周期），不能据此量化各因素对总耗时的贡献。缓存结果不支持“统一的 L1D miss 增加”解释，但不能排除所有缓存/翻译影响。

## 定位到分支

从 discarded_sampling 导出 SamplingModeSamples，使用同一主线程/时间窗/性能核条件，解析符号与加载地址。以下是 PMI 样本，不能视为精确误预测次数：

| 位置 | 基线 | 候选 | 候选复测 |
|---|---:|---:|---:|
| 条件误预测总样本 | 14155 | 16380 | 16314 |
| 输入索引查找闭包 | 3112 | 5711 | 5693 |
| 其中闭包 +0x158 | 2786 | 5178 | 5159 |

调用路径在 src/join/runtime.rs 的 desired 左/右行取值，经 src/join/index.rs::Overlay::get 查 patch/base.rows。闭包内部 BTreeMap key 比较后的 +0x158 是 b.ne，+0x170 是后续条件判断。两版这里指令与相对偏移相同，但符号起点不同：基线 0x1000a64d4（mod64=20），候选 0x100049000（mod64=0）。额外样本集中于此，而非批次 registry 或分配函数。完整符号名、反汇编、PC 换算在 branch-offsets.json 和 *.asm。

地址变化与误预测增加吻合，但单次布局对照不能证明硬件内部的特定索引/别名机制。也不能用随机 padding 获胜一次作为修复。

## 更严格的同实例对照

benchmarks/batch_index_strict.rs 只用于临时副本，配合已有 batch_index_probe.patch。固定同一 DB、查询节点与二进制，通过安全诊断开关选择 tree/inline；该开关没有进入产品。

3 个独立进程 × 每场景 4 个实例 × 2 轮，轮换条件先后、交替 ABBA/BAAB；1632 个窗口组成 408 个四窗口块。实例交替 fresh/churn 分配历史。先交替执行 tree/inline 共 512 次写入，越过默认 journal 128 批次容量（fanout 还受 16384 changes 限额约束）。两模式分别校准后取同一偶数迭代数，窗口目标约 25ms；偶数更新回到相同值端点。fanout 初始双集合写在同一批次，匹配 Criterion。

块内 A/B 各两个窗口求均值，再取实例两轮中位数、进程四实例中位数。下表是 B/A−1%，负数更快，三个数对应三个进程；不是跨进程置信区间。

| 场景 | tree/tree 空对照 | inline/inline 空对照 | tree/inline |
|---|---|---|---|
| unaffected | -0.59 / +0.85 / +0.93 | -0.51 / -0.19 / +0.12 | -8.08 / -10.86 / -9.81 |
| pruned | -0.18 / 0.00 / -0.10 | -0.12 / +0.48 / -1.31 | -9.37 / -8.70 / -8.29 |
| fanout1024 | +1.36 / -0.27 / -0.16 | +0.36 / -1.01 / +1.05 | -0.34 / -0.52 / -0.12 |
| layered10000 | -0.17 / +1.04 / -4.13 | +0.28 / -0.37 / -0.60 | +0.90 / +2.74 / +4.20 |
| converging10000 | +2.90 / +0.04 / +0.58 | -2.10 / -3.17 / +2.11 | +1.94 / +0.36 / -0.12 |

短路径收益明显超过空对照；fanout 的微小差别在空对照范围内，不能声称稳定提升。宽图方向仍差，且空对照暴露不小的噪声；join 没有一致收益。诊断开关改变代码生成/类型布局，因此同实例结果不能直接当作独立候选二进制的接受证明。

分配扰动：在 inline fanout 每次写前额外分配 280 字节，分别立即释放、持有到写后释放。相对 inline 的三进程差值分别为 -0.86/-0.31/-0.25% 和 +0.46/-0.36/+0.50%，均没有重现原 7–9% 退化。280 字节来自上一轮实际 malloc 追踪的单源 registry 节点；本轮 padding 是 black_box 的 Vec<usize>，包括初始化成本，不是对 BTreeMap 完整生命周期的精确复制。不能凭此排除所有分配器影响。harness 的 ALLOC_PROBE 交互模式本轮未运行。

## 尝试与拒绝的实现

在临时副本尝试两个有明确代码路径依据的改动，各自单独及叠加原 inline 批次候选：

1. State::apply 对 Update 使用 get_mut 替换值，避免再次通用 insert/键 clone。
2. 对热点 Overlay::get 使用 inline(never)，减少多个调用点展开。

各做两轮顺序反转 Criterion 筛选，20 samples，warmup 0.5s、measurement 1s。这是候选淘汰筛选，不是用于接受 1% 改进的稳定基准；明显波动的项不解释为确定退化。

### Update 替换值

| 场景 | update | combined |
|---|---:|---:|
| join_shapes/fanout/1024 | +5.31% / -19.07% | +5.15% / -27.34% |
| scheduler/unaffected/10000 | +5.60% / +8.90% | -11.40% / -9.75% |
| scheduler/pruned/10000 | -4.89% / +10.13% | -4.22% / -35.18% |
| scheduler/layered/10000 | -2.23% / +0.23% | -2.88% / +4.79% |
| scheduler/converging/10000 | +21.23% / +3.50% | +8.58% / +4.83% |

### 独立索引查找函数

| 场景 | lookup | batch_lookup |
|---|---:|---:|
| join_shapes/fanout/1024 | +7.16% / -0.40% | +7.62% / +0.64% |
| scheduler/unaffected/10000 | -0.57% / +1.43% | -10.14% / -11.77% |
| scheduler/pruned/10000 | +5.49% / +10.05% | -4.60% / -10.43% |
| scheduler/layered/10000 | -2.53% / -12.79% | -4.09% / -12.38% |
| scheduler/converging/10000 | +2.04% / -5.57% | +5.70% / -0.70% |

Update 单独版本短路径和 join 汇合没有稳定通过；叠加 inline 后 join 两轮均慢。独立查找虽有宽图收益线索，但 pruned 单独版本两轮更慢，叠加 inline 的 fanout 两轮仍慢（7.62% / 0.64%），join 方向不一致。全部拒绝，不继续以偶然有利布局筛选产品。最终产品与基线运行时相同，没有可以报告的本轮产品加速百分比。

## 回归、检查与复现

新增 tests/update_identity.rs：Ord 相等但标签不同的键更新后，快照仍保留 canonical key，delta 携带更新键和同一提交 epoch；释放快照、订阅、句柄与数据库后，新旧值的 Weak 均不能升级。已有全重算模型、同批次 join、自连接、预算失败回滚、稀疏传播、原子发布和释放测试一并运行。

原生 Rust 1.99 的 check --all-targets --all-features、clippy --all-targets --all-features -- -D warnings、debug/release test、doc 警告视为错误均通过；debug/release 各 109 测试（含 doctest）。MSRV 1.88 check 与 109 测试通过。10 轮内存 benchmark live_after_drop 均为 0。严格诊断 harness 的 build/clippy 通过。候选测试通过不代表性能被接受。

使用绝对路径原生 Cargo，并清空调用内 RUSTC_WRAPPER/RUSTC_WORKSPACE_WRAPPER，显式对应 RUSTC/RUSTDOC；未让 Blaze 替代最终验证。已安装 cargo blaze 暴露 ra-jit 行为，兼容性不确定，因此未使用，未修改全局配置。

复现严格对照：在基线临时副本应用 benchmarks/batch_index_probe.patch，把 benchmarks/batch_index_strict.rs 复制为 examples/strict.rs，用原生 cargo build --release --example strict，分别设置 BENCH_RUN=0、1、2 运行该二进制，顺序执行且关闭本任务其他性能任务。不要把 patch 应用到产品开发分支。分析器按 CSV 的 role 字段计算配对，不依赖先后推断 A/B。

证据包包含原始 CSV、MetricTable/PMI/TOC XML、录制日志/配置、分析脚本、反汇编、候选补丁、验证日志及 SHA-256 清单。完整 Instruments .trace 留在本任务 work/counter-study，不放入压缩包；XML 足以重算本文数据，但重新打开完整 Instruments UI 需要对应 trace。脚本记录本机路径，迁移机器需调整路径并重新生成配置；硬件数值不可直接泛化到其他 CPU。
