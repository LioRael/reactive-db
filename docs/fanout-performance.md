> 历史报告：后续单行源发布与多实例交叉验证见 [新报告](rebuilt-performance.md)。

# reactive-db 短路径与大 fanout：配对测量和优化

日期：2026-10-26。直接基线 `c6c1fea`。本轮只修改 reactive-db 与任务验证目录，未改动 livequery、tanstack-db-rs。纯安全 Rust、零运行时依赖，公共 API 不变。

## 结论

**1,024 fanout 的退化已变为明确收益**：最终 Criterion 两组分别降低 42.2% / 41.7%，同进程 12 对测量的比值中位数降低 41.7%。两种方法的空对照都远小于该差异。对应真实成本是重复候选 ID 克隆和逐个树插入；旧版新增克隆回归实测 8,198 次，最终版通过不超过 4,128 次的上界。其余新增语义回归放回基线也通过。

**短路径只确认结构成本减少，不能宣称固定 20–30 ns 已全面消失**。无关节点写入的提交列表省去一次分配，最终 Criterion 两组约降低 10–12%；但同进程只降低 3.7%，接近空对照 -3.1% 的实例偏差。剪枝在同进程约降低 9.7%（约 30 ns），最终 Criterion 却为 +0.2% / +7.0%。方法之间尚未一致，不能只选择较好的结果。

多层宽图最终 Criterion 为 -2.2% / -6.2%；同进程 +3.2% 的配对区间覆盖零。join 汇合 Criterion 为 +8.4% / -4.3%，同进程配对中位数 -1.0%、区间也覆盖零。未观察到一致的持续退化，但不能排除小幅平台相关差异。结构上完整保留此前多层队列和单行 join 索引优化；本轮没有重新测量全图扫描原版，不跨实验计算与原版的优势。

只有 Vec 的中间候选曾在同进程汇合中约慢 3%，所以最终增加空/单一候选内联分支。最终仍保留所有异常窗口。fanout=1、万节点密集更新的不同测量也有差异，未列为稳定收益；批量撤回最终约慢 0.5–0.8%，该差异完整保留在表中。

## 改动及证据

`src/database.rs` 将首个待提交 NodeId 留在栈上，其余仍使用 Vec。源集合单独变化时不再为提交 ID 列表分配；多源及派生节点按原先顺序接续，首个源仍只准备、传播、提交一次。没有新增全图扫描、特殊发布通道或持久缓存。

`src/join/runtime.rs` 原先把旧匹配、新匹配、双边变化产生的候选逐个克隆到 BTreeSet。改为收集借用的 `(left_id, optional_right_id)`：空/单个唯一候选留在内联枚举中；第二个不同候选出现时才分配 Vec，稳定排序后去重，再为输出构造拥有所有权的 ID。`src/join/candidates.rs` 封装这个内部过程。大量相同 old/new 配对不再反复克隆、树查找、分配节点。`src/join/index.rs` 仅将内部 visit 回调的引用生命周期与 overlay 明确关联，允许安全借用。

使用**稳定排序**而非不稳定排序：当用户 ID 的 Ord 将不同内部表示视为相等时，保留最早遇到的表示，延续原 BTreeSet 的行为。新增 64 个右侧 ID 同批次双边替换的回归，检查 delta 中 ID 的附加字段，而不只检查比较相等。初步试验用过不稳定排序，最终未采用；报告只将最终稳定排序的正式数据作为交付证据。

已有的单行 join 索引补丁、紧凑多层队列、稀疏传播和 empty-delta 截断均保留。

## 复杂度、预算与内存边界

设 C 为去重前候选数、U 为唯一候选数。候选准备改为 O(C) 收集、最坏 O(C log C) 排序、O(C) 去重；后续按 U 个候选求值。不是把大 fanout 变成 O(1)，索引查找、输出构造、状态更新仍随受影响结果增长。

旧实现的候选树只保留 U 个拥有 ID 的条目；新缓冲暂存 C 个借用条目，并可能使用排序临时空间。高重复候选的临时槽位可能更多，不能宣称所有类型/形状峰值内存都下降；收益是去掉候选 ID 的所有权与树节点成本。所有缓冲仍为单次 prepare 局部所有，成功/失败均释放，无持久容量高水位。候选枚举仍受已有 work/input 预算约束；预算不是字节、比较次数或墙钟限制，排序比较和原有树比较一样不单独计费。

每条传播边、输入索引维护、旧/新 bucket 访问、唯一候选求值和输出变化继续按原规则收费。排序去重不改变求值顺序和唯一候选集合。仍先准备全部源和派生节点，成功后统一提交；异常、预算拒绝、订阅 epoch 和 panic poison 契约保持不变。

## 回归与验证

新增 5 项，共 **100 项测试**（87 unit/integration、13 rustdoc，含 10 compile-fail）通过：

- 1,024 fanout 的克隆上界：重复旧/新候选不再克隆 ID，仍验证最终行与输出计数。
- 大 fanout 在输出预算不足时拒绝：源/视图/epoch/订阅不发布，Weak 证明暂存 payload 已释放，重试正常发布 1,024 条 delta。
- 独立嵌套循环模型验证 16×128 稠密 join：双边同批次改 key、清空右 bucket、恢复匹配；检查 inner/left 的快照与 delta 重放。
- 两个源原子提交与一个 insert→remove 抵消源并存，少一个 work 拒绝、精确预算成功，两个订阅收到同一 epoch。
- Ord 相等但表示不同的 ID，最终 delta 保留首次候选表示。

原有随机全重算模型、自连接、维护状态无输出、跨集合原子性、预算边界和释放测试继续通过。标准 Cargo check、clippy（all-targets/all-features、-D warnings）、test、release test、fmt、rustdoc（-D warnings）通过；Rust 1.88 check/test 通过。最终验证使用工具链绝对路径、调用级清空 RUSTC_WRAPPER 与 RUSTC_WORKSPACE_WRAPPER，不经过 Blaze；不修改全局配置。

内存测试 10 轮、每轮 10,000 行、payload 256 字节，每轮释放 database 和外部 snapshot 后存活 payload 均为 0。RSS 约 28.8–30.0 MiB；分配器保留页，RSS 不等价于活对象，也不能用它证明所有形状无泄漏。

## 更稳定的测量方法

环境：Apple M2 Pro、16 GiB、macOS 27.0.1、Rust 1.99.0 release。两个基线源目录均逐文件对照 `c6c1fea`（25 个 src 文件）。没有与本任务的构建/测试同时进行计时，没有锁定 CPU 频率或排除系统其他活动。

第一层采用现有 Criterion harness，40 样本、0.5 秒预热、2 秒 measurement；按场景紧邻运行 before/after，最终版本两组交替 AB/BA，并反转场景顺序；此前只有向量的中间候选额外跑了四组，但不混入最终数据。另对同一个基线可执行文件用两种标签跑两组 A/A 空对照，估计运行噪声。表内范围是各运行中位数范围，百分比逐对计算，不以最快一次相除。完整 median/mean 及每轮 95% CI 另存 CSV/JSON。最终版本的两侧先完整构建再计时，所有窗口保留，不选择性删除异常数据。

第二层是独立的纯 Rust 同进程配对 harness：链接基线与最终实现，构造两个等价且独立的数据库；两侧先校准、再各预热三个约 200 ms 窗口，之后每场景进行 12 对相邻计时，交替先后顺序、固定相同写入次数。每个窗口仅在循环外读取时钟，短路径不为每次写入读取时间。加入同一基线的双实例 A/A 控制。该 harness 有双方相同的动态闭包调用成本，且两个图同时驻留；其绝对值不能直接与 Criterion 混用。

同进程表的耗时是窗口平均 ns/op 的跨窗口中位数；变化是 12 个配对比的中位数。95% 区间按配对窗口 bootstrap 10,000 次，保留时间相关性、温度和代码布局可能带来的限制，不代表跨硬件/所有工作负载保证。

## Criterion null

| 场景 | before 中位数范围 µs | after 中位数范围 µs | 各组成对变化 |
|---|---:|---:|---|
| scheduler/unaffected/10000 | 0.250–0.251 | 0.248–0.292 | -0.9%, +16.7% |
| scheduler/pruned/10000 | 0.291–0.307 | 0.297–0.298 | +2.3%, -3.2% |
| join_shapes/fanout/1024 | 226.756–229.092 | 228.352–229.484 | +0.2%, +0.7% |

## Criterion final

| 场景 | before 中位数范围 µs | after 中位数范围 µs | 各组成对变化 |
|---|---:|---:|---|
| scheduler/unaffected/10000 | 0.257–0.262 | 0.229–0.232 | -9.8%, -12.4% |
| scheduler/pruned/10000 | 0.284–0.298 | 0.298–0.304 | +0.2%, +7.0% |
| join_shapes/fanout/1024 | 227.494–229.956 | 132.689–132.828 | -42.2%, -41.7% |
| join_shapes/fanout/1 | 0.691–0.702 | 0.688–0.723 | -2.0%, +4.6% |
| scheduler/layered/10000 | 8593.920–8787.622 | 8246.396–8407.047 | -2.2%, -6.2% |
| scheduler/converging/10000 | 9201.420–9220.399 | 8823.568–9971.705 | +8.4%, -4.3% |
| scheduler/affected/10000 | 3274.721–3405.868 | 3080.635–3149.549 | -5.9%, -7.5% |
| join_shapes/retract_bucket/2000 | 860.599–865.543 | 867.204–869.613 | +0.5%, +0.8% |

## 同进程 null

| 场景 | before 窗口均值中位数 µs | after 窗口均值中位数 µs | 配对变化中位数 | 配对 bootstrap 95% 区间 |
|---|---:|---:|---:|---:|
| unaffected | 0.259 | 0.252 | -3.1% | -3.9%–-2.6% |
| pruned | 0.267 | 0.275 | +2.9% | +2.6%–+3.5% |
| fanout1024 | 232.477 | 232.896 | -0.3% | -2.4%–+0.5% |

## 同进程 paired

| 场景 | before 窗口均值中位数 µs | after 窗口均值中位数 µs | 配对变化中位数 | 配对 bootstrap 95% 区间 |
|---|---:|---:|---:|---:|
| unaffected | 0.248 | 0.239 | -3.7% | -4.4%–-3.2% |
| pruned | 0.290 | 0.260 | -9.7% | -11.2%–-7.3% |
| fanout1024 | 232.647 | 140.003 | -41.7% | -42.7%–-39.7% |
| fanout1 | 0.700 | 0.644 | -7.1% | -15.2%–-4.6% |
| layered | 8272.022 | 8513.487 | +3.2% | -2.7%–+6.8% |
| converging | 8845.003 | 9044.933 | -1.0% | -4.4%–+5.8% |
| affected | 3134.605 | 3105.978 | -1.2% | -2.5%–+1.3% |


## 复现

Criterion 使用同一 `benches/core.rs` / `benches/scheduler.rs`，导出基线 `c6c1fea`，两侧先构建；对每个场景相邻运行 A/B 再 B/A。命令示例：

```sh
cargo bench --bench core --locked --offline -- 'join_shapes/fanout/1024$' --noplot --sample-size 40 --warm-up-time 0.5 --measurement-time 2 --save-baseline before
cargo bench --bench scheduler --locked --offline -- 'scheduler/pruned/10000$' --noplot --sample-size 40 --warm-up-time 0.5 --measurement-time 2 --save-baseline before
```

同进程 harness 保存在 `benchmarks/paired.rs`，不加入产品依赖或默认 Cargo targets。建立独立比较工程，复制该文件为 `src/main.rs`；从基线导出 `src` 到 `before/src`，`before/Cargo.toml` 设置 package 名 `reactive-db-baseline`、version `0.1.0`、edition `2024`、rust-version `1.88`，无依赖。比较工程配置：

```toml
[package]
name = "paired-benchmark"
version = "0.1.0"
edition = "2024"
[dependencies]
old_db = { package = "reactive-db-baseline", path = "before" }
new_db = { package = "reactive-db", path = "/path/to/reactive-db" }
```

`cargo build --release --offline` 后直接运行可执行文件并保存 CSV。控制与候选均由宏生成相同工作负载：同批次写入、视图保持存活、不在计时循环里读快照。同进程 harness 也通过单独 clippy -D warnings。

实际验证调用显式 Rust 1.99.0 工具链路径，清空两个 wrapper；因本机 rust-objcopy/LLVM 兼容问题，两侧一致使用调用级 `CARGO_PROFILE_BENCH_STRIP=none`、`CARGO_PROFILE_RELEASE_STRIP=none`。不修改全局或项目 Cargo 配置。这里的脚手架只服务本地比较，不是项目运行依赖。

下一步若继续追求固定纳秒级收益，需要多次重建数据库实例、交叉实例创建顺序、跨进程重复，并控制频率/后台负载；本轮 A/A 已证明仅增加 Criterion 样本不能消除所有实例与运行偏差。现在交付可确认的 fanout 收益和短路径分配减少，不增加自适应阈值或复杂持久缓存来追逐噪声。
