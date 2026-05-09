# Part 4 Proactive Headroom Scheduler Report

## 1. Strategy Summary / 策略概述

| 中文 | English |
|---|---|
| 本项目 Part 4 的核心挑战是在同一台 4-core `memcache-server` 上同时运行长时间在线服务 memcached 和多个 batch workload。memcached 的 95th percentile latency SLO 是 0.8 ms，因此调度器必须优先保护 memcached，同时尽量利用暂时空闲的 CPU core 来推进 batch jobs。 | The core challenge in Part 4 is to co-schedule the long-running memcached service and several batch workloads on the same 4-core `memcache-server`. Since memcached has a strict 0.8 ms 95th percentile latency SLO, the scheduler must protect memcached first while opportunistically using temporarily idle CPU cores for batch jobs. |
| 我实现的动态调度策略是 Proactive Headroom Scheduler。它不是等到机器已经过载之后才把 core 还给 memcached，而是根据 CPU utilization、softirq、load average、network receive rate 以及短期趋势提前扩展 memcached 的 CPU allocation。 | The implemented dynamic strategy is a Proactive Headroom Scheduler. Instead of waiting until the machine is already overloaded, it uses CPU utilization, softirq, load average, network receive rate, and short-term trends to expand memcached's CPU allocation early. |
| 策略原则是：memcached 优先，batch opportunistic。也就是说，batch jobs 只使用 memcached 安全余量之外的 CPU 资源；当系统压力上升时，batch jobs 会被缩核或暂停。 | The policy principle is: memcached first, batch opportunistic. Batch jobs only use CPU resources outside memcached's safety headroom. When pressure rises, batch jobs are throttled or paused. |

## 2. Baseline Limitation / 原响应式策略的问题

| 中文 | English |
|---|---|
| 原始动态策略主要基于当前 CPU utilization 的阈值。如果 pressure 已经很高，就给 memcached 增加 core；如果 pressure 长时间较低，就减少 memcached core。 | The original dynamic strategy was mainly based on thresholds over current CPU utilization. If pressure was already high, it added cores to memcached; if pressure stayed low for a while, it removed cores from memcached. |
| 这种方法的问题是它偏 reactive。mcperf 的 load interval 会突然跳变，而 memcached 的 tail latency 对短时间 CPU 争用非常敏感。等 controller 看到高利用率之后再调整，某一个 10-15 秒 interval 的 p95 latency 可能已经违反 SLO。 | The issue is that this method is reactive. The mcperf load interval can change abruptly, and memcached tail latency is highly sensitive to short CPU contention. By the time the controller observes high utilization and reacts, one 10-15 second interval may already violate the p95 SLO. |
| 因此更稳健的调度器应该提前保守一些，在压力趋势上升时预留 headroom，而不是把全部空闲 core 立即交给 batch jobs。 | A more robust scheduler should be slightly conservative and reserve headroom when pressure is trending upward, rather than immediately giving every idle core to batch jobs. |

## 3. Monitored Signals / 监控信号

| 中文 | English |
|---|---|
| 调度器直接运行在 `memcache-server` 上，不依赖 Kubernetes，因为 Part 4 要求用 Docker 和 `taskset` 动态调整资源。 | The controller runs directly on `memcache-server` and does not rely on Kubernetes, because Part 4 requires Docker and `taskset` for dynamic resource adjustment. |
| 每个 control loop 默认每 5 秒采样一次系统状态。采样来自 `/proc/stat`、`/proc/net/dev` 和 `/proc/loadavg`。 | Each control loop samples system state every 5 seconds by default. The sampled data comes from `/proc/stat`, `/proc/net/dev`, and `/proc/loadavg`. |
| CPU utilization 表示每个 core 在两个采样点之间的 busy ratio。它用于判断 memcached 当前 core 是否接近饱和，以及整机是否接近饱和。 | CPU utilization is the busy ratio of each core between two samples. It is used to detect whether memcached's current cores or the whole machine are close to saturation. |
| softirq ratio 近似反映网络包处理压力。memcached 很多开销来自网络 packet processing，因此 softirq 上升通常比普通 user CPU 更早提示风险。 | The softirq ratio approximates network packet processing pressure. Since much of memcached's overhead comes from network packet processing, rising softirq can indicate risk earlier than user CPU alone. |
| network receive throughput 用 `/proc/net/dev` 的 RX bytes 估计。它不是精确 QPS，但在动态 mcperf load 上升时通常会同步上升，可以作为 QPS trend 的代理信号。 | Network receive throughput is estimated from RX bytes in `/proc/net/dev`. It is not exact QPS, but it usually rises with dynamic mcperf load and can serve as a proxy for QPS trend. |
| load average normalized by 4 cores 用来判断 run queue pressure。如果 normalized load 接近或超过 1，说明 runnable tasks 已经接近或超过 CPU capacity。 | The 1-minute load average normalized by 4 cores indicates run queue pressure. If normalized load approaches or exceeds 1, runnable tasks are close to or above CPU capacity. |

## 4. Pressure Score / 压力评分

| 中文 | English |
|---|---|
| 调度器维护最近 6 个 sample 的滑动窗口，约覆盖最近 30 秒。每次循环根据当前值和趋势计算 `pressure_score`。 | The controller keeps a sliding window of the last 6 samples, covering roughly 30 seconds. Each loop computes a `pressure_score` from current values and trends. |
| 当前 memcached cores 的平均利用率较高时加分。整机平均利用率较高时加分。单个 core 过热时加分。softirq 较高时加分。normalized load 较高时加分。 | The score increases when memcached's assigned cores have high average utilization, when whole-machine utilization is high, when a single core is hot, when softirq is high, or when normalized load is high. |
| 如果最近窗口中的 CPU utilization 有明显上升趋势，调度器额外加分。如果 RX throughput 明显上升，说明 incoming load 可能正在变大，也额外加分。 | If CPU utilization has a clear upward trend in the recent window, the score is increased. If RX throughput rises clearly, incoming load may be increasing, so the score is increased as well. |
| 这个 score 的作用不是精确预测 latency，而是作为 early warning signal。它让 controller 在真正过载之前开始保护 memcached。 | The score is not intended to predict latency exactly. It is an early warning signal that lets the controller protect memcached before real overload happens. |

## 5. Memcached Core Allocation / memcached 核心分配

| 中文 | English |
|---|---|
| 动态策略中 memcached 至少保留 2 个 core。原因是 0.8 ms p95 SLO 非常严格，单 core 在高 QPS 或 packet processing 上升时风险较大。 | In the dynamic policy, memcached keeps at least 2 cores. The reason is that the 0.8 ms p95 SLO is strict, and one core is risky when QPS or packet processing increases. |
| 当 `pressure_score >= 3` 时，memcached 至少使用 3 个 core。这个状态代表系统已经出现上升趋势或中等压力，此时提前保留 headroom。 | When `pressure_score >= 3`, memcached uses at least 3 cores. This indicates rising or moderate pressure, so the scheduler reserves headroom early. |
| 当 `pressure_score >= 6` 时，memcached 使用全部 4 个 core，batch job 会暂停或无法启动。这是 SLO protection mode。 | When `pressure_score >= 6`, memcached uses all 4 cores and batch jobs are paused or not admitted. This is the SLO protection mode. |
| 缩容非常保守。只有连续 4 次低压力 sample，并且距离上一次 resize 至少 30 秒，memcached 才会减少 1 个 core。这样可以避免频繁抖动。 | Shrinking is conservative. Memcached only gives up one core after 4 consecutive low-pressure samples and at least 30 seconds since the previous resize. This prevents oscillation. |

## 6. Batch Job Admission Control / Batch Job 准入控制

| 中文 | English |
|---|---|
| Batch jobs 不再简单地“有空 core 就跑”。调度器会根据当前 pressure 和 job risk 决定是否启动某个 job，以及给它多少 core。 | Batch jobs no longer run simply whenever a core is free. The scheduler uses current pressure and job risk to decide whether to start a job and how many cores to give it. |
| 每个 job 有一个 profile：`risk`、`preferred_cores` 和 `max_cores`。高风险任务包括 `streamcluster`、`freqmine` 和 `canneal`；低风险任务包括 `blackscholes` 和 `radix`。 | Each job has a profile: `risk`, `preferred_cores`, and `max_cores`. High-risk jobs include `streamcluster`, `freqmine`, and `canneal`; low-risk jobs include `blackscholes` and `radix`. |
| 高风险 job 只在压力较低且窗口已有足够 sample 时启动。这样可以避免在 load 刚开始上升但还没有完全显现时启动 heavy batch。 | High-risk jobs are only admitted when pressure is low and enough samples are available in the window. This avoids launching heavy batch work when load is just starting to rise. |
| 中等压力时，调度器可能跳过高风险 job，先运行低风险 job。这使得系统在 memcached 较忙时仍能推进一些轻量 batch work。 | Under moderate pressure, the scheduler may skip high-risk jobs and run low-risk jobs first. This allows progress on lighter batch work while memcached is busy. |
| 当压力上升时，正在运行的 batch job 会先被缩到 1 core。如果压力继续上升到 protection mode，则 batch job 会被 pause。压力下降后再 unpause。 | When pressure rises, the running batch job is first reduced to 1 core. If pressure reaches protection mode, the batch job is paused. It is unpaused when pressure falls again. |

## 7. Resource Control Mechanisms / 资源控制机制

| 中文 | English |
|---|---|
| memcached 直接运行在 VM 上，调度器用 `taskset -a -cp` 调整 memcached 进程及其线程的 CPU affinity。 | Memcached runs directly on the VM. The controller uses `taskset -a -cp` to change CPU affinity for the memcached process and its threads. |
| Batch jobs 用 Docker 启动，调度器用 `docker container update --cpuset-cpus` 动态调整容器可用 core。 | Batch jobs run in Docker containers. The controller uses `docker container update --cpuset-cpus` to dynamically adjust available cores. |
| 当没有安全 batch core 可用时，调度器用 `docker pause` 暂停容器，释放 CPU 给 memcached；之后用 `docker unpause` 恢复。 | When no safe batch core is available, the controller uses `docker pause` to release CPU to memcached, and later uses `docker unpause` to resume the container. |
| 所有 start、end、update_cores、pause、unpause 和 custom events 都通过课程提供的 logger 写入 `jobs_i.txt`，满足提交格式要求。 | All start, end, update_cores, pause, unpause, and custom events are written to `jobs_i.txt` through the provided logger, matching the required submission format. |

## 8. Expected Benefits / 预期优势

| 中文 | English |
|---|---|
| 与纯 reactive 策略相比，该策略在 CPU utilization、softirq 和 network RX 出现上升趋势时就会提前增加 memcached cores，因此更不容易在 QPS interval 切换后瞬间违反 SLO。 | Compared with a purely reactive policy, this strategy increases memcached cores when CPU utilization, softirq, or network RX starts trending upward, making it less likely to violate the SLO immediately after a QPS interval change. |
| 保守缩容和 cooldown 可以降低 core allocation 频繁震荡的风险。 | Conservative shrinking and cooldown reduce oscillations in core allocation. |
| Job risk profile 让调度器在高压力阶段仍可以选择低风险任务，而不是完全停止所有 batch work。 | Job risk profiles let the scheduler run low-risk jobs during higher-pressure periods instead of stopping all batch work completely. |
| 该策略牺牲了一部分 batch 并行度来换取 memcached SLO 稳定性。考虑到评分目标要求 SLO 不能被破坏，这个 trade-off 是合理的。 | The strategy sacrifices some batch parallelism for memcached SLO stability. Since the scoring objective requires the SLO to be preserved, this trade-off is reasonable. |

## 9. Limitations / 局限性

| 中文 | English |
|---|---|
| 当前策略仍然没有直接读取 mcperf 的实时 p95 latency，因为 dynamic mcperf 的结果通常在运行结束后输出。因此 controller 使用系统级信号作为 latency risk 的代理。 | The current policy still does not read real-time mcperf p95 latency, because dynamic mcperf usually prints output at the end. Therefore the controller uses system-level signals as proxies for latency risk. |
| 如果未来能够使用固定 QPS trace 文件，controller 可以提前读取 trace，并在 QPS jump 之前主动扩容。这会比当前趋势检测更强。 | If a fixed QPS trace file is available, the controller could read it in advance and expand before QPS jumps. This would be stronger than trend-based detection. |
| 当前实现一次只运行一个 batch container。这样更稳定、日志更清晰，但可能不是 makespan 最优。未来可以扩展到多个低风险 job 并发运行。 | The current implementation runs only one batch container at a time. This is more stable and keeps logs clear, but may not minimize makespan. Future work could run multiple low-risk jobs concurrently. |
| Job risk profile 是基于经验设置的，后续可以用 Part 2/Part 3 的真实 runtime 和 interference 数据自动校准。 | Job risk profiles are heuristic. They can later be calibrated automatically using real runtime and interference measurements from Part 2 and Part 3. |

## 10. Future Improvements / 后续改进方向

| 中文 | English |
|---|---|
| 第一，可以加入 trace-aware prediction。如果 mcperf 使用确定性的 load trace，scheduler 可以在每次 QPS jump 前提前调整 memcached cores。 | First, add trace-aware prediction. If mcperf uses a deterministic load trace, the scheduler can adjust memcached cores before each QPS jump. |
| 第二，可以加入 EWMA 或 PID controller，用连续的 pressure error 来平滑调整 core allocation，而不是只用离散阈值。 | Second, add an EWMA or PID controller to smooth core allocation using continuous pressure error instead of discrete thresholds only. |
| 第三，可以建立 job performance model，估计每个 job 在 1 core 和 2 cores 下的 speedup，再决定什么时候值得冒险给 batch 更多 core。 | Third, build a job performance model to estimate each job's speedup on 1 vs. 2 cores, then decide when it is worth giving batch more cores. |
| 第四，可以支持多个 batch containers 并发运行。例如在低负载时同时运行两个 low-risk jobs，各占 1 core。 | Fourth, support multiple concurrent batch containers. For example, during low load, two low-risk jobs could each use 1 core. |
| 第五，可以把 memcached 的 `stats` 或 system-level socket counters 纳入决策，获得比 CPU utilization 更接近 tail latency 的信号。 | Fifth, include memcached `stats` or system-level socket counters to obtain signals closer to tail latency than CPU utilization. |
