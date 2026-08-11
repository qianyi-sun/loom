# Loom Agent Evaluation Platform 中文研究路线图

> 已归档的研究路线图；本文不描述 Loom 当前的产品行为。

截至日期：2026-06-22

本文是 `2026-06-22-agent-eval-platform-research-roadmap.md` 的中文内部讨论版，配套论文语料表见
[`2026-06-22-agent-eval-platform-paper-corpus.md`](2026-06-22-agent-eval-platform-paper-corpus.md)。

核心判断：

> 后续论文方向不应该做“又一个 benchmark”。更强的定位是：Loom 是一个面向现有
> agent benchmark 和真实生产工作负载的 agent evaluation infrastructure / evaluation
> science / AgentEvalOps 平台，重点解决可复现、可诊断、成本感知、多团队治理的
> agent 评测问题。

## 1. 当前项目定位

Loom 现在已经具备一些很适合转化为研究贡献的工程基础：

- 统一的 benchmark、agent、driver、verifier adapter 抽象。
- 事件化 JSONL trajectory 和 ATIF 记录。
- 面向 provider、预算、重试、entitlement 检查的 LLM Gateway。
- 多团队能力：team、billing/cost tracking、角色、run provenance、operator UX。
- 调度与 worker 执行设计，包括 DRF scheduling 和 sandboxed runs。
- catalog-driven benchmark onboarding，覆盖 SWE-bench、LiveCodeBench、BFCL、BrowseComp、
  MMLU-Pro、MBPP、Terminal-Bench、tau-bench 类工作负载。

当前领域的主要缺口是：很多 agent evaluation 工作最后仍然落在分数表、leaderboard
或单个 benchmark environment 上。Loom 更适合被定位为 agent research 和 repeatable
evaluation practice 之间缺失的 operational layer。

## 2. 文献脉络

相关工作可以分为四条线。

### 2.1 从静态 benchmark 到交互式 agent environment

代表性工作：

- 静态/代码/推理类：HumanEval、MBPP、APPS、DS-1000、MMLU、MMLU-Pro、GPQA、
  LiveCodeBench、BigCodeBench。
- 软件工程 agent：SWE-bench、SWE-bench Verified、SWE-agent、UTBoost、CVE-Bench、
  ProjectEval、Terminal-Bench。
- Web/GUI/app agent：WebArena、VisualWebArena、OSWorld、AndroidWorld、AppWorld、
  TheAgentCompany、GAIA、BrowseComp。
- Tool-use 和 function calling：API-Bank、ToolLLM/ToolBench、StableToolBench、BFCL、
  ToolSandbox、tau-bench、tau2-bench、MCP-Bench。

对 Loom 的启发：

这些工作应该被看作 Loom 的 workload，而不是要被 Loom 重新发明的竞争对象。可发表的
问题不是“再做一个任务集”，而是如何在统一的执行与证据模型下运行、比较、诊断和治理
这些已有任务。

### 2.2 从 leaderboard 到 evaluation infrastructure

代表性工作：

- HELM 和 BIG-bench 证明了大规模异构 LLM evaluation infrastructure 本身可以形成研究贡献。
- Chatbot Arena 证明了平台级评测可以通过 ranking methodology、active sampling 和透明性产生影响。
- OpenCompass 和 Evalverse 展示了通用 LLM evaluation tooling 的方向。
- HAL 和 ARE 表明 agent-evaluation infrastructure 已经成为独立研究对象。
- AI Agents That Matter 指出当前 agent evaluation 过度关注 accuracy，忽略 cost，
  缺乏 holdout，且容易混淆 model、agent harness 和 downstream system 的贡献。

对 Loom 的启发：

Loom 必须区别于“能跑很多任务的 harness”。最强的差异化点是：

- trajectory-level causal diagnosis
- contract-checked compatibility
- verifier confidence
- cost-aware evaluation allocation

### 2.3 从 MLOps 到 AgentEvalOps

代表性工作：

- TFX、MLflow、ModelDB、Data Validation for ML 说明，当平台工程形式化 lifecycle、
  metadata、validation 和 reproducibility 时，可以成为系统研究论文。
- Hidden Technical Debt in ML Systems 提供了 hidden coupling、configuration debt、
  monitoring debt 等表述框架。
- Model Cards 和 Datasheets 支持机器可读的文档、透明性和治理 artifacts。

对 Loom 的启发：

Agent evaluation 也有自己的 hidden technical debt：

- benchmark version
- sandbox image
- provider entitlement
- verifier dependency
- task materialization
- network policy
- judge model
- retry policy
- cost ceiling
- artifact retention

Loom 的系统论文可以把这些对象形式化为 contract 和 audit record。

### 2.4 从最终分数到 evaluation integrity

代表性工作：

- LLM-as-a-judge、evaluator bias、TrustJudge、DecodingTrust、UTBoost、StableToolBench
  都说明 evaluator 本身可能有噪声、偏差和不稳定性。
- AgentBoard 和近期 agent evaluation survey 都强调需要 fine-grained、scalable diagnostics。

对 Loom 的启发：

平台不应该只暴露 success/failure 或最终分数，还应该暴露 verifier confidence 和
failure provenance。对于多步 agent，单个最终分数会隐藏中间很多关键失败模式。

## 3. 后续功能路线图

### Phase 0：稳定评测矩阵

目标：让当前 catalog 足够稳定，可以产出研究证据。

建议实现：

- 把 matrix tracker 做成一等对象：benchmark x agent x provider x worker image x verifier。
- provider entitlement 和 model-readiness preflight。
- license allowlist 和 dataset-source readiness check。
- structured malformed-output diagnostics。
- service-mode agent 支持 worker image 内运行。
- script/oracle compatibility check。
- high-concurrency finalize/writeback hardening。

研究价值：

把当前 operational issue 转化为可观测现象。每个失败都应该变成 typed compatibility
或 reliability signal，而不是 opaque run crash。

### Phase 1：Run Observatory

目标：把 ATIF 和 JSONL trajectory 变成可查询、可分析的诊断数据。

建议实现：

- trajectory query API，覆盖 run、task、step、tool call、verifier event、retry、cost、
  artifact。
- 派生特征：token/cost curve、action entropy、tool-error rate、patch/test chronology、
  verifier rerun agreement、timeout phase、sandbox fault class。
- failure taxonomy：environment、benchmark materialization、provider、agent protocol、
  tool use、reasoning/planning、verifier、scheduler、artifact writeback。
- cross-run comparison：同一 task 跨 agent/provider，同一 agent 跨 benchmark version，
  同一 task 在 verifier 改动前后对比。

研究价值：

这是 trace-causal agent evaluation 论文的核心基础：用结构化 trajectory 预测、定位和解释
agent failure，而不是只看最终分数。

### Phase 2：Compatibility and Readiness Compiler

目标：把“这个 run 能不能安全、可复现地执行”变成可编译的 contract，而不是人工 checklist。

建议实现 `EvaluationContract` schema，包含：

- benchmark adapter version
- task source 和 materializer
- worker image 和 verifier dependencies
- sandbox/network policy
- required agent mode
- provider/model entitlement
- license 和 data-use policy
- artifact retention expectation
- cost/rate ceiling

还应支持：

- enqueue 前 contract validation。
- benchmark version 变化时 contract diff。
- run card 中记录 contract evidence。

研究价值：

这可以支撑一篇系统论文：contract-checked agent evaluation infrastructure。类比对象是
Data Validation for ML，但被验证的对象不是数据表，而是完整的 agent-evaluation execution
plan。

### Phase 3：Verifier Confidence Layer

目标：让 evaluator 自身的不确定性成为平台输出的一部分。

建议实现：

- verifier cards：deterministic/scripted、unit-test、state-based、LLM judge、hybrid。
- verifier confidence fields：agreement、rerun stability、oracle coverage、judge model、
  judge prompt hash、confidence interval、known blind spots。
- weak-verifier detection：flaky tests、过宽 oracle、model-judge self-preference、
  inconsistent state checks。
- 低 confidence outcome 自动触发 diagnostic rerun。

研究价值：

可以形成 evaluation integrity 方法论文：什么时候一个 pass/fail label 值得信任，平台如何
检测低可信评测结果。

### Phase 4：Cost-Aware Reliable Evaluation

目标：在有限预算下最大化结论可靠性，而不是盲目增加 run 数量。

建议实现：

- stochastic agent 的 pass^k 和 repeated-run stability tracking。
- sequential allocation：明显胜负提前停止，不确定比较投入更多预算。
- 面向 team、provider、benchmark class 的 cost-aware DRF scheduling。
- report 中展示 ranking confidence 和 pairwise uncertainty。

研究价值：

这是 AI Agents That Matter 和 Chatbot Arena 类 ranking methodology 的自然扩展：
agent evaluation 应该在显式 cost 和 uncertainty budget 下报告性能。

### Phase 5：Auditable AgentEvalOps

目标：让多团队 agent evaluation 对 researcher、operator 和 governance reviewer 都可解释、可审计。

建议实现：

- run cards、benchmark cards、verifier cards、provider cards。
- 不可变 provenance：model/provider、prompt、adapter version、container digest、
  contract hash、artifact manifest、cost summary。
- trajectory 和 provider log 的 retention/redaction policy。
- team-scoped audit views 和 release snapshots。

研究价值：

单独看算法新意不如 Phase 1/2 强，但对真实部署和治理很有价值。最好作为主论文的支撑贡献，
而不是单独作为顶会主线。

## 4. 顶会论文创新点排序

### 方向 A：Trace-Causal Agent Evaluation

核心论点：

长程 agent evaluation 的最终分数过于粗糙。结构化 trajectory evidence 可以定位失败原因、
解释分数差异，并预测什么时候值得投入更多 evaluation runs。

方法：

- 定义覆盖 agent action、tool call、verifier event、sandbox event、provider event、
  cost、artifact 的 trajectory schema。
- 基于 ATIF trajectory 构建 failure attribution rules/models。
- 使用已有工作负载做实验：SWE-bench Verified、LiveCodeBench、BFCL、BrowseComp、
  Terminal-Bench、tau-bench 或 ToolSandbox，再加 MBPP/MMLU-Pro 作为静态对照。
- 与 final-score-only analysis、benchmark-specific logs、generic observability 做对比。

预期贡献：

- 跨 benchmark 的 agent failure taxonomy。
- 基于 trajectory 的诊断方法。
- 证明 trajectory-level diagnosis 可以提升 debugging efficiency、rerun allocation 或
  failure prediction，而且不需要创建新 benchmark。

评分：

| 维度 | 分数 | 说明 |
|---|---:|---|
| 新颖性 | 5 | 超出 harness/leaderboard，也超出单个 benchmark log 分析。 |
| 可行性 | 4 | Loom 已有 trajectory 基础，需要 taxonomy、query layer 和实验。 |
| 影响力 | 5 | 对 agent researcher 和 platform operator 都直接有用。 |
| 证据潜力 | 5 | 当前 matrix failure 可直接作为 label 和 ablation 起点。 |
| 风险 | 4 | 需要清楚地区分 HAL 和 AgentBoard。 |

推荐 venue：

- MLSys
- NeurIPS Datasets and Benchmarks
- ICLR
- ICML/ICLR/NeurIPS evaluation 或 systems workshop 作为前置版本

建议：

这是最值得押注的主论文方向。

### 方向 B：Contract-Checked Agent Evaluation Infrastructure

核心论点：

agent evaluation 的许多失败来自 benchmark、sandbox、agent protocol、model entitlement、
verifier 和 artifact policy 之间的隐藏不兼容。typed evaluation contract 可以提前阻止无效
run，并让平台结果可复现。

方法：

- 形式化 `EvaluationContract`。
- 从 catalog manifests 和 runtime environment 编译 contract。
- 衡量 prevented invalid runs、operator intervention reduction、flaky failure reduction、
  cross-machine/worker-image reproducibility improvement。

评分：

| 维度 | 分数 | 说明 |
|---|---:|---|
| 新颖性 | 4 | 如果形式化足够清楚，是很强的系统贡献。 |
| 可行性 | 5 | 和当前 readiness/license/preflight issues 高度一致。 |
| 影响力 | 4 | 解决 agent-eval platform 的真实痛点。 |
| 证据潜力 | 5 | 现有 issue set 可以提供 before/after metrics。 |
| 风险 | 2 | 比方向 A 更稳，但如果实验不足会被看作纯工程。 |

推荐 venue：

- MLSys
- VLDB/SIGMOD industry 或 systems track
- ICSE/AIware
- NeurIPS Datasets and Benchmarks

建议：

这是最稳的工程优先论文方向，也可以作为方向 A 的系统底座。

### 方向 C：Cost-Aware Reliable Agent Evaluation

核心论点：

agent evaluation 应该在 bounded cost 下优化 reliable conclusion，而不是最大化 raw run count。

方法：

- 跟踪 stochastic run variance 和 pass^k。
- 使用 sequential testing 或 bandit-style allocation 处理不确定 model/task pairs。
- 报告 ranking confidence 和 cost-normalized utility。

评分：

| 维度 | 分数 | 说明 |
|---|---:|---|
| 新颖性 | 4 | cost-aware agent evaluation 仍然不充分。 |
| 可行性 | 4 | 需要 repeated runs 和一定预算。 |
| 影响力 | 5 | 对任何真实支付 provider cost 的团队都很重要。 |
| 证据潜力 | 4 | 需要设计良好的实验矩阵。 |
| 风险 | 3 | 不能只变成 scheduling optimization。 |

推荐 venue：

- NeurIPS/ICML evaluation methodology
- MLSys
- ICLR

建议：

适合作为第二篇论文，或作为方向 A 的重要 ablation。

### 方向 D：Verifier Confidence Layer

核心论点：

benchmark pass/fail label 并不等价可信。agent-evaluation platform 应该估计并报告 verifier
confidence。

方法：

- 分类 verifier type。
- 估计 agreement、stability、coverage、judge bias。
- 对低 confidence outcome 触发 diagnostic rerun。

评分：

| 维度 | 分数 | 说明 |
|---|---:|---|
| 新颖性 | 4 | LLM-as-judge reliability 让这个方向很及时。 |
| 可行性 | 3 | 需要 verifier-specific instrumentation。 |
| 影响力 | 4 | 有用，但比 trajectory-causal diagnosis 窄。 |
| 证据潜力 | 5 | 相关工作和评估指标都比较清楚。 |
| 风险 | 4 | 需要严谨处理 LLM judge 和 scripted verifier。 |

推荐 venue：

- ACL/EMNLP evaluation
- NeurIPS Datasets and Benchmarks
- MLSys

建议：

如果 Loom 的 verifier protocol 很快成为主线，这个方向值得推进。

### 方向 E：Auditable AgentEvalOps

核心论点：

多团队 agent evaluation 需要机器可读 provenance 和 governance artifacts，才能让结果可复现、
可问责、可运营。

方法：

- 定义 run cards、benchmark cards、verifier cards、provider cards。
- 展示这些 cards 如何支持 reproducibility、policy compliance 和 incident/debug workflows。

评分：

| 维度 | 分数 | 说明 |
|---|---:|---|
| 新颖性 | 3 | 应用价值强，但单独做技术新颖性偏弱。 |
| 可行性 | 5 | 主要是平台和产品工程。 |
| 影响力 | 4 | 对 enterprise 和 lab operations 有价值。 |
| 证据潜力 | 3 | 最好有部署证据或 user study。 |
| 风险 | 2 | 工程风险低，但单独投稿风险较高。 |

推荐 venue：

- FAccT
- CSCW
- MLSys industry track
- data-centric AI venues

建议：

作为主论文的支撑贡献更合适，不建议单独作为顶会主线。

## 5. 推荐主线

最强的顶会尝试应是：

**Trace-Causal Agent Evaluation with Contract-Checked Execution in Loom**

论文结构可以是：

1. 问题：agent evaluation 碎片化、成本高、不透明，最终分数隐藏失败原因。
2. 系统：Loom 用 typed adapters、contracts、trajectories、verifiers、gateway 和 scheduler
   统一已有 benchmark。
3. 方法：trace-causal failure attribution 和 queryable trajectory diagnostics。
4. 实验：
   - 跨 benchmark failure taxonomy coverage。
   - 相对 human/operator labels 的 diagnostic accuracy。
   - contract 上线前后的 invalid/failed run reduction。
   - repeated runs 和 early stopping 下的 cost/reliability tradeoff。
   - SWE-bench、LiveCodeBench、BFCL、BrowseComp、Terminal-Bench case studies。
5. 结论：平台级证据可以提升 reproducibility、debugging 和 cost-aware decision making，
   且不需要提出新 benchmark。

这个方向在研究新意和工程可信度之间最平衡，也最符合 Loom 的真实优势。

## 6. 四到六周执行计划

第 1 周：

- 把当前 matrix issues 转成正式 failure taxonomy。
- 为 readiness、provider、license、verifier、malformed output、sandbox、writeback failure
  增加 typed reason codes。
- 定义 `EvaluationContract v0`。

第 2 周：

- 为当前 catalog entries 实现 contract generation。
- 增加 preflight validation 和 contract hash。
- 把 typed failure events 存入 ATIF/trajectory output。

第 3 周：

- 构建 Run Observatory v0：
  - run/task/step query API
  - cost 和 provider-event summaries
  - failure taxonomy dashboard/table
  - cross-run comparison view

第 4 周：

- 在 5-7 个已有 workload 上跑 controlled matrix：
  - SWE-bench Verified 或 SWE-bench Lite
  - LiveCodeBench
  - BFCL
  - BrowseComp
  - Terminal-Bench
  - tau-bench 或 ToolSandbox，如果 integration ready
  - MBPP/MMLU-Pro 作为 static controls

第 5 周：

- 人工标注一批 failures。
- 比较 trace-causal diagnosis、score-only baseline、benchmark-log-only baseline。
- 衡量 invalid-run prevention 和 cost impact。

第 6 周：

- 起草论文主要章节。
- 冻结 artifact snapshot。
- 准备 ablations：
  - without contracts
  - without trajectory features
  - without verifier confidence
  - final-score-only baseline

## 7. 和相近工作的差异化

相对 HAL：

- HAL 更偏 leaderboard/harness infrastructure，强调大量 rollouts 和 public logs。
- Loom 应强调 typed execution contracts、multi-team operational constraints、
  cost-aware scheduling、full-lifecycle causal failure attribution。

相对 ARE：

- ARE 关注 agent environments 和 evaluations 的规模化。
- Loom 应强调 production evaluation operations、reproducibility contracts 和
  cross-benchmark diagnostics。

相对 OpenHands：

- OpenHands 是 agent-development platform。
- Loom 应是 evaluation-operations platform，可以运行 OpenHands 类 agent，但不绑定单一
  agent interface 或 software-engineering task family。

相对 OpenCompass/Evalverse：

- 这些工作主要面向 model/LLM evaluation。
- Loom 应聚焦 interactive、sandboxed、tool-using、stateful agent runs，并把 artifact、
  verifier、cost、governance 一起纳入平台。

相对 AgentBoard：

- AgentBoard 提供 multi-turn agent 的 analytical evaluation。
- Loom 应贡献让这类分析跨 benchmark family 和跨团队稳定成立的 execution/provenance
  substrate。

## 8. 主要风险

- trace taxonomy 如果没有 human labels 和 operational outcome 验证，可能显得过于手工。
- 系统论文如果 contract 和 diagnostics 没有形式化与量化评估，容易被认为只是工程。
- repeated-run experiments 可能成本较高，需要小而多样的矩阵和 sequential allocation。
- trajectory 可能包含敏感数据或 provider logs，redaction policy 应该成为设计的一部分。
- 引言和实验中必须明确区分 Loom 与 HAL、ARE 的贡献边界。

## 9. 近期工程优先级

最高优先级：

1. 把 compatibility/readiness matrix 收敛到可以稳定运行 5-7 个 benchmark families。
2. 让每个 failed run 都产生 typed reason code 和 evidence pointer。
3. 在 ATIF output 中加入 contract hash、environment/provider/verifier metadata。
4. 构建 trajectory query 和 cross-run comparison primitives。
5. 在 report 中加入 cost 和 uncertainty summaries。

暂不优先：

- 创建新的 benchmark dataset。
- 把 leaderboard 当成主要贡献。
- 在当前 integrations 还不能产出高质量 traces 和 failure evidence 前，继续盲目增加更多
  benchmark integrations。

## 10. 关键参考文献

- [Survey on Evaluation of LLM-based Agents](https://arxiv.org/abs/2503.16416)
- [AI Agents That Matter](https://openreview.net/forum?id=Zy4uFzMviZ)
- [Holistic Agent Leaderboard](https://arxiv.org/abs/2510.11977)
- [ARE: Scaling Up Agent Environments and Evaluations](https://arxiv.org/abs/2509.17158)
- [OpenHands](https://openreview.net/forum?id=OJd3ayDDoF)
- [SWE-bench](https://openreview.net/forum?id=VTF8yNQM66)
- [WebArena](https://openreview.net/forum?id=oKn9c6ytLx)
- [OSWorld](https://openreview.net/forum?id=tN61DTr4Ed)
- [AppWorld](https://aclanthology.org/2024.acl-long.850/)
- [ToolSandbox](https://aclanthology.org/2025.findings-naacl.65/)
- [tau-bench](https://openreview.net/forum?id=roNSXZpUDN)
- [tau2-bench](https://openreview.net/forum?id=LGmO9VvuP5)
- [LiveCodeBench](https://openreview.net/forum?id=chfJJYC3iL)
- [BigCodeBench](https://openreview.net/forum?id=YrycTjllL0)
- [HELM](https://openreview.net/forum?id=iO4LZibEqW)
- [Chatbot Arena](https://arxiv.org/abs/2403.04132)
- [Judging LLM-as-a-Judge](https://openreview.net/forum?id=uccHPGDlao)
- [LLM Evaluators Recognize and Favor Their Own Generations](https://openreview.net/forum?id=4NJBV6Wp0h)
- [TrustJudge](https://openreview.net/forum?id=4uPyOCeN6U)
- [OpenCompass](https://arxiv.org/html/2605.19276)
- [TFX](https://www.kdd.org/kdd2017/papers/view/tfx-a-tensorflow-based-production-scale-machine-learning-platform)
- [MLflow](https://people.eecs.berkeley.edu/~matei/papers/2018/ieee_mlflow.pdf)
- [Data Validation for Machine Learning](https://proceedings.mlsys.org/paper_files/paper/2019/hash/928f1160e52192e3e0017fb63ab65391-Abstract.html)
- [Hidden Technical Debt in Machine Learning Systems](https://papers.neurips.cc/paper/5656-hidden-technical-debt-in-machine-learning-systems)
- [Datasheets for Datasets](https://dl.acm.org/doi/10.1145/3458723)
- [Model Cards for Model Reporting](https://dl.acm.org/doi/10.1145/3287560.3287596)
