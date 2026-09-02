# Mini Runtime

Mini Runtime 是一个使用 Go 实现、由预定义 Graph 驱动的最小任务执行 Runtime。

它用于验证任务调度、并发执行、结果评估、状态提交和 Checkpoint Recovery 的完整运行闭环，并明确各模块之间的职责边界。

它不是 Agent Runtime Planner，当前不负责理解自然语言需求，也不会自动将 Query 拆解为 Graph。

## 运行环境

- Go 1.26.4
- 不依赖数据库、消息队列或其他外部服务
- 不依赖第三方 Go Module

## 快速开始

运行全部测试：

```bash
go test ./...
```

运行竞态检测：

```bash
go test -race ./...
```

## 可复现演示

### Graph A → B → C 成功执行

```bash
go test -run '^TestRuntimeABC$' -v
```

该测试验证节点依赖顺序、最终 `RuntimeSuccess`、三次 Checkpoint，以及 `CompleteNodes` 从 A、A+B 推进到 A+B+C。RunMetrics 应记录 3 个步骤、3 次 Tool 成功和 3 次成功 Checkpoint。

### Tool 超时并记录失败状态

```bash
go test -run '^TestRuntimeRecordsToolTimeoutAsFailure$' -v
```

该测试验证 Context 超时不会被误判为任务成功：节点进入 `FailedNodes`，Runtime 返回失败，失败事实被保存。RunMetrics 应记录 1 个步骤、1 次 Tool 失败和 1 次成功 Checkpoint。

### 从 Checkpoint 恢复并跳过已完成节点

```bash
go test -run '^TestRecoverySkipsCompletedNode$' -v
```

该测试从已完成 A 的 Snapshot 恢复，只执行 B、C，最终完成 A+B+C。恢复过程不会重复执行已经提交的节点 A。

## 运行链路

```text
Graph + Snapshot
→ Scheduler
→ ReadyTaskSet
→ Worker
→ Executor
→ TaskResult
→ Evaluator
→ TaskOutcome
→ Committer
→ Candidate Snapshot
→ Checkpointer
→ Committed Snapshot
→ StatusDeterminer
→ Runtime Loop 继续或结束
```

## 核心模块职责

### Scheduler

Scheduler 读取只读的 Graph 和当前 RuntimeSnapshot，根据节点依赖关系及已完成节点计算下一批可执行任务，输出 ReadyTaskSet。

它只负责回答“下一批可以执行哪些 Task”，不负责执行任务、评估结果、修改 Snapshot 或决定 Runtime 的最终状态。

### Executor

Runtime Executor 接收 Context 和 Task，执行任务并返回 TaskResult。ToolTaskExecutor 会把 Task 转换为 Invocation，通过内部 ToolExecutor 调用已注册的 Tool，并将 ExecutionResult 包装回 TaskResult。

Runtime Executor 的数据边界是 `Task → TaskResult`，内部 ToolExecutor 的数据边界是 `Invocation → ExecutionResult`。Executor 不负责根据业务 Criteria 判断 Task 是否完成，也不负责修改 RuntimeSnapshot。

### Evaluator / TaskPolicy

Evaluator 读取 TaskResult，并根据执行结果、业务输出和 Task 中的 Criteria 生成 TaskOutcome。

它负责判断单个任务是否成功，并在失败时记录 Err 或 Reason；不负责更新 RuntimeSnapshot、持久化状态或决定整个 Runtime 是否成功。其中，Err 表示执行失败、输入无效或无法完成评估；Reason 表示能够完成评估，但业务结果不满足 Criteria。

### Committer

Committer 读取当前 RuntimeSnapshot 和一批 TaskOutcome，生成新的 Candidate Snapshot。它不会直接修改当前 Snapshot。

Candidate Snapshot 只有在 Checkpointer 保存成功后，才会升级为 Runtime 当前状态；如果保存失败，Runtime 仍保留原来的已提交 Snapshot。这可以避免内存状态领先于可恢复状态，防止 Checkpoint 失败污染已经提交的运行历史。

```text
Current Snapshot → Candidate Snapshot → Checkpoint Save → Committed Snapshot
```

### Checkpointer

Checkpointer 提供两个能力：

- `Save(snapshot RuntimeSnapshot) error`：保存 Candidate Snapshot。
- `Load(runtimeID string) (RuntimeSnapshot, error)`：根据 RuntimeID 读取最近一次已保存的 RuntimeSnapshot。

Runtime 只有在 Save 成功后，才把 Candidate Snapshot 升级为当前状态。Recovery 使用 Load 返回的 Snapshot 继续调度，并跳过已经完成的节点。

### StatusDeterminer

StatusDeterminer 读取 GraphDefinition 和当前 RuntimeSnapshot，根据完成节点、失败节点及 Graph 的整体状态派生 RuntimeStatus。

它不执行任务、不评估单个 TaskResult，也不修改 Snapshot。StatusDeterminer 本身只返回 RuntimeStatus；是否继续循环或返回错误，由 Runtime Loop 根据该状态处理。

当前最小规则是：

```text
存在失败节点        → RuntimeFailed
所有 Graph 节点完成 → RuntimeSuccess
否则                → RuntimeRunning
```

## RunMetrics

每次 `runLoop` 会重新统计本次运行的观测指标：

- `Steps`：收集到的 TaskResult 数量
- `ToolSuccesses`：Tool 执行成功次数
- `ToolFailures`：Tool 执行失败次数
- `Checkpoints`：成功保存 Candidate Snapshot 的次数
- `Duration`：本次运行总耗时

RunMetrics 只记录运行事实，不参与 Evaluator、StatusDeterminer 或 Runtime 成败判断。因此，Tool 执行成功并不代表 Task 一定满足业务 Criteria。

## 当前能力

Mini Runtime v1 接收预先定义的 Graph，并完成以下运行闭环：

1. Scheduler 根据 Graph 依赖关系和当前 Snapshot 计算 ReadyTaskSet。
2. Worker 并发执行就绪任务。
3. Executor 返回执行结果，Evaluator 判断任务结果。
4. Committer 汇总 TaskOutcome 并生成 Candidate Snapshot。
5. Checkpointer 保存成功后，Runtime 才将 Candidate Snapshot 升级为当前状态。
6. Recovery 从已保存的 Snapshot 继续调度，并跳过已经完成的节点。

## 当前边界

- Runtime 不能把自然语言 Query 自动拆解成 Graph；Graph 必须由调用方预先定义。
- 当前没有根据错误类型进行分类重试，也没有为 Tool 外部副作用提供幂等保护，因此不能保证 exactly-once。
- Evaluation 抽象仍然有限：v1 提供基础 TaskPolicy 和酒店搜索示例，但尚未形成可复用的通用评估框架。
- 当前 Checkpointer 是内存实现，只能证明 Snapshot 保存、隔离和恢复语义；进程退出后状态会丢失，不能实现跨进程的 crash recovery。

