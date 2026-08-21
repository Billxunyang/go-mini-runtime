package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

type GraphDefinition struct {

	// Graph definition
	// TODO: define graph definition
	Edges []Edge
	Nodes []Node
}

type Node struct {
	ID        string
	Name      string
	ToolName  string
	Arguments map[string]any
}

type Edge struct {
	From   string
	To     string
	NodeId string
}

type RuntimeSnapshot struct {
	// Runtime snapshot
	// TODO: define runtime snapshot
	RuntimeID     string
	CompleteNodes map[string]bool
	Status        RuntimeStatus
	FailedNodes   map[string]bool
}

type Task struct {
	// Task
	// TODO: define task
	NodeID    string
	ToolName  string
	Arguments map[string]any
}

type ReadyTaskSet struct {
	// Ready task set
	// TODO: define ready task set
	Tasks []Task
}

type Scheduler interface {
	// Schedule a new task
	Schedule(graph GraphDefinition, snapshot RuntimeSnapshot) (ReadyTaskSet, error)
}

type FakeScheduler struct {
}

// Schedule
func (s *FakeScheduler) Schedule(graph GraphDefinition, snapshot RuntimeSnapshot) (ReadyTaskSet, error) {
	// find not running finished nodes
	readyTaskSet := ReadyTaskSet{Tasks: make([]Task, 0)}
	if len(graph.Nodes) == 0 {
		return readyTaskSet, fmt.Errorf("no nodes in graph")
	}
	for _, node := range graph.Nodes {
		// if finish continue
		if snapshot.CompleteNodes[node.ID] {
			continue
		}
		allDependenciesCompleted := true
		for _, edge := range graph.Edges {
			if edge.To != node.ID {
				continue
			}
			if edge.From == "" {
				continue
			}
			if !snapshot.CompleteNodes[edge.From] {
				allDependenciesCompleted = false
				break
			}
		}
		if allDependenciesCompleted {
			readyTaskSet.Tasks = append(readyTaskSet.Tasks, Task{
				NodeID:    node.ID,
				ToolName:  node.ToolName,
				Arguments: node.Arguments,
			})
		}
	}
	return readyTaskSet, nil
}

// tasks -> dispatch -> execute

type TaskResult struct {
	// Task result
	// TODO: define task result
	NodeID          string
	Err             error
	ExecutionResult ExecutionResult
}
type Executor interface {
	// Execute a task return a taskResult
	Execute(ctx context.Context, task Task) TaskResult
}

type FakeExecutor struct {
}

func (fe *FakeExecutor) Execute(ctx context.Context, task Task) TaskResult {
	fmt.Println("执行任务", task.NodeID)
	return TaskResult{NodeID: task.NodeID, Err: nil}
}

type FakeTaskPolicy struct {
}

func (tp *FakeTaskPolicy) Evaluate(result TaskResult) TaskOutcome {
	if result.Err != nil {
		return TaskOutcome{
			NodeID:  result.NodeID,
			Err:     result.Err,
			Success: false,
		}
	}
	if result.ExecutionResult.Err != nil {
		return TaskOutcome{NodeID: result.NodeID, Success: false, Err: result.ExecutionResult.Err}
	}
	return TaskOutcome{NodeID: result.NodeID, Success: true}
}

type TaskOutcome struct {
	// Task outcome
	// TODO: define task outcome
	NodeID  string
	Success bool
	Err     error
}
type TaskPolicy interface {
	// Task policy
	// TODO: define task policy
	Evaluate(result TaskResult) TaskOutcome
}

// taskOutcome -> batch ->commiter
// Committer not noly aggrate result but also generate new snapshot history
// ->fact alse become new history
type Committer interface {
	// Commit  task outcomes
	Commit(snapshot RuntimeSnapshot, outcomes []TaskOutcome) (RuntimeSnapshot, error)
}

type FakeCommitter struct {
}

func (fc *FakeCommitter) Commit(snapshot RuntimeSnapshot, outcomes []TaskOutcome) (RuntimeSnapshot, error) {
	for _, outcome := range outcomes {
		if outcome.Success {
			snapshot.CompleteNodes[outcome.NodeID] = true
			delete(snapshot.FailedNodes, outcome.NodeID)
			continue
		}
		snapshot.FailedNodes[outcome.NodeID] = true
	}
	return snapshot, nil
}

type FakeCheckpointer struct {
}

func (f *FakeCheckpointer) Save(snapshot RuntimeSnapshot) error {
	return nil
}

func (f *FakeCheckpointer) Load(runtimeId string) (RuntimeSnapshot, error) {
	return RuntimeSnapshot{
		RuntimeID:     runtimeId,
		CompleteNodes: make(map[string]bool),
		FailedNodes:   make(map[string]bool),
	}, nil
}

type Checkpointer interface {
	// Save save   snapshot to checkpoint
	Save(snapshot RuntimeSnapshot) error
	// Load load snapshot from checkpoint
	Load(runtimeId string) (RuntimeSnapshot, error)
}
type RuntimeStatus string

const (
	RuntimeRunning  RuntimeStatus = "running"
	RuntimeWaiting  RuntimeStatus = "waiting"
	RuntimeSuccess  RuntimeStatus = "success"
	RuntimeFailed   RuntimeStatus = "failed"
	RuntimeDeadLock RuntimeStatus = "deadlock"
)

type StatusDeterminer interface {
	// Decision  decide weather need reschedule
	Decision(definition GraphDefinition, snapshot RuntimeSnapshot) RuntimeStatus
}

type Runtime struct {
	waitGroup        sync.WaitGroup
	graph            GraphDefinition
	scheduler        Scheduler
	executor         Executor
	taskPolicy       TaskPolicy
	committer        Committer
	checkpointer     Checkpointer
	statusDeterminer StatusDeterminer
	// Runtime
	// TODO: define runtime
	TaskQueue   chan Task
	ResultQueue chan TaskResult
	workerNum   int
}

func (r *Runtime) initWorker(workerNum int) {
	r.workerNum = workerNum
	r.TaskQueue = make(chan Task, workerNum)
	r.ResultQueue = make(chan TaskResult, workerNum)
}

func (r *Runtime) startWorker(ctx context.Context) {
	for i := 0; i < r.workerNum; i++ {
		r.waitGroup.Add(1)
		go func() {
			defer r.waitGroup.Done()
			for taskInfo := range r.TaskQueue {
				result := r.executor.Execute(ctx, taskInfo)
				r.ResultQueue <- result
			}
		}()
	}
}

// dispatch new
func (r *Runtime) executeReadyTasks(taskSet ReadyTaskSet) (taskOutcomes []TaskOutcome) {
	res := make([]Task, 0)
	taskOutcomes = make([]TaskOutcome, 0)
	for len(taskSet.Tasks) > 0 {
		if len(taskSet.Tasks) > r.workerNum {
			res = taskSet.Tasks[:r.workerNum]
			taskSet.Tasks = taskSet.Tasks[r.workerNum:]
		} else {
			res = taskSet.Tasks
			taskSet.Tasks = []Task{}
		}

		for _, task := range res {
			r.TaskQueue <- task
		}
		taskOutcomes = append(taskOutcomes, r.collect(len(res))...)
	}
	return
}

func (r *Runtime) collect(count int) []TaskOutcome {
	taskOutcomeList := make([]TaskOutcome, 0)
	for i := 0; i < count; i++ {
		taskOutcomeList = append(taskOutcomeList, r.taskPolicy.Evaluate(<-r.ResultQueue))
	}
	return taskOutcomeList
}

func (r *Runtime) stopWorker() {
	close(r.TaskQueue)
	r.waitGroup.Wait()
	close(r.ResultQueue)
}

func (r *Runtime) runLoop(ctx context.Context, snapshot RuntimeSnapshot) (newSnapshot RuntimeSnapshot, err error) {
	valid, validateErr := ValidateGraph(r.graph)
	if validateErr != nil {
		err = validateErr
		return
	}
	if !valid {
		err = fmt.Errorf("invalid graph")
		return
	}
	var taskSet ReadyTaskSet
	r.initWorker(r.workerNum)
	defer r.stopWorker()
	r.startWorker(ctx)
	needContinue := false
	newSnapshot = snapshot
	for {
		taskSet, err = r.scheduler.Schedule(r.graph, newSnapshot)
		if err != nil {
			fmt.Println("get scheduler failed ", err)
			return
		}
		if len(taskSet.Tasks) == 0 {
			needContinue, err = r.decideLoop(&newSnapshot)
			if needContinue {
				continue
			} else {
				return
			}
		}
		taskOutcomes := r.executeReadyTasks(taskSet)
		newSnapshot, err = r.committer.Commit(newSnapshot, taskOutcomes)
		if err != nil {
			fmt.Println("commit err ", err)
			return
		}
		err = r.checkpointer.Save(newSnapshot)
		if err != nil {
			fmt.Println("checkpointer err ", err)
			return
		}
		needContinue, err = r.decideLoop(&newSnapshot)
		if needContinue {
			continue
		} else {
			return
		}
	}
}

func (r *Runtime) decideLoop(snapshot *RuntimeSnapshot) (loopContinue bool, err error) {
	loopContinue = false
	runStatus := r.statusDeterminer.Decision(r.graph, *snapshot)
	switch runStatus {
	case RuntimeSuccess:
		snapshot.Status = RuntimeSuccess
		return
	case RuntimeFailed:
		snapshot.Status = RuntimeFailed
		err = fmt.Errorf("runtime failed ")
		return
	case RuntimeWaiting:
		snapshot.Status = RuntimeWaiting
		err = fmt.Errorf("runtime waiting ")
		return
	case RuntimeDeadLock:
		snapshot.Status = RuntimeDeadLock
		err = fmt.Errorf("runtime deadlock")
		return
	case RuntimeRunning:
		snapshot.Status = RuntimeRunning
		loopContinue = true
	default:
		err = fmt.Errorf("runtime unknown")
		return
	}
	return
}

func NewRuntimeTest() (err error) {
	ctx := context.Background()
	nRun := &Runtime{
		graph: GraphDefinition{
			Nodes: []Node{{ID: "A", Name: "node-A"}, {ID: "B", Name: "node-B"}, {ID: "C", Name: "node-C"}},
			Edges: []Edge{{From: "A", To: "B", NodeId: "B"}, {From: "B", To: "C", NodeId: "C"}},
		},
		workerNum: 2,
	}
	snapshot := RuntimeSnapshot{
		CompleteNodes: make(map[string]bool),
		FailedNodes:   make(map[string]bool),
	}
	fakeSchedule := FakeScheduler{}
	readyTaskSet, err := fakeSchedule.Schedule(nRun.graph, snapshot)
	if err != nil {
		fmt.Println("get schedule err ", err.Error())
		return
	}
	fakeExecutor := FakeExecutor{}
	fakeTaskPolicy := FakeTaskPolicy{}
	fakeCommiter := FakeCommitter{}
	fakeCheckpointer := FakeCheckpointer{}
	fakeDecision := FakeDecision{}
	for _, taskInfo := range readyTaskSet.Tasks {
		taskResult := fakeExecutor.Execute(ctx, taskInfo)
		taskOutcom := fakeTaskPolicy.Evaluate(taskResult)
		snapshot, err = fakeCommiter.Commit(snapshot, []TaskOutcome{taskOutcom})
		if err != nil {
			fmt.Println("get schedule err ", err.Error())
			return
		}
		err = fakeCheckpointer.Save(snapshot)
		if err != nil {
			fmt.Println("get schedule err ", err.Error())
			return
		}
	}
	decisionStatus := fakeDecision.Decision(nRun.graph, snapshot)
	fmt.Println(decisionStatus)
	return
}

func NewRuntime(graph GraphDefinition, workerNum int, scheduler Scheduler, executor Executor, taskPolicy TaskPolicy, committer Committer, checkpointer Checkpointer, statusDeterminer StatusDeterminer) *Runtime {
	return &Runtime{
		graph:            graph,
		scheduler:        scheduler,
		executor:         executor,
		taskPolicy:       taskPolicy,
		committer:        committer,
		checkpointer:     checkpointer,
		statusDeterminer: statusDeterminer,
		workerNum:        workerNum,
	}
}

//1. 实现`FakeStatusDeterminer.Decision`。
//  2. 最小规则：Graph中所有Node都在`CompleteNodes`时返回`RuntimeSuccess`，否则返回`RuntimeRunning`。
//  3. 在`NewRuntime()`中注入`FakeScheduler`、`FakeExecutor`、`FakeTaskPolicy`、`FakeCommitter`、`FakeCheckpointer`、`FakeStatusDeterminer`。
//  4. 构造初始Snapshot时初始化`RuntimeID`、`CompleteNodes`和`Status`。

type FakeDecision struct {
}

func (fd *FakeDecision) Decision(definition GraphDefinition, snapshot RuntimeSnapshot) RuntimeStatus {
	for _, failed := range snapshot.FailedNodes {
		if failed {
			return RuntimeFailed
		}
	}

	for _, nodeInfo := range definition.Nodes {
		if completeValue, ok := snapshot.CompleteNodes[nodeInfo.ID]; !ok || !completeValue {
			return RuntimeRunning
		}
	}
	return RuntimeSuccess
}

func main() {
	graph := GraphDefinition{
		Nodes: []Node{{ID: "A", Name: "node-A"}, {ID: "B", Name: "node-B"}, {ID: "C", Name: "node-C"}},
		Edges: []Edge{{From: "A", To: "B", NodeId: "B"}, {From: "B", To: "C", NodeId: "C"}},
	}
	fakeSchedule := FakeScheduler{}
	fakeExecutor := FakeExecutor{}
	fakeTaskPolicy := FakeTaskPolicy{}
	fakeCommiter := FakeCommitter{}
	fakeCheckpointer := FakeCheckpointer{}
	fakeDecision := FakeDecision{}
	runtime := NewRuntime(graph, 8, &fakeSchedule, &fakeExecutor, &fakeTaskPolicy, &fakeCommiter, &fakeCheckpointer, &fakeDecision)
	ctx := context.Background()
	snapshot := RuntimeSnapshot{
		RuntimeID:     "1",
		CompleteNodes: make(map[string]bool),
		FailedNodes:   make(map[string]bool),
		Status:        RuntimeRunning,
	}
	newSnapshot, err := runtime.runLoop(ctx, snapshot)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(newSnapshot)
}

func ValidateGraph(definition GraphDefinition) (res bool, err error) {
	res = true
	// is empty
	if len(definition.Nodes) == 0 {
		res = false
		err = fmt.Errorf("graph is empty")
		return
	}
	allNodeMap := make(map[string]Node)
	for _, node := range definition.Nodes {
		allNodeMap[node.ID] = node
	}
	if len(allNodeMap) != len(definition.Nodes) {
		res = false
		err = fmt.Errorf("duplicated nodes")
		return
	}

	for _, edge := range definition.Edges {
		if len(edge.From) == 0 {
			res = false
			err = fmt.Errorf("node id %s from can not empty", edge.NodeId)
			return
		}

		if edge.From == edge.To {
			res = false
			err = fmt.Errorf("graph cycle")
			return
		}
		if _, ok := allNodeMap[edge.From]; !ok {
			err = fmt.Errorf("from  node %s not exist, node id %s", edge.From, edge.NodeId)
			res = false
			return
		}
		if len(edge.To) == 0 {
			res = false
			err = fmt.Errorf("node id %s to can not empty", edge.NodeId)
			return
		}

		if _, ok := allNodeMap[edge.To]; !ok {
			err = fmt.Errorf("to  node %s not exist, node id %s", edge.To, edge.NodeId)
			res = false
			return
		}
	}
	// from not exist

	// skip cycleCheck
	cycleExist := isAcyclic(definition)
	if !cycleExist {
		res = false
		err = fmt.Errorf("graph cycle")
		return
	}
	return
}

func isAcyclic(graph GraphDefinition) bool {
	operateCount := 0
	operateQueue := make([]string, 0)
	//入度map
	inputDegreeMap := make(map[string]int)
	// 统计节点出边，出队列的时候要对出边--操作
	outputEdge := make(map[string][]string)
	for _, node := range graph.Nodes {
		inputDegreeMap[node.ID] = 0
	}

	for _, edgeInfo := range graph.Edges {
		inputDegreeMap[edgeInfo.To]++
		if _, ok := outputEdge[edgeInfo.From]; !ok {
			outputEdge[edgeInfo.From] = make([]string, 0)
		}
		outputEdge[edgeInfo.From] = append(outputEdge[edgeInfo.From], edgeInfo.To)
	}

	// 入度为0的入队列
	for nodeKey, inputDegreeValue := range inputDegreeMap {
		if inputDegreeValue == 0 {
			operateQueue = append(operateQueue, nodeKey)
		}
	}
	// 出队列
	for len(operateQueue) > 0 {
		currentNode := operateQueue[0]
		operateCount++
		for _, dstNode := range outputEdge[currentNode] {
			inputDegreeMap[dstNode]--
			// 入度将为0 入队列
			if inputDegreeMap[dstNode] == 0 {
				operateQueue = append(operateQueue, dstNode)
			}
		}
		operateQueue = operateQueue[1:]

	}
	return operateCount == len(graph.Nodes)
}

// 注册阶段：
// Tool → Registry.Register(Tool)
//
// 执行阶段：
// Task
//
//	→ ToolExecutor 构造 Invocation
//	→ Registry.Get(Invocation.ToolName)
//	→ Tool.Execute(ctx, Invocation)
//	→ ExecutionResult
//	→ ToolExecutor 转成 TaskResult
//	→ TaskPolicy
//	→ TaskOutcome
//	→ Committer
//	→ RuntimeSnapshot

type ToolDefinition struct {
	Name         string
	Description  string
	InputSchema  []SchemaInfo
	OutputSchema []SchemaInfo
}
type Tool interface {
	Definition() ToolDefinition
	Execute(ctx context.Context, invocation Invocation) ExecutionResult
}

type SchemaInfo struct {
	SchemaName string
	DataType   string
	Required   bool
}

type Invocation struct {
	ID        string
	ToolName  string
	Arguments map[string]any
}
type ExecutionResult struct {
	InvocationID string
	Output       any
	Err          *ExecutionError
}
type ExecutionErrType string

const (
	ExecutionTypeBusinessErr    ExecutionErrType = "business"
	ExecutionTypeToolErr        ExecutionErrType = "tool"
	ExecutionTypeInfrastructure ExecutionErrType = "infrastructure"
)

type ExecutionError struct {
	ErrType ExecutionErrType
	ErrCode string
	ErrMsg  string
}

type ToolRegistry interface {
	Register(tool Tool) error
	Get(name string) (Tool, error)
}

type MemoryToolRegistry struct {
	registerMap map[string]Tool
	lock        sync.RWMutex
}

func NewMemoryToolRegistry() *MemoryToolRegistry {

	return &MemoryToolRegistry{
		registerMap: make(map[string]Tool),
	}
}

func (mTR *MemoryToolRegistry) Register(tool Tool) error {
	// 1. tool == nil → ErrInvalidTool
	if tool == nil {
		return ErrInvalidTool
	}
	// 2. 取得 tool.Definition().Name
	toolName := tool.Definition().Name
	// 3. name == "" → ErrInvalidToolName
	if len(toolName) == 0 {
		return ErrInvalidToolName
	}
	// 4. 获取写锁，defer 解锁
	mTR.lock.Lock()
	defer mTR.lock.Unlock()
	// 5. 如果名字已存在 → ErrToolAlreadyRegistered
	if _, ok := mTR.registerMap[toolName]; ok {
		return ErrToolAlreadyRegistered
	}
	// 6. 写入 registerMap
	mTR.registerMap[toolName] = tool
	// 7. 返回 nil
	return nil
}

func (mTR *MemoryToolRegistry) Get(name string) (Tool, error) {
	if len(name) == 0 {
		return nil, ErrInvalidToolName
	}
	mTR.lock.RLock()
	defer mTR.lock.RUnlock()
	tool, ok := mTR.registerMap[name]
	if !ok {
		return nil, ErrToolNotFound
	}
	return tool, nil
}

var (
	ErrInvalidTool           = errors.New("invalid tool")
	ErrInvalidToolName       = errors.New("invalid tool name")
	ErrToolAlreadyRegistered = errors.New("tool already registered")
	ErrToolNotFound          = errors.New("tool not found")
)

type RegistryToolExecutor struct {
	registry ToolRegistry
}

func NewRegistryToolExecutor(toolRegistry ToolRegistry) *RegistryToolExecutor {
	return &RegistryToolExecutor{
		registry: toolRegistry,
	}
}

func (e *RegistryToolExecutor) Execute(
	ctx context.Context,
	invocation Invocation,
) ExecutionResult {
	//今天先完成成功路径：
	tool, err := e.registry.Get(invocation.ToolName)
	if err != nil {
		return ExecutionResult{
			InvocationID: invocation.ID,
			Err: &ExecutionError{
				ErrType: ExecutionTypeToolErr,
				ErrCode: "tool not found",
				ErrMsg:  err.Error(),
			},
		}
	}
	//用 invocation.ToolName 调用 e.registry.Get()。
	//获取 Tool。
	//调用并返回 tool.Execute(ctx, invocation)。
	result := tool.Execute(ctx, invocation)
	return result
}

type ToolTaskExecutor struct {
	toolExecutor *RegistryToolExecutor
	sequence     atomic.Uint64
}

func NewToolTaskExecutor(toolExecutor *RegistryToolExecutor) *ToolTaskExecutor {
	return &ToolTaskExecutor{
		toolExecutor: toolExecutor,
	}
}

func (e *ToolTaskExecutor) Execute(
	ctx context.Context,
	task Task,
) TaskResult {
	sequence := e.sequence.Add(1)
	invocationID := fmt.Sprintf("%s-%d", task.NodeID, sequence)
	result := e.toolExecutor.Execute(ctx, Invocation{
		ID:        invocationID,
		ToolName:  task.ToolName,
		Arguments: task.Arguments,
	})

	return TaskResult{
		NodeID:          task.NodeID,
		ExecutionResult: result,
	}
}

func (e *ExecutionError) Error() string {
	return fmt.Sprintf(
		"type=%s code=%s message=%s",
		e.ErrType,
		e.ErrCode,
		e.ErrMsg,
	)
}
