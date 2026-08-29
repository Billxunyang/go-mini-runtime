package main

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"sync"
	"testing"
	"time"
)

type RecordingExecutor struct {
	mu        sync.Mutex
	recordIDs []string
}

// 需要实现两个方法：
// Execute(task Task) TaskResult
// 职责：
// 加锁。
// 将 task.NodeID 加入执行记录。
// 解锁。
// 返回成功的 TaskResult。

func (r *RecordingExecutor) Execute(ctx context.Context, task Task) TaskResult {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.recordIDs = append(r.recordIDs, task.NodeID)
	return TaskResult{Task: task}
}

// ExecutedNodes() []string
// 职责：
// 加锁读取。
// 返回执行记录的副本，不能把内部 Slice 直接暴露出去。
// 解锁。

func (r *RecordingExecutor) ExecutedNodes() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	nodeIDs := make([]string, len(r.recordIDs))
	copy(nodeIDs, r.recordIDs)
	return nodeIDs
}

type RecordingCheckpointer struct {
	mu              sync.Mutex
	recordSnapshots []RuntimeSnapshot
}

//Save 的要求：
//加锁。
//深拷贝 Snapshot。
//将副本追加到历史。
//返回 nil。
//这里不能只复制 struct：

func (rC *RecordingCheckpointer) Save(snapshot RuntimeSnapshot) error {
	rC.mu.Lock()
	defer rC.mu.Unlock()
	dstSnapshot := cloneSnapshot(snapshot)
	rC.recordSnapshots = append(rC.recordSnapshots, dstSnapshot)
	return nil
}

func cloneSnapshot(snapshot RuntimeSnapshot) RuntimeSnapshot {
	dstSnapshot := snapshot
	dstSnapshot.CompleteNodes = make(map[string]bool, len(snapshot.CompleteNodes))
	dstSnapshot.FailedNodes = make(map[string]bool, len(snapshot.FailedNodes))
	maps.Copy(dstSnapshot.CompleteNodes, snapshot.CompleteNodes)
	maps.Copy(dstSnapshot.FailedNodes, snapshot.FailedNodes)
	return dstSnapshot
}

func (rC *RecordingCheckpointer) Load(runtimeId string) (RuntimeSnapshot, error) {
	rC.mu.Lock()
	defer rC.mu.Unlock()
	res := RuntimeSnapshot{}
	canFindSnapshot := false
	for _, snapshot := range rC.recordSnapshots {
		if snapshot.RuntimeID == runtimeId {
			res = cloneSnapshot(snapshot)
			canFindSnapshot = true
		}
	}
	if !canFindSnapshot {
		err := fmt.Errorf("no match snapshot runtime id:%s", runtimeId)
		return res, err
	}
	return res, nil
}

func (rC *RecordingCheckpointer) SaveCount() int {
	rC.mu.Lock()
	defer rC.mu.Unlock()
	return len(rC.recordSnapshots)
}

func (rC *RecordingCheckpointer) Snapshots() []RuntimeSnapshot {
	rC.mu.Lock()
	defer rC.mu.Unlock()
	res := make([]RuntimeSnapshot, len(rC.recordSnapshots))
	for i, snapshot := range rC.recordSnapshots {
		res[i] = cloneSnapshot(snapshot)
	}
	return res
}

func TestRuntimeABC(t *testing.T) {

	graph := GraphDefinition{
		Nodes: []Node{{ID: "A", Name: "node-A"}, {ID: "B", Name: "node-B"}, {ID: "C", Name: "node-C"}},
		Edges: []Edge{{From: "A", To: "B", NodeId: "B"}, {From: "B", To: "C", NodeId: "C"}},
	}
	snapshot := RuntimeSnapshot{
		RuntimeID:     "test-runtime-abc",
		Status:        RuntimeRunning,
		CompleteNodes: make(map[string]bool),
		FailedNodes:   make(map[string]bool),
	}
	fakeSchedule := &FakeScheduler{}
	recordExecutor := &RecordingExecutor{}
	fakeTaskPolicy := &FakeTaskPolicy{}
	fakeCommiter := &FakeCommitter{}
	recordCheckpointer := &RecordingCheckpointer{}
	fakeDecision := &FakeDecision{}
	nRun := NewRuntime(graph, 2, fakeSchedule, recordExecutor, fakeTaskPolicy, fakeCommiter, recordCheckpointer, fakeDecision)

	finalSnapshot, err := nRun.runLoop(context.Background(), snapshot)
	if err != nil {
		t.Fatalf("runLoop returned error: %v", err)
	}

	// 1. 最终状态
	if got, want := finalSnapshot.Status, RuntimeSuccess; got != want {
		t.Fatalf("final status = %q, want %q", got, want)
	}

	// 2. 完成节点
	for _, nodeID := range []string{"A", "B", "C"} {
		if !finalSnapshot.CompleteNodes[nodeID] {
			t.Errorf("node %q was not completed", nodeID)
		}
	}

	if got, want := len(finalSnapshot.CompleteNodes), 3; got != want {
		t.Errorf("completed node count = %d, want %d", got, want)
	}

	// 3. 执行记录
	gotExecuted := recordExecutor.ExecutedNodes()
	wantExecuted := []string{"A", "B", "C"}

	if !reflect.DeepEqual(gotExecuted, wantExecuted) {
		t.Errorf("executed nodes = %v, want %v", gotExecuted, wantExecuted)
	}

	// 4. Checkpoint 次数
	if got, want := recordCheckpointer.SaveCount(), 3; got != want {
		t.Errorf("checkpoint count = %d, want %d", got, want)
	}

	snapshots := recordCheckpointer.Snapshots()
	if got, want := len(snapshots), 3; got != want {
		t.Fatalf("snapshot count = %d, want %d", got, want)
	}

	wantCompleted := []map[string]bool{
		{"A": true},
		{"A": true, "B": true},
		{"A": true, "B": true, "C": true},
	}
	for i := range snapshots {
		if got, want := snapshots[i].RuntimeID, "test-runtime-abc"; got != want {
			t.Errorf(
				"snapshot[%d] runtime ID = %q, want %q",
				i,
				got,
				want,
			)
		}

		gotCompleted := snapshots[i].CompleteNodes
		wantNodes := wantCompleted[i]

		if !reflect.DeepEqual(gotCompleted, wantNodes) {
			t.Errorf(
				"snapshot[%d] completed nodes = %v, want %v",
				i,
				gotCompleted,
				wantNodes,
			)
		}
	}
}
func TestExecuteReadyTasksKeepsAllBatches(t *testing.T) {
	recordExecutor := &RecordingExecutor{}
	fakeTaskPolicy := &FakeTaskPolicy{}

	runtimeEngine := &Runtime{
		executor:   recordExecutor,
		taskPolicy: fakeTaskPolicy,
		workerNum:  2,
	}

	runtimeEngine.initWorker(runtimeEngine.workerNum)
	runtimeEngine.startWorker(context.Background())
	defer runtimeEngine.stopWorker()

	readyTaskSet := ReadyTaskSet{
		Tasks: []Task{
			{NodeID: "A"},
			{NodeID: "B"},
			{NodeID: "C"},
			{NodeID: "D"},
			{NodeID: "E"},
		},
	}

	outcomes := runtimeEngine.executeReadyTasks(readyTaskSet)
	if got, want := len(outcomes), 5; got != want {
		t.Fatalf("outcome count = %d, want %d", got, want)
	}

	wantedMap := map[string]int{"A": 1, "B": 1, "C": 1, "D": 1, "E": 1}
	// 每个 Outcome 都成功
	for _, outcome := range outcomes {
		if !outcome.Success {
			t.Errorf("outcome %q was not completed", outcome.NodeID)
		}
		remaining, ok := wantedMap[outcome.NodeID]
		if !ok {
			t.Errorf("unexpected outcome node %q", outcome.NodeID)
			continue
		}

		remaining--
		wantedMap[outcome.NodeID] = remaining

		if remaining < 0 {
			t.Errorf("outcome node %q appeared more than once", outcome.NodeID)
		}
	}

	for nodeID, remaining := range wantedMap {
		if remaining != 0 {
			t.Errorf(
				"outcome node %q remaining count = %d, want 0",
				nodeID,
				remaining,
			)
		}
	}
}

func TestValidateGraph(t *testing.T) {

	tests := []struct {
		name    string
		graph   GraphDefinition
		wantErr bool
	}{
		{
			name: "valid graph",
			graph: GraphDefinition{
				Nodes: []Node{
					{ID: "A", Name: "A"},
					{ID: "B", Name: "B"},
					{ID: "C", Name: "C"},
				},
				Edges: []Edge{
					{
						From: "A",
						To:   "B",
					},
					{
						From: "B",
						To:   "C",
					},
				},
			},
			wantErr: false,
		},
		{
			name:    "empty graph",
			graph:   GraphDefinition{Nodes: make([]Node, 0)},
			wantErr: true,
		},
		{
			name: "duplicate node ID",
			graph: GraphDefinition{
				Nodes: []Node{
					{ID: "A", Name: "A"},
					{ID: "A", Name: "A"},
					{ID: "B", Name: "B"},
					{ID: "C", Name: "C"},
				},
				Edges: []Edge{
					{
						From:   "A",
						To:     "B",
						NodeId: "B",
					},
					{
						From:   "B",
						To:     "C",
						NodeId: "C",
					},
				},
			},
			wantErr: true,
		},
		{
			name: "empty edge from",
			graph: GraphDefinition{
				Nodes: []Node{
					{ID: "A", Name: "A"},
					{ID: "B", Name: "B"},
					{ID: "C", Name: "C"},
				},
				Edges: []Edge{
					{
						From:   "",
						To:     "B",
						NodeId: "B",
					},
				},
			},
			wantErr: true,
		},
		{
			name: "missing edge From",
			graph: GraphDefinition{
				Nodes: []Node{
					{ID: "A", Name: "A"},
					{ID: "B", Name: "B"},
					{ID: "C", Name: "C"},
				},
				Edges: []Edge{
					{
						From:   "D",
						To:     "B",
						NodeId: "B",
					},
				},
			},
			wantErr: true,
		},
		{
			name: "empty edge To",
			graph: GraphDefinition{
				Nodes: []Node{
					{ID: "A", Name: "A"},
					{ID: "B", Name: "B"},
					{ID: "C", Name: "C"},
				},
				Edges: []Edge{
					{
						From:   "A",
						To:     "",
						NodeId: "B",
					},
				},
			},
			wantErr: true,
		},
		{
			name: "missing edge To",
			graph: GraphDefinition{
				Nodes: []Node{
					{ID: "A", Name: "A"},
					{ID: "B", Name: "B"},
					{ID: "C", Name: "C"},
				},
				Edges: []Edge{
					{
						From:   "A",
						To:     "D",
						NodeId: "B",
					},
				},
			},
			wantErr: true,
		},
		//self loop：A → A
		{
			name: "self loop",
			graph: GraphDefinition{
				Nodes: []Node{
					{ID: "A", Name: "A"},
				},
				Edges: []Edge{
					{
						From:   "A",
						To:     "A",
						NodeId: "A",
					},
				},
			},
			wantErr: true,
		},
		//multi-node cycle：A → B → C → A
		{
			name: "cycle graph",
			graph: GraphDefinition{
				Nodes: []Node{
					{ID: "A", Name: "A"},
					{ID: "B", Name: "B"},
					{ID: "C", Name: "C"},
				},
				Edges: []Edge{
					{
						From:   "A",
						To:     "B",
						NodeId: "B",
					},
					{
						From:   "B",
						To:     "C",
						NodeId: "C",
					},
					{
						From:   "C",
						To:     "A",
						NodeId: "A",
					},
				},
			},
			wantErr: true,
		},
		{
			name: "converging graph with local cycle",
			graph: GraphDefinition{
				Nodes: []Node{
					{ID: "A", Name: "A"},
					{ID: "B", Name: "B"},
					{ID: "C", Name: "C"},
					{ID: "D", Name: "D"},
					{ID: "E", Name: "E"},
				},
				Edges: []Edge{
					{From: "A", To: "C"},
					{From: "B", To: "C"},
					{From: "C", To: "D"},
					{From: "D", To: "E"},
					{From: "E", To: "C"}, // C → D → E → C
				},
			},
			wantErr: true,
		},
		{
			name: "acyclic converging graph",
			graph: GraphDefinition{
				Nodes: []Node{
					{ID: "A", Name: "A"},
					{ID: "B", Name: "B"},
					{ID: "C", Name: "C"},
					{ID: "D", Name: "D"},
				},
				Edges: []Edge{
					{From: "A", To: "C"},
					{From: "B", To: "C"},
					{From: "C", To: "D"},
				}},
			wantErr: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := ValidateGraph(test.graph)
			//if gotErr := err != nil; gotErr != test.wantErr {
			//	t.Errorf(
			//		"checkGraphValidate() error = %v, wantErr %v",
			//		err,
			//		test.wantErr,
			//	)
			//}
			if test.wantErr && err == nil {
				t.Fatal("want err but get nil")
			}
			if !test.wantErr && err != nil {
				t.Fatalf("want nil, got error: %v", err)
			}
			wantValid := !test.wantErr
			if got != wantValid {
				t.Errorf("validation result = %v, want %v", got, wantValid)
			}
		})
	}

}

func TestRegistryRegister(t *testing.T) {
	registry := NewMemoryToolRegistry()
	tool := &FakeTool{
		definition: ToolDefinition{
			Name: "echo",
		},
	}

	err := registry.Register(tool)
	if err != nil {
		t.Fatalf("register tool failed: %v", err)
	}
}

type FakeTool struct {
	definition ToolDefinition
}

func (fT *FakeTool) Definition() ToolDefinition {
	return fT.definition
}
func (fT *FakeTool) Execute(ctx context.Context, invocation Invocation) ExecutionResult {
	return ExecutionResult{}
}

type UnexpectedCallScheduler struct {
	callCount int
}

func (s *UnexpectedCallScheduler) Schedule(
	graph GraphDefinition,
	snapshot RuntimeSnapshot,
) (ReadyTaskSet, error) {
	s.callCount++
	return ReadyTaskSet{}, fmt.Errorf("scheduler should not be called")
}

func TestRegistryGet(t *testing.T) {
	tool := &FakeTool{
		definition: ToolDefinition{
			Name:        "echo",
			Description: "echo tool",
		},
	}
	registry := NewMemoryToolRegistry()

	err := registry.Register(tool)
	if err != nil {
		t.Fatalf("prepare registry failed: %v", err)
	}

	got, err := registry.Get("echo")
	if err != nil {
		t.Fatalf("get tool failed: %v", err)
	}
	if got == nil {
		t.Fatal("get tool returned nil")
	}
	if got != tool {
		t.Fatalf("got a different tool: %#v", got)
	}
}

func TestRuntimeRejectsInvalidGraphBeforeStartingWorkers(t *testing.T) {
	graph := GraphDefinition{
		Nodes: []Node{{ID: "A", Name: "A"}},
		Edges: []Edge{{From: "A", To: "A"}},
	}
	scheduler := &UnexpectedCallScheduler{}

	runtimeInstance := NewRuntime(
		graph,
		1,
		scheduler,
		&FakeExecutor{},
		&FakeTaskPolicy{},
		&FakeCommitter{},
		&FakeCheckpointer{},
		&FakeDecision{},
	)

	snapshot := RuntimeSnapshot{
		RuntimeID:     "invalid-graph",
		CompleteNodes: make(map[string]bool),
		FailedNodes:   make(map[string]bool),
		Status:        RuntimeRunning,
	}

	_, err := runtimeInstance.runLoop(context.Background(), snapshot)

	if err == nil {
		t.Fatal("want graph validation error, got nil")
	}

	if err.Error() != "graph cycle" {
		t.Fatalf("error = %q, want %q", err.Error(), "graph cycle")
	}

	if scheduler.callCount != 0 {
		t.Fatalf(
			"scheduler call count = %d, want 0",
			scheduler.callCount,
		)
	}

	if runtimeInstance.TaskQueue != nil ||
		runtimeInstance.ResultQueue != nil {
		t.Fatal("worker queues should not be initialized")
	}

}
func TestRegistryRejectsDuplicateTool(t *testing.T) {
	registry := NewMemoryToolRegistry()

	originalTool := &FakeTool{
		definition: ToolDefinition{
			Name:        "echo",
			Description: "original",
		},
	}
	duplicateTool := &FakeTool{
		definition: ToolDefinition{
			Name:        "echo",
			Description: "duplicate",
		},
	}

	err := registry.Register(originalTool)
	if err != nil {
		t.Fatalf("register original tool failed: %v", err)
	}

	err = registry.Register(duplicateTool)
	if !errors.Is(err, ErrToolAlreadyRegistered) {
		t.Fatalf(
			"register duplicate tool error = %v, want %v",
			err,
			ErrToolAlreadyRegistered,
		)
	}

	got, err := registry.Get("echo")
	if err != nil {
		t.Fatalf("get original tool failed: %v", err)
	}

	if got != originalTool {
		t.Fatalf(
			"tool was overwritten: got %#v, want %#v",
			got,
			originalTool,
		)
	}
}

func TestRegistryReturnsErrorForMissingTool(t *testing.T) {
	registry := NewMemoryToolRegistry()
	got, err := registry.Get("missing")
	if !errors.Is(err, ErrToolNotFound) {
		t.Fatalf(
			"get missing tool error = %v, want %v",
			err,
			ErrToolNotFound,
		)
	}
	if got != nil {
		t.Fatalf("get missing tool = %#v, want nil", got)
	}
}

type SuccessTool struct {
	definition ToolDefinition
	output     any
}

func (sT *SuccessTool) Definition() ToolDefinition {
	return sT.definition
}

func (sT *SuccessTool) Execute(ctx context.Context, invocation Invocation) ExecutionResult {
	return ExecutionResult{
		InvocationID: invocation.ID,
		Output:       sT.output,
		Err:          nil,
	}
}

func TestSuccessToolExecute(t *testing.T) {
	tool := &SuccessTool{
		definition: ToolDefinition{
			Name: "success",
		},
		output: "hello",
	}
	invocation := Invocation{
		ID:        "invocation-1",
		ToolName:  "success",
		Arguments: map[string]interface{}{},
	}
	result := tool.Execute(context.Background(), invocation)
	if result.InvocationID != invocation.ID {
		t.Fatalf(
			"invocation ID = %q, want %q",
			result.InvocationID,
			invocation.ID,
		)
	}
	if result.Output != tool.output {
		t.Fatalf(
			"tool.output = %q, want %q",
			result.Output, tool.output)
	}
	if result.Err != nil {
		t.Fatalf("error = %v, want nil", result.Err)
	}
}

type InvalidArgumentsTool struct {
	definition ToolDefinition
}

func (t *InvalidArgumentsTool) Definition() ToolDefinition {
	return t.definition
}

func (t *InvalidArgumentsTool) Execute(
	ctx context.Context,
	invocation Invocation,
) ExecutionResult {
	return ExecutionResult{
		InvocationID: invocation.ID,
		Err: &ExecutionError{
			ErrType: ExecutionTypeToolErr,
			ErrCode: "MISSING_REQUIRED_ARGUMENT",
			ErrMsg:  "missing required argument",
		},
	}
}

func TestInvalidArgumentsToolExecute(t *testing.T) {
	tool := &InvalidArgumentsTool{
		definition: ToolDefinition{
			Name: "invalid argument tool",
		},
	}
	invocation := Invocation{
		ID:       "invocation-2",
		ToolName: "invalid argument tool",
	}
	result := tool.Execute(context.Background(), invocation)
	//InvocationID 与请求一致

	if result.InvocationID != invocation.ID {
		t.Fatalf(
			"invocation ID = %q, want %q",
			result.InvocationID,
			invocation.ID,
		)
	}
	//Output == nil
	if result.Output != nil {
		t.Fatalf("output = %#v, want nil", result.Output)
	}

	//Err != nil
	if result.Err == nil {
		t.Fatal("error is nil, want tool error")
	}
	//ErrType == ExecutionTypeToolErr
	if result.Err.ErrType != ExecutionTypeToolErr {
		t.Fatalf("error = %q, want ExecutionTypeToolErr", result.Err.ErrType)
	}
	if result.Err.ErrCode != "MISSING_REQUIRED_ARGUMENT" {
		t.Fatalf("err code = %q, want MISSING_REQUIRED_ARGUMENT", result.Err.ErrCode)
	}
}

type TimeoutTool struct {
	definition ToolDefinition
}

func (t *TimeoutTool) Definition() ToolDefinition {
	return t.definition
}

func (t *TimeoutTool) Execute(
	ctx context.Context,
	invocation Invocation,
) ExecutionResult {
	<-ctx.Done()

	executionErr := &ExecutionError{
		ErrType: ExecutionTypeInfrastructure,
		ErrMsg:  ctx.Err().Error(),
	}

	switch ctx.Err() {
	case context.DeadlineExceeded:
		executionErr.ErrCode = "TOOL_TIMEOUT"
	case context.Canceled:
		executionErr.ErrCode = "TOOL_CANCELED"
	}

	return ExecutionResult{
		InvocationID: invocation.ID,
		Err:          executionErr,
	}
}
func TestTimeoutToolExecute(t *testing.T) {
	tool := &TimeoutTool{
		definition: ToolDefinition{
			Name: "timeout",
		},
	}
	invocation := Invocation{
		ID:       "invocation-3",
		ToolName: "timeout",
	}
	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Millisecond,
	)
	defer cancel()
	result := tool.Execute(ctx, invocation)
	if result.Output != nil {
		t.Fatalf("output = %#v, want nil", result.Output)
	}
	if result.InvocationID != invocation.ID {
		t.Fatalf("invocation ID = %q, want %q", result.InvocationID, invocation.ID)
	}
	if result.Err == nil {
		t.Fatal("error is nil, want infrastructure error")
	}
	if result.Err.ErrType != ExecutionTypeInfrastructure {
		t.Fatalf("error = %q, want ExecutionTypeInfrastructure", result.Err.ErrType)
	}
	if result.Err.ErrCode != "TOOL_TIMEOUT" {
		t.Fatalf("error = %q, want TOOL_TIMEOUT", result.Err.ErrCode)
	}
}

func TestRegistryToolExecutorExecuteSuccess(t *testing.T) {
	memoryToolRegistry := NewMemoryToolRegistry()

	tool := &SuccessTool{
		definition: ToolDefinition{
			Name: "success",
		},
		output: "success",
	}
	err := memoryToolRegistry.Register(tool)
	if err != nil {
		t.Fatalf("error registering tool: %v", err)
	}
	registryToolExecutor := NewRegistryToolExecutor(memoryToolRegistry)
	result := registryToolExecutor.Execute(context.Background(), Invocation{
		ID:       "invocation-1",
		ToolName: "success",
	})
	if result.InvocationID != "invocation-1" {
		t.Fatalf("invocation ID = %q, want invocation-1", result.InvocationID)
	}
	if result.Err != nil {
		t.Fatalf("execute returned unexpected error: %v", result.Err)
	}
	if result.Output != tool.output {
		t.Fatalf("output = %#v, want %#v", result.Output, tool.output)
	}
}

func TestRegistryToolExecutorPropagatesCancellation(t *testing.T) {
	//流程：
	//Given：Registry 注册 TimeoutTool
	memoryToolRegistry := NewMemoryToolRegistry()
	timeoutTool := &TimeoutTool{
		definition: ToolDefinition{
			Name: "timeout",
		},
	}
	err := memoryToolRegistry.Register(timeoutTool)
	if err != nil {
		t.Fatalf("error registering tool: %v", err)
	}

	//And：创建可取消的 ctx，并调用 cancel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	//When：RegistryToolExecutor.Execute(ctx, invocation)
	registryToolExecutor := NewRegistryToolExecutor(memoryToolRegistry)
	result := registryToolExecutor.Execute(ctx, Invocation{
		ID:       "invocation-2",
		ToolName: "timeout",
	})
	if result.InvocationID != "invocation-2" {
		t.Fatalf("invocation ID = %q, want invocation-2", result.InvocationID)
	}
	if result.Err == nil {
		t.Fatalf("execute not nil returned nil")
	}
	if result.Output != nil {
		t.Fatalf("output = %#v, want nil", result.Output)
	}
	if result.Err.ErrType != ExecutionTypeInfrastructure {
		t.Fatalf("error = %q, want Infrastructure error", result.Err.ErrType)
	}

	if result.Err.ErrCode != "TOOL_CANCELED" {
		t.Fatalf("error code = %q, want TOOL_CANCELED", result.Err.ErrCode)
	}
}

func TestToolTaskExecutorExecuteSuccess(t *testing.T) {
	//创建 MemoryToolRegistry。
	memoryToolRegistry := NewMemoryToolRegistry()
	successTool := &SuccessTool{
		definition: ToolDefinition{
			Name: "success",
		},
		output: "success",
	}
	//注册名为 success 的 SuccessTool。
	err := memoryToolRegistry.Register(successTool)
	if err != nil {
		t.Fatalf("error registering tool: %v", err)
	}
	//创建 RegistryToolExecutor。
	registryToolExecutor := NewRegistryToolExecutor(memoryToolRegistry)
	//创建 ToolTaskExecutor。
	toolTaskExecutor := NewToolTaskExecutor(registryToolExecutor)
	//创建 Task：
	task := Task{
		NodeID:   "node-A",
		ToolName: "success",
		Arguments: map[string]any{
			"message": "hello",
		}}
	result := toolTaskExecutor.Execute(context.Background(), task)
	//result.NodeID == "node-A"
	//result.ExecutionResult.InvocationID == "node-A-1"
	//result.ExecutionResult.Output == SuccessTool预期输出
	//result.ExecutionResult.Err == nil
	if result.Task.NodeID != "node-A" {
		t.Fatalf("task ID = %q, want node-A", result.Task.NodeID)
	}
	if result.ExecutionResult.InvocationID != "node-A-1" {
		t.Fatalf("invocation ID = %q, want node-A-1", result.ExecutionResult.InvocationID)
	}
	if result.ExecutionResult.Output != successTool.output {
		t.Fatalf("output = %#v, want %#v", result.ExecutionResult.Output, successTool.output)
	}
	if result.ExecutionResult.Err != nil {
		t.Fatalf("execute returned unexpected error: %v", result.ExecutionResult.Err)
	}
}

func TestRuntimeExecutesToolTaskSuccessfully(t *testing.T) {
	registry := NewMemoryToolRegistry()
	tool := &SuccessTool{
		definition: ToolDefinition{Name: "success"},
		output:     "success",
	}
	if err := registry.Register(tool); err != nil {
		t.Fatalf("register tool: %v", err)
	}
	registryExecutor := NewRegistryToolExecutor(registry)
	toolTaskExecutor := NewToolTaskExecutor(registryExecutor)
	graph := GraphDefinition{
		Nodes: []Node{
			{
				ID:       "A",
				Name:     "node-A",
				ToolName: "success",
				Arguments: map[string]any{
					"message": "hello",
				},
			},
		},
	}
	checkpointer := &RecordingCheckpointer{}

	runtimeEngine := NewRuntime(
		graph,
		1,
		&FakeScheduler{},
		toolTaskExecutor,
		&FakeTaskPolicy{},
		&FakeCommitter{},
		checkpointer,
		&FakeDecision{},
	)
	finalSnapshot, err := runtimeEngine.runLoop(
		context.Background(),
		RuntimeSnapshot{
			RuntimeID:     "runtime-tool-success",
			CompleteNodes: make(map[string]bool),
			FailedNodes:   make(map[string]bool),
			Status:        RuntimeRunning,
		},
	)
	if err != nil {
		t.Fatalf("runLoop: %v", err)
	}
	//finalSnapshot.Status == RuntimeSuccess
	if finalSnapshot.Status != RuntimeSuccess {
		t.Fatalf("finalSnapshot = %#v, want RuntimeSuccess", finalSnapshot.Status)
	}
	//finalSnapshot.CompleteNodes["A"] == true
	if finalSnapshot.CompleteNodes["A"] != true {
		t.Fatalf("finalSnapshot node A = %#v, want true", finalSnapshot.CompleteNodes["A"])
	}
	//checkpointer.SaveCount() == 1
	if checkpointer.SaveCount() != 1 {
		t.Fatalf("checkpointer.SaveCount = %d, want 1", checkpointer.SaveCount())
	}
}

func TestRuntimeRecordsInvalidArgumentsAsFailure(t *testing.T) {
	//Given：
	invalidArgumentTool := InvalidArgumentsTool{
		definition: ToolDefinition{
			Name: "invalid-argument",
		},
	}
	registry := NewMemoryToolRegistry()
	err := registry.Register(&invalidArgumentTool)
	if err != nil {
		t.Fatalf("error registering tool: %v", err)
	}

	registryExecutor := NewRegistryToolExecutor(registry)
	toolTaskExecutor := NewToolTaskExecutor(registryExecutor)
	//- Registry 注册 InvalidArgumentsTool，名称保持一致。
	graph := GraphDefinition{
		Nodes: []Node{
			{
				ID:       "A",
				Name:     "node-A",
				ToolName: "invalid-argument",
				Arguments: map[string]any{
					"message": "hello",
				},
			},
		},
	}
	checkpointer := &RecordingCheckpointer{}

	runtimeEngine := NewRuntime(
		graph,
		1,
		&FakeScheduler{},
		toolTaskExecutor,
		&FakeTaskPolicy{},
		&FakeCommitter{},
		checkpointer,
		&FakeDecision{},
	)
	finalSnapshot, err := runtimeEngine.runLoop(
		context.Background(),
		RuntimeSnapshot{
			RuntimeID:     "runtime-invalid-argument",
			CompleteNodes: make(map[string]bool),
			FailedNodes:   make(map[string]bool),
			Status:        RuntimeRunning,
		},
	)
	if err == nil {
		t.Fatalf("runLoop succeeded unexpectedly")
	}
	//finalSnapshot.Status == RuntimeSuccess
	if finalSnapshot.Status != RuntimeFailed {
		t.Fatalf("finalSnapshot = %#v, want RuntimeFailed", finalSnapshot.Status)
	}
	//finalSnapshot.CompleteNodes["A"] == true
	if finalSnapshot.CompleteNodes["A"] == true {
		t.Fatalf("finalSnapshot = %#v, want false", finalSnapshot.CompleteNodes["A"])
	}
	if finalSnapshot.FailedNodes["A"] != true {
		t.Fatalf("finalSnapshot = %#v, want true", finalSnapshot.FailedNodes["A"])
	}

	//checkpointer.SaveCount() == 1
	if checkpointer.SaveCount() != 1 {
		t.Fatalf("checkpointer.SaveCount = %d, want 1", checkpointer.SaveCount())
	}
	snapshots := checkpointer.Snapshots()
	if !snapshots[0].FailedNodes["A"] {
		t.Fatal("checkpoint did not persist node A failure")
	}
}

func TestRuntimeRecordsToolTimeoutAsFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	timeoutTool := TimeoutTool{
		definition: ToolDefinition{
			Name: "timeout",
		},
	}
	registry := NewMemoryToolRegistry()
	err := registry.Register(&timeoutTool)
	if err != nil {
		t.Fatalf("error registering tool: %v", err)
	}
	registryExecutor := NewRegistryToolExecutor(registry)
	toolTaskExecutor := NewToolTaskExecutor(registryExecutor)
	graph := GraphDefinition{
		Nodes: []Node{{
			ID:       "A",
			Name:     "node-A",
			ToolName: "timeout",
			Arguments: map[string]any{
				"message": "hello",
			},
		}},
	}
	checkpointer := &RecordingCheckpointer{}

	runtimeEngine := NewRuntime(
		graph,
		1,
		&FakeScheduler{},
		toolTaskExecutor,
		&FakeTaskPolicy{},
		&FakeCommitter{},
		checkpointer,
		&FakeDecision{},
	)
	finalSnapshot, err := runtimeEngine.runLoop(
		ctx,
		RuntimeSnapshot{
			RuntimeID:     "runtime-timeout",
			CompleteNodes: make(map[string]bool),
			FailedNodes:   make(map[string]bool),
			Status:        RuntimeRunning,
		},
	)
	if err == nil {
		t.Fatalf("runLoop succeeded unexpectedly")
	}

	if ctx.Err() != context.DeadlineExceeded {
		t.Fatalf("ctx.Err() = %#v, want DeadlineExceeded", ctx.Err())
	}
	//err != nil
	//ctx.Err() == context.DeadlineExceeded
	if finalSnapshot.Status != RuntimeFailed {
		t.Fatalf("finalSnapshot = %#v, want RuntimeFailed", finalSnapshot.Status)
	}
	//finalSnapshot.Status == RuntimeFailed
	if finalSnapshot.CompleteNodes["A"] {
		t.Fatalf("finalSnapshot = %#v, want false", finalSnapshot.CompleteNodes["A"])
	}
	if !finalSnapshot.FailedNodes["A"] {
		t.Fatalf(
			"finalSnapshot = %#v, want true",
			finalSnapshot.FailedNodes["A"],
		)
	}
	//finalSnapshot.FailedNodes["A"] == true
	//finalSnapshot.CompleteNodes["A"] == false
	if checkpointer.SaveCount() != 1 {
		t.Fatalf("checkpointer.SaveCount = %d, want 1", checkpointer.SaveCount())
	}
	//checkpointer.SaveCount() == 1
	snapshots := checkpointer.Snapshots()
	if !snapshots[0].FailedNodes["A"] {
		t.Fatal("checkpoint did not persist node A failure")
	}
	//Checkpoint 中也记录了 FailedNodes["A"]
}

func TestCandidateSnapshot(t *testing.T) {
	currentSnapshot := RuntimeSnapshot{
		RuntimeID:     "runtime-timeout",
		CompleteNodes: make(map[string]bool),
		FailedNodes:   make(map[string]bool),
		Status:        RuntimeRunning,
		Version:       1,
		SchemaVersion: CurrentSnapshotSchemaVersion,
	}
	currentSnapshot.Version = 1
	outcomes := []TaskOutcome{
		{NodeID: "A", Success: true},
	}
	fakeCommiter := FakeCommitter{}
	candidate, err := fakeCommiter.Commit(currentSnapshot, outcomes)
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	if currentSnapshot.Version != 1 {
		t.Fatalf("current snapshot version must be 1")
	}
	if currentSnapshot.CompleteNodes["A"] {
		t.Fatalf("current snapshot CompleteNodes must be empty")
	}
	if candidate.Version != 2 {
		t.Fatalf("candidate snapshot version must be 2")
	}
	if !candidate.CompleteNodes["A"] {
		t.Fatalf("candidate snapshot CompleteNodes must contain node A")
	}
	if candidate.SchemaVersion != CurrentSnapshotSchemaVersion {
		t.Fatalf("unexpected schema version: %d", candidate.SchemaVersion)
	}
}

func TestSaveFailedSnapshotPure(t *testing.T) {
	ctx := context.Background()
	registry := NewMemoryToolRegistry()
	tool := &SuccessTool{
		definition: ToolDefinition{Name: "success"},
		output:     "success",
	}
	if err := registry.Register(tool); err != nil {
		t.Fatalf("register tool: %v", err)
	}
	registryExecutor := NewRegistryToolExecutor(registry)
	toolTaskExecutor := NewToolTaskExecutor(registryExecutor)
	graph := GraphDefinition{
		Nodes: []Node{
			{
				ID:       "A",
				Name:     "node-A",
				ToolName: "success",
				Arguments: map[string]any{
					"message": "hello",
				},
			},
		},
	}
	fc := FailedCheckpointer{}
	currentSnapshot := RuntimeSnapshot{
		RuntimeID:     "runtime-success",
		CompleteNodes: make(map[string]bool),
		FailedNodes:   make(map[string]bool),
		Status:        RuntimeRunning,
		Version:       1,
		SchemaVersion: CurrentSnapshotSchemaVersion,
	}
	fakeCommiter := FakeCommitter{}
	runtimeEngine := NewRuntime(
		graph,
		1,
		&FakeScheduler{},
		toolTaskExecutor,
		&FakeTaskPolicy{},
		&fakeCommiter,
		&fc,
		&FakeDecision{},
	)
	finalSnapshot, err := runtimeEngine.runLoop(
		ctx,
		currentSnapshot,
	)
	if err == nil {
		t.Fatalf("runLoop succeeded unexpectedly")
	}
	if finalSnapshot.Version != 1 {
		t.Fatalf("finalSnapshot.Version = %d, want %d", finalSnapshot.Version, 1)
	}
	if finalSnapshot.CompleteNodes["A"] {
		t.Fatalf("finalSnapshot CompleteNodes must be empty")
	}
}

type FailedCheckpointer struct {
}

func (rC *FailedCheckpointer) Save(snapshot RuntimeSnapshot) error {
	return fmt.Errorf("failed to save")
}

func (rC *FailedCheckpointer) Load(runtimeId string) (RuntimeSnapshot, error) {
	return RuntimeSnapshot{}, fmt.Errorf("load is not supported")
}

func TestSaveSnapshotCheckpoint(t *testing.T) {
	ctx := context.Background()
	registry := NewMemoryToolRegistry()
	tool := &SuccessTool{
		definition: ToolDefinition{Name: "success"},
		output:     "success",
	}
	if err := registry.Register(tool); err != nil {
		t.Fatalf("register tool: %v", err)
	}
	registryExecutor := NewRegistryToolExecutor(registry)
	toolTaskExecutor := NewToolTaskExecutor(registryExecutor)
	graph := GraphDefinition{
		Nodes: []Node{
			{
				ID:       "A",
				Name:     "node-A",
				ToolName: "success",
				Arguments: map[string]any{
					"message": "hello",
				},
			},
			{
				ID:       "B",
				Name:     "node-B",
				ToolName: "success",
				Arguments: map[string]any{
					"message": "hello",
				},
			},
		},
		Edges: []Edge{
			{
				From:   "A",
				To:     "B",
				NodeId: "B",
			},
		},
	}
	fc := RecordingCheckpointer{}
	currentSnapshot := RuntimeSnapshot{
		RuntimeID:     "runtime-success",
		CompleteNodes: make(map[string]bool),
		FailedNodes:   make(map[string]bool),
		Status:        RuntimeRunning,
		Version:       1,
		SchemaVersion: CurrentSnapshotSchemaVersion,
	}
	currentSnapshot.CompleteNodes["A"] = true
	fakeCommiter := FakeCommitter{}
	runtimeEngine := NewRuntime(
		graph,
		1,
		&FakeScheduler{},
		toolTaskExecutor,
		&FakeTaskPolicy{},
		&fakeCommiter,
		&fc,
		&FakeDecision{},
	)
	finalSnapshot, err := runtimeEngine.runLoop(
		ctx,
		currentSnapshot,
	)
	if err != nil {
		t.Fatalf("runloop err:%v", err)
	}
	if finalSnapshot.Version != 2 {
		t.Fatalf("finalSnapshot.Version = %d, want %d", finalSnapshot.Version, 2)
	}

	if !currentSnapshot.CompleteNodes["A"] {
		t.Fatalf("init current snapshot node A must be complete")
	}
	if currentSnapshot.CompleteNodes["B"] {
		t.Fatalf("init current snapshot node B must be empty")
	}
	if !finalSnapshot.CompleteNodes["A"] {
		t.Fatalf("snapshot node A must be complete")
	}
	if !finalSnapshot.CompleteNodes["B"] {
		t.Fatalf("snapshot node B must be complete")
	}
	finalSnapshot.CompleteNodes["C"] = true
	reloadSnapshot, err := fc.Load(currentSnapshot.RuntimeID)
	if err != nil {
		t.Fatalf("load snapshot failed: %v", err)
	}
	if !reloadSnapshot.CompleteNodes["A"] {
		t.Fatalf("snapshot node A must be complete")
	}
	if !reloadSnapshot.CompleteNodes["B"] {
		t.Fatalf("snapshot node B must be complete")
	}
	if reloadSnapshot.CompleteNodes["C"] {
		t.Fatalf("snapshot  node C must be empty")
	}
}

func TestRecoverySkipsCompletedNode(t *testing.T) {
	runtimeID := "test-runtime-abc"
	ctx := context.Background()
	recordExecutor := &RecordingExecutor{}
	graph := GraphDefinition{
		Nodes: []Node{
			{
				ID:       "A",
				Name:     "node-A",
				ToolName: "success",
				Arguments: map[string]any{
					"message": "hello",
				},
			},
			{
				ID:       "B",
				Name:     "node-B",
				ToolName: "success",
				Arguments: map[string]any{
					"message": "hello",
				},
			},
			{
				ID:       "C",
				Name:     "node-C",
				ToolName: "success",
				Arguments: map[string]any{
					"message": "hello",
				},
			},
		},
		Edges: []Edge{
			{
				From:   "A",
				To:     "B",
				NodeId: "B",
			},
			{
				From:   "B",
				To:     "C",
				NodeId: "C",
			},
		},
	}
	recordCheckpointer := RecordingCheckpointer{
		recordSnapshots: []RuntimeSnapshot{
			{
				RuntimeID: runtimeID,
				CompleteNodes: map[string]bool{
					"A": true,
				},
				FailedNodes:   make(map[string]bool),
				Status:        RuntimeRunning,
				Version:       2,
				SchemaVersion: CurrentSnapshotSchemaVersion,
			},
		},
	}
	fakeCommiter := FakeCommitter{}
	runtimeEngine := NewRuntime(
		graph,
		1,
		&FakeScheduler{},
		recordExecutor,
		&FakeTaskPolicy{},
		&fakeCommiter,
		&recordCheckpointer,
		&FakeDecision{},
	)
	currentSnapshot, err := runtimeEngine.Recovery(ctx, runtimeID)
	if err != nil {
		t.Fatalf("recovery run failed %v", err)
	}
	//恢复后的执行记录严格为 ["B", "C"]；
	//最终状态为 RuntimeSuccess；
	//Snapshot 版本依次为 2, 3, 4；
	//CompleteNodes 依次为 A、A+B、A+B+C。
	gotExecuted := recordExecutor.ExecutedNodes()
	wantExecuted := []string{"B", "C"}
	if !reflect.DeepEqual(gotExecuted, wantExecuted) {
		t.Fatalf("executed nodes = %v, want %v", gotExecuted, wantExecuted)
	}
	if currentSnapshot.Status != RuntimeSuccess {
		t.Fatalf("final snapshot runtime status must be success,but got %v", currentSnapshot.Status)
	}
	if currentSnapshot.Version != 4 {
		t.Fatalf("final version must be 4, but got %d", currentSnapshot.Version)
	}
	if !currentSnapshot.CompleteNodes["A"] || !currentSnapshot.CompleteNodes["B"] || !currentSnapshot.CompleteNodes["C"] {
		t.Fatalf("final complete node must be  a,b,c but got %v", currentSnapshot.CompleteNodes)
	}
	expectVersion := []uint64{2, 3, 4}
	expectRecord := []map[string]bool{
		{"A": true},
		{"A": true, "B": true},
		{"A": true, "B": true, "C": true},
	}
	snapshots := recordCheckpointer.Snapshots()
	if len(snapshots) != 3 {
		t.Fatalf("record checkpoint len must be 3,but got %d", len(snapshots))
	}
	for i, snapshot := range snapshots {
		if expectVersion[i] != snapshot.Version {
			t.Fatalf("current version must be %d but %d", expectVersion[i], snapshot.Version)
		}
		if !reflect.DeepEqual(snapshot.CompleteNodes, expectRecord[i]) {
			t.Fatalf("current complete nodes must be %v but %v", expectRecord[i], snapshot.CompleteNodes)
		}
	}

}

func TestRecoveryReturnsLoadErrorWithoutRunning(t *testing.T) {
	runtimeID := "load-error"
	ctx := context.Background()
	recordExecutor := &RecordingExecutor{}
	graph := GraphDefinition{
		Nodes: []Node{
			{
				ID:       "A",
				Name:     "node-A",
				ToolName: "success",
				Arguments: map[string]any{
					"message": "hello",
				},
			},
			{
				ID:       "B",
				Name:     "node-B",
				ToolName: "success",
				Arguments: map[string]any{
					"message": "hello",
				},
			},
			{
				ID:       "C",
				Name:     "node-C",
				ToolName: "success",
				Arguments: map[string]any{
					"message": "hello",
				},
			},
		},
		Edges: []Edge{
			{
				From:   "A",
				To:     "B",
				NodeId: "B",
			},
			{
				From:   "B",
				To:     "C",
				NodeId: "C",
			},
		},
	}
	errLoadFailed := errors.New("load failed")

	checkpointer := &LoadErrorCheckpointer{
		loadSnapshot: RuntimeSnapshot{
			RuntimeID: "load-failed",
			Version:   1,
		},
		loadErr: errLoadFailed,
	}
	fakeCommiter := FakeCommitter{}
	unexpectedScheduler := UnexpectedCallScheduler{}
	runtimeEngine := NewRuntime(
		graph,
		1,
		&unexpectedScheduler,
		recordExecutor,
		&FakeTaskPolicy{},
		&fakeCommiter,
		checkpointer,
		&FakeDecision{},
	)
	currentSnapshot, err := runtimeEngine.Recovery(ctx, runtimeID)
	if err == nil {
		t.Fatalf("recovery run must be not nil")
	}
	if !errors.Is(err, errLoadFailed) {
		t.Fatalf("err must be errLoadFailed but get :%v", err)
	}
	if !reflect.DeepEqual(currentSnapshot, checkpointer.loadSnapshot) {
		t.Fatalf("snapshot must equal check point  got :%v expect :%v", currentSnapshot, checkpointer.loadSnapshot)
	}
	if unexpectedScheduler.callCount != 0 {
		t.Fatalf("call count must be 0 but got :%d", unexpectedScheduler.callCount)
	}
	if checkpointer.saveCallCount != 0 {
		t.Fatalf("save count must be 0 but got :%d", checkpointer.saveCallCount)
	}
}

type LoadErrorCheckpointer struct {
	saveCallCount uint64
	loadErr       error
	loadSnapshot  RuntimeSnapshot
}

func (l *LoadErrorCheckpointer) Save(snapshot RuntimeSnapshot) error {
	l.saveCallCount++
	return nil
}

func (l *LoadErrorCheckpointer) Load(runtimeID string) (RuntimeSnapshot, error) {
	return l.loadSnapshot, l.loadErr
}

func TestRecoveryReturnsRuntimeFailedWithoutRunning(t *testing.T) {
	runtimeID := "Runtime-failed"
	ctx := context.Background()
	recordExecutor := &RecordingExecutor{}
	graph := GraphDefinition{
		Nodes: []Node{
			{
				ID:       "A",
				Name:     "node-A",
				ToolName: "success",
				Arguments: map[string]any{
					"message": "hello",
				},
			},
			{
				ID:       "B",
				Name:     "node-B",
				ToolName: "success",
				Arguments: map[string]any{
					"message": "hello",
				},
			},
			{
				ID:       "C",
				Name:     "node-C",
				ToolName: "success",
				Arguments: map[string]any{
					"message": "hello",
				},
			},
		},
		Edges: []Edge{
			{
				From:   "A",
				To:     "B",
				NodeId: "B",
			},
			{
				From:   "B",
				To:     "C",
				NodeId: "C",
			},
		},
	}
	checkpointer := &LoadErrorCheckpointer{
		loadSnapshot: RuntimeSnapshot{
			RuntimeID:     runtimeID,
			Status:        RuntimeFailed,
			Version:       1,
			SchemaVersion: CurrentSnapshotSchemaVersion,
		},
		loadErr: nil,
	}
	fakeCommiter := FakeCommitter{}
	unexpectedScheduler := UnexpectedCallScheduler{}
	runtimeEngine := NewRuntime(
		graph,
		1,
		&unexpectedScheduler,
		recordExecutor,
		&FakeTaskPolicy{},
		&fakeCommiter,
		checkpointer,
		&FakeDecision{},
	)
	currentSnapshot, err := runtimeEngine.Recovery(ctx, runtimeID)
	if err == nil {
		t.Fatalf("recovery run err must be not nil")
	}
	if !errors.Is(err, ErrRuntimeFailed) {
		t.Fatalf("err must be errRuntimeFailed but get :%v", err)
	}
	if !reflect.DeepEqual(currentSnapshot, checkpointer.loadSnapshot) {
		t.Fatalf("snapshot must equal check point  got :%v expect :%v", currentSnapshot, checkpointer.loadSnapshot)
	}
	if unexpectedScheduler.callCount != 0 {
		t.Fatalf("call count must be 0 but got :%d", unexpectedScheduler.callCount)
	}
	if checkpointer.saveCallCount != 0 {
		t.Fatalf("save count must be 0 but got :%d", checkpointer.saveCallCount)
	}
}
func TestToolTaskExecutorCarriesTaskContext(t *testing.T) {
	registry := NewMemoryToolRegistry()

	tool := &SuccessTool{
		definition: ToolDefinition{Name: "hotel-search"},
		output:     "hotel-result",
	}
	if err := registry.Register(tool); err != nil {
		t.Fatalf("register tool: %v", err)
	}

	executor := NewToolTaskExecutor(
		NewRegistryToolExecutor(registry),
	)

	task := Task{
		NodeID:   "node-hotel",
		ToolName: "hotel-search",
		Arguments: map[string]any{
			"city":     "上海",
			"minPrice": 500,
			"maxPrice": 800,
		},
		Criteria: &FakeCriteria{},
	}

	got := executor.Execute(context.Background(), task)

	if got.Task.NodeID != task.NodeID {
		t.Fatalf("NodeID = %q, want %q", got.Task.NodeID, task.NodeID)
	}
	if got.Task.ToolName != task.ToolName {
		t.Fatalf("ToolName = %q, want %q", got.Task.ToolName, task.ToolName)
	}
	if !reflect.DeepEqual(got.Task.Arguments, task.Arguments) {
		t.Fatalf(
			"Arguments = %#v, want %#v",
			got.Task.Arguments,
			task.Arguments,
		)
	}
	if got.Task.Criteria != task.Criteria {
		t.Fatalf("Criteria = %v, want %v", got.Task.Criteria, task.Criteria)
	}
}

type RecordingTaskPolicy struct {
	mu    sync.Mutex
	tasks []Task
}

func (p *RecordingTaskPolicy) Evaluate(
	result TaskResult,
) TaskOutcome {
	p.mu.Lock()
	p.tasks = append(p.tasks, result.Task)
	p.mu.Unlock()

	return TaskOutcome{
		NodeID:  result.Task.NodeID,
		Success: true,
	}
}

func (p *RecordingTaskPolicy) Tasks() []Task {
	p.mu.Lock()
	defer p.mu.Unlock()

	tasks := make([]Task, len(p.tasks))
	copy(tasks, p.tasks)
	return tasks
}

type FakeCriteria struct {
	Name string
}

func (f *FakeCriteria) Validate() error {
	return nil
}

func TestExecuteReadyTasksKeepsTaskContext(t *testing.T) {
	recordExecutor := &RecordingExecutor{}
	recordTaskPolicy := &RecordingTaskPolicy{}
	fakeScheduler := &FakeScheduler{}
	//
	expectedByNodeID := map[string]Task{
		"A": {
			NodeID:   "A",
			ToolName: "hotel-search",
			Arguments: map[string]any{
				"city": "上海",
			},
			Criteria: &FakeCriteria{
				Name: "上海",
			},
		},
		"B": {
			NodeID:   "B",
			ToolName: "weather-search",
			Arguments: map[string]any{
				"city": "北京",
			},
			Criteria: &FakeCriteria{
				Name: "北京",
			},
		},
	}
	runtimeEngine := &Runtime{
		graph: GraphDefinition{
			Nodes: []Node{
				{
					ID:       "A",
					Name:     "node-hotel",
					ToolName: "hotel-search",
					Arguments: map[string]any{
						"city": "上海",
					},
					Criteria: &FakeCriteria{
						Name: "上海",
					},
				},
				{
					ID:       "B",
					Name:     "node-hotel",
					ToolName: "weather-search",
					Arguments: map[string]any{
						"city": "北京",
					},
					Criteria: &FakeCriteria{
						Name: "北京",
					},
				},
			},
		},
		executor:   recordExecutor,
		taskPolicy: recordTaskPolicy,
		scheduler:  fakeScheduler,
		workerNum:  2,
	}

	runtimeEngine.initWorker(runtimeEngine.workerNum)
	runtimeEngine.startWorker(context.Background())
	defer runtimeEngine.stopWorker()
	readyTaskSet, err := runtimeEngine.scheduler.Schedule(runtimeEngine.graph, RuntimeSnapshot{
		RuntimeID:     "keep-task-ctx",
		CompleteNodes: map[string]bool{},
		Status:        RuntimeRunning,
		FailedNodes:   make(map[string]bool),
		Version:       0,
		SchemaVersion: 0,
	})
	if err != nil {
		t.Fatalf("scheduler schedule err: %v", err)
	}
	seenReady := make(map[string]bool)

	for _, task := range readyTaskSet.Tasks {
		wantTask, ok := expectedByNodeID[task.NodeID]
		if !ok {
			t.Fatalf("scheduler returned unexpected task NodeID = %q", task.NodeID)
		}
		if seenReady[task.NodeID] {
			t.Fatalf("scheduler returned duplicate task NodeID = %q", task.NodeID)
		}
		seenReady[task.NodeID] = true
		if !reflect.DeepEqual(task.Criteria, wantTask.Criteria) {
			t.Fatalf(
				"scheduler task %q Criteria = %#v, want %#v",
				task.NodeID,
				task.Criteria,
				wantTask.Criteria,
			)
		}

	}

	outcomes := runtimeEngine.executeReadyTasks(readyTaskSet)

	if got, want := len(outcomes), len(readyTaskSet.Tasks); got != want {
		t.Fatalf("outcome count = %d, want %d", got, want)
	}
	seen := make(map[string]bool)
	recordedTasks := recordTaskPolicy.Tasks()
	if got, want := len(recordedTasks), len(expectedByNodeID); got != want {
		t.Fatalf("recorded task count = %d, want %d", got, want)
	}
	for _, gotTask := range recordedTasks {
		wantTask, ok := expectedByNodeID[gotTask.NodeID]
		if !ok {
			t.Fatalf("unexpected task NodeID = %q", gotTask.NodeID)
		}

		if seen[gotTask.NodeID] {
			t.Fatalf("duplicate task NodeID = %q", gotTask.NodeID)
		}
		seen[gotTask.NodeID] = true

		if gotTask.ToolName != wantTask.ToolName {
			t.Fatalf(
				"task %q ToolName = %q, want %q",
				gotTask.NodeID,
				gotTask.ToolName,
				wantTask.ToolName,
			)
		}

		if !reflect.DeepEqual(gotTask.Arguments, wantTask.Arguments) {
			t.Fatalf(
				"task %q Arguments = %#v, want %#v",
				gotTask.NodeID,
				gotTask.Arguments,
				wantTask.Arguments,
			)
		}
		if !reflect.DeepEqual(gotTask.Criteria, wantTask.Criteria) {
			t.Fatalf("task %q Criteria = %v, want %v", gotTask.NodeID, gotTask.Criteria, wantTask.Criteria)
		}
	}
}
