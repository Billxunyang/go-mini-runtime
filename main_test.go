package main

import (
	"fmt"
	"reflect"
	"sync"
	"testing"
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

func (r *RecordingExecutor) Execute(task Task) TaskResult {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.recordIDs = append(r.recordIDs, task.NodeID)
	return TaskResult{NodeID: task.NodeID}
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
	for nodeId, completed := range snapshot.CompleteNodes {
		dstSnapshot.CompleteNodes[nodeId] = completed
	}
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
		Edges: []Edge{{From: "A", To: "B", NodeId: "B"}, {From: "B", To: "C", NodeId: "C"}, {From: "", To: "A", NodeId: "A"}},
	}
	snapshot := RuntimeSnapshot{
		RuntimeID:     "test-runtime-abc",
		Status:        RuntimeRunning,
		CompleteNodes: make(map[string]bool),
	}
	fakeSchedule := &FakeScheduler{}
	recordExecutor := &RecordingExecutor{}
	fakeTaskPolicy := &FakeTaskPolicy{}
	fakeCommiter := &FakeCommitter{}
	recordCheckpointer := &RecordingCheckpointer{}
	fakeDecision := &FakeDecision{}
	nRun := NewRuntime(graph, 2, fakeSchedule, recordExecutor, fakeTaskPolicy, fakeCommiter, recordCheckpointer, fakeDecision)

	finalSnapshot, err := nRun.runLoop(snapshot)
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
	runtimeEngine.startWorker()
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
