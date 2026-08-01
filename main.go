package main

import (
	"fmt"
	"sync"
)

type GraphDefinition struct {

	// Graph definition
	// TODO: define graph definition
	Edges []Edge
	Nodes []Node
}

type Node struct {
	ID   string
	Name string
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
}

type Task struct {
	// Task
	// TODO: define task
	NodeID string
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

func (s *FakeScheduler) Schedule(graph GraphDefinition, snapshot RuntimeSnapshot) (ReadyTaskSet, error) {
	// find not running finished nodes
	readyTaskSet := ReadyTaskSet{Tasks: make([]Task, 0)}
	if len(graph.Nodes) == 0 {
		return readyTaskSet, fmt.Errorf("no nodes in graph")
	}
	for _, node := range graph.Nodes {
		// if finish continue
		if _, ok := snapshot.CompleteNodes[node.ID]; ok {
			continue
		}
		readyTaskSet.Tasks = append(readyTaskSet.Tasks, Task{NodeID: node.ID})
	}
	return readyTaskSet, nil
}

// tasks -> dispatch -> execute

type TaskResult struct {
	// Task result
	// TODO: define task result
	NodeID string
	Err    error
}
type Executor interface {
	// Execute a task return a taskResult
	Execute(task Task) TaskResult
}

type FakeExecutor struct {
}

func (fe *FakeExecutor) Execute(task Task) TaskResult {
	fmt.Println("执行任务", task.NodeID)
	return TaskResult{NodeID: task.NodeID, Err: nil}
}

type FakeTaskPolicy struct {
}

func (tp *FakeTaskPolicy) Evaluate(result TaskResult) TaskOutcome {
	if result.Err != nil {
		return TaskOutcome{NodeID: result.NodeID, Success: false}
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
		fmt.Println("保存结果", outcome.NodeID)
		if outcome.Success {

			snapshot.CompleteNodes[outcome.NodeID] = true
		}
	}
	return snapshot, nil
}

type FakeCheckpointer struct {
}

func (f *FakeCheckpointer) Save(snapshot RuntimeSnapshot) error {
	return nil
}

func (f *FakeCheckpointer) Load(runtimeId string) (RuntimeSnapshot, error) {

	return RuntimeSnapshot{RuntimeID: runtimeId}, nil
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

func (r *Runtime) startWorker() {
	for i := 0; i < r.workerNum; i++ {
		r.waitGroup.Add(1)
		go func() {
			defer r.waitGroup.Done()
			for taskInfo := range r.TaskQueue {
				result := r.executor.Execute(taskInfo)
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

func (r *Runtime) runLoop(snapshot RuntimeSnapshot, workerNum int) (err error) {
	var taskSet ReadyTaskSet
	r.initWorker(workerNum)
	defer r.stopWorker()
	r.startWorker()
	needContinue := false
	for {
		taskSet, err = r.scheduler.Schedule(r.graph, snapshot)
		if err != nil {
			fmt.Println("get scheduler failed ", err)
			return
		}
		if len(taskSet.Tasks) == 0 {
			needContinue, err = r.decideLoop(snapshot)
			if needContinue {
				continue
			} else {
				return
			}
		}
		taskOutcomes := r.executeReadyTasks(taskSet)
		snapshot, err = r.committer.Commit(snapshot, taskOutcomes)
		if err != nil {
			fmt.Println("commit err ", err)
			return
		}
		err = r.checkpointer.Save(snapshot)
		if err != nil {
			fmt.Println("checkpointer err ", err)
			return
		}
		needContinue, err = r.decideLoop(snapshot)
		if needContinue {
			continue
		} else {
			return
		}
	}
}

func (r *Runtime) decideLoop(snapshot RuntimeSnapshot) (loopContinue bool, err error) {
	loopContinue = false
	runStatus := r.statusDeterminer.Decision(r.graph, snapshot)
	switch runStatus {
	case RuntimeSuccess:
		return
	case RuntimeFailed:
		err = fmt.Errorf("runtime failed ")
		return
	case RuntimeWaiting:
		err = fmt.Errorf("runtime waiting ")
		return
	case RuntimeDeadLock:
		err = fmt.Errorf("runtime deadlock")
		return
	case RuntimeRunning:
		loopContinue = true
	default:
		err = fmt.Errorf("runtime unknown")
		return
	}
	return
}

func NewRuntimeTest() (err error) {
	nRun := &Runtime{
		graph: GraphDefinition{
			Nodes: []Node{{ID: "A", Name: "node-A"}, {ID: "B", Name: "node-B"}, {ID: "C", Name: "node-C"}},
			Edges: []Edge{{From: "A", To: "B", NodeId: "B"}, {From: "B", To: "C", NodeId: "B"}, {From: "", To: "A", NodeId: "A"}},
		},
		workerNum: 2,
	}
	snapshot := RuntimeSnapshot{
		CompleteNodes: make(map[string]bool),
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
		taskResult := fakeExecutor.Execute(taskInfo)
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
		Edges: []Edge{{From: "A", To: "B", NodeId: "B"}, {From: "B", To: "C", NodeId: "B"}, {From: "", To: "A", NodeId: "A"}},
	}
	fakeSchedule := FakeScheduler{}
	fakeExecutor := FakeExecutor{}
	fakeTaskPolicy := FakeTaskPolicy{}
	fakeCommiter := FakeCommitter{}
	fakeCheckpointer := FakeCheckpointer{}
	fakeDecision := FakeDecision{}
	runtime := NewRuntime(graph, 8, &fakeSchedule, &fakeExecutor, &fakeTaskPolicy, &fakeCommiter, &fakeCheckpointer, &fakeDecision)
	snapshot := RuntimeSnapshot{RuntimeID: "1", CompleteNodes: make(map[string]bool), Status: RuntimeRunning}
	err := runtime.runLoop(snapshot, runtime.workerNum)
	if err != nil {
		fmt.Println(err)
	}
}
