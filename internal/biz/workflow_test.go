package biz

import (
	"context"
	stderrors "errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
)

type fakeWorkerNodeRepo struct {
	nodes map[NodeType]ActivityNode
}

func (r *fakeWorkerNodeRepo) GetActivityNodes() []ActivityNode {
	nodes := make([]ActivityNode, 0, len(r.nodes))
	for _, node := range r.nodes {
		nodes = append(nodes, node)
	}
	return nodes
}

func (r *fakeWorkerNodeRepo) GetActivityNode(nodeType NodeType) ActivityNode {
	return r.nodes[nodeType]
}

type fakeWfExecutionStatusRepo struct {
	markErr   error
	finishErr error
	markedIDs []string
	finishes  []FinishWfExecutionParam
}

func (r *fakeWfExecutionStatusRepo) MarkWfExecutionRunning(_ context.Context, workflowExecutionID string) error {
	r.markedIDs = append(r.markedIDs, workflowExecutionID)
	return r.markErr
}

func (r *fakeWfExecutionStatusRepo) FinishWfExecution(_ context.Context, param FinishWfExecutionParam) error {
	r.finishes = append(r.finishes, param)
	return r.finishErr
}

type fakeActivityNode struct {
	nodeType    NodeType
	retryPolicy temporal.RetryPolicy
}

func (n *fakeActivityNode) Type() NodeType {
	return n.nodeType
}

func (n *fakeActivityNode) Schema() NodeSchema {
	return objectNodeSchema(n.nodeType)
}

func (n *fakeActivityNode) RetryPolicy() temporal.RetryPolicy {
	return n.retryPolicy
}

func (n *fakeActivityNode) Activity(_ context.Context, input map[string]any) (map[string]any, error) {
	return input, nil
}

type statusRecorder struct {
	mu     sync.Mutex
	inputs []UpdateWfExecutionStatusParam
	errors map[WfExecutionStatus]error
}

func (r *statusRecorder) Activity(_ context.Context, param UpdateWfExecutionStatusParam) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.inputs = append(r.inputs, param)
	return r.errors[param.Status]
}

func (r *statusRecorder) Inputs() []UpdateWfExecutionStatusParam {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]UpdateWfExecutionStatusParam(nil), r.inputs...)
}

func newTestWorkerUsecase() (*WorkerUsecase, *fakeWorkerNodeRepo) {
	repo := &fakeWorkerNodeRepo{nodes: make(map[NodeType]ActivityNode)}
	for _, nodeType := range []NodeType{
		NodeTypeSaySomething,
		NodeTypeExample,
		NodeType("ACTIVITY_A"),
		NodeType("ACTIVITY_B"),
		NodeType("ACTIVITY_C"),
		NodeType("ACTIVITY_D"),
		NodeType("ACTIVITY_E"),
		NodeType("ACTIVITY_F"),
		NodeType("ACTIVITY_G"),
		NodeType("ACTIVITY_H"),
		NodeType("ACTIVITY_I"),
		NodeType("ACTIVITY_J"),
	} {
		repo.nodes[nodeType] = &fakeActivityNode{
			nodeType:    nodeType,
			retryPolicy: temporal.RetryPolicy{MaximumAttempts: 3},
		}
	}
	return NewWorkerUsecase(&fakeWfExecutionStatusRepo{}, &nodeExecutionStatusRecorder{}, repo), repo
}

func newTestWorkflowEnvironment(t *testing.T, uc *WorkerUsecase, repo *fakeWorkerNodeRepo, recorder *statusRecorder) *testsuite.TestWorkflowEnvironment {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: "workflow-execution-test"})
	for nodeType, node := range repo.nodes {
		env.RegisterActivityWithOptions(node.Activity, activity.RegisterOptions{Name: string(nodeType)})
	}
	env.RegisterActivityWithOptions(recorder.Activity, activity.RegisterOptions{Name: UpdateWfExecutionStatusActivityType})
	env.OnActivity(UpdateWfExecutionStatusActivityType, mock.Anything, mock.Anything).Return(recorder.Activity)
	t.Cleanup(func() {
		env.AssertExpectations(t)
	})
	return env
}

func executeDynamicWorkflow(t *testing.T, env *testsuite.TestWorkflowEnvironment, uc *WorkerUsecase, definition DefinitionGraph) (*DynamicWorkflowResult, error) {
	t.Helper()
	env.ExecuteWorkflow(uc.DynamicWorkflow, DynamicWorkflowParam{Definition: definition})
	if !env.IsWorkflowCompleted() {
		t.Fatal("DynamicWorkflow did not complete")
	}
	if err := env.GetWorkflowError(); err != nil {
		return nil, err
	}
	var result DynamicWorkflowResult
	if err := env.GetWorkflowResult(&result); err != nil {
		t.Fatalf("GetWorkflowResult() error = %v", err)
	}
	return &result, nil
}

func TestDynamicWorkflow(t *testing.T) {
	tests := []struct {
		name string
		run  func(*testing.T)
	}{
		{name: "linear flow", run: testDynamicWorkflowLinearFlow},
		{name: "parallel fan-out and join", run: testDynamicWorkflowParallelFanOutAndJoin},
		{name: "multiple start nodes and control edge", run: testDynamicWorkflowMultipleStartNodesAndControlEdge},
		{name: "exclusive selects first matching branch", run: testDynamicWorkflowExclusiveSelectsFirstMatchingBranch},
		{name: "inclusive selects all matching branches", run: testDynamicWorkflowInclusiveSelectsAllMatchingBranches},
		{name: "no matching branch returns empty output", run: testDynamicWorkflowNoMatchingBranchReturnsEmptyOutput},
		{name: "skipped nested router unblocks join", run: testDynamicWorkflowSkippedNestedRouterUnblocksJoin},
		{name: "flow failures", run: testDynamicWorkflowFlowFailures},
		{name: "activity retry policy", run: testDynamicWorkflowActivityRetryPolicy},
		{name: "router failures", run: testDynamicWorkflowRouterFailures},
		{name: "invalid definitions", run: testDynamicWorkflowRejectsInvalidDefinitions},
		{name: "status transitions", run: testDynamicWorkflowStatusTransitions},
		{name: "cancellation updates canceled status", run: testDynamicWorkflowCancellationUpdatesCanceledStatus},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.run)
	}
}

func testDynamicWorkflowLinearFlow(t *testing.T) {
	uc, repo := newTestWorkerUsecase()
	recorder := &statusRecorder{errors: make(map[WfExecutionStatus]error)}
	env := newTestWorkflowEnvironment(t, uc, repo, recorder)

	env.OnActivity(string(NodeTypeSaySomething), mock.Anything, map[string]any{"seed": "alpha"}).
		Return(map[string]any{"text": "alpha-result"}, nil).Once()
	env.OnActivity(string(NodeTypeExample), mock.Anything, map[string]any{"preset": "keep", "prompt": "alpha-result"}).
		Return(map[string]any{"result": "dag-complete"}, nil).Once()

	result, err := executeDynamicWorkflow(t, env, uc, DefinitionGraph{
		Nodes: []DefNode{
			{ID: "source", Type: NodeTypeSaySomething, Preset: map[string]any{"seed": "alpha"}},
			{ID: "transform", Type: NodeTypeExample, Preset: map[string]any{"preset": "keep"}},
			{ID: "output", Type: NodeTypeOutput},
		},
		Edges: []DefEdge{
			{FromNode: "source", FromKey: "text", ToNode: "transform", ToKey: "prompt"},
			{FromNode: "transform", FromKey: "result", ToNode: "output", ToKey: "result"},
		},
	})
	if err != nil {
		t.Fatalf("DynamicWorkflow() error = %v", err)
	}
	if want := map[string]any{"result": "dag-complete"}; !reflect.DeepEqual(result.Outputs, want) {
		t.Fatalf("DynamicWorkflow() outputs = %#v, want %#v", result.Outputs, want)
	}
	assertStatusTransitions(t, recorder.Inputs(), WfExecutionStatusRunning, WfExecutionStatusCompleted)
}

func testDynamicWorkflowParallelFanOutAndJoin(t *testing.T) {
	uc, repo := newTestWorkerUsecase()
	recorder := &statusRecorder{errors: make(map[WfExecutionStatus]error)}
	env := newTestWorkflowEnvironment(t, uc, repo, recorder)

	env.OnActivity(string(NodeTypeSaySomething), mock.Anything, mock.Anything).
		Return(map[string]any{"seed": "alpha"}, nil).Once()
	env.OnActivity(string(NodeType("ACTIVITY_A")), mock.Anything, map[string]any{"input": "alpha"}).
		Return(map[string]any{"left": "image"}, nil).Once()
	env.OnActivity(string(NodeType("ACTIVITY_B")), mock.Anything, map[string]any{"input": "alpha"}).
		Return(map[string]any{"right": "video"}, nil).Once()
	env.OnActivity(string(NodeType("ACTIVITY_C")), mock.Anything, map[string]any{"left": "image", "right": "video"}).
		Return(map[string]any{"joined": "image+video"}, nil).Once()

	result, err := executeDynamicWorkflow(t, env, uc, DefinitionGraph{
		Nodes: []DefNode{
			{ID: "source", Type: NodeTypeSaySomething},
			{ID: "left", Type: NodeType("ACTIVITY_A")},
			{ID: "right", Type: NodeType("ACTIVITY_B")},
			{ID: "join", Type: NodeType("ACTIVITY_C")},
			{ID: "output", Type: NodeTypeOutput},
		},
		Edges: []DefEdge{
			{FromNode: "source", FromKey: "seed", ToNode: "left", ToKey: "input"},
			{FromNode: "source", FromKey: "seed", ToNode: "right", ToKey: "input"},
			{FromNode: "left", FromKey: "left", ToNode: "join", ToKey: "left"},
			{FromNode: "right", FromKey: "right", ToNode: "join", ToKey: "right"},
			{FromNode: "join", FromKey: "joined", ToNode: "output", ToKey: "result"},
		},
	})
	if err != nil {
		t.Fatalf("DynamicWorkflow() error = %v", err)
	}
	if got := result.Outputs["result"]; got != "image+video" {
		t.Fatalf("DynamicWorkflow() result = %#v, want image+video", got)
	}
}

func testDynamicWorkflowMultipleStartNodesAndControlEdge(t *testing.T) {
	uc, repo := newTestWorkerUsecase()
	recorder := &statusRecorder{errors: make(map[WfExecutionStatus]error)}
	env := newTestWorkflowEnvironment(t, uc, repo, recorder)

	env.OnActivity(string(NodeTypeSaySomething), mock.Anything, mock.Anything).
		Return(map[string]any{"ignored": "a"}, nil).Once()
	env.OnActivity(string(NodeTypeExample), mock.Anything, mock.Anything).
		Return(map[string]any{"ignored": "b"}, nil).Once()
	env.OnActivity(string(NodeType("ACTIVITY_C")), mock.Anything, map[string]any{"preset": "ready"}).
		Return(map[string]any{"result": "done"}, nil).Once()

	result, err := executeDynamicWorkflow(t, env, uc, DefinitionGraph{
		Nodes: []DefNode{
			{ID: "first", Type: NodeTypeSaySomething},
			{ID: "second", Type: NodeTypeExample},
			{ID: "join", Type: NodeType("ACTIVITY_C"), Preset: map[string]any{"preset": "ready"}},
			{ID: "output", Type: NodeTypeOutput},
		},
		Edges: []DefEdge{
			{FromNode: "first", ToNode: "join"},
			{FromNode: "second", ToNode: "join"},
			{FromNode: "join", FromKey: "result", ToNode: "output", ToKey: "result"},
		},
	})
	if err != nil {
		t.Fatalf("DynamicWorkflow() error = %v", err)
	}
	if got := result.Outputs["result"]; got != "done" {
		t.Fatalf("DynamicWorkflow() result = %#v, want done", got)
	}
}

func testDynamicWorkflowExclusiveSelectsFirstMatchingBranch(t *testing.T) {
	uc, repo := newTestWorkerUsecase()
	recorder := &statusRecorder{errors: make(map[WfExecutionStatus]error)}
	env := newTestWorkflowEnvironment(t, uc, repo, recorder)

	env.OnActivity(string(NodeTypeSaySomething), mock.Anything, mock.Anything).
		Return(map[string]any{"score": 90}, nil).Once()
	env.OnActivity(string(NodeType("ACTIVITY_A")), mock.Anything, map[string]any{"score": float64(90)}).
		Return(map[string]any{"choice": "first"}, nil).Once()

	result, err := executeDynamicWorkflow(t, env, uc, DefinitionGraph{
		Nodes: []DefNode{
			{ID: "source", Type: NodeTypeSaySomething},
			{ID: "router", Type: NodeTypeExclusive, Preset: branchesPreset(
				Branch{ID: "first", Expression: "inputs.score >= 80"},
				Branch{ID: "second", Expression: "inputs.score >= 60"},
			)},
			{ID: "first", Type: NodeType("ACTIVITY_A")},
			{ID: "second", Type: NodeType("ACTIVITY_B")},
			{ID: "output", Type: NodeTypeOutput},
		},
		Edges: []DefEdge{
			{FromNode: "source", FromKey: "score", ToNode: "router", ToKey: "score"},
			{FromNode: "router", FromBranch: "first", FromKey: "score", ToNode: "first", ToKey: "score"},
			{FromNode: "router", FromBranch: "second", FromKey: "score", ToNode: "second", ToKey: "score"},
			{FromNode: "first", FromKey: "choice", ToNode: "output", ToKey: "choice"},
			{FromNode: "second", FromKey: "choice", ToNode: "output", ToKey: "choice"},
		},
	})
	if err != nil {
		t.Fatalf("DynamicWorkflow() error = %v", err)
	}
	if got := result.Outputs["choice"]; got != "first" {
		t.Fatalf("DynamicWorkflow() choice = %#v, want first", got)
	}
}

func testDynamicWorkflowInclusiveSelectsAllMatchingBranches(t *testing.T) {
	uc, repo := newTestWorkerUsecase()
	recorder := &statusRecorder{errors: make(map[WfExecutionStatus]error)}
	env := newTestWorkflowEnvironment(t, uc, repo, recorder)

	env.OnActivity(string(NodeTypeSaySomething), mock.Anything, mock.Anything).
		Return(map[string]any{"left": true, "right": true}, nil).Once()
	env.OnActivity(string(NodeType("ACTIVITY_A")), mock.Anything, map[string]any{"selected": true}).
		Return(map[string]any{"left": "L"}, nil).Once()
	env.OnActivity(string(NodeType("ACTIVITY_B")), mock.Anything, map[string]any{"selected": true}).
		Return(map[string]any{"right": "R"}, nil).Once()

	result, err := executeDynamicWorkflow(t, env, uc, DefinitionGraph{
		Nodes: []DefNode{
			{ID: "source", Type: NodeTypeSaySomething},
			{ID: "router", Type: NodeTypeInclusive, Preset: branchesPreset(
				Branch{ID: "left", Expression: "inputs.left"},
				Branch{ID: "right", Expression: "inputs.right"},
			)},
			{ID: "left", Type: NodeType("ACTIVITY_A")},
			{ID: "right", Type: NodeType("ACTIVITY_B")},
			{ID: "output", Type: NodeTypeOutput},
		},
		Edges: []DefEdge{
			{FromNode: "source", FromKey: "left", ToNode: "router", ToKey: "left"},
			{FromNode: "source", FromKey: "right", ToNode: "router", ToKey: "right"},
			{FromNode: "router", FromBranch: "left", FromKey: "left", ToNode: "left", ToKey: "selected"},
			{FromNode: "router", FromBranch: "right", FromKey: "right", ToNode: "right", ToKey: "selected"},
			{FromNode: "left", FromKey: "left", ToNode: "output", ToKey: "left"},
			{FromNode: "right", FromKey: "right", ToNode: "output", ToKey: "right"},
		},
	})
	if err != nil {
		t.Fatalf("DynamicWorkflow() error = %v", err)
	}
	if want := map[string]any{"left": "L", "right": "R"}; !reflect.DeepEqual(result.Outputs, want) {
		t.Fatalf("DynamicWorkflow() outputs = %#v, want %#v", result.Outputs, want)
	}
}

func testDynamicWorkflowNoMatchingBranchReturnsEmptyOutput(t *testing.T) {
	uc, repo := newTestWorkerUsecase()
	recorder := &statusRecorder{errors: make(map[WfExecutionStatus]error)}
	env := newTestWorkflowEnvironment(t, uc, repo, recorder)

	env.OnActivity(string(NodeTypeSaySomething), mock.Anything, mock.Anything).
		Return(map[string]any{"selected": false}, nil).Once()

	result, err := executeDynamicWorkflow(t, env, uc, DefinitionGraph{
		Nodes: []DefNode{
			{ID: "source", Type: NodeTypeSaySomething},
			{ID: "router", Type: NodeTypeExclusive, Preset: branchesPreset(Branch{ID: "yes", Expression: "inputs.selected"})},
			{ID: "activity", Type: NodeType("ACTIVITY_A")},
			{ID: "output", Type: NodeTypeOutput},
		},
		Edges: []DefEdge{
			{FromNode: "source", FromKey: "selected", ToNode: "router", ToKey: "selected"},
			{FromNode: "router", FromBranch: "yes", FromKey: "selected", ToNode: "activity", ToKey: "selected"},
			{FromNode: "activity", FromKey: "result", ToNode: "output", ToKey: "result"},
		},
	})
	if err != nil {
		t.Fatalf("DynamicWorkflow() error = %v", err)
	}
	if result.Outputs == nil {
		t.Fatal("DynamicWorkflow() outputs = nil, want empty map")
	}
	if len(result.Outputs) != 0 {
		t.Fatalf("DynamicWorkflow() outputs = %#v, want empty", result.Outputs)
	}
}

func testDynamicWorkflowSkippedNestedRouterUnblocksJoin(t *testing.T) {
	uc, repo := newTestWorkerUsecase()
	recorder := &statusRecorder{errors: make(map[WfExecutionStatus]error)}
	env := newTestWorkflowEnvironment(t, uc, repo, recorder)

	env.OnActivity(string(NodeTypeSaySomething), mock.Anything, mock.Anything).
		Return(map[string]any{"use_direct": true}, nil).Once()
	env.OnActivity(string(NodeType("ACTIVITY_A")), mock.Anything, map[string]any{"selected": true}).
		Return(map[string]any{"result": "direct"}, nil).Once()

	result, err := executeDynamicWorkflow(t, env, uc, DefinitionGraph{
		Nodes: []DefNode{
			{ID: "source", Type: NodeTypeSaySomething},
			{ID: "outer", Type: NodeTypeExclusive, Preset: branchesPreset(
				Branch{ID: "direct", Expression: "inputs.use_direct"},
				Branch{ID: "nested", Expression: "!inputs.use_direct"},
			)},
			{ID: "direct", Type: NodeType("ACTIVITY_A")},
			{ID: "nested", Type: NodeTypeExclusive, Preset: branchesPreset(Branch{ID: "child", Expression: "true"})},
			{ID: "child", Type: NodeType("ACTIVITY_B")},
			{ID: "output", Type: NodeTypeOutput},
		},
		Edges: []DefEdge{
			{FromNode: "source", FromKey: "use_direct", ToNode: "outer", ToKey: "use_direct"},
			{FromNode: "outer", FromBranch: "direct", FromKey: "use_direct", ToNode: "direct", ToKey: "selected"},
			{FromNode: "outer", FromBranch: "nested", FromKey: "use_direct", ToNode: "nested", ToKey: "use_direct"},
			{FromNode: "nested", FromBranch: "child", FromKey: "use_direct", ToNode: "child", ToKey: "selected"},
			{FromNode: "direct", FromKey: "result", ToNode: "output", ToKey: "result"},
			{FromNode: "child", FromKey: "result", ToNode: "output", ToKey: "child"},
		},
	})
	if err != nil {
		t.Fatalf("DynamicWorkflow() error = %v", err)
	}
	if want := map[string]any{"result": "direct"}; !reflect.DeepEqual(result.Outputs, want) {
		t.Fatalf("DynamicWorkflow() outputs = %#v, want %#v", result.Outputs, want)
	}
}

func testDynamicWorkflowFlowFailures(t *testing.T) {
	tests := []struct {
		name             string
		definition       DefinitionGraph
		mockNode         NodeType
		output           map[string]any
		activityErr      error
		wantFailedNodeID string
	}{
		{
			name: "activity error",
			definition: DefinitionGraph{
				Nodes: []DefNode{{ID: "activity", Type: NodeTypeSaySomething}, {ID: "output", Type: NodeTypeOutput}},
				Edges: []DefEdge{{FromNode: "activity", FromKey: "result", ToNode: "output", ToKey: "result"}},
			},
			mockNode:         NodeTypeSaySomething,
			activityErr:      temporal.NewNonRetryableApplicationError("activity failed", "ActivityFailed", nil),
			wantFailedNodeID: "activity",
		},
		{
			name: "activity output key missing",
			definition: DefinitionGraph{
				Nodes: []DefNode{{ID: "activity", Type: NodeTypeSaySomething}, {ID: "output", Type: NodeTypeOutput}},
				Edges: []DefEdge{{FromNode: "activity", FromKey: "missing", ToNode: "output", ToKey: "result"}},
			},
			mockNode: NodeTypeSaySomething,
			output:   map[string]any{"result": "present"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			uc, repo := newTestWorkerUsecase()
			recorder := &statusRecorder{errors: make(map[WfExecutionStatus]error)}
			env := newTestWorkflowEnvironment(t, uc, repo, recorder)
			env.OnActivity(string(tt.mockNode), mock.Anything, mock.Anything).Return(tt.output, tt.activityErr).Once()

			_, err := executeDynamicWorkflow(t, env, uc, tt.definition)
			if err == nil {
				t.Fatal("DynamicWorkflow() error = nil, want error")
			}
			assertStatusTransitions(t, recorder.Inputs(), WfExecutionStatusRunning, WfExecutionStatusFailed)
			if tt.wantFailedNodeID != "" {
				assertFailedNodeID(t, recorder.Inputs(), tt.wantFailedNodeID)
			}
		})
	}
}

func testDynamicWorkflowActivityRetryPolicy(t *testing.T) {
	uc, repo := newTestWorkerUsecase()
	repo.nodes[NodeTypeSaySomething].(*fakeActivityNode).retryPolicy = temporal.RetryPolicy{
		InitialInterval: time.Millisecond,
		MaximumAttempts: 2,
	}
	recorder := &statusRecorder{errors: make(map[WfExecutionStatus]error)}
	env := newTestWorkflowEnvironment(t, uc, repo, recorder)
	retryableErr := temporal.NewApplicationError("retryable activity failure", "RetryableActivityFailure")
	env.OnActivity(string(NodeTypeSaySomething), mock.Anything, mock.Anything).
		Return(nil, retryableErr).Twice()

	_, err := executeDynamicWorkflow(t, env, uc, DefinitionGraph{
		Nodes: []DefNode{
			{ID: "activity", Type: NodeTypeSaySomething},
			{ID: "output", Type: NodeTypeOutput},
		},
		Edges: []DefEdge{{FromNode: "activity", FromKey: "result", ToNode: "output", ToKey: "result"}},
	})
	if err == nil || !containsErrorText(err, "retryable activity failure") {
		t.Fatalf("DynamicWorkflow() error = %v, want exhausted retry error", err)
	}
	assertStatusTransitions(t, recorder.Inputs(), WfExecutionStatusRunning, WfExecutionStatusFailed)
}

func testDynamicWorkflowRouterFailures(t *testing.T) {
	tests := []struct {
		name       string
		preset     map[string]any
		sourceData map[string]any
		edgeKey    string
	}{
		{
			name:       "selected branch input key missing",
			preset:     branchesPreset(Branch{ID: "selected", Expression: "true"}),
			sourceData: map[string]any{"other": true},
			edgeKey:    "missing",
		},
		{
			name:       "expression evaluation error",
			preset:     branchesPreset(Branch{ID: "selected", Expression: "inputs.other + 1 > 2"}),
			sourceData: map[string]any{"other": true},
			edgeKey:    "other",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			uc, repo := newTestWorkerUsecase()
			recorder := &statusRecorder{errors: make(map[WfExecutionStatus]error)}
			env := newTestWorkflowEnvironment(t, uc, repo, recorder)
			env.OnActivity(string(NodeTypeSaySomething), mock.Anything, mock.Anything).Return(tt.sourceData, nil).Once()

			_, err := executeDynamicWorkflow(t, env, uc, DefinitionGraph{
				Nodes: []DefNode{
					{ID: "source", Type: NodeTypeSaySomething},
					{ID: "router", Type: NodeTypeExclusive, Preset: tt.preset},
					{ID: "selected", Type: NodeType("ACTIVITY_A")},
					{ID: "output", Type: NodeTypeOutput},
				},
				Edges: []DefEdge{
					{FromNode: "source", FromKey: "other", ToNode: "router", ToKey: "other"},
					{FromNode: "router", FromBranch: "selected", FromKey: tt.edgeKey, ToNode: "selected", ToKey: "input"},
					{FromNode: "selected", FromKey: "result", ToNode: "output", ToKey: "result"},
				},
			})
			if err == nil {
				t.Fatal("DynamicWorkflow() error = nil, want error")
			}
		})
	}
}

func testDynamicWorkflowRejectsInvalidDefinitions(t *testing.T) {
	tests := []struct {
		name       string
		definition DefinitionGraph
	}{
		{name: "empty graph", definition: DefinitionGraph{}},
		{name: "unknown node type", definition: DefinitionGraph{Nodes: []DefNode{{ID: "bad", Type: "UNKNOWN"}, {ID: "output", Type: NodeTypeOutput}}}},
		{name: "missing edge source", definition: DefinitionGraph{Nodes: []DefNode{{ID: "output", Type: NodeTypeOutput}}, Edges: []DefEdge{{FromNode: "missing", ToNode: "output"}}}},
		{name: "missing edge target", definition: DefinitionGraph{Nodes: []DefNode{{ID: "source", Type: NodeTypeSaySomething}, {ID: "output", Type: NodeTypeOutput}}, Edges: []DefEdge{{FromNode: "source", ToNode: "missing"}}}},
		{name: "router edge without branch", definition: DefinitionGraph{Nodes: []DefNode{{ID: "router", Type: NodeTypeExclusive, Preset: branchesPreset(Branch{ID: "yes", Expression: "true"})}, {ID: "output", Type: NodeTypeOutput}}, Edges: []DefEdge{{FromNode: "router", ToNode: "output"}}}},
		{name: "activity edge with branch", definition: DefinitionGraph{Nodes: []DefNode{{ID: "source", Type: NodeTypeSaySomething}, {ID: "output", Type: NodeTypeOutput}}, Edges: []DefEdge{{FromNode: "source", FromBranch: "bad", ToNode: "output"}}}},
		{name: "router missing branches", definition: DefinitionGraph{Nodes: []DefNode{{ID: "router", Type: NodeTypeExclusive}, {ID: "output", Type: NodeTypeOutput}}}},
		{name: "router branches malformed", definition: DefinitionGraph{Nodes: []DefNode{{ID: "router", Type: NodeTypeExclusive, Preset: map[string]any{presetBranchesKey: "bad"}}, {ID: "output", Type: NodeTypeOutput}}}},
		{name: "router expression invalid", definition: DefinitionGraph{Nodes: []DefNode{{ID: "router", Type: NodeTypeExclusive, Preset: branchesPreset(Branch{ID: "yes", Expression: "("})}, {ID: "output", Type: NodeTypeOutput}}}},
		{name: "cycle", definition: DefinitionGraph{Nodes: []DefNode{{ID: "first", Type: NodeTypeSaySomething}, {ID: "second", Type: NodeTypeExample}, {ID: "output", Type: NodeTypeOutput}}, Edges: []DefEdge{{FromNode: "first", ToNode: "second"}, {FromNode: "second", ToNode: "first"}, {FromNode: "second", ToNode: "output"}}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			uc, repo := newTestWorkerUsecase()
			recorder := &statusRecorder{errors: make(map[WfExecutionStatus]error)}
			env := newTestWorkflowEnvironment(t, uc, repo, recorder)
			_, err := executeDynamicWorkflow(t, env, uc, tt.definition)
			if err == nil {
				t.Fatal("DynamicWorkflow() error = nil, want error")
			}
		})
	}
}

func testDynamicWorkflowStatusTransitions(t *testing.T) {
	tests := []struct {
		name          string
		activityError error
		statusErrors  map[WfExecutionStatus]error
		cancel        bool
		wantStatuses  []WfExecutionStatus
		wantErrorText string
	}{
		{
			name:          "running update failure",
			statusErrors:  map[WfExecutionStatus]error{WfExecutionStatusRunning: temporal.NewNonRetryableApplicationError("running update failed", "StatusUpdateFailed", nil)},
			wantStatuses:  []WfExecutionStatus{WfExecutionStatusRunning, WfExecutionStatusFailed},
			wantErrorText: "running update failed",
		},
		{
			name:          "running update retry is bounded",
			statusErrors:  map[WfExecutionStatus]error{WfExecutionStatusRunning: stderrors.New("retryable running update failed")},
			wantStatuses:  []WfExecutionStatus{WfExecutionStatusRunning, WfExecutionStatusRunning, WfExecutionStatusRunning, WfExecutionStatusFailed},
			wantErrorText: "retryable running update failed",
		},
		{
			name:          "completed update failure",
			statusErrors:  map[WfExecutionStatus]error{WfExecutionStatusCompleted: temporal.NewNonRetryableApplicationError("completed update failed", "StatusUpdateFailed", nil)},
			wantStatuses:  []WfExecutionStatus{WfExecutionStatusRunning, WfExecutionStatusCompleted},
			wantErrorText: "completed update failed",
		},
		{
			name:          "failed update does not replace activity error",
			activityError: temporal.NewNonRetryableApplicationError("original activity error", "ActivityFailed", nil),
			statusErrors:  map[WfExecutionStatus]error{WfExecutionStatusFailed: temporal.NewNonRetryableApplicationError("failed update failed", "StatusUpdateFailed", nil)},
			wantStatuses:  []WfExecutionStatus{WfExecutionStatusRunning, WfExecutionStatusFailed},
			wantErrorText: "original activity error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			uc, repo := newTestWorkerUsecase()
			recorder := &statusRecorder{errors: tt.statusErrors}
			env := newTestWorkflowEnvironment(t, uc, repo, recorder)
			if tt.statusErrors[WfExecutionStatusRunning] == nil {
				env.OnActivity(string(NodeTypeSaySomething), mock.Anything, mock.Anything).
					Return(map[string]any{"result": "done"}, tt.activityError).Once()
			}

			_, err := executeDynamicWorkflow(t, env, uc, DefinitionGraph{
				Nodes: []DefNode{{ID: "activity", Type: NodeTypeSaySomething}, {ID: "output", Type: NodeTypeOutput}},
				Edges: []DefEdge{{FromNode: "activity", FromKey: "result", ToNode: "output", ToKey: "result"}},
			})
			if err == nil || !containsErrorText(err, tt.wantErrorText) {
				t.Fatalf("DynamicWorkflow() error = %v, want containing %q", err, tt.wantErrorText)
			}
			assertStatusTransitions(t, recorder.Inputs(), tt.wantStatuses...)
		})
	}
}

func testDynamicWorkflowCancellationUpdatesCanceledStatus(t *testing.T) {
	uc, repo := newTestWorkerUsecase()
	recorder := &statusRecorder{errors: make(map[WfExecutionStatus]error)}
	env := newTestWorkflowEnvironment(t, uc, repo, recorder)
	env.OnActivity(string(NodeTypeSaySomething), mock.Anything, mock.Anything).
		Return(func(context.Context, map[string]any) (map[string]any, error) {
			return nil, temporal.NewCanceledError()
		}).Once()

	_, err := executeDynamicWorkflow(t, env, uc, DefinitionGraph{
		Nodes: []DefNode{{ID: "activity", Type: NodeTypeSaySomething}, {ID: "output", Type: NodeTypeOutput}},
		Edges: []DefEdge{{FromNode: "activity", FromKey: "result", ToNode: "output", ToKey: "result"}},
	})
	if err == nil {
		t.Fatal("DynamicWorkflow() error = nil, want cancellation")
	}
	assertStatusTransitions(t, recorder.Inputs(), WfExecutionStatusRunning, WfExecutionStatusCanceled)
}

type nodeExecutionStatusRecorder struct {
	mu        sync.Mutex
	markedIDs []string
	finishes  []FinishNodeExecutionParam
	markErr   error
	finishErr error
}

func (r *nodeExecutionStatusRecorder) MarkNodeExecutionRunning(_ context.Context, nodeExecutionID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.markedIDs = append(r.markedIDs, nodeExecutionID)
	return r.markErr
}

func (r *nodeExecutionStatusRecorder) FinishNodeExecution(_ context.Context, param FinishNodeExecutionParam) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.finishes = append(r.finishes, param)
	return r.finishErr
}

func (r *nodeExecutionStatusRecorder) Inputs() ([]string, []FinishNodeExecutionParam) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.markedIDs...), append([]FinishNodeExecutionParam(nil), r.finishes...)
}

func newStandAloneWorkflowEnvironment(t *testing.T, repo *fakeWorkerNodeRepo, recorder *nodeExecutionStatusRecorder) *testsuite.TestWorkflowEnvironment {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	for nodeType, node := range repo.nodes {
		env.RegisterActivityWithOptions(node.Activity, activity.RegisterOptions{Name: string(nodeType)})
	}
	env.RegisterActivityWithOptions(recorder.MarkNodeExecutionRunning, activity.RegisterOptions{
		Name: MarkNodeExecutionRunningActivityType,
	})
	env.RegisterActivityWithOptions(recorder.FinishNodeExecution, activity.RegisterOptions{
		Name: FinishNodeExecutionActivityType,
	})
	t.Cleanup(func() {
		env.AssertExpectations(t)
	})
	return env
}

func executeStandAloneNodeWorkflow(t *testing.T, env *testsuite.TestWorkflowEnvironment, uc *WorkerUsecase, param ActivityNodeWorkflowParam) (map[string]any, error) {
	t.Helper()
	env.ExecuteWorkflow(uc.StandAloneNodeWorkflow, param)
	if !env.IsWorkflowCompleted() {
		t.Fatal("StandAloneNodeWorkflow did not complete")
	}
	if err := env.GetWorkflowError(); err != nil {
		return nil, err
	}
	var result map[string]any
	if err := env.GetWorkflowResult(&result); err != nil {
		t.Fatalf("GetWorkflowResult() error = %v", err)
	}
	return result, nil
}

func TestStandAloneNodeWorkflowSuccess(t *testing.T) {
	uc, repo := newTestWorkerUsecase()
	recorder := &nodeExecutionStatusRecorder{}
	env := newStandAloneWorkflowEnvironment(t, repo, recorder)
	input := map[string]any{"text": "hello"}
	want := map[string]any{"audio": "voice.mp3"}
	env.OnActivity(string(NodeType("ACTIVITY_C")), mock.Anything, input).Return(want, nil).Once()

	got, err := executeStandAloneNodeWorkflow(t, env, uc, ActivityNodeWorkflowParam{
		NodeExecutionID: "node-execution-1",
		NodeType:        NodeType("ACTIVITY_C"),
		Input:           input,
	})
	if err != nil {
		t.Fatalf("StandAloneNodeWorkflow() error = %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("StandAloneNodeWorkflow() result = %#v, want %#v", got, want)
	}

	markedIDs, finishes := recorder.Inputs()
	if !reflect.DeepEqual(markedIDs, []string{"node-execution-1"}) {
		t.Fatalf("marked node execution IDs = %v", markedIDs)
	}
	finish := singleNodeExecutionFinish(t, finishes, NodeExecutionStatusCompleted)
	if !reflect.DeepEqual(finish.Output, want) || finish.InternalErrorMessage != nil {
		t.Fatalf("FinishNodeExecution() input = %#v, want completed output", finish)
	}
}

func TestStandAloneNodeWorkflowNodeFailure(t *testing.T) {
	uc, repo := newTestWorkerUsecase()
	recorder := &nodeExecutionStatusRecorder{}
	env := newStandAloneWorkflowEnvironment(t, repo, recorder)
	nodeErr := temporal.NewNonRetryableApplicationError("node failed", "NodeFailed", nil)
	env.OnActivity(string(NodeType("ACTIVITY_C")), mock.Anything, mock.Anything).Return(nil, nodeErr).Once()

	_, err := executeStandAloneNodeWorkflow(t, env, uc, ActivityNodeWorkflowParam{
		NodeExecutionID: "node-execution-1",
		NodeType:        NodeType("ACTIVITY_C"),
	})
	if err == nil || !containsErrorText(err, "node failed") {
		t.Fatalf("StandAloneNodeWorkflow() error = %v, want node failure", err)
	}

	_, finishes := recorder.Inputs()
	finish := singleNodeExecutionFinish(t, finishes, NodeExecutionStatusFailed)
	if finish.Output != nil || finish.InternalErrorMessage == nil || !strings.Contains(*finish.InternalErrorMessage, "node failed") {
		t.Fatalf("FinishNodeExecution() input = %#v, want failed error", finish)
	}
}

func TestStandAloneNodeWorkflowActivityRetryPolicy(t *testing.T) {
	uc, repo := newTestWorkerUsecase()
	repo.nodes[NodeType("ACTIVITY_C")].(*fakeActivityNode).retryPolicy = temporal.RetryPolicy{
		InitialInterval: time.Millisecond,
		MaximumAttempts: 2,
	}
	recorder := &nodeExecutionStatusRecorder{}
	env := newStandAloneWorkflowEnvironment(t, repo, recorder)
	retryableErr := temporal.NewApplicationError("retryable node failure", "RetryableNodeFailure")
	env.OnActivity(string(NodeType("ACTIVITY_C")), mock.Anything, mock.Anything).
		Return(nil, retryableErr).Twice()

	_, err := executeStandAloneNodeWorkflow(t, env, uc, ActivityNodeWorkflowParam{
		NodeExecutionID: "node-execution-1",
		NodeType:        NodeType("ACTIVITY_C"),
	})
	if err == nil || !containsErrorText(err, "retryable node failure") {
		t.Fatalf("StandAloneNodeWorkflow() error = %v, want exhausted retry error", err)
	}

	_, finishes := recorder.Inputs()
	finish := singleNodeExecutionFinish(t, finishes, NodeExecutionStatusFailed)
	if finish.InternalErrorMessage == nil || !strings.Contains(*finish.InternalErrorMessage, "retryable node failure") {
		t.Fatalf("FinishNodeExecution() input = %#v, want exhausted retry error", finish)
	}
}

func TestStandAloneNodeWorkflowMarkRunningFailureFinishesExecution(t *testing.T) {
	uc, repo := newTestWorkerUsecase()
	markErr := temporal.NewNonRetryableApplicationError("mark running failed", "MarkRunningFailed", nil)
	recorder := &nodeExecutionStatusRecorder{markErr: markErr}
	env := newStandAloneWorkflowEnvironment(t, repo, recorder)

	_, err := executeStandAloneNodeWorkflow(t, env, uc, ActivityNodeWorkflowParam{
		NodeExecutionID: "node-execution-1",
		NodeType:        NodeType("ACTIVITY_C"),
	})
	if err == nil || !containsErrorText(err, "mark running failed") {
		t.Fatalf("StandAloneNodeWorkflow() error = %v, want mark-running failure", err)
	}

	_, finishes := recorder.Inputs()
	finish := singleNodeExecutionFinish(t, finishes, NodeExecutionStatusFailed)
	if finish.InternalErrorMessage == nil || !strings.Contains(*finish.InternalErrorMessage, "mark running failed") {
		t.Fatalf("FinishNodeExecution() input = %#v, want mark-running failure", finish)
	}
}

func TestStandAloneNodeWorkflowStatusRetryIsBounded(t *testing.T) {
	uc, repo := newTestWorkerUsecase()
	markErr := stderrors.New("retryable mark running failed")
	recorder := &nodeExecutionStatusRecorder{markErr: markErr}
	env := newStandAloneWorkflowEnvironment(t, repo, recorder)

	_, err := executeStandAloneNodeWorkflow(t, env, uc, ActivityNodeWorkflowParam{
		NodeExecutionID: "node-execution-1",
		NodeType:        NodeType("ACTIVITY_C"),
	})
	if err == nil || !containsErrorText(err, "retryable mark running failed") {
		t.Fatalf("StandAloneNodeWorkflow() error = %v, want exhausted status retry error", err)
	}

	markedIDs, finishes := recorder.Inputs()
	if want := []string{"node-execution-1", "node-execution-1", "node-execution-1"}; !reflect.DeepEqual(markedIDs, want) {
		t.Fatalf("MarkNodeExecutionRunning() IDs = %v, want %v", markedIDs, want)
	}
	finish := singleNodeExecutionFinish(t, finishes, NodeExecutionStatusFailed)
	if finish.InternalErrorMessage == nil || !strings.Contains(*finish.InternalErrorMessage, "retryable mark running failed") {
		t.Fatalf("FinishNodeExecution() input = %#v, want exhausted status retry error", finish)
	}
}

func TestStandAloneNodeWorkflowCancellationFinishesExecution(t *testing.T) {
	uc, repo := newTestWorkerUsecase()
	recorder := &nodeExecutionStatusRecorder{}
	env := newStandAloneWorkflowEnvironment(t, repo, recorder)
	env.OnActivity(string(NodeType("ACTIVITY_C")), mock.Anything, mock.Anything).
		After(time.Hour).
		Return(map[string]any{"result": "late"}, nil).
		Once()
	env.RegisterDelayedCallback(env.CancelWorkflow, time.Minute)

	_, err := executeStandAloneNodeWorkflow(t, env, uc, ActivityNodeWorkflowParam{
		NodeExecutionID: "node-execution-1",
		NodeType:        NodeType("ACTIVITY_C"),
	})
	if err == nil || !temporal.IsCanceledError(err) {
		t.Fatalf("StandAloneNodeWorkflow() error = %v, want cancellation", err)
	}
	_, finishes := recorder.Inputs()
	finish := singleNodeExecutionFinish(t, finishes, NodeExecutionStatusFailed)
	if finish.InternalErrorMessage == nil || !strings.Contains(strings.ToLower(*finish.InternalErrorMessage), "cancel") {
		t.Fatalf("FinishNodeExecution() input = %#v, want cancellation error", finish)
	}
}

func singleNodeExecutionFinish(t *testing.T, finishes []FinishNodeExecutionParam, wantStatus NodeExecutionStatus) FinishNodeExecutionParam {
	t.Helper()
	if len(finishes) != 1 {
		t.Fatalf("FinishNodeExecution() calls = %d, want 1", len(finishes))
	}
	if finishes[0].NodeExecutionID != "node-execution-1" || finishes[0].Status != wantStatus {
		t.Fatalf("FinishNodeExecution() input = %#v, want status %q", finishes[0], wantStatus)
	}
	return finishes[0]
}

func TestWorkerUsecaseDelegatesToRepositories(t *testing.T) {
	wfMarkErr := stderrors.New("mark workflow running failed")
	wfFinishErr := stderrors.New("finish workflow failed")
	wfExecutionRepo := &fakeWfExecutionStatusRepo{markErr: wfMarkErr, finishErr: wfFinishErr}
	nodeMarkErr := stderrors.New("mark node running failed")
	nodeFinishErr := stderrors.New("finish node failed")
	nodeExecutionRepo := &nodeExecutionStatusRecorder{markErr: nodeMarkErr, finishErr: nodeFinishErr}
	_, nodeRepo := newTestWorkerUsecase()
	uc := NewWorkerUsecase(wfExecutionRepo, nodeExecutionRepo, nodeRepo)

	if got := uc.GetActivityNodes(); len(got) != len(nodeRepo.nodes) {
		t.Fatalf("GetActivityNodes() len = %d, want %d", len(got), len(nodeRepo.nodes))
	}
	if err := uc.UpdateWfExecutionStatus(context.Background(), UpdateWfExecutionStatusParam{
		WorkflowExecutionID: "workflow-execution-1",
		Status:              WfExecutionStatusRunning,
	}); !stderrors.Is(err, wfMarkErr) {
		t.Fatalf("UpdateWfExecutionStatus(running) error = %v, want %v", err, wfMarkErr)
	}
	if !reflect.DeepEqual(wfExecutionRepo.markedIDs, []string{"workflow-execution-1"}) {
		t.Fatalf("MarkWfExecutionRunning() IDs = %v, want [workflow-execution-1]", wfExecutionRepo.markedIDs)
	}

	errorMessage := "workflow failed"
	param := UpdateWfExecutionStatusParam{
		WorkflowExecutionID: "workflow-execution-1",
		Status:              WfExecutionStatusFailed,
		ErrorInfo:           &WfExecutionErrorInfo{InternalErrorMessage: &errorMessage},
	}
	if err := uc.UpdateWfExecutionStatus(context.Background(), param); !stderrors.Is(err, wfFinishErr) {
		t.Fatalf("UpdateWfExecutionStatus(failed) error = %v, want %v", err, wfFinishErr)
	}
	wantFinish := []FinishWfExecutionParam{{
		WorkflowExecutionID: param.WorkflowExecutionID,
		Status:              param.Status,
		ErrorInfo:           param.ErrorInfo,
	}}
	if !reflect.DeepEqual(wfExecutionRepo.finishes, wantFinish) {
		t.Fatalf("FinishWfExecution() params = %#v, want %#v", wfExecutionRepo.finishes, wantFinish)
	}

	if err := uc.UpdateWfExecutionStatus(context.Background(), UpdateWfExecutionStatusParam{
		WorkflowExecutionID: "workflow-execution-1",
		Status:              WfExecutionStatusPending,
	}); !stderrors.Is(err, ErrWfExecutionInvalidArgument) {
		t.Fatalf("UpdateWfExecutionStatus(pending) error = %v, want %v", err, ErrWfExecutionInvalidArgument)
	}

	if err := uc.MarkNodeExecutionRunning(context.Background(), "node-execution-1"); !stderrors.Is(err, nodeMarkErr) {
		t.Fatalf("MarkNodeExecutionRunning() error = %v, want %v", err, nodeMarkErr)
	}
	markedIDs, _ := nodeExecutionRepo.Inputs()
	if !reflect.DeepEqual(markedIDs, []string{"node-execution-1"}) {
		t.Fatalf("MarkNodeExecutionRunning() IDs = %v, want [node-execution-1]", markedIDs)
	}

	nodeErrorMessage := "node failed"
	nodeParam := FinishNodeExecutionParam{
		NodeExecutionID:      "node-execution-1",
		Status:               NodeExecutionStatusFailed,
		InternalErrorMessage: &nodeErrorMessage,
	}
	if err := uc.FinishNodeExecution(context.Background(), nodeParam); !stderrors.Is(err, nodeFinishErr) {
		t.Fatalf("FinishNodeExecution(failed) error = %v, want %v", err, nodeFinishErr)
	}
	_, finishes := nodeExecutionRepo.Inputs()
	if !reflect.DeepEqual(finishes, []FinishNodeExecutionParam{nodeParam}) {
		t.Fatalf("FinishNodeExecution() params = %#v, want %#v", finishes, []FinishNodeExecutionParam{nodeParam})
	}

	if err := uc.FinishNodeExecution(context.Background(), FinishNodeExecutionParam{
		NodeExecutionID: "node-execution-1",
		Status:          NodeExecutionStatusRunning,
	}); !stderrors.Is(err, ErrToolInvalidArgument) {
		t.Fatalf("FinishNodeExecution(running) error = %v, want %v", err, ErrToolInvalidArgument)
	}
}

func branchesPreset(branches ...Branch) map[string]any {
	return map[string]any{presetBranchesKey: branches}
}

func assertStatusTransitions(t *testing.T, inputs []UpdateWfExecutionStatusParam, want ...WfExecutionStatus) {
	t.Helper()
	got := make([]WfExecutionStatus, len(inputs))
	for i := range inputs {
		got[i] = inputs[i].Status
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("status transitions = %v, want %v", got, want)
	}
}

func assertFailedNodeID(t *testing.T, inputs []UpdateWfExecutionStatusParam, want string) {
	t.Helper()
	if len(inputs) == 0 {
		t.Fatal("status updates are empty")
	}
	errorInfo := inputs[len(inputs)-1].ErrorInfo
	if errorInfo == nil || errorInfo.FailedNodeID == nil || *errorInfo.FailedNodeID != want {
		t.Fatalf("failed node id = %#v, want %q", errorInfo, want)
	}
}

func containsErrorText(err error, text string) bool {
	for err != nil {
		if strings.Contains(err.Error(), text) {
			return true
		}
		err = stderrors.Unwrap(err)
	}
	return false
}
