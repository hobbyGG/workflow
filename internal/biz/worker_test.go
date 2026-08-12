package biz

import (
	"context"
	stderrors "errors"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
)

type fakeWorkerNodeRepo struct {
	nodes map[NodeType]ActivityNode
}

func (r *fakeWorkerNodeRepo) GetActivityNode(nodeType NodeType) ActivityNode {
	return r.nodes[nodeType]
}

type fakeExecutionStatusRepo struct {
	err error
}

func (r *fakeExecutionStatusRepo) UpdateExecutionStatus(context.Context, UpdateExecutionStatusInput) error {
	return r.err
}

type fakeActivityNode struct {
	nodeType NodeType
}

func (n *fakeActivityNode) Type() NodeType {
	return n.nodeType
}

func (n *fakeActivityNode) Schema() NodeSchema {
	return NodeSchema{Type: n.nodeType}
}

func (n *fakeActivityNode) Activity(_ context.Context, input map[string]any) (map[string]any, error) {
	return input, nil
}

type statusRecorder struct {
	mu     sync.Mutex
	inputs []UpdateExecutionStatusInput
	errors map[ExecutionStatus]error
}

func (r *statusRecorder) Activity(_ context.Context, input UpdateExecutionStatusInput) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.inputs = append(r.inputs, input)
	return r.errors[input.Status]
}

func (r *statusRecorder) Inputs() []UpdateExecutionStatusInput {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]UpdateExecutionStatusInput(nil), r.inputs...)
}

func newTestWorkerUsecase() (*WorkerUsecase, *fakeWorkerNodeRepo) {
	repo := &fakeWorkerNodeRepo{nodes: make(map[NodeType]ActivityNode)}
	for _, nodeType := range activityNodeTypes {
		repo.nodes[nodeType] = &fakeActivityNode{nodeType: nodeType}
	}
	return NewWorkerUsecase(&fakeExecutionStatusRepo{}, repo), repo
}

func newTestWorkflowEnvironment(t *testing.T, uc *WorkerUsecase, repo *fakeWorkerNodeRepo, recorder *statusRecorder) *testsuite.TestWorkflowEnvironment {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: "execution-test"})
	for nodeType, node := range repo.nodes {
		env.RegisterActivityWithOptions(node.Activity, activity.RegisterOptions{Name: string(nodeType)})
	}
	env.RegisterActivityWithOptions(recorder.Activity, activity.RegisterOptions{Name: UpdateExecutionStatusActivityType})
	env.OnActivity(UpdateExecutionStatusActivityType, mock.Anything, mock.Anything).Return(recorder.Activity)
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
	recorder := &statusRecorder{errors: make(map[ExecutionStatus]error)}
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
	assertStatusTransitions(t, recorder.Inputs(), ExecutionStatusRunning, ExecutionStatusCompleted)
}

func testDynamicWorkflowParallelFanOutAndJoin(t *testing.T) {
	uc, repo := newTestWorkerUsecase()
	recorder := &statusRecorder{errors: make(map[ExecutionStatus]error)}
	env := newTestWorkflowEnvironment(t, uc, repo, recorder)

	env.OnActivity(string(NodeTypeSaySomething), mock.Anything, mock.Anything).
		Return(map[string]any{"seed": "alpha"}, nil).Once()
	env.OnActivity(string(NodeTypeSaySomething), mock.Anything, map[string]any{"input": "alpha"}).
		Return(map[string]any{"left": "image"}, nil).Once()
	env.OnActivity(string(NodeTypeExample), mock.Anything, map[string]any{"input": "alpha"}).
		Return(map[string]any{"right": "video"}, nil).Once()
	env.OnActivity(string(NodeTypeSaySomething), mock.Anything, map[string]any{"left": "image", "right": "video"}).
		Return(map[string]any{"joined": "image+video"}, nil).Once()

	result, err := executeDynamicWorkflow(t, env, uc, DefinitionGraph{
		Nodes: []DefNode{
			{ID: "source", Type: NodeTypeSaySomething},
			{ID: "left", Type: NodeTypeSaySomething},
			{ID: "right", Type: NodeTypeExample},
			{ID: "join", Type: NodeTypeSaySomething},
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
	recorder := &statusRecorder{errors: make(map[ExecutionStatus]error)}
	env := newTestWorkflowEnvironment(t, uc, repo, recorder)

	env.OnActivity(string(NodeTypeSaySomething), mock.Anything, mock.Anything).
		Return(map[string]any{"ignored": "a"}, nil).Once()
	env.OnActivity(string(NodeTypeExample), mock.Anything, mock.Anything).
		Return(map[string]any{"ignored": "b"}, nil).Once()
	env.OnActivity(string(NodeTypeSaySomething), mock.Anything, map[string]any{"preset": "ready"}).
		Return(map[string]any{"result": "done"}, nil).Once()

	result, err := executeDynamicWorkflow(t, env, uc, DefinitionGraph{
		Nodes: []DefNode{
			{ID: "first", Type: NodeTypeSaySomething},
			{ID: "second", Type: NodeTypeExample},
			{ID: "join", Type: NodeTypeSaySomething, Preset: map[string]any{"preset": "ready"}},
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
	recorder := &statusRecorder{errors: make(map[ExecutionStatus]error)}
	env := newTestWorkflowEnvironment(t, uc, repo, recorder)

	env.OnActivity(string(NodeTypeSaySomething), mock.Anything, mock.Anything).
		Return(map[string]any{"score": 90}, nil).Once()
	env.OnActivity(string(NodeTypeSaySomething), mock.Anything, map[string]any{"score": float64(90)}).
		Return(map[string]any{"choice": "first"}, nil).Once()

	result, err := executeDynamicWorkflow(t, env, uc, DefinitionGraph{
		Nodes: []DefNode{
			{ID: "source", Type: NodeTypeSaySomething},
			{ID: "router", Type: NodeTypeExclusive, Preset: branchesPreset(
				Branch{ID: "first", Expression: "inputs.score >= 80"},
				Branch{ID: "second", Expression: "inputs.score >= 60"},
			)},
			{ID: "first", Type: NodeTypeSaySomething},
			{ID: "second", Type: NodeTypeExample},
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
	recorder := &statusRecorder{errors: make(map[ExecutionStatus]error)}
	env := newTestWorkflowEnvironment(t, uc, repo, recorder)

	env.OnActivity(string(NodeTypeSaySomething), mock.Anything, mock.Anything).
		Return(map[string]any{"left": true, "right": true}, nil).Once()
	env.OnActivity(string(NodeTypeSaySomething), mock.Anything, map[string]any{"selected": true}).
		Return(map[string]any{"left": "L"}, nil).Once()
	env.OnActivity(string(NodeTypeExample), mock.Anything, map[string]any{"selected": true}).
		Return(map[string]any{"right": "R"}, nil).Once()

	result, err := executeDynamicWorkflow(t, env, uc, DefinitionGraph{
		Nodes: []DefNode{
			{ID: "source", Type: NodeTypeSaySomething},
			{ID: "router", Type: NodeTypeInclusive, Preset: branchesPreset(
				Branch{ID: "left", Expression: "inputs.left"},
				Branch{ID: "right", Expression: "inputs.right"},
			)},
			{ID: "left", Type: NodeTypeSaySomething},
			{ID: "right", Type: NodeTypeExample},
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
	recorder := &statusRecorder{errors: make(map[ExecutionStatus]error)}
	env := newTestWorkflowEnvironment(t, uc, repo, recorder)

	env.OnActivity(string(NodeTypeSaySomething), mock.Anything, mock.Anything).
		Return(map[string]any{"selected": false}, nil).Once()

	result, err := executeDynamicWorkflow(t, env, uc, DefinitionGraph{
		Nodes: []DefNode{
			{ID: "source", Type: NodeTypeSaySomething},
			{ID: "router", Type: NodeTypeExclusive, Preset: branchesPreset(Branch{ID: "yes", Expression: "inputs.selected"})},
			{ID: "activity", Type: NodeTypeSaySomething},
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
	recorder := &statusRecorder{errors: make(map[ExecutionStatus]error)}
	env := newTestWorkflowEnvironment(t, uc, repo, recorder)

	env.OnActivity(string(NodeTypeSaySomething), mock.Anything, mock.Anything).
		Return(map[string]any{"use_direct": true}, nil).Once()
	env.OnActivity(string(NodeTypeSaySomething), mock.Anything, map[string]any{"selected": true}).
		Return(map[string]any{"result": "direct"}, nil).Once()

	result, err := executeDynamicWorkflow(t, env, uc, DefinitionGraph{
		Nodes: []DefNode{
			{ID: "source", Type: NodeTypeSaySomething},
			{ID: "outer", Type: NodeTypeExclusive, Preset: branchesPreset(
				Branch{ID: "direct", Expression: "inputs.use_direct"},
				Branch{ID: "nested", Expression: "!inputs.use_direct"},
			)},
			{ID: "direct", Type: NodeTypeSaySomething},
			{ID: "nested", Type: NodeTypeExclusive, Preset: branchesPreset(Branch{ID: "child", Expression: "true"})},
			{ID: "child", Type: NodeTypeExample},
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
			recorder := &statusRecorder{errors: make(map[ExecutionStatus]error)}
			env := newTestWorkflowEnvironment(t, uc, repo, recorder)
			env.OnActivity(string(tt.mockNode), mock.Anything, mock.Anything).Return(tt.output, tt.activityErr).Once()

			_, err := executeDynamicWorkflow(t, env, uc, tt.definition)
			if err == nil {
				t.Fatal("DynamicWorkflow() error = nil, want error")
			}
			assertStatusTransitions(t, recorder.Inputs(), ExecutionStatusRunning, ExecutionStatusFailed)
			if tt.wantFailedNodeID != "" {
				assertFailedNodeID(t, recorder.Inputs(), tt.wantFailedNodeID)
			}
		})
	}
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
			recorder := &statusRecorder{errors: make(map[ExecutionStatus]error)}
			env := newTestWorkflowEnvironment(t, uc, repo, recorder)
			env.OnActivity(string(NodeTypeSaySomething), mock.Anything, mock.Anything).Return(tt.sourceData, nil).Once()

			_, err := executeDynamicWorkflow(t, env, uc, DefinitionGraph{
				Nodes: []DefNode{
					{ID: "source", Type: NodeTypeSaySomething},
					{ID: "router", Type: NodeTypeExclusive, Preset: tt.preset},
					{ID: "selected", Type: NodeTypeSaySomething},
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
			recorder := &statusRecorder{errors: make(map[ExecutionStatus]error)}
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
		statusErrors  map[ExecutionStatus]error
		cancel        bool
		wantStatuses  []ExecutionStatus
		wantErrorText string
	}{
		{
			name:          "running update failure",
			statusErrors:  map[ExecutionStatus]error{ExecutionStatusRunning: temporal.NewNonRetryableApplicationError("running update failed", "StatusUpdateFailed", nil)},
			wantStatuses:  []ExecutionStatus{ExecutionStatusRunning, ExecutionStatusFailed},
			wantErrorText: "running update failed",
		},
		{
			name:          "completed update failure",
			statusErrors:  map[ExecutionStatus]error{ExecutionStatusCompleted: temporal.NewNonRetryableApplicationError("completed update failed", "StatusUpdateFailed", nil)},
			wantStatuses:  []ExecutionStatus{ExecutionStatusRunning, ExecutionStatusCompleted},
			wantErrorText: "completed update failed",
		},
		{
			name:          "failed update does not replace activity error",
			activityError: temporal.NewNonRetryableApplicationError("original activity error", "ActivityFailed", nil),
			statusErrors:  map[ExecutionStatus]error{ExecutionStatusFailed: temporal.NewNonRetryableApplicationError("failed update failed", "StatusUpdateFailed", nil)},
			wantStatuses:  []ExecutionStatus{ExecutionStatusRunning, ExecutionStatusFailed},
			wantErrorText: "original activity error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			uc, repo := newTestWorkerUsecase()
			recorder := &statusRecorder{errors: tt.statusErrors}
			env := newTestWorkflowEnvironment(t, uc, repo, recorder)
			if tt.statusErrors[ExecutionStatusRunning] == nil {
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
	recorder := &statusRecorder{errors: make(map[ExecutionStatus]error)}
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
	assertStatusTransitions(t, recorder.Inputs(), ExecutionStatusRunning, ExecutionStatusCanceled)
}

func TestWorkerUsecaseDelegatesToRepositories(t *testing.T) {
	repoErr := stderrors.New("update failed")
	executionRepo := &fakeExecutionStatusRepo{err: repoErr}
	_, nodeRepo := newTestWorkerUsecase()
	uc := NewWorkerUsecase(executionRepo, nodeRepo)

	if got := uc.GetActivityNodes(); len(got) != len(activityNodeTypes) {
		t.Fatalf("GetActivityNodes() len = %d, want %d", len(got), len(activityNodeTypes))
	}
	if err := uc.UpdateExecutionStatus(context.Background(), UpdateExecutionStatusInput{}); !stderrors.Is(err, repoErr) {
		t.Fatalf("UpdateExecutionStatus() error = %v, want %v", err, repoErr)
	}
}

func branchesPreset(branches ...Branch) map[string]any {
	return map[string]any{presetBranchesKey: branches}
}

func assertStatusTransitions(t *testing.T, inputs []UpdateExecutionStatusInput, want ...ExecutionStatus) {
	t.Helper()
	got := make([]ExecutionStatus, len(inputs))
	for i := range inputs {
		got[i] = inputs[i].Status
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("status transitions = %v, want %v", got, want)
	}
}

func assertFailedNodeID(t *testing.T, inputs []UpdateExecutionStatusInput, want string) {
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
