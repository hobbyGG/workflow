package biz

import (
	"context"
	"errors"
	"testing"

	"go.temporal.io/sdk/temporal"
)

type fakeToolNode struct {
	nodeType NodeType
}

func (n *fakeToolNode) Type() NodeType {
	return n.nodeType
}

func (n *fakeToolNode) Schema() NodeSchema {
	return objectNodeSchema(n.nodeType)
}

func (*fakeToolNode) RetryPolicy() temporal.RetryPolicy {
	return temporal.RetryPolicy{}
}

func (*fakeToolNode) Activity(context.Context, map[string]any) (map[string]any, error) {
	return nil, nil
}

type fakeToolNodeRepo struct {
	nodes []ActivityNode
}

func (r *fakeToolNodeRepo) GetActivityNodes() []ActivityNode {
	return r.nodes
}

type fakeToolNodeExecutionRepo struct {
	infos           map[NodeType]NodeExecutionInfo
	execution       *NodeExecution
	created         *NodeExecution
	createResult    *NodeExecution
	requestedID     string
	dispatchedID    string
	dispatchedParam ActivityNodeWorkflowParam
	infoErr         error
	getErr          error
	createErr       error
	dispatchErr     error
	createCalls     int
	dispatchCalls   int
}

func (r *fakeToolNodeExecutionRepo) GetNodeExecutionInfos(context.Context) (map[NodeType]NodeExecutionInfo, error) {
	return r.infos, r.infoErr
}

func (r *fakeToolNodeExecutionRepo) GetNodeExecution(_ context.Context, nodeExecutionID string) (*NodeExecution, error) {
	r.requestedID = nodeExecutionID
	return r.execution, r.getErr
}

func (r *fakeToolNodeExecutionRepo) CreateNodeExecution(_ context.Context, execution *NodeExecution) (*NodeExecution, error) {
	r.createCalls++
	r.created = execution
	if r.createResult != nil {
		return r.createResult, r.createErr
	}
	return execution, r.createErr
}

func (r *fakeToolNodeExecutionRepo) DispatchNodeExecution(_ context.Context, nodeExecutionID string, param ActivityNodeWorkflowParam) error {
	r.dispatchCalls++
	r.dispatchedID = nodeExecutionID
	r.dispatchedParam = param
	return r.dispatchErr
}

func TestToolUsecaseListTools(t *testing.T) {
	profile := NodeExecutionInfo{AvgDurationsMS: 1250.5, SuccessRatio: 0.75}
	uc := NewToolUsecase(
		&fakeToolNodeRepo{nodes: []ActivityNode{
			&fakeToolNode{nodeType: NodeType("ACTIVITY_C")},
			&fakeToolNode{nodeType: NodeType("ACTIVITY_A")},
		}},
		&fakeToolNodeExecutionRepo{infos: map[NodeType]NodeExecutionInfo{
			NodeType("ACTIVITY_C"): profile,
		}},
	)

	tools, err := uc.ListTools(context.Background())
	if err != nil {
		t.Fatalf("ListTools() error = %v", err)
	}
	if len(tools) != 2 {
		t.Fatalf("ListTools() len = %d, want 2", len(tools))
	}
	if tools[0].Schema.Type != NodeType("ACTIVITY_C") || tools[0].Profile == nil || *tools[0].Profile != profile {
		t.Fatalf("ListTools()[0] = %#v, want ACTIVITY_C tool with profile %#v", tools[0], profile)
	}
	if tools[1].Schema.Type != NodeType("ACTIVITY_A") || tools[1].Profile != nil {
		t.Fatalf("ListTools()[1] = %#v, want image generation tool without profile", tools[1])
	}
}

func TestToolUsecaseListToolsReturnsRepoError(t *testing.T) {
	wantErr := errors.New("query failed")
	uc := NewToolUsecase(
		&fakeToolNodeRepo{},
		&fakeToolNodeExecutionRepo{infoErr: wantErr},
	)

	_, err := uc.ListTools(context.Background())
	if !errors.Is(err, wantErr) {
		t.Fatalf("ListTools() error = %v, want %v", err, wantErr)
	}
}

func TestToolUsecaseExecuteToolRejectsNilInput(t *testing.T) {
	repo := &fakeToolNodeExecutionRepo{}
	_, err := newTestToolUsecase(repo).ExecuteTool(context.Background(), nil)
	if !errors.Is(err, ErrToolInvalidArgument) {
		t.Fatalf("ExecuteTool() error = %v, want %v", err, ErrToolInvalidArgument)
	}
	if repo.createCalls != 0 || repo.dispatchCalls != 0 {
		t.Fatal("ExecuteTool() called repo for nil input")
	}
}

func TestToolUsecaseExecuteToolRejectsUnknownNodeType(t *testing.T) {
	repo := &fakeToolNodeExecutionRepo{}
	_, err := newTestToolUsecase(repo).ExecuteTool(context.Background(), &ExecuteToolParam{
		NodeType:       NodeType("UNKNOWN"),
		IdempotencyKey: "request-1",
	})
	if !errors.Is(err, ErrToolInvalidArgument) {
		t.Fatalf("ExecuteTool() error = %v, want %v", err, ErrToolInvalidArgument)
	}
	if repo.createCalls != 0 || repo.dispatchCalls != 0 {
		t.Fatal("ExecuteTool() persisted or dispatched an unknown node type")
	}
}

func TestToolUsecaseExecuteToolRejectsNonJSONInput(t *testing.T) {
	repo := &fakeToolNodeExecutionRepo{}
	_, err := newTestToolUsecase(repo).ExecuteTool(context.Background(), &ExecuteToolParam{
		NodeType:       NodeType("ACTIVITY_C"),
		Input:          map[string]any{"invalid": make(chan struct{})},
		IdempotencyKey: "request-1",
	})
	if !errors.Is(err, ErrToolInvalidArgument) {
		t.Fatalf("ExecuteTool() error = %v, want %v", err, ErrToolInvalidArgument)
	}
	if repo.createCalls != 0 || repo.dispatchCalls != 0 {
		t.Fatal("ExecuteTool() called repo for non-JSON input")
	}
}

func TestToolUsecaseExecuteToolCreatesAndDispatches(t *testing.T) {
	repo := &fakeToolNodeExecutionRepo{}
	uc := newTestToolUsecase(repo)

	got, err := uc.ExecuteTool(context.Background(), &ExecuteToolParam{
		NodeType:       NodeType("ACTIVITY_C"),
		Input:          map[string]any{"text": "hello"},
		IdempotencyKey: "request-1",
	})
	if err != nil {
		t.Fatalf("ExecuteTool() error = %v", err)
	}
	if got == nil || got.ID == "" || got.Status != NodeExecutionStatusPending {
		t.Fatalf("ExecuteTool() = %#v, want generated pending execution", got)
	}
	if got.WorkflowExecutionID != nil || got.NodeID != nil {
		t.Fatalf("ExecuteTool() parent IDs = (%v, %v), want standalone execution", got.WorkflowExecutionID, got.NodeID)
	}
	if got.NodeType != NodeType("ACTIVITY_C") || got.IdempotencyKey != "request-1" || got.Input["text"] != "hello" {
		t.Fatalf("ExecuteTool() = %#v, want request fields unchanged", got)
	}
	if repo.created != got || repo.createCalls != 1 {
		t.Fatal("ExecuteTool() did not persist the new execution")
	}
	assertNodeExecutionDispatched(t, repo, got)
}

func TestToolUsecaseExecuteToolUsesCanonicalCreateResult(t *testing.T) {
	canonical := toolExecution("execution-existing", NodeExecutionStatusPending, map[string]any{})
	repo := &fakeToolNodeExecutionRepo{
		createResult: canonical,
	}

	got, err := newTestToolUsecase(repo).ExecuteTool(context.Background(), &ExecuteToolParam{
		NodeType:       NodeType("ACTIVITY_C"),
		IdempotencyKey: "request-1",
	})
	if err != nil || got != canonical {
		t.Fatalf("ExecuteTool() = (%#v, %v), want canonical execution", got, err)
	}
	if repo.created == nil || repo.created.ID == canonical.ID {
		t.Fatal("CreateNodeExecution() did not receive a newly generated candidate")
	}
	assertNodeExecutionDispatched(t, repo, canonical)
}

func TestToolUsecaseExecuteToolAcceptsJSONEquivalentInput(t *testing.T) {
	canonical := toolExecution("execution-existing", NodeExecutionStatusPending, map[string]any{"count": float64(1)})
	repo := &fakeToolNodeExecutionRepo{createResult: canonical}

	got, err := newTestToolUsecase(repo).ExecuteTool(context.Background(), &ExecuteToolParam{
		NodeType:       NodeType("ACTIVITY_C"),
		Input:          map[string]any{"count": 1},
		IdempotencyKey: "request-1",
	})
	if err != nil || got != canonical {
		t.Fatalf("ExecuteTool() = (%#v, %v), want canonical execution", got, err)
	}
	assertNodeExecutionDispatched(t, repo, canonical)
}

func TestToolUsecaseExecuteToolRejectsIdempotencyConflict(t *testing.T) {
	tests := []struct {
		name      string
		canonical *NodeExecution
	}{
		{
			name: "different node type",
			canonical: &NodeExecution{
				ID:             "execution-existing",
				IdempotencyKey: "request-1",
				NodeType:       NodeType("ACTIVITY_A"),
				Input:          map[string]any{"text": "hello"},
				Status:         NodeExecutionStatusPending,
			},
		},
		{
			name:      "different input",
			canonical: toolExecution("execution-existing", NodeExecutionStatusPending, map[string]any{"text": "different"}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := &fakeToolNodeExecutionRepo{createResult: tt.canonical}
			_, err := newTestToolUsecase(repo).ExecuteTool(context.Background(), &ExecuteToolParam{
				NodeType:       NodeType("ACTIVITY_C"),
				Input:          map[string]any{"text": "hello"},
				IdempotencyKey: "request-1",
			})
			if !errors.Is(err, ErrToolExecutionConflict) {
				t.Fatalf("ExecuteTool() error = %v, want %v", err, ErrToolExecutionConflict)
			}
			if repo.dispatchCalls != 0 {
				t.Fatal("ExecuteTool() dispatched a conflicting execution")
			}
		})
	}
}

func TestToolUsecaseExecuteToolRejectsInvalidCanonicalInput(t *testing.T) {
	canonical := toolExecution("execution-existing", NodeExecutionStatusPending, map[string]any{"invalid": make(chan struct{})})
	repo := &fakeToolNodeExecutionRepo{createResult: canonical}

	_, err := newTestToolUsecase(repo).ExecuteTool(context.Background(), &ExecuteToolParam{
		NodeType:       NodeType("ACTIVITY_C"),
		IdempotencyKey: "request-1",
	})
	if !errors.Is(err, ErrToolInternal) {
		t.Fatalf("ExecuteTool() error = %v, want %v", err, ErrToolInternal)
	}
	if repo.dispatchCalls != 0 {
		t.Fatal("ExecuteTool() dispatched an execution with invalid canonical input")
	}
}

func TestToolUsecaseExecuteToolReturnsRepoErrors(t *testing.T) {
	wantErr := errors.New("repo unavailable")
	tests := []struct {
		name string
		repo *fakeToolNodeExecutionRepo
	}{
		{name: "create", repo: &fakeToolNodeExecutionRepo{createErr: wantErr}},
		{name: "dispatch", repo: &fakeToolNodeExecutionRepo{dispatchErr: wantErr}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := newTestToolUsecase(tt.repo).ExecuteTool(context.Background(), &ExecuteToolParam{
				NodeType:       NodeType("ACTIVITY_C"),
				IdempotencyKey: "request-1",
			})
			if !errors.Is(err, wantErr) {
				t.Fatalf("ExecuteTool() error = %v, want %v", err, wantErr)
			}
		})
	}
}

func newTestToolUsecase(repo NodeExecutionRepo) *ToolUsecase {
	return NewToolUsecase(
		&fakeToolNodeRepo{nodes: []ActivityNode{&fakeToolNode{nodeType: NodeType("ACTIVITY_C")}}},
		repo,
	)
}

func toolExecution(id string, status NodeExecutionStatus, input map[string]any) *NodeExecution {
	return &NodeExecution{
		ID:             id,
		IdempotencyKey: "request-1",
		NodeType:       NodeType("ACTIVITY_C"),
		Input:          input,
		Status:         status,
	}
}

func assertNodeExecutionDispatched(t *testing.T, repo *fakeToolNodeExecutionRepo, execution *NodeExecution) {
	t.Helper()
	if repo.dispatchCalls != 1 || repo.dispatchedID != execution.ID {
		t.Fatalf("DispatchNodeExecution() = (%d, %q), want (1, %q)", repo.dispatchCalls, repo.dispatchedID, execution.ID)
	}
	param := repo.dispatchedParam
	if param.NodeExecutionID != execution.ID || param.NodeType != execution.NodeType || param.Input["text"] != execution.Input["text"] {
		t.Fatalf("DispatchNodeExecution() param = %#v, want execution payload", param)
	}
}

func TestToolUsecaseGetToolExecution(t *testing.T) {
	execution := &NodeExecution{ID: "node-execution-1"}
	repo := &fakeToolNodeExecutionRepo{execution: execution}
	uc := NewToolUsecase(&fakeToolNodeRepo{}, repo)

	got, err := uc.GetToolExecution(context.Background(), "  node-execution-1  ")
	if err != nil {
		t.Fatalf("GetToolExecution() error = %v", err)
	}
	if got != execution {
		t.Fatalf("GetToolExecution() = %#v, want %#v", got, execution)
	}
	if repo.requestedID != execution.ID {
		t.Fatalf("GetToolExecution() repo ID = %q, want %q", repo.requestedID, execution.ID)
	}
}

func TestToolUsecaseGetToolExecutionValidatesResult(t *testing.T) {
	uc := NewToolUsecase(&fakeToolNodeRepo{}, &fakeToolNodeExecutionRepo{})

	_, err := uc.GetToolExecution(context.Background(), "")
	if !errors.Is(err, ErrToolInvalidArgument) {
		t.Fatalf("GetToolExecution(\"\") error = %v, want %v", err, ErrToolInvalidArgument)
	}

	_, err = uc.GetToolExecution(context.Background(), "missing")
	if !errors.Is(err, ErrToolExecutionNotFound) {
		t.Fatalf("GetToolExecution(missing) error = %v, want %v", err, ErrToolExecutionNotFound)
	}
}
