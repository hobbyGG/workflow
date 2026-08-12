package biz

import (
	"context"
	stderrors "errors"
	"testing"
)

type fakeExecutionRepo struct {
	found             *Execution
	findErr           error
	created           *Execution
	createResult      *Execution
	createErr         error
	startErr          error
	startedID         string
	startedDefinition DefinitionGraph
	findCalls         int
}

func (r *fakeExecutionRepo) FindByID(context.Context, string) (*Execution, error) {
	return r.found, r.findErr
}

func (r *fakeExecutionRepo) FindByIdempotencyKey(context.Context, int64, string) (*Execution, error) {
	r.findCalls++
	if r.findCalls > 1 && r.createResult != nil {
		return r.createResult, nil
	}
	return r.found, r.findErr
}

func (r *fakeExecutionRepo) ListExecutions(context.Context, ListExecutionsQuery) ([]*Execution, error) {
	return []*Execution{r.found}, r.findErr
}

func (r *fakeExecutionRepo) CreateExecution(_ context.Context, execution *Execution) (*Execution, error) {
	r.created = execution
	if r.createResult != nil {
		return r.createResult, r.createErr
	}
	return execution, r.createErr
}

func (r *fakeExecutionRepo) Start(_ context.Context, executionID string, param DynamicWorkflowParam) error {
	r.startedID = executionID
	r.startedDefinition = param.Definition
	return r.startErr
}

func TestExecutionUsecaseCreateExecution(t *testing.T) {
	tests := []struct {
		name string
		run  func(*testing.T)
	}{
		{name: "creates and starts a new execution", run: testExecutionUsecaseCreatesExecution},
		{name: "reuses existing execution", run: testExecutionUsecaseReusesExistingExecution},
		{name: "handles create conflict", run: testExecutionUsecaseHandlesCreateConflict},
		{name: "rejects workflow from another user", run: testExecutionUsecaseRejectsWorkflowFromAnotherUser},
		{name: "returns start error", run: testExecutionUsecaseStartError},
		{name: "validates input", run: testExecutionUsecaseValidation},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.run)
	}
}

func testExecutionUsecaseCreatesExecution(t *testing.T) {
	definition := DefinitionGraph{Nodes: []DefNode{{ID: "output", Type: NodeTypeOutput}}}
	executionRepo := &fakeExecutionRepo{findErr: ErrExecutionNotFound}
	workflowRepo := &fakeWorkflowRepo{workflow: &Workflow{ID: "workflow-1", UID: 42, Definition: definition}}
	uc := NewExecutionUsecase(executionRepo, workflowRepo)

	got, err := uc.CreateExecution(context.Background(), &CreateExecutionInput{
		UID:            42,
		IdempotencyKey: "request-1",
		WorkflowID:     "workflow-1",
		Input:          map[string]any{"prompt": "hello"},
	})
	if err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}
	if got.ID == "" || got.Status != ExecutionStatusPending {
		t.Fatalf("CreateExecution() = %#v, want generated pending execution", got)
	}
	if executionRepo.created != got {
		t.Fatal("CreateExecution() did not persist the execution")
	}
	if executionRepo.startedID != got.ID {
		t.Fatalf("Start() execution ID = %q, want %q", executionRepo.startedID, got.ID)
	}
	if len(executionRepo.startedDefinition.Nodes) != 1 {
		t.Fatalf("Start() definition = %#v, want workflow definition", executionRepo.startedDefinition)
	}
}

func testExecutionUsecaseReusesExistingExecution(t *testing.T) {
	existing := &Execution{ID: "execution-1", UID: 42, Definition: DefinitionGraph{Nodes: []DefNode{{ID: "output", Type: NodeTypeOutput}}}}
	executionRepo := &fakeExecutionRepo{found: existing}
	uc := NewExecutionUsecase(executionRepo, &fakeWorkflowRepo{})

	got, err := uc.CreateExecution(context.Background(), &CreateExecutionInput{UID: 42, IdempotencyKey: "request-1", WorkflowID: "workflow-1"})
	if err != nil || got != existing {
		t.Fatalf("CreateExecution() = (%v, %v), want existing execution", got, err)
	}
	if executionRepo.created != nil {
		t.Fatal("CreateExecution() created a duplicate execution")
	}
	if executionRepo.startedID != existing.ID {
		t.Fatalf("Start() execution ID = %q, want %q", executionRepo.startedID, existing.ID)
	}
}

func testExecutionUsecaseHandlesCreateConflict(t *testing.T) {
	existing := &Execution{ID: "execution-existing", UID: 42, Definition: DefinitionGraph{Nodes: []DefNode{{ID: "output", Type: NodeTypeOutput}}}}
	executionRepo := &fakeExecutionRepo{
		findErr:      ErrExecutionNotFound,
		createResult: existing,
		createErr:    ErrExecutionAlreadyExists,
	}
	workflowRepo := &fakeWorkflowRepo{workflow: &Workflow{ID: "workflow-1", UID: 42, Definition: existing.Definition}}

	got, err := NewExecutionUsecase(executionRepo, workflowRepo).CreateExecution(context.Background(), &CreateExecutionInput{
		UID: 42, IdempotencyKey: "request-1", WorkflowID: "workflow-1",
	})
	if err != nil || got != existing {
		t.Fatalf("CreateExecution() = (%v, %v), want concurrent existing execution", got, err)
	}
	if executionRepo.findCalls != 2 {
		t.Fatalf("FindByIdempotencyKey() calls = %d, want 2", executionRepo.findCalls)
	}
	if executionRepo.startedID != existing.ID {
		t.Fatalf("Start() execution ID = %q, want %q", executionRepo.startedID, existing.ID)
	}
}

func testExecutionUsecaseRejectsWorkflowFromAnotherUser(t *testing.T) {
	executionRepo := &fakeExecutionRepo{findErr: ErrExecutionNotFound}
	workflowRepo := &fakeWorkflowRepo{workflow: &Workflow{ID: "workflow-1", UID: 7}}

	_, err := NewExecutionUsecase(executionRepo, workflowRepo).CreateExecution(context.Background(), &CreateExecutionInput{
		UID: 42, IdempotencyKey: "request-1", WorkflowID: "workflow-1",
	})
	if !stderrors.Is(err, ErrWorkflowNotFound) {
		t.Fatalf("CreateExecution() error = %v, want %v", err, ErrWorkflowNotFound)
	}
	if executionRepo.created != nil || executionRepo.startedID != "" {
		t.Fatal("CreateExecution() persisted or started an unauthorized workflow")
	}
}

func testExecutionUsecaseStartError(t *testing.T) {
	startErr := stderrors.New("temporal unavailable")
	existing := &Execution{ID: "execution-1", UID: 42}
	executionRepo := &fakeExecutionRepo{found: existing, startErr: startErr}

	_, err := NewExecutionUsecase(executionRepo, &fakeWorkflowRepo{}).CreateExecution(context.Background(), &CreateExecutionInput{
		UID: 42, IdempotencyKey: "request-1", WorkflowID: "workflow-1",
	})
	if !stderrors.Is(err, startErr) {
		t.Fatalf("CreateExecution() error = %v, want %v", err, startErr)
	}
}

func testExecutionUsecaseValidation(t *testing.T) {
	tests := []struct {
		name  string
		input *CreateExecutionInput
	}{
		{name: "nil"},
		{name: "missing uid", input: &CreateExecutionInput{IdempotencyKey: "key", WorkflowID: "workflow"}},
		{name: "missing idempotency key", input: &CreateExecutionInput{UID: 1, WorkflowID: "workflow"}},
		{name: "missing workflow id", input: &CreateExecutionInput{UID: 1, IdempotencyKey: "key"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := &fakeExecutionRepo{}
			_, err := NewExecutionUsecase(repo, &fakeWorkflowRepo{}).CreateExecution(context.Background(), tt.input)
			if !stderrors.Is(err, ErrExecutionInvalidArgument) {
				t.Fatalf("CreateExecution() error = %v, want %v", err, ErrExecutionInvalidArgument)
			}
			if repo.findCalls != 0 {
				t.Fatal("CreateExecution() called repo for invalid input")
			}
		})
	}
}
