package biz

import (
	"context"
	stderrors "errors"
	"testing"
)

type fakeWfExecutionRepo struct {
	found                *WfExecution
	findErr              error
	created              *WfExecution
	createResult         *WfExecution
	createErr            error
	dispatchErr          error
	dispatchedID         string
	dispatchedDefinition DefinitionGraph
	findCalls            int
}

func (r *fakeWfExecutionRepo) GetWfExecution(context.Context, string) (*WfExecution, error) {
	return r.found, r.findErr
}

func (r *fakeWfExecutionRepo) GetWfExecutionByIdempotencyKey(context.Context, int64, string) (*WfExecution, error) {
	r.findCalls++
	if r.findCalls > 1 && r.createResult != nil {
		return r.createResult, nil
	}
	return r.found, r.findErr
}

func (r *fakeWfExecutionRepo) ListWfExecutions(context.Context, ListWfExecutionsQuery) ([]*WfExecution, error) {
	return []*WfExecution{r.found}, r.findErr
}

func (r *fakeWfExecutionRepo) CreateWfExecution(_ context.Context, wfExecution *WfExecution) (*WfExecution, error) {
	r.created = wfExecution
	if r.createResult != nil {
		return r.createResult, r.createErr
	}
	return wfExecution, r.createErr
}

func (r *fakeWfExecutionRepo) DispatchWfExecution(_ context.Context, executionID string, param DynamicWorkflowParam) error {
	r.dispatchedID = executionID
	r.dispatchedDefinition = param.Definition
	return r.dispatchErr
}

func TestWfExecutionUsecaseCreateWfExecution(t *testing.T) {
	tests := []struct {
		name string
		run  func(*testing.T)
	}{
		{name: "creates and dispatches a new workflow execution", run: testWfExecutionUsecaseCreatesWfExecution},
		{name: "reuses existing workflow execution", run: testWfExecutionUsecaseReusesWfExecution},
		{name: "handles create conflict", run: testWfExecutionUsecaseHandlesCreateConflict},
		{name: "rejects definition from another user", run: testWfExecutionUsecaseRejectsDefinitionFromAnotherUser},
		{name: "returns dispatch error", run: testWfExecutionUsecaseDispatchError},
		{name: "validates input", run: testWfExecutionUsecaseValidation},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.run)
	}
}

func testWfExecutionUsecaseCreatesWfExecution(t *testing.T) {
	definition := DefinitionGraph{Nodes: []DefNode{{ID: "output", Type: NodeTypeOutput}}}
	wfExecutionRepo := &fakeWfExecutionRepo{findErr: ErrWfExecutionNotFound}
	definitionRepo := &fakeDefinitionRepo{definition: &Definition{ID: "definition-1", UID: 42, DefGraph: definition}}
	uc := NewWfExecutionUsecase(wfExecutionRepo, definitionRepo)

	got, err := uc.CreateWfExecution(context.Background(), &CreateWfExecutionParam{
		UID:            42,
		IdempotencyKey: "request-1",
		DefinitionID:   "definition-1",
		Input:          map[string]any{"prompt": "hello"},
	})
	if err != nil {
		t.Fatalf("CreateWfExecution() error = %v", err)
	}
	if got.ID == "" || got.Status != WfExecutionStatusPending {
		t.Fatalf("CreateWfExecution() = %#v, want generated pending workflow execution", got)
	}
	if wfExecutionRepo.created != got {
		t.Fatal("CreateWfExecution() did not persist the workflow execution")
	}
	if wfExecutionRepo.dispatchedID != got.ID {
		t.Fatalf("DispatchWfExecution() workflow execution ID = %q, want %q", wfExecutionRepo.dispatchedID, got.ID)
	}
	if len(wfExecutionRepo.dispatchedDefinition.Nodes) != 1 {
		t.Fatalf("DispatchWfExecution() definition = %#v, want definition graph", wfExecutionRepo.dispatchedDefinition)
	}
}

func testWfExecutionUsecaseReusesWfExecution(t *testing.T) {
	existing := &WfExecution{ID: "execution-1", UID: 42, Definition: DefinitionGraph{Nodes: []DefNode{{ID: "output", Type: NodeTypeOutput}}}}
	wfExecutionRepo := &fakeWfExecutionRepo{found: existing}
	uc := NewWfExecutionUsecase(wfExecutionRepo, &fakeDefinitionRepo{})

	got, err := uc.CreateWfExecution(context.Background(), &CreateWfExecutionParam{UID: 42, IdempotencyKey: "request-1", DefinitionID: "definition-1"})
	if err != nil || got != existing {
		t.Fatalf("CreateWfExecution() = (%v, %v), want existing workflow execution", got, err)
	}
	if wfExecutionRepo.created != nil {
		t.Fatal("CreateWfExecution() created a duplicate workflow execution")
	}
	if wfExecutionRepo.dispatchedID != existing.ID {
		t.Fatalf("DispatchWfExecution() workflow execution ID = %q, want %q", wfExecutionRepo.dispatchedID, existing.ID)
	}
}

func testWfExecutionUsecaseHandlesCreateConflict(t *testing.T) {
	existing := &WfExecution{ID: "execution-existing", UID: 42, Definition: DefinitionGraph{Nodes: []DefNode{{ID: "output", Type: NodeTypeOutput}}}}
	wfExecutionRepo := &fakeWfExecutionRepo{
		findErr:      ErrWfExecutionNotFound,
		createResult: existing,
		createErr:    ErrWfExecutionAlreadyExists,
	}
	definitionRepo := &fakeDefinitionRepo{definition: &Definition{ID: "definition-1", UID: 42, DefGraph: existing.Definition}}

	got, err := NewWfExecutionUsecase(wfExecutionRepo, definitionRepo).CreateWfExecution(context.Background(), &CreateWfExecutionParam{
		UID: 42, IdempotencyKey: "request-1", DefinitionID: "definition-1",
	})
	if err != nil || got != existing {
		t.Fatalf("CreateWfExecution() = (%v, %v), want concurrent existing workflow execution", got, err)
	}
	if wfExecutionRepo.findCalls != 2 {
		t.Fatalf("GetWfExecutionByIdempotencyKey() calls = %d, want 2", wfExecutionRepo.findCalls)
	}
	if wfExecutionRepo.dispatchedID != existing.ID {
		t.Fatalf("DispatchWfExecution() workflow execution ID = %q, want %q", wfExecutionRepo.dispatchedID, existing.ID)
	}
}

func testWfExecutionUsecaseRejectsDefinitionFromAnotherUser(t *testing.T) {
	wfExecutionRepo := &fakeWfExecutionRepo{findErr: ErrWfExecutionNotFound}
	definitionRepo := &fakeDefinitionRepo{definition: &Definition{ID: "definition-1", UID: 7}}

	_, err := NewWfExecutionUsecase(wfExecutionRepo, definitionRepo).CreateWfExecution(context.Background(), &CreateWfExecutionParam{
		UID: 42, IdempotencyKey: "request-1", DefinitionID: "definition-1",
	})
	if !stderrors.Is(err, ErrDefinitionNotFound) {
		t.Fatalf("CreateWfExecution() error = %v, want %v", err, ErrDefinitionNotFound)
	}
	if wfExecutionRepo.created != nil || wfExecutionRepo.dispatchedID != "" {
		t.Fatal("CreateWfExecution() persisted or dispatched an unauthorized definition")
	}
}

func testWfExecutionUsecaseDispatchError(t *testing.T) {
	dispatchErr := stderrors.New("temporal unavailable")
	existing := &WfExecution{ID: "execution-1", UID: 42}
	wfExecutionRepo := &fakeWfExecutionRepo{found: existing, dispatchErr: dispatchErr}

	_, err := NewWfExecutionUsecase(wfExecutionRepo, &fakeDefinitionRepo{}).CreateWfExecution(context.Background(), &CreateWfExecutionParam{
		UID: 42, IdempotencyKey: "request-1", DefinitionID: "definition-1",
	})
	if !stderrors.Is(err, dispatchErr) {
		t.Fatalf("CreateWfExecution() error = %v, want %v", err, dispatchErr)
	}
}

func testWfExecutionUsecaseValidation(t *testing.T) {
	tests := []struct {
		name  string
		param *CreateWfExecutionParam
	}{
		{name: "nil"},
		{name: "missing uid", param: &CreateWfExecutionParam{IdempotencyKey: "key", DefinitionID: "definition"}},
		{name: "missing idempotency key", param: &CreateWfExecutionParam{UID: 1, DefinitionID: "definition"}},
		{name: "missing definition id", param: &CreateWfExecutionParam{UID: 1, IdempotencyKey: "key"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := &fakeWfExecutionRepo{}
			_, err := NewWfExecutionUsecase(repo, &fakeDefinitionRepo{}).CreateWfExecution(context.Background(), tt.param)
			if !stderrors.Is(err, ErrWfExecutionInvalidArgument) {
				t.Fatalf("CreateWfExecution() error = %v, want %v", err, ErrWfExecutionInvalidArgument)
			}
			if repo.findCalls != 0 {
				t.Fatal("CreateWfExecution() called repo for invalid input")
			}
		})
	}
}
