package biz

import (
	"context"
	stderrors "errors"
	"testing"
)

type fakeWorkflowRepo struct {
	workflow *Workflow
	list     []*Workflow
	err      error
	created  *Workflow
	query    ListWorkflowsQuery
}

func (r *fakeWorkflowRepo) FindByID(context.Context, string) (*Workflow, error) {
	return r.workflow, r.err
}

func (r *fakeWorkflowRepo) ListWorkflows(_ context.Context, query ListWorkflowsQuery) ([]*Workflow, error) {
	r.query = query
	return r.list, r.err
}

func (r *fakeWorkflowRepo) CreateWorkflow(_ context.Context, workflow *Workflow) (*Workflow, error) {
	r.created = workflow
	return workflow, r.err
}

func TestWorkflowUsecaseCreateWorkflow(t *testing.T) {
	tests := []struct {
		name string
		run  func(*testing.T)
	}{
		{name: "creates valid workflow", run: testWorkflowUsecaseCreatesWorkflow},
		{name: "validates workflow", run: testWorkflowUsecaseCreateWorkflowValidation},
		{name: "returns repo error", run: testWorkflowUsecaseCreateWorkflowRepoError},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.run)
	}
}

func testWorkflowUsecaseCreatesWorkflow(t *testing.T) {
	repo := &fakeWorkflowRepo{}
	uc := NewWorkflowUsecase(repo)
	workflow := &Workflow{
		UID:  42,
		Name: "flow",
		Type: "dynamic",
		Definition: DefinitionGraph{
			Nodes: []DefNode{{ID: "activity", Type: NodeTypeSaySomething}, {ID: "output", Type: NodeTypeOutput}},
			Edges: []DefEdge{{FromNode: "activity", FromKey: "result", ToNode: "output", ToKey: "result"}},
		},
	}

	got, err := uc.CreateWorkflow(context.Background(), workflow)
	if err != nil {
		t.Fatalf("CreateWorkflow() error = %v", err)
	}
	if got.ID == "" {
		t.Fatal("CreateWorkflow() did not generate an ID")
	}
	if repo.created != workflow {
		t.Fatal("CreateWorkflow() did not pass the workflow to repo")
	}
}

func testWorkflowUsecaseCreateWorkflowValidation(t *testing.T) {
	tests := []struct {
		name     string
		workflow *Workflow
	}{
		{name: "nil workflow"},
		{name: "missing uid", workflow: &Workflow{Name: "flow", Type: "dynamic"}},
		{name: "missing name", workflow: &Workflow{UID: 1, Type: "dynamic"}},
		{name: "missing type", workflow: &Workflow{UID: 1, Name: "flow"}},
		{name: "duplicate node id", workflow: validWorkflowWithDefinition(DefinitionGraph{Nodes: []DefNode{{ID: "same"}, {ID: "same"}}})},
		{name: "missing edge source", workflow: validWorkflowWithDefinition(DefinitionGraph{Nodes: []DefNode{{ID: "output", Type: NodeTypeOutput}}, Edges: []DefEdge{{FromNode: "missing", ToNode: "output"}}})},
		{name: "missing edge target", workflow: validWorkflowWithDefinition(DefinitionGraph{Nodes: []DefNode{{ID: "source", Type: NodeTypeSaySomething}}, Edges: []DefEdge{{FromNode: "source", ToNode: "missing"}}})},
		{name: "cycle", workflow: validWorkflowWithDefinition(DefinitionGraph{Nodes: []DefNode{{ID: "first"}, {ID: "second"}}, Edges: []DefEdge{{FromNode: "first", ToNode: "second"}, {FromNode: "second", ToNode: "first"}}})},
		{name: "empty graph", workflow: validWorkflowWithDefinition(DefinitionGraph{})},
		{name: "missing output", workflow: validWorkflowWithDefinition(DefinitionGraph{Nodes: []DefNode{{ID: "activity", Type: NodeTypeSaySomething}}})},
		{name: "multiple outputs", workflow: validWorkflowWithDefinition(DefinitionGraph{Nodes: []DefNode{{ID: "first", Type: NodeTypeOutput}, {ID: "second", Type: NodeTypeOutput}}})},
		{name: "node cannot reach output", workflow: validWorkflowWithDefinition(DefinitionGraph{Nodes: []DefNode{{ID: "orphan", Type: NodeTypeSaySomething}, {ID: "source", Type: NodeTypeExample}, {ID: "output", Type: NodeTypeOutput}}, Edges: []DefEdge{{FromNode: "source", ToNode: "output"}}})},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := &fakeWorkflowRepo{}
			_, err := NewWorkflowUsecase(repo).CreateWorkflow(context.Background(), tt.workflow)
			if !stderrors.Is(err, ErrWorkflowInvalidArgument) {
				t.Fatalf("CreateWorkflow() error = %v, want %v", err, ErrWorkflowInvalidArgument)
			}
			if repo.created != nil {
				t.Fatal("CreateWorkflow() called repo for invalid input")
			}
		})
	}
}

func testWorkflowUsecaseCreateWorkflowRepoError(t *testing.T) {
	repoErr := stderrors.New("create failed")
	repo := &fakeWorkflowRepo{err: repoErr}
	workflow := validWorkflowWithDefinition(DefinitionGraph{Nodes: []DefNode{{ID: "output", Type: NodeTypeOutput}}})

	_, err := NewWorkflowUsecase(repo).CreateWorkflow(context.Background(), workflow)
	if !stderrors.Is(err, repoErr) {
		t.Fatalf("CreateWorkflow() error = %v, want %v", err, repoErr)
	}
}

func TestWorkflowUsecaseQueries(t *testing.T) {
	want := &Workflow{ID: "workflow-1"}
	repo := &fakeWorkflowRepo{workflow: want, list: []*Workflow{want}}
	uc := NewWorkflowUsecase(repo)

	got, err := uc.GetWorkflow(context.Background(), " workflow-1 ")
	if err != nil || got != want {
		t.Fatalf("GetWorkflow() = (%v, %v), want (%v, nil)", got, err, want)
	}
	list, err := uc.ListWorkflows(context.Background(), ListWorkflowsQuery{UID: 1, Type: " dynamic ", Limit: 20})
	if err != nil || len(list) != 1 {
		t.Fatalf("ListWorkflows() = (%v, %v), want one workflow", list, err)
	}
	if repo.query.Type != "dynamic" {
		t.Fatalf("ListWorkflows() type = %q, want dynamic", repo.query.Type)
	}
	if _, err := uc.GetWorkflow(context.Background(), " "); !stderrors.Is(err, ErrWorkflowInvalidArgument) {
		t.Fatalf("GetWorkflow(empty) error = %v, want %v", err, ErrWorkflowInvalidArgument)
	}
	if _, err := uc.ListWorkflows(context.Background(), ListWorkflowsQuery{}); !stderrors.Is(err, ErrWorkflowInvalidArgument) {
		t.Fatalf("ListWorkflows(invalid) error = %v, want %v", err, ErrWorkflowInvalidArgument)
	}
}

func validWorkflowWithDefinition(definition DefinitionGraph) *Workflow {
	return &Workflow{UID: 1, Name: "flow", Type: "dynamic", Definition: definition}
}
