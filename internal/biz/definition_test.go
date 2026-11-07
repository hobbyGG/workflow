package biz

import (
	"context"
	stderrors "errors"
	"testing"
)

type fakeDefinitionRepo struct {
	definition *Definition
	list       []*Definition
	err        error
	created    *Definition
	query      ListDefinitionsQuery
}

func (r *fakeDefinitionRepo) GetDefinition(context.Context, string) (*Definition, error) {
	return r.definition, r.err
}

func (r *fakeDefinitionRepo) ListDefinitions(_ context.Context, query ListDefinitionsQuery) ([]*Definition, error) {
	r.query = query
	return r.list, r.err
}

func (r *fakeDefinitionRepo) CreateDefinition(_ context.Context, definition *Definition) (*Definition, error) {
	r.created = definition
	return definition, r.err
}

func TestDefinitionUsecaseCreateDefinition(t *testing.T) {
	tests := []struct {
		name string
		run  func(*testing.T)
	}{
		{name: "creates valid definition", run: testDefinitionUsecaseCreatesDefinition},
		{name: "validates definition", run: testDefinitionUsecaseCreateDefinitionValidation},
		{name: "returns repo error", run: testDefinitionUsecaseCreateDefinitionRepoError},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.run)
	}
}

func testDefinitionUsecaseCreatesDefinition(t *testing.T) {
	repo := &fakeDefinitionRepo{}
	uc := NewDefinitionUsecase(repo)
	definition := &Definition{
		UID:  42,
		Name: "flow",
		Type: "dynamic",
		DefGraph: DefinitionGraph{
			Nodes: []DefNode{{ID: "activity", Type: NodeTypeSaySomething}, {ID: "output", Type: NodeTypeOutput}},
			Edges: []DefEdge{{FromNode: "activity", FromKey: "result", ToNode: "output", ToKey: "result"}},
		},
	}

	got, err := uc.CreateDefinition(context.Background(), definition)
	if err != nil {
		t.Fatalf("CreateDefinition() error = %v", err)
	}
	if got.ID == "" {
		t.Fatal("CreateDefinition() did not generate an ID")
	}
	if repo.created != definition {
		t.Fatal("CreateDefinition() did not pass the definition to repo")
	}
}

func testDefinitionUsecaseCreateDefinitionValidation(t *testing.T) {
	tests := []struct {
		name       string
		definition *Definition
	}{
		{name: "nil definition"},
		{name: "missing uid", definition: &Definition{Name: "flow", Type: "dynamic"}},
		{name: "missing name", definition: &Definition{UID: 1, Type: "dynamic"}},
		{name: "missing type", definition: &Definition{UID: 1, Name: "flow"}},
		{name: "duplicate node id", definition: validDefinitionWithGraph(DefinitionGraph{Nodes: []DefNode{{ID: "same"}, {ID: "same"}}})},
		{name: "missing edge source", definition: validDefinitionWithGraph(DefinitionGraph{Nodes: []DefNode{{ID: "output", Type: NodeTypeOutput}}, Edges: []DefEdge{{FromNode: "missing", ToNode: "output"}}})},
		{name: "missing edge target", definition: validDefinitionWithGraph(DefinitionGraph{Nodes: []DefNode{{ID: "source", Type: NodeTypeSaySomething}}, Edges: []DefEdge{{FromNode: "source", ToNode: "missing"}}})},
		{name: "cycle", definition: validDefinitionWithGraph(DefinitionGraph{Nodes: []DefNode{{ID: "first"}, {ID: "second"}}, Edges: []DefEdge{{FromNode: "first", ToNode: "second"}, {FromNode: "second", ToNode: "first"}}})},
		{name: "empty graph", definition: validDefinitionWithGraph(DefinitionGraph{})},
		{name: "missing output", definition: validDefinitionWithGraph(DefinitionGraph{Nodes: []DefNode{{ID: "activity", Type: NodeTypeSaySomething}}})},
		{name: "multiple outputs", definition: validDefinitionWithGraph(DefinitionGraph{Nodes: []DefNode{{ID: "first", Type: NodeTypeOutput}, {ID: "second", Type: NodeTypeOutput}}})},
		{name: "node cannot reach output", definition: validDefinitionWithGraph(DefinitionGraph{Nodes: []DefNode{{ID: "orphan", Type: NodeTypeSaySomething}, {ID: "source", Type: NodeTypeExample}, {ID: "output", Type: NodeTypeOutput}}, Edges: []DefEdge{{FromNode: "source", ToNode: "output"}}})},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := &fakeDefinitionRepo{}
			_, err := NewDefinitionUsecase(repo).CreateDefinition(context.Background(), tt.definition)
			if !stderrors.Is(err, ErrDefinitionInvalidArgument) {
				t.Fatalf("CreateDefinition() error = %v, want %v", err, ErrDefinitionInvalidArgument)
			}
			if repo.created != nil {
				t.Fatal("CreateDefinition() called repo for invalid input")
			}
		})
	}
}

func testDefinitionUsecaseCreateDefinitionRepoError(t *testing.T) {
	repoErr := stderrors.New("create failed")
	repo := &fakeDefinitionRepo{err: repoErr}
	definition := validDefinitionWithGraph(DefinitionGraph{Nodes: []DefNode{{ID: "output", Type: NodeTypeOutput}}})

	_, err := NewDefinitionUsecase(repo).CreateDefinition(context.Background(), definition)
	if !stderrors.Is(err, repoErr) {
		t.Fatalf("CreateDefinition() error = %v, want %v", err, repoErr)
	}
}

func TestDefinitionUsecaseQueries(t *testing.T) {
	want := &Definition{ID: "definition-1"}
	repo := &fakeDefinitionRepo{definition: want, list: []*Definition{want}}
	uc := NewDefinitionUsecase(repo)

	got, err := uc.GetDefinition(context.Background(), " definition-1 ")
	if err != nil || got != want {
		t.Fatalf("GetDefinition() = (%v, %v), want (%v, nil)", got, err, want)
	}
	list, err := uc.ListDefinitions(context.Background(), ListDefinitionsQuery{UID: 1, Type: " dynamic ", Limit: 20})
	if err != nil || len(list) != 1 {
		t.Fatalf("ListDefinitions() = (%v, %v), want one definition", list, err)
	}
	if repo.query.Type != "dynamic" {
		t.Fatalf("ListDefinitions() type = %q, want dynamic", repo.query.Type)
	}
	if _, err := uc.GetDefinition(context.Background(), " "); !stderrors.Is(err, ErrDefinitionInvalidArgument) {
		t.Fatalf("GetDefinition(empty) error = %v, want %v", err, ErrDefinitionInvalidArgument)
	}
	if _, err := uc.ListDefinitions(context.Background(), ListDefinitionsQuery{}); !stderrors.Is(err, ErrDefinitionInvalidArgument) {
		t.Fatalf("ListDefinitions(invalid) error = %v, want %v", err, ErrDefinitionInvalidArgument)
	}
}

func validDefinitionWithGraph(definition DefinitionGraph) *Definition {
	return &Definition{UID: 1, Name: "flow", Type: "dynamic", DefGraph: definition}
}
