package biz

import "testing"

func TestNodeUsecaseGetNodeSchemas(t *testing.T) {
	uc := NewNodeUsecase(&fakeToolNodeRepo{nodes: []ActivityNode{
		&fakeToolNode{nodeType: NodeType("ACTIVITY_C")},
	}})

	schemas := uc.GetNodeSchemas()
	wantTypes := map[NodeType]struct{}{
		NodeTypeOutput:         {},
		NodeTypeExclusive:      {},
		NodeTypeInclusive:      {},
		NodeType("ACTIVITY_C"): {},
	}
	if len(schemas) != len(wantTypes) {
		t.Fatalf("GetNodeSchemas() len = %d, want %d", len(schemas), len(wantTypes))
	}
	gotTypes := make(map[NodeType]struct{}, len(schemas))
	for _, schema := range schemas {
		if _, ok := wantTypes[schema.Type]; !ok {
			t.Fatalf("GetNodeSchemas() contains unexpected type %s", schema.Type)
		}
		if _, exists := gotTypes[schema.Type]; exists {
			t.Fatalf("GetNodeSchemas() contains duplicate type %s", schema.Type)
		}
		gotTypes[schema.Type] = struct{}{}
		if schema.InputSchema == nil || schema.OutputSchema == nil {
			t.Fatalf("GetNodeSchemas() schema = %#v, want object schemas", schema)
		}
		if schema.InputSchema.Type != "object" || schema.OutputSchema.Type != "object" {
			t.Fatalf("GetNodeSchemas() schema = %#v, want object schemas", schema)
		}
	}
	for nodeType := range wantTypes {
		if _, ok := gotTypes[nodeType]; !ok {
			t.Errorf("GetNodeSchemas() missing type %s", nodeType)
		}
	}
}
