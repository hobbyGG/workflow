package service

import (
	"context"
	"errors"
	"strings"
	"testing"

	pb "github.com/hobbyGG/workflow/api/workflow/v1"
	"github.com/hobbyGG/workflow/internal/biz"

	"github.com/google/jsonschema-go/jsonschema"
)

func TestWorkflowServiceListNodeSchemas(t *testing.T) {
	schema := biz.NodeSchema{
		Type: biz.NodeType("ACTIVITY_C"),
		InputSchema: &jsonschema.Schema{
			Type: "object",
			Properties: map[string]*jsonschema.Schema{
				"text": {Type: "string"},
			},
			Required: []string{"text"},
		},
		OutputSchema: &jsonschema.Schema{
			Type: "object",
			Properties: map[string]*jsonschema.Schema{
				"audio": {Type: "string"},
			},
		},
	}
	service := NewWorkflowService(
		nil,
		nil,
		biz.NewNodeUsecase(&fakeServiceToolNodeRepo{
			nodes: []biz.ActivityNode{&fakeServiceToolNode{schema: schema}},
		}),
	)

	reply, err := service.ListNodeSchemas(context.Background(), &pb.ListNodeSchemasRequest{})
	if err != nil {
		t.Fatalf("ListNodeSchemas() error = %v", err)
	}

	for _, nodeSchema := range reply.GetNodeSchemas() {
		if nodeSchema.GetType() != string(biz.NodeType("ACTIVITY_C")) {
			continue
		}
		text := nodeSchema.GetInputSchema().GetFields()["properties"].GetStructValue().GetFields()["text"].GetStructValue()
		if text.GetFields()["type"].GetStringValue() != "string" {
			t.Fatalf("ListNodeSchemas() input schema = %#v, want text string", nodeSchema.GetInputSchema())
		}
		audio := nodeSchema.GetOutputSchema().GetFields()["properties"].GetStructValue().GetFields()["audio"].GetStructValue()
		if audio.GetFields()["type"].GetStringValue() != "string" {
			t.Fatalf("ListNodeSchemas() output schema = %#v, want audio string", nodeSchema.GetOutputSchema())
		}
		return
	}

	t.Fatal("ListNodeSchemas() did not return the ACTIVITY_C schema")
}

func TestConvertNodeSchemaRejectsMissingSchema(t *testing.T) {
	tests := []struct {
		name        string
		schema      biz.NodeSchema
		wantContext string
	}{
		{
			name: "missing input",
			schema: biz.NodeSchema{
				Type:         biz.NodeType("ACTIVITY_C"),
				OutputSchema: &jsonschema.Schema{Type: "object"},
			},
			wantContext: "convert ACTIVITY_C input schema",
		},
		{
			name: "missing output",
			schema: biz.NodeSchema{
				Type:        biz.NodeType("ACTIVITY_C"),
				InputSchema: &jsonschema.Schema{Type: "object"},
			},
			wantContext: "convert ACTIVITY_C output schema",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := convertNodeSchema(tt.schema)
			if err == nil {
				t.Fatal("convertNodeSchema() error = nil, want missing schema error")
			}
			if !strings.Contains(err.Error(), tt.wantContext) {
				t.Fatalf("convertNodeSchema() error = %q, want context %q", err, tt.wantContext)
			}
		})
	}
}

func TestWorkflowServiceListNodeSchemasMapsMissingSchema(t *testing.T) {
	service := NewWorkflowService(
		nil,
		nil,
		biz.NewNodeUsecase(&fakeServiceToolNodeRepo{
			nodes: []biz.ActivityNode{&fakeServiceToolNode{schema: biz.NodeSchema{
				Type:         biz.NodeType("ACTIVITY_C"),
				OutputSchema: &jsonschema.Schema{Type: "object"},
			}}},
		}),
	)

	_, err := service.ListNodeSchemas(context.Background(), &pb.ListNodeSchemasRequest{})
	if !errors.Is(err, biz.ErrDefinitionInternal) {
		t.Fatalf("ListNodeSchemas() error = %v, want %v", err, biz.ErrDefinitionInternal)
	}
}
