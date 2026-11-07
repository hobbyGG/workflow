package service

import (
	"context"
	"errors"
	"testing"

	pb "github.com/hobbyGG/workflow/api/workflow/v1"
	"github.com/hobbyGG/workflow/internal/biz"

	"github.com/google/jsonschema-go/jsonschema"
	"go.temporal.io/sdk/temporal"
)

type fakeServiceToolNode struct {
	schema biz.NodeSchema
}

func (n *fakeServiceToolNode) Type() biz.NodeType {
	return n.schema.Type
}

func (n *fakeServiceToolNode) Schema() biz.NodeSchema {
	return n.schema
}

func (*fakeServiceToolNode) RetryPolicy() temporal.RetryPolicy {
	return temporal.RetryPolicy{}
}

func (*fakeServiceToolNode) Activity(context.Context, map[string]any) (map[string]any, error) {
	return nil, nil
}

type fakeServiceToolNodeRepo struct {
	nodes []biz.ActivityNode
}

func (r *fakeServiceToolNodeRepo) GetActivityNodes() []biz.ActivityNode {
	return r.nodes
}

type fakeServiceNodeExecutionRepo struct {
	infos       map[biz.NodeType]biz.NodeExecutionInfo
	execution   *biz.NodeExecution
	requestedID string
	err         error
}

func (r *fakeServiceNodeExecutionRepo) GetNodeExecutionInfos(context.Context) (map[biz.NodeType]biz.NodeExecutionInfo, error) {
	return r.infos, r.err
}

func (r *fakeServiceNodeExecutionRepo) GetNodeExecution(_ context.Context, nodeExecutionID string) (*biz.NodeExecution, error) {
	r.requestedID = nodeExecutionID
	return r.execution, r.err
}

func (*fakeServiceNodeExecutionRepo) CreateNodeExecution(_ context.Context, execution *biz.NodeExecution) (*biz.NodeExecution, error) {
	return execution, nil
}

func (*fakeServiceNodeExecutionRepo) DispatchNodeExecution(context.Context, string, biz.ActivityNodeWorkflowParam) error {
	return nil
}

func newTestToolService(nodeRepo biz.NodeRepo, executionRepo biz.NodeExecutionRepo) *ToolService {
	return NewToolService(biz.NewToolUsecase(nodeRepo, executionRepo))
}

func TestToolServiceListTools(t *testing.T) {
	schema := biz.NodeSchema{
		Type: biz.NodeType("ACTIVITY_C"),
		InputSchema: &jsonschema.Schema{
			Type: "object",
			Properties: map[string]*jsonschema.Schema{
				"text": {Type: "string"},
				"speakers": {
					Type: "array",
					Items: &jsonschema.Schema{
						Type: "object",
						Properties: map[string]*jsonschema.Schema{
							"voice_id": {Type: "string"},
						},
					},
				},
			},
			Required: []string{"text", "speakers"},
		},
		OutputSchema: &jsonschema.Schema{
			Type: "object",
			Properties: map[string]*jsonschema.Schema{
				"audio": {Type: "string"},
			},
		},
	}
	profile := biz.NodeExecutionInfo{SuccessRatio: 0.75, AvgDurationsMS: 1250.5}
	service := newTestToolService(
		&fakeServiceToolNodeRepo{nodes: []biz.ActivityNode{&fakeServiceToolNode{schema: schema}}},
		&fakeServiceNodeExecutionRepo{infos: map[biz.NodeType]biz.NodeExecutionInfo{biz.NodeType("ACTIVITY_C"): profile}},
	)

	reply, err := service.ListTools(context.Background(), &pb.ListToolsRequest{})
	if err != nil {
		t.Fatalf("ListTools() error = %v", err)
	}
	if len(reply.GetTools()) != 1 {
		t.Fatalf("ListTools() tools len = %d, want 1", len(reply.GetTools()))
	}
	tool := reply.GetTools()[0]
	if tool.GetSchema().GetType() != string(biz.NodeType("ACTIVITY_C")) {
		t.Fatalf("ListTools() schema = %#v, want ACTIVITY_C schema", tool.GetSchema())
	}
	properties := tool.GetSchema().GetInputSchema().GetFields()["properties"].GetStructValue()
	speakers := properties.GetFields()["speakers"].GetStructValue()
	items := speakers.GetFields()["items"].GetStructValue()
	voiceID := items.GetFields()["properties"].GetStructValue().GetFields()["voice_id"].GetStructValue()
	if voiceID.GetFields()["type"].GetStringValue() != "string" {
		t.Fatalf("ListTools() input schema = %#v, want speakers object items", tool.GetSchema().GetInputSchema())
	}
	audio := tool.GetSchema().GetOutputSchema().GetFields()["properties"].GetStructValue().GetFields()["audio"].GetStructValue()
	if audio.GetFields()["type"].GetStringValue() != "string" {
		t.Fatalf("ListTools() output schema = %#v, want audio string", tool.GetSchema().GetOutputSchema())
	}
	if tool.GetProfile().GetSuccessRate() != profile.SuccessRatio || tool.GetProfile().GetAverageDurationMs() != profile.AvgDurationsMS {
		t.Fatalf("ListTools() profile = %#v, want %#v", tool.GetProfile(), profile)
	}
}

func TestToolServiceListToolsMapsMissingSchema(t *testing.T) {
	service := newTestToolService(
		&fakeServiceToolNodeRepo{nodes: []biz.ActivityNode{&fakeServiceToolNode{schema: biz.NodeSchema{
			Type:        biz.NodeType("ACTIVITY_C"),
			InputSchema: &jsonschema.Schema{Type: "object"},
		}}}},
		&fakeServiceNodeExecutionRepo{},
	)

	_, err := service.ListTools(context.Background(), &pb.ListToolsRequest{})
	if !errors.Is(err, biz.ErrToolInternal) {
		t.Fatalf("ListTools() error = %v, want %v", err, biz.ErrToolInternal)
	}
}

func TestToolServiceExecuteToolDelegatesToUsecase(t *testing.T) {
	service := newTestToolService(
		&fakeServiceToolNodeRepo{nodes: []biz.ActivityNode{&fakeServiceToolNode{schema: biz.NodeSchema{Type: biz.NodeType("ACTIVITY_C")}}}},
		&fakeServiceNodeExecutionRepo{},
	)

	reply, err := service.ExecuteTool(context.Background(), &pb.ExecuteToolRequest{
		NodeType:       "  ACTIVITY_C  ",
		IdempotencyKey: "  request-1  ",
	})
	if err != nil {
		t.Fatalf("ExecuteTool() error = %v", err)
	}
	if reply.GetNodeExecutionId() == "" {
		t.Fatal("ExecuteTool() returned an empty node execution ID")
	}
}

func TestToolServiceExecuteToolValidatesRequest(t *testing.T) {
	tests := []struct {
		name string
		req  *pb.ExecuteToolRequest
	}{
		{name: "nil request"},
		{name: "missing node type", req: &pb.ExecuteToolRequest{IdempotencyKey: "request-1"}},
		{name: "missing idempotency key", req: &pb.ExecuteToolRequest{NodeType: "ACTIVITY_C"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			service := newTestToolService(&fakeServiceToolNodeRepo{}, &fakeServiceNodeExecutionRepo{})
			_, err := service.ExecuteTool(context.Background(), tt.req)
			if !errors.Is(err, biz.ErrToolInvalidArgument) {
				t.Fatalf("ExecuteTool() error = %v, want %v", err, biz.ErrToolInvalidArgument)
			}
		})
	}
}

func TestToolServiceCheckToolExecution(t *testing.T) {
	internalErrorMessage := "activity failed"
	durationMS := int64(250)
	executionRepo := &fakeServiceNodeExecutionRepo{execution: &biz.NodeExecution{
		ID:                   "node-execution-1",
		Status:               biz.NodeExecutionStatusFailed,
		Output:               map[string]any{"partial": true},
		InternalErrorMessage: &internalErrorMessage,
		DurationMS:           &durationMS,
	}}
	service := newTestToolService(&fakeServiceToolNodeRepo{}, executionRepo)

	reply, err := service.CheckToolExecution(context.Background(), &pb.CheckToolExecutionRequest{
		NodeExecutionId: "  node-execution-1  ",
	})
	if err != nil {
		t.Fatalf("CheckToolExecution() error = %v", err)
	}
	if executionRepo.requestedID != "node-execution-1" {
		t.Fatalf("CheckToolExecution() repo ID = %q, want node-execution-1", executionRepo.requestedID)
	}
	if reply.GetStatus() != string(biz.NodeExecutionStatusFailed) ||
		reply.GetOutput().AsMap()["partial"] != true ||
		reply.GetInternalErrorMessage() != internalErrorMessage ||
		reply.GetDurationMs() != durationMS {
		t.Fatalf("CheckToolExecution() = %#v, want failed execution", reply)
	}
}

func TestToolServiceCheckToolExecutionValidatesRequest(t *testing.T) {
	service := newTestToolService(&fakeServiceToolNodeRepo{}, &fakeServiceNodeExecutionRepo{})
	_, err := service.CheckToolExecution(context.Background(), &pb.CheckToolExecutionRequest{})
	if !errors.Is(err, biz.ErrToolInvalidArgument) {
		t.Fatalf("CheckToolExecution() error = %v, want %v", err, biz.ErrToolInvalidArgument)
	}
}
