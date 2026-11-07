package wfx

import (
	"context"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	workflowv1 "github.com/hobbyGG/workflow/api/workflow/v1"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestClientNodeAndToolMethods(t *testing.T) {
	toolOutput, err := structpb.NewStruct(map[string]any{"audio": "voice.mp3"})
	if err != nil {
		t.Fatal(err)
	}
	nodeSchema := testNodeSchema(t)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /internal/v1/node-schemas", func(w http.ResponseWriter, _ *http.Request) {
		writeProtoJSON(t, w, &workflowv1.ListNodeSchemasResponse{
			NodeSchemas: []*workflowv1.NodeSchema{nodeSchema},
		})
	})
	mux.HandleFunc("GET /internal/v1/tool/nodes", func(w http.ResponseWriter, _ *http.Request) {
		writeProtoJSON(t, w, &workflowv1.ListToolsResponse{
			Tools: []*workflowv1.ToolNode{{
				Schema: nodeSchema,
				Profile: &workflowv1.ToolProfile{
					SuccessRate:       0.75,
					AverageDurationMs: 1250.5,
				},
			}},
		})
	})
	mux.HandleFunc("POST /internal/v1/tool/node", func(w http.ResponseWriter, r *http.Request) {
		var request workflowv1.ExecuteToolRequest
		readProtoJSON(t, r, &request)
		if request.GetNodeType() != "SAY_SOMETHING" ||
			request.GetIdempotencyKey() != "request-1" ||
			request.GetInput().AsMap()["text"] != "hello" {
			t.Errorf("ExecuteTool request = %#v", &request)
		}
		writeProtoJSON(t, w, &workflowv1.ExecuteToolResponse{NodeExecutionId: "node-execution-1"})
	})
	mux.HandleFunc("POST /internal/v1/tool/execution", func(w http.ResponseWriter, r *http.Request) {
		var request workflowv1.CheckToolExecutionRequest
		readProtoJSON(t, r, &request)
		if request.GetNodeExecutionId() != "node-execution-1" {
			t.Errorf("CheckToolExecution ID = %q", request.GetNodeExecutionId())
		}
		writeProtoJSON(t, w, &workflowv1.CheckToolExecutionResponse{
			Status:     "completed",
			Output:     toolOutput,
			DurationMs: 250,
		})
	})

	server := httptest.NewServer(mux)
	defer server.Close()

	client, err := NewClient(context.Background(), server.URL, WithHTTPClient(server.Client()))
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	defer client.Close()

	nodeTypes, err := client.NodeTypes(context.Background())
	if err != nil {
		t.Fatalf("NodeTypes() error = %v", err)
	}
	if len(nodeTypes) != 1 || nodeTypes[0].Type != "SAY_SOMETHING" {
		t.Fatalf("NodeTypes() = %#v", nodeTypes)
	}
	assertNodeSchema(t, nodeTypes[0])

	tools, err := client.ListTools(context.Background())
	if err != nil {
		t.Fatalf("ListTools() error = %v", err)
	}
	if len(tools) != 1 ||
		tools[0].Schema.Type != "SAY_SOMETHING" ||
		tools[0].Profile == nil ||
		tools[0].Profile.SuccessRate != 0.75 ||
		tools[0].Profile.AverageDurationMS != 1250.5 {
		t.Fatalf("ListTools() = %#v", tools)
	}
	assertNodeSchema(t, tools[0].Schema)

	nodeExecutionID, err := client.ExecuteTool(context.Background(), ExecuteToolOptions{
		NodeType:       "SAY_SOMETHING",
		Input:          map[string]any{"text": "hello"},
		IdempotencyKey: "request-1",
	})
	if err != nil {
		t.Fatalf("ExecuteTool() error = %v", err)
	}
	if nodeExecutionID != "node-execution-1" {
		t.Fatalf("ExecuteTool() ID = %q", nodeExecutionID)
	}

	execution, err := client.CheckToolExecution(context.Background(), nodeExecutionID)
	if err != nil {
		t.Fatalf("CheckToolExecution() error = %v", err)
	}
	if execution.Status != ToolExecutionStatusCompleted ||
		execution.Output["audio"] != "voice.mp3" ||
		execution.DurationMS != 250 {
		t.Fatalf("CheckToolExecution() = %#v", execution)
	}
}

func TestNodeTypesFromProtoReturnsSchemaMarshalError(t *testing.T) {
	_, err := nodeTypesFromProto([]*workflowv1.NodeSchema{{
		Type: "invalid",
		InputSchema: &structpb.Struct{Fields: map[string]*structpb.Value{
			"invalid": structpb.NewNumberValue(math.NaN()),
		}},
		OutputSchema: &structpb.Struct{},
	}})
	if err == nil {
		t.Fatal("nodeTypesFromProto() error = nil")
	}
	if !strings.Contains(err.Error(), "node schema 0: input schema") {
		t.Fatalf("nodeTypesFromProto() error = %v, want input schema error", err)
	}
}

func TestNodeTypeFromProtoRejectsIncompleteSchema(t *testing.T) {
	tests := []struct {
		name      string
		schema    *workflowv1.NodeSchema
		wantError string
	}{
		{name: "missing node schema", wantError: "node schema is missing"},
		{name: "missing type", schema: &workflowv1.NodeSchema{
			InputSchema:  &structpb.Struct{},
			OutputSchema: &structpb.Struct{},
		}, wantError: "node type is missing"},
		{name: "missing input schema", schema: &workflowv1.NodeSchema{
			Type:         "SAY_SOMETHING",
			OutputSchema: &structpb.Struct{},
		}, wantError: `input schema for node "SAY_SOMETHING": schema is missing`},
		{name: "missing output schema", schema: &workflowv1.NodeSchema{
			Type:        "SAY_SOMETHING",
			InputSchema: &structpb.Struct{},
		}, wantError: `output schema for node "SAY_SOMETHING": schema is missing`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := nodeTypeFromProto(tt.schema)
			if err == nil {
				t.Fatal("nodeTypeFromProto() error = nil")
			}
			if !strings.Contains(err.Error(), tt.wantError) {
				t.Fatalf("nodeTypeFromProto() error = %v, want error containing %q", err, tt.wantError)
			}
		})
	}
}

func TestToolsFromProtoRejectsMissingToolSchema(t *testing.T) {
	tests := []struct {
		name      string
		tool      *workflowv1.ToolNode
		wantError string
	}{
		{name: "missing tool", wantError: "tool 0 is missing"},
		{name: "missing schema", tool: &workflowv1.ToolNode{}, wantError: "tool 0: node schema is missing"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := toolsFromProto([]*workflowv1.ToolNode{tt.tool})
			if err == nil {
				t.Fatal("toolsFromProto() error = nil")
			}
			if !strings.Contains(err.Error(), tt.wantError) {
				t.Fatalf("toolsFromProto() error = %v, want error containing %q", err, tt.wantError)
			}
		})
	}
}

func TestToolsFromProtoReturnsSchemaMarshalError(t *testing.T) {
	_, err := toolsFromProto([]*workflowv1.ToolNode{{
		Schema: &workflowv1.NodeSchema{
			Type:        "invalid",
			InputSchema: &structpb.Struct{},
			OutputSchema: &structpb.Struct{Fields: map[string]*structpb.Value{
				"invalid": structpb.NewNumberValue(math.Inf(1)),
			}},
		},
	}})
	if err == nil {
		t.Fatal("toolsFromProto() error = nil")
	}
	if !strings.Contains(err.Error(), "tool 0: output schema") {
		t.Fatalf("toolsFromProto() error = %v, want output schema error", err)
	}
}

func testNodeSchema(t *testing.T) *workflowv1.NodeSchema {
	t.Helper()
	inputSchema, err := structpb.NewStruct(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"text": map[string]any{"type": "string"},
			"speakers": map[string]any{
				"type": "array",
				"items": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"voice_id": map[string]any{"type": "string"},
					},
					"required": []any{"voice_id"},
				},
			},
		},
		"required":             []any{"text", "speakers"},
		"additionalProperties": false,
	})
	if err != nil {
		t.Fatalf("create input schema: %v", err)
	}
	outputSchema, err := structpb.NewStruct(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"audio": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"url": map[string]any{"type": "string"},
				},
				"required": []any{"url"},
			},
		},
		"required": []any{"audio"},
	})
	if err != nil {
		t.Fatalf("create output schema: %v", err)
	}
	return &workflowv1.NodeSchema{
		Type:         "SAY_SOMETHING",
		InputSchema:  inputSchema,
		OutputSchema: outputSchema,
	}
}

func assertNodeSchema(t *testing.T, nodeType NodeType) {
	t.Helper()
	var input struct {
		Required   []string `json:"required"`
		Properties map[string]struct {
			Items struct {
				Required []string `json:"required"`
			} `json:"items"`
		} `json:"properties"`
	}
	if err := json.Unmarshal(nodeType.InputSchema, &input); err != nil {
		t.Fatalf("decode input schema: %v", err)
	}
	if !slices.Contains(input.Required, "text") ||
		!slices.Contains(input.Required, "speakers") ||
		!slices.Contains(input.Properties["speakers"].Items.Required, "voice_id") {
		t.Fatalf("input schema = %s", nodeType.InputSchema)
	}

	var output struct {
		Required   []string `json:"required"`
		Properties map[string]struct {
			Required []string `json:"required"`
		} `json:"properties"`
	}
	if err := json.Unmarshal(nodeType.OutputSchema, &output); err != nil {
		t.Fatalf("decode output schema: %v", err)
	}
	if !slices.Contains(output.Required, "audio") ||
		!slices.Contains(output.Properties["audio"].Required, "url") {
		t.Fatalf("output schema = %s", nodeType.OutputSchema)
	}
}

func readProtoJSON(t *testing.T, request *http.Request, message proto.Message) {
	t.Helper()
	body, err := io.ReadAll(request.Body)
	if err != nil {
		t.Errorf("read request: %v", err)
		return
	}
	if err := protojson.Unmarshal(body, message); err != nil {
		t.Errorf("decode request: %v", err)
	}
}

func writeProtoJSON(t *testing.T, response http.ResponseWriter, message proto.Message) {
	t.Helper()
	body, err := protojson.Marshal(message)
	if err != nil {
		t.Errorf("encode response: %v", err)
		http.Error(response, "encode response", http.StatusInternalServerError)
		return
	}
	response.Header().Set("Content-Type", "application/protojson")
	if _, err := response.Write(body); err != nil {
		t.Errorf("write response: %v", err)
	}
}
