package data

import (
	"context"
	"fmt"
	"io"
	"maps"
	"net/http"
	"slices"

	"github.com/hobbyGG/workflow/internal/biz"

	"github.com/go-viper/mapstructure/v2"
	"go.temporal.io/sdk/temporal"
)

type nodeRepo struct {
	nodes map[biz.NodeType]biz.ActivityNode
}

var (
	_ biz.NodeRepo       = (*nodeRepo)(nil)
	_ biz.WorkerNodeRepo = (*nodeRepo)(nil)
)

// NewNodeRepo 创建节点信息仓库。
func NewNodeRepo(data *Data) biz.NodeRepo {
	return newNodeRepo(data)
}

// NewWorkerNodeRepo 创建 Worker 节点仓库。
func NewWorkerNodeRepo(data *Data) biz.WorkerNodeRepo {
	return newNodeRepo(data)
}

func newNodeRepo(data *Data) *nodeRepo {
	saySomethingNode := &saySomethingNode{}
	exampleNode := &exampleNode{client: data.httpClient}
	return &nodeRepo{
		nodes: map[biz.NodeType]biz.ActivityNode{
			saySomethingNode.Type(): saySomethingNode,
			exampleNode.Type():      exampleNode,
		},
	}
}

func (r *nodeRepo) GetActivityNode(nodeType biz.NodeType) biz.ActivityNode {
	return r.nodes[nodeType]
}

func (r *nodeRepo) GetActivityNodes() []biz.ActivityNode {
	nodeTypes := slices.Sorted(maps.Keys(r.nodes))
	nodes := make([]biz.ActivityNode, 0, len(nodeTypes))
	for _, nodeType := range nodeTypes {
		nodes = append(nodes, r.nodes[nodeType])
	}
	return nodes
}

type saySomethingNode struct{}

type saySomethingInput struct {
	Prompt *string `mapstructure:"prompt"`
}

func (*saySomethingNode) Type() biz.NodeType {
	return biz.NodeTypeSaySomething
}

func (*saySomethingNode) Schema() biz.NodeSchema {
	return biz.NodeSchema{
		Type: biz.NodeTypeSaySomething,
		Inputs: map[string]biz.InputDefinition{
			"prompt": {
				Type:     "string",
				Required: true,
			},
		},
		Outputs: map[string]biz.OutputDefinition{
			"text": {Type: "string"},
		},
	}
}

func (*saySomethingNode) Activity(_ context.Context, inputValues map[string]any) (map[string]any, error) {
	var input saySomethingInput
	if err := mapstructure.Decode(inputValues, &input); err != nil {
		return nil, temporal.NewNonRetryableApplicationError(
			"decode say something input",
			"SaySomethingInvalidInput",
			err,
		)
	}
	if input.Prompt == nil {
		return nil, temporal.NewNonRetryableApplicationError(
			"say something prompt must be a string",
			"SaySomethingInvalidInput",
			biz.ErrDynamicWorkflowInvalidDefinition,
		)
	}
	return map[string]any{"text": *input.Prompt}, nil
}

type exampleNode struct {
	client *http.Client
}

const exampleURL = "https://example.com"

func (*exampleNode) Type() biz.NodeType {
	return biz.NodeTypeExample
}

func (*exampleNode) Schema() biz.NodeSchema {
	return biz.NodeSchema{
		Type:   biz.NodeTypeExample,
		Inputs: map[string]biz.InputDefinition{},
		Outputs: map[string]biz.OutputDefinition{
			"status_code": {Type: "integer"},
			"body":        {Type: "string"},
		},
	}
}

func (n *exampleNode) Activity(ctx context.Context, _ map[string]any) (map[string]any, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, exampleURL, nil)
	if err != nil {
		return nil, temporal.NewNonRetryableApplicationError(
			"create example request",
			"ExampleRequestInvalid",
			err,
		)
	}
	resp, err := n.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return nil, fmt.Errorf("example request failed with status %s", resp.Status)
	}
	return map[string]any{
		"status_code": resp.StatusCode,
		"body":        string(body),
	}, nil
}
