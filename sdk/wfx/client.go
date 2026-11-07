// Package wfx provides a Go client for the workflow service.
package wfx

import (
	"context"
	"errors"
	"fmt"
	"strings"

	khttp "github.com/go-kratos/kratos/v3/transport/http"
	v1 "github.com/hobbyGG/workflow/api/workflow/v1"
)

// Client calls the workflow service over HTTP.
type Client struct {
	transport *khttp.Client
	api       v1.WorkflowServiceHTTPClient
	tools     v1.ToolServiceHTTPClient
}

// NewClient creates a workflow client for endpoint(baseURL:port).
func NewClient(ctx context.Context, endpoint string, opts ...Option) (*Client, error) {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		return nil, errors.New("wfx: endpoint is required")
	}

	config := options{}
	for _, opt := range opts {
		opt(&config)
	}

	httpClient := config.httpClient
	if httpClient == nil {
		httpClient = NewHTTPClient()
	}
	timeout := httpClient.Timeout
	if config.timeout != nil {
		timeout = *config.timeout
	}
	if timeout < 0 {
		return nil, errors.New("wfx: timeout cannot be negative")
	}

	transport, err := khttp.NewClient(
		ctx,
		khttp.WithEndpoint(endpoint),
		khttp.WithTimeout(timeout),
		khttp.WithTransport(httpClient.Transport),
	)
	if err != nil {
		return nil, fmt.Errorf("wfx: create HTTP client: %w", err)
	}

	return &Client{
		transport: transport,
		api:       v1.NewWorkflowServiceHTTPClient(transport),
		tools:     v1.NewToolServiceHTTPClient(transport),
	}, nil
}

// Close releases resources owned by the client.
func (c *Client) Close() error {
	return c.transport.Close()
}

// Create creates a workflow definition.
func (c *Client) Create(ctx context.Context, opts CreateOptions) (*Workflow, error) {
	definition, err := definitionToProto(opts.Definition)
	if err != nil {
		return nil, fmt.Errorf("wfx: encode workflow definition: %w", err)
	}

	reply, err := c.api.CreateDefinition(ctx, &v1.CreateDefinitionRequest{
		Uid:        opts.UserID,
		Name:       opts.Name,
		Type:       opts.Type,
		Definition: definition,
	})
	if err != nil {
		return nil, fmt.Errorf("wfx: create workflow: %w", err)
	}
	if reply == nil {
		return nil, errors.New("wfx: create workflow returned an empty response")
	}
	if err := responseError("create workflow", reply.GetErrorCode(), reply.GetErrorMsg(), reply.GetInternalErrorCode(), reply.GetInternalErrorMsg()); err != nil {
		return nil, err
	}
	if reply.GetData() == nil {
		return nil, errors.New("wfx: create workflow returned no workflow")
	}
	return workflowFromProto(reply.GetData()), nil
}

// Get returns a workflow definition by ID.
func (c *Client) Get(ctx context.Context, id string) (*Workflow, error) {
	reply, err := c.api.GetDefinition(ctx, &v1.GetDefinitionRequest{DefinitionId: id})
	if err != nil {
		return nil, fmt.Errorf("wfx: get workflow: %w", err)
	}
	if reply == nil {
		return nil, errors.New("wfx: get workflow returned an empty response")
	}
	if err := responseError("get workflow", reply.GetErrorCode(), reply.GetErrorMsg(), reply.GetInternalErrorCode(), reply.GetInternalErrorMsg()); err != nil {
		return nil, err
	}
	if reply.GetData() == nil {
		return nil, errors.New("wfx: get workflow returned no workflow")
	}
	return workflowFromProto(reply.GetData()), nil
}

// List returns workflow definitions matching opts.
func (c *Client) List(ctx context.Context, opts ListOptions) (*Page[Workflow], error) {
	reply, err := c.api.ListDefinitions(ctx, &v1.ListDefinitionsRequest{
		Uid:    opts.UserID,
		Type:   opts.Type,
		Limit:  opts.Limit,
		Offset: opts.Offset,
	})
	if err != nil {
		return nil, fmt.Errorf("wfx: list workflows: %w", err)
	}
	if reply == nil {
		return nil, errors.New("wfx: list workflows returned an empty response")
	}
	if err := responseError("list workflows", reply.GetErrorCode(), reply.GetErrorMsg(), reply.GetInternalErrorCode(), reply.GetInternalErrorMsg()); err != nil {
		return nil, err
	}
	if reply.GetData() == nil {
		return nil, errors.New("wfx: list workflows returned no page")
	}

	data := reply.GetData()
	items := make([]Workflow, 0, len(data.GetDefinitions()))
	for _, item := range data.GetDefinitions() {
		if workflow := workflowFromProto(item); workflow != nil {
			items = append(items, *workflow)
		}
	}
	return &Page[Workflow]{
		Items:  items,
		Limit:  data.GetLimit(),
		Offset: data.GetOffset(),
	}, nil
}

// CreateWorkflowExecution starts an asynchronous workflow execution and returns its ID.
func (c *Client) CreateWorkflowExecution(ctx context.Context, opts CreateWorkflowExecutionOptions) (string, error) {
	input, err := inputToProto(opts.Input)
	if err != nil {
		return "", fmt.Errorf("wfx: encode workflow execution input: %w", err)
	}

	reply, err := c.api.CreateWorkflowExecution(ctx, &v1.CreateWorkflowExecutionRequest{
		Uid:            opts.UserID,
		IdempotencyKey: opts.IdempotencyKey,
		DefinitionId:   opts.DefinitionID,
		Input:          input,
	})
	if err != nil {
		return "", fmt.Errorf("wfx: create workflow execution: %w", err)
	}
	if reply == nil {
		return "", errors.New("wfx: create workflow execution returned an empty response")
	}
	if err := responseError("create workflow execution", reply.GetErrorCode(), reply.GetErrorMsg(), reply.GetInternalErrorCode(), reply.GetInternalErrorMsg()); err != nil {
		return "", err
	}
	if reply.GetData() == nil || reply.GetData().GetWorkflowExecutionId() == "" {
		return "", errors.New("wfx: create workflow execution returned no workflow execution ID")
	}
	return reply.GetData().GetWorkflowExecutionId(), nil
}

// GetWorkflowExecution returns a workflow execution by ID.
func (c *Client) GetWorkflowExecution(ctx context.Context, id string) (*WorkflowExecution, error) {
	reply, err := c.api.GetWorkflowExecution(ctx, &v1.GetWorkflowExecutionRequest{WorkflowExecutionId: id})
	if err != nil {
		return nil, fmt.Errorf("wfx: get workflow execution: %w", err)
	}
	if reply == nil {
		return nil, errors.New("wfx: get workflow execution returned an empty response")
	}
	if err := responseError("get workflow execution", reply.GetErrorCode(), reply.GetErrorMsg(), reply.GetInternalErrorCode(), reply.GetInternalErrorMsg()); err != nil {
		return nil, err
	}
	if reply.GetData() == nil {
		return nil, errors.New("wfx: get workflow execution returned no workflow execution")
	}
	return wfExecutionFromProto(reply.GetData()), nil
}

// ListWorkflowExecutions returns workflow executions matching opts.
func (c *Client) ListWorkflowExecutions(ctx context.Context, opts ListWorkflowExecutionsOptions) (*Page[WorkflowExecution], error) {
	reply, err := c.api.ListWorkflowExecutions(ctx, &v1.ListWorkflowExecutionsRequest{
		Uid:            opts.UserID,
		DefinitionType: opts.DefinitionType,
		Limit:          opts.Limit,
		Offset:         opts.Offset,
	})
	if err != nil {
		return nil, fmt.Errorf("wfx: list workflow executions: %w", err)
	}
	if reply == nil {
		return nil, errors.New("wfx: list workflow executions returned an empty response")
	}
	if err := responseError("list workflow executions", reply.GetErrorCode(), reply.GetErrorMsg(), reply.GetInternalErrorCode(), reply.GetInternalErrorMsg()); err != nil {
		return nil, err
	}
	if reply.GetData() == nil {
		return nil, errors.New("wfx: list workflow executions returned no page")
	}

	data := reply.GetData()
	items := make([]WorkflowExecution, 0, len(data.GetWorkflowExecutions()))
	for _, item := range data.GetWorkflowExecutions() {
		if wfExecution := wfExecutionFromProto(item); wfExecution != nil {
			items = append(items, *wfExecution)
		}
	}
	return &Page[WorkflowExecution]{
		Items:  items,
		Limit:  data.GetLimit(),
		Offset: data.GetOffset(),
	}, nil
}

// NodeTypes returns the supported workflow node types.
func (c *Client) NodeTypes(ctx context.Context) ([]NodeType, error) {
	reply, err := c.api.ListNodeSchemas(ctx, &v1.ListNodeSchemasRequest{})
	if err != nil {
		return nil, fmt.Errorf("wfx: list node schemas: %w", err)
	}
	if reply == nil {
		return nil, errors.New("wfx: list node schemas returned an empty response")
	}
	nodeTypes, err := nodeTypesFromProto(reply.GetNodeSchemas())
	if err != nil {
		return nil, fmt.Errorf("wfx: decode node schemas: %w", err)
	}
	return nodeTypes, nil
}

// ListTools returns the nodes that can be executed as standalone tools.
func (c *Client) ListTools(ctx context.Context) ([]Tool, error) {
	reply, err := c.tools.ListTools(ctx, &v1.ListToolsRequest{})
	if err != nil {
		return nil, fmt.Errorf("wfx: list tools: %w", err)
	}
	if reply == nil {
		return nil, errors.New("wfx: list tools returned an empty response")
	}
	tools, err := toolsFromProto(reply.GetTools())
	if err != nil {
		return nil, fmt.Errorf("wfx: decode tools: %w", err)
	}
	return tools, nil
}

// ExecuteTool starts an asynchronous tool execution and returns its ID.
func (c *Client) ExecuteTool(ctx context.Context, opts ExecuteToolOptions) (string, error) {
	input, err := inputToProto(opts.Input)
	if err != nil {
		return "", fmt.Errorf("wfx: encode tool input: %w", err)
	}

	reply, err := c.tools.ExecuteTool(ctx, &v1.ExecuteToolRequest{
		NodeType:       opts.NodeType,
		Input:          input,
		IdempotencyKey: opts.IdempotencyKey,
	})
	if err != nil {
		return "", fmt.Errorf("wfx: execute tool: %w", err)
	}
	if reply == nil {
		return "", errors.New("wfx: execute tool returned an empty response")
	}
	if reply.GetNodeExecutionId() == "" {
		return "", errors.New("wfx: execute tool returned no node execution ID")
	}
	return reply.GetNodeExecutionId(), nil
}

// CheckToolExecution returns the current state and result of a tool execution.
func (c *Client) CheckToolExecution(ctx context.Context, nodeExecutionID string) (*ToolExecution, error) {
	reply, err := c.tools.CheckToolExecution(ctx, &v1.CheckToolExecutionRequest{
		NodeExecutionId: nodeExecutionID,
	})
	if err != nil {
		return nil, fmt.Errorf("wfx: check tool execution: %w", err)
	}
	if reply == nil {
		return nil, errors.New("wfx: check tool execution returned an empty response")
	}
	return toolExecutionFromProto(reply), nil
}
