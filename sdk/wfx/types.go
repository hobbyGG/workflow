package wfx

import (
	"encoding/json"
	"time"
)

// CreateOptions contains the fields needed to create a workflow definition.
type CreateOptions struct {
	UserID     int64
	Name       string
	Type       string
	Definition Definition
}

// ListOptions filters workflow definitions.
type ListOptions struct {
	UserID int64
	Type   string
	Limit  int32
	Offset int32
}

// CreateWorkflowExecutionOptions contains the fields needed to start a workflow execution.
type CreateWorkflowExecutionOptions struct {
	UserID         int64
	IdempotencyKey string
	DefinitionID   string
	Input          map[string]any
}

// ListWorkflowExecutionsOptions filters workflow executions.
type ListWorkflowExecutionsOptions struct {
	UserID         int64
	DefinitionType string
	Limit          int32
	Offset         int32
}

// ExecuteToolOptions contains the fields needed to start a standalone tool execution.
type ExecuteToolOptions struct {
	NodeType       string
	Input          map[string]any
	IdempotencyKey string
}

// Page is one page of results.
type Page[T any] struct {
	Items  []T
	Limit  int32
	Offset int32
}

// Workflow is a saved workflow definition.
type Workflow struct {
	ID         string
	UserID     int64
	Name       string
	Type       string
	Definition Definition
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

// Definition describes the nodes and edges in a workflow.
type Definition struct {
	Nodes []Node
	Edges []Edge
}

// Node is one executable unit in a workflow definition.
type Node struct {
	ID     string
	Type   string
	Preset map[string]any
}

// Edge connects two workflow nodes.
type Edge struct {
	FromNode   string
	FromBranch string
	FromPort   string
	ToNode     string
	ToInput    string
}

// WorkflowExecutionStatus is the lifecycle state of a workflow execution.
type WorkflowExecutionStatus string

const (
	WorkflowExecutionStatusPending        WorkflowExecutionStatus = "pending"
	WorkflowExecutionStatusRunning        WorkflowExecutionStatus = "running"
	WorkflowExecutionStatusCompleted      WorkflowExecutionStatus = "completed"
	WorkflowExecutionStatusFailed         WorkflowExecutionStatus = "failed"
	WorkflowExecutionStatusCanceled       WorkflowExecutionStatus = "canceled"
	WorkflowExecutionStatusTerminated     WorkflowExecutionStatus = "terminated"
	WorkflowExecutionStatusContinuedAsNew WorkflowExecutionStatus = "continued_as_new"
	WorkflowExecutionStatusTimedOut       WorkflowExecutionStatus = "timed_out"
	WorkflowExecutionStatusPaused         WorkflowExecutionStatus = "paused"
)

// WorkflowExecution is one execution of a workflow definition.
type WorkflowExecution struct {
	ID             string
	UserID         int64
	IdempotencyKey string
	DefinitionID   string
	DefinitionType string
	Status         WorkflowExecutionStatus
	Input          map[string]any
	Output         any
	Error          *WorkflowExecutionError
	CreatedAt      time.Time
	StartedAt      *time.Time
	CompletedAt    *time.Time
	UpdatedAt      time.Time
}

// WorkflowExecutionError describes a workflow execution failure.
type WorkflowExecutionError struct {
	Code         int32
	Message      string
	FailedNodeID string
}

// ToolExecutionStatus is the lifecycle state of a standalone tool execution.
type ToolExecutionStatus string

const (
	ToolExecutionStatusPending   ToolExecutionStatus = "pending"
	ToolExecutionStatusRunning   ToolExecutionStatus = "running"
	ToolExecutionStatusCompleted ToolExecutionStatus = "completed"
	ToolExecutionStatusFailed    ToolExecutionStatus = "failed"
)

// Tool describes a node that can be executed independently.
type Tool struct {
	Schema  NodeType
	Profile *ToolProfile
}

// ToolProfile summarizes recent executions of a tool.
type ToolProfile struct {
	SuccessRate       float64
	AverageDurationMS float64
}

// ToolExecution is the current state and result of a standalone tool execution.
type ToolExecution struct {
	Status               ToolExecutionStatus
	Output               map[string]any
	InternalErrorMessage string
	DurationMS           int64
}

// NodeType describes a supported node and its input/output schema.
type NodeType struct {
	Type         string
	InputSchema  json.RawMessage
	OutputSchema json.RawMessage
}
