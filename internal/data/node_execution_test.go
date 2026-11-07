package data

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/hobbyGG/workflow/internal/biz"
	"github.com/hobbyGG/workflow/internal/data/db/sqlcgen"

	"github.com/jackc/pgx/v5/pgtype"
)

func TestNodeExecutionInfos(t *testing.T) {
	pendingOnlyNodeType := biz.NodeType("PENDING_ONLY")
	rows := []sqlcgen.ListNodeExecutionsSinceRow{
		{NodeType: string(biz.NodeType("ACTIVITY_C")), Status: string(biz.NodeExecutionStatusCompleted), DurationMs: pgtype.Int8{Int64: 100, Valid: true}},
		{NodeType: string(biz.NodeType("ACTIVITY_C")), Status: string(biz.NodeExecutionStatusFailed), DurationMs: pgtype.Int8{Int64: 300, Valid: true}},
		{NodeType: string(biz.NodeType("ACTIVITY_C")), Status: string(biz.NodeExecutionStatusRunning), DurationMs: pgtype.Int8{Int64: 1000, Valid: true}},
		{NodeType: string(biz.NodeType("ACTIVITY_A")), Status: string(biz.NodeExecutionStatusCompleted)},
		{NodeType: string(biz.NodeType("ACTIVITY_A")), Status: string(biz.NodeExecutionStatusFailed), DurationMs: pgtype.Int8{Int64: 400, Valid: true}},
		{NodeType: string(pendingOnlyNodeType), Status: string(biz.NodeExecutionStatusPending)},
	}

	want := map[biz.NodeType]biz.NodeExecutionInfo{
		biz.NodeType("ACTIVITY_C"): {AvgDurationsMS: 200, SuccessRatio: 0.5},
		biz.NodeType("ACTIVITY_A"): {AvgDurationsMS: 400, SuccessRatio: 0.5},
	}
	if got := toBiznodeExecutionInfos(rows); !reflect.DeepEqual(got, want) {
		t.Fatalf("nodeExecutionInfos() = %#v, want %#v", got, want)
	}
}

func TestNodeExecutionConversion(t *testing.T) {
	workflowExecutionID := "workflow-execution-1"
	nodeID := "node-1"
	internalErrorMessage := "model failed"
	durationMS := int64(1250)
	createdAt := time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)
	execution := &biz.NodeExecution{
		ID:                   "node-execution-1",
		WorkflowExecutionID:  &workflowExecutionID,
		IdempotencyKey:       "request-1",
		NodeID:               &nodeID,
		NodeType:             biz.NodeType("ACTIVITY_C"),
		Input:                map[string]any{"text": "hello"},
		Status:               biz.NodeExecutionStatusFailed,
		Output:               map[string]any{"task_id": "task-1"},
		InternalErrorMessage: &internalErrorMessage,
		CreatedAt:            createdAt,
		DurationMS:           &durationMS,
	}

	params, err := newNodeExecution(execution)
	if err != nil {
		t.Fatalf("newNodeExecution() error = %v", err)
	}
	if !params.WorkflowExecutionID.Valid || params.WorkflowExecutionID.String != workflowExecutionID ||
		!params.NodeID.Valid || params.NodeID.String != nodeID {
		t.Fatalf("newNodeExecution() optional IDs = (%#v, %#v)", params.WorkflowExecutionID, params.NodeID)
	}

	got, err := toBizNodeExecution(sqlcgen.NodeExecution{
		NodeExecutionID:      execution.ID,
		WorkflowExecutionID:  params.WorkflowExecutionID,
		IdempotencyKey:       execution.IdempotencyKey,
		NodeID:               params.NodeID,
		NodeType:             params.NodeType,
		Input:                params.Input,
		Status:               string(execution.Status),
		Output:               []byte(`{"task_id":"task-1"}`),
		InternalErrorMessage: pgtype.Text{String: internalErrorMessage, Valid: true},
		CreatedAt:            pgtype.Timestamptz{Time: createdAt, Valid: true},
		DurationMs:           pgtype.Int8{Int64: durationMS, Valid: true},
	})
	if err != nil {
		t.Fatalf("toBizNodeExecution() error = %v", err)
	}
	if !reflect.DeepEqual(got, execution) {
		t.Fatalf("toBizNodeExecution() = %#v, want %#v", got, execution)
	}
}

func TestNodeExecutionConversionRejectsInvalidJSON(t *testing.T) {
	_, err := newNodeExecution(&biz.NodeExecution{Input: map[string]any{"invalid": make(chan struct{})}})
	if !errors.Is(err, biz.ErrToolInvalidArgument) {
		t.Fatalf("newNodeExecution() error = %v, want %v", err, biz.ErrToolInvalidArgument)
	}

	_, err = toBizNodeExecution(sqlcgen.NodeExecution{Input: []byte("not-json")})
	if !errors.Is(err, biz.ErrToolInternal) {
		t.Fatalf("toBizNodeExecution() error = %v, want %v", err, biz.ErrToolInternal)
	}
}
