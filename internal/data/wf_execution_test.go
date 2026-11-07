package data

import (
	"context"
	"errors"
	"testing"

	"github.com/hobbyGG/workflow/internal/biz"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
)

type dispatchTemporalClient struct {
	client.Client
	err error
}

func (c *dispatchTemporalClient) ExecuteWorkflow(
	context.Context,
	client.StartWorkflowOptions,
	interface{},
	...interface{},
) (client.WorkflowRun, error) {
	return nil, c.err
}

func TestWfExecutionRepoDispatchWfExecution(t *testing.T) {
	temporalErr := errors.New("temporal unavailable")
	tests := []struct {
		name        string
		ctx         func() context.Context
		dispatchErr error
		wantErr     error
	}{
		{
			name: "dispatches new workflow",
			ctx:  context.Background,
		},
		{
			name:        "reuses existing workflow",
			ctx:         context.Background,
			dispatchErr: serviceerror.NewWorkflowExecutionAlreadyStarted("workflow already started", "", ""),
		},
		{
			name:        "returns temporal failure",
			ctx:         context.Background,
			dispatchErr: temporalErr,
			wantErr:     biz.ErrWfExecutionInternal,
		},
		{
			name: "returns context cancellation",
			ctx: func() context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx
			},
			dispatchErr: serviceerror.NewWorkflowExecutionAlreadyStarted("workflow already started", "", ""),
			wantErr:     context.Canceled,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := &wfExecutionRepo{data: &Data{
				temporalClient: &dispatchTemporalClient{err: tt.dispatchErr},
				taskQueue:      "workflow",
			}}

			err := repo.DispatchWfExecution(tt.ctx(), "workflow-execution-1", biz.DynamicWorkflowParam{})
			if tt.wantErr == nil && err != nil {
				t.Fatalf("DispatchWfExecution() error = %v, want nil", err)
			}
			if tt.wantErr != nil && !errors.Is(err, tt.wantErr) {
				t.Fatalf("DispatchWfExecution() error = %v, want %v", err, tt.wantErr)
			}
		})
	}
}
