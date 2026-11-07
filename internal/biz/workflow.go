package biz

import (
	"maps"
	"slices"
	"time"

	v1 "github.com/hobbyGG/workflow/api/workflow/v1"
	"github.com/hobbyGG/workflow/pkg/queue"

	"github.com/go-kratos/kratos/v3/errors"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// DynamicWorkflowType 是动态工作流注册到 Temporal 的 Workflow Type。
const (
	DynamicWorkflowType      = "DynamicWorkflow"
	ActivityNodeWorkflowType = "StandAloneActivityNode"
)

var (
	// ErrDynamicWorkflowNoStartNode 表示动态工作流没有起始节点。
	ErrDynamicWorkflowNoStartNode = errors.BadRequest(v1.ErrorReason_DYNAMIC_WORKFLOW_INVALID_ARGUMENT.String(), "dynamic workflow has no start node")
	// ErrDynamicWorkflowInvalidDefinition 表示动态工作流定义无效。
	ErrDynamicWorkflowInvalidDefinition = errors.BadRequest(v1.ErrorReason_DYNAMIC_WORKFLOW_INVALID_ARGUMENT.String(), "invalid dynamic workflow definition")
)

// DefinitionGraph 表示由 DAG 引擎执行的工作流定义图。
type DefinitionGraph struct {
	Nodes []DefNode
	Edges []DefEdge
}

// DefNode 表示工作流定义图中的节点。
type DefNode struct {
	ID     string
	Type   NodeType
	Preset map[string]any
}

// DefEdge 表示工作流定义图中的数据依赖或控制流分支。
type DefEdge struct {
	FromNode   string
	FromBranch string // Router 分支 ID，非 Router 出边为空
	FromKey    string // 源数据 map 的 key
	ToNode     string
	ToKey      string // 目标节点 inputValues map 的 key
}

type runtimeGraph struct {
	nodesByID map[string]*runtimeNode
}

type nodeStatus uint8

const (
	nodeStatusPending nodeStatus = iota
	nodeStatusRunning
	nodeStatusSuccess
	nodeStatusFailed
	nodeStatusSkipped
)

// runtimeNode 承载运行时状态，类似 nodeState，具体功能由 Node interface 实现
type runtimeNode struct {
	id   string
	node Node

	status nodeStatus
	input  map[string]any
	output map[string]any
	err    error

	inEdges          []*runtimeEdge
	outEdges         []*runtimeEdge
	branches         []*runtimeBranch // 仅 Router 节点使用
	remainingInEdges int              // 防止每次检查都要遍历 edges
}

type runtimeBranch struct {
	id       string
	outEdges []*runtimeEdge
}

// Inputs 获取该节点输入值的副本
func (n *runtimeNode) Inputs() map[string]any {
	return maps.Clone(n.input)
}

// Receive 只接收来自 Edge 的数据，不会校验 Edge 的合法性
func (n *runtimeNode) Receive(edge *runtimeEdge) {
	if edge.toKey != "" {
		n.input[edge.toKey] = edge.data
	}
}

func (n *runtimeNode) Start() {
	n.status = nodeStatusRunning
}

func (n *runtimeNode) Skip() {
	n.status = nodeStatusSkipped
}

// Complete 接收节点执行完成的结果，设置节点状态与输出值
func (n *runtimeNode) Complete(output map[string]any, err error) {
	n.output = output
	n.err = err
	if err != nil {
		n.status = nodeStatusFailed
		return
	}
	n.status = nodeStatusSuccess
}

// Result 返回节点执行完成的结果，包含输出值与错误信息
func (n *runtimeNode) Result() (map[string]any, error) {
	return maps.Clone(n.output), n.err
}

type edgeStatus uint8

const (
	edgeStatusPending edgeStatus = iota
	edgeStatusActive
	edgeStatusSkipped
)

// runtimeEdge 表示工作流执行期间的运行时边。
type runtimeEdge struct {
	target  *runtimeNode
	fromKey string
	toKey   string
	status  edgeStatus
	data    any
}

func (uc *WorkerUsecase) newRuntimeGraph(dg DefinitionGraph) (*runtimeGraph, error) {
	if err := validateDefinitionGraph(dg); err != nil {
		return nil, ErrDynamicWorkflowInvalidDefinition
	}

	rg := &runtimeGraph{
		nodesByID: make(map[string]*runtimeNode, len(dg.Nodes)),
	}

	for _, defNode := range dg.Nodes {
		node, err := uc.newNode(defNode)
		if err != nil {
			return nil, err
		}
		input := make(map[string]any, len(defNode.Preset))
		if _, isRouter := node.(RouterNode); !isRouter {
			// 非 router 才需要初始化 input，router 已经将 preset 作为分支条件存储在自身结构体中
			maps.Copy(input, defNode.Preset)
		}
		rg.nodesByID[defNode.ID] = &runtimeNode{
			id:     defNode.ID,
			node:   node,
			status: nodeStatusPending,
			input:  input,
		}
	}

	for _, defEdge := range dg.Edges {
		from, fromExists := rg.nodesByID[defEdge.FromNode]
		to, toExists := rg.nodesByID[defEdge.ToNode]
		if !fromExists || !toExists {
			return nil, ErrDynamicWorkflowInvalidDefinition
		}

		_, isFromRouter := from.node.(RouterNode)
		if isFromRouter && defEdge.FromBranch == "" {
			return nil, ErrDynamicWorkflowInvalidDefinition
		}
		if !isFromRouter && defEdge.FromBranch != "" {
			return nil, ErrDynamicWorkflowInvalidDefinition
		}

		edge := &runtimeEdge{
			target:  to,
			fromKey: defEdge.FromKey,
			toKey:   defEdge.ToKey,
		}

		if !isFromRouter {
			from.outEdges = append(from.outEdges, edge)
		} else {
			var branch *runtimeBranch
			for _, candidate := range from.branches {
				if candidate.id == defEdge.FromBranch {
					branch = candidate
					break
				}
			}
			if branch == nil {
				branch = &runtimeBranch{id: defEdge.FromBranch}
				from.branches = append(from.branches, branch)
			}
			branch.outEdges = append(branch.outEdges, edge)
		}

		to.inEdges = append(to.inEdges, edge)
		to.remainingInEdges++
	}

	return rg, nil
}

func (uc *WorkerUsecase) newNode(defNode DefNode) (Node, error) {
	switch defNode.Type {
	case NodeTypeOutput:
		return &outputNode{}, nil
	case NodeTypeExclusive, NodeTypeInclusive:
		return newRouterNode(defNode.Type, defNode.Preset)
	default:
		node := uc.nodeRepo.GetActivityNode(defNode.Type)
		if node == nil {
			return nil, ErrDynamicWorkflowInvalidDefinition
		}
		return node, nil
	}
}

type DynamicWorkflowParam struct {
	Definition DefinitionGraph
}

type DynamicWorkflowResult struct {
	Outputs map[string]any
}

func statusActivityOptions() workflow.ActivityOptions {
	return workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 3,
		},
	}
}

func (uc *WorkerUsecase) DynamicWorkflow(ctx workflow.Context, param DynamicWorkflowParam) (result *DynamicWorkflowResult, retErr error) {
	ctx = workflow.WithActivityOptions(ctx, statusActivityOptions())
	workflowExecutionID := workflow.GetInfo(ctx).WorkflowExecution.ID
	var failedNodeID string
	var failedNodeType NodeType
	defer func() {
		if retErr == nil || temporal.IsCanceledError(retErr) {
			return
		}
		fields := []any{"workflow_execution_id", workflowExecutionID, "error", retErr}
		if failedNodeID != "" {
			fields = append(fields, "node_id", failedNodeID, "node_type", failedNodeType)
		}
		workflow.GetLogger(ctx).Error("dynamic workflow failed", fields...)
	}()
	defer syncWfExecutionStatus(ctx, workflowExecutionID, &failedNodeID, &result, &retErr)
	if err := workflow.ExecuteActivity(ctx, UpdateWfExecutionStatusActivityType, UpdateWfExecutionStatusParam{
		WorkflowExecutionID: workflowExecutionID,
		Status:              WfExecutionStatusRunning,
	}).Get(ctx, nil); err != nil {
		return nil, err
	}

	// init
	rg, err := uc.newRuntimeGraph(param.Definition)
	if err != nil {
		return nil, err
	}
	// ready 中的节点已解析全部入边，出队后再判断执行或跳过。
	ready := make(queue.Queue[string], 0, len(param.Definition.Nodes))
	completed := workflow.NewBufferedChannel(ctx, len(rg.nodesByID))
	runningCount := 0
	result = &DynamicWorkflowResult{Outputs: make(map[string]any)}

	for _, nodeID := range workflow.DeterministicKeys(rg.nodesByID) {
		if rg.nodesByID[nodeID].remainingInEdges == 0 {
			ready.Enqueue(nodeID)
		}
	}
	if ready.Empty() {
		return nil, ErrDynamicWorkflowNoStartNode
	}

	enqueueIfReady := func(node *runtimeNode) {
		node.remainingInEdges--
		if node.remainingInEdges == 0 {
			ready.Enqueue(node.id)
		}
	}

	for !ready.Empty() || runningCount > 0 {
		for !ready.Empty() {
			currNodeID := ready.Dequeue()
			node := rg.nodesByID[currNodeID]

			shouldSkip := len(node.inEdges) > 0
			for _, edge := range node.inEdges {
				if edge.status == edgeStatusActive {
					shouldSkip = false
					break
				}
			}
			if shouldSkip {
				node.Skip()
				for _, edge := range node.outEdges {
					edge.status = edgeStatusSkipped
					enqueueIfReady(edge.target)
				}
				for _, branch := range node.branches {
					for _, edge := range branch.outEdges {
						edge.status = edgeStatusSkipped
						enqueueIfReady(edge.target)
					}
				}
				continue
			}

			inputs := node.Inputs()

			switch node.node.Type() {
			case NodeTypeOutput:
				// 到终点，收集输出结果
				output := inputs
				node.Complete(output, nil)
				result.Outputs = output
			case NodeTypeExclusive, NodeTypeInclusive:
				router, ok := node.node.(RouterNode)
				if !ok {
					return nil, ErrDynamicWorkflowInvalidDefinition
				}
				node.Start()
				selectedBranchIDs, err := router.Route(inputs)
				if err != nil {
					node.Complete(nil, err)
					failedNodeID = node.id
					failedNodeType = node.node.Type()
					return nil, err
				}
				node.Complete(inputs, nil)

				for _, branch := range node.branches {
					selected := slices.Contains(selectedBranchIDs, branch.id)
					for _, edge := range branch.outEdges {
						if !selected {
							edge.status = edgeStatusSkipped
							enqueueIfReady(edge.target)
							continue
						}

						// Router 不产生新数据，选中的分支继续传递其输入。
						if edge.toKey != "" {
							value, inputExists := inputs[edge.fromKey]
							if !inputExists {
								return nil, ErrDynamicWorkflowInvalidDefinition
							}
							edge.data = value
						}
						edge.status = edgeStatusActive
						edge.target.Receive(edge)
						enqueueIfReady(edge.target)
					}
				}
			default:
				activityNode, ok := node.node.(ActivityNode)
				if !ok {
					return nil, ErrDynamicWorkflowInvalidDefinition
				}
				activityType := activityNode.Type()
				retryPolicy := activityNode.RetryPolicy()
				activityCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
					StartToCloseTimeout: 30 * time.Minute,
					RetryPolicy:         &retryPolicy,
				})
				future := workflow.ExecuteActivity(activityCtx, activityType.String(), inputs)
				node.Start()

				workflow.Go(ctx, func(ctx workflow.Context) {
					var output map[string]any
					err := future.Get(ctx, &output)
					node.Complete(output, err)
					completed.Send(ctx, currNodeID)
				})
				runningCount++
			}
		}

		// 当前没有运行中的 activity，继续处理同步节点产生的新队列。
		if runningCount == 0 {
			continue
		}

		var completedNodeID string
		for received := completed.Receive(ctx, &completedNodeID); received; received = completed.ReceiveAsync(&completedNodeID) {
			runningCount--

			// 检查该节点执行完成的状态，如果 error 直接返回，整个 workflow 将会被标记为失败
			node := rg.nodesByID[completedNodeID]
			output, err := node.Result()
			if err != nil {
				failedNodeID = node.id
				failedNodeType = node.node.Type()
				return nil, err
			}

			for _, edge := range node.outEdges {
				if edge.toKey != "" {
					value, outputExists := output[edge.fromKey]
					if !outputExists {
						return nil, ErrDynamicWorkflowInvalidDefinition
					}
					edge.data = value
				}
				edge.status = edgeStatusActive
				edge.target.Receive(edge)
				enqueueIfReady(edge.target)
			}
		}
	}

	return result, nil
}

func syncWfExecutionStatus(ctx workflow.Context, workflowExecutionID string, failedNodeID *string, result **DynamicWorkflowResult, retErr *error) {
	canceled := temporal.IsCanceledError(*retErr)
	param := UpdateWfExecutionStatusParam{WorkflowExecutionID: workflowExecutionID}
	switch {
	case canceled:
		param.Status = WfExecutionStatusCanceled
	case *retErr != nil:
		message := (*retErr).Error()
		param.Status = WfExecutionStatusFailed
		param.ErrorInfo = &WfExecutionErrorInfo{
			InternalErrorMessage: &message,
		}
		if failedNodeID != nil && *failedNodeID != "" {
			param.ErrorInfo.FailedNodeID = failedNodeID
		}
	default:
		param.Status = WfExecutionStatusCompleted
		if *result != nil {
			param.Output = (*result).Outputs
		}
	}

	statusCtx := ctx
	if temporal.IsCanceledError(ctx.Err()) {
		statusCtx, _ = workflow.NewDisconnectedContext(ctx)
	}
	if err := workflow.ExecuteActivity(statusCtx, UpdateWfExecutionStatusActivityType, param).Get(statusCtx, nil); err != nil {
		if *retErr == nil {
			*result = nil
			*retErr = err
			return
		}
		workflow.GetLogger(statusCtx).Error(
			"update workflow execution status failed",
			"workflow_execution_id", workflowExecutionID,
			"status", param.Status,
			"error", err,
		)
	}
}

// StandAloneNodeWorkflow 执行一个已创建 NodeExecution 对应的 ActivityNode。
func (uc *WorkerUsecase) StandAloneNodeWorkflow(ctx workflow.Context, param ActivityNodeWorkflowParam) (result map[string]any, retErr error) {
	ctx = workflow.WithActivityOptions(ctx, statusActivityOptions())

	node := uc.nodeRepo.GetActivityNode(param.NodeType)
	if node == nil {
		return nil, ErrToolInvalidArgument
	}

	defer syncNodeExecutionStatus(ctx, param.NodeExecutionID, &result, &retErr)

	if err := workflow.ExecuteActivity(ctx, MarkNodeExecutionRunningActivityType, param.NodeExecutionID).Get(ctx, nil); err != nil {
		return nil, err
	}

	nodeType := node.Type()
	retryPolicy := node.RetryPolicy()
	activityCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Minute,
		RetryPolicy:         &retryPolicy,
	})
	retErr = workflow.ExecuteActivity(activityCtx, nodeType.String(), param.Input).Get(ctx, &result)
	return result, retErr
}

func syncNodeExecutionStatus(ctx workflow.Context, nodeExecutionID string, result *map[string]any, retErr *error) {
	param := FinishNodeExecutionParam{NodeExecutionID: nodeExecutionID}
	if *retErr == nil {
		param.Status = NodeExecutionStatusCompleted
		param.Output = *result
	} else {
		message := (*retErr).Error()
		param.Status = NodeExecutionStatusFailed
		param.InternalErrorMessage = &message
	}

	statusCtx := ctx
	if temporal.IsCanceledError(ctx.Err()) {
		statusCtx, _ = workflow.NewDisconnectedContext(ctx)
	}
	if err := workflow.ExecuteActivity(statusCtx, FinishNodeExecutionActivityType, param).Get(statusCtx, nil); err != nil {
		if *retErr == nil {
			*result = nil
			*retErr = err
			return
		}
		workflow.GetLogger(statusCtx).Error("finish node execution failed", "node_execution_id", nodeExecutionID, "status", param.Status, "error", err)
	}
}
