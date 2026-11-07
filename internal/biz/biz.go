package biz

import "github.com/google/wire"

// ProviderSet 包含 biz 层的依赖注入提供者。
var ProviderSet = wire.NewSet(NewWfExecutionUsecase, NewDefinitionUsecase, NewNodeUsecase, NewToolUsecase, NewWorkerUsecase)
