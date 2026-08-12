package server

import (
	"github.com/google/wire"
)

// ProviderSet contains the servers enabled by the Workflow application.
// NewGRPCServer remains available in grpc.go but is intentionally not wired.
var ProviderSet = wire.NewSet(NewHTTPServer)

// WorkerProviderSet contains the servers enabled by the Worker application.
// NewWorkerGRPCServer remains available in grpc.go but is intentionally not wired.
var WorkerProviderSet = wire.NewSet(NewWorkerHTTPServer, NewTemporalWorker)
