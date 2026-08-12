//go:build wireinject
// +build wireinject

// The build tag makes sure the stub is not built in the final build.

package main

import (
	"log/slog"

	"github.com/hobbyGG/workflow/internal/biz"
	"github.com/hobbyGG/workflow/internal/conf"
	"github.com/hobbyGG/workflow/internal/data"
	"github.com/hobbyGG/workflow/internal/server"
	"github.com/hobbyGG/workflow/internal/service"

	"github.com/go-kratos/kratos/v3"
	"github.com/google/wire"
)

// wireApp init kratos application.
func wireApp(*conf.Server, *conf.Data, *slog.Logger) (*kratos.App, func(), error) {
	panic(wire.Build(server.WorkerProviderSet, data.ProviderSet, biz.ProviderSet, service.ProviderSet, newApp))
}
