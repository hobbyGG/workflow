package main

import (
	"flag"
	stdslog "log/slog"
	"os"

	"github.com/hobbyGG/workflow/internal/conf"
	"github.com/hobbyGG/workflow/internal/server"
	"github.com/hobbyGG/workflow/pkg/slog"

	"github.com/go-kratos/kratos/v3"
	"github.com/go-kratos/kratos/v3/config"
	"github.com/go-kratos/kratos/v3/config/file"
	"github.com/go-kratos/kratos/v3/transport/http"

	_ "go.uber.org/automaxprocs"
)

// go build -ldflags "-X main.Version=x.y.z"
var (
	// Name is the name of the compiled software.
	Name = "workflow"
	// Version is the version of the compiled software.
	Version string
	// flagconf is the config flag.
	flagconf string

	id, _ = os.Hostname()
)

func init() {
	flag.StringVar(&flagconf, "conf", "../../configs", "config path, eg: -conf config.yaml")
}

func newApp(logger *stdslog.Logger, httpServer *http.Server, temporalWorker *server.TemporalWorker) *kratos.App {
	return kratos.New(
		kratos.ID(id),
		kratos.Name(Name),
		kratos.Version(Version),
		kratos.Metadata(map[string]string{}),
		kratos.Logger(logger),
		kratos.Server(
			httpServer,
			temporalWorker,
		),
	)
}

func main() {
	flag.Parse()
	logger := slog.Init(id, Name, Version)
	if err := run(logger); err != nil {
		os.Exit(1)
	}
}

func run(logger *stdslog.Logger) error {
	c := config.New(config.WithSource(file.NewSource(flagconf)))
	defer c.Close()

	if err := c.Load(); err != nil {
		logger.Error("application initialization failed", "stage", "config_load", "error", err)
		return err
	}

	var bc conf.Bootstrap
	if err := c.Scan(&bc); err != nil {
		logger.Error("application initialization failed", "stage", "config_scan", "error", err)
		return err
	}

	app, cleanup, err := wireApp(bc.Server, bc.Data, logger)
	if err != nil {
		logger.Error("application initialization failed", "stage", "dependency_init", "error", err)
		return err
	}
	defer cleanup()

	// start and wait for stop signal
	if err := app.Run(); err != nil {
		logger.Error("application exited", "error", err)
		return err
	}
	return nil
}
