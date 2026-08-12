GOHOSTOS:=$(shell go env GOHOSTOS)
GOPATH:=$(shell go env GOPATH)
VERSION=$(shell git describe --tags --always)

.PHONY: init
# init env
init:
	GOWORK=off go install github.com/sqlc-dev/sqlc/cmd/sqlc@latest
	GOWORK=off go install github.com/go-kratos/kratos/cmd/kratos/v3@latest
	GOWORK=off go install github.com/google/wire/cmd/wire@latest
	GOWORK=off go install github.com/bufbuild/buf/cmd/buf@latest

.PHONY: config
# generate internal proto
config:
	GOWORK=off buf generate --template buf.gen.config.yaml

.PHONY: api
# generate api proto
api:
	GOWORK=off buf generate --template buf.gen.yaml

.PHONY: sqlc
# generate database access code
sqlc:
	sqlc generate

.PHONY: build
# build
build:
	mkdir -p bin/
	GOWORK=off go build -ldflags "-X main.Version=$(VERSION)" -o ./bin/workflow ./cmd/workflow
	GOWORK=off go build -ldflags "-X main.Version=$(VERSION)" -o ./bin/worker ./cmd/worker

.PHONY: generate
# generate
generate:
	GOWORK=off go generate ./...
	GOWORK=off go mod tidy

.PHONY: all
# generate all
all:
	make api
	make config
	make generate

# show help
help:
	@echo ''
	@echo 'Usage:'
	@echo ' make [target]'
	@echo ''
	@echo 'Targets:'
	@awk '/^[a-zA-Z\-\_0-9]+:/ { \
	helpMessage = match(lastLine, /^# (.*)/); \
		if (helpMessage) { \
			helpCommand = substr($$1, 0, index($$1, ":")); \
			helpMessage = substr(lastLine, RSTART + 2, RLENGTH); \
			printf "\033[36m%-22s\033[0m %s\n", helpCommand,helpMessage; \
		} \
	} \
	{ lastLine = $$0 }' $(MAKEFILE_LIST)

.DEFAULT_GOAL := help
