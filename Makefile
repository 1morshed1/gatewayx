# GatewayX Makefile
#
# The Go toolchain on this machine lives in /usr/local/go/bin but is not on the
# default PATH, so we prepend it here. This makes every `make` target work in a
# fresh shell without the developer having to fix their PATH first.
export PATH := /usr/local/go/bin:$(HOME)/go/bin:$(PATH)

GO       ?= go
BIN_DIR  := bin
PKG      := ./...

.DEFAULT_GOAL := help

## help: show this help
.PHONY: help
help:
	@grep -E '^## ' $(MAKEFILE_LIST) | sed 's/## //' | awk -F': ' '{printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

## tidy: sync go.mod/go.sum
.PHONY: tidy
tidy:
	$(GO) mod tidy

## build: compile the gateway and echo binaries into ./bin
.PHONY: build
build:
	@mkdir -p $(BIN_DIR)
	$(GO) build -o $(BIN_DIR)/gateway ./cmd/gateway
	$(GO) build -o $(BIN_DIR)/echo ./cmd/echo

## run: run the gateway locally (needs infra-up first)
.PHONY: run
run:
	$(GO) run ./cmd/gateway

## test: run all unit tests
.PHONY: test
test:
	$(GO) test -race -count=1 $(PKG)

## cover: run tests with coverage report
.PHONY: cover
cover:
	$(GO) test -coverprofile=coverage.out $(PKG)
	$(GO) tool cover -func=coverage.out | tail -1

## vet: run go vet
.PHONY: vet
vet:
	$(GO) vet $(PKG)

## fmt: format all Go code
.PHONY: fmt
fmt:
	$(GO) fmt $(PKG)

## infra-up: start postgres, redis, kafka, demo upstreams
.PHONY: infra-up
infra-up:
	docker compose -f deploy/docker-compose.yml up -d

## infra-down: stop and remove the local stack
.PHONY: infra-down
infra-down:
	docker compose -f deploy/docker-compose.yml down

## infra-logs: tail the local stack logs
.PHONY: infra-logs
infra-logs:
	docker compose -f deploy/docker-compose.yml logs -f

## migrate: apply database migrations
.PHONY: migrate
migrate:
	$(GO) run ./cmd/gateway migrate

## seed: load example routes/keys for a quick demo
.PHONY: seed
seed:
	$(GO) run ./cmd/gateway seed

## loadtest: run the k6 throughput scenario (needs k6 installed)
.PHONY: loadtest
loadtest:
	k6 run test/k6/throughput.js
