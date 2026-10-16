GO       ?= go
CARGO    ?= cargo
PYTHON   ?= python3
PKG      := github.com/BibekP/infersched
BIN      := bin/infersched

.PHONY: all build test test-race vet fmt lint clean run sim bench help

all: fmt vet test build

build:
	$(GO) build -o $(BIN) ./cmd/infersched

test:
	$(GO) test ./...

test-race:
	$(GO) test -race ./...

vet:
	$(GO) vet ./...

fmt:
	gofmt -w internal cmd

lint: vet
	@command -v staticcheck >/dev/null 2>&1 && staticcheck ./... || echo "staticcheck not installed; skipping"

run: build
	$(BIN)

sim:
	$(GO) run ./simulator/cmd/sim -trace benchmarks/workloads/sample-short-interactive.jsonl

bench:
	$(PYTHON) benchmarks/scripts/run_benchmarks.py

clean:
	rm -rf bin

help:
	@echo "make build       - build the coordinator binary"
	@echo "make test        - run Go unit tests"
	@echo "make test-race   - run tests with the race detector"
	@echo "make fmt/vet     - format and vet"
	@echo "make run         - build and run the coordinator"
