GO       ?= go
CARGO    ?= cargo
PYTHON   ?= python3
PKG      := github.com/BibekP/infersched
BIN      := bin/infersched

.PHONY: all build test test-race vet fmt lint clean run sim bench bench-quick plots gen-workloads help

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

# Full reproducible benchmark sweep (all profiles, utilization sweep, capacity
# sensitivity) across three seeds, plus plots.
bench:
	$(PYTHON) benchmarks/scripts/run_benchmarks.py --out benchmarks/results \
		--seeds 1,2,3 --suite profiles,sweep,capacity
	$(PYTHON) benchmarks/scripts/plot_results.py benchmarks/results

bench-quick:
	$(PYTHON) benchmarks/scripts/run_benchmarks.py --out benchmarks/results --quick

plots:
	$(PYTHON) benchmarks/scripts/plot_results.py benchmarks/results

gen-workloads:
	@mkdir -p benchmarks/workloads
	@for p in A B C D E F G; do \
		$(PYTHON) benchmarks/scripts/workload_gen.py --profile $$p --seed 1 \
			--out benchmarks/workloads/profile-$$p-seed1.jsonl; \
	done

clean:
	rm -rf bin

help:
	@echo "make build       - build the coordinator binary"
	@echo "make test        - run Go unit tests"
	@echo "make test-race   - run tests with the race detector"
	@echo "make fmt/vet     - format and vet"
	@echo "make run         - build and run the coordinator"
	@echo "make sim         - replay the sample trace through the simulator"
	@echo "make bench       - full benchmark suite + plots (SIMULATION output)"
	@echo "make bench-quick - quick single-seed benchmark"
	@echo "make gen-workloads - generate profiles A-G at seed 1"
