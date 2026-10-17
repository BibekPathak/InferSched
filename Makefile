GO       ?= go
CARGO    ?= cargo
PYTHON   ?= python3
PKG      := github.com/BibekP/infersched
BIN      := bin/infersched

.PHONY: all build build-rust test test-race vet fmt lint clean run serve sim bench bench-quick plots gen-workloads rust-core rust-test rust-equivalence help

all: fmt vet test build

build:
	$(GO) build -o $(BIN) ./cmd/infersched

# Build the coordinator with the Rust scheduling core linked in (requires a Rust
# toolchain). The default `build` target does not need Rust.
build-rust:
	$(CARGO) build --manifest-path crates/scheduler-core/Cargo.toml --release
	CGO_ENABLED=1 $(GO) build -tags rustcore -o $(BIN) ./cmd/infersched

# Run the Rust scheduler-core unit tests.
rust-test:
	$(CARGO) test --manifest-path crates/scheduler-core/Cargo.toml

# Build only the Rust static library.
rust-core:
	$(CARGO) build --manifest-path crates/scheduler-core/Cargo.toml --release

# Run the Go <-> Rust equivalence tests.
rust-equivalence: rust-core
	CGO_ENABLED=1 $(GO) test -tags rustcore ./internal/scheduler/ -run TestRust -v

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

# Run the HTTP runtime backed by the mock execution backend.
serve: build
	$(BIN) -serve -addr :8080 -policy fifo

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
	$(CARGO) clean --manifest-path crates/scheduler-core/Cargo.toml 2>/dev/null || true

help:
	@echo "make build       - build the coordinator binary"
	@echo "make test        - run Go unit tests"
	@echo "make test-race   - run tests with the race detector"
	@echo "make fmt/vet     - format and vet"
	@echo "make run         - build and run the coordinator"
	@echo "make serve       - run the HTTP runtime on :8080 (mock backend)"
	@echo "make sim         - replay the sample trace through the simulator"
	@echo "make bench       - full benchmark suite + plots (SIMULATION output)"
	@echo "make bench-quick - quick single-seed benchmark"
	@echo "make gen-workloads - generate profiles A-G at seed 1"
	@echo "make rust-test   - run the Rust scheduler-core unit tests"
	@echo "make build-rust  - build the coordinator with the Rust core linked"
	@echo "make rust-equivalence - Go vs Rust ordering equivalence tests"
