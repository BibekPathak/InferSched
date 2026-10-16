# ---- Stage 1: build the Go simulator CLI and coordinator ----
FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY . .
RUN CGO_ENABLED=0 go build -o /out/sim ./simulator/cmd/sim \
 && CGO_ENABLED=0 go build -o /out/infersched ./cmd/infersched

# ---- Stage 2: runtime with Python benchmark tooling ----
FROM python:3.12-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir numpy matplotlib
WORKDIR /app
COPY --from=build /out/sim /app/bin/sim
COPY --from=build /out/infersched /app/bin/infersched
COPY benchmarks ./benchmarks
ENV PATH="/app/bin:${PATH}"
# Default: generate workloads and run the benchmark suite, writing to a mounted
# volume at /app/benchmarks/results.
CMD ["python3", "benchmarks/scripts/run_benchmarks.py", "--out", "benchmarks/results", "--seeds", "1,2,3", "--suite", "profiles,sweep,capacity"]
