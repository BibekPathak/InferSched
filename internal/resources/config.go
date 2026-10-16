// Package resources implements InferSched's compute and KV-cache accounting.
//
// The manager models a set of simulated inference workers. It tracks execution
// slots, active sequences, and KV-cache token blocks under fixed capacity.
// Every reservation is atomic: either all of a request's resources are reserved
// or none are. Reservations are keyed by request ID and released exactly once.
//
// This is a model of resource consumption, not a claim that GPU memory has been
// allocated. All figures are estimates derived from request metadata.
package resources

import "fmt"

// Config describes the fixed capacity of the simulated resource pool.
type Config struct {
	// Workers is the number of simulated workers. Each worker contributes
	// MaxBatchSize execution slots and a share of KV-cache capacity.
	Workers int
	// MaxBatchSize is the maximum number of concurrently running sequences per
	// worker (the continuous-batching batch size per worker).
	MaxBatchSize int
	// KVBlockSize is the KV-cache block size in tokens. Blocks are the unit of
	// KV allocation: blocks = ceil(tokens / KVBlockSize).
	KVBlockSize int
	// KVBlocksPerWorker is the number of KV blocks available on each worker.
	KVBlocksPerWorker int
}

// Validate checks the configuration for obvious errors.
func (c Config) Validate() error {
	if c.Workers <= 0 {
		return fmt.Errorf("resources: Workers must be > 0, got %d", c.Workers)
	}
	if c.MaxBatchSize <= 0 {
		return fmt.Errorf("resources: MaxBatchSize must be > 0, got %d", c.MaxBatchSize)
	}
	if c.KVBlockSize <= 0 {
		return fmt.Errorf("resources: KVBlockSize must be > 0, got %d", c.KVBlockSize)
	}
	if c.KVBlocksPerWorker <= 0 {
		return fmt.Errorf("resources: KVBlocksPerWorker must be > 0, got %d", c.KVBlocksPerWorker)
	}
	return nil
}

// totalSlots is the pool-wide execution-slot capacity.
func (c Config) totalSlots() int { return c.Workers * c.MaxBatchSize }

// totalKVBlocks is the pool-wide KV block capacity.
func (c Config) totalKVBlocks() int { return c.Workers * c.KVBlocksPerWorker }

// BlocksForTokens returns the number of KV blocks needed to cache tokens using
// ceiling division: ceil(tokens / KVBlockSize). tokens <= 0 maps to 0 blocks.
func (c Config) BlocksForTokens(tokens int) int {
	if tokens <= 0 {
		return 0
	}
	return (tokens + c.KVBlockSize - 1) / c.KVBlockSize
}

// Snapshot is an immutable view of current resource usage, safe to emit as
// metrics. All counters are for the whole pool unless named per-worker.
type Snapshot struct {
	SlotsTotal       int `json:"slots_total"`
	SlotsUsed        int `json:"slots_used"`
	SlotsAvailable   int `json:"slots_available"`
	SeqTotal         int `json:"seq_total"`
	ActiveSequences  int `json:"active_sequences"`
	KVBlocksTotal    int `json:"kv_blocks_total"`
	KVBlocksUsed     int `json:"kv_blocks_used"`
	KVBlocksReserved int `json:"kv_blocks_reserved"`
	KVBlocksFree     int `json:"kv_blocks_free"`
	Reservations     int `json:"reservations"`
}
