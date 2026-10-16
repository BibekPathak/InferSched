package resources

import (
	"errors"
	"fmt"
	"sync"
)

// ErrInsufficientCapacity is returned when a reservation cannot fit in the
// remaining capacity. The manager is left unchanged when this occurs.
var ErrInsufficientCapacity = errors.New("resources: insufficient capacity")

// ErrUnknownReservation is returned when releasing a request that holds no
// reservation (already released, never reserved, or double release).
var ErrUnknownReservation = errors.New("resources: unknown reservation")

// Reservation records the resources held by a single admitted request.
type Reservation struct {
	RequestID string
	Slots     int
	Sequences int
	KVBlocks  int
	KVTokens  int
}

// Manager is a concurrency-safe resource pool for simulated workers. A single
// mutex guards all counters; reservations are all-or-nothing and released
// exactly once.
type Manager struct {
	cfg Config

	mu        sync.Mutex
	slotsUsed int
	seqActive int
	kvUsed    int
	byRequest map[string]Reservation
}

// New constructs a Manager from cfg. It panics on an invalid configuration:
// capacity limits are a programmer error, not a runtime condition.
func New(cfg Config) *Manager {
	if err := cfg.Validate(); err != nil {
		panic(err)
	}
	return &Manager{
		cfg:       cfg,
		byRequest: make(map[string]Reservation),
	}
}

// Config returns the manager's configuration.
func (m *Manager) Config() Config { return m.cfg }

// Need describes the resources a request requires. Slots and Sequences are
// usually both 1 per request; KVBlocks is derived from KVTokens.
type Need struct {
	RequestID string
	Slots     int
	Sequences int
	KVTokens  int
}

// Reserve atomically reserves the requested resources for need.RequestID.
//
// Semantics:
//   - If the request already holds a reservation, Reserve is a no-op returning
//     the existing reservation and nil (idempotent admission).
//   - If any dimension would exceed capacity, nothing is reserved and
//     ErrInsufficientCapacity is returned. No partial reservation is left
//     behind, so a failed admission rolls back cleanly by construction.
func (m *Manager) Reserve(need Need) (Reservation, error) {
	if need.RequestID == "" {
		return Reservation{}, fmt.Errorf("resources: empty request id")
	}
	slots := need.Slots
	if slots <= 0 {
		slots = 1
	}
	seqs := need.Sequences
	if seqs <= 0 {
		seqs = 1
	}
	blocks := m.cfg.BlocksForTokens(need.KVTokens)

	m.mu.Lock()
	defer m.mu.Unlock()

	if existing, ok := m.byRequest[need.RequestID]; ok {
		return existing, nil
	}
	if m.slotsUsed+slots > m.cfg.totalSlots() ||
		m.seqActive+seqs > m.cfg.totalSlots() ||
		m.kvUsed+blocks > m.cfg.totalKVBlocks() {
		return Reservation{}, ErrInsufficientCapacity
	}

	res := Reservation{
		RequestID: need.RequestID,
		Slots:     slots,
		Sequences: seqs,
		KVBlocks:  blocks,
		KVTokens:  need.KVTokens,
	}
	m.byRequest[need.RequestID] = res
	m.slotsUsed += slots
	m.seqActive += seqs
	m.kvUsed += blocks
	return res, nil
}

// Release frees the reservation held by requestID and returns it. Releasing a
// request that holds no reservation returns ErrUnknownReservation and leaves
// the manager unchanged, which makes double release detectable and harmless.
func (m *Manager) Release(requestID string) (Reservation, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	res, ok := m.byRequest[requestID]
	if !ok {
		return Reservation{}, ErrUnknownReservation
	}
	delete(m.byRequest, requestID)
	m.slotsUsed -= res.Slots
	m.seqActive -= res.Sequences
	m.kvUsed -= res.KVBlocks

	// Defensive invariants: under correct usage these never trip.
	if m.slotsUsed < 0 || m.seqActive < 0 || m.kvUsed < 0 {
		panic("resources: negative usage after release (accounting bug)")
	}
	return res, nil
}

// Holds reports whether requestID currently holds a reservation.
func (m *Manager) Holds(requestID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.byRequest[requestID]
	return ok
}

// Snapshot returns an immutable view of current usage.
func (m *Manager) Snapshot() Snapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	return Snapshot{
		SlotsTotal:      m.cfg.totalSlots(),
		SlotsUsed:       m.slotsUsed,
		SlotsAvailable:  m.cfg.totalSlots() - m.slotsUsed,
		SeqTotal:        m.cfg.totalSlots(),
		ActiveSequences: m.seqActive,
		KVBlocksTotal:   m.cfg.totalKVBlocks(),
		KVBlocksUsed:    m.kvUsed,
		KVBlocksFree:    m.cfg.totalKVBlocks() - m.kvUsed,
		Reservations:    len(m.byRequest),
	}
}

// Invariants reports whether the manager is internally consistent. It is
// exported for tests and diagnostics. It re-derives totals from the per-request
// reservations and checks they match the running counters, and that no capacity
// is exceeded.
func (m *Manager) Invariants() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var slots, seqs, blocks int
	for _, r := range m.byRequest {
		slots += r.Slots
		seqs += r.Sequences
		blocks += r.KVBlocks
	}
	switch {
	case slots != m.slotsUsed:
		return fmt.Errorf("slots counter %d != reservations %d", m.slotsUsed, slots)
	case seqs != m.seqActive:
		return fmt.Errorf("seq counter %d != reservations %d", m.seqActive, seqs)
	case blocks != m.kvUsed:
		return fmt.Errorf("kv counter %d != reservations %d", m.kvUsed, blocks)
	case m.slotsUsed > m.cfg.totalSlots():
		return fmt.Errorf("slots %d exceed capacity %d", m.slotsUsed, m.cfg.totalSlots())
	case m.kvUsed > m.cfg.totalKVBlocks():
		return fmt.Errorf("kv %d exceed capacity %d", m.kvUsed, m.cfg.totalKVBlocks())
	}
	return nil
}
