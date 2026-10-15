package lifecycle

import "fmt"

// State is the lifecycle state of a request. The nominal happy path is
// QUEUED -> ADMITTED -> RUNNING -> COMPLETED. Terminal states are REJECTED,
// CANCELLED, EXPIRED, FAILED and (for normal service) COMPLETED.
type State string

const (
	StateQueued    State = "QUEUED"
	StateAdmitted  State = "ADMITTED"
	StateRunning   State = "RUNNING"
	StateCompleted State = "COMPLETED"
	StateRejected  State = "REJECTED"
	StateCancelled State = "CANCELLED"
	StateExpired   State = "EXPIRED"
	StateFailed    State = "FAILED"
)

// Reason explains why a transition into a terminal state was taken. It is
// deliberately a bounded enum so it can be used as a metric label.
type Reason string

const (
	ReasonNone            Reason = "NONE"
	ReasonCompleted       Reason = "COMPLETED"
	ReasonOverloadQueue   Reason = "OVERLOAD_QUEUE_LENGTH"
	ReasonOverloadTokens  Reason = "OVERLOAD_QUEUED_TOKENS"
	ReasonOverloadFlights Reason = "OVERLOAD_OUTSTANDING_REQUESTS"
	ReasonOverloadTenant  Reason = "OVERLOAD_TENANT_LIMIT"
	ReasonDeadlineMiss    Reason = "DEADLINE_INFEASIBLE"
	ReasonClientCancel    Reason = "CLIENT_CANCELLED"
	ReasonTimeout         Reason = "TIMEOUT"
	ReasonWorkerFailure   Reason = "WORKER_FAILURE"
	ReasonShutdown        Reason = "SHUTDOWN"
)

// terminal reports whether s is a terminal state with no outgoing transitions.
func (s State) terminal() bool {
	switch s {
	case StateCompleted, StateRejected, StateCancelled, StateExpired, StateFailed:
		return true
	default:
		return false
	}
}

// transitionTable is the authoritative allow-list of legal state transitions.
// Every mutation of a request's state must go through it. This is what prevents
// illegal transitions and double completion/cancellation.
var transitionTable = map[State]map[State]struct{}{
	StateQueued: {
		StateAdmitted:  {},
		StateRejected:  {},
		StateCancelled: {},
		StateExpired:   {},
	},
	StateAdmitted: {
		StateRunning:   {},
		StateCancelled: {},
		StateExpired:   {},
	},
	StateRunning: {
		StateCompleted: {},
		StateCancelled: {},
		StateFailed:    {},
	},
	// Terminal states have no outgoing transitions.
	StateCompleted: {},
	StateRejected:  {},
	StateCancelled: {},
	StateExpired:   {},
	StateFailed:    {},
}

// CanTransition reports whether from -> to is a legal lifecycle transition.
func CanTransition(from, to State) bool {
	dst, ok := transitionTable[from]
	if !ok {
		return false
	}
	_, ok = dst[to]
	return ok
}

// TransitionError is returned when an illegal state transition is attempted.
type TransitionError struct {
	RequestID string
	From      State
	To        State
}

func (e *TransitionError) Error() string {
	return fmt.Sprintf("lifecycle: request %s illegal transition %s -> %s", e.RequestID, e.From, e.To)
}

// Terminal reports whether the state has no outgoing transitions.
func (s State) Terminal() bool { return s.terminal() }
