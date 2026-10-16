package simulator

import "container/heap"

// EventKind identifies the type of a scheduled event.
type EventKind int

const (
	// EventArrival delivers a request into the system.
	EventArrival EventKind = iota
	// EventStepEnd marks the end of a batch execution step on a worker.
	EventStepEnd
	// EventCancel delivers a cancellation to an in-flight request.
	EventCancel
	// EventDeadlineExpire checks/expires a queued request at its deadline.
	EventDeadlineExpire
	// EventWorkerFailure injects a synthetic worker failure.
	EventWorkerFailure
)

// event is a scheduled simulator event. seq is a monotonically increasing
// sequence number assigned at insertion; it is the tie-breaker so events at the
// same virtual time are processed in a deterministic, insertion-stable order
// regardless of the heap's internal layout.
type event struct {
	time int64 // virtual nanoseconds
	seq  uint64
	kind EventKind
	// reqIdx indexes into the simulator's request slice for arrival/cancel/
	// deadline events.
	reqIdx int
	// worker identifies the worker for step-end and failure events.
	worker int
}

// eventQueue is a min-heap ordered by (time, seq).
type eventQueue []event

func (q eventQueue) Len() int { return len(q) }

func (q eventQueue) Less(i, j int) bool {
	if q[i].time != q[j].time {
		return q[i].time < q[j].time
	}
	return q[i].seq < q[j].seq
}

func (q eventQueue) Swap(i, j int) { q[i], q[j] = q[j], q[i] }

func (q *eventQueue) Push(x any) { *q = append(*q, x.(event)) }

func (q *eventQueue) Pop() any {
	old := *q
	n := len(old)
	e := old[n-1]
	*q = old[:n-1]
	return e
}

// push inserts an event with the given sequence number.
func (q *eventQueue) push(e event) { heap.Push(q, e) }

// pop removes and returns the earliest event.
func (q *eventQueue) pop() event { return heap.Pop(q).(event) }
