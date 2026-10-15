package lifecycle

// Admit moves QUEUED -> ADMITTED.
func (r *Request) Admit() error {
	_, err := r.transition(StateAdmitted)
	return err
}

// Start moves ADMITTED -> RUNNING.
func (r *Request) Start() error {
	_, err := r.transition(StateRunning)
	return err
}

// Complete moves RUNNING -> COMPLETED.
func (r *Request) Complete() error {
	_, err := r.transition(StateCompleted)
	return err
}

// Reject moves QUEUED -> REJECTED.
func (r *Request) Reject() error {
	_, err := r.transition(StateRejected)
	return err
}

// Cancel moves QUEUED|ADMITTED|RUNNING -> CANCELLED. It is idempotent: calling
// Cancel on an already-terminal request returns the prior terminal state as an
// error, allowing callers to detect double cancellation without panicking.
func (r *Request) Cancel() error {
	_, err := r.transition(StateCancelled)
	return err
}

// Expire moves QUEUED|ADMITTED -> EXPIRED. RUNNING requests do not expire here;
// expiration of an in-flight request is modelled as cancellation/timeout.
func (r *Request) Expire() error {
	_, err := r.transition(StateExpired)
	return err
}

// Fail moves RUNNING -> FAILED.
func (r *Request) Fail() error {
	_, err := r.transition(StateFailed)
	return err
}

// TryTransition exposes the guarded transition to the coordinator for cases not
// covered by the named helpers above. It returns the prior state on success.
func (r *Request) TryTransition(next State) (State, error) {
	return r.transition(next)
}
