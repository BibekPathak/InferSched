package scheduler

import "github.com/BibekP/infersched/internal/lifecycle"

// AgingConfig implements voluntary aging: a request that waits longer than
// AgingMS accumulates an effective-priority boost proportional to how long it
// has waited, capped at MaxBoost. This bounds starvation for low-priority work.
//
// Policies must be pure w.r.t. ordering, so aging is evaluated at an explicit
// reference time (the scheduling instant). The coordinator/simulator passes the
// reference time via NowFunc. When NowFunc is nil the reference time is zero,
// which disables aging in effect but keeps ordering deterministic.
type AgingConfig struct {
	// AgingMS is the wait duration after which a request begins to gain boost.
	// A value <= 0 disables aging.
	AgingMS int64
	// BoostPerSec is the effective-priority gained per second of wait beyond
	// AgingMS.
	BoostPerSec float64
	// MaxBoost caps accumulated boost (<= 0 means uncapped).
	MaxBoost float64
}

// Enabled reports whether aging is active.
func (a AgingConfig) Enabled() bool { return a.AgingMS > 0 && a.BoostPerSec > 0 }

// boost returns the accumulated boost for a request waited-on until nowNS.
func (a AgingConfig) boost(nowNS, arrivalNS int64) float64 {
	if !a.Enabled() {
		return 0
	}
	waitMS := (nowNS - arrivalNS) / 1e6
	if waitMS <= a.AgingMS {
		return 0
	}
	extraSec := float64(waitMS-a.AgingMS) / 1000.0
	b := extraSec * a.BoostPerSec
	if a.MaxBoost > 0 && b > a.MaxBoost {
		b = a.MaxBoost
	}
	return b
}

// Aging is a reusable helper embedded by policies that support aging. It binds
// a config to a reference-time provider.
type Aging struct {
	Config  AgingConfig
	NowFunc func() int64
}

// now returns the reference monotonic time in nanoseconds.
func (a Aging) now() int64 {
	if a.NowFunc == nil {
		return 0
	}
	return a.NowFunc()
}

// boostFor returns the aging boost for r at the current reference time.
func (a Aging) boostFor(r *lifecycle.Request) float64 {
	return a.Config.boost(a.now(), r.Arrival)
}
