// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package reconciler

import (
	"context"
	"maps"
	"math"
	"sync"
	"time"

	"github.com/google/ax/internal/lifecycle"
	"github.com/google/ax/internal/store"
)

const (
	// DefaultMaxInFlightTasks bounds concurrent leased tasks per replica.
	DefaultMaxInFlightTasks = 512
	// DefaultMaxInFlightPerAtespace bounds concurrent actions for any single
	// atespace on one replica so one tenant's burst cannot starve others.
	DefaultMaxInFlightPerAtespace = 64

	defaultOtherActionKindLimit = 128
)

// DefaultActionKindLimits returns the per-replica concurrency caps by action kind:
//   - RESUME_ACTOR: 256
//   - SUSPEND_ACTOR_SYNC: 128
//   - REVERT_ACTOR: 64
//   - CREATE_TAG: 32
//   - all other actions (shared bucket): 128
func DefaultActionKindLimits() map[lifecycle.ActionKind]int {
	return map[lifecycle.ActionKind]int{
		lifecycle.ActionResumeActor:      256,
		lifecycle.ActionSuspendActorSync: 128,
		lifecycle.ActionRevertActor:      64,
		lifecycle.ActionCreateTag:        32,
	}
}

const otherKindKey lifecycle.ActionKind = "__OTHER__"

// admissionController enforces per-replica task concurrency, per-atespace
// fairness, and per-action-kind slot caps.
type admissionController struct {
	mu               sync.Mutex
	maxTasks         int
	inFlightTasks    int
	maxPerAtespace   int
	otherKindLimit   int
	kindLimits       map[lifecycle.ActionKind]int
	inFlightAtespace map[string]int
	inFlightKind     map[lifecycle.ActionKind]int
	notifyCh         chan struct{}
}

func newAdmissionController(maxTasks, maxPerAtespace, otherKindLimit int, kindLimits map[lifecycle.ActionKind]int) *admissionController {
	if maxTasks <= 0 {
		maxTasks = DefaultMaxInFlightTasks
	}
	if maxPerAtespace <= 0 {
		maxPerAtespace = DefaultMaxInFlightPerAtespace
	}
	if otherKindLimit <= 0 {
		otherKindLimit = defaultOtherActionKindLimit
	}
	limits := DefaultActionKindLimits()
	maps.Copy(limits, kindLimits)
	return &admissionController{
		maxTasks:         maxTasks,
		maxPerAtespace:   maxPerAtespace,
		otherKindLimit:   otherKindLimit,
		kindLimits:       limits,
		inFlightAtespace: make(map[string]int),
		inFlightKind:     make(map[lifecycle.ActionKind]int),
		notifyCh:         make(chan struct{}),
	}
}

func (a *admissionController) broadcastLocked() {
	close(a.notifyCh)
	a.notifyCh = make(chan struct{})
}

// AcquireTaskSlots reserves up to requested task slots without exceeding maxTasks.
func (a *admissionController) AcquireTaskSlots(requested int) int {
	if requested <= 0 {
		return 0
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	free := a.maxTasks - a.inFlightTasks
	if free <= 0 {
		return 0
	}
	n := min(requested, free)
	a.inFlightTasks += n
	return n
}

// ReleaseTaskSlots releases n previously acquired task slots and reports
// whether the replica had been completely full prior to this release (so the
// caller can wake the poller immediately).
func (a *admissionController) ReleaseTaskSlots(n int) (wasAtCapacity bool) {
	if n <= 0 {
		return false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	wasAtCapacity = a.inFlightTasks >= a.maxTasks
	a.inFlightTasks = max(a.inFlightTasks-n, 0)
	if wasAtCapacity && a.inFlightTasks < a.maxTasks {
		a.broadcastLocked()
	}
	return wasAtCapacity
}

// LeaseFilter returns a [store.LeaseFilter] capturing both saturated atespaces
// and saturated per-action-kind buckets on this replica so LeaseDue skips them
// directly during the store scan instead of causing head-of-line blocking or
// lease-release churn.
func (a *admissionController) LeaseFilter() store.LeaseFilter {
	a.mu.Lock()
	defer a.mu.Unlock()

	var f store.LeaseFilter
	for ns, count := range a.inFlightAtespace {
		if count >= a.maxPerAtespace {
			f.ExcludeAtespaces = append(f.ExcludeAtespaces, ns)
		}
	}
	for kind, limit := range a.kindLimits {
		if limit <= 0 {
			continue
		}
		if a.inFlightKind[kind] >= limit {
			f.ExcludeActionKinds = append(f.ExcludeActionKinds, kind)
		}
	}
	if a.inFlightKind[otherKindKey] >= a.otherKindLimit {
		f.ExcludeOtherActionKinds = true
		for kind, limit := range a.kindLimits {
			if limit > 0 {
				f.BucketedActionKinds = append(f.BucketedActionKinds, kind)
			}
		}
	}
	return f
}

// WaitForAtespaceSlot blocks until atespace has at least one free per-atespace
// action slot locally or ctx is canceled. Used by ModeInline to avoid
// acquiring and releasing PostgreSQL row leases while local slots are full.
func (a *admissionController) WaitForAtespaceSlot(ctx context.Context, atespace string) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		a.mu.Lock()
		if a.inFlightAtespace[atespace] < a.maxPerAtespace {
			a.mu.Unlock()
			return nil
		}
		ch := a.notifyCh
		a.mu.Unlock()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ch:
		}
	}
}

// WaitForActionSlot blocks until both atespace and kind have at least one free
// action slot locally or ctx is canceled.
func (a *admissionController) WaitForActionSlot(ctx context.Context, atespace string, kind lifecycle.ActionKind) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		a.mu.Lock()
		bucket, limit := a.kindBucket(kind)
		if a.inFlightAtespace[atespace] < a.maxPerAtespace && a.inFlightKind[bucket] < limit {
			a.mu.Unlock()
			return nil
		}
		ch := a.notifyCh
		a.mu.Unlock()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ch:
		}
	}
}

// TryAcquireAction attempts to claim both a per-atespace slot and a
// per-action-kind slot without blocking. When ok is true, the caller must invoke
// release once the action finishes; release reports whether freeing the slot
// transitioned a saturated atespace or action-kind bucket back below capacity.
func (a *admissionController) TryAcquireAction(atespace string, kind lifecycle.ActionKind) (release func() bool, ok bool) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.inFlightAtespace[atespace] >= a.maxPerAtespace {
		return nil, false
	}
	bucket, limit := a.kindBucket(kind)
	if a.inFlightKind[bucket] >= limit {
		return nil, false
	}

	a.inFlightAtespace[atespace]++
	a.inFlightKind[bucket]++

	var once sync.Once
	return func() bool {
		var freedSaturated bool
		once.Do(func() {
			a.mu.Lock()
			wasNsFull := a.inFlightAtespace[atespace] >= a.maxPerAtespace
			wasKindFull := a.inFlightKind[bucket] >= limit
			if c := a.inFlightAtespace[atespace] - 1; c <= 0 {
				delete(a.inFlightAtespace, atespace)
			} else {
				a.inFlightAtespace[atespace] = c
			}
			if c := a.inFlightKind[bucket] - 1; c <= 0 {
				delete(a.inFlightKind, bucket)
			} else {
				a.inFlightKind[bucket] = c
			}
			freedSaturated = wasNsFull || wasKindFull
			a.broadcastLocked()
			a.mu.Unlock()
		})
		return freedSaturated
	}, true
}

func (a *admissionController) kindBucket(kind lifecycle.ActionKind) (lifecycle.ActionKind, int) {
	if limit, ok := a.kindLimits[kind]; ok && limit > 0 {
		return kind, limit
	}
	return otherKindKey, a.otherKindLimit
}

// rateLimiter is a context-aware token-bucket limiter using only the Go
// standard library. A non-positive qps disables limiting.
type rateLimiter struct {
	qps   float64
	burst float64

	mu     sync.Mutex
	tokens float64
	last   time.Time
}

func newRateLimiter(qps float64, burst int) *rateLimiter {
	if qps <= 0 {
		return &rateLimiter{qps: 0}
	}
	b := float64(burst)
	if b < 1 {
		b = max(1, min(qps, 50))
	}
	return &rateLimiter{
		qps:    qps,
		burst:  b,
		tokens: b,
		last:   time.Now(),
	}
}

// Wait blocks until one token is available or ctx is canceled.
func (l *rateLimiter) Wait(ctx context.Context) error {
	if l == nil || l.qps <= 0 {
		return ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	l.mu.Lock()
	now := time.Now()
	if elapsed := now.Sub(l.last).Seconds(); elapsed > 0 {
		l.tokens = min(l.burst, l.tokens+elapsed*l.qps)
		l.last = now
	}
	nextTokens := l.tokens - 1.0
	var waitDur time.Duration
	if nextTokens < 0 {
		waitNs := (-nextTokens / l.qps) * float64(time.Second)
		if waitNs >= float64(math.MaxInt64) {
			waitDur = time.Duration(math.MaxInt64)
		} else {
			waitDur = time.Duration(waitNs)
		}
		if dl, ok := ctx.Deadline(); ok && now.Add(waitDur).After(dl) {
			l.mu.Unlock()
			return context.DeadlineExceeded
		}
	}
	l.tokens = nextTokens
	l.mu.Unlock()

	if waitDur <= 0 {
		return nil
	}

	timer := time.NewTimer(waitDur)
	defer timer.Stop()

	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		// Restore the single token reserved by this canceled caller (clamped to
		// burst) after advancing the bucket to refundNow so neither active
		// waiters behind us nor subsequent callers forfeit unconsumed QPS.
		l.mu.Lock()
		refundNow := time.Now()
		if elapsed := refundNow.Sub(l.last).Seconds(); elapsed > 0 {
			l.tokens = min(l.burst, l.tokens+elapsed*l.qps)
			l.last = refundNow
		}
		l.tokens = min(l.burst, l.tokens+1.0)
		l.mu.Unlock()
		return ctx.Err()
	}
}

// CircuitBreakerConfig configures the per-replica Substrate outage circuit breaker.
type CircuitBreakerConfig struct {
	// Disabled turns off the circuit breaker when true.
	Disabled bool
	// Window is the rolling observation window over which transient failure rate
	// is measured (default 30s).
	Window time.Duration
	// FailureThreshold is the fraction of transient failures (0 < threshold <= 1)
	// above which the breaker opens (default 0.5).
	FailureThreshold float64
	// MinCalls is the minimum number of Substrate calls in Window required
	// before FailureThreshold can trip the breaker (default 10).
	MinCalls int
	// ProbeInterval is how often an open breaker allows a single probe task
	// lease to test Substrate recovery (default 5s).
	ProbeInterval time.Duration
}

type cbBucket struct {
	sec      int64
	total    int
	failures int
}

// circuitBreaker pauses bulk task leasing when Substrate's transient failure
// rate exceeds 50% over a rolling 30-second window, issuing one probe every 5s
// until a call succeeds.
type circuitBreaker struct {
	disabled   bool
	window     time.Duration
	threshold  float64
	minCalls   int
	probeEvery time.Duration
	clock      func() time.Time

	mu            sync.Mutex
	open          bool
	lastProbeAt   time.Time
	probeInFlight bool
	buckets       []cbBucket
}

func newCircuitBreaker(cfg CircuitBreakerConfig, clock func() time.Time) *circuitBreaker {
	if clock == nil {
		clock = time.Now
	}
	window := cfg.Window
	if window <= 0 {
		window = 30 * time.Second
	}
	threshold := cfg.FailureThreshold
	if threshold <= 0 || threshold > 1 {
		threshold = 0.5
	}
	minCalls := cfg.MinCalls
	if minCalls <= 0 {
		minCalls = 10
	}
	probeEvery := cfg.ProbeInterval
	if probeEvery <= 0 {
		probeEvery = 5 * time.Second
	}
	numBuckets := max(int(window/time.Second), 1)
	return &circuitBreaker{
		disabled:   cfg.Disabled,
		window:     window,
		threshold:  threshold,
		minCalls:   minCalls,
		probeEvery: probeEvery,
		clock:      clock,
		buckets:    make([]cbBucket, numBuckets),
	}
}

// IsOpen reports whether the circuit breaker is currently open.
func (cb *circuitBreaker) IsOpen() bool {
	if cb == nil || cb.disabled {
		return false
	}
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.open
}

// PlanLease returns how many task rows the poller may lease on this cycle and
// whether the lease represents a half-open recovery probe.
//
// PlanLease intentionally sets only probeInFlight (without advancing
// lastProbeAt) so an empty poll tick or non-RPC deferral that calls CancelProbe
// does not consume the 5-second probe cooldown.
func (cb *circuitBreaker) PlanLease(requested int) (limit int, isProbe bool) {
	if requested <= 0 {
		return 0, false
	}
	if cb == nil || cb.disabled {
		return requested, false
	}
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if !cb.open {
		return requested, false
	}
	now := cb.clock()
	if cb.probeInFlight || (!cb.lastProbeAt.IsZero() && now.Sub(cb.lastProbeAt) < cb.probeEvery) {
		return 0, false
	}
	cb.probeInFlight = true
	return 1, true
}

// CancelProbe releases an unconsumed probe permit (for example when LeaseDue
// found no due tasks, admission deferred the task, or an action exited without
// testing Substrate connectivity).
func (cb *circuitBreaker) CancelProbe() {
	if cb == nil || cb.disabled {
		return
	}
	cb.mu.Lock()
	cb.probeInFlight = false
	cb.mu.Unlock()
}

// Record records the outcome of one live Substrate RPC:
//   - While the breaker is Open, completions from pre-open in-flight workers
//     (isProbe == false) are ignored so draining workers neither prematurely
//     close the breaker during a partial brownout nor clear probeInFlight;
//     only the designated probe worker (isProbe == true) updates the breaker.
//   - While Closed, transientFailure increments the rolling window's failure
//     count and opens the breaker once total >= minCalls and failures/total >
//     threshold.
func (cb *circuitBreaker) Record(transientFailure bool, isProbe bool) (closedFromOpen bool) {
	if cb == nil || cb.disabled {
		return false
	}
	cb.mu.Lock()
	defer cb.mu.Unlock()

	now := cb.clock()
	if cb.open {
		if !isProbe {
			return false
		}
		cb.probeInFlight = false
		if !transientFailure {
			cb.open = false
			clear(cb.buckets)
			return true
		}
		cb.lastProbeAt = now
		return false
	}

	sec := now.Unix()
	idx := int(sec % int64(len(cb.buckets)))
	if idx < 0 {
		idx += len(cb.buckets)
	}
	if cb.buckets[idx].sec != sec {
		cb.buckets[idx] = cbBucket{sec: sec}
	}
	cb.buckets[idx].total++
	if transientFailure {
		cb.buckets[idx].failures++
	}

	cutoff := sec - int64(len(cb.buckets)) + 1
	var total, failures int
	for _, b := range cb.buckets {
		if b.sec >= cutoff && b.sec <= sec {
			total += b.total
			failures += b.failures
		}
	}
	if total >= cb.minCalls && float64(failures)/float64(total) > cb.threshold {
		cb.open = true
		cb.lastProbeAt = now
		cb.probeInFlight = false
	}
	return false
}
