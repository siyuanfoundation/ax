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

// Package reconciler executes queued Agent Substrate operations for AX Tasks.
//
// Tasks store their pending Substrate operations on their PostgreSQL row
// alongside a wakeup timestamp (next_reconcile_at). Each ax-server replica runs
// a [Reconciler] whose poller leases due task rows for 30 seconds (advancing
// next_reconcile_at out of the due range while leased), executes each task's
// actions in order outside any database transaction, renews the lease every 10
// seconds while long calls are in flight (canceling obsolete calls if a newer
// Apply dropped or replaced the head action), and commits results through
// [store.TaskApplier.Apply] fenced on both the lease token and head Action.ID.
//
// For M0 rollouts and operational fallback (--reconcile-mode=inline),
// [Reconciler.ReconcileInline] runs the identical leased action executor
// synchronously from request handlers without background polling.
package reconciler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"sync"
	"time"

	"github.com/google/ax/internal/apivalidation"
	"github.com/google/ax/internal/lifecycle"
	"github.com/google/ax/internal/store"
	"github.com/google/ax/internal/substrate"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"github.com/google/uuid"
	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// Mode selects how queued task actions are driven.
type Mode string

const (
	// ModeQueue runs background polling workers that lease and execute due task
	// actions asynchronously.
	ModeQueue Mode = "queue"
	// ModeInline disables background polling so callers drive queued task
	// actions synchronously after store.Apply via ReconcileInline. Transient
	// action backoffs (up to MaxBackoff) are waited out under the caller's
	// context against an advancing store clock, while capacity-unavailable
	// backoffs and future lifecycle timers (such as ClaimDeadline) remain
	// persisted on the task row until due on a subsequent ReconcileInline call
	// or ModeQueue poller.
	ModeInline Mode = "inline"
)

const (
	// DefaultLeaseTTL is the duration for which a worker leases a due task row.
	DefaultLeaseTTL = 30 * time.Second
	// DefaultActionTimeout bounds the wall-clock duration of a single Substrate
	// action call before it is canceled and retried transiently with backoff.
	DefaultActionTimeout = 2 * time.Minute
	// DefaultPollInterval is the nominal background queue polling cadence in
	// ModeQueue (jittered ±20%).
	DefaultPollInterval = time.Second
	// DefaultBatchSize is the maximum number of due tasks leased in one query.
	DefaultBatchSize = 64
	// DefaultMutatingQPS is the deployment-wide budget (200 QPS) for mutating
	// Substrate Control API calls; a single replica uses this cap directly
	// until live-replica count scaling is active.
	DefaultMutatingQPS = 200.0
	// DefaultAdmissionRequeueDelay is the nominal requeue delay when per-kind or
	// per-atespace admission slots are saturated (jittered ±50%).
	DefaultAdmissionRequeueDelay = time.Second
	// DefaultMaxTemplateSkewWait bounds how long ActionEnsureTemplate waits for
	// an originating replica to publish a content-addressed ActorTemplate during
	// a rolling ax-server upgrade before failing with PROVISIONING_FAILED.
	DefaultMaxTemplateSkewWait = 5 * time.Minute
	// DefaultMaxWorkspaceTagWait bounds how long ActionWaitWorkspaceTag waits
	// for a preparing Workspace snapshot Tag to reach COMPLETED state before
	// failing with PROVISIONING_FAILED.
	DefaultMaxWorkspaceTagWait = 10 * time.Minute

	maxSteadyStatePreconditionRetries = 2
	inlineContentionPollInterval      = 25 * time.Millisecond
	leaseQueryMaxTimeout              = 10 * time.Second
	shutdownReleaseTimeout            = 5 * time.Second
)

// WorkspaceTagResolver resolves whether a preparing Workspace snapshot Tag is
// ready for [lifecycle.ActionWaitWorkspaceTag].
//
// Returning ready == true with a non-empty tag completes WAIT_WORKSPACE_TAG and
// passes tag as ActionCompletedInput.SourceTag; returning ready == false with
// err == nil backs off up to MaxWorkspaceTagWait and re-checks later without
// counting as a Substrate outage. Returning a non-nil err classifies it using
// gRPC status codes: permanent failures must return a permanent gRPC status
// code (such as codes.FailedPrecondition, codes.InvalidArgument,
// codes.PermissionDenied, or codes.NotFound), whereas uncoded plain Go errors
// default to codes.Unknown and are retried transiently with backoff.
type WorkspaceTagResolver func(ctx context.Context, atespace, workspaceName, templateName, requestedTag string) (tag string, ready bool, err error)

// Config configures a [Reconciler]. Zero values use production defaults.
type Config struct {
	// Mode selects ModeQueue (default) or ModeInline.
	Mode Mode
	// WorkerID identifies this replica in task lease tokens (defaults to
	// "<hostname>-<short-uuid>").
	WorkerID string
	// Transition computes pure task state transitions inside store.Apply.
	// Required.
	Transition lifecycle.TransitionFunc
	// ExtraEgressRules are built-in EgressRule entries (such as the internal
	// runner :8081 Actor JWT rule) prepended ahead of TaskRow.EgressRules
	// during ActionEnsureEgress.
	ExtraEgressRules []*ateapipb.EgressRule
	// WorkspaceTagResolver optionally overrides how ActionWaitWorkspaceTag
	// checks whether a Workspace snapshot Tag has finished warming. When nil,
	// the reconciler checks RuntimeClient.GetTag for Action.Tag.
	WorkspaceTagResolver WorkspaceTagResolver

	// LeaseTTL is the duration of each task row lease (default 30s).
	LeaseTTL time.Duration
	// RenewInterval is how often an active worker renews its lease and checks
	// whether its in-flight head action is still current (default LeaseTTL/3 = 10s).
	RenewInterval time.Duration
	// ActionTimeout bounds the duration of any single Substrate action attempt
	// so a hung network connection cannot hold a task lease or admission slot
	// forever (default 2m).
	ActionTimeout time.Duration
	// PollInterval is how often ModeQueue polls for due tasks in addition to
	// immediate local Notify wakeups (default 1s ± 20% jitter).
	PollInterval time.Duration
	// BatchSize is the maximum number of due tasks leased in a single query
	// (default 64).
	BatchSize int

	// MaxInFlightTasks bounds concurrent leased tasks in ModeQueue on this
	// replica (default 512). ReconcileInline executes on caller goroutines while
	// still enforcing MaxInFlightPerAtespace, ActionKindLimits, and MutatingQPS.
	MaxInFlightTasks int
	// MaxInFlightPerAtespace bounds concurrent actions per atespace on this
	// replica for multi-tenant fairness (default 64).
	MaxInFlightPerAtespace int
	// ActionKindLimits overrides per-action-kind concurrency caps (merged onto
	// DefaultActionKindLimits).
	ActionKindLimits map[lifecycle.ActionKind]int
	// OtherActionKindLimit bounds concurrency for actions outside
	// ActionKindLimits (default 128).
	OtherActionKindLimit int

	// MutatingQPS caps mutating Substrate RPCs issued by this replica (defaults
	// to DefaultMutatingQPS; set < 0 to disable rate limiting in unit tests).
	MutatingQPS float64
	// MutatingBurst sets the token-bucket burst for MutatingQPS.
	MutatingBurst int
	// CircuitBreaker configures the Substrate outage circuit breaker.
	CircuitBreaker CircuitBreakerConfig

	// InitialBackoff is the base delay for transient action retries (default 1s).
	InitialBackoff time.Duration
	// MaxBackoff caps transient action retry delays (default 60s).
	MaxBackoff time.Duration
	// AdmissionRequeueDelay is the nominal requeue delay when per-atespace or
	// per-kind admission slots are full (default 1s ± 50% jitter).
	AdmissionRequeueDelay time.Duration
	// MaxTemplateSkewWait bounds how long ActionEnsureTemplate waits for an
	// originating replica to publish a content-addressed ActorTemplate during a
	// rolling upgrade before failing permanently (default 5m).
	MaxTemplateSkewWait time.Duration
	// MaxWorkspaceTagWait bounds how long ActionWaitWorkspaceTag waits for a
	// preparing Workspace snapshot Tag before failing permanently (default 10m).
	MaxWorkspaceTagWait time.Duration

	// Clock overrides replica-local wall-clock reads for circuit-breaker window
	// and cooldown testing (defaults to time.Now; task queue and action-age
	// deadlines always use the store's clock from LeasedTask.LeasedAt, falling
	// back to Clock only when LeasedAt is zero).
	Clock func() time.Time
}

type inlineRetryState struct {
	seq      uint64
	wakeMono time.Time
	delay    time.Duration
}

// Reconciler drives queued Agent Substrate actions and lifecycle timers for AX
// Tasks.
type Reconciler struct {
	queue         store.TaskQueue
	runtime       RuntimeClient
	transition    lifecycle.TransitionFunc
	tagResolve    WorkspaceTagResolver
	defaultTagRes bool
	extraRules    []*ateapipb.EgressRule

	mode                Mode
	workerID            string
	leaseTTL            time.Duration
	renewInterval       time.Duration
	actionTimeout       time.Duration
	pollInterval        time.Duration
	batchSize           int
	initialBackoff      time.Duration
	maxBackoff          time.Duration
	requeueDelay        time.Duration
	maxTemplateSkewWait time.Duration
	maxWorkspaceTagWait time.Duration
	clock               func() time.Time
	customClock         bool

	admission *admissionController
	limiter   *rateLimiter
	breaker   *circuitBreaker

	wakeCh chan struct{}

	// workerMu serializes wg.Add(1) against Run shutdown's wg.Wait() so
	// concurrent ReconcileInline calls can never race wg.Add with wg.Wait.
	workerMu     sync.Mutex
	shuttingDown bool
	wg           sync.WaitGroup

	// inlineSleepMu tracks the monotonic retry deadline of any transient action
	// backoff committed in ModeInline so surviving concurrent inline callers
	// sleep out the remaining backoff even if an earlier caller's context cancels.
	inlineSleepMu    sync.Mutex
	inlineSleepSeq   uint64
	inlineSleepUntil map[store.TaskKey]inlineRetryState
}

// New constructs a [Reconciler] over queue and runtime.
func New(queue store.TaskQueue, runtime RuntimeClient, cfg Config) *Reconciler {
	if queue == nil {
		panic("reconciler: New requires a non-nil store.TaskQueue")
	}
	if runtime == nil {
		panic("reconciler: New requires a non-nil RuntimeClient")
	}

	mode := cfg.Mode
	if mode == "" {
		mode = ModeQueue
	}
	if mode != ModeQueue && mode != ModeInline {
		panic(fmt.Sprintf("reconciler: unsupported Mode %q (want %q or %q)", mode, ModeQueue, ModeInline))
	}
	workerID := cfg.WorkerID
	if workerID == "" {
		host, _ := os.Hostname()
		if host == "" {
			host = "ax-server"
		}
		workerID = fmt.Sprintf("%s-%s", host, uuid.NewString()[:8])
	}
	transition := cfg.Transition
	if transition == nil {
		panic("reconciler: New requires a non-nil Config.Transition")
	}
	leaseTTL := cfg.LeaseTTL
	if leaseTTL <= 0 {
		leaseTTL = DefaultLeaseTTL
	}
	renewInterval := cfg.RenewInterval
	if renewInterval <= 0 {
		renewInterval = max(leaseTTL/3, 10*time.Millisecond)
	}
	actionTimeout := cfg.ActionTimeout
	if actionTimeout <= 0 {
		actionTimeout = DefaultActionTimeout
	}
	pollInterval := cfg.PollInterval
	if pollInterval <= 0 {
		pollInterval = DefaultPollInterval
	}
	batchSize := cfg.BatchSize
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}
	initialBackoff := cfg.InitialBackoff
	if initialBackoff <= 0 {
		initialBackoff = lifecycle.DefaultActionInitialBackoff
	}
	maxBackoff := cfg.MaxBackoff
	if maxBackoff <= 0 {
		maxBackoff = lifecycle.DefaultActionMaxBackoff
	}
	requeueDelay := cfg.AdmissionRequeueDelay
	if requeueDelay <= 0 {
		requeueDelay = DefaultAdmissionRequeueDelay
	}
	maxTemplateSkewWait := cfg.MaxTemplateSkewWait
	if maxTemplateSkewWait <= 0 {
		maxTemplateSkewWait = DefaultMaxTemplateSkewWait
	}
	maxWorkspaceTagWait := cfg.MaxWorkspaceTagWait
	if maxWorkspaceTagWait <= 0 {
		maxWorkspaceTagWait = DefaultMaxWorkspaceTagWait
	}
	customClock := cfg.Clock != nil
	clock := cfg.Clock
	if clock == nil {
		clock = time.Now
	}

	qps := cfg.MutatingQPS
	if qps == 0 {
		qps = DefaultMutatingQPS
	}
	var limiter *rateLimiter
	if qps > 0 {
		limiter = newRateLimiter(qps, cfg.MutatingBurst)
	}

	var extraRules []*ateapipb.EgressRule
	if len(cfg.ExtraEgressRules) > 0 {
		extraRules = make([]*ateapipb.EgressRule, 0, len(cfg.ExtraEgressRules))
		for _, rule := range cfg.ExtraEgressRules {
			if rule != nil {
				extraRules = append(extraRules, proto.Clone(rule).(*ateapipb.EgressRule))
			}
		}
	}

	r := &Reconciler{
		queue:               queue,
		runtime:             runtime,
		transition:          transition,
		tagResolve:          cfg.WorkspaceTagResolver,
		defaultTagRes:       cfg.WorkspaceTagResolver == nil,
		extraRules:          extraRules,
		mode:                mode,
		workerID:            workerID,
		leaseTTL:            leaseTTL,
		renewInterval:       renewInterval,
		actionTimeout:       actionTimeout,
		pollInterval:        pollInterval,
		batchSize:           batchSize,
		initialBackoff:      initialBackoff,
		maxBackoff:          maxBackoff,
		requeueDelay:        requeueDelay,
		maxTemplateSkewWait: maxTemplateSkewWait,
		maxWorkspaceTagWait: maxWorkspaceTagWait,
		clock:               clock,
		customClock:         customClock,
		admission: newAdmissionController(
			cfg.MaxInFlightTasks,
			cfg.MaxInFlightPerAtespace,
			cfg.OtherActionKindLimit,
			cfg.ActionKindLimits,
		),
		limiter:          limiter,
		breaker:          newCircuitBreaker(cfg.CircuitBreaker, clock),
		wakeCh:           make(chan struct{}, 1),
		inlineSleepUntil: make(map[store.TaskKey]inlineRetryState),
	}
	if r.tagResolve == nil {
		r.tagResolve = r.defaultResolveWorkspaceTag
	}
	return r
}

// Mode returns the reconciler's configured execution mode.
func (r *Reconciler) Mode() Mode {
	return r.mode
}

// CircuitBreakerOpen reports whether the replica's Substrate circuit breaker is
// currently open.
func (r *Reconciler) CircuitBreakerOpen() bool {
	return r.breaker.IsOpen()
}

// Notify signals the background poller that one or more task rows are due now.
// It never blocks.
func (r *Reconciler) Notify() {
	select {
	case r.wakeCh <- struct{}{}:
	default:
	}
}

func (r *Reconciler) beginWorker() bool {
	r.workerMu.Lock()
	defer r.workerMu.Unlock()
	if r.shuttingDown {
		return false
	}
	r.wg.Add(1)
	return true
}

func (r *Reconciler) isShuttingDown() bool {
	r.workerMu.Lock()
	defer r.workerMu.Unlock()
	return r.shuttingDown
}

func (r *Reconciler) waitForWorkersOnStop() {
	r.workerMu.Lock()
	r.shuttingDown = true
	r.workerMu.Unlock()
	r.wg.Wait()
}

func (r *Reconciler) recordInlineRetrySleep(key store.TaskKey, delay time.Duration) uint64 {
	now := time.Now()
	wake := now.Add(delay)
	r.inlineSleepMu.Lock()
	defer r.inlineSleepMu.Unlock()

	if len(r.inlineSleepUntil) >= 64 {
		for k, st := range r.inlineSleepUntil {
			if !st.wakeMono.After(now) {
				delete(r.inlineSleepUntil, k)
			}
		}
	}
	r.inlineSleepSeq++
	seq := r.inlineSleepSeq
	if cur, ok := r.inlineSleepUntil[key]; ok && cur.wakeMono.After(wake) {
		wake = cur.wakeMono
		delay = max(delay, cur.delay)
	}
	r.inlineSleepUntil[key] = inlineRetryState{
		seq:      seq,
		wakeMono: wake,
		delay:    delay,
	}
	return seq
}

func (r *Reconciler) cancelInlineRetrySleep(key store.TaskKey, seq uint64) {
	r.inlineSleepMu.Lock()
	if cur, ok := r.inlineSleepUntil[key]; ok && cur.seq == seq {
		delete(r.inlineSleepUntil, key)
	}
	r.inlineSleepMu.Unlock()
}

func (r *Reconciler) clearInlineRetrySleep(key store.TaskKey) {
	r.inlineSleepMu.Lock()
	delete(r.inlineSleepUntil, key)
	r.inlineSleepMu.Unlock()
}

func (r *Reconciler) clearExpiredInlineRetrySleep(key store.TaskKey) {
	now := time.Now()
	r.inlineSleepMu.Lock()
	if cur, ok := r.inlineSleepUntil[key]; ok && !cur.wakeMono.After(now) {
		delete(r.inlineSleepUntil, key)
	}
	r.inlineSleepMu.Unlock()
}

func (r *Reconciler) remainingInlineRetrySleep(key store.TaskKey) (waitDur, nominalDelay time.Duration) {
	r.inlineSleepMu.Lock()
	defer r.inlineSleepMu.Unlock()
	cur, ok := r.inlineSleepUntil[key]
	if !ok {
		return 0, 0
	}
	rem := time.Until(cur.wakeMono)
	if rem <= 0 {
		delete(r.inlineSleepUntil, key)
		return 0, 0
	}
	return rem, cur.delay
}

func sleepWithContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	if dl, ok := ctx.Deadline(); ok && time.Now().Add(d).After(dl) {
		return context.DeadlineExceeded
	}
	timer := time.NewTimer(d)
	select {
	case <-ctx.Done():
		timer.Stop()
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// Run starts the action reconciliation loop and blocks until ctx is canceled
// and all in-flight worker goroutines have finished and released their leases.
//
// In ModeInline, Run performs no background polling and simply waits for ctx
// cancellation and active inline reconciliations to drain.
func (r *Reconciler) Run(ctx context.Context) error {
	r.workerMu.Lock()
	r.shuttingDown = false
	r.workerMu.Unlock()

	if r.mode == ModeInline {
		<-ctx.Done()
		r.waitForWorkersOnStop()
		return nil
	}

	r.queue.SetQueueWakeup(r.Notify)
	defer func() {
		r.queue.SetQueueWakeup(nil)
		r.waitForWorkersOnStop()
	}()

	// Perform an initial poll immediately on startup so pre-existing due work
	// does not wait for the first poll interval.
	r.pollOnce(ctx)

	timer := time.NewTimer(jitter20Pct(r.pollInterval))
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-r.wakeCh:
			r.pollOnce(ctx)
		case <-timer.C:
			r.pollOnce(ctx)
			timer.Reset(jitter20Pct(r.pollInterval))
		}
	}
}

// pollOnce leases due tasks up to the replica's free worker capacity (and
// circuit-breaker budget), skipping saturated atespaces and saturated
// action-kind buckets on this replica, and dispatches one worker goroutine per
// admitted task.
func (r *Reconciler) pollOnce(ctx context.Context) {
	emptyAdmitRetries := 0
	for ctx.Err() == nil {
		reserved := r.admission.AcquireTaskSlots(r.batchSize)
		if reserved == 0 {
			return
		}
		limit, isProbe := r.breaker.PlanLease(reserved)
		if limit < reserved {
			r.admission.ReleaseTaskSlots(reserved - limit)
		}
		if limit == 0 {
			return
		}

		leaseFilter := r.admission.LeaseFilter()
		leasedMono := time.Now()
		leaseCtx, cancelLease := context.WithTimeout(ctx, min(r.leaseTTL, leaseQueryMaxTimeout))
		leased, err := r.queue.LeaseDue(leaseCtx, r.workerID, r.leaseTTL, limit, leaseFilter)
		cancelLease()
		if err != nil {
			r.admission.ReleaseTaskSlots(limit)
			if isProbe {
				r.breaker.CancelProbe()
			}
			if ctx.Err() == nil {
				slog.Warn("leasing due tasks failed", "worker", r.workerID, "error", err)
			}
			return
		}
		if unused := limit - len(leased); unused > 0 {
			r.admission.ReleaseTaskSlots(unused)
		}
		if len(leased) == 0 {
			if isProbe {
				r.breaker.CancelProbe()
			}
			return
		}

		type deferredRelease struct {
			key   store.TaskKey
			token string
			delay time.Duration
		}
		var (
			admittedCount int
			rejected      []deferredRelease
		)
		for i, item := range leased {
			probeTask := isProbe && i == 0

			// Acquire the initial action admission slot synchronously before
			// spawning the worker goroutine so inFlightAtespace and
			// inFlightKind reflect this batch immediately before the next
			// LeaseDue iteration.
			var initialSlot func() bool
			if item.Row != nil && len(item.Row.Actions) > 0 &&
				(item.Row.Actions[0].NotBefore.IsZero() || !item.Row.Actions[0].NotBefore.After(item.LeasedAt)) {
				slot, admitted := r.admission.TryAcquireAction(item.Key.Atespace, item.Row.Actions[0].Kind)
				if !admitted {
					if probeTask {
						r.breaker.CancelProbe()
					}
					rejected = append(rejected, deferredRelease{
						key:   item.Key,
						token: item.LeaseToken,
						delay: jitter50Pct(r.requeueDelay),
					})
					if r.admission.ReleaseTaskSlots(1) {
						r.Notify()
					}
					continue
				}
				initialSlot = slot
			}

			if !r.beginWorker() {
				if initialSlot != nil {
					initialSlot()
				}
				if probeTask {
					r.breaker.CancelProbe()
				}
				rejected = append(rejected, deferredRelease{
					key:   item.Key,
					token: item.LeaseToken,
					delay: 0,
				})
				r.admission.ReleaseTaskSlots(1)
				continue
			}
			admittedCount++
			go func(lt store.LeasedTask, probe bool, firstSlot func() bool, startMono time.Time) {
				defer r.wg.Done()
				defer func() {
					if r.admission.ReleaseTaskSlots(1) {
						r.Notify()
					}
				}()
				_, _, _ = r.processLeasedTask(ctx, lt, startMono, probe, false, firstSlot, nil)
			}(item, probeTask, initialSlot, leasedMono)
		}

		// Release any overflow leases from newly saturated buckets with bounded
		// concurrency so a bucket crossing its cap never stalls the poller on N
		// sequential database round-trips.
		if len(rejected) == 1 {
			r.releaseLeaseBestEffort(rejected[0].key, rejected[0].token, rejected[0].delay)
		} else if len(rejected) > 1 {
			workers := min(len(rejected), 8)
			ch := make(chan deferredRelease, len(rejected))
			for _, item := range rejected {
				ch <- item
			}
			close(ch)
			var relWG sync.WaitGroup
			relWG.Add(workers)
			for range workers {
				go func() {
					defer relWG.Done()
					for item := range ch {
						r.releaseLeaseBestEffort(item.key, item.token, item.delay)
					}
				}()
			}
			relWG.Wait()
		}

		if isProbe || len(leased) < limit {
			return
		}
		if admittedCount == 0 {
			emptyAdmitRetries++
			if emptyAdmitRetries > 1 {
				return
			}
			continue
		}
		emptyAdmitRetries = 0
	}
}

// ReconcileInline synchronously executes runnable actions on key under a
// per-row worker lease. It is used when ax-server runs with
// --reconcile-mode=inline and shares the same fencing, renewal, and error
// handling as ModeQueue workers. If admission control, a concurrent lease, or a
// short action backoff (<= MaxBackoff on an advancing store clock) temporarily
// defers execution, ReconcileInline waits and retries under ctx.
func (r *Reconciler) ReconcileInline(ctx context.Context, key store.TaskKey) error {
	key = key.Normalized()
	var sleptBackoff time.Duration
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if r.isShuttingDown() {
			return context.Canceled
		}
		// Wait locally if this replica's per-atespace slots are already full so
		// inline callers never churn PostgreSQL row leases while waiting.
		if err := r.admission.WaitForAtespaceSlot(ctx, key.Atespace); err != nil {
			return err
		}

		leasedMono := time.Now()
		leased, contended, retryIn, err := r.queue.LeaseTask(ctx, key, r.workerID, r.leaseTTL)
		if err != nil {
			return err
		}
		if leased != nil {
			sleptBackoff = 0
			r.clearInlineRetrySleep(key)
			if !r.beginWorker() {
				r.releaseLeaseBestEffort(leased.Key, leased.LeaseToken, 0)
				return context.Canceled
			}
			var blockedKind lifecycle.ActionKind
			deferred, retryAfter, execErr := r.processLeasedTask(ctx, *leased, leasedMono, false, true, nil, &blockedKind)
			r.wg.Done()
			if err := ctx.Err(); err != nil {
				return err
			}
			if execErr != nil {
				return execErr
			}
			if retryAfter > 0 {
				// In ModeInline (where background polling is disabled), sleep
				// out transient action backoff under ctx and re-check LeaseTask.
				if err := sleepWithContext(ctx, retryAfter); err != nil {
					return err
				}
				sleptBackoff = retryAfter
				r.clearExpiredInlineRetrySleep(key)
				continue
			}
			if !deferred {
				return nil
			}
			if blockedKind != "" {
				if err := r.admission.WaitForActionSlot(ctx, key.Atespace, blockedKind); err != nil {
					return err
				}
				continue
			}
		} else if !contended {
			if waitDur, backoffDelay := r.remainingInlineRetrySleep(key); waitDur > 0 {
				if err := sleepWithContext(ctx, waitDur); err != nil {
					return err
				}
				sleptBackoff = max(sleptBackoff, backoffDelay)
				r.clearExpiredInlineRetrySleep(key)
				continue
			}
			if retryIn > 0 && sleptBackoff == 0 && !r.customClock && retryIn <= r.maxBackoff+r.maxBackoff/2 {
				// Another inline replica backed off this action (including up to
				// 1.5x jitter above MaxBackoff); wait out the remaining
				// store-clock backoff under ctx so retried inline RPCs across
				// stateless replicas still drive completion.
				sleptBackoff = retryIn
				if err := sleepWithContext(ctx, retryIn); err != nil {
					return err
				}
				continue
			}
			if retryIn > 0 && sleptBackoff > 0 && retryIn < sleptBackoff {
				// Store clock advanced during the local sleep and trails only by
				// a small clock-rate slew; sleep the remaining gap and retry.
				// Against a frozen unit-test clock retryIn >= sleptBackoff, so
				// we return nil cleanly.
				sleptBackoff = retryIn
				if err := sleepWithContext(ctx, max(retryIn, time.Millisecond)); err != nil {
					return err
				}
				continue
			}
			return nil
		}

		if err := sleepWithContext(ctx, inlineContentionPollInterval); err != nil {
			return err
		}
	}
}

// cbSignal describes whether an action execution tested Agent Substrate's
// control-plane health.
type cbSignal int

const (
	cbSignalNone cbSignal = iota
	cbSignalHealthy
	cbSignalTransientFailure
)

// actionExecOutcome captures the result of running a single head Action outside
// store.Apply.
type actionExecOutcome struct {
	kind          outcomeKind
	signal        cbSignal
	completed     *lifecycle.ActionCompletedInput
	capacity      *lifecycle.CapacityUnavailableInput
	failed        *lifecycle.ActionFailedInput
	observedActor *lifecycle.ObservedActorInput
}

type outcomeKind int

const (
	outcomeCompleted outcomeKind = iota
	outcomeCapacityUnavailable
	outcomePermanentFailure
	outcomeObservedActor
	outcomeTransientRetry
	outcomeCanceled
)

func (r *Reconciler) processLeasedTask(
	ctx context.Context,
	leased store.LeasedTask,
	leasedMono time.Time,
	isProbe bool,
	inline bool,
	initialSlot func() bool,
	blockedKindOut *lifecycle.ActionKind,
) (deferred bool, retryAfter time.Duration, fatalErr error) {
	probePending := isProbe
	defer func() {
		if initialSlot != nil {
			if initialSlot() {
				r.Notify()
			}
		}
		if probePending {
			r.breaker.CancelProbe()
		}
	}()

	key := leased.Key
	leaseToken := leased.LeaseToken
	row := leased.Row
	leasedAt := leased.LeasedAt
	storeClockAnchor := leased.LeasedAt
	leaseMonoAnchor := leasedMono
	if leaseMonoAnchor.IsZero() {
		leaseMonoAnchor = time.Now()
	}
	lastConfirmedMono := leaseMonoAnchor
	var verifiedUID string

	for ctx.Err() == nil && row != nil {
		if len(row.Actions) == 0 || (!leasedAt.IsZero() && !row.Actions[0].NotBefore.IsZero() && row.Actions[0].NotBefore.After(leasedAt)) {
			if initialSlot != nil {
				if initialSlot() {
					r.Notify()
				}
				initialSlot = nil
			}
			updated, hasMore, stalledTimer, deleted, leaseLost, err := r.handleDueWithoutRunnableAction(ctx, key, leaseToken)
			if deleted {
				return false, 0, nil
			}
			if leaseLost {
				return inline, 0, nil
			}
			if err != nil {
				requeue := r.errorRequeueDelay(ctx, inline)
				r.releaseLeaseBestEffort(key, leaseToken, requeue)
				if inline {
					if errors.Is(err, store.ErrLockTimeout) {
						return true, 0, nil
					}
					return false, 0, err
				}
				return false, 0, nil
			}
			if updated == nil || !hasMore {
				var requeue time.Duration
				if stalledTimer {
					slog.Warn("lifecycle timer transition left timer due without runnable actions; delaying requeue to prevent spin",
						"atespace", key.Atespace,
						"task", key.Name,
					)
					requeue = r.errorRequeueDelay(ctx, inline)
				}
				r.releaseLeaseBestEffort(key, leaseToken, requeue)
				return false, 0, nil
			}
			row = updated
			leasedAt = time.Time{} // updated came from Apply, which already validated hasMore against DB now
			continue
		}

		head := row.Actions[0]
		var releaseSlot func() bool
		if initialSlot != nil {
			releaseSlot = initialSlot
			initialSlot = nil
		} else {
			var admitted bool
			releaseSlot, admitted = r.admission.TryAcquireAction(key.Atespace, head.Kind)
			if !admitted {
				requeue := jitter50Pct(r.requeueDelay)
				if inline {
					requeue = 0
					if blockedKindOut != nil {
						*blockedKindOut = head.Kind
					}
				}
				r.releaseLeaseBestEffort(key, leaseToken, requeue)
				return inline, 0, nil
			}
		}

		var storeNow time.Time
		if !storeClockAnchor.IsZero() {
			storeNow = storeClockAnchor.Add(max(time.Since(leaseMonoAnchor), 0))
		} else {
			storeNow = r.clock()
		}
		outcome, leaseLost, obsoleteCanceled := r.runWithLeaseRenewal(ctx, key, leaseToken, row, head, storeNow, &lastConfirmedMono, &verifiedUID)
		if releaseSlot() {
			r.Notify()
		}

		if leaseLost {
			return inline, 0, nil
		}

		if !inline || r.mode != ModeInline {
			switch outcome.signal {
			case cbSignalHealthy:
				if r.breaker.Record(false, probePending) {
					r.Notify()
				}
				probePending = false
			case cbSignalTransientFailure:
				r.breaker.Record(true, probePending)
				probePending = false
			case cbSignalNone:
			}
		}
		if ctx.Err() != nil || outcome.kind == outcomeCanceled {
			if obsoleteCanceled && ctx.Err() == nil {
				// Head action changed while our call was in flight; re-read
				// under our still-active lease so a replacement action (such as
				// DELETE_ACTOR) runs immediately.
				fresh, ready, deleted, lost, refreshErr := r.refreshUnderLease(ctx, key, leaseToken)
				if deleted {
					return false, 0, nil
				}
				if lost {
					return inline, 0, nil
				}
				if refreshErr != nil {
					r.releaseLeaseBestEffort(key, leaseToken, r.errorRequeueDelay(ctx, inline))
					if inline {
						if errors.Is(refreshErr, store.ErrLockTimeout) {
							return true, 0, nil
						}
						return false, 0, refreshErr
					}
					return false, 0, nil
				}
				if fresh != nil && ready {
					row = fresh
					leasedAt = time.Time{}
					continue
				}
			}
			r.releaseLeaseBestEffort(key, leaseToken, 0)
			return false, 0, nil
		}

		if outcome.kind == outcomeTransientRetry {
			delay := r.computeJitteredBackoff(head.Tries)
			var sleepSeq uint64
			if inline {
				sleepSeq = r.recordInlineRetrySleep(key, delay)
			}
			if err := r.queue.BackoffAction(ctx, key, leaseToken, head.ID, delay); err != nil {
				if inline {
					r.cancelInlineRetrySleep(key, sleepSeq)
				}
				if errors.Is(err, store.ErrLeaseLost) {
					return inline, 0, nil
				}
				slog.Warn("recording transient action backoff failed",
					"atespace", key.Atespace,
					"task", key.Name,
					"actionID", head.ID,
					"kind", head.Kind,
					"error", err,
				)
				requeue := delay
				if inline {
					requeue = 0
				}
				r.releaseLeaseBestEffort(key, leaseToken, requeue)
				if inline {
					if errors.Is(err, store.ErrLockTimeout) {
						return true, 0, nil
					}
					return false, 0, err
				}
				return false, 0, nil
			}
			if inline {
				return false, delay, nil
			}
			return false, 0, nil
		}

		input := outcome.toLifecycleInput()
		var (
			nextReady   bool
			stalledHead bool
		)
		updated, applyErr := r.queue.Apply(ctx, key, func(cur *store.TaskRow, now time.Time) (*store.TaskRow, error) {
			nextReady = false
			stalledHead = false
			if !store.HasActiveLease(cur, leaseToken, now) {
				return nil, store.ErrLeaseLost
			}
			if len(cur.Actions) == 0 || cur.Actions[0].ID != head.ID {
				nextReady = len(cur.Actions) > 0 && (cur.Actions[0].NotBefore.IsZero() || !cur.Actions[0].NotBefore.After(now))
				return nil, nil
			}
			nextState, out, err := r.transition(cur.LifecycleState(), input, now)
			if err != nil {
				return nil, err
			}
			if out.DeleteRow {
				return store.DeleteTaskRow, nil
			}
			next := cur.Clone()
			next.SetLifecycleState(nextState, now)
			nextReady = len(nextState.Actions) > 0 && (nextState.Actions[0].NotBefore.IsZero() || !nextState.Actions[0].NotBefore.After(now))
			stalledHead = nextReady && nextState.Actions[0].ID == head.ID
			return next, nil
		})
		if errors.Is(applyErr, store.ErrLeaseLost) {
			return inline, 0, nil
		}
		if applyErr != nil {
			slog.Warn("applying action result failed",
				"atespace", key.Atespace,
				"task", key.Name,
				"actionID", head.ID,
				"kind", head.Kind,
				"error", applyErr,
			)
			r.releaseLeaseBestEffort(key, leaseToken, r.errorRequeueDelay(ctx, inline))
			if inline {
				if errors.Is(applyErr, store.ErrLockTimeout) {
					return true, 0, nil
				}
				return false, 0, applyErr
			}
			return false, 0, nil
		}
		if updated == nil {
			// Task row was deleted after DELETE_ACTOR completed.
			return false, 0, nil
		}
		if stalledHead {
			// Defensive spin guard: Transition left the same head Action.ID
			// immediately runnable without advancing NotBefore. Back off and
			// release the lease rather than spinning in a hot loop.
			delay := r.computeJitteredBackoff(updated.Actions[0].Tries)
			slog.Warn("lifecycle transition left head action unchanged and runnable; backing off to prevent spin",
				"atespace", key.Atespace,
				"task", key.Name,
				"actionID", head.ID,
				"kind", head.Kind,
			)
			var sleepSeq uint64
			if inline {
				sleepSeq = r.recordInlineRetrySleep(key, delay)
			}
			if err := r.queue.BackoffAction(ctx, key, leaseToken, head.ID, delay); err != nil {
				if errors.Is(err, store.ErrLeaseLost) {
					if inline {
						r.cancelInlineRetrySleep(key, sleepSeq)
					}
					return inline, 0, nil
				}
				r.releaseLeaseBestEffort(key, leaseToken, delay)
			}
			if inline {
				return false, delay, nil
			}
			return false, 0, nil
		}
		row = updated
		leasedAt = time.Time{} // nextReady was verified against DB clock inside Apply
		if !nextReady {
			r.releaseLeaseBestEffort(key, leaseToken, 0)
			return false, 0, nil
		}
	}

	if row != nil {
		r.releaseLeaseBestEffort(key, leaseToken, 0)
	}
	return false, 0, nil
}

func (r *Reconciler) errorRequeueDelay(ctx context.Context, inline bool) time.Duration {
	if ctx.Err() != nil {
		return 0
	}
	if inline {
		return 0
	}
	return jitter50Pct(r.initialBackoff)
}

func (o actionExecOutcome) toLifecycleInput() lifecycle.Input {
	switch o.kind {
	case outcomeCompleted:
		return o.completed
	case outcomeCapacityUnavailable:
		return o.capacity
	case outcomePermanentFailure:
		return o.failed
	case outcomeObservedActor:
		return o.observedActor
	default:
		return nil
	}
}

func (r *Reconciler) handleDueWithoutRunnableAction(ctx context.Context, key store.TaskKey, leaseToken string) (updated *store.TaskRow, hasRunnableAction bool, stalledTimer bool, deleted bool, leaseLost bool, err error) {
	var markedDelete bool
	row, applyErr := r.queue.Apply(ctx, key, func(cur *store.TaskRow, now time.Time) (*store.TaskRow, error) {
		hasRunnableAction = false
		stalledTimer = false
		markedDelete = false
		if !store.HasActiveLease(cur, leaseToken, now) {
			return nil, store.ErrLeaseLost
		}
		if len(cur.Actions) > 0 && (cur.Actions[0].NotBefore.IsZero() || !cur.Actions[0].NotBefore.After(now)) {
			hasRunnableAction = true
			return nil, nil
		}
		st := cur.LifecycleState()
		// Evaluate whether a non-action timer (such as ClaimDeadline) is due even
		// when Actions[0] is still gated in the future by NotBefore > now.
		timerOnlyState := st
		timerOnlyState.Actions = nil
		due := lifecycle.NextReconcileAt(timerOnlyState, now)
		if due == nil || due.After(now) {
			return nil, nil
		}
		nextState, out, transErr := r.transition(st, &lifecycle.TimerExpiredInput{}, now)
		if transErr != nil {
			return nil, transErr
		}
		if out.DeleteRow {
			markedDelete = true
			return store.DeleteTaskRow, nil
		}
		next := cur.Clone()
		next.SetLifecycleState(nextState, now)
		hasRunnableAction = len(nextState.Actions) > 0 && (nextState.Actions[0].NotBefore.IsZero() || !nextState.Actions[0].NotBefore.After(now))
		if !hasRunnableAction {
			nextTimerState := nextState
			nextTimerState.Actions = nil
			if nextDue := lifecycle.NextReconcileAt(nextTimerState, now); nextDue != nil && !nextDue.After(now) {
				stalledTimer = true
			}
		}
		return next, nil
	})
	if errors.Is(applyErr, store.ErrLeaseLost) {
		return nil, false, false, false, true, nil
	}
	if applyErr != nil {
		slog.Warn("applying timer expiration failed", "atespace", key.Atespace, "task", key.Name, "error", applyErr)
		return nil, false, false, false, false, applyErr
	}
	return row, hasRunnableAction, stalledTimer, markedDelete || row == nil, false, nil
}

func (r *Reconciler) refreshUnderLease(ctx context.Context, key store.TaskKey, leaseToken string) (row *store.TaskRow, ready bool, deleted bool, leaseLost bool, err error) {
	cur, applyErr := r.queue.Apply(ctx, key, func(current *store.TaskRow, now time.Time) (*store.TaskRow, error) {
		ready = false
		if current == nil {
			return nil, nil
		}
		if !store.HasActiveLease(current, leaseToken, now) {
			return nil, store.ErrLeaseLost
		}
		ready = len(current.Actions) > 0 && (current.Actions[0].NotBefore.IsZero() || !current.Actions[0].NotBefore.After(now))
		return nil, nil
	})
	if errors.Is(applyErr, store.ErrLeaseLost) {
		return nil, false, false, true, nil
	}
	if applyErr != nil {
		return nil, false, false, false, applyErr
	}
	return cur, ready, cur == nil, false, nil
}

func (r *Reconciler) releaseLeaseBestEffort(key store.TaskKey, leaseToken string, requeueAfter time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), shutdownReleaseTimeout)
	defer cancel()
	if err := r.queue.ReleaseLease(ctx, key, leaseToken, requeueAfter); err != nil && !errors.Is(err, store.ErrLeaseLost) {
		slog.Warn("releasing task lease failed", "atespace", key.Atespace, "task", key.Name, "error", err)
	}
}

func (r *Reconciler) runWithLeaseRenewal(
	parentCtx context.Context,
	key store.TaskKey,
	leaseToken string,
	row *store.TaskRow,
	head lifecycle.Action,
	storeNow time.Time,
	lastConfirmedMono *time.Time,
	verifiedUID *string,
) (outcome actionExecOutcome, leaseLost bool, obsoleteCanceled bool) {
	callCtx, cancelCall := context.WithTimeout(parentCtx, r.actionTimeout)
	renewStop := make(chan struct{})
	renewDone := make(chan struct{})

	var (
		confirmedMonoNext time.Time
		renewed           bool
	)
	confirmedMono := *lastConfirmedMono
	renewCallTimeout := min(r.renewInterval, 5*time.Second)
	renewFailureRetry := max(min(r.renewInterval/3, time.Second), 5*time.Millisecond)

	// When a multi-step lease already reached renewInterval before starting the
	// next step, renew synchronously up front rather than racing a 1ms timer
	// against limiter.Wait and executeAction.
	if time.Since(confirmedMono) >= r.renewInterval {
		attemptMono := time.Now()
		remainingTTL := max(r.leaseTTL-attemptMono.Sub(confirmedMono), time.Millisecond)
		renewCtx, cancelRenew := context.WithTimeout(parentCtx, min(renewCallTimeout, remainingTTL))
		currentHeadID, err := r.queue.RenewLease(renewCtx, key, leaseToken, r.leaseTTL)
		cancelRenew()
		if errors.Is(err, store.ErrLeaseLost) || (err != nil && time.Since(confirmedMono) >= r.leaseTTL) {
			cancelCall()
			return actionExecOutcome{kind: outcomeCanceled, signal: cbSignalNone}, true, false
		}
		if err == nil {
			confirmedMono = attemptMono
			*lastConfirmedMono = confirmedMono
			if currentHeadID != head.ID {
				cancelCall()
				return actionExecOutcome{kind: outcomeCanceled, signal: cbSignalNone}, false, true
			}
		}
	}

	initialWait := max(r.renewInterval-time.Since(confirmedMono), time.Millisecond)

	go func() {
		defer close(renewDone)
		timer := time.NewTimer(initialWait)
		defer timer.Stop()
		for {
			select {
			case <-renewStop:
				return
			case <-callCtx.Done():
				return
			case <-timer.C:
			}
			attemptMono := time.Now()
			remainingTTL := max(r.leaseTTL-attemptMono.Sub(confirmedMono), time.Millisecond)
			renewCtx, cancelRenew := context.WithTimeout(parentCtx, min(renewCallTimeout, remainingTTL))
			currentHeadID, err := r.queue.RenewLease(renewCtx, key, leaseToken, r.leaseTTL)
			cancelRenew()
			if errors.Is(err, store.ErrLeaseLost) {
				slog.Warn("task worker lease lost during action; aborting call",
					"atespace", key.Atespace,
					"task", key.Name,
					"actionID", head.ID,
					"kind", head.Kind,
				)
				leaseLost = true
				cancelCall()
				return
			}
			if err != nil {
				if time.Since(confirmedMono) >= r.leaseTTL {
					slog.Warn("task worker lease renewal timed out past LeaseTTL; aborting call",
						"atespace", key.Atespace,
						"task", key.Name,
						"actionID", head.ID,
						"error", err,
					)
					leaseLost = true
					cancelCall()
					return
				}
				if parentCtx.Err() == nil {
					slog.Warn("renewing task worker lease failed; will retry",
						"atespace", key.Atespace,
						"task", key.Name,
						"actionID", head.ID,
						"error", err,
					)
				}
				timer.Reset(renewFailureRetry)
				continue
			}
			renewed = true
			confirmedMono = attemptMono
			confirmedMonoNext = confirmedMono
			if currentHeadID != head.ID {
				slog.Info("head action superseded during call; canceling obsolete Substrate RPC",
					"atespace", key.Atespace,
					"task", key.Name,
					"obsoleteActionID", head.ID,
					"newHeadActionID", currentHeadID,
				)
				obsoleteCanceled = true
				cancelCall()
				return
			}
			timer.Reset(r.renewInterval)
		}
	}()

	needsMutatingToken := r.limiter != nil &&
		head.Kind != lifecycle.ActionWaitWorkspaceTag &&
		(head.Kind != lifecycle.ActionEnsureEgress || len(r.combinedEgressRules(row.EgressRules)) > 0)

	if needsMutatingToken {
		if waitErr := r.limiter.Wait(callCtx); waitErr != nil {
			close(renewStop)
			cancelCall()
			<-renewDone
			if renewed {
				*lastConfirmedMono = confirmedMonoNext
			}
			if parentCtx.Err() != nil || leaseLost || obsoleteCanceled {
				return actionExecOutcome{kind: outcomeCanceled, signal: cbSignalNone}, leaseLost, obsoleteCanceled
			}
			return actionExecOutcome{kind: outcomeTransientRetry, signal: cbSignalNone}, false, false
		}
	}

	outcome = r.executeAction(parentCtx, callCtx, key, row, head, storeNow, verifiedUID)
	close(renewStop)
	cancelCall()
	<-renewDone
	if renewed {
		*lastConfirmedMono = confirmedMonoNext
	}
	if (parentCtx.Err() != nil || leaseLost || obsoleteCanceled) &&
		(outcome.kind == outcomeTransientRetry || outcome.kind == outcomeCanceled) {
		outcome = actionExecOutcome{kind: outcomeCanceled, signal: cbSignalNone}
	}
	return outcome, leaseLost, obsoleteCanceled
}

func (r *Reconciler) executeAction(parentCtx, ctx context.Context, key store.TaskKey, row *store.TaskRow, action lifecycle.Action, storeNow time.Time, verifiedUID *string) actionExecOutcome {
	atespace := key.Atespace
	actorName := key.Name
	templateName := row.Task.GetStatus().GetAteActorTemplate()

	switch action.Kind {
	case lifecycle.ActionWaitWorkspaceTag:
		wsName := row.Task.GetSpec().GetWorkspace().GetName()
		tag, ready, err := r.tagResolve(ctx, atespace, wsName, templateName, action.Tag)
		sig := cbSignalNone
		if r.defaultTagRes && action.Tag != "" {
			sig = cbSignalHealthy
		}
		if err != nil {
			out := r.classifyFailureOutcome(parentCtx, ctx, key, action, err)
			if !r.defaultTagRes || action.Tag == "" {
				out.signal = cbSignalNone
			}
			return out
		}
		if !ready {
			if !action.Since.IsZero() && storeNow.Sub(action.Since) >= r.maxWorkspaceTagWait {
				msg := fmt.Sprintf("timed out after %s waiting for Workspace snapshot tag %q", r.maxWorkspaceTagWait, action.Tag)
				if action.Tag == "" && wsName != "" {
					msg = fmt.Sprintf("timed out after %s waiting for snapshot tag of Workspace %q", r.maxWorkspaceTagWait, wsName)
				}
				return actionExecOutcome{
					kind:   outcomePermanentFailure,
					signal: sig,
					failed: &lifecycle.ActionFailedInput{
						ActionID: action.ID,
						Kind:     action.Kind,
						Reason:   v1alpha1.TaskStatusReason_TASK_STATUS_REASON_PROVISIONING_FAILED,
						Message:  msg,
					},
				}
			}
			return actionExecOutcome{kind: outcomeTransientRetry, signal: sig}
		}
		if tag == "" {
			tag = action.Tag
		}
		if tag == "" {
			return actionExecOutcome{
				kind:   outcomePermanentFailure,
				signal: cbSignalNone,
				failed: &lifecycle.ActionFailedInput{
					ActionID: action.ID,
					Kind:     action.Kind,
					Reason:   v1alpha1.TaskStatusReason_TASK_STATUS_REASON_PROVISIONING_FAILED,
					Message:  "WAIT_WORKSPACE_TAG resolved ready=true with an empty snapshot tag",
				},
			}
		}
		return actionExecOutcome{
			kind:   outcomeCompleted,
			signal: sig,
			completed: &lifecycle.ActionCompletedInput{
				ActionID:  action.ID,
				Kind:      action.Kind,
				SourceTag: tag,
			},
		}

	case lifecycle.ActionEnsureTemplate:
		if row.EffectiveProfile != nil && row.EffectiveProfile.GetAteActorTemplate() == "" {
			if err := apivalidation.ValidateResources(row.EffectiveProfile.GetResources()); err != nil {
				return actionExecOutcome{
					kind:   outcomePermanentFailure,
					signal: cbSignalNone,
					failed: &lifecycle.ActionFailedInput{
						ActionID: action.ID,
						Kind:     action.Kind,
						Reason:   v1alpha1.TaskStatusReason_TASK_STATUS_REASON_PROVISIONING_FAILED,
						Message:  err.Error(),
					},
				}
			}
		}
		if err := r.runtime.EnsureTemplate(ctx, atespace, templateName, row.EffectiveProfile); err != nil {
			if errors.Is(err, substrate.ErrTemplateSkew) && !action.Since.IsZero() && storeNow.Sub(action.Since) >= r.maxTemplateSkewWait {
				return actionExecOutcome{
					kind:   outcomePermanentFailure,
					signal: cbSignalHealthy,
					failed: &lifecycle.ActionFailedInput{
						ActionID: action.ID,
						Kind:     action.Kind,
						Reason:   v1alpha1.TaskStatusReason_TASK_STATUS_REASON_PROVISIONING_FAILED,
						Message: fmt.Sprintf("timed out after %s waiting for originating replica to publish ActorTemplate %s/%s: %s",
							r.maxTemplateSkewWait, atespace, templateName, substrateMessage(err)),
					},
				}
			}
			return r.classifyFailureOutcome(parentCtx, ctx, key, action, err)
		}
		return actionExecOutcome{
			kind:   outcomeCompleted,
			signal: cbSignalHealthy,
			completed: &lifecycle.ActionCompletedInput{
				ActionID: action.ID,
				Kind:     action.Kind,
			},
		}

	case lifecycle.ActionCreateActor:
		var createdAt time.Time
		if ts := row.Task.GetMetadata().GetCreateTime(); ts != nil {
			createdAt = ts.AsTime()
		}
		uid, err := r.runtime.EnsureActor(substrate.WithTaskCreateTime(ctx, createdAt), atespace, actorName, templateName, action.Tag)
		if err != nil {
			return r.classifyFailureOutcome(parentCtx, ctx, key, action, err)
		}
		if verifiedUID != nil && uid != "" {
			*verifiedUID = uid
		}
		return actionExecOutcome{
			kind:   outcomeCompleted,
			signal: cbSignalHealthy,
			completed: &lifecycle.ActionCompletedInput{
				ActionID: action.ID,
				Kind:     action.Kind,
				ActorUID: uid,
			},
		}

	case lifecycle.ActionEnsureEgress:
		rules := r.combinedEgressRules(row.EgressRules)
		if len(rules) == 0 {
			return actionExecOutcome{
				kind:   outcomeCompleted,
				signal: cbSignalNone,
				completed: &lifecycle.ActionCompletedInput{
					ActionID: action.ID,
					Kind:     action.Kind,
				},
			}
		}
		if _, mismatchOut := r.verifyOwnedActor(parentCtx, ctx, key, row, action, false, verifiedUID); mismatchOut != nil {
			return *mismatchOut
		}
		if err := r.runtime.EnsureActorEgress(ctx, atespace, actorName, rules); err != nil {
			if statusCode(err) == codes.NotFound && row.ActorUID != "" {
				return actionExecOutcome{
					kind:   outcomeObservedActor,
					signal: cbSignalHealthy,
					observedActor: &lifecycle.ObservedActorInput{
						MissingConfirmed: true,
					},
				}
			}
			return r.classifyFailureOutcome(parentCtx, ctx, key, action, err)
		}
		return actionExecOutcome{
			kind:   outcomeCompleted,
			signal: cbSignalHealthy,
			completed: &lifecycle.ActionCompletedInput{
				ActionID: action.ID,
				Kind:     action.Kind,
			},
		}

	case lifecycle.ActionResumeActor:
		liveActor, mismatchOut := r.verifyOwnedActor(parentCtx, ctx, key, row, action, false, verifiedUID)
		if mismatchOut != nil {
			return *mismatchOut
		}
		if liveActor != nil {
			switch liveActor.GetStatus().GetState() {
			case ateapipb.ActorState_ACTOR_STATE_RUNNING:
				return actionExecOutcome{
					kind:   outcomeCompleted,
					signal: cbSignalHealthy,
					completed: &lifecycle.ActionCompletedInput{
						ActionID: action.ID,
						Kind:     action.Kind,
					},
				}
			case ateapipb.ActorState_ACTOR_STATE_CRASHED:
				return actionExecOutcome{
					kind:   outcomeObservedActor,
					signal: cbSignalHealthy,
					observedActor: &lifecycle.ObservedActorInput{
						ActorState:   ateapipb.ActorState_ACTOR_STATE_CRASHED,
						ActorUID:     liveActor.GetMetadata().GetUid(),
						CrashMessage: liveActor.GetStatus().GetCrash().GetMessage(),
					},
				}
			}
		}
		if err := r.runtime.ResumeActor(ctx, atespace, actorName); err != nil {
			switch statusCode(err) {
			case codes.FailedPrecondition:
				return r.resolveResumeFailedPrecondition(parentCtx, ctx, key, row, action, liveActor, err)
			case codes.NotFound:
				if row.ActorUID != "" {
					return actionExecOutcome{
						kind:   outcomeObservedActor,
						signal: cbSignalHealthy,
						observedActor: &lifecycle.ObservedActorInput{
							MissingConfirmed: true,
						},
					}
				}
			}
			return r.classifyFailureOutcome(parentCtx, ctx, key, action, err)
		}
		return actionExecOutcome{
			kind:   outcomeCompleted,
			signal: cbSignalHealthy,
			completed: &lifecycle.ActionCompletedInput{
				ActionID: action.ID,
				Kind:     action.Kind,
			},
		}

	case lifecycle.ActionSuspendActorSync:
		terminalPark := action.SuspendReason == lifecycle.SuspendReasonTerminal ||
			lifecycle.IsTerminal(row.Task.GetStatus().GetState())
		if row.ActorUID == "" && terminalPark {
			// Provisioning failed before CREATE_ACTOR bound an ActorUID;
			// complete terminal parking as a no-op without touching Substrate.
			return actionExecOutcome{
				kind:   outcomeCompleted,
				signal: cbSignalNone,
				completed: &lifecycle.ActionCompletedInput{
					ActionID: action.ID,
					Kind:     action.Kind,
				},
			}
		}
		liveActor, mismatchOut := r.verifyOwnedActor(parentCtx, ctx, key, row, action, terminalPark, verifiedUID)
		if mismatchOut != nil {
			return *mismatchOut
		}
		if liveActor != nil {
			switch liveActor.GetStatus().GetState() {
			case ateapipb.ActorState_ACTOR_STATE_SUSPENDED:
				if substrate.HasPendingSnapshotUpload(liveActor) {
					return actionExecOutcome{kind: outcomeTransientRetry, signal: cbSignalHealthy}
				}
				return actionExecOutcome{
					kind:   outcomeCompleted,
					signal: cbSignalHealthy,
					completed: &lifecycle.ActionCompletedInput{
						ActionID:    action.ID,
						Kind:        action.Kind,
						SnapshotGen: substrate.LatestDurableSnapshotGeneration(liveActor),
					},
				}
			case ateapipb.ActorState_ACTOR_STATE_CRASHED:
				return actionExecOutcome{
					kind:   outcomeObservedActor,
					signal: cbSignalHealthy,
					observedActor: &lifecycle.ObservedActorInput{
						ActorState:   ateapipb.ActorState_ACTOR_STATE_CRASHED,
						ActorUID:     liveActor.GetMetadata().GetUid(),
						CrashMessage: liveActor.GetStatus().GetCrash().GetMessage(),
					},
				}
			}
		}
		snapGen, err := r.runtime.SuspendActorSync(ctx, atespace, actorName)
		if err != nil {
			if errors.Is(err, substrate.ErrSnapshotUploadPending) {
				return actionExecOutcome{kind: outcomeTransientRetry, signal: cbSignalHealthy}
			}
			switch statusCode(err) {
			case codes.FailedPrecondition:
				return r.resolveSuspendFailedPrecondition(parentCtx, ctx, key, row, action, terminalPark, liveActor, err)
			case codes.NotFound:
				if terminalPark {
					return actionExecOutcome{
						kind:   outcomeCompleted,
						signal: cbSignalHealthy,
						completed: &lifecycle.ActionCompletedInput{
							ActionID: action.ID,
							Kind:     action.Kind,
						},
					}
				}
				if row.ActorUID != "" {
					return actionExecOutcome{
						kind:   outcomeObservedActor,
						signal: cbSignalHealthy,
						observedActor: &lifecycle.ObservedActorInput{
							MissingConfirmed: true,
						},
					}
				}
			}
			return r.classifyFailureOutcome(parentCtx, ctx, key, action, err)
		}
		return actionExecOutcome{
			kind:   outcomeCompleted,
			signal: cbSignalHealthy,
			completed: &lifecycle.ActionCompletedInput{
				ActionID:    action.ID,
				Kind:        action.Kind,
				SnapshotGen: snapGen,
			},
		}

	case lifecycle.ActionRevertActor:
		terminalRevert := lifecycle.IsTerminal(row.Task.GetStatus().GetState())
		if _, mismatchOut := r.verifyOwnedActor(parentCtx, ctx, key, row, action, terminalRevert, verifiedUID); mismatchOut != nil {
			return *mismatchOut
		}
		snapGen, err := r.runtime.RevertActor(ctx, atespace, actorName)
		if err != nil {
			if errors.Is(err, substrate.ErrRevertPreconditionSettled) && action.Tries >= maxSteadyStatePreconditionRetries {
				return actionExecOutcome{
					kind:   outcomePermanentFailure,
					signal: cbSignalHealthy,
					failed: &lifecycle.ActionFailedInput{
						ActionID: action.ID,
						Kind:     action.Kind,
						Reason:   v1alpha1.TaskStatusReason_TASK_STATUS_REASON_PROVISIONING_FAILED,
						Message:  substrateMessage(err),
					},
				}
			}
			if statusCode(err) == codes.NotFound {
				if terminalRevert {
					return actionExecOutcome{
						kind:   outcomeCompleted,
						signal: cbSignalHealthy,
						completed: &lifecycle.ActionCompletedInput{
							ActionID: action.ID,
							Kind:     action.Kind,
						},
					}
				}
				if row.ActorUID != "" {
					return actionExecOutcome{
						kind:   outcomeObservedActor,
						signal: cbSignalHealthy,
						observedActor: &lifecycle.ObservedActorInput{
							MissingConfirmed: true,
						},
					}
				}
			}
			return r.classifyFailureOutcome(parentCtx, ctx, key, action, err)
		}
		return actionExecOutcome{
			kind:   outcomeCompleted,
			signal: cbSignalHealthy,
			completed: &lifecycle.ActionCompletedInput{
				ActionID:    action.ID,
				Kind:        action.Kind,
				SnapshotGen: snapGen,
			},
		}

	case lifecycle.ActionDeleteActor:
		targetUID := row.ActorUID
		if targetUID == "" {
			existing, getErr := r.runtime.GetActor(ctx, atespace, actorName)
			if statusCode(getErr) == codes.NotFound {
				return actionExecOutcome{
					kind:   outcomeCompleted,
					signal: cbSignalHealthy,
					completed: &lifecycle.ActionCompletedInput{
						ActionID: action.ID,
						Kind:     action.Kind,
					},
				}
			}
			if getErr != nil {
				return r.classifyFailureOutcome(parentCtx, ctx, key, action, getErr)
			}
			if existing.GetStatus().GetState() == ateapipb.ActorState_ACTOR_STATE_DELETING ||
				existing.GetMetadata().GetDeleteTime() != nil {
				// Asynchronous actor deletion is still finalizing on Substrate
				// (for example from a prior DELETE_ACTOR attempt on an
				// uncommitted orphan); wait until GetActor confirms NotFound
				// before removing the task row.
				return actionExecOutcome{
					kind:   outcomeTransientRetry,
					signal: cbSignalHealthy,
				}
			}
			var createdAt time.Time
			if ts := row.Task.GetMetadata().GetCreateTime(); ts != nil {
				createdAt = ts.AsTime()
			}
			if !substrate.IsUncommittedTaskActorOrphan(existing, templateName, "", false, createdAt) {
				slog.Warn("skipping actor deletion for unowned foreign actor",
					"atespace", atespace,
					"actor", actorName,
					"actorState", existing.GetStatus().GetState(),
					"actorTemplate", existing.GetActorTemplate().GetName(),
					"wantTemplate", templateName,
				)
				return actionExecOutcome{
					kind:   outcomeCompleted,
					signal: cbSignalHealthy,
					completed: &lifecycle.ActionCompletedInput{
						ActionID: action.ID,
						Kind:     action.Kind,
					},
				}
			}
			if _, polErr := r.runtime.GetActorEgressPolicy(ctx, atespace, actorName); polErr == nil {
				slog.Warn("skipping actor deletion for pre-existing actor carrying an egress policy",
					"atespace", atespace,
					"actor", actorName,
				)
				return actionExecOutcome{
					kind:   outcomeCompleted,
					signal: cbSignalHealthy,
					completed: &lifecycle.ActionCompletedInput{
						ActionID: action.ID,
						Kind:     action.Kind,
					},
				}
			} else if statusCode(polErr) != codes.NotFound {
				return r.classifyFailureOutcome(parentCtx, ctx, key, action, polErr)
			}
			targetUID = existing.GetMetadata().GetUid()
		}
		if err := r.runtime.DeleteActor(ctx, atespace, actorName, targetUID); err != nil {
			return r.classifyFailureOutcome(parentCtx, ctx, key, action, err)
		}
		return actionExecOutcome{
			kind:   outcomeCompleted,
			signal: cbSignalHealthy,
			completed: &lifecycle.ActionCompletedInput{
				ActionID: action.ID,
				Kind:     action.Kind,
			},
		}

	case lifecycle.ActionCreateTag:
		liveActor, mismatchOut := r.verifyOwnedActor(parentCtx, ctx, key, row, action, false, verifiedUID)
		if mismatchOut != nil {
			return *mismatchOut
		}
		if liveActor != nil && liveActor.GetStatus().GetState() == ateapipb.ActorState_ACTOR_STATE_CRASHED {
			return actionExecOutcome{
				kind:   outcomeObservedActor,
				signal: cbSignalHealthy,
				observedActor: &lifecycle.ObservedActorInput{
					ActorState:   ateapipb.ActorState_ACTOR_STATE_CRASHED,
					ActorUID:     liveActor.GetMetadata().GetUid(),
					CrashMessage: liveActor.GetStatus().GetCrash().GetMessage(),
				},
			}
		}
		if err := r.runtime.CreateSnapshotTag(ctx, atespace, actorName, action.Tag); err != nil {
			switch statusCode(err) {
			case codes.NotFound:
				if row.ActorUID != "" {
					return actionExecOutcome{
						kind:   outcomeObservedActor,
						signal: cbSignalHealthy,
						observedActor: &lifecycle.ObservedActorInput{
							MissingConfirmed: true,
						},
					}
				}
			case codes.FailedPrecondition:
				return r.resolveCreateTagFailedPrecondition(parentCtx, ctx, key, row, action, liveActor, err)
			}
			return r.classifyFailureOutcome(parentCtx, ctx, key, action, err)
		}
		return actionExecOutcome{
			kind:   outcomeCompleted,
			signal: cbSignalHealthy,
			completed: &lifecycle.ActionCompletedInput{
				ActionID: action.ID,
				Kind:     action.Kind,
			},
		}

	case lifecycle.ActionDeleteTag:
		if err := r.runtime.DeleteSnapshotTag(ctx, atespace, action.Tag); err != nil {
			return r.classifyFailureOutcome(parentCtx, ctx, key, action, err)
		}
		return actionExecOutcome{
			kind:   outcomeCompleted,
			signal: cbSignalHealthy,
			completed: &lifecycle.ActionCompletedInput{
				ActionID: action.ID,
				Kind:     action.Kind,
			},
		}

	default:
		return actionExecOutcome{
			kind:   outcomePermanentFailure,
			signal: cbSignalNone,
			failed: &lifecycle.ActionFailedInput{
				ActionID: action.ID,
				Kind:     action.Kind,
				Reason:   v1alpha1.TaskStatusReason_TASK_STATUS_REASON_PROVISIONING_FAILED,
				Message:  fmt.Sprintf("unknown task action kind %q", action.Kind),
			},
		}
	}
}

// verifyOwnedActor confirms before a post-create actor mutation that the live
// Actor on Substrate matches row.ActorUID so AX never mutates a foreign or
// replacement actor. Within the same uninterrupted worker lease, once
// row.ActorUID has been verified (or created via ActionCreateActor),
// verifiedUID avoids redundant GetActor calls on subsequent steps.
func (r *Reconciler) verifyOwnedActor(
	parentCtx, ctx context.Context,
	key store.TaskKey,
	row *store.TaskRow,
	action lifecycle.Action,
	completeIfGone bool,
	verifiedUID *string,
) (*ateapipb.Actor, *actionExecOutcome) {
	if row.ActorUID == "" {
		if completeIfGone {
			out := actionExecOutcome{
				kind:   outcomeCompleted,
				signal: cbSignalNone,
				completed: &lifecycle.ActionCompletedInput{
					ActionID: action.ID,
					Kind:     action.Kind,
				},
			}
			return nil, &out
		}
		out := actionExecOutcome{
			kind:   outcomePermanentFailure,
			signal: cbSignalNone,
			failed: &lifecycle.ActionFailedInput{
				ActionID: action.ID,
				Kind:     action.Kind,
				Reason:   v1alpha1.TaskStatusReason_TASK_STATUS_REASON_PROVISIONING_FAILED,
				Message:  fmt.Sprintf("cannot execute %s on %s/%s before CREATE_ACTOR records ActorUID", action.Kind, key.Atespace, key.Name),
			},
		}
		return nil, &out
	}

	if verifiedUID != nil && *verifiedUID == row.ActorUID {
		return nil, nil
	}

	actor, getErr := r.runtime.GetActor(ctx, key.Atespace, key.Name)
	if getErr != nil {
		if statusCode(getErr) == codes.NotFound {
			if completeIfGone {
				out := actionExecOutcome{
					kind:   outcomeCompleted,
					signal: cbSignalHealthy,
					completed: &lifecycle.ActionCompletedInput{
						ActionID: action.ID,
						Kind:     action.Kind,
					},
				}
				return nil, &out
			}
			out := actionExecOutcome{
				kind:   outcomeObservedActor,
				signal: cbSignalHealthy,
				observedActor: &lifecycle.ObservedActorInput{
					MissingConfirmed: true,
				},
			}
			return nil, &out
		}
		out := r.classifyFailureOutcome(parentCtx, ctx, key, action, getErr)
		return nil, &out
	}

	gotUID := actor.GetMetadata().GetUid()
	if gotUID != "" && gotUID != row.ActorUID {
		if completeIfGone {
			out := actionExecOutcome{
				kind:   outcomeCompleted,
				signal: cbSignalHealthy,
				completed: &lifecycle.ActionCompletedInput{
					ActionID: action.ID,
					Kind:     action.Kind,
				},
			}
			return nil, &out
		}
		out := actionExecOutcome{
			kind:   outcomeObservedActor,
			signal: cbSignalHealthy,
			observedActor: &lifecycle.ObservedActorInput{
				MissingConfirmed: true,
			},
		}
		return nil, &out
	}
	if verifiedUID != nil {
		*verifiedUID = row.ActorUID
	}
	return actor, nil
}

// unchangedSteadyStateVersion reports whether preActor and postActor share the
// same steady ActorState and positive Metadata.Version, proving the actor did
// not transition between the pre-RPC check and the post-error GetActor lookup.
func unchangedSteadyStateVersion(preActor, postActor *ateapipb.Actor, steady ateapipb.ActorState) bool {
	if preActor == nil || postActor == nil {
		return false
	}
	if preActor.GetStatus().GetState() != steady || postActor.GetStatus().GetState() != steady {
		return false
	}
	v := preActor.GetMetadata().GetVersion()
	return v > 0 && v == postActor.GetMetadata().GetVersion()
}

// resolveSuspendFailedPrecondition inspects the live Actor when
// SuspendActorSync fails with FailedPrecondition:
//   - Mismatched ActorUID (an actor incarnation not owned by this task):
//     reports ObservedActorInput{MissingConfirmed: true} (or completes no-op
//     terminal parking) without modifying the unrelated actor.
//   - SUSPENDED: retries transiently while HasPendingSnapshotUpload is true,
//     otherwise treats as idempotent completion using
//     LatestDurableSnapshotGeneration.
//   - CRASHED: feeds ObservedActorInput{ActorState: ACTOR_STATE_CRASHED} into
//     Transition so the state machine either runs crash recovery or replaces
//     SUSPEND_ACTOR_SYNC with REVERT_ACTOR (preserving RunnerTokenGen rollback
//     tracking).
//   - DELETING: completes no-op terminal parking or retries transiently until
//     NotFound confirms the actor is gone.
//   - RUNNING: retries up to maxSteadyStatePreconditionRetries when the actor
//     just finished RESUMING -> RUNNING, then classifies as permanent failure.
//   - NotFound: feeds ObservedActorInput{MissingConfirmed: true} when
//     row.ActorUID is set (or completes terminal parking).
//   - Transitional states (RESUMING, SUSPENDING, REVERTING): retries transiently.
func (r *Reconciler) resolveSuspendFailedPrecondition(
	parentCtx, ctx context.Context,
	key store.TaskKey,
	row *store.TaskRow,
	action lifecycle.Action,
	terminalPark bool,
	preActor *ateapipb.Actor,
	suspendErr error,
) actionExecOutcome {
	actor, getErr := r.runtime.GetActor(ctx, key.Atespace, key.Name)
	if getErr != nil {
		if statusCode(getErr) == codes.NotFound {
			if terminalPark {
				return actionExecOutcome{
					kind:   outcomeCompleted,
					signal: cbSignalHealthy,
					completed: &lifecycle.ActionCompletedInput{
						ActionID: action.ID,
						Kind:     action.Kind,
					},
				}
			}
			if row.ActorUID != "" {
				return actionExecOutcome{
					kind:   outcomeObservedActor,
					signal: cbSignalHealthy,
					observedActor: &lifecycle.ObservedActorInput{
						MissingConfirmed: true,
					},
				}
			}
			return r.classifyFailureOutcome(parentCtx, ctx, key, action, suspendErr)
		}
		return r.classifyFailureOutcome(parentCtx, ctx, key, action, getErr)
	}

	actorUID := actor.GetMetadata().GetUid()
	if row.ActorUID != "" && actorUID != "" && actorUID != row.ActorUID {
		if terminalPark {
			return actionExecOutcome{
				kind:   outcomeCompleted,
				signal: cbSignalHealthy,
				completed: &lifecycle.ActionCompletedInput{
					ActionID: action.ID,
					Kind:     action.Kind,
				},
			}
		}
		return actionExecOutcome{
			kind:   outcomeObservedActor,
			signal: cbSignalHealthy,
			observedActor: &lifecycle.ObservedActorInput{
				MissingConfirmed: true,
			},
		}
	}

	switch actor.GetStatus().GetState() {
	case ateapipb.ActorState_ACTOR_STATE_SUSPENDED:
		if substrate.HasPendingSnapshotUpload(actor) {
			return actionExecOutcome{
				kind:   outcomeTransientRetry,
				signal: cbSignalHealthy,
			}
		}
		return actionExecOutcome{
			kind:   outcomeCompleted,
			signal: cbSignalHealthy,
			completed: &lifecycle.ActionCompletedInput{
				ActionID:    action.ID,
				Kind:        action.Kind,
				SnapshotGen: substrate.LatestDurableSnapshotGeneration(actor),
			},
		}

	case ateapipb.ActorState_ACTOR_STATE_CRASHED:
		return actionExecOutcome{
			kind:   outcomeObservedActor,
			signal: cbSignalHealthy,
			observedActor: &lifecycle.ObservedActorInput{
				ActorState:   ateapipb.ActorState_ACTOR_STATE_CRASHED,
				ActorUID:     actorUID,
				CrashMessage: actor.GetStatus().GetCrash().GetMessage(),
			},
		}

	case ateapipb.ActorState_ACTOR_STATE_DELETING:
		if terminalPark {
			return actionExecOutcome{
				kind:   outcomeCompleted,
				signal: cbSignalHealthy,
				completed: &lifecycle.ActionCompletedInput{
					ActionID: action.ID,
					Kind:     action.Kind,
				},
			}
		}
		return actionExecOutcome{
			kind:   outcomeTransientRetry,
			signal: cbSignalHealthy,
		}

	case ateapipb.ActorState_ACTOR_STATE_RUNNING:
		if !unchangedSteadyStateVersion(preActor, actor, ateapipb.ActorState_ACTOR_STATE_RUNNING) &&
			action.Tries < maxSteadyStatePreconditionRetries {
			return actionExecOutcome{
				kind:   outcomeTransientRetry,
				signal: cbSignalHealthy,
			}
		}
		return r.classifyFailureOutcome(parentCtx, ctx, key, action, suspendErr)

	case ateapipb.ActorState_ACTOR_STATE_RESUMING,
		ateapipb.ActorState_ACTOR_STATE_SUSPENDING,
		ateapipb.ActorState_ACTOR_STATE_REVERTING:
		return actionExecOutcome{
			kind:   outcomeTransientRetry,
			signal: cbSignalHealthy,
		}

	default:
		return r.classifyFailureOutcome(parentCtx, ctx, key, action, suspendErr)
	}
}

func (r *Reconciler) resolveResumeFailedPrecondition(
	parentCtx, ctx context.Context,
	key store.TaskKey,
	row *store.TaskRow,
	action lifecycle.Action,
	preActor *ateapipb.Actor,
	resumeErr error,
) actionExecOutcome {
	actor, getErr := r.runtime.GetActor(ctx, key.Atespace, key.Name)
	if getErr != nil {
		if statusCode(getErr) == codes.NotFound && row.ActorUID != "" {
			return actionExecOutcome{
				kind:   outcomeObservedActor,
				signal: cbSignalHealthy,
				observedActor: &lifecycle.ObservedActorInput{
					MissingConfirmed: true,
				},
			}
		}
		return r.classifyFailureOutcome(parentCtx, ctx, key, action, getErr)
	}

	actorUID := actor.GetMetadata().GetUid()
	if row.ActorUID != "" && actorUID != "" && actorUID != row.ActorUID {
		return actionExecOutcome{
			kind:   outcomeObservedActor,
			signal: cbSignalHealthy,
			observedActor: &lifecycle.ObservedActorInput{
				MissingConfirmed: true,
			},
		}
	}

	switch actor.GetStatus().GetState() {
	case ateapipb.ActorState_ACTOR_STATE_RUNNING:
		return actionExecOutcome{
			kind:   outcomeCompleted,
			signal: cbSignalHealthy,
			completed: &lifecycle.ActionCompletedInput{
				ActionID: action.ID,
				Kind:     action.Kind,
			},
		}
	case ateapipb.ActorState_ACTOR_STATE_CRASHED:
		return actionExecOutcome{
			kind:   outcomeObservedActor,
			signal: cbSignalHealthy,
			observedActor: &lifecycle.ObservedActorInput{
				ActorState:   ateapipb.ActorState_ACTOR_STATE_CRASHED,
				ActorUID:     actorUID,
				CrashMessage: actor.GetStatus().GetCrash().GetMessage(),
			},
		}
	case ateapipb.ActorState_ACTOR_STATE_SUSPENDED:
		if substrate.HasPendingSnapshotUpload(actor) {
			return actionExecOutcome{
				kind:   outcomeTransientRetry,
				signal: cbSignalHealthy,
			}
		}
		if !unchangedSteadyStateVersion(preActor, actor, ateapipb.ActorState_ACTOR_STATE_SUSPENDED) &&
			action.Tries < maxSteadyStatePreconditionRetries {
			return actionExecOutcome{
				kind:   outcomeTransientRetry,
				signal: cbSignalHealthy,
			}
		}
		return r.classifyFailureOutcome(parentCtx, ctx, key, action, resumeErr)
	case ateapipb.ActorState_ACTOR_STATE_RESUMING,
		ateapipb.ActorState_ACTOR_STATE_SUSPENDING,
		ateapipb.ActorState_ACTOR_STATE_REVERTING,
		ateapipb.ActorState_ACTOR_STATE_DELETING:
		return actionExecOutcome{
			kind:   outcomeTransientRetry,
			signal: cbSignalHealthy,
		}
	default:
		return r.classifyFailureOutcome(parentCtx, ctx, key, action, resumeErr)
	}
}

func (r *Reconciler) resolveCreateTagFailedPrecondition(
	parentCtx, ctx context.Context,
	key store.TaskKey,
	row *store.TaskRow,
	action lifecycle.Action,
	preActor *ateapipb.Actor,
	tagErr error,
) actionExecOutcome {
	actor, getErr := r.runtime.GetActor(ctx, key.Atespace, key.Name)
	if getErr != nil {
		if statusCode(getErr) == codes.NotFound && row.ActorUID != "" {
			return actionExecOutcome{
				kind:   outcomeObservedActor,
				signal: cbSignalHealthy,
				observedActor: &lifecycle.ObservedActorInput{
					MissingConfirmed: true,
				},
			}
		}
		return r.classifyFailureOutcome(parentCtx, ctx, key, action, getErr)
	}

	actorUID := actor.GetMetadata().GetUid()
	if row.ActorUID != "" && actorUID != "" && actorUID != row.ActorUID {
		return actionExecOutcome{
			kind:   outcomeObservedActor,
			signal: cbSignalHealthy,
			observedActor: &lifecycle.ObservedActorInput{
				MissingConfirmed: true,
			},
		}
	}

	switch actor.GetStatus().GetState() {
	case ateapipb.ActorState_ACTOR_STATE_CRASHED:
		return actionExecOutcome{
			kind:   outcomeObservedActor,
			signal: cbSignalHealthy,
			observedActor: &lifecycle.ObservedActorInput{
				ActorState:   ateapipb.ActorState_ACTOR_STATE_CRASHED,
				ActorUID:     actorUID,
				CrashMessage: actor.GetStatus().GetCrash().GetMessage(),
			},
		}
	case ateapipb.ActorState_ACTOR_STATE_SUSPENDED:
		if substrate.HasPendingSnapshotUpload(actor) {
			return actionExecOutcome{kind: outcomeTransientRetry, signal: cbSignalHealthy}
		}
		if substrate.LatestDurableSnapshotGeneration(actor) > 0 &&
			!unchangedSteadyStateVersion(preActor, actor, ateapipb.ActorState_ACTOR_STATE_SUSPENDED) &&
			action.Tries < maxSteadyStatePreconditionRetries {
			return actionExecOutcome{kind: outcomeTransientRetry, signal: cbSignalHealthy}
		}
		return r.classifyFailureOutcome(parentCtx, ctx, key, action, tagErr)
	case ateapipb.ActorState_ACTOR_STATE_RUNNING:
		// If an external ingress request woke the source actor between
		// SUSPEND_ACTOR_SYNC and CREATE_TAG, re-suspend it synchronously and
		// retry CREATE_TAG cleanly.
		if _, susErr := r.runtime.SuspendActorSync(ctx, key.Atespace, key.Name); susErr != nil {
			if substrate.IsHealthyRetry(susErr) {
				return actionExecOutcome{kind: outcomeTransientRetry, signal: cbSignalHealthy}
			}
			switch statusCode(susErr) {
			case codes.FailedPrecondition:
				out := r.resolveSuspendFailedPrecondition(parentCtx, ctx, key, row, action, false, actor, susErr)
				if out.kind == outcomeCompleted {
					// SuspendActorSync converged on SUSPENDED, so CREATE_TAG can
					// now proceed cleanly on the next attempt.
					return actionExecOutcome{kind: outcomeTransientRetry, signal: cbSignalHealthy}
				}
				return out
			case codes.NotFound:
				if row.ActorUID != "" {
					return actionExecOutcome{
						kind:   outcomeObservedActor,
						signal: cbSignalHealthy,
						observedActor: &lifecycle.ObservedActorInput{
							MissingConfirmed: true,
						},
					}
				}
			}
			return r.classifyFailureOutcome(parentCtx, ctx, key, action, susErr)
		}
		return actionExecOutcome{kind: outcomeTransientRetry, signal: cbSignalHealthy}
	case ateapipb.ActorState_ACTOR_STATE_RESUMING,
		ateapipb.ActorState_ACTOR_STATE_SUSPENDING,
		ateapipb.ActorState_ACTOR_STATE_REVERTING,
		ateapipb.ActorState_ACTOR_STATE_DELETING:
		return actionExecOutcome{kind: outcomeTransientRetry, signal: cbSignalHealthy}
	default:
		return r.classifyFailureOutcome(parentCtx, ctx, key, action, tagErr)
	}
}

func (r *Reconciler) classifyFailureOutcome(parentCtx, callCtx context.Context, _ store.TaskKey, action lifecycle.Action, err error) actionExecOutcome {
	if parentCtx.Err() != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
		statusCode(err) == codes.Canceled || statusCode(err) == codes.DeadlineExceeded) {
		return actionExecOutcome{kind: outcomeCanceled, signal: cbSignalNone}
	}
	switch classifySubstrateError(callCtx, action.Kind, err) {
	case errorClassCanceled:
		return actionExecOutcome{kind: outcomeCanceled, signal: cbSignalNone}
	case errorClassCapacityUnavailable:
		return actionExecOutcome{
			kind:   outcomeCapacityUnavailable,
			signal: cbSignalHealthy,
			capacity: &lifecycle.CapacityUnavailableInput{
				ActionID: action.ID,
				Message:  substrateMessage(err),
			},
		}
	case errorClassPermanent:
		return actionExecOutcome{
			kind:   outcomePermanentFailure,
			signal: cbSignalNone,
			failed: &lifecycle.ActionFailedInput{
				ActionID: action.ID,
				Kind:     action.Kind,
				Reason:   v1alpha1.TaskStatusReason_TASK_STATUS_REASON_PROVISIONING_FAILED,
				Message:  substrateMessage(err),
			},
		}
	default:
		if substrate.IsHealthyRetry(err) || statusCode(err) == codes.Aborted {
			return actionExecOutcome{kind: outcomeTransientRetry, signal: cbSignalHealthy}
		}
		return actionExecOutcome{kind: outcomeTransientRetry, signal: cbSignalTransientFailure}
	}
}

func (r *Reconciler) combinedEgressRules(taskRules []*ateapipb.EgressRule) []*ateapipb.EgressRule {
	if len(r.extraRules) == 0 {
		return taskRules
	}
	combined := make([]*ateapipb.EgressRule, 0, len(r.extraRules)+len(taskRules))
	combined = append(combined, r.extraRules...)
	combined = append(combined, taskRules...)
	return combined
}

func (r *Reconciler) defaultResolveWorkspaceTag(ctx context.Context, atespace, _, _, requestedTag string) (string, bool, error) {
	if requestedTag == "" {
		return "", false, status.Error(codes.FailedPrecondition, "WAIT_WORKSPACE_TAG requires a non-empty action tag or configured WorkspaceTagResolver")
	}
	tag, err := r.runtime.GetTag(ctx, atespace, requestedTag)
	if err != nil {
		if statusCode(err) == codes.NotFound {
			return "", false, nil
		}
		return "", false, err
	}
	if tag.GetMetadata().GetDeleteTime() != nil {
		return "", false, nil
	}
	if tag.GetStatus().GetSnapshot().GetDurableSnapshot().GetState() != ateapipb.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_COMPLETED {
		return "", false, nil
	}
	return requestedTag, true, nil
}

// computeJitteredBackoff returns a delay drawn uniformly from [0.5x, 1.5x] of
// min(initialBackoff * 2^tries, maxBackoff).
func (r *Reconciler) computeJitteredBackoff(tries int) time.Duration {
	if tries < 0 {
		tries = 0
	}
	shift := min(tries, 30)
	nominal := r.initialBackoff
	for range shift {
		if nominal >= r.maxBackoff {
			nominal = r.maxBackoff
			break
		}
		nominal *= 2
	}
	if nominal > r.maxBackoff {
		nominal = r.maxBackoff
	}
	return jitter50Pct(nominal)
}

func jitter50Pct(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	factor := 0.5 + rand.Float64() // [0.5, 1.5)
	return time.Duration(float64(d) * factor)
}

func jitter20Pct(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	factor := 0.8 + 0.4*rand.Float64() // [0.8, 1.2)
	return time.Duration(float64(d) * factor)
}
