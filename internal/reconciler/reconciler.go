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
// Tasks store their pending Substrate operations on their storage row alongside
// a wakeup timestamp (next_reconcile_at). Each ax-server replica runs a
// [Reconciler] whose poller leases due task rows for 30 seconds, executes each
// task's actions sequentially outside any database transaction, renews the
// lease every 10 seconds while calls are in flight (canceling obsolete calls if
// a newer Apply replaced the head action), and commits results through
// [store.TaskApplier.Apply] fenced on both the lease token and head Action.ID.
//
// TODO (deferred until basic reconciliation is wired end-to-end):
//   - Background actor drift sweep (ListActors) and runner heartbeat liveness.
//   - Per-atespace fairness and per-action-kind admission slots (+ LeaseDue pushdown).
//   - Outbound mutating Substrate QPS rate limiting and outage circuit breaker.
//   - Rolling-upgrade template-skew retry and pre-existing foreign-actor guards.
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

	"github.com/google/ax/internal/lifecycle"
	"github.com/google/ax/internal/store"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"github.com/google/uuid"
	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
)

const (
	// DefaultLeaseTTL is the duration for which a worker leases a due task row.
	DefaultLeaseTTL = store.DefaultLeaseTTL
	// DefaultActionTimeout bounds the wall-clock duration of a single Substrate
	// action call before it is canceled and retried transiently with backoff.
	DefaultActionTimeout = 2 * time.Minute
	// DefaultPollInterval is the nominal background queue polling cadence
	// (jittered ±20%).
	DefaultPollInterval = time.Second
	// DefaultBatchSize is the maximum number of due tasks leased in one query.
	DefaultBatchSize = 64
	// DefaultMaxInFlightTasks bounds concurrent leased tasks per replica.
	DefaultMaxInFlightTasks = 512
	// DefaultMaxWorkspaceTagWait bounds how long ActionWaitWorkspaceTag waits
	// for a preparing Workspace snapshot Tag to reach COMPLETED state before
	// failing with PROVISIONING_FAILED.
	DefaultMaxWorkspaceTagWait = 10 * time.Minute

	leaseQueryMaxTimeout   = 10 * time.Second
	shutdownReleaseTimeout = 5 * time.Second
)

// WorkspaceTagResolver resolves whether a preparing Workspace snapshot Tag is
// ready for [lifecycle.ActionWaitWorkspaceTag].
//
// Returning ready == true with a non-empty tag completes WAIT_WORKSPACE_TAG and
// passes tag as ActionCompletedInput.SourceTag; returning ready == false with
// err == nil backs off up to MaxWorkspaceTagWait and re-checks later.
type WorkspaceTagResolver func(ctx context.Context, atespace, workspaceName, templateName, requestedTag string) (tag string, ready bool, err error)

// Config configures a [Reconciler]. Zero values use production defaults.
type Config struct {
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
	// (default 2m).
	ActionTimeout time.Duration
	// PollInterval is how often Run polls for due tasks in addition to
	// immediate local Notify wakeups (default 1s ± 20% jitter).
	PollInterval time.Duration
	// BatchSize is the maximum number of due tasks leased in a single query
	// (default 64).
	BatchSize int
	// MaxInFlightTasks bounds concurrent leased tasks on this replica (default 512).
	MaxInFlightTasks int

	// InitialBackoff is the base delay for transient action retries (default 1s).
	InitialBackoff time.Duration
	// MaxBackoff caps transient action retry delays (default 60s).
	MaxBackoff time.Duration
	// MaxWorkspaceTagWait bounds how long ActionWaitWorkspaceTag waits for a
	// preparing Workspace snapshot Tag before failing permanently (default 10m).
	MaxWorkspaceTagWait time.Duration
}

// Reconciler drives queued Agent Substrate actions and lifecycle timers for AX
// Tasks.
type Reconciler struct {
	queue      store.TaskQueue
	runtime    RuntimeClient
	transition lifecycle.TransitionFunc
	tagResolve WorkspaceTagResolver
	extraRules []*ateapipb.EgressRule

	workerID            string
	leaseTTL            time.Duration
	renewInterval       time.Duration
	actionTimeout       time.Duration
	pollInterval        time.Duration
	batchSize           int
	initialBackoff      time.Duration
	maxBackoff          time.Duration
	maxWorkspaceTagWait time.Duration

	sem       chan struct{}
	wakeCh    chan struct{}
	wg        sync.WaitGroup
	executors map[lifecycle.ActionKind]ActionExecutor
}

// New constructs a [Reconciler] over queue and runtime.
func New(queue store.TaskQueue, runtime RuntimeClient, cfg Config) *Reconciler {
	if queue == nil {
		panic("reconciler: New requires a non-nil store.TaskQueue")
	}
	if runtime == nil {
		panic("reconciler: New requires a non-nil RuntimeClient")
	}
	if cfg.Transition == nil {
		panic("reconciler: New requires a non-nil Config.Transition")
	}

	workerID := cfg.WorkerID
	if workerID == "" {
		host, _ := os.Hostname()
		if host == "" {
			host = "ax-server"
		}
		workerID = fmt.Sprintf("%s-%s", host, uuid.NewString()[:8])
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
	maxTasks := cfg.MaxInFlightTasks
	if maxTasks <= 0 {
		maxTasks = DefaultMaxInFlightTasks
	}
	initialBackoff := cfg.InitialBackoff
	if initialBackoff <= 0 {
		initialBackoff = lifecycle.DefaultActionInitialBackoff
	}
	maxBackoff := cfg.MaxBackoff
	if maxBackoff <= 0 {
		maxBackoff = lifecycle.DefaultActionMaxBackoff
	}
	maxWorkspaceTagWait := cfg.MaxWorkspaceTagWait
	if maxWorkspaceTagWait <= 0 {
		maxWorkspaceTagWait = DefaultMaxWorkspaceTagWait
	}

	var extraRules []*ateapipb.EgressRule
	for _, rule := range cfg.ExtraEgressRules {
		if rule != nil {
			extraRules = append(extraRules, proto.Clone(rule).(*ateapipb.EgressRule))
		}
	}

	r := &Reconciler{
		queue:               queue,
		runtime:             runtime,
		transition:          cfg.Transition,
		tagResolve:          cfg.WorkspaceTagResolver,
		extraRules:          extraRules,
		workerID:            workerID,
		leaseTTL:            leaseTTL,
		renewInterval:       renewInterval,
		actionTimeout:       actionTimeout,
		pollInterval:        pollInterval,
		batchSize:           batchSize,
		initialBackoff:      initialBackoff,
		maxBackoff:          maxBackoff,
		maxWorkspaceTagWait: maxWorkspaceTagWait,
		sem:                 make(chan struct{}, maxTasks),
		wakeCh:              make(chan struct{}, 1),
	}
	if r.tagResolve == nil {
		r.tagResolve = r.defaultResolveWorkspaceTag
	}
	r.executors = defaultExecutors(r)
	return r
}

// Notify signals the background poller that one or more task rows are due now.
// It never blocks.
func (r *Reconciler) Notify() {
	select {
	case r.wakeCh <- struct{}{}:
	default:
	}
}

// Run starts the background action reconciliation loop and blocks until ctx is
// canceled and all in-flight worker goroutines have finished and released their
// leases.
func (r *Reconciler) Run(ctx context.Context) error {
	r.queue.SetQueueWakeup(r.Notify)
	defer func() {
		r.queue.SetQueueWakeup(nil)
		r.wg.Wait()
	}()

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

// ProcessOnce synchronously leases up to BatchSize currently due task rows and
// reconciles each leased row to quiescence (or first backoff) on the caller's
// goroutine. Intended for deterministic unit tests with a controlled clock.
func (r *Reconciler) ProcessOnce(ctx context.Context) (int, error) {
	leased, err := r.queue.LeaseDue(ctx, r.workerID, r.leaseTTL, r.batchSize)
	if err != nil {
		return 0, err
	}
	for _, item := range leased {
		r.processLeasedTask(ctx, item)
	}
	return len(leased), nil
}

func (r *Reconciler) reserveSlots(maxWanted int) int {
	n := 0
	for n < maxWanted {
		select {
		case r.sem <- struct{}{}:
			n++
		default:
			return n
		}
	}
	return n
}

func (r *Reconciler) releaseSlots(n int) {
	for range n {
		<-r.sem
	}
}

func (r *Reconciler) pollOnce(ctx context.Context) {
	for ctx.Err() == nil {
		reserved := r.reserveSlots(r.batchSize)
		if reserved == 0 {
			return
		}
		leaseCtx, cancelLease := context.WithTimeout(ctx, min(r.leaseTTL, leaseQueryMaxTimeout))
		leased, err := r.queue.LeaseDue(leaseCtx, r.workerID, r.leaseTTL, reserved)
		cancelLease()
		if err != nil {
			r.releaseSlots(reserved)
			if ctx.Err() == nil {
				slog.Warn("leasing due tasks failed", "worker", r.workerID, "error", err)
			}
			return
		}
		if unused := reserved - len(leased); unused > 0 {
			r.releaseSlots(unused)
		}
		if len(leased) == 0 {
			return
		}

		for _, item := range leased {
			r.wg.Add(1)
			go func(lt store.LeasedTask) {
				defer func() {
					<-r.sem
					r.wg.Done()
					// Wake the poller in case all MaxInFlightTasks slots were busy.
					r.Notify()
				}()
				r.processLeasedTask(ctx, lt)
			}(item)
		}
		if len(leased) < reserved {
			return
		}
	}
}

// execOutcomeKind categorizes the control-flow disposition of an action execution attempt.
type execOutcomeKind int

const (
	outcomeCompleted execOutcomeKind = iota
	outcomeTransientRetry
	outcomeCapacityUnavailable
	outcomePermanentFailure
	outcomeObservedActor
	outcomeCanceled
)

// actionExecOutcome captures the control-flow result of executing a single task action.
type actionExecOutcome struct {
	// kind identifies the execution disposition (e.g. completed, retry, failed, canceled).
	// Used by the worker to decide control flow: whether to back off transiently, abort
	// after cancellation, or proceed to commit a state-machine transition via store.Apply.
	kind execOutcomeKind

	// input is the domain event payload to feed into lifecycle.Transition when
	// committing the action result via store.TaskApplier.Apply.
	// It is non-nil for outcomeCompleted, outcomePermanentFailure,
	// outcomeCapacityUnavailable, and outcomeObservedActor, and nil for
	// outcomeTransientRetry and outcomeCanceled.
	input lifecycle.Input
}

func (r *Reconciler) processLeasedTask(ctx context.Context, item store.LeasedTask) {
	key := item.Key
	leaseToken := item.LeaseToken
	row := item.Row
	storeClockAnchor := item.LeasedAt
	monoAnchor := time.Now()

	for ctx.Err() == nil {
		if row == nil || row.Task == nil {
			r.releaseLeaseBestEffort(key, leaseToken, 0)
			return
		}
		storeNow := storeClockAnchor.Add(max(time.Since(monoAnchor), 0))
		if storeClockAnchor.IsZero() {
			storeNow = time.Now().UTC()
		}

		hasRunnable := len(row.Actions) > 0 &&
			(row.Actions[0].NotBefore.IsZero() || !row.Actions[0].NotBefore.After(storeNow))
		if !hasRunnable {
			updated, moreRunnable, deleted, leaseLost := r.handleDueTimer(ctx, key, leaseToken)
			if deleted || leaseLost || !moreRunnable {
				if !deleted && !leaseLost {
					r.releaseLeaseBestEffort(key, leaseToken, 0)
				}
				return
			}
			row = updated
			continue
		}

		head := row.Actions[0]
		outcome, leaseLost, obsoleteCanceled := r.runWithLeaseRenewal(ctx, key, leaseToken, row, head, storeNow)
		if leaseLost {
			return
		}
		if obsoleteCanceled && ctx.Err() == nil {
			fresh, getErr := r.queue.GetTaskRow(ctx, key)
			if errors.Is(getErr, store.ErrNotFound) {
				return
			}
			if getErr != nil || fresh == nil || fresh.LeaseOwner != leaseToken {
				r.releaseLeaseBestEffort(key, leaseToken, time.Second)
				return
			}
			row = fresh
			continue
		}
		if ctx.Err() != nil || outcome.kind == outcomeCanceled {
			r.releaseLeaseBestEffort(key, leaseToken, 0)
			return
		}
		if outcome.kind == outcomeTransientRetry {
			delay := r.computeJitteredBackoff(head.Tries)
			if err := r.queue.BackoffAction(ctx, key, leaseToken, head.ID, delay); err != nil && ctx.Err() == nil {
				slog.Warn("backing off transient task action failed", "atespace", key.Atespace, "task", key.Name, "error", err)
				r.releaseLeaseBestEffort(key, leaseToken, delay)
			}
			return
		}

		in := outcome.input
		var (
			deleted   bool
			lostInTx  bool
			unchanged bool
		)
		updated, applyErr := r.queue.Apply(ctx, key, func(cur *store.TaskRow, now time.Time) (*store.TaskRow, error) {
			if cur == nil {
				deleted = true
				return nil, nil
			}
			if !store.HasActiveLease(cur, leaseToken, now) {
				lostInTx = true
				return cur, nil
			}
			nextState, out, tErr := r.transition(cur.LifecycleState(), in, now)
			if tErr != nil {
				return nil, tErr
			}
			if out.DeleteRow {
				deleted = true
				return store.DeleteTaskRow, nil
			}
			unchanged = len(nextState.Actions) > 0 && nextState.Actions[0].ID == head.ID
			next := cur.Clone()
			next.SetLifecycleState(nextState, now)
			return next, nil
		})
		if applyErr != nil {
			if ctx.Err() == nil {
				slog.Warn("committing task action transition failed", "atespace", key.Atespace, "task", key.Name, "error", applyErr)
			}
			r.releaseLeaseBestEffort(key, leaseToken, time.Second)
			return
		}
		if deleted || lostInTx {
			return
		}
		if unchanged {
			// Head action stayed in place (e.g. CapacityUnavailableInput set
			// NotBefore or a no-op transition); release the lease and wait for
			// NextReconcileAt.
			r.releaseLeaseBestEffort(key, leaseToken, 0)
			return
		}
		row = updated
	}

	r.releaseLeaseBestEffort(key, leaseToken, 0)
}

// handleDueTimer evaluates and applies expired lifecycle timers (specifically
// [lifecycle.State.ClaimDeadline]) when a leased task row has no runnable head
// action.
//
// It executes inside an atomic [store.TaskApplier.Apply] transaction:
//  1. Verifies that the worker still holds the active lease; if not, it returns
//     leaseLost = true.
//  2. Checks whether a concurrent writer enqueued a runnable head action; if so,
//     the timer is left alone and hasRunnable = true is returned so the caller
//     can immediately switch to executing that action.
//  3. Checks if ClaimDeadline is set and has expired (<= now). If not expired,
//     the row remains untouched.
//  4. If ClaimDeadline has expired, it commits a [lifecycle.TimerExpiredInput]
//     transition. If the transition enqueues new runnable actions (such as
//     parking or deleting the actor upon watchdog timeout), hasRunnable = true
//     is returned so the caller can process them without dropping the lease.
//
// Returns:
//   - updated: the freshly committed TaskRow after transition or no-op check.
//   - hasRunnable: true if there are runnable actions ready for immediate execution.
//   - deleted: true if the row was deleted by the transition or missing.
//   - leaseLost: true if the worker lost the lease or the row was stolen/expired.
func (r *Reconciler) handleDueTimer(ctx context.Context, key store.TaskKey, leaseToken string) (updated *store.TaskRow, hasRunnable bool, deleted bool, leaseLost bool) {
	var timerFired bool
	row, err := r.queue.Apply(ctx, key, func(cur *store.TaskRow, now time.Time) (*store.TaskRow, error) {
		if cur == nil {
			deleted = true
			return nil, nil
		}
		if !store.HasActiveLease(cur, leaseToken, now) {
			leaseLost = true
			return cur, nil
		}
		if len(cur.Actions) > 0 && (cur.Actions[0].NotBefore.IsZero() || !cur.Actions[0].NotBefore.After(now)) {
			hasRunnable = true
			return cur, nil
		}
		if cur.ClaimDeadline == nil || cur.ClaimDeadline.After(now) {
			return cur, nil
		}
		timerFired = true
		nextState, out, tErr := r.transition(cur.LifecycleState(), &lifecycle.TimerExpiredInput{}, now)
		if tErr != nil {
			return nil, tErr
		}
		if out.DeleteRow {
			deleted = true
			return store.DeleteTaskRow, nil
		}
		next := cur.Clone()
		next.SetLifecycleState(nextState, now)
		hasRunnable = len(next.Actions) > 0 &&
			(next.Actions[0].NotBefore.IsZero() || !next.Actions[0].NotBefore.After(now))
		return next, nil
	})
	if err != nil {
		if ctx.Err() == nil {
			slog.Warn("applying task timer transition failed", "atespace", key.Atespace, "task", key.Name, "error", err)
		}
		return nil, false, false, false
	}
	if !timerFired && !hasRunnable {
		return row, false, deleted, leaseLost
	}
	return row, hasRunnable, deleted, leaseLost
}

func (r *Reconciler) runWithLeaseRenewal(
	parentCtx context.Context,
	key store.TaskKey,
	leaseToken string,
	row *store.TaskRow,
	head lifecycle.Action,
	storeNow time.Time,
) (outcome actionExecOutcome, leaseLost bool, obsoleteCanceled bool) {
	callCtx, cancelCall := context.WithTimeout(parentCtx, r.actionTimeout)
	renewStop := make(chan struct{})
	renewDone := make(chan struct{})

	go func() {
		defer close(renewDone)
		ticker := time.NewTicker(r.renewInterval)
		defer ticker.Stop()

		for {
			select {
			case <-renewStop:
				return
			case <-parentCtx.Done():
				cancelCall()
				return
			case <-ticker.C:
				renewCtx, cancelRenew := context.WithTimeout(parentCtx, min(r.leaseTTL/2, leaseQueryMaxTimeout))
				currentHeadID, err := r.queue.RenewLease(renewCtx, key, leaseToken, r.leaseTTL)
				cancelRenew()
				if errors.Is(err, store.ErrLeaseLost) {
					leaseLost = true
					cancelCall()
					return
				}
				if err != nil {
					continue
				}
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
			}
		}
	}()

	outcome = r.executeAction(parentCtx, callCtx, key, row, head, storeNow)
	close(renewStop)
	cancelCall()
	<-renewDone
	if (parentCtx.Err() != nil || leaseLost || obsoleteCanceled) &&
		(outcome.kind == outcomeTransientRetry || outcome.kind == outcomeCanceled) {
		outcome = actionExecOutcome{kind: outcomeCanceled}
	}
	return outcome, leaseLost, obsoleteCanceled
}

func (r *Reconciler) executeAction(parentCtx, ctx context.Context, key store.TaskKey, row *store.TaskRow, action lifecycle.Action, storeNow time.Time) actionExecOutcome {
	executor, ok := r.executors[action.Kind]
	if !ok {
		return actionExecOutcome{
			kind: outcomePermanentFailure,
			input: &lifecycle.ActionFailedInput{
				ActionID: action.ID,
				Kind:     action.Kind,
				Reason:   v1alpha1.TaskStatusReason_TASK_STATUS_REASON_PROVISIONING_FAILED,
				Message:  fmt.Sprintf("unknown task action kind %q", action.Kind),
			},
		}
	}
	return executor.Execute(ActionContext{
		ParentCtx: parentCtx,
		Ctx:       ctx,
		Key:       key,
		Row:       row,
		Action:    action,
		StoreNow:  storeNow,
	})
}

func (r *Reconciler) releaseLeaseBestEffort(key store.TaskKey, leaseToken string, requeueAfter time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), shutdownReleaseTimeout)
	defer cancel()
	_ = r.queue.ReleaseLease(ctx, key, leaseToken, requeueAfter)
}

func (r *Reconciler) defaultResolveWorkspaceTag(ctx context.Context, atespace, _, _, requestedTag string) (string, bool, error) {
	if requestedTag == "" {
		return "", false, nil
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

func (r *Reconciler) computeJitteredBackoff(tries int) time.Duration {
	delay := r.initialBackoff
	for i := 0; i < tries && delay < r.maxBackoff; i++ {
		if delay > r.maxBackoff/2 {
			delay = r.maxBackoff
			break
		}
		delay *= 2
	}
	if delay > r.maxBackoff {
		delay = r.maxBackoff
	}
	return jitter20Pct(delay)
}

func jitter20Pct(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	factor := 0.8 + rand.Float64()*0.4
	return max(time.Duration(float64(d)*factor), time.Millisecond)
}
