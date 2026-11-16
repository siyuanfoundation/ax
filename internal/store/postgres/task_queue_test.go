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

package postgres

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/ax/internal/lifecycle"
	"github.com/google/ax/internal/store"
	"github.com/google/ax/pkg/apis/v1alpha1"
	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/protobuf/proto"
)

func TestPostgres_ApplyRoundTripAndConcurrency(t *testing.T) {
	s := openTest(t, Options{
		ApplyLockTimeout:      150 * time.Millisecond,
		ApplyStatementTimeout: 2 * time.Second,
	})
	ctx := context.Background()

	key := store.TaskKey{Atespace: "payments", Name: "fix-refund"}

	// 1. Concurrent creation race: 8 goroutines attempt initial insert; exactly
	// one wins with current == nil and the rest retry seeing the winner's row.
	var (
		wg      sync.WaitGroup
		created atomic.Int32
		existed atomic.Int32
	)
	errAlreadyExists := errors.New("already exists")
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := s.Apply(ctx, key, func(cur *store.TaskRow, now time.Time) (*store.TaskRow, error) {
				if cur != nil {
					return nil, errAlreadyExists
				}
				claimDeadline := now.Add(5 * time.Minute)
				resumedAt := now.Add(-time.Second)
				hbAt := now
				st := lifecycle.State{
					Task: &v1alpha1.Task{
						Spec: &v1alpha1.TaskSpec{SessionId: "sess-42"},
						Status: &v1alpha1.TaskStatus{
							State:            v1alpha1.TaskState_TASK_STATE_QUEUED,
							AteActorTemplate: "ax-0123456789abcdef",
							Actor:            "scrub-me",
							ActorUid:         "scrub-uid",
						},
					},
					ActorUID:             "actor-uid-1",
					ActiveRunID:          "r-1",
					RunSeq:               1,
					RunnerTokenHash:      []byte{0xaa, 0xbb, 0xcc},
					RunnerTokenConfirmed: true,
					RunnerTokenGen:       3,
					LastAckedRunnerSeq:   7,
					ClaimDeadline:        &claimDeadline,
					LastResumedAt:        &resumedAt,
				}
				a1 := st.NextAction(lifecycle.ActionEnsureTemplate, now)
				a2 := st.NextAction(lifecycle.ActionCreateActor, now)
				a2.Tag = "ws-tag-r1"
				st.Actions = []lifecycle.Action{a1, a2}

				row := &store.TaskRow{
					EffectiveProfile: &v1alpha1.Profile{
						Image: "us-docker.pkg.dev/acme/agent@sha256:1111",
					},
					EgressRules: []*ateapipb.EgressRule{{
						Https: &ateapipb.HttpsRule{
							Hostnames: []string{"generativelanguage.googleapis.com"},
						},
					}},
					LastHeartbeatAt: &hbAt,
				}
				row.SetLifecycleState(st, now)
				return row, nil
			})
			switch {
			case err == nil:
				created.Add(1)
			case errors.Is(err, errAlreadyExists):
				existed.Add(1)
			default:
				t.Errorf("unexpected Apply error: %v", err)
			}
		}()
	}
	wg.Wait()
	if created.Load() != 1 || existed.Load() != 7 {
		t.Fatalf("created=%d existed=%d, want 1 and 7", created.Load(), existed.Load())
	}

	// 2. Verify lossless round-trip of all TaskRow and lifecycle.State columns.
	got, err := s.GetTaskRow(ctx, key)
	if err != nil {
		t.Fatalf("GetTaskRow: %v", err)
	}
	if got.Generation != 1 || got.Task.GetStatus().GetGeneration() != 1 {
		t.Fatalf("Generation = %d / %d, want 1", got.Generation, got.Task.GetStatus().GetGeneration())
	}
	if got.Task.GetStatus().GetActor() != "" || got.Task.GetStatus().GetActorUid() != "" {
		t.Fatalf("live actor fields persisted: %+v", got.Task.GetStatus())
	}
	if got.ActorUID != "actor-uid-1" || got.ActiveRunID != "r-1" || got.RunSeq != 1 || got.ActionSeq != 2 {
		t.Fatalf("unexpected sequence/runtime fields: %+v", got.State)
	}
	if !bytes.Equal(got.RunnerTokenHash, []byte{0xaa, 0xbb, 0xcc}) || !got.RunnerTokenConfirmed || got.RunnerTokenGen != 3 || got.LastAckedRunnerSeq != 7 {
		t.Fatalf("unexpected runner token/seq fields: %+v", got.State)
	}
	if got.ClaimDeadline == nil || got.LastResumedAt == nil || got.LastHeartbeatAt == nil {
		t.Fatalf("timestamp pointers lost: claim=%v resumed=%v hb=%v", got.ClaimDeadline, got.LastResumedAt, got.LastHeartbeatAt)
	}
	if got.EffectiveProfile.GetImage() != "us-docker.pkg.dev/acme/agent@sha256:1111" {
		t.Fatalf("EffectiveProfile = %+v", got.EffectiveProfile)
	}
	if len(got.EgressRules) != 1 || !proto.Equal(got.EgressRules[0], &ateapipb.EgressRule{
		Https: &ateapipb.HttpsRule{Hostnames: []string{"generativelanguage.googleapis.com"}},
	}) {
		t.Fatalf("EgressRules = %+v", got.EgressRules)
	}
	if len(got.Actions) != 2 || got.Actions[1].Tag != "ws-tag-r1" {
		t.Fatalf("Actions = %+v", got.Actions)
	}

	// 3. Updating only queue/runtime columns skips rewriting data JSONB while
	// GetTask and ListTasks still report the new Generation.
	updated, err := s.Apply(ctx, key, func(cur *store.TaskRow, now time.Time) (*store.TaskRow, error) {
		next := cur.Clone()
		st := next.LifecycleState()
		st.Actions = st.Actions[1:]
		next.SetLifecycleState(st, now)
		return next, nil
	})
	if err != nil || updated.Generation != 2 {
		t.Fatalf("Apply(pop action) = %v, %v; want gen 2", updated, err)
	}
	apiTask, err := s.GetTask(ctx, key.Atespace, key.Name)
	if err != nil || apiTask.GetStatus().GetGeneration() != 2 {
		t.Fatalf("GetTask generation = %d, err = %v; want 2", apiTask.GetStatus().GetGeneration(), err)
	}

	// 4. Lock contention beyond ApplyLockTimeout returns store.ErrLockTimeout.
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var dummy string
	if err := tx.QueryRow(ctx, `SELECT name FROM tasks WHERE atespace = $1 AND name = $2 FOR UPDATE`, key.Atespace, key.Name).Scan(&dummy); err != nil {
		t.Fatalf("locking row: %v", err)
	}
	_, err = s.Apply(ctx, key, func(cur *store.TaskRow, _ time.Time) (*store.TaskRow, error) {
		return cur.Clone(), nil
	})
	if !errors.Is(err, store.ErrLockTimeout) {
		t.Fatalf("Apply under lock contention = %v, want store.ErrLockTimeout", err)
	}

	// 5. Caller context deadline/cancellation while blocked on FOR UPDATE wraps
	// context.DeadlineExceeded / context.Canceled even when PostgreSQL reports 57014.
	shortCtx, cancelShort := context.WithTimeout(ctx, 30*time.Millisecond)
	defer cancelShort()
	if _, _, _, lErr := s.LeaseTask(shortCtx, key, "w-timeout", 30*time.Second); !errors.Is(lErr, context.DeadlineExceeded) {
		t.Fatalf("LeaseTask with short caller deadline under lock contention = %v, want context.DeadlineExceeded", lErr)
	}
	cancelCtx, cancelNow := context.WithCancel(ctx)
	go func() {
		time.Sleep(25 * time.Millisecond)
		cancelNow()
	}()
	if _, aErr := s.Apply(cancelCtx, key, func(cur *store.TaskRow, _ time.Time) (*store.TaskRow, error) {
		return cur.Clone(), nil
	}); !errors.Is(aErr, context.Canceled) {
		t.Fatalf("Apply with canceled caller context under lock contention = %v, want context.Canceled", aErr)
	}
	_ = tx.Rollback(ctx)
}

func TestPostgres_LeaseQueueFencingAndSkipLocked(t *testing.T) {
	s := openTest(t, Options{})
	ctx := context.Background()

	const totalTasks = 12
	for i := range totalTasks {
		k := store.TaskKey{Atespace: "default", Name: "task-" + string(rune('a'+i))}
		if _, err := s.Apply(ctx, k, func(_ *store.TaskRow, now time.Time) (*store.TaskRow, error) {
			st := lifecycle.State{
				Task: &v1alpha1.Task{
					Status: &v1alpha1.TaskStatus{State: v1alpha1.TaskState_TASK_STATE_QUEUED},
				},
			}
			a := st.NextAction(lifecycle.ActionResumeActor, now)
			st.Actions = []lifecycle.Action{a}
			row := &store.TaskRow{}
			row.SetLifecycleState(st, now)
			return row, nil
		}); err != nil {
			t.Fatalf("seed Apply(%v): %v", k, err)
		}
	}

	// 4 concurrent pollers lease batches of 4; each task must be leased at most once.
	var (
		mu   sync.Mutex
		seen = make(map[store.TaskKey]store.LeasedTask)
		wg   sync.WaitGroup
	)
	for w := range 4 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			batch, err := s.LeaseDue(ctx, "worker", 30*time.Second, 4)
			if err != nil {
				t.Errorf("worker %d LeaseDue: %v", id, err)
				return
			}
			mu.Lock()
			for _, lt := range batch {
				if prev, dup := seen[lt.Key]; dup {
					t.Errorf("duplicate lease on %v: %q vs %q", lt.Key, prev.LeaseToken, lt.LeaseToken)
				}
				seen[lt.Key] = lt
			}
			mu.Unlock()
		}(w)
	}
	wg.Wait()
	if len(seen) != totalTasks {
		t.Fatalf("leased %d tasks across concurrent workers, want %d", len(seen), totalTasks)
	}

	// Pick one leased task and verify Apply clamping, LeaseTask contention,
	// RenewLease, ReleaseLease, BackoffAction, and DeleteTaskRow.
	firstKey := store.TaskKey{Atespace: "default", Name: "task-a"}
	lt := seen[firstKey]

	// Concurrent Apply while leased must clamp NextReconcileAt to >= LeaseExpiresAt.
	clamped, err := s.Apply(ctx, firstKey, func(cur *store.TaskRow, now time.Time) (*store.TaskRow, error) {
		next := cur.Clone()
		st := next.LifecycleState()
		st.Task.Spec = &v1alpha1.TaskSpec{SessionId: "clamped-sess"}
		next.SetLifecycleState(st, now)
		return next, nil
	})
	if err != nil {
		t.Fatalf("Apply during active lease: %v", err)
	}
	if clamped.NextReconcileAt == nil || clamped.NextReconcileAt.Before(lt.ExpiresAt) {
		t.Fatalf("expected NextReconcileAt >= %v during active lease, got %v", lt.ExpiresAt, clamped.NextReconcileAt)
	}

	// LeaseTask while another worker holds the lease reports contended == true.
	if gotLT, contended, retryIn, err := s.LeaseTask(ctx, firstKey, "inline-caller", 30*time.Second); err != nil || gotLT != nil || !contended || retryIn != 0 {
		t.Fatalf("LeaseTask under active lease = (%v, %v, %v, %v), want (nil, true, 0, nil)", gotLT, contended, retryIn, err)
	}

	headID, err := s.RenewLease(ctx, firstKey, lt.LeaseToken, 30*time.Second)
	if err != nil || headID != "a-1" {
		t.Fatalf("RenewLease = %q, %v; want a-1", headID, err)
	}
	if _, err := s.RenewLease(ctx, firstKey, "wrong-token", 30*time.Second); !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("RenewLease(wrong-token) = %v, want ErrLeaseLost", err)
	}

	// ReleaseLease with requeueAfter > 0 delays due time without bumping Tries,
	// and repeating ReleaseLease with the released token returns ErrLeaseLost.
	secondKey := store.TaskKey{Atespace: "default", Name: "task-b"}
	ltB := seen[secondKey]
	if err := s.ReleaseLease(ctx, secondKey, ltB.LeaseToken, 10*time.Second); err != nil {
		t.Fatalf("ReleaseLease(task-b): %v", err)
	}
	if err := s.ReleaseLease(ctx, secondKey, ltB.LeaseToken, 0); !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("second ReleaseLease(task-b) = %v, want ErrLeaseLost", err)
	}
	rowB, err := s.GetTaskRow(ctx, secondKey)
	if err != nil || rowB.LeaseOwner != "" || rowB.Actions[0].Tries != 0 {
		t.Fatalf("GetTaskRow(task-b) after ReleaseLease = %+v, %v", rowB, err)
	}
	// Because task-b is now delayed with no active lease, LeaseTask returns
	// (nil, false, retryIn > 0, nil).
	if gotB, contendedB, retryInB, err := s.LeaseTask(ctx, secondKey, "inline-caller", 30*time.Second); err != nil || gotB != nil || contendedB || retryInB <= 0 {
		t.Fatalf("LeaseTask on unleased delayed row = (%v, %v, %v, %v), want (nil, false, >0, nil)", gotB, contendedB, retryInB, err)
	}

	if err := s.BackoffAction(ctx, firstKey, lt.LeaseToken, "a-1", 5*time.Second); err != nil {
		t.Fatalf("BackoffAction: %v", err)
	}
	backedOff, err := s.GetTaskRow(ctx, firstKey)
	if err != nil {
		t.Fatalf("GetTaskRow: %v", err)
	}
	if backedOff.LeaseOwner != "" || backedOff.Actions[0].Tries != 1 || backedOff.Generation != 2 {
		t.Fatalf("BackoffAction unexpected state: owner=%q tries=%d gen=%d", backedOff.LeaseOwner, backedOff.Actions[0].Tries, backedOff.Generation)
	}

	// Delete the row via DeleteTaskRow sentinel.
	res, err := s.Apply(ctx, firstKey, func(*store.TaskRow, time.Time) (*store.TaskRow, error) {
		return store.DeleteTaskRow, nil
	})
	if err != nil || res != nil {
		t.Fatalf("Apply(DeleteTaskRow) = %v, %v; want nil, nil", res, err)
	}
	if _, err := s.GetTaskRow(ctx, firstKey); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("GetTaskRow after delete = %v, want ErrNotFound", err)
	}
}

func TestPostgres_LeaseTaskPostLockClockAndLeaseExpiryTakeover(t *testing.T) {
	s := openTest(t, Options{})
	ctx := context.Background()

	// 1. Regression test for post-lock clock_timestamp() in LeaseTask:
	// Seed an at-rest task (NextReconcileAt == nil), hold its FOR UPDATE lock in
	// tx1, start LeaseTask concurrently (so LeaseTask's BEGIN precedes tx1's
	// commit), then queue a due action in tx1 and commit. LeaseTask MUST see the
	// post-lock clock and acquire the lease rather than returning (nil, false).
	key := store.TaskKey{Atespace: "default", Name: "post-lock-clock"}
	if _, err := s.Apply(ctx, key, func(*store.TaskRow, time.Time) (*store.TaskRow, error) {
		return &store.TaskRow{
			State: lifecycle.State{
				Task: &v1alpha1.Task{
					Status: &v1alpha1.TaskStatus{State: v1alpha1.TaskState_TASK_STATE_SUSPENDED},
				},
			},
		}, nil
	}); err != nil {
		t.Fatalf("seed Apply: %v", err)
	}

	tx1, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin tx1: %v", err)
	}
	defer rollbackTx(tx1)
	var lockedName string
	if err := tx1.QueryRow(ctx, `SELECT name FROM tasks WHERE atespace = $1 AND name = $2 FOR UPDATE`, key.Atespace, key.Name).Scan(&lockedName); err != nil {
		t.Fatalf("locking row in tx1: %v", err)
	}

	type leaseResult struct {
		lt        *store.LeasedTask
		contended bool
		retryIn   time.Duration
		err       error
	}
	leaseCh := make(chan leaseResult, 1)
	go func() {
		lt, contended, retryIn, lErr := s.LeaseTask(ctx, key, "inline-worker", 30*time.Second)
		leaseCh <- leaseResult{lt: lt, contended: contended, retryIn: retryIn, err: lErr}
	}()

	// Let LeaseTask's transaction start BEGIN and block on FOR UPDATE, then
	// advance clock_timestamp() slightly before setting next_reconcile_at and committing.
	time.Sleep(40 * time.Millisecond)
	if _, err := tx1.Exec(ctx, `
		UPDATE tasks
		SET state             = $3,
		    pending_actions   = '[{"id":"a-1","kind":"RESUME_ACTOR","tries":0,"since":"2026-11-16T00:00:00Z"}]'::jsonb,
		    next_reconcile_at = clock_timestamp()
		WHERE atespace = $1 AND name = $2`,
		key.Atespace, key.Name, int16(v1alpha1.TaskState_TASK_STATE_QUEUED),
	); err != nil {
		t.Fatalf("tx1 update: %v", err)
	}
	if err := tx1.Commit(ctx); err != nil {
		t.Fatalf("tx1 commit: %v", err)
	}

	res := <-leaseCh
	if res.err != nil || res.lt == nil || res.contended || res.retryIn != 0 {
		t.Fatalf("LeaseTask after concurrent update = (%+v, contended=%v, retryIn=%v, err=%v), want acquired lease",
			res.lt, res.contended, res.retryIn, res.err)
	}
	if err := s.ReleaseLease(ctx, key, res.lt.LeaseToken, 0); err != nil {
		t.Fatalf("ReleaseLease: %v", err)
	}

	// 2. Short lease TTL expiry: expired lease fails RenewLease, is scrubbed by
	// GetTaskRow, and is taken over by a subsequent LeaseDue call.
	shortBatch, err := s.LeaseDue(ctx, "crashed-worker", 50*time.Millisecond, 10)
	if err != nil || len(shortBatch) != 1 {
		t.Fatalf("LeaseDue(50ms) = %+v, %v; want 1 task", shortBatch, err)
	}
	staleToken := shortBatch[0].LeaseToken
	time.Sleep(80 * time.Millisecond)

	if _, err := s.RenewLease(ctx, key, staleToken, 30*time.Second); !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("RenewLease after 50ms TTL = %v, want ErrLeaseLost", err)
	}
	expiredView, err := s.GetTaskRow(ctx, key)
	if err != nil || expiredView.LeaseOwner != "" || expiredView.LeaseExpiresAt != nil {
		t.Fatalf("GetTaskRow after expiry = %+v, %v; want scrubbed lease", expiredView, err)
	}
	noopView, err := s.Apply(ctx, key, func(cur *store.TaskRow, _ time.Time) (*store.TaskRow, error) {
		if cur.LeaseOwner != "" || cur.LeaseExpiresAt != nil {
			t.Fatalf("Apply callback saw unscrubbed expired lease: %+v", cur)
		}
		return nil, nil
	})
	if err != nil || noopView.LeaseOwner != "" || noopView.LeaseExpiresAt != nil {
		t.Fatalf("no-op Apply after expiry returned unscrubbed lease = %+v, %v", noopView, err)
	}

	reclaimed, err := s.LeaseDue(ctx, "healthy-worker", 30*time.Second, 10)
	if err != nil || len(reclaimed) != 1 || reclaimed[0].LeaseToken == staleToken {
		t.Fatalf("reclaimed LeaseDue = %+v, %v; want fresh lease token", reclaimed, err)
	}
	if err := s.ReleaseLease(ctx, key, reclaimed[0].LeaseToken, 0); err != nil {
		t.Fatalf("ReleaseLease(reclaimed): %v", err)
	}

	// 3. LeaseDue LeaseFilter skips saturated tenants and saturated runnable
	// action kinds without blocking due timers or unsaturated kinds, and
	// LeaseTask reports contended=true on a leased expired timer.
	timerKey := store.TaskKey{Atespace: "tenant-other", Name: "timer-due"}
	if _, err := s.Apply(ctx, timerKey, func(_ *store.TaskRow, ts time.Time) (*store.TaskRow, error) {
		dl := ts.Add(-time.Second)
		st := lifecycle.State{
			Task:          &v1alpha1.Task{Status: &v1alpha1.TaskStatus{State: v1alpha1.TaskState_TASK_STATE_QUEUED}},
			ClaimDeadline: &dl,
		}
		row := &store.TaskRow{}
		row.SetLifecycleState(st, ts)
		return row, nil
	}); err != nil {
		t.Fatalf("Apply(timerKey): %v", err)
	}

	// Excluding RESUME_ACTOR skips key (in "default" with RESUME_ACTOR) while
	// still leasing timerKey (due timer with no pending actions).
	filtered, err := s.LeaseDue(ctx, "w-filtered", 30*time.Second, 10, store.LeaseFilter{
		ExcludeActionKinds: []lifecycle.ActionKind{lifecycle.ActionResumeActor},
	})
	if err != nil || len(filtered) != 1 || filtered[0].Key != timerKey {
		t.Fatalf("LeaseDue(exclude RESUME_ACTOR) = %+v, %v; want only %v", filtered, err, timerKey)
	}
	if ltContend, isContend, retryContend, err := s.LeaseTask(ctx, timerKey, "w-inline", 30*time.Second); err != nil || ltContend != nil || !isContend || retryContend != 0 {
		t.Fatalf("LeaseTask on leased expired timer = (%v, %v, %v, %v), want (nil, true, 0, nil)",
			ltContend, isContend, retryContend, err)
	}

	// Verify ExcludeOtherActionKinds skips unbucketed runnable actions (e.g.
	// ENSURE_TEMPLATE) while admitting explicitly bucketed actions (RESUME_ACTOR).
	otherKey := store.TaskKey{Atespace: "tenant-other", Name: "other-kind"}
	if _, err := s.Apply(ctx, otherKey, func(_ *store.TaskRow, ts time.Time) (*store.TaskRow, error) {
		st := lifecycle.State{
			Task: &v1alpha1.Task{Status: &v1alpha1.TaskStatus{State: v1alpha1.TaskState_TASK_STATE_QUEUED}},
		}
		st.Actions = []lifecycle.Action{st.NextAction(lifecycle.ActionEnsureTemplate, ts)}
		row := &store.TaskRow{}
		row.SetLifecycleState(st, ts)
		return row, nil
	}); err != nil {
		t.Fatalf("Apply(otherKey): %v", err)
	}
	otherFiltered, err := s.LeaseDue(ctx, "w-other-filter", 30*time.Second, 10, store.LeaseFilter{
		ExcludeOtherActionKinds: true,
		BucketedActionKinds:     []lifecycle.ActionKind{lifecycle.ActionResumeActor},
	})
	if err != nil || len(otherFiltered) != 1 || otherFiltered[0].Key != key {
		t.Fatalf("LeaseDue(ExcludeOtherActionKinds) = %+v, %v; want only %v", otherFiltered, err, key)
	}
}
