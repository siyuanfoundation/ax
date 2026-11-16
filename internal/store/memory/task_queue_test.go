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

package memory_test

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/ax/internal/lifecycle"
	"github.com/google/ax/internal/store"
	"github.com/google/ax/internal/store/memory"
	"github.com/google/ax/pkg/apis/v1alpha1"
)

func TestMemoryStore_ApplyAndLeaseSemantics(t *testing.T) {
	ctx := t.Context()
	now := time.Date(2026, 11, 16, 10, 0, 0, 0, time.UTC)
	s := memory.NewStore(memory.WithClock(func() time.Time { return now }))

	var wakeups atomic.Int32
	s.SetQueueWakeup(func() { wakeups.Add(1) })

	key := store.TaskKey{Atespace: "payments", Name: "fix-bug"}

	// 1. No-op and DeleteTaskRow on missing row leave storage empty.
	got, err := s.Apply(ctx, key, func(cur *store.TaskRow, _ time.Time) (*store.TaskRow, error) {
		if cur != nil {
			t.Fatalf("expected nil current on missing row, got %+v", cur)
		}
		return nil, nil
	})
	if err != nil || got != nil {
		t.Fatalf("Apply(nil on missing) = %v, %v; want nil, nil", got, err)
	}
	got, err = s.Apply(ctx, key, func(*store.TaskRow, time.Time) (*store.TaskRow, error) {
		return store.DeleteTaskRow, nil
	})
	if err != nil || got != nil {
		t.Fatalf("Apply(DeleteTaskRow on missing) = %v, %v; want nil, nil", got, err)
	}

	// 2. Returning a copy of DeleteTaskRow (nil Task) fails without mutating storage.
	if _, err := s.Apply(ctx, key, func(*store.TaskRow, time.Time) (*store.TaskRow, error) {
		return store.DeleteTaskRow.Clone(), nil
	}); err == nil {
		t.Fatal("expected error when returning row with nil Task")
	}

	// 3. Insert initializes Generation = 1, scrubs live actor fields, and wakes poller.
	inserted, err := s.Apply(ctx, key, func(_ *store.TaskRow, ts time.Time) (*store.TaskRow, error) {
		st := lifecycle.State{
			Task: &v1alpha1.Task{
				Status: &v1alpha1.TaskStatus{
					State:    v1alpha1.TaskState_TASK_STATE_QUEUED,
					Actor:    "must-scrub",
					ActorUid: "must-scrub-uid",
				},
			},
		}
		a1 := st.NextAction(lifecycle.ActionEnsureTemplate, ts)
		a2 := st.NextAction(lifecycle.ActionResumeActor, ts)
		st.Actions = []lifecycle.Action{a1, a2}
		row := &store.TaskRow{}
		row.SetLifecycleState(st, ts)
		return row, nil
	})
	if err != nil {
		t.Fatalf("Apply(insert): %v", err)
	}
	if inserted.Generation != 1 || inserted.Task.GetStatus().GetGeneration() != 1 {
		t.Fatalf("inserted Generation = %d / %d, want 1", inserted.Generation, inserted.Task.GetStatus().GetGeneration())
	}
	if inserted.Task.GetStatus().GetActor() != "" || inserted.Task.GetStatus().GetActorUid() != "" {
		t.Fatalf("live actor fields were not scrubbed: %+v", inserted.Task.GetStatus())
	}
	if wakeups.Load() == 0 {
		t.Fatal("expected queue wakeup after inserting due row")
	}

	// 4. Returning current itself leaves storage unchanged even if mutated in place.
	unchanged, err := s.Apply(ctx, key, func(cur *store.TaskRow, _ time.Time) (*store.TaskRow, error) {
		cur.ActorUID = "in-place-mutation"
		return cur, nil
	})
	if err != nil {
		t.Fatalf("Apply(return current): %v", err)
	}
	if unchanged.Generation != 1 || unchanged.ActorUID != "" {
		t.Fatalf("in-place mutation leaked: gen=%d actorUID=%q", unchanged.Generation, unchanged.ActorUID)
	}

	// 5. LeaseDue claims the row and advances NextReconcileAt out of the due range.
	leased, err := s.LeaseDue(ctx, "w1", 30*time.Second, 10)
	if err != nil || len(leased) != 1 {
		t.Fatalf("LeaseDue = %v, %v; want 1 leased task", leased, err)
	}
	token := leased[0].LeaseToken
	if second, err := s.LeaseDue(ctx, "w2", 30*time.Second, 10); err != nil || len(second) != 0 {
		t.Fatalf("second LeaseDue while leased = %v, %v; want empty", second, err)
	}

	// 6. Concurrent Apply during active lease clamps NextReconcileAt to LeaseExpiresAt.
	now = now.Add(5 * time.Second)
	clamped, err := s.Apply(ctx, key, func(cur *store.TaskRow, ts time.Time) (*store.TaskRow, error) {
		next := cur.Clone()
		st := next.LifecycleState()
		// Replace actions with DELETE_ACTOR due at ts (< LeaseExpiresAt).
		del := st.NextAction(lifecycle.ActionDeleteActor, ts)
		st.Actions = []lifecycle.Action{del}
		next.SetLifecycleState(st, ts)
		return next, nil
	})
	if err != nil {
		t.Fatalf("Apply during active lease: %v", err)
	}
	if clamped.Generation != 2 || !clamped.NextReconcileAt.Equal(leased[0].ExpiresAt) {
		t.Fatalf("clamped gen=%d nextReconcileAt=%v, want gen=2 and %v", clamped.Generation, clamped.NextReconcileAt, leased[0].ExpiresAt)
	}

	// 7. RenewLease extends the lease and returns the superseded head Action.ID ("a-3").
	headID, err := s.RenewLease(ctx, key, token, 30*time.Second)
	if err != nil || headID != "a-3" {
		t.Fatalf("RenewLease = %q, %v; want a-3, nil", headID, err)
	}

	// 8. BackoffAction on an obsolete action ID ("a-1") leaves "a-3" un-backed-off and releases lease.
	if err := s.BackoffAction(ctx, key, token, "a-1", 10*time.Second); err != nil {
		t.Fatalf("BackoffAction(obsolete): %v", err)
	}
	afterRelease, err := s.GetTaskRow(ctx, key)
	if err != nil {
		t.Fatalf("GetTaskRow: %v", err)
	}
	if afterRelease.LeaseOwner != "" || afterRelease.Actions[0].Tries != 0 || !afterRelease.NextReconcileAt.Equal(now) {
		t.Fatalf("expected a-3 ready immediately with Tries=0, got %+v", afterRelease)
	}

	// 9. Expired lease is rejected by RenewLease, scrubbed by GetTaskRow, and cleared when passed to Apply.
	reLeased, contended, retryIn, err := s.LeaseTask(ctx, key, "w1", 10*time.Second)
	if err != nil || reLeased == nil || contended || retryIn != 0 {
		t.Fatalf("LeaseTask = %v, contended=%v, retryIn=%v, %v; want leased", reLeased, contended, retryIn, err)
	}
	now = now.Add(11 * time.Second)
	if _, err := s.RenewLease(ctx, key, reLeased.LeaseToken, 10*time.Second); !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("RenewLease after expiry = %v, want ErrLeaseLost", err)
	}
	scrubbedRow, err := s.GetTaskRow(ctx, key)
	if err != nil || scrubbedRow.LeaseOwner != "" || scrubbedRow.LeaseExpiresAt != nil {
		t.Fatalf("GetTaskRow after lease expiry = %+v, %v; want expired lease scrubbed", scrubbedRow, err)
	}
	deleted, err := s.Apply(ctx, key, func(cur *store.TaskRow, _ time.Time) (*store.TaskRow, error) {
		if cur.LeaseOwner != "" || cur.LeaseExpiresAt != nil {
			t.Fatalf("expected expired lease cleared on current, got owner=%q exp=%v", cur.LeaseOwner, cur.LeaseExpiresAt)
		}
		return store.DeleteTaskRow, nil
	})
	if err != nil || deleted != nil {
		t.Fatalf("Apply(DeleteTaskRow) = %v, %v; want nil, nil", deleted, err)
	}
	if _, err := s.GetTaskRow(ctx, key); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("GetTaskRow after delete = %v, want ErrNotFound", err)
	}

	// 10. UpdateTaskStatus(..., nil) followed by GetTaskRow / LeaseTask must
	// safely initialize Status when overlaying Generation without nil panic, and
	// ReleaseLease with requeueAfter > 0 delays NextReconcileAt without bumping Tries.
	k2 := store.TaskKey{Atespace: "payments", Name: "nil-status-task"}
	if _, err := s.Apply(ctx, k2, func(_ *store.TaskRow, ts time.Time) (*store.TaskRow, error) {
		st := lifecycle.State{
			Task: &v1alpha1.Task{Status: &v1alpha1.TaskStatus{State: v1alpha1.TaskState_TASK_STATE_QUEUED}},
		}
		st.Actions = []lifecycle.Action{st.NextAction(lifecycle.ActionResumeActor, ts)}
		row := &store.TaskRow{}
		row.SetLifecycleState(st, ts)
		return row, nil
	}); err != nil {
		t.Fatalf("Apply(k2): %v", err)
	}
	if err := s.UpdateTaskStatus(ctx, k2.Atespace, k2.Name, nil); err != nil {
		t.Fatalf("UpdateTaskStatus(nil): %v", err)
	}
	r2, err := s.GetTaskRow(ctx, k2)
	if err != nil || r2.Task.GetStatus().GetGeneration() != 1 {
		t.Fatalf("GetTaskRow after nil status = %+v, %v; want generation 1", r2, err)
	}
	lt2, contended2, retryIn2, err := s.LeaseTask(ctx, k2, "w1", 30*time.Second)
	if err != nil || lt2 == nil || contended2 || retryIn2 != 0 || lt2.Row.Task.GetStatus().GetGeneration() != 1 {
		t.Fatalf("LeaseTask after nil status = %+v, contended=%v, retryIn=%v, %v", lt2, contended2, retryIn2, err)
	}
	// While lt2 holds an active lease on runnable action, a second LeaseTask reports contended=true.
	if ltContended, isContended, retryContended, err := s.LeaseTask(ctx, k2, "w2", 30*time.Second); err != nil || ltContended != nil || !isContended || retryContended != 0 {
		t.Fatalf("LeaseTask under active lease = (%v, %v, %v, %v), want (nil, true, 0, nil)", ltContended, isContended, retryContended, err)
	}
	if err := s.ReleaseLease(ctx, k2, lt2.LeaseToken, 3*time.Second); err != nil {
		t.Fatalf("ReleaseLease(requeueAfter): %v", err)
	}
	r2After, err := s.GetTaskRow(ctx, k2)
	if err != nil || r2After.Actions[0].Tries != 0 || !r2After.NextReconcileAt.Equal(now.Add(3*time.Second)) {
		t.Fatalf("after ReleaseLease(3s): tries=%d due=%v, want 0 and %v",
			r2After.Actions[0].Tries, r2After.NextReconcileAt, now.Add(3*time.Second))
	}
	if ltDelayed, isDelayedContended, delayedRetryIn, err := s.LeaseTask(ctx, k2, "w2", 30*time.Second); err != nil || ltDelayed != nil || isDelayedContended || delayedRetryIn != 3*time.Second {
		t.Fatalf("LeaseTask on delayed row = (%v, %v, %v, %v), want (nil, false, 3s, nil)", ltDelayed, isDelayedContended, delayedRetryIn, err)
	}

	// 11. LeaseDue LeaseFilter skips saturated tenants and saturated runnable
	// action kinds while admitting due timers, and LeaseTask reports
	// contended=true while an expired timer is actively leased.
	now = now.Add(5 * time.Second)
	kTimer := store.TaskKey{Atespace: "tenant-b", Name: "timer-task"}
	if _, err := s.Apply(ctx, kTimer, func(_ *store.TaskRow, ts time.Time) (*store.TaskRow, error) {
		dl := ts
		st := lifecycle.State{
			Task:          &v1alpha1.Task{Status: &v1alpha1.TaskStatus{State: v1alpha1.TaskState_TASK_STATE_QUEUED}},
			ClaimDeadline: &dl,
		}
		row := &store.TaskRow{}
		row.SetLifecycleState(st, ts)
		return row, nil
	}); err != nil {
		t.Fatalf("Apply(kTimer): %v", err)
	}
	leasedFiltered, err := s.LeaseDue(ctx, "w-filter", 30*time.Second, 10, store.LeaseFilter{
		ExcludeAtespaces:   []string{"payments"},
		ExcludeActionKinds: []lifecycle.ActionKind{lifecycle.ActionResumeActor},
	})
	if err != nil || len(leasedFiltered) != 1 || leasedFiltered[0].Key != kTimer {
		t.Fatalf("LeaseDue(exclude payments) = %+v, %v; want only kTimer", leasedFiltered, err)
	}
	if ltTimer, contendedTimer, retryTimer, err := s.LeaseTask(ctx, kTimer, "w-inline", 30*time.Second); err != nil || ltTimer != nil || !contendedTimer || retryTimer != 0 {
		t.Fatalf("LeaseTask on actively leased expired timer = (%v, %v, %v, %v), want (nil, true, 0, nil)",
			ltTimer, contendedTimer, retryTimer, err)
	}

	// Verify RestrictActionKinds skips non-allowed runnable actions (e.g.
	// ENSURE_TEMPLATE) while admitting AllowedActionKinds (RESUME_ACTOR).
	kOther := store.TaskKey{Atespace: "tenant-c", Name: "other-kind-task"}
	if _, err := s.Apply(ctx, kOther, func(_ *store.TaskRow, ts time.Time) (*store.TaskRow, error) {
		st := lifecycle.State{
			Task: &v1alpha1.Task{Status: &v1alpha1.TaskStatus{State: v1alpha1.TaskState_TASK_STATE_QUEUED}},
		}
		st.Actions = []lifecycle.Action{st.NextAction(lifecycle.ActionEnsureTemplate, ts)}
		row := &store.TaskRow{}
		row.SetLifecycleState(st, ts)
		return row, nil
	}); err != nil {
		t.Fatalf("Apply(kOther): %v", err)
	}
	otherFiltered, err := s.LeaseDue(ctx, "w-other", 30*time.Second, 10, store.LeaseFilter{
		RestrictActionKinds: true,
		AllowedActionKinds:  []lifecycle.ActionKind{lifecycle.ActionResumeActor},
	})
	if err != nil || len(otherFiltered) != 1 || otherFiltered[0].Key != k2 {
		t.Fatalf("LeaseDue(RestrictActionKinds) = %+v, %v; want only %v", otherFiltered, err, k2)
	}

	// No-op Apply after lease expiration scrubs expired LeaseOwner / LeaseExpiresAt on returned row.
	now = now.Add(35 * time.Second)
	noopRet, err := s.Apply(ctx, kTimer, func(cur *store.TaskRow, _ time.Time) (*store.TaskRow, error) {
		if cur.LeaseOwner != "" || cur.LeaseExpiresAt != nil {
			t.Fatalf("Apply callback saw unscrubbed expired lease: %+v", cur)
		}
		return nil, nil
	})
	if err != nil || noopRet.LeaseOwner != "" || noopRet.LeaseExpiresAt != nil {
		t.Fatalf("no-op Apply returned unscrubbed expired lease: %+v, err=%v", noopRet, err)
	}
}
