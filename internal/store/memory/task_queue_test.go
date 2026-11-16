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

func TestMemoryStore_ApplyAndLeaseLifecycle(t *testing.T) {
	now := time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)
	s := memory.NewStore(memory.WithClock(func() time.Time { return now }))

	var wakeups atomic.Int32
	s.SetQueueWakeup(func() { wakeups.Add(1) })

	ctx := t.Context()
	key := store.TaskKey{Atespace: "payments", Name: "t1"}

	// 1. Insert via Apply assigns Generation 1 and fires wakeup when due now.
	written, err := s.Apply(ctx, key, func(cur *store.TaskRow, ts time.Time) (*store.TaskRow, error) {
		if cur != nil {
			t.Fatalf("expected nil current on insert, got %+v", cur)
		}
		st := lifecycle.State{
			Task: &v1alpha1.Task{
				Status: &v1alpha1.TaskStatus{
					State:    v1alpha1.TaskState_TASK_STATE_QUEUED,
					Actor:    "scrub-me",
					ActorUid: "scrub-uid",
				},
			},
		}
		st.Actions = []lifecycle.Action{st.NextAction(lifecycle.ActionEnsureTemplate, ts)}
		row := &store.TaskRow{}
		row.SetLifecycleState(st, ts)
		return row, nil
	})
	if err != nil {
		t.Fatalf("Apply(insert): %v", err)
	}
	if written.Generation != 1 || written.Task.GetStatus().GetGeneration() != 1 {
		t.Fatalf("Generation = %d / %d, want 1", written.Generation, written.Task.GetStatus().GetGeneration())
	}
	if written.Task.GetStatus().GetActor() != "" || written.Task.GetStatus().GetActorUid() != "" {
		t.Fatalf("expected live actor fields scrubbed, got %+v", written.Task.GetStatus())
	}
	if wakeups.Load() != 1 {
		t.Fatalf("wakeups = %d, want 1", wakeups.Load())
	}

	// 2. LeaseDue claims due row and pushes NextReconcileAt out by leaseTTL.
	leased, err := s.LeaseDue(ctx, "worker-a", 30*time.Second, 10)
	if err != nil || len(leased) != 1 || leased[0].Key != key {
		t.Fatalf("LeaseDue = %+v, %v", leased, err)
	}
	token := leased[0].LeaseToken

	// Second LeaseDue while leased returns nothing.
	again, err := s.LeaseDue(ctx, "worker-b", 30*time.Second, 10)
	if err != nil || len(again) != 0 {
		t.Fatalf("LeaseDue while leased = %+v, %v; want empty", again, err)
	}

	// 3. RenewLease returns head Action.ID and rejects wrong token.
	headID, err := s.RenewLease(ctx, key, token, 30*time.Second)
	if err != nil || headID != "a-1" {
		t.Fatalf("RenewLease = (%q, %v), want (a-1, nil)", headID, err)
	}
	if _, err := s.RenewLease(ctx, key, "wrong-token", 30*time.Second); !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("RenewLease(wrong-token) = %v, want ErrLeaseLost", err)
	}

	// 4. Concurrent Apply while leased clamps NextReconcileAt to LeaseExpiresAt.
	updated, err := s.Apply(ctx, key, func(cur *store.TaskRow, ts time.Time) (*store.TaskRow, error) {
		next := cur.Clone()
		st := next.LifecycleState()
		st.Actions = []lifecycle.Action{st.NextAction(lifecycle.ActionDeleteActor, ts)}
		next.SetLifecycleState(st, ts)
		return next, nil
	})
	if err != nil {
		t.Fatalf("Apply(replace head action): %v", err)
	}
	if updated.Generation != 2 || !updated.NextReconcileAt.Equal(leased[0].ExpiresAt) {
		t.Fatalf("expected Generation=2 and NextReconcileAt clamped to %v, got gen=%d due=%v",
			leased[0].ExpiresAt, updated.Generation, updated.NextReconcileAt)
	}

	// RenewLease sees superseded head action ID ("a-2").
	headID, err = s.RenewLease(ctx, key, token, 30*time.Second)
	if err != nil || headID != "a-2" {
		t.Fatalf("RenewLease after supersede = (%q, %v), want (a-2, nil)", headID, err)
	}

	// 5. BackoffAction increments Tries, sets NotBefore, and clears lease.
	if err := s.BackoffAction(ctx, key, token, "a-2", 5*time.Second); err != nil {
		t.Fatalf("BackoffAction: %v", err)
	}
	backedOff, err := s.GetTaskRow(ctx, key)
	if err != nil {
		t.Fatalf("GetTaskRow: %v", err)
	}
	if backedOff.LeaseOwner != "" || backedOff.Actions[0].Tries != 1 || !backedOff.NextReconcileAt.Equal(now.Add(5*time.Second)) {
		t.Fatalf("unexpected row after BackoffAction: %+v", backedOff)
	}

	// Not due before backoff expires; due once clock advances.
	now = now.Add(5 * time.Second)
	reLeased, err := s.LeaseDue(ctx, "worker-b", 30*time.Second, 10)
	if err != nil || len(reLeased) != 1 {
		t.Fatalf("LeaseDue after backoff = %+v, %v", reLeased, err)
	}
	if err := s.ReleaseLease(ctx, key, reLeased[0].LeaseToken, 2*time.Second); err != nil {
		t.Fatalf("ReleaseLease: %v", err)
	}

	// 6. DeleteTaskRow removes the row cleanly.
	deleted, err := s.Apply(ctx, key, func(*store.TaskRow, time.Time) (*store.TaskRow, error) {
		return store.DeleteTaskRow, nil
	})
	if err != nil || deleted != nil {
		t.Fatalf("Apply(DeleteTaskRow) = %+v, %v; want nil, nil", deleted, err)
	}
	if _, err := s.GetTaskRow(ctx, key); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("GetTaskRow after delete = %v, want ErrNotFound", err)
	}
}
