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
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/ax/internal/lifecycle"
	"github.com/google/ax/internal/store"
	"github.com/google/ax/pkg/apis/v1alpha1"
	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
)

func TestPostgresStore_ApplyAndLeaseLifecycle(t *testing.T) {
	s := openTest(t, Options{
		ApplyLockTimeout:      500 * time.Millisecond,
		ApplyStatementTimeout: 3 * time.Second,
	})
	ctx := context.Background()
	key := store.TaskKey{Atespace: "default", Name: "pg-queue-task"}

	// 1. Insert via Apply persists profile, egress rules, actions, and Generation 1.
	written, err := s.Apply(ctx, key, func(cur *store.TaskRow, now time.Time) (*store.TaskRow, error) {
		if cur != nil {
			t.Fatalf("expected nil current on insert, got %+v", cur)
		}
		st := lifecycle.State{
			Task: &v1alpha1.Task{
				Status: &v1alpha1.TaskStatus{
					State:            v1alpha1.TaskState_TASK_STATE_QUEUED,
					AteActorTemplate: "ax-tmpl-1",
				},
			},
		}
		st.Actions = []lifecycle.Action{st.NextAction(lifecycle.ActionEnsureTemplate, now)}
		row := &store.TaskRow{
			EffectiveProfile: &v1alpha1.Profile{Image: "us-docker.pkg.dev/acme/agent@sha256:abc"},
			EgressRules: []*ateapipb.EgressRule{{
				Https: &ateapipb.HttpsRule{Hostnames: []string{"api.example.com"}},
			}},
		}
		row.SetLifecycleState(st, now)
		return row, nil
	})
	if err != nil {
		t.Fatalf("Apply(insert): %v", err)
	}
	if written.Generation != 1 || written.Task.GetStatus().GetGeneration() != 1 {
		t.Fatalf("Generation = %d / %d, want 1", written.Generation, written.Task.GetStatus().GetGeneration())
	}

	gotTask, err := s.GetTask(ctx, "default", "pg-queue-task")
	if err != nil || gotTask.GetStatus().GetGeneration() != 1 {
		t.Fatalf("GetTask = %+v, %v", gotTask, err)
	}

	// 2. LeaseDue claims due row and blocks duplicate LeaseDue while active.
	leased, err := s.LeaseDue(ctx, "pg-w1", 30*time.Second, 10)
	if err != nil || len(leased) != 1 || leased[0].Key != key {
		t.Fatalf("LeaseDue = %+v, %v", leased, err)
	}
	token := leased[0].LeaseToken

	dup, err := s.LeaseDue(ctx, "pg-w2", 30*time.Second, 10)
	if err != nil || len(dup) != 0 {
		t.Fatalf("LeaseDue while leased = %+v, %v; want empty", dup, err)
	}

	// 3. RenewLease returns head Action.ID ("a-1"), then reflects superseded action ("a-2").
	headID, err := s.RenewLease(ctx, key, token, 30*time.Second)
	if err != nil || headID != "a-1" {
		t.Fatalf("RenewLease = (%q, %v), want (a-1, nil)", headID, err)
	}
	if _, err := s.Apply(ctx, key, func(cur *store.TaskRow, now time.Time) (*store.TaskRow, error) {
		next := cur.Clone()
		st := next.LifecycleState()
		st.Actions = []lifecycle.Action{st.NextAction(lifecycle.ActionResumeActor, now)}
		next.SetLifecycleState(st, now)
		return next, nil
	}); err != nil {
		t.Fatalf("Apply(update): %v", err)
	}
	headID, err = s.RenewLease(ctx, key, token, 30*time.Second)
	if err != nil || headID != "a-2" {
		t.Fatalf("RenewLease after supersede = (%q, %v), want (a-2, nil)", headID, err)
	}

	// 4. BackoffAction increments Tries, clears lease, and updates generation.
	if err := s.BackoffAction(ctx, key, token, "a-2", 50*time.Millisecond); err != nil {
		t.Fatalf("BackoffAction: %v", err)
	}
	afterBackoff, err := s.GetTaskRow(ctx, key)
	if err != nil {
		t.Fatalf("GetTaskRow: %v", err)
	}
	if afterBackoff.LeaseOwner != "" || afterBackoff.Actions[0].Tries != 1 || afterBackoff.Generation != 3 {
		t.Fatalf("unexpected row after BackoffAction: %+v", afterBackoff)
	}

	time.Sleep(60 * time.Millisecond)
	reclaimed, err := s.LeaseDue(ctx, "pg-w2", 30*time.Second, 10)
	if err != nil || len(reclaimed) != 1 {
		t.Fatalf("LeaseDue after backoff = %+v, %v", reclaimed, err)
	}
	if err := s.ReleaseLease(ctx, key, reclaimed[0].LeaseToken, 0); err != nil {
		t.Fatalf("ReleaseLease: %v", err)
	}

	// 5. Lock timeout classification when another transaction holds FOR UPDATE.
	shortStore, err := Open(ctx, s.pool.Config().ConnString(), Options{
		ApplyLockTimeout:      50 * time.Millisecond,
		ApplyStatementTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("Open(shortStore): %v", err)
	}
	defer func() { _ = shortStore.Close() }()

	holdTx, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin(holdTx): %v", err)
	}
	defer rollbackTx(holdTx)
	var lockedName string
	if err := holdTx.QueryRow(ctx, `SELECT name FROM tasks WHERE atespace = 'default' AND name = 'pg-queue-task' FOR UPDATE`).Scan(&lockedName); err != nil {
		t.Fatalf("FOR UPDATE: %v", err)
	}
	_, lockErr := shortStore.Apply(ctx, key, func(cur *store.TaskRow, _ time.Time) (*store.TaskRow, error) {
		return cur, nil
	})
	if !errors.Is(lockErr, store.ErrLockTimeout) {
		t.Fatalf("Apply under lock contention = %v, want ErrLockTimeout", lockErr)
	}
	_ = holdTx.Rollback(ctx)

	// 6. DeleteTaskRow removes row.
	if _, err := s.Apply(ctx, key, func(*store.TaskRow, time.Time) (*store.TaskRow, error) {
		return store.DeleteTaskRow, nil
	}); err != nil {
		t.Fatalf("Apply(DeleteTaskRow): %v", err)
	}
	if _, err := s.GetTaskRow(ctx, key); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("GetTaskRow after delete = %v, want ErrNotFound", err)
	}
}
