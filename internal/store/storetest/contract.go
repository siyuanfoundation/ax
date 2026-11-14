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

// Package storetest provides shared contract tests for store backends
// implementing [store.Store], [store.TaskApplier], and
// [store.WorkspaceSnapshotStore].
package storetest

import (
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	"github.com/google/ax/internal/lifecycle"
	"github.com/google/ax/internal/store"
	runnerv1 "github.com/google/ax/pkg/apis/runner/v1"
	"github.com/google/ax/pkg/apis/v1alpha1"
	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
)

// Backend combines the store interfaces exercised by the shared contract suite.
type Backend interface {
	store.Store
	store.TaskApplier
	store.WorkspaceSnapshotStore
}

// RunContract runs the shared TaskApplier, WorkspaceStatus, and
// WorkspaceSnapshotStore contract tests against b.
func RunContract(t *testing.T, b Backend) {
	t.Helper()

	t.Run("ApplyLifecycleAndDueNotify", func(t *testing.T) {
		ctx := t.Context()
		key := store.TaskKey{Atespace: "payments", Name: "fix-bug"}

		// Drain any leftover signal.
		select {
		case <-b.DueNotify():
		default:
		}

		// 1. No-op when row does not exist and callback returns (nil, nil).
		noopRow, err := b.Apply(ctx, key, func(cur *store.TaskRow, now time.Time) (*store.TaskRow, error) {
			if cur != nil {
				t.Fatalf("expected nil cur on missing row, got %+v", cur)
			}
			return nil, nil
		})
		if err != nil || noopRow != nil {
			t.Fatalf("missing no-op Apply = (%v, %v), want (nil, nil)", noopRow, err)
		}

		// 2. Create via Apply -> Generation = 1, immediately due -> DueNotify fires.
		tokHash := sha256.Sum256([]byte("secret-token"))
		created, err := b.Apply(ctx, key, func(cur *store.TaskRow, now time.Time) (*store.TaskRow, error) {
			s, _, err := lifecycle.Transition(lifecycle.State{}, lifecycle.Input{
				Create: &lifecycle.CreateInput{
					Task: &v1alpha1.Task{
						Metadata: &v1alpha1.ObjectMeta{Atespace: key.Atespace, Name: key.Name},
						Spec: &v1alpha1.TaskSpec{
							Commands:  []string{"pytest"},
							SessionId: "sess-1",
						},
						Status: &v1alpha1.TaskStatus{
							Actor:    "must-be-scrubbed",
							ActorUid: "must-be-scrubbed-uid",
						},
					},
					AteActorTemplate: "ax-0123456789abcdef",
					SourceTag:        "ws-tag-r1",
					WorkspaceReady:   true,
				},
			}, now)
			if err != nil {
				return nil, err
			}
			row := &store.TaskRow{
				EffectiveProfile: &v1alpha1.Profile{Image: "img@sha256:abc"},
				EgressRules: []*ateapipb.EgressRule{{
					Https: &ateapipb.HttpsRule{Hostnames: []string{"generativelanguage.googleapis.com"}},
				}},
			}
			row.SetLifecycleState(s, now)
			row.RunnerTokenHash = tokHash[:]
			row.LastClaimNonce = "n-1"
			row.LastClaimResponse = &runnerv1.ClaimAssignmentResponse{
				Mode:        runnerv1.AssignmentMode_ASSIGNMENT_MODE_RUN,
				RunId:       "r-1",
				RunnerToken: "plaintext-must-be-stripped",
			}
			return row, nil
		})
		if err != nil {
			t.Fatalf("Create Apply: %v", err)
		}
		if created.Generation != 1 || created.Task.GetStatus().GetGeneration() != 1 {
			t.Fatalf("generation = (%d, %d), want (1, 1)", created.Generation, created.Task.GetStatus().GetGeneration())
		}
		if created.LastClaimResponse.GetRunnerToken() != "" {
			t.Fatalf("expected plaintext RunnerToken stripped on persist, got %q", created.LastClaimResponse.GetRunnerToken())
		}
		if created.Task.GetStatus().GetActor() != "" || created.Task.GetStatus().GetActorUid() != "" {
			t.Fatalf("expected live Actor/ActorUid scrubbed on persist, got %+v", created.Task.GetStatus())
		}
		select {
		case <-b.DueNotify():
		default:
			t.Fatal("expected DueNotify signal after due row creation")
		}

		// 3. Returning cur unchanged is a no-op (Generation stays 1, no DueNotify).
		same, err := b.Apply(ctx, key, func(cur *store.TaskRow, _ time.Time) (*store.TaskRow, error) {
			return cur, nil
		})
		if err != nil || same.Generation != 1 {
			t.Fatalf("no-op Apply = (gen=%d, err=%v), want gen=1", same.Generation, err)
		}
		select {
		case <-b.DueNotify():
			t.Fatal("unexpected DueNotify on no-op Apply")
		default:
		}

		// 4. Live lease clamps NextReconcileAt >= LeaseExpiresAt and suppresses DueNotify;
		// expired lease is cleared automatically.
		updated, err := b.Apply(ctx, key, func(cur *store.TaskRow, now time.Time) (*store.TaskRow, error) {
			next := cur.Clone()
			exp := now.Add(30 * time.Second)
			next.LeaseOwner = "worker-1:lease-a"
			next.LeaseExpiresAt = &exp
			next.NextReconcileAt = &now
			return next, nil
		})
		if err != nil {
			t.Fatalf("live-lease Apply: %v", err)
		}
		if updated.Generation != 2 || !updated.NextReconcileAt.Equal(*updated.LeaseExpiresAt) {
			t.Fatalf("live lease NextReconcileAt = %v, LeaseExpiresAt = %v, gen = %d",
				updated.NextReconcileAt, updated.LeaseExpiresAt, updated.Generation)
		}
		select {
		case <-b.DueNotify():
			t.Fatal("unexpected DueNotify while live lease is held")
		default:
		}

		// Verify GetTask and ListTasks merge authoritative Generation = 2.
		gotTask, err := b.GetTask(ctx, key.Atespace, key.Name)
		if err != nil || gotTask.GetStatus().GetGeneration() != 2 {
			t.Fatalf("GetTask generation = %d, err = %v; want 2", gotTask.GetStatus().GetGeneration(), err)
		}
		listed, err := b.ListTasks(ctx, key.Atespace, 10, 0)
		if err != nil || len(listed) != 1 || listed[0].GetStatus().GetGeneration() != 2 {
			t.Fatalf("ListTasks = %+v, err = %v; want 1 task at generation 2", listed, err)
		}

		// Expired lease + nil NextReconcileAt must clear lease and keep NextReconcileAt nil!
		cleared, err := b.Apply(ctx, key, func(cur *store.TaskRow, now time.Time) (*store.TaskRow, error) {
			next := cur.Clone()
			past := now.Add(-time.Second)
			next.LeaseOwner = "worker-dead"
			next.LeaseExpiresAt = &past
			next.PendingActions = nil
			next.NextReconcileAt = nil
			next.Task.Status.State = v1alpha1.TaskState_TASK_STATE_WORKING
			return next, nil
		})
		if err != nil {
			t.Fatalf("expired-lease Apply: %v", err)
		}
		if cleared.LeaseOwner != "" || cleared.LeaseExpiresAt != nil || cleared.NextReconcileAt != nil {
			t.Fatalf("expired lease not cleared: owner=%q, exp=%v, due=%v",
				cleared.LeaseOwner, cleared.LeaseExpiresAt, cleared.NextReconcileAt)
		}

		// 5. Returning (nil, nil) on an existing row deletes it.
		deleted, err := b.Apply(ctx, key, func(cur *store.TaskRow, _ time.Time) (*store.TaskRow, error) {
			return nil, nil
		})
		if err != nil || deleted != nil {
			t.Fatalf("delete Apply = (%v, %v), want (nil, nil)", deleted, err)
		}
		if _, err := b.GetTaskRow(ctx, key); !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("GetTaskRow after delete = %v, want ErrNotFound", err)
		}
	})

	t.Run("WorkspaceStatusAndSnapshots", func(t *testing.T) {
		ctx := t.Context()
		if err := b.SaveWorkspace(ctx, &v1alpha1.Workspace{
			Metadata: &v1alpha1.ObjectMeta{Atespace: "payments", Name: "payments-ws"},
		}); err != nil {
			t.Fatalf("SaveWorkspace: %v", err)
		}
		if err := b.UpdateWorkspaceStatus(ctx, "payments", "payments-ws", &v1alpha1.WorkspaceStatus{
			Phase: "Ready",
		}); err != nil {
			t.Fatalf("UpdateWorkspaceStatus: %v", err)
		}
		ws, err := b.GetWorkspace(ctx, "payments", "payments-ws")
		if err != nil || ws.GetStatus().GetPhase() != "Ready" {
			t.Fatalf("GetWorkspace phase = %q, err = %v; want Ready", ws.GetStatus().GetPhase(), err)
		}

		if _, err := b.GetWorkspaceSnapshot(ctx, "payments", "payments-ws", "ax-aaaa"); !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("GetWorkspaceSnapshot uncached = %v, want ErrNotFound", err)
		}
		savedSnap, err := b.PutWorkspaceSnapshot(ctx, store.WorkspaceSnapshot{
			Atespace:         "payments",
			WorkspaceName:    "payments-ws",
			AteActorTemplate: "ax-aaaa",
			ActiveTag:        "ws-payments-ws-r1",
			Revision:         1,
		})
		if err != nil || savedSnap.ActiveTag != "ws-payments-ws-r1" || savedSnap.Revision != 1 {
			t.Fatalf("PutWorkspaceSnapshot = %+v, %v", savedSnap, err)
		}
		if _, err := b.PutWorkspaceSnapshot(ctx, store.WorkspaceSnapshot{
			Atespace:         "payments",
			WorkspaceName:    "payments-ws",
			AteActorTemplate: "ax-bbbb",
			ActiveTag:        "ws-payments-ws-b-r2",
			Revision:         2,
		}); err != nil {
			t.Fatalf("PutWorkspaceSnapshot second template: %v", err)
		}

		snaps, err := b.ListWorkspaceSnapshots(ctx, "payments", "payments-ws")
		if err != nil || len(snaps) != 2 {
			t.Fatalf("ListWorkspaceSnapshots = %+v, %v; want 2 entries", snaps, err)
		}
		if err := b.DeleteWorkspaceSnapshots(ctx, "payments", "payments-ws"); err != nil {
			t.Fatalf("DeleteWorkspaceSnapshots: %v", err)
		}
		snaps, err = b.ListWorkspaceSnapshots(ctx, "payments", "payments-ws")
		if err != nil || len(snaps) != 0 {
			t.Fatalf("ListWorkspaceSnapshots after delete = %+v, %v; want empty", snaps, err)
		}
	})
}
