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
	"fmt"
	"testing"
	"time"

	"github.com/google/ax/internal/lifecycle"
	"github.com/google/ax/internal/store"
	runnerv1 "github.com/google/ax/pkg/apis/runner/v1"
	"github.com/google/ax/pkg/apis/v1alpha1"
	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
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

		// 1. On a missing row, returning nil or DeleteTaskRow does nothing:
		// there is still no row afterwards.
		for name, ret := range map[string]*store.TaskRow{"nil": nil, "DeleteTaskRow": store.DeleteTaskRow} {
			noopRow, err := b.Apply(ctx, key, func(cur *store.TaskRow, now time.Time) (*store.TaskRow, error) {
				if cur != nil {
					t.Fatalf("expected nil cur on missing row, got %+v", cur)
				}
				return ret, nil
			})
			if err != nil || noopRow != nil {
				t.Fatalf("Apply returning %s on a missing row = (%v, %v), want (nil, nil)", name, noopRow, err)
			}
			if _, err := b.GetTaskRow(ctx, key); !errors.Is(err, store.ErrNotFound) {
				t.Fatalf("GetTaskRow after returning %s on a missing row = %v, want ErrNotFound", name, err)
			}
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

		// Editing cur in place and returning it is still a no-op: nothing is
		// written, and Apply reports the stored row rather than the edits.
		edited, err := b.Apply(ctx, key, func(cur *store.TaskRow, _ time.Time) (*store.TaskRow, error) {
			cur.RunSeq = 99
			cur.ActiveRunID = "r-99"
			cur.Task.Status.State = v1alpha1.TaskState_TASK_STATE_FAILED
			return cur, nil
		})
		if err != nil {
			t.Fatalf("in-place no-op Apply: %v", err)
		}
		storedRow, err := b.GetTaskRow(ctx, key)
		if err != nil {
			t.Fatalf("GetTaskRow: %v", err)
		}
		for name, row := range map[string]*store.TaskRow{"returned": edited, "stored": storedRow} {
			if row.Generation != 1 || row.RunSeq == 99 || row.ActiveRunID == "r-99" ||
				row.Task.GetStatus().GetState() != v1alpha1.TaskState_TASK_STATE_QUEUED {
				t.Fatalf("%s row after in-place no-op = (gen=%d, runSeq=%d, run=%q, state=%s), want the unedited generation-1 row",
					name, row.Generation, row.RunSeq, row.ActiveRunID, row.Task.GetStatus().GetState())
			}
		}

		// Editing cur and then returning a copy of it is a normal write: the
		// stored row carries exactly what was returned, including the Task
		// document, and its generation follows the stored one, not an edited one.
		written, err := b.Apply(ctx, key, func(cur *store.TaskRow, _ time.Time) (*store.TaskRow, error) {
			cur.Task.Spec.Commands = []string{"pytest", "-x"}
			cur.Generation = 50
			return cur.Clone(), nil
		})
		if err != nil {
			t.Fatalf("edit-then-clone Apply: %v", err)
		}
		storedRow, err = b.GetTaskRow(ctx, key)
		if err != nil {
			t.Fatalf("GetTaskRow: %v", err)
		}
		for name, row := range map[string]*store.TaskRow{"returned": written, "stored": storedRow} {
			if cmds := row.Task.GetSpec().GetCommands(); row.Generation != 2 || len(cmds) != 2 || cmds[1] != "-x" {
				t.Fatalf("%s row after edit-then-clone = (gen=%d, commands=%v), want generation 2 with the edited commands",
					name, row.Generation, cmds)
			}
		}
		if gotTask, err := b.GetTask(ctx, key.Atespace, key.Name); err != nil || len(gotTask.GetSpec().GetCommands()) != 2 {
			t.Fatalf("GetTask after edit-then-clone = (%v, %v), want the edited commands in the task document", gotTask.GetSpec().GetCommands(), err)
		}
		// That write committed a due row, so it signaled DueNotify; drain it
		// before the lease checks below.
		select {
		case <-b.DueNotify():
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
		if updated.Generation != 3 || !updated.NextReconcileAt.Equal(*updated.LeaseExpiresAt) {
			t.Fatalf("live lease NextReconcileAt = %v, LeaseExpiresAt = %v, gen = %d",
				updated.NextReconcileAt, updated.LeaseExpiresAt, updated.Generation)
		}
		select {
		case <-b.DueNotify():
			t.Fatal("unexpected DueNotify while live lease is held")
		default:
		}

		// Verify GetTask and ListTasks merge authoritative Generation = 3.
		gotTask, err := b.GetTask(ctx, key.Atespace, key.Name)
		if err != nil || gotTask.GetStatus().GetGeneration() != 3 {
			t.Fatalf("GetTask generation = %d, err = %v; want 3", gotTask.GetStatus().GetGeneration(), err)
		}
		listed, err := b.ListTasks(ctx, key.Atespace, 10, 0)
		if err != nil || len(listed) != 1 || listed[0].GetStatus().GetGeneration() != 3 {
			t.Fatalf("ListTasks = %+v, err = %v; want 1 task at generation 3", listed, err)
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

		// 5. Returning nil on an existing row never deletes it: nothing is
		// written, the generation is unchanged, and Apply reports the stored row.
		kept, err := b.Apply(ctx, key, func(cur *store.TaskRow, _ time.Time) (*store.TaskRow, error) {
			return nil, nil
		})
		if err != nil || kept == nil || kept.Generation != cleared.Generation {
			t.Fatalf("Apply returning nil on an existing row = (%+v, %v), want the stored row at generation %d",
				kept, err, cleared.Generation)
		}
		if still, err := b.GetTaskRow(ctx, key); err != nil || still.Generation != cleared.Generation {
			t.Fatalf("GetTaskRow after returning nil = (%+v, %v), want the row still at generation %d",
				still, err, cleared.Generation)
		}
		// An error leaves the row alone too, and is passed through unchanged.
		errStop := errors.New("stop")
		if _, err := b.Apply(ctx, key, func(cur *store.TaskRow, _ time.Time) (*store.TaskRow, error) {
			return store.DeleteTaskRow, errStop
		}); !errors.Is(err, errStop) {
			t.Fatalf("Apply returning an error = %v, want it passed through", err)
		}
		if _, err := b.GetTaskRow(ctx, key); err != nil {
			t.Fatalf("GetTaskRow after an ApplyFunc error = %v, want the row kept", err)
		}

		// 6. Only the DeleteTaskRow sentinel deletes the row.
		deleted, err := b.Apply(ctx, key, func(cur *store.TaskRow, _ time.Time) (*store.TaskRow, error) {
			return store.DeleteTaskRow, nil
		})
		if err != nil || deleted != nil {
			t.Fatalf("Apply returning DeleteTaskRow = (%v, %v), want (nil, nil)", deleted, err)
		}
		if _, err := b.GetTaskRow(ctx, key); !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("GetTaskRow after delete = %v, want ErrNotFound", err)
		}
	})

	t.Run("LegacyWritesKeepGenerationMonotonic", func(t *testing.T) {
		ctx := t.Context()
		key := store.TaskKey{Atespace: "payments", Name: "gen-monotonic"}
		newTask := func() *v1alpha1.Task {
			return &v1alpha1.Task{
				Metadata: &v1alpha1.ObjectMeta{Atespace: key.Atespace, Name: key.Name},
				Spec:     &v1alpha1.TaskSpec{Commands: []string{"true"}},
			}
		}
		generation := func(step string) int64 {
			t.Helper()
			got, err := b.GetTask(ctx, key.Atespace, key.Name)
			if err != nil {
				t.Fatalf("GetTask after %s: %v", step, err)
			}
			row, err := b.GetTaskRow(ctx, key)
			if err != nil {
				t.Fatalf("GetTaskRow after %s: %v", step, err)
			}
			if got.GetStatus().GetGeneration() != row.Generation {
				t.Fatalf("after %s: GetTask generation %d != row generation %d",
					step, got.GetStatus().GetGeneration(), row.Generation)
			}
			return row.Generation
		}

		// Advance the row to generation 3 through Apply.
		for range 3 {
			if _, err := b.Apply(ctx, key, func(cur *store.TaskRow, _ time.Time) (*store.TaskRow, error) {
				if cur == nil {
					return &store.TaskRow{Task: newTask()}, nil
				}
				next := cur.Clone()
				next.RunSeq++
				return next, nil
			}); err != nil {
				t.Fatalf("Apply: %v", err)
			}
		}
		last := generation("Apply x3")
		if last != 3 {
			t.Fatalf("setup: generation = %d, want 3", last)
		}
		advanced := func(step string) {
			t.Helper()
			got := generation(step)
			if got <= last {
				t.Fatalf("generation after %s = %d, want > %d (monotonic)", step, got, last)
			}
			last = got
		}

		// A writer with no status.generation (or a stale one) still advances
		// the row and never moves it backwards.
		if err := b.SaveTask(ctx, newTask()); err != nil {
			t.Fatalf("SaveTask: %v", err)
		}
		advanced("SaveTask without status.generation")

		stale := newTask()
		stale.Status = &v1alpha1.TaskStatus{Generation: 1}
		if err := b.SaveTask(ctx, stale); err != nil {
			t.Fatalf("SaveTask stale: %v", err)
		}
		advanced("SaveTask with stale status.generation")

		if err := b.UpdateTaskStatus(ctx, key.Atespace, key.Name, &v1alpha1.TaskStatus{
			State: v1alpha1.TaskState_TASK_STATE_WORKING,
		}); err != nil {
			t.Fatalf("UpdateTaskStatus: %v", err)
		}
		advanced("UpdateTaskStatus without status.generation")

		if err := b.SetTaskDeleteTime(ctx, key.Atespace, key.Name, timestamppb.Now()); err != nil {
			t.Fatalf("SetTaskDeleteTime: %v", err)
		}
		advanced("SetTaskDeleteTime")

		// A caller that already bumped the generation is honored, not doubled.
		ahead := newTask()
		ahead.Status = &v1alpha1.TaskStatus{Generation: last + 1}
		if err := b.SaveTask(ctx, ahead); err != nil {
			t.Fatalf("SaveTask ahead: %v", err)
		}
		if got := generation("SaveTask with bumped status.generation"); got != last+1 {
			t.Fatalf("generation after caller bump = %d, want %d", got, last+1)
		}

		if err := b.DeleteTask(ctx, key.Atespace, key.Name); err != nil {
			t.Fatalf("DeleteTask: %v", err)
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
		// First snapshot for each template: nothing is stored yet, so the
		// expected revision is 0.
		savedSnap, err := b.CompareAndSwapWorkspaceSnapshot(ctx, 0, store.WorkspaceSnapshot{
			Atespace:         "payments",
			WorkspaceName:    "payments-ws",
			AteActorTemplate: "ax-aaaa",
			ActiveTag:        "ws-payments-ws-r1",
			Revision:         1,
		})
		if err != nil || savedSnap.ActiveTag != "ws-payments-ws-r1" || savedSnap.Revision != 1 || savedSnap.UpdatedAt.IsZero() {
			t.Fatalf("CompareAndSwapWorkspaceSnapshot first = %+v, %v", savedSnap, err)
		}
		if _, err := b.CompareAndSwapWorkspaceSnapshot(ctx, 0, store.WorkspaceSnapshot{
			Atespace:         "payments",
			WorkspaceName:    "payments-ws",
			AteActorTemplate: "ax-bbbb",
			ActiveTag:        "ws-payments-ws-b-r2",
			Revision:         2,
		}); err != nil {
			t.Fatalf("CompareAndSwapWorkspaceSnapshot second template: %v", err)
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

	t.Run("WorkspaceSnapshotCompareAndSwap", func(t *testing.T) {
		ctx := t.Context()
		const atespace, ws, tmpl = "payments", "cas-ws", "ax-cccc"
		snap := func(revision uint64) store.WorkspaceSnapshot {
			return store.WorkspaceSnapshot{
				Atespace:         atespace,
				WorkspaceName:    ws,
				AteActorTemplate: tmpl,
				ActiveTag:        fmt.Sprintf("ws-cas-r%d", revision),
				Revision:         revision,
			}
		}
		// stored returns the revision currently stored, or 0 when there is none.
		stored := func() uint64 {
			t.Helper()
			got, err := b.GetWorkspaceSnapshot(ctx, atespace, ws, tmpl)
			if errors.Is(err, store.ErrNotFound) {
				return 0
			}
			if err != nil {
				t.Fatalf("GetWorkspaceSnapshot: %v", err)
			}
			if want := fmt.Sprintf("ws-cas-r%d", got.Revision); got.ActiveTag != want {
				t.Fatalf("stored snapshot = %+v, tag and revision disagree", got)
			}
			return got.Revision
		}
		// conflicts asserts that a swap is refused and leaves the stored snapshot alone.
		conflicts := func(what string, expected, revision, wantStored uint64) {
			t.Helper()
			got, err := b.CompareAndSwapWorkspaceSnapshot(ctx, expected, snap(revision))
			if !errors.Is(err, store.ErrSnapshotConflict) || got != nil {
				t.Fatalf("%s: swap(expected %d -> revision %d) = (%+v, %v), want ErrSnapshotConflict", what, expected, revision, got, err)
			}
			if now := stored(); now != wantStored {
				t.Fatalf("%s: stored revision = %d after a refused swap, want %d", what, now, wantStored)
			}
		}
		swaps := func(what string, expected, revision uint64) {
			t.Helper()
			got, err := b.CompareAndSwapWorkspaceSnapshot(ctx, expected, snap(revision))
			if err != nil || got.Revision != revision || got.UpdatedAt.IsZero() {
				t.Fatalf("%s: swap(expected %d -> revision %d) = (%+v, %v), want success", what, expected, revision, got, err)
			}
			if now := stored(); now != revision {
				t.Fatalf("%s: stored revision = %d, want %d", what, now, revision)
			}
		}

		conflicts("replace a snapshot that does not exist", 3, 4, 0)
		swaps("first snapshot", 0, 1)
		conflicts("create when one already exists", 0, 2, 1)
		swaps("replace the revision that was read", 1, 2)
		conflicts("writer that read the old revision", 1, 3, 2)
		conflicts("writer that expects a newer revision than stored", 5, 6, 2)
		swaps("revisions may skip numbers", 2, 7)

		// A new revision that is not greater than the expected one is a caller
		// bug, reported as an ordinary error rather than a conflict.
		for _, bad := range []struct{ expected, revision uint64 }{{7, 7}, {7, 3}, {0, 0}} {
			got, err := b.CompareAndSwapWorkspaceSnapshot(ctx, bad.expected, snap(bad.revision))
			if err == nil || errors.Is(err, store.ErrSnapshotConflict) || got != nil {
				t.Fatalf("swap(expected %d -> revision %d) = (%+v, %v), want a non-conflict error", bad.expected, bad.revision, got, err)
			}
		}
		if _, err := b.CompareAndSwapWorkspaceSnapshot(ctx, 0, store.WorkspaceSnapshot{Atespace: atespace, ActiveTag: "t", Revision: 1}); err == nil {
			t.Fatal("swap without a workspace name and template succeeded, want an error")
		}
		untagged := snap(8)
		untagged.ActiveTag = ""
		if got, err := b.CompareAndSwapWorkspaceSnapshot(ctx, 7, untagged); err == nil || errors.Is(err, store.ErrSnapshotConflict) || got != nil {
			t.Fatalf("swap without an ActiveTag = (%+v, %v), want a non-conflict error", got, err)
		}
		if now := stored(); now != 7 {
			t.Fatalf("stored revision = %d after rejected arguments, want 7", now)
		}

		// Deleting a Workspace's snapshots fences out a writer that had read one.
		if err := b.DeleteWorkspaceSnapshots(ctx, atespace, ws); err != nil {
			t.Fatalf("DeleteWorkspaceSnapshots: %v", err)
		}
		conflicts("writer whose base snapshot was deleted", 7, 8, 0)

		// When multiple writers race from the same expected revision, exactly
		// one succeeds and the rest receive ErrSnapshotConflict.
		const writers = 8
		results := make(chan error, writers)
		for i := range writers {
			go func() {
				_, err := b.CompareAndSwapWorkspaceSnapshot(ctx, 0, snap(uint64(i+1)))
				results <- err
			}()
		}
		won := 0
		for range writers {
			switch err := <-results; {
			case err == nil:
				won++
			case !errors.Is(err, store.ErrSnapshotConflict):
				t.Fatalf("racing swap failed with %v, want success or ErrSnapshotConflict", err)
			}
		}
		if won != 1 {
			t.Fatalf("%d of %d racing writers succeeded, want exactly 1", won, writers)
		}
		if now := stored(); now < 1 || now > writers {
			t.Fatalf("stored revision after the race = %d, want the winner's (1..%d)", now, writers)
		}
		if err := b.DeleteWorkspaceSnapshots(ctx, atespace, ws); err != nil {
			t.Fatalf("DeleteWorkspaceSnapshots: %v", err)
		}
	})
}
