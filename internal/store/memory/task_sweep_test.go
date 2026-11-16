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
	"slices"
	"testing"
	"time"

	"github.com/google/ax/internal/lifecycle"
	"github.com/google/ax/internal/store"
	"github.com/google/ax/internal/store/memory"
	"github.com/google/ax/pkg/apis/v1alpha1"
)

func TestMemoryStore_TaskSweepStoreSemantics(t *testing.T) {
	ctx := t.Context()
	now := time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)
	s := memory.NewStore(memory.WithClock(func() time.Time { return now }))

	putTask := func(ns, name string, state v1alpha1.TaskState, actorUID string, tokenHash []byte, withAction bool) {
		t.Helper()
		key := store.TaskKey{Atespace: ns, Name: name}
		if _, err := s.Apply(ctx, key, func(_ *store.TaskRow, ts time.Time) (*store.TaskRow, error) {
			resumed := ts.Add(-10 * time.Second)
			st := lifecycle.State{
				Task: &v1alpha1.Task{
					Status: &v1alpha1.TaskStatus{
						State:            state,
						AteActorTemplate: "ax-tmpl",
					},
				},
				ActorUID:             actorUID,
				RunnerTokenHash:      tokenHash,
				RunnerTokenConfirmed: len(tokenHash) > 0,
				LastResumedAt:        &resumed,
			}
			if withAction {
				st.Actions = []lifecycle.Action{st.NextAction(lifecycle.ActionResumeActor, ts)}
			}
			row := &store.TaskRow{}
			row.SetLifecycleState(st, ts)
			return row, nil
		}); err != nil {
			t.Fatalf("Apply(%s/%s): %v", ns, name, err)
		}
	}

	// Empty store returns empty atespaces and zero live replicas.
	if ns, err := s.ListTaskAtespaces(ctx); err != nil || len(ns) != 0 {
		t.Fatalf("empty ListTaskAtespaces = (%v, %v), want empty", ns, err)
	}
	if stats, err := s.InspectReplicas(ctx); err != nil || stats.HasLiveReplicas || stats.LiveReplicas != 0 || stats.WatermarkAge != 0 {
		t.Fatalf("empty InspectReplicas = (%+v, %v), want 0 live replicas", stats, err)
	}

	tokA := []byte{0x01, 0x02, 0x03}
	putTask("payments", "active-working", v1alpha1.TaskState_TASK_STATE_WORKING, "uid-1", tokA, false)
	putTask("payments", "done-task", v1alpha1.TaskState_TASK_STATE_COMPLETED, "uid-2", nil, false)
	putTask("archive-only", "failed-task", v1alpha1.TaskState_TASK_STATE_FAILED, "uid-3", nil, false)
	putTask("", "default-queued", v1alpha1.TaskState_TASK_STATE_QUEUED, "uid-4", nil, true)

	// 1. ListTaskAtespaces includes both active and terminal-only atespaces in sorted order.
	atespaces, err := s.ListTaskAtespaces(ctx)
	if err != nil {
		t.Fatalf("ListTaskAtespaces: %v", err)
	}
	wantNs := []string{"archive-only", "default", "payments"}
	if !slices.Equal(atespaces, wantNs) {
		t.Fatalf("ListTaskAtespaces = %v, want %v", atespaces, wantNs)
	}

	// 2. SnapshotActiveTasks excludes terminal tasks, respects atespace filters, and reports HasPendingActions.
	allActive, snapNow, err := s.SnapshotActiveTasks(ctx, nil)
	if err != nil || !snapNow.Equal(now) || len(allActive) != 2 {
		t.Fatalf("SnapshotActiveTasks(all) = (%+v, %v, %v), want 2 active records", allActive, snapNow, err)
	}
	if allActive[0].Key.Atespace != "default" || !allActive[0].HasPendingActions {
		t.Fatalf("unexpected first active record: %+v", allActive[0])
	}
	if allActive[1].Key.Atespace != "payments" || allActive[1].HasPendingActions || allActive[1].ActorUID != "uid-1" {
		t.Fatalf("unexpected second active record: %+v", allActive[1])
	}

	filteredActive, _, err := s.SnapshotActiveTasks(ctx, []string{"payments", "archive-only"})
	if err != nil || len(filteredActive) != 1 || filteredActive[0].Key.Name != "active-working" {
		t.Fatalf("SnapshotActiveTasks(filtered) = (%+v, %v), want [payments/active-working]", filteredActive, err)
	}

	// 3. GetTaskRows batch-loads rows, omits missing keys, and scrubs expired worker leases.
	leased, _, _, err := s.LeaseTask(ctx, store.TaskKey{Atespace: "default", Name: "default-queued"}, "worker-1", 10*time.Second)
	if err != nil || leased == nil {
		t.Fatalf("LeaseTask: %v", err)
	}
	now = now.Add(15 * time.Second) // expire worker-1's 10s lease
	batch, err := s.GetTaskRows(ctx, []store.TaskKey{
		{Atespace: "payments", Name: "done-task"},
		{Atespace: "default", Name: "default-queued"},
		{Atespace: "payments", Name: "nonexistent"},
	})
	if err != nil || len(batch) != 2 {
		t.Fatalf("GetTaskRows = (%d rows, %v), want 2", len(batch), err)
	}
	if got := batch[store.TaskKey{Atespace: "default", Name: "default-queued"}]; got.LeaseOwner != "" || got.LeaseExpiresAt != nil {
		t.Fatalf("expected expired lease scrubbed in GetTaskRows, got owner=%q exp=%v", got.LeaseOwner, got.LeaseExpiresAt)
	}

	// 4. FlushHeartbeats updates LastHeartbeatAt using (storeNow - Age) without bumping Generation,
	// ignores mismatched tokens and older timestamps, and tracks multi-replica watermarks.
	workKey := store.TaskKey{Atespace: "payments", Name: "active-working"}
	if err := s.FlushHeartbeats(ctx, "rep-a", 45*time.Second, []store.HeartbeatEntry{
		{Key: workKey, RunnerTokenHash: []byte{0xff}, Age: time.Second}, // wrong token -> ignored
		{Key: workKey, RunnerTokenHash: tokA, Age: 4 * time.Second},     // valid -> now - 4s
	}); err != nil {
		t.Fatalf("FlushHeartbeats(rep-a): %v", err)
	}
	row, err := s.GetTaskRow(ctx, workKey)
	if err != nil {
		t.Fatalf("GetTaskRow: %v", err)
	}
	wantHB := now.Add(-4 * time.Second)
	if row.Generation != 1 || row.LastHeartbeatAt == nil || !row.LastHeartbeatAt.Equal(wantHB) {
		t.Fatalf("after valid heartbeat: gen=%d lastHB=%v, want gen=1 lastHB=%v", row.Generation, row.LastHeartbeatAt, wantHB)
	}

	// Older heartbeat timestamp (now - 10s < now - 4s) does not move LastHeartbeatAt backward.
	if err := s.FlushHeartbeats(ctx, "rep-a", 45*time.Second, []store.HeartbeatEntry{
		{Key: workKey, RunnerTokenHash: tokA, Age: 10 * time.Second},
	}); err != nil {
		t.Fatalf("FlushHeartbeats(older): %v", err)
	}
	row, _ = s.GetTaskRow(ctx, workKey)
	if !row.LastHeartbeatAt.Equal(wantHB) {
		t.Fatalf("LastHeartbeatAt regressed to %v, want %v", row.LastHeartbeatAt, wantHB)
	}

	// Advance clock by 20s and register idle rep-b; WatermarkAge reflects rep-a's 20s-old flush.
	now = now.Add(20 * time.Second)
	if err := s.FlushHeartbeats(ctx, "rep-b", 45*time.Second, nil); err != nil {
		t.Fatalf("FlushHeartbeats(rep-b): %v", err)
	}
	stats, err := s.InspectReplicas(ctx)
	if err != nil || stats.LiveReplicas != 2 || stats.WatermarkAge != 20*time.Second {
		t.Fatalf("InspectReplicas(2 live) = %+v, %v; want LiveReplicas=2 WatermarkAge=20s", stats, err)
	}

	// Advance clock by 30s (rep-a's 45s TTL expired 5s ago; rep-b's flush is now 30s old).
	now = now.Add(30 * time.Second)
	stats, err = s.InspectReplicas(ctx)
	if err != nil || stats.LiveReplicas != 1 || stats.WatermarkAge != 30*time.Second {
		t.Fatalf("InspectReplicas(after rep-a expiry) = %+v, %v; want LiveReplicas=1 WatermarkAge=30s", stats, err)
	}

	if err := s.UnregisterReplica(ctx, "rep-b"); err != nil {
		t.Fatalf("UnregisterReplica: %v", err)
	}
	stats, _ = s.InspectReplicas(ctx)
	if stats.HasLiveReplicas || stats.LiveReplicas != 0 {
		t.Fatalf("expected 0 live replicas after UnregisterReplica, got %+v", stats)
	}

	// 5. Shard lease contention, renewal, expiry takeover, and token-fenced release.
	acq, leaseAt, err := s.TryAcquireShardLease(ctx, "lock:sweep-shard:0", "tok-1", 45*time.Second)
	if err != nil || !acq || !leaseAt.Equal(now) {
		t.Fatalf("TryAcquireShardLease(tok-1) = (%v, %v, %v)", acq, leaseAt, err)
	}
	if acq2, _, err := s.TryAcquireShardLease(ctx, "lock:sweep-shard:0", "tok-2", 45*time.Second); err != nil || acq2 {
		t.Fatalf("competing TryAcquireShardLease(tok-2) = (%v, %v), want false", acq2, err)
	}
	if sameAcq, _, err := s.TryAcquireShardLease(ctx, "lock:sweep-shard:0", "tok-1", 45*time.Second); err != nil || !sameAcq {
		t.Fatalf("idempotent TryAcquireShardLease(tok-1) = (%v, %v), want true", sameAcq, err)
	}
	now = now.Add(15 * time.Second)
	if renewed, _, err := s.RenewShardLease(ctx, "lock:sweep-shard:0", "tok-2", 45*time.Second); err != nil || renewed {
		t.Fatalf("RenewShardLease(wrong token) = (%v, %v), want false", renewed, err)
	}
	if renewed, _, err := s.RenewShardLease(ctx, "lock:sweep-shard:0", "tok-1", 45*time.Second); err != nil || !renewed {
		t.Fatalf("RenewShardLease(tok-1) = (%v, %v), want true", renewed, err)
	}

	// Wrong token cannot release; after TTL expires,RenewShardLease returns false and tok-2 takes over.
	if err := s.ReleaseShardLease(ctx, "lock:sweep-shard:0", "tok-2"); err != nil {
		t.Fatalf("ReleaseShardLease(wrong token): %v", err)
	}
	now = now.Add(46 * time.Second)
	if renewed, _, err := s.RenewShardLease(ctx, "lock:sweep-shard:0", "tok-1", 45*time.Second); err != nil || renewed {
		t.Fatalf("RenewShardLease(expired) = (%v, %v), want false", renewed, err)
	}
	if acq2, _, err := s.TryAcquireShardLease(ctx, "lock:sweep-shard:0", "tok-2", 45*time.Second); err != nil || !acq2 {
		t.Fatalf("takeover TryAcquireShardLease(tok-2) = (%v, %v), want true", acq2, err)
	}
	if err := s.ReleaseShardLease(ctx, "lock:sweep-shard:0", "tok-2"); err != nil {
		t.Fatalf("ReleaseShardLease(tok-2): %v", err)
	}
	if acq3, _, err := s.TryAcquireShardLease(ctx, "lock:sweep-shard:0", "tok-3", 45*time.Second); err != nil || !acq3 {
		t.Fatalf("TryAcquireShardLease(tok-3 after release) = (%v, %v), want true", acq3, err)
	}

	// 6. Claiming an unclaimed QUEUED task (clearing ClaimDeadline) automatically
	// stamps LastHeartbeatAt = &now so slow claims start with a fresh baseline.
	claimKey := store.TaskKey{Atespace: "payments", Name: "slow-claim"}
	if _, err := s.Apply(ctx, claimKey, func(_ *store.TaskRow, ts time.Time) (*store.TaskRow, error) {
		dl := ts.Add(lifecycle.ClaimWatchdogTimeout)
		st := lifecycle.State{
			Task: &v1alpha1.Task{
				Status: &v1alpha1.TaskStatus{
					State:            v1alpha1.TaskState_TASK_STATE_QUEUED,
					AteActorTemplate: "ax-tmpl",
				},
			},
			ActorUID:        "uid-claim",
			RunnerTokenHash: tokA,
			ClaimDeadline:   &dl,
		}
		row := &store.TaskRow{}
		row.SetLifecycleState(st, ts)
		return row, nil
	}); err != nil {
		t.Fatalf("Apply(seed slow-claim): %v", err)
	}
	now = now.Add(200 * time.Second)
	claimedRow, err := s.Apply(ctx, claimKey, func(cur *store.TaskRow, ts time.Time) (*store.TaskRow, error) {
		cp := cur.Clone()
		st := cp.LifecycleState()
		st.ClaimDeadline = nil
		st.RunnerTokenConfirmed = true
		cp.SetLifecycleState(st, ts)
		return cp, nil
	})
	if err != nil {
		t.Fatalf("Apply(claim slow-claim): %v", err)
	}
	if claimedRow.LastHeartbeatAt == nil || !claimedRow.LastHeartbeatAt.Equal(now) {
		t.Fatalf("expected LastHeartbeatAt stamped to %v on ClaimDeadline clear, got %v", now, claimedRow.LastHeartbeatAt)
	}

	// 7. Direct atenet-router wakeup (IDLE -> WORKING with ClaimDeadline == nil)
	// and /readyz doorbell claim (Actions=[RESUME_ACTOR] -> WORKING) also stamp
	// LastHeartbeatAt = &now on transition into steady-state heartbeat monitoring.
	routerKey := store.TaskKey{Atespace: "payments", Name: "router-wakeup"}
	_, _ = s.Apply(ctx, routerKey, func(_ *store.TaskRow, ts time.Time) (*store.TaskRow, error) {
		st := lifecycle.State{
			Task: &v1alpha1.Task{
				Status: &v1alpha1.TaskStatus{
					State:            v1alpha1.TaskState_TASK_STATE_IDLE,
					AteActorTemplate: "ax-tmpl",
				},
			},
			ActorUID:             "uid-router",
			RunnerTokenHash:      tokA,
			RunnerTokenConfirmed: true,
		}
		row := &store.TaskRow{}
		row.SetLifecycleState(st, ts)
		return row, nil
	})
	now = now.Add(300 * time.Second)
	wokenRow, err := s.Apply(ctx, routerKey, func(cur *store.TaskRow, ts time.Time) (*store.TaskRow, error) {
		cp := cur.Clone()
		st := cp.LifecycleState()
		st.Task.Status.State = v1alpha1.TaskState_TASK_STATE_WORKING
		cp.SetLifecycleState(st, ts)
		return cp, nil
	})
	if err != nil || wokenRow.LastHeartbeatAt == nil || !wokenRow.LastHeartbeatAt.Equal(now) {
		t.Fatalf("router wakeup IDLE->WORKING LastHeartbeatAt = %v (err=%v), want %v", wokenRow.LastHeartbeatAt, err, now)
	}
}
