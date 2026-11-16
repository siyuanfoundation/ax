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
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/ax/internal/lifecycle"
	"github.com/google/ax/internal/store"
	"github.com/google/ax/pkg/apis/v1alpha1"
)

func TestPostgres_TaskSweepSnapshotHeartbeatsAndShardLeases(t *testing.T) {
	s := openTest(t, Options{
		ApplyLockTimeout:      500 * time.Millisecond,
		ApplyStatementTimeout: 5 * time.Second,
	})
	ctx := context.Background()

	insertTask := func(ns, name string, state v1alpha1.TaskState, actorUID string, tokenHash []byte, withAction bool) {
		t.Helper()
		key := store.TaskKey{Atespace: ns, Name: name}
		if _, err := s.Apply(ctx, key, func(_ *store.TaskRow, now time.Time) (*store.TaskRow, error) {
			resumed := now.Add(-5 * time.Second)
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
				st.Actions = []lifecycle.Action{st.NextAction(lifecycle.ActionResumeActor, now)}
			}
			row := &store.TaskRow{}
			row.SetLifecycleState(st, now)
			return row, nil
		}); err != nil {
			t.Fatalf("Apply(%s/%s): %v", ns, name, err)
		}
	}

	tok1 := []byte{0x10, 0x20, 0x30, 0x40}
	tok2 := []byte{0xaa, 0xbb, 0xcc, 0xdd}
	insertTask("payments", "active-1", v1alpha1.TaskState_TASK_STATE_WORKING, "uid-1", tok1, false)
	insertTask("payments", "active-2", v1alpha1.TaskState_TASK_STATE_QUEUED, "uid-2", tok2, true)
	insertTask("payments", "done-1", v1alpha1.TaskState_TASK_STATE_COMPLETED, "uid-3", nil, false)
	insertTask("terminal-ns", "cancelled-1", v1alpha1.TaskState_TASK_STATE_CANCELLED, "uid-4", nil, false)

	// 1. Loose index skip scan discovers all distinct atespaces (including terminal-only).
	atespaces, err := s.ListTaskAtespaces(ctx)
	if err != nil {
		t.Fatalf("ListTaskAtespaces: %v", err)
	}
	if !slices.Equal(atespaces, []string{"payments", "terminal-ns"}) {
		t.Fatalf("ListTaskAtespaces = %v, want [payments terminal-ns]", atespaces)
	}

	// 2. Verify PostgreSQL generic cached plans push atespace = ANY($1) directly
	// into tasks_active_idx Index Cond without OR-predicate degradation.
	planTx, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin plan tx: %v", err)
	}
	defer rollbackTx(planTx)
	if _, err := planTx.Exec(ctx, `SET LOCAL plan_cache_mode = force_generic_plan; SET LOCAL enable_seqscan = off`); err != nil {
		t.Fatalf("SET LOCAL plan_cache_mode: %v", err)
	}
	var planLines []string
	planRows, err := planTx.Query(ctx,
		`EXPLAIN (FORMAT TEXT) `+activeSweepSelectPrefix+` AND atespace = ANY($1::text[]) AND (atespace, name) > ($2, $3) ORDER BY atespace ASC, name ASC LIMIT $4`,
		[]string{"payments"}, "a", "b", sweepActivePageSize,
	)
	if err != nil {
		t.Fatalf("EXPLAIN SnapshotActiveTasks generic plan: %v", err)
	}
	for planRows.Next() {
		var line string
		if err := planRows.Scan(&line); err != nil {
			planRows.Close()
			t.Fatalf("scan plan line: %v", err)
		}
		planLines = append(planLines, line)
	}
	planRows.Close()
	_ = planTx.Rollback(ctx)
	planText := strings.Join(planLines, "\n")
	if !strings.Contains(planText, "tasks_active_idx") || !strings.Contains(planText, "Index Cond:") || !strings.Contains(planText, "atespace = ANY") {
		t.Fatalf("expected generic plan to push atespace = ANY($1) into tasks_active_idx Index Cond, got:\n%s", planText)
	}

	// 3. SnapshotActiveTasks returns only non-terminal rows with accurate HasPendingActions,
	// HasRunnerToken, and HasClaimDeadline.
	records, storeNow, err := s.SnapshotActiveTasks(ctx, []string{"payments", "terminal-ns"})
	if err != nil {
		t.Fatalf("SnapshotActiveTasks: %v", err)
	}
	if storeNow.IsZero() || len(records) != 2 {
		t.Fatalf("SnapshotActiveTasks = (%+v, %v), want 2 active records", records, storeNow)
	}
	if records[0].Key.Name != "active-1" || records[0].HasPendingActions || !records[0].HasRunnerToken || records[0].HasClaimDeadline ||
		records[1].Key.Name != "active-2" || !records[1].HasPendingActions || !records[1].HasRunnerToken {
		t.Fatalf("unexpected active records: %+v", records)
	}

	// 4. GetTaskRows batch-loads tasks and scrubs expired worker leases.
	if _, _, _, err := s.LeaseTask(ctx, store.TaskKey{Atespace: "payments", Name: "active-2"}, "pg-worker", 40*time.Millisecond); err != nil {
		t.Fatalf("LeaseTask: %v", err)
	}
	time.Sleep(60 * time.Millisecond)
	batch, err := s.GetTaskRows(ctx, []store.TaskKey{
		{Atespace: "payments", Name: "done-1"},
		{Atespace: "payments", Name: "active-2"},
		{Atespace: "payments", Name: "missing"},
	})
	if err != nil || len(batch) != 2 {
		t.Fatalf("GetTaskRows = (%d, %v), want 2", len(batch), err)
	}
	if got := batch[store.TaskKey{Atespace: "payments", Name: "active-2"}]; got.LeaseOwner != "" || got.LeaseExpiresAt != nil {
		t.Fatalf("expected expired lease scrubbed on Postgres, got owner=%q exp=%v", got.LeaseOwner, got.LeaseExpiresAt)
	}

	// 5. Concurrent unsorted FlushHeartbeats calls verify token hashes, never deadlock,
	// never bump generation, and maintain ax_replicas watermarks.
	var wg sync.WaitGroup
	for g := range 4 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			entries := []store.HeartbeatEntry{
				{Key: store.TaskKey{Atespace: "payments", Name: "active-2"}, RunnerTokenHash: tok2, Age: 200 * time.Millisecond},
				{Key: store.TaskKey{Atespace: "payments", Name: "active-1"}, RunnerTokenHash: []byte{0x99}, Age: 0}, // wrong token
				{Key: store.TaskKey{Atespace: "payments", Name: "active-1"}, RunnerTokenHash: tok1, Age: 500 * time.Millisecond},
			}
			if id%2 == 1 {
				slices.Reverse(entries)
			}
			if flushErr := s.FlushHeartbeats(ctx, fmt.Sprintf("rep-%d", id), 45*time.Second, entries); flushErr != nil {
				t.Errorf("concurrent FlushHeartbeats(rep-%d): %v", id, flushErr)
			}
		}(g)
	}
	wg.Wait()

	r1, err := s.GetTaskRow(ctx, store.TaskKey{Atespace: "payments", Name: "active-1"})
	if err != nil {
		t.Fatalf("GetTaskRow(active-1): %v", err)
	}
	if r1.Generation != 1 || r1.LastHeartbeatAt == nil {
		t.Fatalf("expected Generation=1 and populated LastHeartbeatAt, got gen=%d hb=%v", r1.Generation, r1.LastHeartbeatAt)
	}
	prevHB := *r1.LastHeartbeatAt
	// Older heartbeat (Age = 10s) must not regress LastHeartbeatAt.
	if err := s.FlushHeartbeats(ctx, "rep-0", 45*time.Second, []store.HeartbeatEntry{
		{Key: store.TaskKey{Atespace: "payments", Name: "active-1"}, RunnerTokenHash: tok1, Age: 10 * time.Second},
	}); err != nil {
		t.Fatalf("FlushHeartbeats(stale age): %v", err)
	}
	r1After, _ := s.GetTaskRow(ctx, store.TaskKey{Atespace: "payments", Name: "active-1"})
	if !r1After.LastHeartbeatAt.Equal(prevHB) {
		t.Fatalf("LastHeartbeatAt changed on older age: got %v, want %v", r1After.LastHeartbeatAt, prevHB)
	}

	stats, err := s.InspectReplicas(ctx)
	if err != nil || stats.LiveReplicas != 4 || !stats.HasLiveReplicas || stats.WatermarkAge > 5*time.Second {
		t.Fatalf("InspectReplicas = %+v, %v; want 4 live replicas with fresh watermark", stats, err)
	}
	for g := range 4 {
		if err := s.UnregisterReplica(ctx, fmt.Sprintf("rep-%d", g)); err != nil {
			t.Fatalf("UnregisterReplica(rep-%d): %v", g, err)
		}
	}
	statsAfter, err := s.InspectReplicas(ctx)
	if err != nil || statsAfter.LiveReplicas != 0 || statsAfter.HasLiveReplicas {
		t.Fatalf("InspectReplicas after UnregisterReplica = %+v, %v; want 0", statsAfter, err)
	}

	// 6. Shard leases in PostgreSQL locks table: contention, renewal, expiry, and takeover.
	acq, _, err := s.TryAcquireShardLease(ctx, "lock:sweep-shard:3", "holder-a", 80*time.Millisecond)
	if err != nil || !acq {
		t.Fatalf("TryAcquireShardLease(holder-a) = (%v, %v), want true", acq, err)
	}
	if acqB, _, err := s.TryAcquireShardLease(ctx, "lock:sweep-shard:3", "holder-b", 80*time.Millisecond); err != nil || acqB {
		t.Fatalf("TryAcquireShardLease(holder-b while held) = (%v, %v), want false", acqB, err)
	}
	if renewed, _, err := s.RenewShardLease(ctx, "lock:sweep-shard:3", "holder-a", 80*time.Millisecond); err != nil || !renewed {
		t.Fatalf("RenewShardLease(holder-a) = (%v, %v), want true", renewed, err)
	}
	time.Sleep(110 * time.Millisecond)
	if renewed, _, err := s.RenewShardLease(ctx, "lock:sweep-shard:3", "holder-a", 80*time.Millisecond); err != nil || renewed {
		t.Fatalf("RenewShardLease(expired) = (%v, %v), want false", renewed, err)
	}
	if acqB, _, err := s.TryAcquireShardLease(ctx, "lock:sweep-shard:3", "holder-b", 5*time.Second); err != nil || !acqB {
		t.Fatalf("TryAcquireShardLease(holder-b after expiry) = (%v, %v), want true", acqB, err)
	}
	if err := s.ReleaseShardLease(ctx, "lock:sweep-shard:3", "holder-a"); err != nil {
		t.Fatalf("ReleaseShardLease(stale holder-a): %v", err)
	}
	if renewedB, _, err := s.RenewShardLease(ctx, "lock:sweep-shard:3", "holder-b", 5*time.Second); err != nil || !renewedB {
		t.Fatalf("expected holder-b lease intact after stale release, got (%v, %v)", renewedB, err)
	}
	if err := s.ReleaseShardLease(ctx, "lock:sweep-shard:3", "holder-b"); err != nil {
		t.Fatalf("ReleaseShardLease(holder-b): %v", err)
	}

	// 7. EvalPlanQual MVCC regression test: while Apply holds FOR UPDATE on a task
	// row and commits a row update (moving its heap ctid), a concurrent blocking
	// FlushHeartbeats still advances last_heartbeat_at without losing the heartbeat!
	epqKey := store.TaskKey{Atespace: "payments", Name: "epq-race"}
	insertTask(epqKey.Atespace, epqKey.Name, v1alpha1.TaskState_TASK_STATE_WORKING, "uid-epq", tok1, false)

	applyEntered := make(chan struct{})
	releaseApply := make(chan struct{})
	applyDone := make(chan error, 1)
	go func() {
		_, aErr := s.Apply(ctx, epqKey, func(cur *store.TaskRow, now time.Time) (*store.TaskRow, error) {
			close(applyEntered)
			<-releaseApply
			cp := cur.Clone()
			st := cp.LifecycleState()
			st.ActiveRunID = "epq-moved-tuple"
			cp.SetLifecycleState(st, now)
			return cp, nil
		})
		applyDone <- aErr
	}()

	<-applyEntered
	flushDone := make(chan error, 1)
	go func() {
		flushDone <- s.FlushHeartbeats(ctx, "rep-epq", 45*time.Second, []store.HeartbeatEntry{
			{Key: epqKey, RunnerTokenHash: tok1, Age: 10 * time.Millisecond},
		})
	}()
	// Give FlushHeartbeats time to capture its statement snapshot and block on FOR NO KEY UPDATE.
	time.Sleep(40 * time.Millisecond)
	close(releaseApply)

	if err := <-applyDone; err != nil {
		t.Fatalf("concurrent Apply: %v", err)
	}
	if err := <-flushDone; err != nil {
		t.Fatalf("concurrent FlushHeartbeats: %v", err)
	}
	epqRow, err := s.GetTaskRow(ctx, epqKey)
	if err != nil {
		t.Fatalf("GetTaskRow(epq-race): %v", err)
	}
	if epqRow.Generation != 2 || epqRow.LastHeartbeatAt == nil {
		t.Fatalf("EvalPlanQual dropped concurrent heartbeat! gen=%d lastHB=%v", epqRow.Generation, epqRow.LastHeartbeatAt)
	}

	// 8. Exercise multi-page SnapshotActiveTasks keyset pagination across all 4 SQL
	// query shapes (both unfiltered and atespace-filtered with hasCursor == true).
	insertTask("alpha", "p1", v1alpha1.TaskState_TASK_STATE_WORKING, "uid-a1", tok1, false)
	insertTask("alpha", "p2", v1alpha1.TaskState_TASK_STATE_WORKING, "uid-a2", tok1, false)
	oldPageSize := sweepActivePageSize
	sweepActivePageSize = 2
	defer func() { sweepActivePageSize = oldPageSize }()

	allPaged, _, err := s.SnapshotActiveTasks(ctx, nil)
	if err != nil || len(allPaged) != 5 {
		t.Fatalf("multi-page unfiltered SnapshotActiveTasks = (%d rows, %v), want 5", len(allPaged), err)
	}
	filteredPaged, _, err := s.SnapshotActiveTasks(ctx, []string{"alpha", "payments"})
	if err != nil || len(filteredPaged) != 5 {
		t.Fatalf("multi-page filtered SnapshotActiveTasks = (%d rows, %v), want 5", len(filteredPaged), err)
	}

	// 9. Lock-timeout fallback on FlushHeartbeats: when a tasks row lock times
	// out, FlushHeartbeats returns store.ErrLockTimeout while still renewing
	// ax_replicas.expires_at without advancing last_flush_at.
	sShortLock := openTest(t, Options{
		ApplyLockTimeout:      50 * time.Millisecond,
		ApplyStatementTimeout: 2 * time.Second,
	})
	insertShort := func() {
		_, err := sShortLock.Apply(ctx, store.TaskKey{Atespace: "payments", Name: "locked-row"}, func(_ *store.TaskRow, now time.Time) (*store.TaskRow, error) {
			st := lifecycle.State{
				Task: &v1alpha1.Task{
					Status: &v1alpha1.TaskStatus{
						State:            v1alpha1.TaskState_TASK_STATE_WORKING,
						AteActorTemplate: "ax-tmpl",
					},
				},
				ActorUID:        "uid-lk",
				RunnerTokenHash: tok1,
			}
			row := &store.TaskRow{}
			row.SetLifecycleState(st, now)
			return row, nil
		})
		if err != nil {
			t.Fatalf("Apply(locked-row): %v", err)
		}
	}
	insertShort()
	if err := sShortLock.FlushHeartbeats(ctx, "rep-lock-test", 45*time.Second, nil); err != nil {
		t.Fatalf("initial FlushHeartbeats(rep-lock-test): %v", err)
	}
	time.Sleep(80 * time.Millisecond)

	holdTx, err := sShortLock.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin holdTx: %v", err)
	}
	defer rollbackTx(holdTx)
	var dummy int
	if err := holdTx.QueryRow(ctx, `SELECT 1 FROM tasks WHERE atespace = 'payments' AND name = 'locked-row' FOR UPDATE`).Scan(&dummy); err != nil {
		t.Fatalf("SELECT FOR UPDATE: %v", err)
	}

	lockErr := sShortLock.FlushHeartbeats(ctx, "rep-lock-test", 45*time.Second, []store.HeartbeatEntry{
		{Key: store.TaskKey{Atespace: "payments", Name: "locked-row"}, RunnerTokenHash: tok1, Age: 0},
	})
	_ = holdTx.Rollback(ctx)
	if !errors.Is(lockErr, store.ErrLockTimeout) {
		t.Fatalf("FlushHeartbeats under row lock = %v, want store.ErrLockTimeout", lockErr)
	}
	lockStats, err := sShortLock.InspectReplicas(ctx)
	if err != nil || lockStats.LiveReplicas != 1 || lockStats.WatermarkAge < 80*time.Millisecond {
		t.Fatalf("InspectReplicas after lock-timeout FlushHeartbeats = %+v, %v; want LiveReplicas=1 and un-advanced WatermarkAge >= 80ms", lockStats, err)
	}
}
