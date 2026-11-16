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

package reconciler_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/ax/internal/lifecycle"
	"github.com/google/ax/internal/reconciler"
	"github.com/google/ax/internal/store"
	"github.com/google/ax/internal/store/memory"
	"github.com/google/ax/pkg/apis/v1alpha1"
)

type interceptSweepStore struct {
	store.TaskSweepStore

	mu             sync.Mutex
	flushErr       error
	getTaskRowsErr error
	getTaskRowsCnt int
	unregistered   int
	flushes        int
	onFlush        func()
}

func (f *interceptSweepStore) GetTaskRows(ctx context.Context, keys []store.TaskKey) (map[store.TaskKey]*store.TaskRow, error) {
	f.mu.Lock()
	f.getTaskRowsCnt++
	err := f.getTaskRowsErr
	f.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return f.TaskSweepStore.GetTaskRows(ctx, keys)
}

func (f *interceptSweepStore) FlushHeartbeats(ctx context.Context, replicaID string, replicaTTL time.Duration, entries []store.HeartbeatEntry) error {
	f.mu.Lock()
	f.flushes++
	err := f.flushErr
	hook := f.onFlush
	f.mu.Unlock()
	if hook != nil {
		hook()
	}
	if err != nil {
		return err
	}
	return f.TaskSweepStore.FlushHeartbeats(ctx, replicaID, replicaTTL, entries)
}

func (f *interceptSweepStore) UnregisterReplica(ctx context.Context, replicaID string) error {
	f.mu.Lock()
	f.unregistered++
	f.mu.Unlock()
	return f.TaskSweepStore.UnregisterReplica(ctx, replicaID)
}

func TestHeartbeatBuffer_CoalesceSkewImmunityAndFailureMergeBack(t *testing.T) {
	ctx := t.Context()

	// Simulate a 2-hour wall-clock offset between the store and the replica to
	// prove elapsed Age makes persisted timestamps immune to replica clock skew.
	storeClock := time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)
	replicaClock := storeClock.Add(2 * time.Hour)

	mem := memory.NewStore(memory.WithClock(func() time.Time { return storeClock }))
	wrap := &interceptSweepStore{TaskSweepStore: mem}

	buf := reconciler.NewHeartbeatBuffer(wrap, reconciler.HeartbeatConfig{
		ReplicaID:     "rep-skew-1",
		FlushInterval: 30 * time.Second,
		ReplicaTTL:    45 * time.Second,
		Clock:         func() time.Time { return replicaClock },
	})

	keyA := store.TaskKey{Atespace: "payments", Name: "task-a"}
	keyB := store.TaskKey{Atespace: "payments", Name: "task-b"}
	tokOld := []byte{0x01}
	tokNew := []byte{0x02, 0x03}

	for _, tc := range []struct {
		key store.TaskKey
		tok []byte
	}{
		{keyA, tokNew},
		{keyB, tokNew},
	} {
		if _, err := mem.Apply(ctx, tc.key, func(_ *store.TaskRow, now time.Time) (*store.TaskRow, error) {
			st := lifecycle.State{
				Task: &v1alpha1.Task{
					Status: &v1alpha1.TaskStatus{
						State:            v1alpha1.TaskState_TASK_STATE_WORKING,
						AteActorTemplate: "ax-tmpl",
					},
				},
				ActorUID:             "uid-" + tc.key.Name,
				RunnerTokenHash:      tc.tok,
				RunnerTokenConfirmed: true,
			}
			row := &store.TaskRow{}
			row.SetLifecycleState(st, now)
			return row, nil
		}); err != nil {
			t.Fatalf("Apply(%v): %v", tc.key, err)
		}
	}

	// Empty task or empty token hash is ignored; caller mutating slice after Record does not corrupt buffer.
	buf.Record(store.TaskKey{Atespace: "payments"}, tokNew)
	buf.Record(keyA, nil)
	if buf.PendingCount() != 0 {
		t.Fatalf("PendingCount after invalid records = %d, want 0", buf.PendingCount())
	}

	callerSlice := []byte{0x01}
	buf.Record(keyA, callerSlice)
	callerSlice[0] = 0xff

	// Advance both clocks by 3s and overwrite keyA with tokNew; record keyB at +3s as well.
	storeClock = storeClock.Add(3 * time.Second)
	replicaClock = replicaClock.Add(3 * time.Second)
	buf.Record(keyA, tokNew)
	buf.Record(keyB, tokNew)
	if buf.PendingCount() != 2 {
		t.Fatalf("PendingCount = %d, want 2", buf.PendingCount())
	}

	// Advance 5s more and inject a transient DB error on Flush; while Flush is in
	// flight, a newer heartbeat arrives for keyB.
	storeClock = storeClock.Add(5 * time.Second)
	replicaClock = replicaClock.Add(5 * time.Second)
	wrap.flushErr = errors.New("transient db timeout")
	wrap.onFlush = func() {
		replicaClock = replicaClock.Add(2 * time.Second)
		storeClock = storeClock.Add(2 * time.Second)
		buf.Record(keyB, tokNew) // newer receipt timestamp while flush is in flight
	}
	if err := buf.Flush(ctx); err == nil {
		t.Fatal("expected Flush error on injected failure")
	}
	if buf.HasFreshFlush(time.Minute) {
		t.Fatal("HasFreshFlush = true before any successful flush")
	}
	if buf.PendingCount() != 2 {
		t.Fatalf("expected un-flushed entries merged back (2 pending), got %d", buf.PendingCount())
	}

	// Clear error, advance 1s more, and flush cleanly:
	// - keyA kept its original receipt time (8s before now -> storeClock - 8s)
	// - keyB kept its newer in-flight receipt time (1s before now -> storeClock - 1s)
	wrap.mu.Lock()
	wrap.flushErr = nil
	wrap.onFlush = nil
	wrap.mu.Unlock()
	storeClock = storeClock.Add(time.Second)
	replicaClock = replicaClock.Add(time.Second)

	if err := buf.Flush(ctx); err != nil {
		t.Fatalf("retry Flush: %v", err)
	}
	if !buf.HasFreshFlush(time.Minute) || buf.PendingCount() != 0 {
		t.Fatalf("after successful Flush: HasFreshFlush=%v PendingCount=%d", buf.HasFreshFlush(time.Minute), buf.PendingCount())
	}

	rowA, _ := mem.GetTaskRow(ctx, keyA)
	rowB, _ := mem.GetTaskRow(ctx, keyB)
	wantHBA := storeClock.Add(-8 * time.Second)
	wantHBB := storeClock.Add(-1 * time.Second)
	if rowA.LastHeartbeatAt == nil || !rowA.LastHeartbeatAt.Equal(wantHBA) {
		t.Fatalf("keyA LastHeartbeatAt = %v, want %v (preserved original receipt age across failed flush)", rowA.LastHeartbeatAt, wantHBA)
	}
	if rowB.LastHeartbeatAt == nil || !rowB.LastHeartbeatAt.Equal(wantHBB) {
		t.Fatalf("keyB LastHeartbeatAt = %v, want %v (kept newer concurrent heartbeat)", rowB.LastHeartbeatAt, wantHBB)
	}
	if !buf.CanAcceptHeartbeats() {
		t.Fatal("CanAcceptHeartbeats() = false immediately after successful Flush")
	}
	// Once ReplicaTTL (45s) elapses without a successful flush, CanAcceptHeartbeats
	// returns false so `:8081` returns HTTP 503 and runners fail over.
	replicaClock = replicaClock.Add(46 * time.Second)
	storeClock = storeClock.Add(46 * time.Second)
	if buf.CanAcceptHeartbeats() {
		t.Fatal("CanAcceptHeartbeats() = true after ReplicaTTL elapsed without flush")
	}
	_ = tokOld
}

func TestHeartbeatBuffer_BogusSandboxTokenFloodPreservesValidRunnerToken(t *testing.T) {
	ctx := t.Context()
	now := time.Date(2026, 11, 16, 13, 0, 0, 0, time.UTC)
	mem := memory.NewStore(memory.WithClock(func() time.Time { return now }))

	validTok := []byte{0xde, 0xad, 0xbe, 0xef}
	key := store.TaskKey{Atespace: "payments", Name: "sandboxed-task"}
	if _, err := mem.Apply(ctx, key, func(_ *store.TaskRow, ts time.Time) (*store.TaskRow, error) {
		st := lifecycle.State{
			Task: &v1alpha1.Task{
				Status: &v1alpha1.TaskStatus{
					State:            v1alpha1.TaskState_TASK_STATE_WORKING,
					AteActorTemplate: "ax-tmpl",
				},
			},
			ActorUID:             "uid-sandbox",
			RunnerTokenHash:      validTok,
			RunnerTokenConfirmed: true,
		}
		row := &store.TaskRow{}
		row.SetLifecycleState(st, ts)
		return row, nil
	}); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	buf := reconciler.NewHeartbeatBuffer(mem, reconciler.HeartbeatConfig{
		ReplicaID:  "rep-flood",
		ReplicaTTL: 45 * time.Second,
		Clock:      func() time.Time { return now },
	})

	// 1. Untrusted uid 1000 floods 12 bogus tokens BEFORE PID 1 sends its valid
	// heartbeat, and then floods 12 MORE bogus tokens AFTER PID 1's valid heartbeat.
	for i := range 12 {
		now = now.Add(10 * time.Millisecond)
		buf.Record(key, []byte{0xaa, byte(i)})
	}
	now = now.Add(10 * time.Millisecond)
	pid1Receipt := now
	buf.Record(key, validTok)
	for i := range 12 {
		now = now.Add(10 * time.Millisecond)
		buf.Record(key, []byte{0xbb, byte(i)})
	}

	now = now.Add(time.Second)
	if err := buf.Flush(ctx); err != nil {
		t.Fatalf("Flush after bogus token flood: %v", err)
	}
	row, err := mem.GetTaskRow(ctx, key)
	if err != nil {
		t.Fatalf("GetTaskRow: %v", err)
	}
	if row.LastHeartbeatAt == nil || !row.LastHeartbeatAt.Equal(pid1Receipt) {
		t.Fatalf("LastHeartbeatAt = %v, want %v (PID 1 valid token evicted by bogus uid 1000 flood)", row.LastHeartbeatAt, pid1Receipt)
	}

	// 2. Token rotation after crash/revert: even if ConfirmToken pinned an older
	// token earlier, once the 5s pin cooldown elapses, a rotated token recorded
	// before a transient Flush failure is preserved across merge-back and flushed!
	oldPinTok := []byte{0x11, 0x11}
	rotatedTok := []byte{0x22, 0x22}
	buf.ConfirmToken(key, oldPinTok)
	now = now.Add(6 * time.Second) // past tokenVerifyCooldown (5s)

	if _, err := mem.Apply(ctx, key, func(cur *store.TaskRow, ts time.Time) (*store.TaskRow, error) {
		cp := cur.Clone()
		st := cp.LifecycleState()
		st.RunnerTokenHash = rotatedTok
		cp.SetLifecycleState(st, ts)
		return cp, nil
	}); err != nil {
		t.Fatalf("Apply(rotate token): %v", err)
	}

	wrap := &interceptSweepStore{TaskSweepStore: mem, flushErr: errors.New("transient flush error")}
	bufRot := reconciler.NewHeartbeatBuffer(wrap, reconciler.HeartbeatConfig{
		ReplicaID:  "rep-rot",
		ReplicaTTL: 45 * time.Second,
		Clock:      func() time.Time { return now },
	})
	bufRot.ConfirmToken(key, oldPinTok)
	now = now.Add(6 * time.Second) // expire oldPinTok pin freshness
	rotReceipt := now
	bufRot.Record(key, rotatedTok)

	if err := bufRot.Flush(ctx); err == nil {
		t.Fatal("expected transient Flush failure")
	}
	wrap.mu.Lock()
	wrap.flushErr = nil
	wrap.mu.Unlock()
	now = now.Add(time.Second)
	if err := bufRot.Flush(ctx); err != nil {
		t.Fatalf("retry Flush after rotation: %v", err)
	}
	rotRow, _ := mem.GetTaskRow(ctx, key)
	if rotRow.LastHeartbeatAt == nil || !rotRow.LastHeartbeatAt.Equal(rotReceipt) {
		t.Fatalf("rotated token heartbeat lost across failed-flush merge-back: got %v, want %v", rotRow.LastHeartbeatAt, rotReceipt)
	}

	// 3. Negative lookup (missing/deleted task) and transient GetTaskRows error
	// enforce the 5s per-task cooldown without unbounded DB amplification!
	wrapNeg := &interceptSweepStore{TaskSweepStore: mem}
	bufNeg := reconciler.NewHeartbeatBuffer(wrapNeg, reconciler.HeartbeatConfig{
		ReplicaID:  "rep-neg",
		ReplicaTTL: 45 * time.Second,
		Clock:      func() time.Time { return now },
	})
	missingKey := store.TaskKey{Atespace: "payments", Name: "deleted-task-jwt-cached"}
	for i := range 50 {
		now = now.Add(time.Millisecond)
		bufNeg.Record(missingKey, []byte{0xcc, byte(i)})
	}
	wrapNeg.mu.Lock()
	negLookups := wrapNeg.getTaskRowsCnt
	wrapNeg.mu.Unlock()
	if negLookups != 1 || bufNeg.PendingCount() != 0 {
		t.Fatalf("negative pin: GetTaskRows=%d PendingCount=%d, want 1 and 0", negLookups, bufNeg.PendingCount())
	}

	// Transient GetTaskRows error also enforces the 5s cooldown (1 call, not 47).
	wrapNeg.mu.Lock()
	wrapNeg.getTaskRowsErr = errors.New("db timeout")
	wrapNeg.getTaskRowsCnt = 0
	wrapNeg.mu.Unlock()
	errKey := store.TaskKey{Atespace: "payments", Name: "err-cooldown-task"}
	for i := range 50 {
		now = now.Add(time.Millisecond)
		bufNeg.Record(errKey, []byte{0xdd, byte(i)})
	}
	wrapNeg.mu.Lock()
	errLookups := wrapNeg.getTaskRowsCnt
	wrapNeg.mu.Unlock()
	if errLookups != 1 {
		t.Fatalf("error cooldown: GetTaskRows=%d across 50 flood requests, want 1", errLookups)
	}
}

func TestHeartbeatBuffer_RunLifecycleAndShutdownUnregisterGate(t *testing.T) {
	mem := memory.NewStore()
	wrap := &interceptSweepStore{TaskSweepStore: mem}

	// 1. Clean shutdown: registers on startup, flushes buffered heartbeat, and calls UnregisterReplica.
	buf := reconciler.NewHeartbeatBuffer(wrap, reconciler.HeartbeatConfig{
		ReplicaID:     "rep-clean",
		FlushInterval: 25 * time.Millisecond,
		ReplicaTTL:    45 * time.Second,
	})
	ctx1, cancel1 := context.WithCancel(t.Context())
	done1 := make(chan struct{})
	go func() {
		defer close(done1)
		_ = buf.Run(ctx1)
	}()

	waitFor(t, 2*time.Second, func() bool {
		stats, err := mem.InspectReplicas(t.Context())
		return err == nil && stats.LiveReplicas == 1
	})
	cancel1()
	<-done1

	stats, _ := mem.InspectReplicas(t.Context())
	if stats.LiveReplicas != 0 {
		t.Fatalf("expected 0 live replicas after clean shutdown, got %+v", stats)
	}

	// 2. Failed final flush on shutdown: MUST NOT call UnregisterReplica so the
	// replica lease stays until TTL expiry instead of hiding unflushed heartbeats.
	wrap2 := &interceptSweepStore{TaskSweepStore: mem}
	buf2 := reconciler.NewHeartbeatBuffer(wrap2, reconciler.HeartbeatConfig{
		ReplicaID:     "rep-failed-shut",
		FlushInterval: time.Hour,
		ReplicaTTL:    45 * time.Second,
	})
	ctx2, cancel2 := context.WithCancel(t.Context())
	done2 := make(chan struct{})
	go func() {
		defer close(done2)
		_ = buf2.Run(ctx2)
	}()

	waitFor(t, 2*time.Second, func() bool {
		s, err := mem.InspectReplicas(t.Context())
		return err == nil && s.LiveReplicas == 1
	})
	wrap2.mu.Lock()
	wrap2.flushErr = errors.New("db down during shutdown")
	wrap2.mu.Unlock()

	cancel2()
	<-done2

	wrap2.mu.Lock()
	unregCalls := wrap2.unregistered
	wrap2.mu.Unlock()
	if unregCalls != 0 {
		t.Fatalf("UnregisterReplica called %d times after failed shutdown flush, want 0", unregCalls)
	}
	if s, _ := mem.InspectReplicas(t.Context()); s.LiveReplicas != 1 {
		t.Fatalf("expected replica lease retained until TTL expiry on failed shutdown flush, got %+v", s)
	}
}
