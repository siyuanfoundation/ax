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
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/ax/internal/lifecycle"
	"github.com/google/ax/internal/reconciler"
	"github.com/google/ax/internal/store"
	"github.com/google/ax/internal/store/memory"
	"github.com/google/ax/internal/substrate/substratetest"
	"github.com/google/ax/pkg/apis/v1alpha1"
	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type sweepNoIOGuard struct {
	store.TaskSweepStore
	reconciler.RuntimeClient
	t       *testing.T
	inApply atomic.Int32
	applies atomic.Int32
}

func (g *sweepNoIOGuard) Apply(ctx context.Context, key store.TaskKey, fn store.ApplyFunc) (*store.TaskRow, error) {
	return g.TaskSweepStore.Apply(ctx, key, func(cur *store.TaskRow, now time.Time) (*store.TaskRow, error) {
		g.inApply.Add(1)
		defer g.inApply.Add(-1)
		next, err := fn(cur, now)
		if err == nil && next != nil && next != cur {
			g.applies.Add(1)
		}
		return next, err
	})
}

func (g *sweepNoIOGuard) checkNoTx(op string) {
	if g.inApply.Load() > 0 {
		g.t.Errorf("Substrate call %s executed inside store.Apply transaction", op)
	}
}

func (g *sweepNoIOGuard) ListActorsPage(ctx context.Context, ns string, pageSize int32, pageToken string) ([]*ateapipb.Actor, string, error) {
	g.checkNoTx("ListActorsPage")
	return g.RuntimeClient.ListActorsPage(ctx, ns, pageSize, pageToken)
}

func (g *sweepNoIOGuard) GetActor(ctx context.Context, ns, actor string) (*ateapipb.Actor, error) {
	g.checkNoTx("GetActor")
	return g.RuntimeClient.GetActor(ctx, ns, actor)
}

type countingSweepRuntime struct {
	reconciler.RuntimeClient
	mu             sync.Mutex
	getActorCalls  int
	listPageCalls  int
	globalListReqs int
	onListPage     func(ns string, pageSize int32, pageToken string) ([]*ateapipb.Actor, string, error, bool)
	onGetActor     func(ns, name string) (*ateapipb.Actor, error, bool)
}

func (r *countingSweepRuntime) ListActorsPage(ctx context.Context, ns string, pageSize int32, pageToken string) ([]*ateapipb.Actor, string, error) {
	r.mu.Lock()
	r.listPageCalls++
	if ns == "" {
		r.globalListReqs++
	}
	hook := r.onListPage
	r.mu.Unlock()
	if hook != nil {
		if actors, next, err, handled := hook(ns, pageSize, pageToken); handled {
			return actors, next, err
		}
	}
	return r.RuntimeClient.ListActorsPage(ctx, ns, pageSize, pageToken)
}

func (r *countingSweepRuntime) GetActor(ctx context.Context, ns, name string) (*ateapipb.Actor, error) {
	r.mu.Lock()
	r.getActorCalls++
	hook := r.onGetActor
	r.mu.Unlock()
	if hook != nil {
		if actor, err, handled := hook(ns, name); handled {
			return actor, err
		}
	}
	return r.RuntimeClient.GetActor(ctx, ns, name)
}

func seedSweepTask(
	t *testing.T,
	s store.TaskApplier,
	ns, name string,
	state v1alpha1.TaskState,
	actorUID string,
	tokenHash []byte,
	actions []lifecycle.Action,
	updateTime *time.Time,
) store.TaskKey {
	t.Helper()
	key := store.TaskKey{Atespace: ns, Name: name}
	if _, err := s.Apply(t.Context(), key, func(_ *store.TaskRow, now time.Time) (*store.TaskRow, error) {
		resumed := now
		st := lifecycle.State{
			Task: &v1alpha1.Task{
				Metadata: &v1alpha1.ObjectMeta{
					CreateTime: timestamppb.New(now),
				},
				Status: &v1alpha1.TaskStatus{
					State:            state,
					AteActorTemplate: "ax-tmpl",
					Attempt:          1,
				},
			},
			ActorUID:             actorUID,
			RunnerTokenHash:      tokenHash,
			RunnerTokenConfirmed: len(tokenHash) > 0,
			LastResumedAt:        &resumed,
			Actions:              actions,
		}
		if updateTime != nil {
			st.Task.Metadata.UpdateTime = timestamppb.New(*updateTime)
		}
		row := &store.TaskRow{}
		row.SetLifecycleState(st, now)
		return row, nil
	}); err != nil {
		t.Fatalf("seedSweepTask(%s/%s): %v", ns, name, err)
	}
	return key
}

func TestSweeper_DriftComparisonTableAndSteadyStateZeroWrites(t *testing.T) {
	ctx := t.Context()
	now := time.Date(2026, 11, 16, 14, 0, 0, 0, time.UTC)
	mem := memory.NewStore(memory.WithClock(func() time.Time { return now }))
	sub := substratetest.NewServer(t)
	baseRT := reconciler.NewSubstrateRuntime(sub.Client(), reconciler.SubstrateRuntimeOptions{})
	guard := &sweepNoIOGuard{TaskSweepStore: mem, RuntimeClient: baseRT, t: t}

	if err := mem.FlushHeartbeats(ctx, "rep-1", 45*time.Second, nil); err != nil {
		t.Fatalf("FlushHeartbeats: %v", err)
	}

	// Seed every row of §6.5.2's comparison table:
	// 1. Healthy WORKING + RUNNING -> zero writes.
	kHealthy := seedSweepTask(t, mem, "payments", "healthy-work", v1alpha1.TaskState_TASK_STATE_WORKING, "uid-healthy", []byte{1}, nil, nil)
	sub.SetActor(&ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "payments", Name: "healthy-work", Uid: "uid-healthy"},
		Status:   &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_RUNNING},
	})

	// 2. WORKING + CRASHED -> QUEUED with REVERT_ACTOR + RESUME_ACTOR.
	kCrashed := seedSweepTask(t, mem, "payments", "crashed-work", v1alpha1.TaskState_TASK_STATE_WORKING, "uid-crashed", []byte{2}, nil, nil)
	sub.SetActor(&ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "payments", Name: "crashed-work", Uid: "uid-crashed"},
		Status: &ateapipb.ActorStatus{
			State: ateapipb.ActorState_ACTOR_STATE_CRASHED,
			Crash: &ateapipb.ActorCrash{Message: "node OOM"},
		},
	})

	// 3. Task with pending actions is owned by its worker -> skipped even if CRASHED.
	pendingAct := lifecycle.Action{ID: "a-1", Kind: lifecycle.ActionSuspendActorSync, Since: now}
	kPending := seedSweepTask(t, mem, "payments", "worker-owned", v1alpha1.TaskState_TASK_STATE_SUSPENDING, "uid-pending", nil, []lifecycle.Action{pendingAct}, nil)
	sub.SetActor(&ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "payments", Name: "worker-owned", Uid: "uid-pending"},
		Status:   &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_CRASHED},
	})

	// 4. SUSPENDED + RUNNING (HTTP ingress wakeup) -> QUEUED + ClaimDeadline.
	kWokenSusp := seedSweepTask(t, mem, "payments", "woken-suspended", v1alpha1.TaskState_TASK_STATE_SUSPENDED, "uid-ws", nil, nil, nil)
	sub.SetActor(&ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "payments", Name: "woken-suspended", Uid: "uid-ws"},
		Status:   &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_RUNNING},
	})

	// 5. IDLE + RUNNING -> SUSPENDING + SUSPEND_ACTOR_SYNC.
	kWokenIdle := seedSweepTask(t, mem, "payments", "woken-idle", v1alpha1.TaskState_TASK_STATE_IDLE, "uid-wi", nil, nil, nil)
	sub.SetActor(&ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "payments", Name: "woken-idle", Uid: "uid-wi"},
		Status:   &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_RUNNING},
	})

	// 6. WORKING + SUSPENDED and QUEUED + SUSPENDED -> SUSPENDED.
	kExtSusp := seedSweepTask(t, mem, "payments", "ext-suspended", v1alpha1.TaskState_TASK_STATE_WORKING, "uid-es", nil, nil, nil)
	sub.SetActor(&ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "payments", Name: "ext-suspended", Uid: "uid-es"},
		Status:   &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
	})
	kQueuedExtSusp := seedSweepTask(t, mem, "payments", "queued-ext-suspended", v1alpha1.TaskState_TASK_STATE_QUEUED, "uid-qes", []byte{9}, nil, nil)
	sub.SetActor(&ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "payments", Name: "queued-ext-suspended", Uid: "uid-qes"},
		Status:   &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
	})

	// 7. Terminal COMPLETED + RUNNING -> re-parked via SUSPEND_ACTOR_SYNC.
	doneAt := now.Add(-time.Minute)
	kWokenDone := seedSweepTask(t, mem, "payments", "woken-completed", v1alpha1.TaskState_TASK_STATE_COMPLETED, "uid-done", nil, nil, &doneAt)
	sub.SetActor(&ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "payments", Name: "woken-completed", Uid: "uid-done"},
		Status:   &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_RUNNING},
	})

	sweeper := reconciler.NewSweeper(guard, guard, reconciler.SweepConfig{
		ReplicaID:     "rep-1",
		Transition:    stubTransition,
		NumShards:     2,
		ListPageSize:  2, // exercise multi-page ListActorsPage across 7 actors
		MonitoringQPS: -1,
	})

	if err := sweeper.SweepOnce(ctx); err != nil {
		t.Fatalf("SweepOnce(drift): %v", err)
	}

	// Verify each row's outcome.
	if r, _ := mem.GetTaskRow(ctx, kHealthy); r.Generation != 1 {
		t.Fatalf("healthy task was unexpectedly mutated (gen=%d)", r.Generation)
	}
	if r, _ := mem.GetTaskRow(ctx, kPending); r.Generation != 1 || len(r.Actions) != 1 || r.Actions[0].ID != "a-1" {
		t.Fatalf("worker-owned task with pending action was mutated: %+v", r.State)
	}
	if r, _ := mem.GetTaskRow(ctx, kCrashed); r.Task.GetStatus().GetState() != v1alpha1.TaskState_TASK_STATE_QUEUED ||
		len(r.Actions) != 2 || r.Actions[0].Kind != lifecycle.ActionRevertActor || r.Actions[1].Kind != lifecycle.ActionResumeActor {
		t.Fatalf("crashed task did not enqueue REVERT+RESUME: %+v", r.State)
	}
	if r, _ := mem.GetTaskRow(ctx, kWokenSusp); r.Task.GetStatus().GetState() != v1alpha1.TaskState_TASK_STATE_QUEUED || r.ClaimDeadline == nil {
		t.Fatalf("woken SUSPENDED task did not return to QUEUED with ClaimDeadline: %+v", r.State)
	}
	if r, _ := mem.GetTaskRow(ctx, kWokenIdle); r.Task.GetStatus().GetState() != v1alpha1.TaskState_TASK_STATE_SUSPENDING ||
		len(r.Actions) != 1 || r.Actions[0].Kind != lifecycle.ActionSuspendActorSync {
		t.Fatalf("woken IDLE task did not enqueue re-suspend: %+v", r.State)
	}
	if r, _ := mem.GetTaskRow(ctx, kExtSusp); r.Task.GetStatus().GetState() != v1alpha1.TaskState_TASK_STATE_SUSPENDED {
		t.Fatalf("externally suspended WORKING task state = %v, want SUSPENDED", r.Task.GetStatus().GetState())
	}
	if r, _ := mem.GetTaskRow(ctx, kQueuedExtSusp); r.Task.GetStatus().GetState() != v1alpha1.TaskState_TASK_STATE_SUSPENDED {
		t.Fatalf("externally suspended QUEUED task state = %v, want SUSPENDED", r.Task.GetStatus().GetState())
	}
	if r, _ := mem.GetTaskRow(ctx, kWokenDone); len(r.Actions) != 1 || r.Actions[0].Kind != lifecycle.ActionSuspendActorSync {
		t.Fatalf("woken COMPLETED task did not enqueue terminal re-suspend: %+v", r.State)
	}

	// Align Substrate with the updated steady state and verify the next pass performs ZERO writes.
	sub.SetActor(&ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "payments", Name: "woken-completed", Uid: "uid-done"},
		Status:   &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
	})
	guard.applies.Store(0)
	if err := sweeper.SweepOnce(ctx); err != nil {
		t.Fatalf("SweepOnce(steady state): %v", err)
	}
	if got := guard.applies.Load(); got != 0 {
		t.Fatalf("steady-state SweepOnce performed %d Apply writes, want 0", got)
	}
}

func TestSweeper_TwoPassMissingConfirmationForeignUIDAndErrorRetention(t *testing.T) {
	ctx := t.Context()
	now := time.Date(2026, 11, 16, 15, 0, 0, 0, time.UTC)
	mem := memory.NewStore(memory.WithClock(func() time.Time { return now }))
	sub := substratetest.NewServer(t)
	rt := &countingSweepRuntime{
		RuntimeClient: reconciler.NewSubstrateRuntime(sub.Client(), reconciler.SubstrateRuntimeOptions{}),
	}
	_ = mem.FlushHeartbeats(ctx, "rep-1", 45*time.Second, nil)

	// Task 1: completely absent on Substrate -> fails only after 2 consecutive passes + GetActor(NotFound).
	kGone := seedSweepTask(t, mem, "payments", "gone-task", v1alpha1.TaskState_TASK_STATE_WORKING, "uid-gone", []byte{1}, nil, nil)

	// Task 2: omitted from ListActors twice, but live GetActor finds it -> recovers without failing!
	kOmitted := seedSweepTask(t, mem, "payments", "omitted-from-list", v1alpha1.TaskState_TASK_STATE_WORKING, "uid-omitted", []byte{2}, nil, nil)
	sub.SetActor(&ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "payments", Name: "omitted-from-list", Uid: "uid-omitted"},
		Status:   &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_RUNNING},
	})

	// Task 4: recreated under the same name with a NEW ActorUID between Pass 1 and Pass 2 ->
	// missingOnce keyed by (TaskKey, ActorUID) must NOT fail the new incarnation on Pass 2!
	kRecreated := seedSweepTask(t, mem, "payments", "recreated-same-name", v1alpha1.TaskState_TASK_STATE_WORKING, "uid-incarnation-1", []byte{4}, nil, nil)
	kForeign := seedSweepTask(t, mem, "payments", "foreign-collision", v1alpha1.TaskState_TASK_STATE_WORKING, "uid-expected", []byte{3}, nil, nil)
	sub.SetActor(&ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "payments", Name: "foreign-collision", Uid: "uid-someone-else"},
		Status:   &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_RUNNING},
	})

	rt.onListPage = func(ns string, pageSize int32, pageToken string) ([]*ateapipb.Actor, string, error, bool) {
		actors, next, err := rt.RuntimeClient.ListActorsPage(ctx, ns, pageSize, pageToken)
		if err != nil {
			return nil, "", err, true
		}
		filtered := actors[:0]
		for _, a := range actors {
			if a.GetMetadata().GetName() != "omitted-from-list" {
				filtered = append(filtered, a)
			}
		}
		return filtered, next, nil, true
	}

	sweeper := reconciler.NewSweeper(mem, rt, reconciler.SweepConfig{
		ReplicaID:     "rep-1",
		Transition:    stubTransition,
		NumShards:     1,
		MonitoringQPS: -1,
	})

	// Pass 1: records missingOnce for all 3; ZERO GetActor calls and ZERO failures.
	if err := sweeper.SweepOnce(ctx); err != nil {
		t.Fatalf("Pass 1 SweepOnce: %v", err)
	}
	if rt.getActorCalls != 0 {
		t.Fatalf("Pass 1 GetActor calls = %d, want 0", rt.getActorCalls)
	}
	for _, k := range []store.TaskKey{kGone, kOmitted, kForeign} {
		if r, _ := mem.GetTaskRow(ctx, k); r.Task.GetStatus().GetState() != v1alpha1.TaskState_TASK_STATE_WORKING {
			t.Fatalf("task %v failed prematurely after 1 missing pass: %v", k, r.Task.GetStatus().GetState())
		}
	}

	// Transient ListActorsPage failure between pass 1 and pass 2 preserves missingOnce without calling GetActor.
	sub.SetErrors(substratetest.Errors{ListActors: status.Error(codes.Unavailable, "ateapi restarting")})
	if err := sweeper.SweepOnce(ctx); err == nil {
		t.Fatal("expected error from transient ListActors failure")
	}
	sub.SetErrors(substratetest.Errors{})
	if rt.getActorCalls != 0 {
		t.Fatalf("GetActor called during incomplete ListActors pass: %d", rt.getActorCalls)
	}

	// Pass 2: before running Pass 2, recreate kRecreated with uid-incarnation-2;
	// Pass 2 confirms via GetActor ONLY for kGone, kOmitted, and kForeign (3 calls),
	// while kRecreated starts fresh at missingOnce = 1!
	_, _ = mem.Apply(ctx, kRecreated, func(*store.TaskRow, time.Time) (*store.TaskRow, error) {
		return store.DeleteTaskRow, nil
	})
	seedSweepTask(t, mem, "payments", "recreated-same-name", v1alpha1.TaskState_TASK_STATE_WORKING, "uid-incarnation-2", []byte{5}, nil, nil)

	if err := sweeper.SweepOnce(ctx); err != nil {
		t.Fatalf("Pass 2 SweepOnce: %v", err)
	}
	if rt.getActorCalls != 3 {
		t.Fatalf("Pass 2 GetActor calls = %d, want 3 (recreated task must not trigger GetActor on its first pass)", rt.getActorCalls)
	}
	if r, _ := mem.GetTaskRow(ctx, kRecreated); r.Task.GetStatus().GetState() != v1alpha1.TaskState_TASK_STATE_WORKING {
		t.Fatalf("recreated task with new ActorUID failed prematurely on its first missing pass: %v", r.Task.GetStatus().GetState())
	}
	for _, k := range []store.TaskKey{kGone, kForeign} {
		r, _ := mem.GetTaskRow(ctx, k)
		if r.Task.GetStatus().GetState() != v1alpha1.TaskState_TASK_STATE_FAILED ||
			r.Task.GetStatus().GetStatusDetail().GetReason() != v1alpha1.TaskStatusReason_TASK_STATUS_REASON_SANDBOX_CRASHED {
			t.Fatalf("task %v after 2 missing passes = %+v, want FAILED/SANDBOX_CRASHED", k, r.Task.GetStatus())
		}
	}
	if r, _ := mem.GetTaskRow(ctx, kOmitted); r.Task.GetStatus().GetState() != v1alpha1.TaskState_TASK_STATE_WORKING {
		t.Fatalf("omitted-from-list task rescued by GetActor should stay WORKING, got %v", r.Task.GetStatus().GetState())
	}
	if foreignActor, ok := sub.Actor("payments", "foreign-collision"); !ok || foreignActor.GetMetadata().GetUid() != "uid-someone-else" ||
		foreignActor.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_RUNNING {
		t.Fatalf("foreign actor on Substrate was modified: %+v, ok=%v", foreignActor, ok)
	}
}

func TestSweeper_GenerationFencingAndTimeTravelWokenTerminalGuard(t *testing.T) {
	ctx := t.Context()
	now := time.Date(2026, 11, 16, 16, 0, 0, 0, time.UTC)
	var clockMu sync.Mutex
	readClock := func() time.Time {
		clockMu.Lock()
		defer clockMu.Unlock()
		return now
	}
	advanceClock := func(d time.Duration) time.Time {
		clockMu.Lock()
		defer clockMu.Unlock()
		now = now.Add(d)
		return now
	}

	mem := memory.NewStore(memory.WithClock(readClock))
	sub := substratetest.NewServer(t)
	rt := &countingSweepRuntime{
		RuntimeClient: reconciler.NewSubstrateRuntime(sub.Client(), reconciler.SubstrateRuntimeOptions{}),
	}
	_ = mem.FlushHeartbeats(ctx, "rep-1", 45*time.Second, nil)

	// 1. Active SUSPENDED task that transitions concurrently while ListActorsPage is in flight:
	// pre-ListActors snapshot sees Generation=1 (SUSPENDED), ListActors sees RUNNING,
	// concurrent Apply advances row to Generation=2 (WORKING) before sweep's Apply runs.
	kRace := seedSweepTask(t, mem, "payments", "gen-race", v1alpha1.TaskState_TASK_STATE_SUSPENDED, "uid-race", []byte{1}, nil, nil)
	sub.SetActor(&ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "payments", Name: "gen-race", Uid: "uid-race"},
		Status:   &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_RUNNING},
	})

	// 2. Newly created task that completes and parks AFTER preListStoreNow while ListActorsPage
	// already sampled its earlier RUNNING state -> must NOT be falsely re-parked!
	// 3. Pre-existing COMPLETED task (CreateTime/UpdateTime <= preListStoreNow) whose
	// in-flight SUSPEND_ACTOR_SYNC finishes while ListActorsPage was paging (so ListActorsPage
	// saw RUNNING, GetTaskRows sees Actions == [], and live GetActor sees SUSPENDED) ->
	// must NOT enqueue a duplicate SUSPEND_ACTOR_SYNC!
	older := now.Add(-10 * time.Second)
	kInFlightPark := seedSweepTask(t, mem, "payments", "inflight-park", v1alpha1.TaskState_TASK_STATE_COMPLETED, "uid-park", nil,
		[]lifecycle.Action{{ID: "park-1", Kind: lifecycle.ActionSuspendActorSync, Since: older}}, &older)
	sub.SetActor(&ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "payments", Name: "inflight-park", Uid: "uid-park"},
		Status:   &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_RUNNING},
	})

	rt.onListPage = func(ns string, pageSize int32, pageToken string) ([]*ateapipb.Actor, string, error, bool) {
		actors, next, err := rt.RuntimeClient.ListActorsPage(ctx, ns, pageSize, pageToken)
		if err != nil {
			return nil, "", err, true
		}
		// Advance clock after SnapshotActiveTasks captured preListStoreNow.
		later := advanceClock(2 * time.Second)
		_, _ = mem.Apply(ctx, kRace, func(cur *store.TaskRow, ts time.Time) (*store.TaskRow, error) {
			cp := cur.Clone()
			st := cp.LifecycleState()
			st.Task.Status.State = v1alpha1.TaskState_TASK_STATE_WORKING
			cp.SetLifecycleState(st, ts)
			return cp, nil
		})
		// Simulate worker completing kInFlightPark's SUSPEND_ACTOR_SYNC mid-list
		// without changing Task.Metadata.UpdateTime!
		sub.SetActor(&ateapipb.Actor{
			Metadata: &ateapipb.ResourceMetadata{Atespace: "payments", Name: "inflight-park", Uid: "uid-park"},
			Status:   &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
		})
		_, _ = mem.Apply(ctx, kInFlightPark, func(cur *store.TaskRow, ts time.Time) (*store.TaskRow, error) {
			cp := cur.Clone()
			st := cp.LifecycleState()
			st.Actions = nil
			cp.SetLifecycleState(st, ts)
			return cp, nil
		})
		// Inject a task that completed at `later > preListStoreNow` while an earlier page saw RUNNING.
		seedSweepTask(t, mem, "payments", "finished-mid-list", v1alpha1.TaskState_TASK_STATE_COMPLETED, "uid-mid", nil, nil, &later)
		actors = append(actors, &ateapipb.Actor{
			Metadata: &ateapipb.ResourceMetadata{Atespace: "payments", Name: "finished-mid-list", Uid: "uid-mid"},
			Status:   &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_RUNNING},
		})
		return actors, next, nil, true
	}

	sweeper := reconciler.NewSweeper(mem, rt, reconciler.SweepConfig{
		ReplicaID:     "rep-1",
		Transition:    stubTransition,
		NumShards:     1,
		MonitoringQPS: -1,
	})
	if err := sweeper.SweepOnce(ctx); err != nil {
		t.Fatalf("SweepOnce: %v", err)
	}

	rRace, _ := mem.GetTaskRow(ctx, kRace)
	if rRace.Generation != 2 || rRace.Task.GetStatus().GetState() != v1alpha1.TaskState_TASK_STATE_WORKING {
		t.Fatalf("generation fence failed: gen=%d state=%v, want gen=2 WORKING", rRace.Generation, rRace.Task.GetStatus().GetState())
	}
	rPark, _ := mem.GetTaskRow(ctx, kInFlightPark)
	if len(rPark.Actions) != 0 {
		t.Fatalf("completed task whose SUSPEND_ACTOR_SYNC finished mid-list was spuriously re-parked: actions=%+v", rPark.Actions)
	}
	rMid, _ := mem.GetTaskRow(ctx, store.TaskKey{Atespace: "payments", Name: "finished-mid-list"})
	if len(rMid.Actions) != 0 {
		t.Fatalf("task completed after preListStoreNow was falsely re-parked: actions=%+v", rMid.Actions)
	}
}

func TestSweeper_WatermarkGatedLostRunnerAndConcurrentFlushUnderLock(t *testing.T) {
	ctx := t.Context()
	now := time.Date(2026, 11, 16, 17, 0, 0, 0, time.UTC)
	var clockMu sync.Mutex
	readClock := func() time.Time {
		clockMu.Lock()
		defer clockMu.Unlock()
		return now
	}
	setClock := func(ts time.Time) {
		clockMu.Lock()
		now = ts
		clockMu.Unlock()
	}

	mem := memory.NewStore(memory.WithClock(readClock))
	sub := substratetest.NewServer(t)
	rt := &countingSweepRuntime{
		RuntimeClient: reconciler.NewSubstrateRuntime(sub.Client(), reconciler.SubstrateRuntimeOptions{}),
	}

	tokSilent := []byte{0xaa}
	tokSaved := []byte{0xbb}
	tokClaimedQueued := []byte{0xcc}
	kSilent := seedSweepTask(t, mem, "payments", "silent-runner", v1alpha1.TaskState_TASK_STATE_WORKING, "uid-silent", tokSilent, nil, nil)
	kSaved := seedSweepTask(t, mem, "payments", "concurrent-hb-runner", v1alpha1.TaskState_TASK_STATE_WORKING, "uid-saved", tokSaved, nil, nil)
	kClaimedQueued := seedSweepTask(t, mem, "payments", "claimed-queued", v1alpha1.TaskState_TASK_STATE_QUEUED, "uid-cq", tokClaimedQueued, nil, nil)
	// Unclaimed QUEUED task with an active 5-minute ClaimDeadline MUST NOT be
	// preempted at 150s by HeartbeatLostInput!
	kUnclaimedQueued := seedSweepTask(t, mem, "payments", "unclaimed-queued", v1alpha1.TaskState_TASK_STATE_QUEUED, "uid-uq", []byte{0xdd}, nil, nil)
	_, _ = mem.Apply(ctx, kUnclaimedQueued, func(cur *store.TaskRow, ts time.Time) (*store.TaskRow, error) {
		cp := cur.Clone()
		st := cp.LifecycleState()
		dl := ts.Add(lifecycle.ClaimWatchdogTimeout)
		st.ClaimDeadline = &dl
		cp.SetLifecycleState(st, ts)
		return cp, nil
	})
	for name, uid := range map[string]string{
		"silent-runner":        "uid-silent",
		"concurrent-hb-runner": "uid-saved",
		"claimed-queued":       "uid-cq",
		"unclaimed-queued":     "uid-uq",
	} {
		sub.SetActor(&ateapipb.Actor{
			Metadata: &ateapipb.ResourceMetadata{Atespace: "payments", Name: name, Uid: uid},
			Status:   &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_RUNNING},
		})
	}

	hb := reconciler.NewHeartbeatBuffer(mem, reconciler.HeartbeatConfig{
		ReplicaID:  "rep-1",
		ReplicaTTL: 90 * time.Second,
		Clock:      readClock,
	})
	if err := hb.Flush(ctx); err != nil {
		t.Fatalf("initial Flush: %v", err)
	}

	sweeper := reconciler.NewSweeper(mem, rt, reconciler.SweepConfig{
		ReplicaID:            "rep-1",
		HeartbeatBuffer:      hb,
		Transition:           stubTransition,
		NumShards:            1,
		ShardLeaseTTL:        5 * time.Minute,
		RunnerSilenceTimeout: 150 * time.Second,
		MaxWatermarkAge:      60 * time.Second,
		MonitoringQPS:        -1,
	})
	// Pass 0 acquires shard lease at t=0.
	if err := sweeper.SweepOnce(ctx); err != nil {
		t.Fatalf("Pass 0: %v", err)
	}

	// Register a second replica rep-stalled at t=100s with 90s TTL, then advance to t=170s:
	// - rep-1 flushes at t=170s (fresh)
	// - rep-stalled is still live (expires at 190s), with last_flush_at = 100s -> WatermarkAge = 70s > 60s!
	// Lost-runner detection MUST pause even though kSilent has been silent for 170s (> 150s)!
	setClock(now.Add(100 * time.Second))
	if err := mem.FlushHeartbeats(ctx, "rep-stalled", 90*time.Second, nil); err != nil {
		t.Fatalf("FlushHeartbeats(rep-stalled): %v", err)
	}
	setClock(readClock().Add(70 * time.Second)) // t = 170s
	if err := hb.Flush(ctx); err != nil {
		t.Fatalf("hb.Flush at t=170s: %v", err)
	}
	if err := sweeper.SweepOnce(ctx); err != nil {
		t.Fatalf("SweepOnce(stalled watermark): %v", err)
	}
	if r, _ := mem.GetTaskRow(ctx, kSilent); r.Task.GetStatus().GetState() != v1alpha1.TaskState_TASK_STATE_WORKING {
		t.Fatalf("lost-runner check fired while WatermarkAge=70s > 60s: state=%v", r.Task.GetStatus().GetState())
	}

	// Now rep-stalled recovers and flushes at t=175s (WatermarkAge = 5s <= 60s).
	// Simultaneously, while ListActorsPage is in flight, kSaved flushes a fresh heartbeat
	// (HOT update: LastHeartbeatAt advances WITHOUT bumping Generation!).
	// Under-lock re-verification inside store.Apply must save kSaved while failing kSilent!
	setClock(readClock().Add(5 * time.Second)) // t = 175s
	_ = mem.FlushHeartbeats(ctx, "rep-stalled", 90*time.Second, nil)
	_ = hb.Flush(ctx)

	rt.onListPage = func(ns string, pageSize int32, pageToken string) ([]*ateapipb.Actor, string, error, bool) {
		actors, next, err := rt.RuntimeClient.ListActorsPage(ctx, ns, pageSize, pageToken)
		// Flush a heartbeat for kSaved AFTER SnapshotActiveTasks ran!
		_ = mem.FlushHeartbeats(ctx, "rep-1", 90*time.Second, []store.HeartbeatEntry{
			{Key: kSaved, RunnerTokenHash: tokSaved, Age: time.Second},
		})
		return actors, next, err, true
	}

	if err := sweeper.SweepOnce(ctx); err != nil {
		t.Fatalf("SweepOnce(healthy watermark): %v", err)
	}

	rSilent, _ := mem.GetTaskRow(ctx, kSilent)
	if rSilent.Task.GetStatus().GetState() != v1alpha1.TaskState_TASK_STATE_FAILED ||
		rSilent.Task.GetStatus().GetStatusDetail().GetReason() != v1alpha1.TaskStatusReason_TASK_STATUS_REASON_SANDBOX_CRASHED ||
		len(rSilent.Actions) != 1 || rSilent.Actions[0].Kind != lifecycle.ActionSuspendActorSync {
		t.Fatalf("silent runner was not failed + parked: %+v", rSilent.State)
	}
	rSaved, _ := mem.GetTaskRow(ctx, kSaved)
	if rSaved.Task.GetStatus().GetState() != v1alpha1.TaskState_TASK_STATE_WORKING || len(rSaved.Actions) != 0 {
		t.Fatalf("runner with concurrent heartbeat flush was falsely marked lost: %+v", rSaved.State)
	}
	rClaimedQ, _ := mem.GetTaskRow(ctx, kClaimedQueued)
	if rClaimedQ.Task.GetStatus().GetState() != v1alpha1.TaskState_TASK_STATE_FAILED {
		t.Fatalf("claimed QUEUED task silent >150s was not marked lost: %+v", rClaimedQ.State)
	}
	rUnclaimedQ, _ := mem.GetTaskRow(ctx, kUnclaimedQueued)
	if rUnclaimedQ.Task.GetStatus().GetState() != v1alpha1.TaskState_TASK_STATE_QUEUED || len(rUnclaimedQ.Actions) != 0 {
		t.Fatalf("unclaimed QUEUED task with active ClaimDeadline was falsely preempted at 175s (< 5m): %+v", rUnclaimedQ.State)
	}

	// Verify that a parked IDLE task woken via atenet-router directly into WORKING
	// at t=175s (when LastResumedAt and shard.acquiredAt are 175s old!) AND a brand-new
	// doorbell-claimed task created at t=175s with LastResumedAt == nil BOTH survive
	// SweepOnce without false-positive HeartbeatLostInput!
	kRouterTurn2 := seedSweepTask(t, mem, "payments", "router-turn2", v1alpha1.TaskState_TASK_STATE_IDLE, "uid-rt2", []byte{0xee}, nil, nil)
	_, _ = mem.Apply(ctx, kRouterTurn2, func(cur *store.TaskRow, ts time.Time) (*store.TaskRow, error) {
		cp := cur.Clone()
		old := ts.Add(-175 * time.Second)
		cp.LastResumedAt = &old
		cp.LastHeartbeatAt = &old
		return cp, nil
	})
	// Now ClaimAssignment transitions IDLE -> WORKING (with ClaimDeadline == nil).
	_, _ = mem.Apply(ctx, kRouterTurn2, func(cur *store.TaskRow, ts time.Time) (*store.TaskRow, error) {
		cp := cur.Clone()
		st := cp.LifecycleState()
		st.Task.Status.State = v1alpha1.TaskState_TASK_STATE_WORKING
		cp.SetLifecycleState(st, ts)
		return cp, nil
	})
	sub.SetActor(&ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "payments", Name: "router-turn2", Uid: "uid-rt2"},
		Status:   &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_RUNNING},
	})

	kDoorbell := store.TaskKey{Atespace: "payments", Name: "doorbell-claim"}
	_, _ = mem.Apply(ctx, kDoorbell, func(_ *store.TaskRow, ts time.Time) (*store.TaskRow, error) {
		st := lifecycle.State{
			Task: &v1alpha1.Task{
				Metadata: &v1alpha1.ObjectMeta{CreateTime: timestamppb.New(ts)},
				Status: &v1alpha1.TaskStatus{
					State:            v1alpha1.TaskState_TASK_STATE_QUEUED,
					AteActorTemplate: "ax-tmpl",
				},
			},
			ActorUID:             "uid-db",
			RunnerTokenHash:      []byte{0xef},
			RunnerTokenConfirmed: false,
		}
		st.Actions = []lifecycle.Action{st.NextAction(lifecycle.ActionResumeActor, ts)}
		row := &store.TaskRow{}
		row.SetLifecycleState(st, ts)
		return row, nil
	})
	// /readyz doorbell ClaimAssignment arrives before ActionCompleted(RESUME_ACTOR),
	// clearing Actions while LastResumedAt is still nil on a 175s-old shard!
	_, _ = mem.Apply(ctx, kDoorbell, func(cur *store.TaskRow, ts time.Time) (*store.TaskRow, error) {
		cp := cur.Clone()
		st := cp.LifecycleState()
		st.Actions = nil
		st.RunnerTokenConfirmed = true
		st.Task.Status.State = v1alpha1.TaskState_TASK_STATE_WORKING
		cp.SetLifecycleState(st, ts)
		return cp, nil
	})
	sub.SetActor(&ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "payments", Name: "doorbell-claim", Uid: "uid-db"},
		Status:   &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_RUNNING},
	})

	rt.onListPage = nil
	setClock(readClock().Add(5 * time.Second)) // t = 180s (before next 30s heartbeat flush!)
	_ = hb.Flush(ctx)
	_ = mem.FlushHeartbeats(ctx, "rep-stalled", 90*time.Second, nil)
	if err := sweeper.SweepOnce(ctx); err != nil {
		t.Fatalf("SweepOnce(router & doorbell): %v", err)
	}
	for _, k := range []store.TaskKey{kRouterTurn2, kDoorbell} {
		r, _ := mem.GetTaskRow(ctx, k)
		if r.Task.GetStatus().GetState() != v1alpha1.TaskState_TASK_STATE_WORKING {
			t.Fatalf("task %v falsely marked lost right after claim: %+v", k, r.State)
		}
	}
}

func TestSweeper_MidPassShardLeaseTheftAbortsAndDropsShard(t *testing.T) {
	ctx := t.Context()
	now := time.Date(2026, 11, 16, 18, 0, 0, 0, time.UTC)
	var clockMu sync.Mutex
	readClock := func() time.Time {
		clockMu.Lock()
		defer clockMu.Unlock()
		return now
	}
	advanceClock := func(d time.Duration) {
		clockMu.Lock()
		now = now.Add(d)
		clockMu.Unlock()
	}

	mem := memory.NewStore(memory.WithClock(readClock))
	sub := substratetest.NewServer(t)
	rt := &countingSweepRuntime{
		RuntimeClient: reconciler.NewSubstrateRuntime(sub.Client(), reconciler.SubstrateRuntimeOptions{}),
	}
	_ = mem.FlushHeartbeats(ctx, "rep-victim", 45*time.Second, nil)

	kCrash := seedSweepTask(t, mem, "payments", "crash-mid-steal", v1alpha1.TaskState_TASK_STATE_WORKING, "uid-steal", nil, nil, nil)
	sub.SetActor(&ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "payments", Name: "crash-mid-steal", Uid: "uid-steal"},
		Status:   &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_CRASHED},
	})

	sweeper := reconciler.NewSweeper(mem, rt, reconciler.SweepConfig{
		ReplicaID:     "rep-victim",
		Transition:    stubTransition,
		NumShards:     1,
		ShardLeaseTTL: 30 * time.Millisecond,
		MonitoringQPS: -1,
	})

	rt.onListPage = func(ns string, pageSize int32, pageToken string) ([]*ateapipb.Actor, string, error, bool) {
		actors, next, err := rt.RuntimeClient.ListActorsPage(ctx, ns, pageSize, pageToken)
		// Simulate a pause > ShardLeaseTTL/3 (10ms) and another replica stealing shard 0!
		time.Sleep(15 * time.Millisecond)
		advanceClock(50 * time.Millisecond)
		_, _, _ = mem.TryAcquireShardLease(ctx, "lock:sweep-shard:0", "rep-thief:tok", 45*time.Second)
		return actors, next, err, true
	}

	if err := sweeper.SweepOnce(ctx); err != nil {
		t.Fatalf("SweepOnce(stolen shard): %v", err)
	}
	if ids := sweeper.OwnedShardIDs(); len(ids) != 0 {
		t.Fatalf("expected stolen shard dropped from OwnedShardIDs, got %v", ids)
	}
	if r, _ := mem.GetTaskRow(ctx, kCrash); r.Generation != 1 {
		t.Fatalf("expected stolen shard pass to abort before mutating task, got gen=%d", r.Generation)
	}
}

func TestSweeper_MultiReplicaShardRebalanceGlobalListAndModes(t *testing.T) {
	ctx := t.Context()
	mem := memory.NewStore()
	sub := substratetest.NewServer(t)
	rt := &countingSweepRuntime{
		RuntimeClient: reconciler.NewSubstrateRuntime(sub.Client(), reconciler.SubstrateRuntimeOptions{}),
	}

	// Seed 4 distinct atespaces that all hash to shard 0 when NumShards=1, triggering
	// GlobalListThreshold=2 fallback to a single cluster-wide ListActorsPage("", ...).
	for i := range 4 {
		ns := fmt.Sprintf("tenant-%d", i)
		seedSweepTask(t, mem, ns, "t1", v1alpha1.TaskState_TASK_STATE_WORKING, fmt.Sprintf("uid-%d", i), []byte{1}, nil, nil)
		sub.SetActor(&ateapipb.Actor{
			Metadata: &ateapipb.ResourceMetadata{Atespace: ns, Name: "t1", Uid: fmt.Sprintf("uid-%d", i)},
			Status:   &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_RUNNING},
		})
	}

	_ = mem.FlushHeartbeats(ctx, "rep-a", 45*time.Second, nil)
	recA := reconciler.New(mem, rt, reconciler.Config{
		Mode:        reconciler.ModeInline,
		Transition:  stubTransition,
		MutatingQPS: 200,
	})

	swGlobal := reconciler.NewSweeper(mem, rt, reconciler.SweepConfig{
		ReplicaID:           "rep-a",
		Reconciler:          recA,
		Transition:          stubTransition,
		NumShards:           1,
		GlobalListThreshold: 2,
		MonitoringQPS:       -1,
	})
	if err := swGlobal.SweepOnce(ctx); err != nil {
		t.Fatalf("swGlobal.SweepOnce: %v", err)
	}
	if rt.globalListReqs != 1 {
		t.Fatalf("expected 1 global ListActorsPage(\"\", ...) call above GlobalListThreshold, got %d", rt.globalListReqs)
	}

	// Multi-replica shard balancing (K = 8) + dynamic MutatingQPS scaling across 1 -> 2 -> 3 replicas.
	memShards := memory.NewStore()
	_ = memShards.FlushHeartbeats(ctx, "r1", 45*time.Second, nil)

	mkSweeper := func(id string) *reconciler.Sweeper {
		return reconciler.NewSweeper(memShards, rt, reconciler.SweepConfig{
			ReplicaID:     id,
			Reconciler:    recA,
			Transition:    stubTransition,
			NumShards:     8,
			MonitoringQPS: -1,
		})
	}
	s1 := mkSweeper("r1")
	s2 := mkSweeper("r2")
	s3 := mkSweeper("r3")

	// 1 live replica -> owns all 8 shards; MutatingQPS = 200.
	if err := s1.SweepOnce(ctx); err != nil {
		t.Fatalf("s1 (1 replica): %v", err)
	}
	if len(s1.OwnedShardIDs()) != 8 || math.Abs(recA.EffectiveMutatingQPS()-200) > 0.01 {
		t.Fatalf("1 replica: shards=%v qps=%v, want 8 shards & 200 QPS", s1.OwnedShardIDs(), recA.EffectiveMutatingQPS())
	}

	// 2 live replicas -> ceil(8/2) = 4 shards each; MutatingQPS = 100.
	_ = memShards.FlushHeartbeats(ctx, "r2", 45*time.Second, nil)
	_ = s1.SweepOnce(ctx)
	_ = s2.SweepOnce(ctx)
	if len(s1.OwnedShardIDs()) != 4 || len(s2.OwnedShardIDs()) != 4 || math.Abs(recA.EffectiveMutatingQPS()-100) > 0.01 {
		t.Fatalf("2 replicas: s1=%v s2=%v qps=%v, want 4 shards each & 100 QPS",
			s1.OwnedShardIDs(), s2.OwnedShardIDs(), recA.EffectiveMutatingQPS())
	}

	// 3 live replicas -> ceil(8/3) = 3, 3, 2 shards covering all 8 distinct shards; MutatingQPS = 200/3.
	_ = memShards.FlushHeartbeats(ctx, "r3", 45*time.Second, nil)
	_ = s1.SweepOnce(ctx)
	_ = s2.SweepOnce(ctx)
	_ = s3.SweepOnce(ctx)
	ownedUnion := make(map[int]string)
	for name, sw := range map[string]*reconciler.Sweeper{"r1": s1, "r2": s2, "r3": s3} {
		ids := sw.OwnedShardIDs()
		if len(ids) > 3 || len(ids) < 2 {
			t.Fatalf("3 replicas: %s owns %v (want 2..3 shards)", name, ids)
		}
		for _, id := range ids {
			if prev, dup := ownedUnion[id]; dup {
				t.Fatalf("shard %d owned by both %s and %s", id, prev, name)
			}
			ownedUnion[id] = name
		}
	}
	if len(ownedUnion) != 8 || math.Abs(recA.EffectiveMutatingQPS()-200.0/3.0) > 0.01 {
		t.Fatalf("3 replicas: union=%d qps=%v, want 8 unique shards & 66.67 QPS", len(ownedUnion), recA.EffectiveMutatingQPS())
	}

	// Observe-only and Off modes never mutate storage.
	kCrash := seedSweepTask(t, memShards, "obs-ns", "crash-obs", v1alpha1.TaskState_TASK_STATE_WORKING, "uid-obs", nil, nil, nil)
	sub.SetActor(&ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "obs-ns", Name: "crash-obs", Uid: "uid-obs"},
		Status:   &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_CRASHED},
	})
	memObs := memory.NewStore()
	_ = memObs.FlushHeartbeats(ctx, "obs", 45*time.Second, nil)
	kCrashObs := seedSweepTask(t, memObs, "obs-ns", "crash-obs", v1alpha1.TaskState_TASK_STATE_WORKING, "uid-obs", nil, nil, nil)

	rt.mu.Lock()
	beforeList := rt.listPageCalls
	rt.mu.Unlock()
	swOff := reconciler.NewSweeper(memObs, rt, reconciler.SweepConfig{
		Mode:       reconciler.SweepModeOff,
		Reconciler: recA,
		Transition: stubTransition,
	})
	_ = memObs.FlushHeartbeats(ctx, "obs-2", 45*time.Second, nil) // 2 live replicas in memObs
	_ = swOff.SweepOnce(ctx)
	rt.mu.Lock()
	afterOffList := rt.listPageCalls
	rt.mu.Unlock()
	if afterOffList != beforeList || len(swOff.OwnedShardIDs()) != 0 {
		t.Fatalf("SweepModeOff issued ListActorsPage calls or leased shards: before=%d after=%d shards=%v", beforeList, afterOffList, swOff.OwnedShardIDs())
	}
	if math.Abs(recA.EffectiveMutatingQPS()-100.0) > 0.01 {
		t.Fatalf("SweepModeOff did not scale Reconciler MutatingQPS across 2 live replicas: got %v, want 100", recA.EffectiveMutatingQPS())
	}

	swObs := reconciler.NewSweeper(memObs, rt, reconciler.SweepConfig{
		Mode:          reconciler.SweepModeObserveOnly,
		ReplicaID:     "obs",
		Transition:    stubTransition,
		NumShards:     1,
		MonitoringQPS: -1,
	})
	if err := swObs.SweepOnce(ctx); err != nil {
		t.Fatalf("SweepModeObserveOnly: %v", err)
	}
	if r, _ := memObs.GetTaskRow(ctx, kCrashObs); r.Generation != 1 || r.Task.GetStatus().GetState() != v1alpha1.TaskState_TASK_STATE_WORKING {
		t.Fatalf("SweepModeObserveOnly mutated task row: gen=%d state=%v", r.Generation, r.Task.GetStatus().GetState())
	}
	_ = kCrash
}
