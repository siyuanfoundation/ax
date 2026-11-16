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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/ax/internal/controller"
	"github.com/google/ax/internal/lifecycle"
	"github.com/google/ax/internal/reconciler"
	"github.com/google/ax/internal/store"
	"github.com/google/ax/internal/store/memory"
	"github.com/google/ax/internal/substrate"
	"github.com/google/ax/internal/substrate/substratetest"
	"github.com/google/ax/pkg/apis/v1alpha1"
	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// stubTransition implements a deterministic lifecycle.TransitionFunc covering
// the action, capacity, failure, crash-observation, and watchdog contracts so
// reconciler tests exercise the full reconciliation loop independently of the
// state-machine workstream.
func stubTransition(state lifecycle.State, in lifecycle.Input, now time.Time) (lifecycle.State, lifecycle.Output, error) {
	next := state.Clone()
	if next.Task == nil {
		next.Task = &v1alpha1.Task{Status: &v1alpha1.TaskStatus{}}
	}
	if next.Task.Status == nil {
		next.Task.Status = &v1alpha1.TaskStatus{}
	}

	switch ev := in.(type) {
	case *lifecycle.ActionCompletedInput:
		if len(next.Actions) == 0 || next.Actions[0].ID != ev.ActionID {
			return next, lifecycle.Output{}, nil
		}
		head := next.Actions[0]
		next.Actions = next.Actions[1:]
		switch head.Kind {
		case lifecycle.ActionWaitWorkspaceTag:
			for i := range next.Actions {
				if next.Actions[i].Kind == lifecycle.ActionCreateActor && next.Actions[i].Tag == "" {
					next.Actions[i].Tag = ev.SourceTag
				}
			}
		case lifecycle.ActionCreateActor:
			next.ActorUID = ev.ActorUID
		case lifecycle.ActionResumeActor:
			resumed := now
			next.LastResumedAt = &resumed
			if len(next.Actions) == 0 && next.Task.Status.State == v1alpha1.TaskState_TASK_STATE_QUEUED {
				dl := now.Add(lifecycle.ClaimWatchdogTimeout)
				next.ClaimDeadline = &dl
			}
		case lifecycle.ActionSuspendActorSync:
			if next.RunnerTokenConfirmed && next.RunnerTokenGen == 0 && ev.SnapshotGen > 0 {
				next.RunnerTokenGen = ev.SnapshotGen
			}
			switch head.SuspendReason {
			case lifecycle.SuspendReasonUser:
				next.Task.Status.State = v1alpha1.TaskState_TASK_STATE_SUSPENDED
			case lifecycle.SuspendReasonTurnIdle:
				next.Task.Status.State = v1alpha1.TaskState_TASK_STATE_IDLE
			case lifecycle.SuspendReasonEviction:
				next.Task.Status.State = v1alpha1.TaskState_TASK_STATE_QUEUED
				next.Actions = append([]lifecycle.Action{next.NextAction(lifecycle.ActionResumeActor, now)}, next.Actions...)
			}
		case lifecycle.ActionRevertActor:
			if next.RunnerTokenGen == 0 || ev.SnapshotGen < next.RunnerTokenGen {
				next.RunnerTokenHash = nil
				next.RunnerTokenConfirmed = false
				next.RunnerTokenGen = 0
			}
		case lifecycle.ActionDeleteActor:
			return next, lifecycle.Output{DeleteRow: true}, nil
		}
		return next, lifecycle.Output{}, nil

	case *lifecycle.CapacityUnavailableInput:
		if len(next.Actions) == 0 || next.Actions[0].ID != ev.ActionID {
			return next, lifecycle.Output{}, nil
		}
		delay := lifecycle.DefaultActionInitialBackoff
		for i := 0; i < next.Actions[0].Tries && delay < lifecycle.DefaultActionMaxBackoff; i++ {
			delay *= 2
			if delay > lifecycle.DefaultActionMaxBackoff {
				delay = lifecycle.DefaultActionMaxBackoff
			}
		}
		next.Actions[0].Tries++
		next.Actions[0].NotBefore = now.Add(delay)
		return next, lifecycle.Output{}, nil

	case *lifecycle.ActionFailedInput:
		if len(next.Actions) == 0 || next.Actions[0].ID != ev.ActionID {
			return next, lifecycle.Output{}, nil
		}
		next.Actions = nil
		next.Task.Status.State = v1alpha1.TaskState_TASK_STATE_FAILED
		next.Task.Status.StatusDetail = &v1alpha1.TaskStatusDetail{
			Reason: ev.Reason,
			Detail: ev.Message,
		}
		return next, lifecycle.Output{}, nil

	case *lifecycle.ObservedActorInput:
		if ev.MissingConfirmed {
			next.Actions = nil
			next.ActorUID = ""
			next.Task.Status.State = v1alpha1.TaskState_TASK_STATE_FAILED
			next.Task.Status.StatusDetail = &v1alpha1.TaskStatusDetail{
				Reason: v1alpha1.TaskStatusReason_TASK_STATUS_REASON_SANDBOX_CRASHED,
				Detail: "actor confirmed missing on Substrate",
			}
			return next, lifecycle.Output{}, nil
		}
		switch ev.ActorState {
		case ateapipb.ActorState_ACTOR_STATE_CRASHED:
			curState := next.Task.Status.State
			if lifecycle.IsTerminal(curState) || curState == v1alpha1.TaskState_TASK_STATE_IDLE {
				next.Actions = []lifecycle.Action{next.NextAction(lifecycle.ActionRevertActor, now)}
				return next, lifecycle.Output{}, nil
			}
			rev := next.NextAction(lifecycle.ActionRevertActor, now)
			res := next.NextAction(lifecycle.ActionResumeActor, now)
			next.Actions = []lifecycle.Action{rev, res}
			next.Task.Status.State = v1alpha1.TaskState_TASK_STATE_QUEUED
			next.Task.Status.Attempt++
		case ateapipb.ActorState_ACTOR_STATE_RUNNING:
			curState := next.Task.Status.State
			switch {
			case curState == v1alpha1.TaskState_TASK_STATE_SUSPENDED:
				next.Task.Status.State = v1alpha1.TaskState_TASK_STATE_QUEUED
				resumed := now
				next.LastResumedAt = &resumed
				dl := now.Add(lifecycle.ClaimWatchdogTimeout)
				next.ClaimDeadline = &dl
			case curState == v1alpha1.TaskState_TASK_STATE_IDLE:
				next.Task.Status.State = v1alpha1.TaskState_TASK_STATE_SUSPENDING
				sus := next.NextAction(lifecycle.ActionSuspendActorSync, now)
				sus.SuspendReason = lifecycle.SuspendReasonTurnIdle
				next.Actions = []lifecycle.Action{sus}
			case lifecycle.IsTerminal(curState):
				sus := next.NextAction(lifecycle.ActionSuspendActorSync, now)
				sus.SuspendReason = lifecycle.SuspendReasonTerminal
				next.Actions = []lifecycle.Action{sus}
			}
		case ateapipb.ActorState_ACTOR_STATE_SUSPENDED:
			if next.Task.Status.State == v1alpha1.TaskState_TASK_STATE_WORKING ||
				next.Task.Status.State == v1alpha1.TaskState_TASK_STATE_QUEUED {
				next.ClaimDeadline = nil
				next.Task.Status.State = v1alpha1.TaskState_TASK_STATE_SUSPENDED
			}
		}
		return next, lifecycle.Output{}, nil

	case *lifecycle.HeartbeatLostInput:
		next.ClaimDeadline = nil
		next.Task.Status.State = v1alpha1.TaskState_TASK_STATE_FAILED
		next.Task.Status.StatusDetail = &v1alpha1.TaskStatusDetail{
			Reason: v1alpha1.TaskStatusReason_TASK_STATUS_REASON_SANDBOX_CRASHED,
			Detail: "runner heartbeat silence exceeded threshold",
		}
		sus := next.NextAction(lifecycle.ActionSuspendActorSync, now)
		sus.SuspendReason = lifecycle.SuspendReasonTerminal
		next.Actions = []lifecycle.Action{sus}
		return next, lifecycle.Output{}, nil

	case *lifecycle.TimerExpiredInput:
		next.ClaimDeadline = nil
		next.Task.Status.State = v1alpha1.TaskState_TASK_STATE_FAILED
		next.Task.Status.StatusDetail = &v1alpha1.TaskStatusDetail{
			Reason: v1alpha1.TaskStatusReason_TASK_STATUS_REASON_PROVISIONING_FAILED,
			Detail: "runner claim watchdog expired",
		}
		sus := next.NextAction(lifecycle.ActionSuspendActorSync, now)
		sus.SuspendReason = lifecycle.SuspendReasonTerminal
		next.Actions = []lifecycle.Action{sus}
		return next, lifecycle.Output{}, nil

	default:
		return next, lifecycle.Output{}, fmt.Errorf("unexpected lifecycle input %T", in)
	}
}

// noIOInTxGuard wraps a TaskQueue and RuntimeClient to fail the test if any
// RuntimeClient call ever executes while a store.Apply callback is open on the
// same replica.
type noIOInTxGuard struct {
	store.TaskQueue
	reconciler.RuntimeClient
	t       *testing.T
	inApply atomic.Int32
}

func (g *noIOInTxGuard) Apply(ctx context.Context, key store.TaskKey, fn store.ApplyFunc) (*store.TaskRow, error) {
	return g.TaskQueue.Apply(ctx, key, func(cur *store.TaskRow, now time.Time) (*store.TaskRow, error) {
		g.inApply.Add(1)
		defer g.inApply.Add(-1)
		return fn(cur, now)
	})
}

func (g *noIOInTxGuard) checkNoTx(op string) {
	if g.inApply.Load() > 0 {
		g.t.Errorf("Substrate call %s executed inside store.Apply transaction", op)
	}
}

func (g *noIOInTxGuard) EnsureTemplate(ctx context.Context, ns, tmpl string, p *v1alpha1.Profile) error {
	g.checkNoTx("EnsureTemplate")
	return g.RuntimeClient.EnsureTemplate(ctx, ns, tmpl, p)
}
func (g *noIOInTxGuard) EnsureActor(ctx context.Context, ns, actor, tmpl, tag string) (string, error) {
	g.checkNoTx("EnsureActor")
	return g.RuntimeClient.EnsureActor(ctx, ns, actor, tmpl, tag)
}
func (g *noIOInTxGuard) EnsureActorEgress(ctx context.Context, ns, actor string, rules []*ateapipb.EgressRule) error {
	g.checkNoTx("EnsureActorEgress")
	return g.RuntimeClient.EnsureActorEgress(ctx, ns, actor, rules)
}
func (g *noIOInTxGuard) ResumeActor(ctx context.Context, ns, actor string) error {
	g.checkNoTx("ResumeActor")
	return g.RuntimeClient.ResumeActor(ctx, ns, actor)
}
func (g *noIOInTxGuard) SuspendActorSync(ctx context.Context, ns, actor string) (int32, error) {
	g.checkNoTx("SuspendActorSync")
	return g.RuntimeClient.SuspendActorSync(ctx, ns, actor)
}
func (g *noIOInTxGuard) RevertActor(ctx context.Context, ns, actor string) (int32, error) {
	g.checkNoTx("RevertActor")
	return g.RuntimeClient.RevertActor(ctx, ns, actor)
}
func (g *noIOInTxGuard) DeleteActor(ctx context.Context, ns, actor, expectedUID string) error {
	g.checkNoTx("DeleteActor")
	return g.RuntimeClient.DeleteActor(ctx, ns, actor, expectedUID)
}
func (g *noIOInTxGuard) CreateSnapshotTag(ctx context.Context, ns, actor, tag string) error {
	g.checkNoTx("CreateSnapshotTag")
	return g.RuntimeClient.CreateSnapshotTag(ctx, ns, actor, tag)
}
func (g *noIOInTxGuard) DeleteSnapshotTag(ctx context.Context, ns, tag string) error {
	g.checkNoTx("DeleteSnapshotTag")
	return g.RuntimeClient.DeleteSnapshotTag(ctx, ns, tag)
}
func (g *noIOInTxGuard) GetActor(ctx context.Context, ns, actor string) (*ateapipb.Actor, error) {
	g.checkNoTx("GetActor")
	return g.RuntimeClient.GetActor(ctx, ns, actor)
}
func (g *noIOInTxGuard) ListActorsPage(ctx context.Context, ns string, pageSize int32, pageToken string) ([]*ateapipb.Actor, string, error) {
	g.checkNoTx("ListActorsPage")
	return g.RuntimeClient.ListActorsPage(ctx, ns, pageSize, pageToken)
}
func (g *noIOInTxGuard) GetActorEgressPolicy(ctx context.Context, ns, actor string) (*ateapipb.EgressPolicy, error) {
	g.checkNoTx("GetActorEgressPolicy")
	return g.RuntimeClient.GetActorEgressPolicy(ctx, ns, actor)
}
func (g *noIOInTxGuard) GetTag(ctx context.Context, ns, tag string) (*ateapipb.Tag, error) {
	g.checkNoTx("GetTag")
	return g.RuntimeClient.GetTag(ctx, ns, tag)
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("timed out waiting for condition")
}

func TestReconciler_FullProvisionCheckpointTagAndDeleteFlow(t *testing.T) {
	sub := substratetest.NewServer(t)
	mem := memory.NewStore()
	rt := reconciler.NewSubstrateRuntime(sub.Client(), reconciler.SubstrateRuntimeOptions{
		DefaultTaskImage: "us-docker.pkg.dev/acme/agent@sha256:abcdef",
	})
	guard := &noIOInTxGuard{TaskQueue: mem, RuntimeClient: rt, t: t}

	rec := reconciler.New(guard, guard, reconciler.Config{
		Mode:         reconciler.ModeQueue,
		Transition:   stubTransition,
		PollInterval: 20 * time.Millisecond,
		MutatingQPS:  -1,
	})

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = rec.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})

	prof := &v1alpha1.Profile{Image: "us-docker.pkg.dev/acme/agent@sha256:abcdef"}
	tmplName, _, err := controller.ResolveProfileTemplate("payments", prof, "", "")
	if err != nil {
		t.Fatalf("ResolveProfileTemplate: %v", err)
	}

	key := store.TaskKey{Atespace: "payments", Name: "fix-bug"}
	if _, err := guard.Apply(ctx, key, func(_ *store.TaskRow, now time.Time) (*store.TaskRow, error) {
		st := lifecycle.State{
			Task: &v1alpha1.Task{
				Status: &v1alpha1.TaskStatus{
					State:            v1alpha1.TaskState_TASK_STATE_QUEUED,
					AteActorTemplate: tmplName,
					Attempt:          1,
				},
			},
			RunnerTokenHash:      []byte{1, 2, 3},
			RunnerTokenConfirmed: true,
		}
		st.Actions = []lifecycle.Action{
			st.NextAction(lifecycle.ActionEnsureTemplate, now),
			st.NextAction(lifecycle.ActionCreateActor, now),
			st.NextAction(lifecycle.ActionEnsureEgress, now),
			st.NextAction(lifecycle.ActionResumeActor, now),
		}
		row := &store.TaskRow{
			EffectiveProfile: prof,
			EgressRules: []*ateapipb.EgressRule{{
				Https: &ateapipb.HttpsRule{Hostnames: []string{"api.example.com"}},
			}},
		}
		row.SetLifecycleState(st, now)
		return row, nil
	}); err != nil {
		t.Fatalf("Apply(create): %v", err)
	}

	// Wait for all 4 provisioning actions to complete under the queue worker.
	waitFor(t, 3*time.Second, func() bool {
		row, err := mem.GetTaskRow(ctx, key)
		return err == nil && len(row.Actions) == 0 && row.LeaseOwner == ""
	})

	row, err := mem.GetTaskRow(ctx, key)
	if err != nil {
		t.Fatalf("GetTaskRow: %v", err)
	}
	if row.ActorUID == "" || row.ClaimDeadline == nil {
		t.Fatalf("expected ActorUID and ClaimDeadline populated after RESUME_ACTOR, got %+v", row.State)
	}
	actor, ok := sub.Actor("payments", "fix-bug")
	if !ok || actor.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_RUNNING {
		t.Fatalf("Substrate actor = %+v, ok=%v; want RUNNING", actor, ok)
	}
	if policy, hasPolicy := sub.EgressPolicy("payments", "fix-bug"); !hasPolicy || len(policy.GetRules()) != 1 {
		t.Fatalf("Substrate egress policy = %+v, ok=%v; want 1 rule", policy, hasPolicy)
	}

	// Queue SUSPEND_ACTOR_SYNC + CREATE_TAG and verify SnapshotGen and Tag creation.
	if _, err := guard.Apply(ctx, key, func(cur *store.TaskRow, now time.Time) (*store.TaskRow, error) {
		next := cur.Clone()
		st := next.LifecycleState()
		st.ClaimDeadline = nil
		st.Task.Status.State = v1alpha1.TaskState_TASK_STATE_SUSPENDING
		sus := st.NextAction(lifecycle.ActionSuspendActorSync, now)
		sus.SuspendReason = lifecycle.SuspendReasonUser
		tag := st.NextAction(lifecycle.ActionCreateTag, now)
		tag.Tag = "snap-tag-1"
		st.Actions = []lifecycle.Action{sus, tag}
		next.SetLifecycleState(st, now)
		return next, nil
	}); err != nil {
		t.Fatalf("Apply(suspend+tag): %v", err)
	}

	waitFor(t, 3*time.Second, func() bool {
		r, err := mem.GetTaskRow(ctx, key)
		return err == nil && len(r.Actions) == 0 && r.LeaseOwner == ""
	})
	row, _ = mem.GetTaskRow(ctx, key)
	if row.Task.GetStatus().GetState() != v1alpha1.TaskState_TASK_STATE_SUSPENDED || row.RunnerTokenGen != 2 {
		t.Fatalf("state=%v tokenGen=%d, want SUSPENDED and 2", row.Task.GetStatus().GetState(), row.RunnerTokenGen)
	}
	if _, tagOK := sub.Tag("payments", "snap-tag-1"); !tagOK {
		t.Fatal("expected snap-tag-1 created on Substrate")
	}

	// Queue DELETE_TAG + DELETE_ACTOR and verify both Substrate resources and the row are removed.
	if _, err := guard.Apply(ctx, key, func(cur *store.TaskRow, now time.Time) (*store.TaskRow, error) {
		next := cur.Clone()
		st := next.LifecycleState()
		st.Task.Metadata.DeleteTime = timestamppb.New(now)
		delTag := st.NextAction(lifecycle.ActionDeleteTag, now)
		delTag.Tag = "snap-tag-1"
		delActor := st.NextAction(lifecycle.ActionDeleteActor, now)
		st.Actions = []lifecycle.Action{delTag, delActor}
		next.SetLifecycleState(st, now)
		return next, nil
	}); err != nil {
		t.Fatalf("Apply(delete): %v", err)
	}

	waitFor(t, 3*time.Second, func() bool {
		_, err := mem.GetTaskRow(ctx, key)
		return errors.Is(err, store.ErrNotFound)
	})
	if _, actorOK := sub.Actor("payments", "fix-bug"); actorOK {
		t.Fatal("expected Substrate actor deleted")
	}
	if _, tagOK := sub.Tag("payments", "snap-tag-1"); tagOK {
		t.Fatal("expected Substrate tag deleted")
	}
}

// customRuntime lets tests block specific RPCs and inject synthetic errors.
type customRuntime struct {
	reconciler.RuntimeClient
	mu          sync.Mutex
	onResume    func(ctx context.Context, ns, actor string) error
	onDelete    func(ctx context.Context, ns, actor string) error
	resumeCalls int
	deleteCalls int
}

func (c *customRuntime) ResumeActor(ctx context.Context, ns, actor string) error {
	c.mu.Lock()
	c.resumeCalls++
	fn := c.onResume
	c.mu.Unlock()
	if fn != nil {
		return fn(ctx, ns, actor)
	}
	return c.RuntimeClient.ResumeActor(ctx, ns, actor)
}

func (c *customRuntime) DeleteActor(ctx context.Context, ns, actor, expectedUID string) error {
	c.mu.Lock()
	c.deleteCalls++
	fn := c.onDelete
	c.mu.Unlock()
	if fn != nil {
		return fn(ctx, ns, actor)
	}
	return c.RuntimeClient.DeleteActor(ctx, ns, actor, expectedUID)
}

func TestReconciler_CancelsObsoleteInFlightActionAndRunsReplacement(t *testing.T) {
	sub := substratetest.NewServer(t)
	mem := memory.NewStore()
	baseRT := reconciler.NewSubstrateRuntime(sub.Client(), reconciler.SubstrateRuntimeOptions{
		DefaultTaskImage: "us-docker.pkg.dev/acme/agent@sha256:abcdef",
	})

	resumeStarted := make(chan struct{})
	resumeCanceled := make(chan struct{})
	rt := &customRuntime{
		RuntimeClient: baseRT,
		onResume: func(ctx context.Context, _, _ string) error {
			close(resumeStarted)
			<-ctx.Done()
			close(resumeCanceled)
			return ctx.Err()
		},
	}

	rec := reconciler.New(mem, rt, reconciler.Config{
		Mode:          reconciler.ModeQueue,
		Transition:    stubTransition,
		LeaseTTL:      500 * time.Millisecond,
		RenewInterval: 30 * time.Millisecond,
		PollInterval:  20 * time.Millisecond,
		MutatingQPS:   -1,
	})

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = rec.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})

	sub.SetActor(&ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "default", Name: "cancel-inflight", Uid: "uid-ci"},
		Status:   &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
	})
	key := store.TaskKey{Atespace: "default", Name: "cancel-inflight"}
	if _, err := mem.Apply(ctx, key, func(_ *store.TaskRow, now time.Time) (*store.TaskRow, error) {
		st := lifecycle.State{
			Task:     &v1alpha1.Task{Status: &v1alpha1.TaskStatus{State: v1alpha1.TaskState_TASK_STATE_QUEUED}},
			ActorUID: "uid-ci",
		}
		st.Actions = []lifecycle.Action{st.NextAction(lifecycle.ActionResumeActor, now)}
		row := &store.TaskRow{}
		row.SetLifecycleState(st, now)
		return row, nil
	}); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	// Wait until ResumeActor is blocked in flight, then supersede it with DELETE_ACTOR.
	<-resumeStarted
	if _, err := mem.Apply(ctx, key, func(cur *store.TaskRow, now time.Time) (*store.TaskRow, error) {
		next := cur.Clone()
		st := next.LifecycleState()
		st.Task.Metadata.DeleteTime = timestamppb.New(now)
		st.Actions = []lifecycle.Action{st.NextAction(lifecycle.ActionDeleteActor, now)}
		next.SetLifecycleState(st, now)
		return next, nil
	}); err != nil {
		t.Fatalf("Apply(DeleteTask): %v", err)
	}

	select {
	case <-resumeCanceled:
	case <-time.After(2 * time.Second):
		t.Fatal("obsolete ResumeActor was not canceled on lease renewal")
	}

	// The worker must pick up DELETE_ACTOR immediately under its active lease and remove the row.
	waitFor(t, 2*time.Second, func() bool {
		_, err := mem.GetTaskRow(ctx, key)
		return errors.Is(err, store.ErrNotFound)
	})
}

func TestReconciler_SuspendOnCrashedActorAndWatchdogAndInlineMode(t *testing.T) {
	sub := substratetest.NewServer(t)
	nowMu := sync.Mutex{}
	nowVal := time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)
	getNow := func() time.Time {
		nowMu.Lock()
		defer nowMu.Unlock()
		return nowVal
	}
	advance := func(d time.Duration) {
		nowMu.Lock()
		nowVal = nowVal.Add(d)
		nowMu.Unlock()
	}

	mem := memory.NewStore(memory.WithClock(getNow))
	rt := reconciler.NewSubstrateRuntime(sub.Client(), reconciler.SubstrateRuntimeOptions{
		DefaultTaskImage: "us-docker.pkg.dev/acme/agent@sha256:abcdef",
	})

	rec := reconciler.New(mem, rt, reconciler.Config{
		Mode:        reconciler.ModeInline,
		Transition:  stubTransition,
		MutatingQPS: -1,
		Clock:       getNow,
	})

	ctx := t.Context()

	// 1. Seed a crashed actor on Substrate and run SUSPEND_ACTOR_SYNC on an
	// active SUSPENDING task: reconciler must convert FailedPrecondition into
	// ObservedActor{CRASHED} -> REVERT_ACTOR + RESUME_ACTOR and recover it!
	sub.SetActor(&ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "default", Name: "crashed-task", Uid: "uid-c1"},
		Status: &ateapipb.ActorStatus{
			State: ateapipb.ActorState_ACTOR_STATE_CRASHED,
			Crash: &ateapipb.ActorCrash{Message: "OOM"},
		},
	})
	key := store.TaskKey{Atespace: "default", Name: "crashed-task"}
	if _, err := mem.Apply(ctx, key, func(_ *store.TaskRow, ts time.Time) (*store.TaskRow, error) {
		st := lifecycle.State{
			Task: &v1alpha1.Task{
				Status: &v1alpha1.TaskStatus{
					State:   v1alpha1.TaskState_TASK_STATE_SUSPENDING,
					Attempt: 1,
				},
			},
			ActorUID: "uid-c1",
		}
		sus := st.NextAction(lifecycle.ActionSuspendActorSync, ts)
		sus.SuspendReason = lifecycle.SuspendReasonUser
		st.Actions = []lifecycle.Action{sus}
		row := &store.TaskRow{}
		row.SetLifecycleState(st, ts)
		return row, nil
	}); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	if err := rec.ReconcileInline(ctx, key); err != nil {
		t.Fatalf("ReconcileInline(crash recovery): %v", err)
	}
	row, err := mem.GetTaskRow(ctx, key)
	if err != nil {
		t.Fatalf("GetTaskRow: %v", err)
	}
	if row.Task.GetStatus().GetState() != v1alpha1.TaskState_TASK_STATE_QUEUED || row.Task.GetStatus().GetAttempt() != 2 || len(row.Actions) != 0 {
		t.Fatalf("expected crash recovered to QUEUED attempt 2 with empty actions, got state=%v attempt=%d actions=%v",
			row.Task.GetStatus().GetState(), row.Task.GetStatus().GetAttempt(), row.Actions)
	}
	actor, _ := sub.Actor("default", "crashed-task")
	if actor.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_RUNNING {
		t.Fatalf("actor state after REVERT+RESUME = %v, want RUNNING", actor.GetStatus().GetState())
	}

	// 2. ClaimDeadline watchdog does not fire early, then fires once 5m elapses.
	if err := rec.ReconcileInline(ctx, key); err != nil {
		t.Fatalf("ReconcileInline before deadline: %v", err)
	}
	row, _ = mem.GetTaskRow(ctx, key)
	if row.Task.GetStatus().GetState() != v1alpha1.TaskState_TASK_STATE_QUEUED {
		t.Fatalf("watchdog fired early: state=%v", row.Task.GetStatus().GetState())
	}

	advance(lifecycle.ClaimWatchdogTimeout + time.Second)
	if err := rec.ReconcileInline(ctx, key); err != nil {
		t.Fatalf("ReconcileInline at deadline: %v", err)
	}
	row, _ = mem.GetTaskRow(ctx, key)
	if row.Task.GetStatus().GetState() != v1alpha1.TaskState_TASK_STATE_FAILED {
		t.Fatalf("expected FAILED after watchdog expiration, got %v", row.Task.GetStatus().GetState())
	}
}

func TestReconciler_CircuitBreakerOpensOnOutageAndRecoversOnProbe(t *testing.T) {
	sub := substratetest.NewServer(t)
	nowMu := sync.Mutex{}
	nowVal := time.Date(2026, 11, 16, 14, 0, 0, 0, time.UTC)
	getNow := func() time.Time {
		nowMu.Lock()
		defer nowMu.Unlock()
		return nowVal
	}
	advance := func(d time.Duration) {
		nowMu.Lock()
		nowVal = nowVal.Add(d)
		nowMu.Unlock()
	}

	mem := memory.NewStore(memory.WithClock(getNow))
	baseRT := reconciler.NewSubstrateRuntime(sub.Client(), reconciler.SubstrateRuntimeOptions{
		DefaultTaskImage: "us-docker.pkg.dev/acme/agent@sha256:abcdef",
	})

	// Exercise NewSubstrateRuntime directly against substratetest.Server.SetErrors
	// so WithSingleAttempt is verified to keep outbound calls 1:1 with worker
	// attempts without triggering 10x internal retry loops during an outage.
	sub.SetErrors(substratetest.Errors{
		ResumeActor: status.Error(codes.Unavailable, "substrate control plane down"),
	})

	rec := reconciler.New(mem, baseRT, reconciler.Config{
		Mode:           reconciler.ModeQueue,
		Transition:     stubTransition,
		PollInterval:   15 * time.Millisecond,
		InitialBackoff: 50 * time.Millisecond,
		MaxBackoff:     100 * time.Millisecond,
		MutatingQPS:    -1,
		Clock:          getNow,
		CircuitBreaker: reconciler.CircuitBreakerConfig{
			Window:           30 * time.Second,
			FailureThreshold: 0.5,
			MinCalls:         4,
			ProbeInterval:    5 * time.Second,
		},
	})

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = rec.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})

	for i := range 6 {
		name := "outage-" + string(rune('a'+i))
		uid := "u-" + name
		sub.SetActor(&ateapipb.Actor{
			Metadata: &ateapipb.ResourceMetadata{Atespace: "default", Name: name, Uid: uid},
			Status:   &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
		})
		k := store.TaskKey{Atespace: "default", Name: name}
		if _, err := mem.Apply(ctx, k, func(_ *store.TaskRow, ts time.Time) (*store.TaskRow, error) {
			st := lifecycle.State{
				Task:     &v1alpha1.Task{Status: &v1alpha1.TaskStatus{State: v1alpha1.TaskState_TASK_STATE_QUEUED}},
				ActorUID: uid,
			}
			st.Actions = []lifecycle.Action{st.NextAction(lifecycle.ActionResumeActor, ts)}
			row := &store.TaskRow{}
			row.SetLifecycleState(st, ts)
			return row, nil
		}); err != nil {
			t.Fatalf("seed Apply: %v", err)
		}
	}

	waitFor(t, 2*time.Second, func() bool {
		if !rec.CircuitBreakerOpen() {
			return false
		}
		// Wait for all 6 initial worker attempts to release their leases at the
		// current fake timestamp before advancing the frozen test clock.
		for i := range 6 {
			k := store.TaskKey{Atespace: "default", Name: "outage-" + string(rune('a'+i))}
			r, err := mem.GetTaskRow(ctx, k)
			if err != nil || r.LeaseOwner != "" {
				return false
			}
		}
		return true
	})

	// Verify no task entered FAILED during the outage.
	for i := range 6 {
		k := store.TaskKey{Atespace: "default", Name: "outage-" + string(rune('a'+i))}
		r, err := mem.GetTaskRow(ctx, k)
		if err != nil || r.Task.GetStatus().GetState() == v1alpha1.TaskState_TASK_STATE_FAILED {
			t.Fatalf("task %v unexpectedly failed during outage: %+v, err=%v", k, r, err)
		}
	}

	// Restore Substrate, advance clock past backoff + 5s probe interval, and verify
	// a single probe closes the breaker and drains all 6 tasks!
	sub.SetErrors(substratetest.Errors{})
	advance(6 * time.Second)
	rec.Notify()

	waitFor(t, 3*time.Second, func() bool {
		if rec.CircuitBreakerOpen() {
			return false
		}
		for i := range 6 {
			k := store.TaskKey{Atespace: "default", Name: "outage-" + string(rune('a'+i))}
			r, err := mem.GetTaskRow(ctx, k)
			if err != nil || len(r.Actions) != 0 {
				return false
			}
		}
		return true
	})
}

func TestReconciler_WaitWorkspaceTagCapacityActionTimeoutAndTerminalCrash(t *testing.T) {
	sub := substratetest.NewServer(t)
	nowMu := sync.Mutex{}
	nowVal := time.Date(2026, 11, 16, 16, 0, 0, 0, time.UTC)
	getNow := func() time.Time {
		nowMu.Lock()
		defer nowMu.Unlock()
		return nowVal
	}
	advance := func(d time.Duration) {
		nowMu.Lock()
		nowVal = nowVal.Add(d)
		nowMu.Unlock()
	}

	mem := memory.NewStore(memory.WithClock(getNow))
	baseRT := reconciler.NewSubstrateRuntime(sub.Client(), reconciler.SubstrateRuntimeOptions{
		DefaultTaskImage: "us-docker.pkg.dev/acme/agent@sha256:abcdef",
	})

	var (
		tagReady   atomic.Bool
		hangResume atomic.Bool
		capExhaust atomic.Bool
	)
	rt := &customRuntime{
		RuntimeClient: baseRT,
		onResume: func(ctx context.Context, ns, actor string) error {
			if hangResume.Load() {
				<-ctx.Done()
				return ctx.Err()
			}
			if capExhaust.Load() {
				return status.Error(codes.ResourceExhausted, "no worker capacity")
			}
			return baseRT.ResumeActor(ctx, ns, actor)
		},
	}

	rec := reconciler.New(mem, rt, reconciler.Config{
		Mode:                   reconciler.ModeInline,
		Transition:             stubTransition,
		ActionTimeout:          40 * time.Millisecond,
		InitialBackoff:         50 * time.Millisecond,
		MaxBackoff:             100 * time.Millisecond,
		MaxInFlightPerAtespace: 1,
		MutatingQPS:            -1,
		Clock:                  getNow,
		WorkspaceTagResolver: func(_ context.Context, _, _, _, requestedTag string) (string, bool, error) {
			if tagReady.Load() {
				return requestedTag, true, nil
			}
			return "", false, nil
		},
	})

	ctx := t.Context()
	prof := &v1alpha1.Profile{Image: "us-docker.pkg.dev/acme/agent@sha256:abcdef"}
	tmplName, tmpl, err := controller.ResolveProfileTemplate("default", prof, "", "")
	if err != nil {
		t.Fatalf("ResolveProfileTemplate: %v", err)
	}
	if err := sub.Client().EnsureAtespace(ctx, "default"); err != nil {
		t.Fatalf("EnsureAtespace: %v", err)
	}
	createdTmpl, err := sub.Client().EnsureActorTemplate(ctx, tmpl)
	if err != nil {
		t.Fatalf("EnsureActorTemplate: %v", err)
	}
	sub.SetTag(&ateapipb.Tag{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "default", Name: "warm-tag-r1"},
		Status: &ateapipb.TagStatus{
			ActorTemplateUid: createdTmpl.GetMetadata().GetUid(),
			Snapshot: &ateapipb.Snapshot{
				Generation: 1,
				DurableSnapshot: &ateapipb.SnapshotStorage{
					State: ateapipb.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_COMPLETED,
				},
			},
		},
	})

	// 1. WAIT_WORKSPACE_TAG backs off while tagReady == false, then propagates
	// SourceTag to CREATE_ACTOR once tagReady == true.
	wsKey := store.TaskKey{Atespace: "default", Name: "warm-task"}
	if _, err := mem.Apply(ctx, wsKey, func(_ *store.TaskRow, ts time.Time) (*store.TaskRow, error) {
		st := lifecycle.State{
			Task: &v1alpha1.Task{
				Spec: &v1alpha1.TaskSpec{Workspace: &v1alpha1.WorkspaceRef{Name: "ws-1"}},
				Status: &v1alpha1.TaskStatus{
					State:            v1alpha1.TaskState_TASK_STATE_QUEUED,
					AteActorTemplate: tmplName,
				},
			},
		}
		waitAct := st.NextAction(lifecycle.ActionWaitWorkspaceTag, ts)
		waitAct.Tag = "warm-tag-r1"
		createAct := st.NextAction(lifecycle.ActionCreateActor, ts)
		resumeAct := st.NextAction(lifecycle.ActionResumeActor, ts)
		st.Actions = []lifecycle.Action{waitAct, createAct, resumeAct}
		row := &store.TaskRow{EffectiveProfile: prof}
		row.SetLifecycleState(st, ts)
		return row, nil
	}); err != nil {
		t.Fatalf("Apply(warm-task): %v", err)
	}

	if err := rec.ReconcileInline(ctx, wsKey); err != nil {
		t.Fatalf("ReconcileInline(tag warming): %v", err)
	}
	rWarm, _ := mem.GetTaskRow(ctx, wsKey)
	if len(rWarm.Actions) != 3 || rWarm.Actions[0].Kind != lifecycle.ActionWaitWorkspaceTag || rWarm.Actions[0].Tries != 1 {
		t.Fatalf("expected WAIT_WORKSPACE_TAG backed off with Tries=1, got %+v", rWarm.Actions)
	}

	// Mark tag ready, advance clock past backoff, and test ActionTimeout on hung ResumeActor.
	tagReady.Store(true)
	hangResume.Store(true)
	advance(time.Second)
	if err := rec.ReconcileInline(ctx, wsKey); err != nil {
		t.Fatalf("ReconcileInline(hung resume): %v", err)
	}
	rAfterHang, _ := mem.GetTaskRow(ctx, wsKey)
	if len(rAfterHang.Actions) != 1 || rAfterHang.Actions[0].Kind != lifecycle.ActionResumeActor || rAfterHang.Actions[0].Tries != 1 {
		t.Fatalf("expected RESUME_ACTOR timed out via ActionTimeout and backed off, got %+v", rAfterHang.Actions)
	}
	createdActor, ok := sub.Actor("default", "warm-task")
	if !ok || createdActor.GetSourceTag().GetName() != "warm-tag-r1" {
		t.Fatalf("expected actor seeded from warm-tag-r1, got %+v", createdActor)
	}

	// 2. CapacityUnavailable backs off RESUME_ACTOR while keeping the task in
	// TASK_STATE_QUEUED (never failing the task for capacity exhaustion), then
	// resumes once capacity becomes available.
	hangResume.Store(false)
	capExhaust.Store(true)
	advance(time.Second)
	if err := rec.ReconcileInline(ctx, wsKey); err != nil {
		t.Fatalf("ReconcileInline(capacity 1): %v", err)
	}
	rCap, _ := mem.GetTaskRow(ctx, wsKey)
	if rCap.Task.GetStatus().GetState() != v1alpha1.TaskState_TASK_STATE_QUEUED || len(rCap.Actions) != 1 || rCap.Actions[0].Tries != 2 {
		t.Fatalf("expected QUEUED during capacity backoff with Tries=2, got state=%v actions=%v", rCap.Task.GetStatus().GetState(), rCap.Actions)
	}

	advance(45 * time.Minute)
	if err := rec.ReconcileInline(ctx, wsKey); err != nil {
		t.Fatalf("ReconcileInline(capacity sustained >30m): %v", err)
	}
	rStillQueued, _ := mem.GetTaskRow(ctx, wsKey)
	if rStillQueued.Task.GetStatus().GetState() != v1alpha1.TaskState_TASK_STATE_QUEUED || len(rStillQueued.Actions) != 1 {
		t.Fatalf("expected task to stay QUEUED across extended capacity exhaustion, got state=%v actions=%v",
			rStillQueued.Task.GetStatus().GetState(), rStillQueued.Actions)
	}

	capExhaust.Store(false)
	advance(lifecycle.DefaultActionMaxBackoff + time.Second)
	if err := rec.ReconcileInline(ctx, wsKey); err != nil {
		t.Fatalf("ReconcileInline(capacity recovered): %v", err)
	}
	rResumed, _ := mem.GetTaskRow(ctx, wsKey)
	if rResumed.Task.GetStatus().GetState() != v1alpha1.TaskState_TASK_STATE_QUEUED || len(rResumed.Actions) != 0 || rResumed.ClaimDeadline == nil {
		t.Fatalf("expected RESUME_ACTOR completed once capacity recovered, got state=%v actions=%v deadline=%v",
			rResumed.Task.GetStatus().GetState(), rResumed.Actions, rResumed.ClaimDeadline)
	}

	// 3. Crashed terminal task executing SUSPEND_ACTOR_SYNC routes via
	// ObservedActor{CRASHED} -> REVERT_ACTOR and clears uncheckpointed RunnerTokenHash!
	sub.SetActor(&ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "default", Name: "done-crashed", Uid: "uid-dc"},
		Status:   &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_CRASHED},
	})
	dcKey := store.TaskKey{Atespace: "default", Name: "done-crashed"}
	if _, err := mem.Apply(ctx, dcKey, func(_ *store.TaskRow, ts time.Time) (*store.TaskRow, error) {
		st := lifecycle.State{
			Task: &v1alpha1.Task{
				Status: &v1alpha1.TaskStatus{State: v1alpha1.TaskState_TASK_STATE_COMPLETED},
			},
			ActorUID:             "uid-dc",
			RunnerTokenHash:      []byte{9, 9, 9},
			RunnerTokenConfirmed: true,
			RunnerTokenGen:       0, // never durably checkpointed before crash
		}
		sus := st.NextAction(lifecycle.ActionSuspendActorSync, ts)
		sus.SuspendReason = lifecycle.SuspendReasonTerminal
		st.Actions = []lifecycle.Action{sus}
		row := &store.TaskRow{}
		row.SetLifecycleState(st, ts)
		return row, nil
	}); err != nil {
		t.Fatalf("Apply(done-crashed): %v", err)
	}

	if err := rec.ReconcileInline(ctx, dcKey); err != nil {
		t.Fatalf("ReconcileInline(done-crashed): %v", err)
	}
	rDC, _ := mem.GetTaskRow(ctx, dcKey)
	if len(rDC.Actions) != 0 || rDC.RunnerTokenHash != nil || rDC.RunnerTokenConfirmed {
		t.Fatalf("expected terminal crash reverted with uncheckpointed token cleared, got %+v", rDC.State)
	}

	// 4. Foreign actor protection: pre-existing crashed actor on a DIFFERENT
	// template must NOT be adopted, reverted, or deleted on CREATE_ACTOR!
	sub.SetActor(&ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: "default", Name: "foreign-actor", Uid: "foreign-uid"},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: "default", Name: "foreign-template"},
		Status:        &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_CRASHED},
	})
	foreignKey := store.TaskKey{Atespace: "default", Name: "foreign-actor"}
	if _, err := mem.Apply(ctx, foreignKey, func(_ *store.TaskRow, ts time.Time) (*store.TaskRow, error) {
		st := lifecycle.State{
			Task: &v1alpha1.Task{
				Status: &v1alpha1.TaskStatus{
					State:            v1alpha1.TaskState_TASK_STATE_QUEUED,
					AteActorTemplate: tmplName,
				},
			},
		}
		st.Actions = []lifecycle.Action{st.NextAction(lifecycle.ActionCreateActor, ts)}
		row := &store.TaskRow{EffectiveProfile: prof}
		row.SetLifecycleState(st, ts)
		return row, nil
	}); err != nil {
		t.Fatalf("Apply(foreign-actor): %v", err)
	}
	if err := rec.ReconcileInline(ctx, foreignKey); err != nil {
		t.Fatalf("ReconcileInline(foreign-actor): %v", err)
	}
	rForeign, _ := mem.GetTaskRow(ctx, foreignKey)
	if rForeign.Task.GetStatus().GetState() != v1alpha1.TaskState_TASK_STATE_FAILED {
		t.Fatalf("expected CREATE_ACTOR against foreign actor to fail task permanently, got %v", rForeign.Task.GetStatus().GetState())
	}
	preservedForeign, stillExists := sub.Actor("default", "foreign-actor")
	if !stillExists || preservedForeign.GetMetadata().GetUid() != "foreign-uid" ||
		preservedForeign.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_CRASHED {
		t.Fatalf("foreign actor was modified or deleted: %+v, exists=%v", preservedForeign, stillExists)
	}

	// Deleting that failed task (row.ActorUID == "") MUST also leave the
	// foreign actor completely untouched while removing the task row!
	if _, err := mem.Apply(ctx, foreignKey, func(cur *store.TaskRow, ts time.Time) (*store.TaskRow, error) {
		next := cur.Clone()
		st := next.LifecycleState()
		st.Task.Metadata.DeleteTime = timestamppb.New(ts)
		st.Actions = []lifecycle.Action{st.NextAction(lifecycle.ActionDeleteActor, ts)}
		next.SetLifecycleState(st, ts)
		return next, nil
	}); err != nil {
		t.Fatalf("Apply(DeleteTask foreign-actor): %v", err)
	}
	if err := rec.ReconcileInline(ctx, foreignKey); err != nil {
		t.Fatalf("ReconcileInline(DeleteTask foreign-actor): %v", err)
	}
	if _, err := mem.GetTaskRow(ctx, foreignKey); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("expected foreign-actor task row removed after DELETE_ACTOR, got err=%v", err)
	}
	preservedAfterDelete, stillExistsAfterDelete := sub.Actor("default", "foreign-actor")
	if !stillExistsAfterDelete || preservedAfterDelete.GetMetadata().GetUid() != "foreign-uid" ||
		preservedAfterDelete.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_CRASHED {
		t.Fatalf("foreign actor was deleted or mutated by DELETE_ACTOR: %+v, exists=%v", preservedAfterDelete, stillExistsAfterDelete)
	}

	// 5. Pending durable snapshot upload on already-SUSPENDED actor:
	// SUSPEND_ACTOR_SYNC backs off until durable_snapshot reaches COMPLETED so
	// RunnerTokenGen is never stamped with an older snapshot generation.
	pendingKey := store.TaskKey{Atespace: "default", Name: "pending-upload"}
	sub.SetActor(&ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: "default", Name: "pending-upload", Uid: "uid-pu"},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: "default", Name: tmplName},
		Status: &ateapipb.ActorStatus{
			State:                  ateapipb.ActorState_ACTOR_STATE_SUSPENDED,
			LastAssignedGeneration: 2,
			Snapshots: []*ateapipb.Snapshot{
				{
					Generation:      1,
					DurableSnapshot: &ateapipb.SnapshotStorage{State: ateapipb.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_COMPLETED},
				},
				{
					Generation:      2,
					Locality:        "node-1",
					DurableSnapshot: &ateapipb.SnapshotStorage{State: ateapipb.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_PENDING},
				},
			},
		},
	})
	if _, err := mem.Apply(ctx, pendingKey, func(_ *store.TaskRow, ts time.Time) (*store.TaskRow, error) {
		st := lifecycle.State{
			Task: &v1alpha1.Task{
				Status: &v1alpha1.TaskStatus{
					State:            v1alpha1.TaskState_TASK_STATE_SUSPENDING,
					AteActorTemplate: tmplName,
				},
			},
			ActorUID:             "uid-pu",
			RunnerTokenHash:      []byte{7, 7, 7},
			RunnerTokenConfirmed: true,
			RunnerTokenGen:       0,
		}
		sus := st.NextAction(lifecycle.ActionSuspendActorSync, ts)
		sus.SuspendReason = lifecycle.SuspendReasonUser
		st.Actions = []lifecycle.Action{sus}
		row := &store.TaskRow{EffectiveProfile: prof}
		row.SetLifecycleState(st, ts)
		return row, nil
	}); err != nil {
		t.Fatalf("Apply(pending-upload): %v", err)
	}
	if err := rec.ReconcileInline(ctx, pendingKey); err != nil {
		t.Fatalf("ReconcileInline(pending-upload while PENDING): %v", err)
	}
	rPending, _ := mem.GetTaskRow(ctx, pendingKey)
	if rPending.Task.GetStatus().GetState() != v1alpha1.TaskState_TASK_STATE_SUSPENDING || rPending.RunnerTokenGen != 0 {
		t.Fatalf("expected SUSPENDING with RunnerTokenGen=0 while upload PENDING, got state=%v gen=%d",
			rPending.Task.GetStatus().GetState(), rPending.RunnerTokenGen)
	}

	// Mark generation 2 COMPLETED in object storage, advance past backoff, and
	// verify SUSPEND_ACTOR_SYNC completes with RunnerTokenGen == 2!
	sub.SetActor(&ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: "default", Name: "pending-upload", Uid: "uid-pu"},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: "default", Name: tmplName},
		Status: &ateapipb.ActorStatus{
			State:                  ateapipb.ActorState_ACTOR_STATE_SUSPENDED,
			LastAssignedGeneration: 2,
			Snapshots: []*ateapipb.Snapshot{
				{
					Generation:      2,
					DurableSnapshot: &ateapipb.SnapshotStorage{State: ateapipb.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_COMPLETED},
				},
			},
		},
	})
	advance(time.Second)
	if err := rec.ReconcileInline(ctx, pendingKey); err != nil {
		t.Fatalf("ReconcileInline(pending-upload once COMPLETED): %v", err)
	}
	rCompletedUpload, _ := mem.GetTaskRow(ctx, pendingKey)
	if rCompletedUpload.Task.GetStatus().GetState() != v1alpha1.TaskState_TASK_STATE_SUSPENDED || rCompletedUpload.RunnerTokenGen != 2 {
		t.Fatalf("expected SUSPENDED with RunnerTokenGen=2 after upload completed, got state=%v gen=%d",
			rCompletedUpload.Task.GetStatus().GetState(), rCompletedUpload.RunnerTokenGen)
	}

	// 6. Idempotent REVERT_ACTOR on an already-reverted SUSPENDED actor (where
	// Substrate returns FailedPrecondition) completes cleanly with SnapshotGen=2!
	if _, err := mem.Apply(ctx, pendingKey, func(cur *store.TaskRow, ts time.Time) (*store.TaskRow, error) {
		next := cur.Clone()
		st := next.LifecycleState()
		st.Actions = []lifecycle.Action{st.NextAction(lifecycle.ActionRevertActor, ts)}
		next.SetLifecycleState(st, ts)
		return next, nil
	}); err != nil {
		t.Fatalf("Apply(idempotent revert): %v", err)
	}
	if err := rec.ReconcileInline(ctx, pendingKey); err != nil {
		t.Fatalf("ReconcileInline(idempotent revert): %v", err)
	}
	rAfterRevert, _ := mem.GetTaskRow(ctx, pendingKey)
	if len(rAfterRevert.Actions) != 0 || rAfterRevert.RunnerTokenGen != 2 {
		t.Fatalf("expected idempotent REVERT_ACTOR completion preserving RunnerTokenGen=2, got %+v", rAfterRevert.State)
	}
}

func TestReconciler_MultiStepSequenceRenewsLeaseAcrossStepsAndLeaseLossAbortsCall(t *testing.T) {
	sub := substratetest.NewServer(t)
	mem := memory.NewStore()
	baseRT := reconciler.NewSubstrateRuntime(sub.Client(), reconciler.SubstrateRuntimeOptions{
		DefaultTaskImage: "us-docker.pkg.dev/acme/agent@sha256:abcdef",
	})

	// LeaseTTL is 120ms and RenewInterval is 35ms. Each of the 4 steps sleeps
	// 45ms (> RenewInterval, < LeaseTTL), totaling 180ms > LeaseTTL under a
	// single lease. Continuous cross-step lease renewal must keep the lease
	// alive for all 4 steps!
	prof := &v1alpha1.Profile{Image: "us-docker.pkg.dev/acme/agent@sha256:abcdef"}
	tmplName, _, err := controller.ResolveProfileTemplate("default", prof, "", "")
	if err != nil {
		t.Fatalf("ResolveProfileTemplate: %v", err)
	}

	var loseLeaseOnResume atomic.Bool
	resumeAborted := make(chan struct{})
	slowRT := &slowStepRuntime{
		RuntimeClient: baseRT,
		stepDelay:     45 * time.Millisecond,
		onResume: func(ctx context.Context, ns, actor string) error {
			if loseLeaseOnResume.Load() {
				<-ctx.Done()
				close(resumeAborted)
				return ctx.Err()
			}
			time.Sleep(45 * time.Millisecond)
			return baseRT.ResumeActor(ctx, ns, actor)
		},
	}

	// Use ModeQueue (without calling Run) with MinCalls=1 so ReconcileInline
	// exercises the live circuit-breaker recording path as well.
	rec := reconciler.New(mem, slowRT, reconciler.Config{
		Mode:          reconciler.ModeQueue,
		Transition:    stubTransition,
		LeaseTTL:      120 * time.Millisecond,
		RenewInterval: 35 * time.Millisecond,
		MutatingQPS:   -1,
		CircuitBreaker: reconciler.CircuitBreakerConfig{
			MinCalls:         1,
			FailureThreshold: 0.5,
		},
	})

	ctx := t.Context()
	key := store.TaskKey{Atespace: "default", Name: "multi-step-renew"}
	if _, err := mem.Apply(ctx, key, func(_ *store.TaskRow, ts time.Time) (*store.TaskRow, error) {
		st := lifecycle.State{
			Task: &v1alpha1.Task{
				Status: &v1alpha1.TaskStatus{
					State:            v1alpha1.TaskState_TASK_STATE_QUEUED,
					AteActorTemplate: tmplName,
				},
			},
		}
		st.Actions = []lifecycle.Action{
			st.NextAction(lifecycle.ActionEnsureTemplate, ts),
			st.NextAction(lifecycle.ActionCreateActor, ts),
			st.NextAction(lifecycle.ActionEnsureEgress, ts),
			st.NextAction(lifecycle.ActionResumeActor, ts),
		}
		row := &store.TaskRow{
			EffectiveProfile: prof,
			EgressRules: []*ateapipb.EgressRule{{
				Https: &ateapipb.HttpsRule{Hostnames: []string{"api.example.com"}},
			}},
		}
		row.SetLifecycleState(st, ts)
		return row, nil
	}); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	if err := rec.ReconcileInline(ctx, key); err != nil {
		t.Fatalf("ReconcileInline(multi-step): %v", err)
	}
	doneRow, err := mem.GetTaskRow(ctx, key)
	if err != nil || len(doneRow.Actions) != 0 || doneRow.ActorUID == "" {
		t.Fatalf("multi-step sequence did not complete under renewed lease: row=%+v err=%v", doneRow, err)
	}

	// Verify that caller context deadline expiration in ReconcileInline returns
	// context.DeadlineExceeded without tripping the circuit breaker.
	loseLeaseOnResume.Store(true)
	sub.SetActor(&ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "default", Name: "caller-timeout", Uid: "uid-ct"},
		Status:   &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
	})
	key2 := store.TaskKey{Atespace: "default", Name: "caller-timeout"}
	if _, err := mem.Apply(ctx, key2, func(_ *store.TaskRow, ts time.Time) (*store.TaskRow, error) {
		st := lifecycle.State{
			Task:     &v1alpha1.Task{Status: &v1alpha1.TaskStatus{State: v1alpha1.TaskState_TASK_STATE_QUEUED}},
			ActorUID: "uid-ct",
		}
		st.Actions = []lifecycle.Action{st.NextAction(lifecycle.ActionResumeActor, ts)}
		row := &store.TaskRow{}
		row.SetLifecycleState(st, ts)
		return row, nil
	}); err != nil {
		t.Fatalf("Apply(key2): %v", err)
	}
	shortCtx, cancelShort := context.WithTimeout(ctx, 40*time.Millisecond)
	defer cancelShort()
	err = rec.ReconcileInline(shortCtx, key2)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ReconcileInline(shortCtx) = %v, want context.DeadlineExceeded", err)
	}
	<-resumeAborted
	if rec.CircuitBreakerOpen() {
		t.Fatal("caller context timeout must not open circuit breaker")
	}
}

type slowStepRuntime struct {
	reconciler.RuntimeClient
	stepDelay time.Duration
	onResume  func(ctx context.Context, ns, actor string) error
}

func (s *slowStepRuntime) EnsureTemplate(ctx context.Context, ns, tmpl string, p *v1alpha1.Profile) error {
	time.Sleep(s.stepDelay)
	return s.RuntimeClient.EnsureTemplate(ctx, ns, tmpl, p)
}

func (s *slowStepRuntime) EnsureActor(ctx context.Context, ns, actor, tmpl, tag string) (string, error) {
	time.Sleep(s.stepDelay)
	return s.RuntimeClient.EnsureActor(ctx, ns, actor, tmpl, tag)
}

func (s *slowStepRuntime) EnsureActorEgress(ctx context.Context, ns, actor string, rules []*ateapipb.EgressRule) error {
	time.Sleep(s.stepDelay)
	return s.RuntimeClient.EnsureActorEgress(ctx, ns, actor, rules)
}

func (s *slowStepRuntime) ResumeActor(ctx context.Context, ns, actor string) error {
	if s.onResume != nil {
		return s.onResume(ctx, ns, actor)
	}
	time.Sleep(s.stepDelay)
	return s.RuntimeClient.ResumeActor(ctx, ns, actor)
}

type blockingRenewQueue struct {
	store.TaskQueue
	blockWorkerPrefix string
}

func (b *blockingRenewQueue) RenewLease(ctx context.Context, key store.TaskKey, leaseToken string, leaseTTL time.Duration) (string, error) {
	if len(leaseToken) >= len(b.blockWorkerPrefix) && leaseToken[:len(b.blockWorkerPrefix)] == b.blockWorkerPrefix {
		return "", status.Error(codes.Unavailable, "simulated DB partition on worker-1")
	}
	return b.TaskQueue.RenewLease(ctx, key, leaseToken, leaseTTL)
}

func TestReconciler_MultiWorkerLeaseFailoverFencingAndInlineContention(t *testing.T) {
	sub := substratetest.NewServer(t)
	mem := memory.NewStore()
	baseRT := reconciler.NewSubstrateRuntime(sub.Client(), reconciler.SubstrateRuntimeOptions{
		DefaultTaskImage: "us-docker.pkg.dev/acme/agent@sha256:abcdef",
	})

	sub.SetActor(&ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "default", Name: "failover-task", Uid: "uid-fo"},
		Status:   &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
	})

	// Worker 1 stalls on ResumeActor past LeaseTTL and cannot renew its lease;
	// Worker 2 must take over after LeaseTTL expires and complete the action,
	// while Worker 1's late result is fenced out via ErrLeaseLost!
	var callNum atomic.Int32
	w1Started := make(chan struct{})
	releaseW1 := make(chan struct{})
	w1Finished := make(chan struct{})

	rt := &customRuntime{
		RuntimeClient: baseRT,
		onResume: func(ctx context.Context, ns, actor string) error {
			n := callNum.Add(1)
			if n == 1 {
				close(w1Started)
				<-releaseW1
				return nil
			}
			return baseRT.ResumeActor(ctx, ns, actor)
		},
	}

	var transitionApplies atomic.Int32
	countingTransition := func(state lifecycle.State, in lifecycle.Input, now time.Time) (lifecycle.State, lifecycle.Output, error) {
		transitionApplies.Add(1)
		return stubTransition(state, in, now)
	}

	partitionedQ := &blockingRenewQueue{TaskQueue: mem, blockWorkerPrefix: "worker-1:"}
	w1 := reconciler.New(partitionedQ, rt, reconciler.Config{
		Mode:          reconciler.ModeQueue,
		WorkerID:      "worker-1",
		Transition:    countingTransition,
		LeaseTTL:      80 * time.Millisecond,
		RenewInterval: 25 * time.Millisecond,
		ActionTimeout: 2 * time.Second,
		PollInterval:  20 * time.Millisecond,
		MutatingQPS:   -1,
	})
	w2 := reconciler.New(mem, rt, reconciler.Config{
		Mode:          reconciler.ModeQueue,
		WorkerID:      "worker-2",
		Transition:    countingTransition,
		LeaseTTL:      500 * time.Millisecond,
		RenewInterval: 100 * time.Millisecond,
		PollInterval:  20 * time.Millisecond,
		MutatingQPS:   -1,
	})

	ctx := t.Context()
	key := store.TaskKey{Atespace: "default", Name: "failover-task"}
	if _, err := mem.Apply(ctx, key, func(_ *store.TaskRow, ts time.Time) (*store.TaskRow, error) {
		st := lifecycle.State{
			Task:     &v1alpha1.Task{Status: &v1alpha1.TaskStatus{State: v1alpha1.TaskState_TASK_STATE_QUEUED}},
			ActorUID: "uid-fo",
		}
		st.Actions = []lifecycle.Action{st.NextAction(lifecycle.ActionResumeActor, ts)}
		row := &store.TaskRow{}
		row.SetLifecycleState(st, ts)
		return row, nil
	}); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	ctxW1, cancelW1 := context.WithCancel(ctx)
	go func() {
		defer close(w1Finished)
		_ = w1.Run(ctxW1)
	}()
	<-w1Started

	// Start Worker 2 after Worker 1 has leased the row; once Worker 1's 80ms
	// lease expires, Worker 2 takes over and completes RESUME_ACTOR.
	ctxW2, cancelW2 := context.WithCancel(ctx)
	w2Done := make(chan struct{})
	go func() {
		defer close(w2Done)
		_ = w2.Run(ctxW2)
	}()
	t.Cleanup(func() {
		cancelW2()
		<-w2Done
	})

	waitFor(t, 2*time.Second, func() bool {
		r, err := mem.GetTaskRow(ctx, key)
		return err == nil && len(r.Actions) == 0 && r.LeaseOwner == ""
	})

	// Now unblock Worker 1's stalled ResumeActor and verify its late completion
	// is rejected by lease fencing (transitionApplies remains 1!).
	close(releaseW1)
	cancelW1()
	<-w1Finished
	if got := transitionApplies.Load(); got != 1 {
		t.Fatalf("transitionApplies = %d, want 1 (stale worker result was not fenced)", got)
	}

	// Also verify concurrent ReconcileInline callers contending on the same key
	// both return nil cleanly once the action completes without duplicate transitions.
	cancelW2()
	<-w2Done

	sub.SetActor(&ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "default", Name: "inline-contend", Uid: "uid-ic"},
		Status:   &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
	})
	inlineRT := &customRuntime{
		RuntimeClient: baseRT,
		onResume: func(ctx context.Context, ns, actor string) error {
			time.Sleep(60 * time.Millisecond)
			return baseRT.ResumeActor(ctx, ns, actor)
		},
	}
	inlineRec := reconciler.New(mem, inlineRT, reconciler.Config{
		Mode:        reconciler.ModeInline,
		Transition:  stubTransition,
		MutatingQPS: -1,
	})
	icKey := store.TaskKey{Atespace: "default", Name: "inline-contend"}
	if _, err := mem.Apply(ctx, icKey, func(_ *store.TaskRow, ts time.Time) (*store.TaskRow, error) {
		st := lifecycle.State{
			Task:     &v1alpha1.Task{Status: &v1alpha1.TaskStatus{State: v1alpha1.TaskState_TASK_STATE_QUEUED}},
			ActorUID: "uid-ic",
		}
		st.Actions = []lifecycle.Action{st.NextAction(lifecycle.ActionResumeActor, ts)}
		row := &store.TaskRow{}
		row.SetLifecycleState(st, ts)
		return row, nil
	}); err != nil {
		t.Fatalf("Apply(inline-contend): %v", err)
	}

	var wg sync.WaitGroup
	for range 3 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := inlineRec.ReconcileInline(ctx, icKey); err != nil {
				t.Errorf("concurrent ReconcileInline: %v", err)
			}
		}()
	}
	wg.Wait()
	rIC, err := mem.GetTaskRow(ctx, icKey)
	if err != nil || len(rIC.Actions) != 0 {
		t.Fatalf("inline-contend after concurrent ReconcileInline = %+v, %v", rIC, err)
	}
}

func TestReconciler_PostCallApplyLeaseFencingAndTenantFairness(t *testing.T) {
	sub := substratetest.NewServer(t)
	nowMu := sync.Mutex{}
	nowVal := time.Date(2026, 11, 16, 18, 0, 0, 0, time.UTC)
	getNow := func() time.Time {
		nowMu.Lock()
		defer nowMu.Unlock()
		return nowVal
	}
	advance := func(d time.Duration) {
		nowMu.Lock()
		nowVal = nowVal.Add(d)
		nowMu.Unlock()
	}

	mem := memory.NewStore(memory.WithClock(getNow))
	baseRT := reconciler.NewSubstrateRuntime(sub.Client(), reconciler.SubstrateRuntimeOptions{
		DefaultTaskImage: "us-docker.pkg.dev/acme/agent@sha256:abcdef",
	})

	// 1. Exercise DB-level HasActiveLease -> ErrLeaseLost inside Apply when a
	// worker stalls (e.g. GC pause) AFTER its Substrate RPC succeeds and right
	// before calling queue.Apply, while another worker steals the expired lease.
	sub.SetActor(&ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "default", Name: "post-rpc-stall", Uid: "uid-prs"},
		Status:   &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
	})
	prsKey := store.TaskKey{Atespace: "default", Name: "post-rpc-stall"}
	if _, err := mem.Apply(t.Context(), prsKey, func(_ *store.TaskRow, ts time.Time) (*store.TaskRow, error) {
		st := lifecycle.State{
			Task:     &v1alpha1.Task{Status: &v1alpha1.TaskStatus{State: v1alpha1.TaskState_TASK_STATE_QUEUED}},
			ActorUID: "uid-prs",
		}
		st.Actions = []lifecycle.Action{st.NextAction(lifecycle.ActionResumeActor, ts)}
		row := &store.TaskRow{}
		row.SetLifecycleState(st, ts)
		return row, nil
	}); err != nil {
		t.Fatalf("Apply(prsKey): %v", err)
	}

	var resumeCount atomic.Int32
	w1AboutToReturn := make(chan struct{})
	allowW1Return := make(chan struct{})
	rtStall := &customRuntime{
		RuntimeClient: baseRT,
		onResume: func(ctx context.Context, ns, actor string) error {
			if resumeCount.Add(1) == 1 {
				if err := baseRT.ResumeActor(ctx, ns, actor); err != nil {
					return err
				}
				close(w1AboutToReturn)
				<-allowW1Return
				return nil
			}
			return baseRT.ResumeActor(ctx, ns, actor)
		},
	}

	var transitionCalls atomic.Int32
	countTrans := func(state lifecycle.State, in lifecycle.Input, now time.Time) (lifecycle.State, lifecycle.Output, error) {
		transitionCalls.Add(1)
		return stubTransition(state, in, now)
	}

	// Set RenewInterval (10s) > LeaseTTL (1s) on w1 so runWithLeaseRenewal does
	// NOT self-abort locally; w1 returns a clean outcomeCompleted and hits
	// store.HasActiveLease inside queue.Apply!
	w1 := reconciler.New(mem, rtStall, reconciler.Config{
		Mode:          reconciler.ModeInline,
		WorkerID:      "w1-gc-stall",
		Transition:    countTrans,
		LeaseTTL:      time.Second,
		RenewInterval: 10 * time.Second,
		MutatingQPS:   -1,
		Clock:         getNow,
	})
	w2 := reconciler.New(mem, rtStall, reconciler.Config{
		Mode:        reconciler.ModeInline,
		WorkerID:    "w2-takeover",
		Transition:  countTrans,
		LeaseTTL:    30 * time.Second,
		MutatingQPS: -1,
		Clock:       getNow,
	})

	w1Done := make(chan error, 1)
	go func() {
		w1Done <- w1.ReconcileInline(t.Context(), prsKey)
	}()
	<-w1AboutToReturn

	// Advance store clock past w1's 1s LeaseTTL; w2 takes over and commits first!
	advance(2 * time.Second)
	if err := w2.ReconcileInline(t.Context(), prsKey); err != nil {
		t.Fatalf("w2 ReconcileInline: %v", err)
	}
	close(allowW1Return)
	if err := <-w1Done; err != nil {
		t.Fatalf("w1 ReconcileInline after lease loss = %v, want nil", err)
	}
	if got := transitionCalls.Load(); got != 1 {
		t.Fatalf("transitionCalls = %d, want 1 (DB-level HasActiveLease inside Apply failed to fence w1)", got)
	}

	// 2. Saturated tenant fairness: tenant-a has 20 queued tasks with
	// MaxInFlightPerAtespace = 2 blocked in flight; a newly submitted tenant-b
	// task completes immediately with zero head-of-line blocking!
	blockTenantA := make(chan struct{})
	tenantASlots := atomic.Int32{}
	rtFair := &customRuntime{
		RuntimeClient: baseRT,
		onResume: func(ctx context.Context, ns, actor string) error {
			if ns == "tenant-a" {
				tenantASlots.Add(1)
				select {
				case <-blockTenantA:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return baseRT.ResumeActor(ctx, ns, actor)
		},
	}

	qRec := reconciler.New(mem, rtFair, reconciler.Config{
		Mode:                   reconciler.ModeQueue,
		Transition:             stubTransition,
		MaxInFlightTasks:       64,
		MaxInFlightPerAtespace: 2,
		BatchSize:              4,
		PollInterval:           15 * time.Millisecond,
		MutatingQPS:            -1,
		Clock:                  getNow,
	})
	ctxQ, cancelQ := context.WithCancel(t.Context())
	qDone := make(chan struct{})
	go func() {
		defer close(qDone)
		_ = qRec.Run(ctxQ)
	}()
	defer func() {
		close(blockTenantA)
		cancelQ()
		<-qDone
	}()

	for i := range 20 {
		name := "ta-" + string(rune('a'+i))
		uid := "uid-" + name
		sub.SetActor(&ateapipb.Actor{
			Metadata: &ateapipb.ResourceMetadata{Atespace: "tenant-a", Name: name, Uid: uid},
			Status:   &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
		})
		k := store.TaskKey{Atespace: "tenant-a", Name: name}
		if _, err := mem.Apply(ctxQ, k, func(_ *store.TaskRow, ts time.Time) (*store.TaskRow, error) {
			st := lifecycle.State{
				Task:     &v1alpha1.Task{Status: &v1alpha1.TaskStatus{State: v1alpha1.TaskState_TASK_STATE_QUEUED}},
				ActorUID: uid,
			}
			st.Actions = []lifecycle.Action{st.NextAction(lifecycle.ActionResumeActor, ts)}
			row := &store.TaskRow{}
			row.SetLifecycleState(st, ts)
			return row, nil
		}); err != nil {
			t.Fatalf("Apply(tenant-a): %v", err)
		}
	}
	waitFor(t, 2*time.Second, func() bool { return tenantASlots.Load() == 2 })

	// Submit tenant-b while tenant-a still has 18 unleased due tasks waiting!
	sub.SetActor(&ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "tenant-b", Name: "tb-fast", Uid: "uid-tb-fast"},
		Status:   &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
	})
	tbKey := store.TaskKey{Atespace: "tenant-b", Name: "tb-fast"}
	if _, err := mem.Apply(ctxQ, tbKey, func(_ *store.TaskRow, ts time.Time) (*store.TaskRow, error) {
		st := lifecycle.State{
			Task:     &v1alpha1.Task{Status: &v1alpha1.TaskStatus{State: v1alpha1.TaskState_TASK_STATE_QUEUED}},
			ActorUID: "uid-tb-fast",
		}
		st.Actions = []lifecycle.Action{st.NextAction(lifecycle.ActionResumeActor, ts)}
		row := &store.TaskRow{}
		row.SetLifecycleState(st, ts)
		return row, nil
	}); err != nil {
		t.Fatalf("Apply(tenant-b): %v", err)
	}

	waitFor(t, 2*time.Second, func() bool {
		r, err := mem.GetTaskRow(ctxQ, tbKey)
		return err == nil && len(r.Actions) == 0
	})

	// 3. Reverted pending snapshot generation (monotonic LastAssignedGeneration=3
	// > LatestDurableSnapshotGeneration=1 after RevertActor) must NOT wedge
	// SUSPEND_ACTOR_SYNC or break idempotent REVERT_ACTOR!
	monoKey := store.TaskKey{Atespace: "default", Name: "mono-gen-actor"}
	prof := &v1alpha1.Profile{Image: "us-docker.pkg.dev/acme/agent@sha256:abcdef"}
	tmplName, _, err := controller.ResolveProfileTemplate("default", prof, "", "")
	if err != nil {
		t.Fatalf("ResolveProfileTemplate: %v", err)
	}
	sub.SetActor(&ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: "default", Name: "mono-gen-actor", Uid: "uid-mono"},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: "default", Name: tmplName},
		Status: &ateapipb.ActorStatus{
			State:                  ateapipb.ActorState_ACTOR_STATE_SUSPENDED,
			LastAssignedGeneration: 3, // monotonic generation retained after reverting pending gen 3 -> durable gen 1
			Snapshots: []*ateapipb.Snapshot{{
				Generation:      1,
				DurableSnapshot: &ateapipb.SnapshotStorage{State: ateapipb.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_COMPLETED},
			}},
		},
	})
	if _, err := mem.Apply(t.Context(), monoKey, func(_ *store.TaskRow, ts time.Time) (*store.TaskRow, error) {
		st := lifecycle.State{
			Task: &v1alpha1.Task{
				Status: &v1alpha1.TaskStatus{
					State:            v1alpha1.TaskState_TASK_STATE_SUSPENDING,
					AteActorTemplate: tmplName,
				},
			},
			ActorUID: "uid-mono",
		}
		rev := st.NextAction(lifecycle.ActionRevertActor, ts)
		sus := st.NextAction(lifecycle.ActionSuspendActorSync, ts)
		sus.SuspendReason = lifecycle.SuspendReasonUser
		st.Actions = []lifecycle.Action{rev, sus}
		row := &store.TaskRow{EffectiveProfile: prof}
		row.SetLifecycleState(st, ts)
		return row, nil
	}); err != nil {
		t.Fatalf("Apply(monoKey): %v", err)
	}
	if err := w2.ReconcileInline(t.Context(), monoKey); err != nil {
		t.Fatalf("ReconcileInline(monoKey): %v", err)
	}
	rMono, _ := mem.GetTaskRow(t.Context(), monoKey)
	if len(rMono.Actions) != 0 || rMono.Task.GetStatus().GetState() != v1alpha1.TaskState_TASK_STATE_SUSPENDED {
		t.Fatalf("expected REVERT_ACTOR + SUSPEND_ACTOR_SYNC with monotonic LastAssignedGeneration=3 to succeed, got %+v", rMono.State)
	}

	// 4. Same-template RUNNING foreign actor (e.g., shared ax-<hash> template
	// without matching orphan state): CREATE_ACTOR fails and subsequent
	// DELETE_ACTOR leaves the RUNNING foreign actor untouched!
	sameTmplForeignKey := store.TaskKey{Atespace: "default", Name: "same-tmpl-foreign"}
	sub.SetActor(&ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: "default", Name: "same-tmpl-foreign", Uid: "uid-stf"},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: "default", Name: tmplName},
		Status:        &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_RUNNING},
	})
	if _, err := mem.Apply(t.Context(), sameTmplForeignKey, func(_ *store.TaskRow, ts time.Time) (*store.TaskRow, error) {
		st := lifecycle.State{
			Task: &v1alpha1.Task{
				Status: &v1alpha1.TaskStatus{
					State:            v1alpha1.TaskState_TASK_STATE_QUEUED,
					AteActorTemplate: tmplName,
				},
			},
		}
		st.Actions = []lifecycle.Action{st.NextAction(lifecycle.ActionCreateActor, ts)}
		row := &store.TaskRow{EffectiveProfile: prof}
		row.SetLifecycleState(st, ts)
		return row, nil
	}); err != nil {
		t.Fatalf("Apply(same-tmpl-foreign): %v", err)
	}
	if err := w2.ReconcileInline(t.Context(), sameTmplForeignKey); err != nil {
		t.Fatalf("ReconcileInline(same-tmpl-foreign create): %v", err)
	}
	rSTF, _ := mem.GetTaskRow(t.Context(), sameTmplForeignKey)
	if rSTF.Task.GetStatus().GetState() != v1alpha1.TaskState_TASK_STATE_FAILED || rSTF.ActorUID != "" {
		t.Fatalf("expected RUNNING same-template foreign actor rejected on CREATE_ACTOR, got %+v", rSTF.State)
	}
	// Even if a task in QUEUED state (not yet FAILED) runs DELETE_ACTOR with
	// ActorUID == "" against that RUNNING same-template actor, it MUST NOT delete it!
	if _, err := mem.Apply(t.Context(), sameTmplForeignKey, func(cur *store.TaskRow, ts time.Time) (*store.TaskRow, error) {
		next := cur.Clone()
		st := next.LifecycleState()
		st.Task.Status.State = v1alpha1.TaskState_TASK_STATE_QUEUED
		st.Task.Metadata.DeleteTime = timestamppb.New(ts)
		st.Actions = []lifecycle.Action{st.NextAction(lifecycle.ActionDeleteActor, ts)}
		next.SetLifecycleState(st, ts)
		return next, nil
	}); err != nil {
		t.Fatalf("Apply(same-tmpl-foreign delete): %v", err)
	}
	if err := w2.ReconcileInline(t.Context(), sameTmplForeignKey); err != nil {
		t.Fatalf("ReconcileInline(same-tmpl-foreign delete): %v", err)
	}
	if gotForeign, ok := sub.Actor("default", "same-tmpl-foreign"); !ok || gotForeign.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_RUNNING {
		t.Fatalf("same-template RUNNING foreign actor was modified or deleted: %+v, ok=%v", gotForeign, ok)
	}

	// 5. Rolling-upgrade template skew (resolvedName != templateName and not yet
	// on Substrate) backs off transiently instead of failing the task, and
	// ReconcileInline surfaces Transition errors directly to callers!
	skewRT := reconciler.NewSubstrateRuntime(sub.Client(), reconciler.SubstrateRuntimeOptions{
		DefaultTaskImage: "us-docker.pkg.dev/acme/agent@sha256:abcdef",
		RunnerImage:      "us-docker.pkg.dev/acme/runner@sha256:999999",
	})
	skewRec := reconciler.New(mem, skewRT, reconciler.Config{
		Mode:           reconciler.ModeInline,
		Transition:     stubTransition,
		InitialBackoff: 50 * time.Millisecond,
		MutatingQPS:    -1,
		Clock:          getNow,
	})
	skewKey := store.TaskKey{Atespace: "default", Name: "rolling-skew-task"}
	if _, err := mem.Apply(t.Context(), skewKey, func(_ *store.TaskRow, ts time.Time) (*store.TaskRow, error) {
		st := lifecycle.State{
			Task: &v1alpha1.Task{
				Status: &v1alpha1.TaskStatus{
					State:            v1alpha1.TaskState_TASK_STATE_QUEUED,
					AteActorTemplate: "ax-deadbeef12345678",
				},
			},
		}
		st.Actions = []lifecycle.Action{st.NextAction(lifecycle.ActionEnsureTemplate, ts)}
		row := &store.TaskRow{EffectiveProfile: prof}
		row.SetLifecycleState(st, ts)
		return row, nil
	}); err != nil {
		t.Fatalf("Apply(skewKey): %v", err)
	}
	if err := skewRec.ReconcileInline(t.Context(), skewKey); err != nil {
		t.Fatalf("ReconcileInline(skewKey): %v", err)
	}
	rSkew, _ := mem.GetTaskRow(t.Context(), skewKey)
	if rSkew.Task.GetStatus().GetState() == v1alpha1.TaskState_TASK_STATE_FAILED || len(rSkew.Actions) != 1 || rSkew.Actions[0].Tries != 1 {
		t.Fatalf("expected rolling-upgrade template skew to back off transiently (Tries=1, state=QUEUED), got state=%v actions=%+v",
			rSkew.Task.GetStatus().GetState(), rSkew.Actions)
	}
	// Once DefaultMaxTemplateSkewWait elapses without the originating replica
	// publishing templateName, ActionEnsureTemplate fails permanently rather than
	// retrying forever.
	advance(reconciler.DefaultMaxTemplateSkewWait + time.Second)
	if err := skewRec.ReconcileInline(t.Context(), skewKey); err != nil {
		t.Fatalf("ReconcileInline(skewKey expired): %v", err)
	}
	rSkewExpired, _ := mem.GetTaskRow(t.Context(), skewKey)
	if rSkewExpired.Task.GetStatus().GetState() != v1alpha1.TaskState_TASK_STATE_FAILED || len(rSkewExpired.Actions) != 0 {
		t.Fatalf("expected template skew past DefaultMaxTemplateSkewWait to fail task, got state=%v actions=%+v",
			rSkewExpired.Task.GetStatus().GetState(), rSkewExpired.Actions)
	}

	// Verify ReconcileInline propagates Transition errors instead of swallowing them!
	errBoom := errors.New("transition boom")
	errRec := reconciler.New(mem, baseRT, reconciler.Config{
		Mode: reconciler.ModeInline,
		Transition: func(lifecycle.State, lifecycle.Input, time.Time) (lifecycle.State, lifecycle.Output, error) {
			return lifecycle.State{}, lifecycle.Output{}, errBoom
		},
		MutatingQPS: -1,
		Clock:       getNow,
	})
	advance(time.Second)
	errKey := store.TaskKey{Atespace: "default", Name: "err-prop-task"}
	if _, err := mem.Apply(t.Context(), errKey, func(_ *store.TaskRow, ts time.Time) (*store.TaskRow, error) {
		st := lifecycle.State{
			Task: &v1alpha1.Task{
				Status: &v1alpha1.TaskStatus{
					State:            v1alpha1.TaskState_TASK_STATE_QUEUED,
					AteActorTemplate: tmplName,
				},
			},
		}
		st.Actions = []lifecycle.Action{st.NextAction(lifecycle.ActionEnsureTemplate, ts)}
		row := &store.TaskRow{EffectiveProfile: prof}
		row.SetLifecycleState(st, ts)
		return row, nil
	}); err != nil {
		t.Fatalf("Apply(errKey): %v", err)
	}
	if gotErr := errRec.ReconcileInline(t.Context(), errKey); !errors.Is(gotErr, errBoom) {
		t.Fatalf("ReconcileInline(transition error) = %v, want %v", gotErr, errBoom)
	}

	// 6. ActionResumeActor against a CRASHED actor routes through
	// ObservedActorInput{CRASHED} -> REVERT_ACTOR + RESUME_ACTOR instead of
	// failing permanently with PROVISIONING_FAILED!
	crashedResumeKey := store.TaskKey{Atespace: "default", Name: "crashed-on-resume"}
	sub.SetActor(&ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: "default", Name: "crashed-on-resume", Uid: "uid-cor"},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: "default", Name: tmplName},
		Status: &ateapipb.ActorStatus{
			State: ateapipb.ActorState_ACTOR_STATE_CRASHED,
			Crash: &ateapipb.ActorCrash{Message: "wakeup probe failed"},
		},
	})
	if _, err := mem.Apply(t.Context(), crashedResumeKey, func(_ *store.TaskRow, ts time.Time) (*store.TaskRow, error) {
		st := lifecycle.State{
			Task: &v1alpha1.Task{
				Status: &v1alpha1.TaskStatus{
					State:            v1alpha1.TaskState_TASK_STATE_QUEUED,
					Attempt:          1,
					AteActorTemplate: tmplName,
				},
			},
			ActorUID: "uid-cor",
		}
		st.Actions = []lifecycle.Action{st.NextAction(lifecycle.ActionResumeActor, ts)}
		row := &store.TaskRow{EffectiveProfile: prof}
		row.SetLifecycleState(st, ts)
		return row, nil
	}); err != nil {
		t.Fatalf("Apply(crashed-on-resume): %v", err)
	}
	if err := w2.ReconcileInline(t.Context(), crashedResumeKey); err != nil {
		t.Fatalf("ReconcileInline(crashed-on-resume): %v", err)
	}
	rCOR, _ := mem.GetTaskRow(t.Context(), crashedResumeKey)
	if rCOR.Task.GetStatus().GetState() != v1alpha1.TaskState_TASK_STATE_QUEUED || rCOR.Task.GetStatus().GetAttempt() != 2 || len(rCOR.Actions) != 0 {
		t.Fatalf("expected CRASHED on RESUME_ACTOR to recover via REVERT_ACTOR+RESUME_ACTOR (attempt=2), got state=%v attempt=%d actions=%+v",
			rCOR.Task.GetStatus().GetState(), rCOR.Task.GetStatus().GetAttempt(), rCOR.Actions)
	}

	// 7. Uncommitted golden-snapshot orphan adoption & cleanup when Actor.source_tag is empty:
	// CreateActor creates an actor with SNAPSHOT_OWNER_TAG golden snapshot;
	// EnsureTaskActor adopts it idempotently, and stalledHead spin guard backs off a no-op Transition!
	_, tmpl, _ := controller.ResolveProfileTemplate("default", prof, "", "")
	if _, err := sub.Client().EnsureActorTemplate(t.Context(), tmpl); err != nil {
		t.Fatalf("EnsureActorTemplate: %v", err)
	}
	createdOrphan, createErr := sub.CreateActor(t.Context(), &ateapipb.CreateActorRequest{
		Actor: &ateapipb.Actor{
			Metadata:      &ateapipb.ResourceMetadata{Atespace: "default", Name: "golden-orphan"},
			ActorTemplate: &ateapipb.ObjectRef{Atespace: "default", Name: tmplName},
		},
	})
	if createErr != nil {
		t.Fatalf("CreateActor(golden-orphan): %v", createErr)
	}
	orphanKey := store.TaskKey{Atespace: "default", Name: "golden-orphan"}
	if _, err := mem.Apply(t.Context(), orphanKey, func(_ *store.TaskRow, ts time.Time) (*store.TaskRow, error) {
		st := lifecycle.State{
			Task: &v1alpha1.Task{
				Status: &v1alpha1.TaskStatus{
					State:            v1alpha1.TaskState_TASK_STATE_QUEUED,
					AteActorTemplate: tmplName,
				},
			},
		}
		st.Actions = []lifecycle.Action{st.NextAction(lifecycle.ActionCreateActor, ts)}
		row := &store.TaskRow{EffectiveProfile: prof}
		row.SetLifecycleState(st, ts)
		return row, nil
	}); err != nil {
		t.Fatalf("Apply(golden-orphan): %v", err)
	}
	if err := w2.ReconcileInline(t.Context(), orphanKey); err != nil {
		t.Fatalf("ReconcileInline(golden-orphan): %v", err)
	}
	rOrphan, _ := mem.GetTaskRow(t.Context(), orphanKey)
	if rOrphan.ActorUID != createdOrphan.GetMetadata().GetUid() || len(rOrphan.Actions) != 0 {
		t.Fatalf("expected golden-snapshot orphan adopted with UID %q, got %+v", createdOrphan.GetMetadata().GetUid(), rOrphan.State)
	}

	// Defensive stalledHead spin guard: a no-op Transition leaving Actions[0].ID
	// unchanged backs off after 1 attempt instead of looping forever!
	noopCalls := atomic.Int32{}
	noopRec := reconciler.New(mem, baseRT, reconciler.Config{
		Mode: reconciler.ModeInline,
		Transition: func(st lifecycle.State, _ lifecycle.Input, _ time.Time) (lifecycle.State, lifecycle.Output, error) {
			noopCalls.Add(1)
			return st, lifecycle.Output{}, nil
		},
		InitialBackoff: 50 * time.Millisecond,
		MutatingQPS:    -1,
		Clock:          getNow,
	})
	stallKey := store.TaskKey{Atespace: "default", Name: "stall-guard-task"}
	if _, err := mem.Apply(t.Context(), stallKey, func(_ *store.TaskRow, ts time.Time) (*store.TaskRow, error) {
		st := lifecycle.State{
			Task: &v1alpha1.Task{
				Status: &v1alpha1.TaskStatus{
					State:            v1alpha1.TaskState_TASK_STATE_QUEUED,
					AteActorTemplate: tmplName,
				},
			},
		}
		st.Actions = []lifecycle.Action{st.NextAction(lifecycle.ActionEnsureTemplate, ts)}
		row := &store.TaskRow{EffectiveProfile: prof}
		row.SetLifecycleState(st, ts)
		return row, nil
	}); err != nil {
		t.Fatalf("Apply(stallKey): %v", err)
	}
	if err := noopRec.ReconcileInline(t.Context(), stallKey); err != nil {
		t.Fatalf("ReconcileInline(stallKey): %v", err)
	}
	if got := noopCalls.Load(); got != 1 {
		t.Fatalf("stalledHead noopCalls = %d, want 1", got)
	}
	rStall, _ := mem.GetTaskRow(t.Context(), stallKey)
	if len(rStall.Actions) != 1 || rStall.Actions[0].Tries != 1 {
		t.Fatalf("expected stalledHead to back off with Tries=1, got %+v", rStall.Actions)
	}
}

type hookRuntime struct {
	reconciler.RuntimeClient
	onDeleteActor       func(ctx context.Context, ns, actor, expectedUID string) error
	onCreateSnapshotTag func(ctx context.Context, ns, actor, tag string) error
}

func (h *hookRuntime) DeleteActor(ctx context.Context, ns, actor, expectedUID string) error {
	if h.onDeleteActor != nil {
		return h.onDeleteActor(ctx, ns, actor, expectedUID)
	}
	return h.RuntimeClient.DeleteActor(ctx, ns, actor, expectedUID)
}

func (h *hookRuntime) CreateSnapshotTag(ctx context.Context, ns, actor, tag string) error {
	if h.onCreateSnapshotTag != nil {
		return h.onCreateSnapshotTag(ctx, ns, actor, tag)
	}
	return h.RuntimeClient.CreateSnapshotTag(ctx, ns, actor, tag)
}

func TestReconciler_InlineConcurrentCancelDuringBackoffAndKindSaturation(t *testing.T) {
	t.Run("concurrent ReconcileInline survives earlier caller context cancellation mid-backoff", func(t *testing.T) {
		sub := substratetest.NewServer(t)
		mem := memory.NewStore() // real advancing clock
		baseRT := reconciler.NewSubstrateRuntime(sub.Client(), reconciler.SubstrateRuntimeOptions{})

		var (
			deleteAttempts atomic.Int32
			firstBackoffCh = make(chan struct{})
		)
		rt := &hookRuntime{
			RuntimeClient: baseRT,
			onDeleteActor: func(ctx context.Context, ns, actor, expectedUID string) error {
				if deleteAttempts.Add(1) == 1 {
					return substrate.NewHealthyRetryError(substrate.ErrResourceDeleting, "actor %s/%s is still deleting", ns, actor)
				}
				return baseRT.DeleteActor(ctx, ns, actor, expectedUID)
			},
		}

		rec := reconciler.New(mem, rt, reconciler.Config{
			Mode:           reconciler.ModeInline,
			Transition:     stubTransition,
			InitialBackoff: 90 * time.Millisecond,
			MaxBackoff:     120 * time.Millisecond,
			MutatingQPS:    -1,
		})

		ctx := t.Context()
		if err := sub.Client().EnsureAtespace(ctx, "default"); err != nil {
			t.Fatalf("EnsureAtespace: %v", err)
		}
		tmpl := substrate.BuildActorTemplate("default", "ax-del-test", "us-docker.pkg.dev/p/img@sha256:111", nil, nil, "", nil)
		if _, err := sub.Client().EnsureActorTemplate(ctx, tmpl); err != nil {
			t.Fatalf("EnsureActorTemplate: %v", err)
		}
		actor, err := sub.Client().EnsureActor(ctx, "default", "inline-del", "default", "ax-del-test")
		if err != nil {
			t.Fatalf("EnsureActor: %v", err)
		}

		delKey := store.TaskKey{Atespace: "default", Name: "inline-del"}
		if _, err := mem.Apply(ctx, delKey, func(_ *store.TaskRow, ts time.Time) (*store.TaskRow, error) {
			st := lifecycle.State{
				Task: &v1alpha1.Task{
					Metadata: &v1alpha1.ObjectMeta{DeleteTime: timestamppb.New(ts)},
					Status: &v1alpha1.TaskStatus{
						State:            v1alpha1.TaskState_TASK_STATE_QUEUED,
						AteActorTemplate: "ax-del-test",
					},
				},
				ActorUID: actor.GetMetadata().GetUid(),
			}
			st.Actions = []lifecycle.Action{st.NextAction(lifecycle.ActionDeleteActor, ts)}
			row := &store.TaskRow{}
			row.SetLifecycleState(st, ts)
			return row, nil
		}); err != nil {
			t.Fatalf("Apply(delKey): %v", err)
		}

		ctx1, cancel1 := context.WithCancel(ctx)
		defer cancel1()
		err1Ch := make(chan error, 1)
		go func() {
			err1Ch <- rec.ReconcileInline(ctx1, delKey)
		}()

		// Wait until Caller 1 has committed BackoffAction (Tries == 1, LeaseOwner == "").
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			row, getErr := mem.GetTaskRow(ctx, delKey)
			if getErr == nil && len(row.Actions) == 1 && row.Actions[0].Tries == 1 && row.LeaseOwner == "" {
				close(firstBackoffCh)
				break
			}
			time.Sleep(2 * time.Millisecond)
		}
		select {
		case <-firstBackoffCh:
		default:
			t.Fatal("timed out waiting for Caller 1 to enter transient backoff")
		}

		// Launch Caller 2 while the row is unleased and waiting on NotBefore, then
		// cancel Caller 1 mid-backoff. Caller 2 must NOT return nil early!
		ctx2, cancel2 := context.WithTimeout(ctx, 3*time.Second)
		defer cancel2()
		err2Ch := make(chan error, 1)
		go func() {
			err2Ch <- rec.ReconcileInline(ctx2, delKey)
		}()

		time.Sleep(10 * time.Millisecond)
		cancel1()
		if err1 := <-err1Ch; !errors.Is(err1, context.Canceled) {
			t.Fatalf("Caller 1 ReconcileInline = %v, want context.Canceled", err1)
		}

		if err2 := <-err2Ch; err2 != nil {
			t.Fatalf("Caller 2 ReconcileInline = %v, want nil", err2)
		}
		if _, getErr := mem.GetTaskRow(ctx, delKey); !errors.Is(getErr, store.ErrNotFound) {
			t.Fatalf("expected task row deleted before Caller 2 returned nil, got err=%v", getErr)
		}
		if _, exists := sub.Actor("default", "inline-del"); exists {
			t.Fatal("expected actor deleted from Substrate")
		}
	})

	t.Run("saturated ActionKind bucket does not head-of-line block other ActionKinds across atespaces", func(t *testing.T) {
		sub := substratetest.NewServer(t)
		mem := memory.NewStore()
		baseRT := reconciler.NewSubstrateRuntime(sub.Client(), reconciler.SubstrateRuntimeOptions{})

		var (
			inFlightTags atomic.Int32
			releaseTags  = make(chan struct{})
		)
		rt := &hookRuntime{
			RuntimeClient: baseRT,
			onCreateSnapshotTag: func(ctx context.Context, ns, actor, tag string) error {
				inFlightTags.Add(1)
				select {
				case <-releaseTags:
				case <-ctx.Done():
					return ctx.Err()
				}
				return baseRT.CreateSnapshotTag(ctx, ns, actor, tag)
			},
		}

		rec := reconciler.New(mem, rt, reconciler.Config{
			Mode:         reconciler.ModeQueue,
			Transition:   stubTransition,
			PollInterval: 20 * time.Millisecond,
			BatchSize:    2,
			ActionKindLimits: map[lifecycle.ActionKind]int{
				lifecycle.ActionCreateTag: 2,
			},
			MutatingQPS: -1,
		})

		runCtx, cancelRun := context.WithCancel(t.Context())
		runDone := make(chan error, 1)
		go func() {
			runDone <- rec.Run(runCtx)
		}()
		defer func() {
			cancelRun()
			<-runDone
		}()

		ctx := t.Context()
		// Enqueue 8 CREATE_TAG tasks across 8 distinct atespaces so per-atespace
		// limits (64) are nowhere near saturation while CREATE_TAG (2) is full.
		for i := range 8 {
			ns := fmt.Sprintf("tag-ns-%d", i)
			name := fmt.Sprintf("tag-task-%d", i)
			if err := sub.Client().EnsureAtespace(ctx, ns); err != nil {
				t.Fatalf("EnsureAtespace(%s): %v", ns, err)
			}
			tmpl := substrate.BuildActorTemplate(ns, "ax-tmpl", "us-docker.pkg.dev/p/img@sha256:111", nil, nil, "", nil)
			if _, err := sub.Client().EnsureActorTemplate(ctx, tmpl); err != nil {
				t.Fatalf("EnsureActorTemplate(%s): %v", ns, err)
			}
			actor, err := sub.Client().EnsureActor(ctx, ns, name, ns, "ax-tmpl")
			if err != nil {
				t.Fatalf("EnsureActor(%s/%s): %v", ns, name, err)
			}
			if _, err := mem.Apply(ctx, store.TaskKey{Atespace: ns, Name: name}, func(_ *store.TaskRow, ts time.Time) (*store.TaskRow, error) {
				st := lifecycle.State{
					Task: &v1alpha1.Task{
						Status: &v1alpha1.TaskStatus{
							State:            v1alpha1.TaskState_TASK_STATE_SUSPENDED,
							AteActorTemplate: "ax-tmpl",
						},
					},
					ActorUID: actor.GetMetadata().GetUid(),
				}
				tagAct := st.NextAction(lifecycle.ActionCreateTag, ts)
				tagAct.Tag = fmt.Sprintf("snap-%d", i)
				st.Actions = []lifecycle.Action{tagAct}
				row := &store.TaskRow{}
				row.SetLifecycleState(st, ts)
				return row, nil
			}); err != nil {
				t.Fatalf("Apply(%s/%s): %v", ns, name, err)
			}
		}

		// Wait until both CREATE_TAG slots are occupied.
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) && inFlightTags.Load() < 2 {
			time.Sleep(5 * time.Millisecond)
		}
		if got := inFlightTags.Load(); got != 2 {
			t.Fatalf("inFlightTags = %d, want 2", got)
		}

		// Now enqueue a RESUME_ACTOR task in a new atespace behind the 6 due
		// CREATE_TAG tasks; LeaseFilter must skip CREATE_TAG so RESUME_ACTOR runs
		// immediately without head-of-line blocking!
		if err := sub.Client().EnsureAtespace(ctx, "resume-ns"); err != nil {
			t.Fatalf("EnsureAtespace(resume-ns): %v", err)
		}
		resTmpl := substrate.BuildActorTemplate("resume-ns", "ax-tmpl", "us-docker.pkg.dev/p/img@sha256:111", nil, nil, "", nil)
		if _, err := sub.Client().EnsureActorTemplate(ctx, resTmpl); err != nil {
			t.Fatalf("EnsureActorTemplate(resume-ns): %v", err)
		}
		resActor, err := sub.Client().EnsureActor(ctx, "resume-ns", "fast-resume", "resume-ns", "ax-tmpl")
		if err != nil {
			t.Fatalf("EnsureActor(fast-resume): %v", err)
		}
		resKey := store.TaskKey{Atespace: "resume-ns", Name: "fast-resume"}
		if _, err := mem.Apply(ctx, resKey, func(_ *store.TaskRow, ts time.Time) (*store.TaskRow, error) {
			st := lifecycle.State{
				Task: &v1alpha1.Task{
					Status: &v1alpha1.TaskStatus{
						State:            v1alpha1.TaskState_TASK_STATE_QUEUED,
						AteActorTemplate: "ax-tmpl",
					},
				},
				ActorUID: resActor.GetMetadata().GetUid(),
			}
			st.Actions = []lifecycle.Action{st.NextAction(lifecycle.ActionResumeActor, ts)}
			row := &store.TaskRow{}
			row.SetLifecycleState(st, ts)
			return row, nil
		}); err != nil {
			t.Fatalf("Apply(fast-resume): %v", err)
		}

		resDeadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(resDeadline) {
			rRes, getErr := mem.GetTaskRow(ctx, resKey)
			if getErr == nil && len(rRes.Actions) == 0 {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		rRes, err := mem.GetTaskRow(ctx, resKey)
		if err != nil || len(rRes.Actions) != 0 {
			t.Fatalf("RESUME_ACTOR was head-of-line blocked by saturated CREATE_TAG bucket: row=%+v err=%v", rRes, err)
		}
		liveRes, ok := sub.Actor("resume-ns", "fast-resume")
		if !ok || liveRes.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_RUNNING {
			t.Fatalf("expected fast-resume RUNNING while CREATE_TAG bucket saturated, got %+v", liveRes)
		}

		// Unblock CREATE_TAG and verify all 8 tag tasks drain cleanly.
		close(releaseTags)
		drainDeadline := time.Now().Add(3 * time.Second)
		for i := range 8 {
			tk := store.TaskKey{Atespace: fmt.Sprintf("tag-ns-%d", i), Name: fmt.Sprintf("tag-task-%d", i)}
			for time.Now().Before(drainDeadline) {
				tr, getErr := mem.GetTaskRow(ctx, tk)
				if getErr == nil && len(tr.Actions) == 0 {
					break
				}
				time.Sleep(5 * time.Millisecond)
			}
			tr, getErr := mem.GetTaskRow(ctx, tk)
			if getErr != nil || len(tr.Actions) != 0 {
				t.Fatalf("tag task %v did not drain: row=%+v err=%v", tk, tr, getErr)
			}
		}
	})
}

type flakyRenewQueue struct {
	store.TaskQueue
	failRenew atomic.Bool
}

func (q *flakyRenewQueue) RenewLease(ctx context.Context, key store.TaskKey, leaseToken string, leaseTTL time.Duration) (string, error) {
	if q.failRenew.Load() {
		return "", errors.New("simulated transient db outage during RenewLease")
	}
	return q.TaskQueue.RenewLease(ctx, key, leaseToken, leaseTTL)
}

type preconditionRaceRuntime struct {
	reconciler.RuntimeClient
	suspendErrOnce atomic.Bool
	resumeErrOnce  atomic.Bool
	tagErrOnce     atomic.Bool
	onGetActor     func(ctx context.Context, ns, actor string) (*ateapipb.Actor, error)
	onResume       func(ctx context.Context, ns, actor string) error
}

func (p *preconditionRaceRuntime) SuspendActorSync(ctx context.Context, ns, actor string) (int32, error) {
	if p.suspendErrOnce.CompareAndSwap(true, false) {
		return 0, status.Error(codes.FailedPrecondition, "actor was RESUMING moments ago")
	}
	return p.RuntimeClient.SuspendActorSync(ctx, ns, actor)
}

func (p *preconditionRaceRuntime) ResumeActor(ctx context.Context, ns, actor string) error {
	if p.onResume != nil {
		return p.onResume(ctx, ns, actor)
	}
	if p.resumeErrOnce.CompareAndSwap(true, false) {
		return status.Error(codes.FailedPrecondition, "actor was SUSPENDING moments ago")
	}
	return p.RuntimeClient.ResumeActor(ctx, ns, actor)
}

func (p *preconditionRaceRuntime) CreateSnapshotTag(ctx context.Context, ns, actor, tag string) error {
	if p.tagErrOnce.CompareAndSwap(true, false) {
		return status.Error(codes.FailedPrecondition, "actor was SUSPENDING moments ago")
	}
	return p.RuntimeClient.CreateSnapshotTag(ctx, ns, actor, tag)
}

func (p *preconditionRaceRuntime) GetActor(ctx context.Context, ns, actor string) (*ateapipb.Actor, error) {
	if p.onGetActor != nil {
		return p.onGetActor(ctx, ns, actor)
	}
	return p.RuntimeClient.GetActor(ctx, ns, actor)
}

func TestReconciler_TransitionalPreconditionRacesLeaseRecoveryAndDeletingOrphan(t *testing.T) {
	ctx := t.Context()

	t.Run("FailedPrecondition races settling into steady state retry cleanly without failing tasks", func(t *testing.T) {
		sub := substratetest.NewServer(t)
		nowMu := sync.Mutex{}
		nowVal := time.Date(2026, 11, 16, 18, 0, 0, 0, time.UTC)
		getNow := func() time.Time {
			nowMu.Lock()
			defer nowMu.Unlock()
			return nowVal
		}
		advance := func(d time.Duration) {
			nowMu.Lock()
			nowVal = nowVal.Add(d)
			nowMu.Unlock()
		}

		mem := memory.NewStore(memory.WithClock(getNow))
		baseRT := reconciler.NewSubstrateRuntime(sub.Client(), reconciler.SubstrateRuntimeOptions{})
		raceRT := &preconditionRaceRuntime{RuntimeClient: baseRT}
		raceRT.suspendErrOnce.Store(true)
		raceRT.resumeErrOnce.Store(true)
		raceRT.tagErrOnce.Store(true)

		rec := reconciler.New(mem, raceRT, reconciler.Config{
			Mode:           reconciler.ModeInline,
			Transition:     stubTransition,
			InitialBackoff: 10 * time.Millisecond,
			MaxBackoff:     20 * time.Millisecond,
			MutatingQPS:    -1,
		})

		if err := sub.Client().EnsureAtespace(ctx, "default"); err != nil {
			t.Fatalf("EnsureAtespace: %v", err)
		}
		tmpl := substrate.BuildActorTemplate("default", "ax-race", "us-docker.pkg.dev/p/img@sha256:111", nil, nil, "", nil)
		if _, err := sub.Client().EnsureActorTemplate(ctx, tmpl); err != nil {
			t.Fatalf("EnsureActorTemplate: %v", err)
		}
		actor, err := sub.Client().EnsureActor(ctx, "default", "race-actor", "default", "ax-race")
		if err != nil {
			t.Fatalf("EnsureActor: %v", err)
		}
		if _, _, err := sub.Client().ResumeActor(ctx, "default", "race-actor"); err != nil {
			t.Fatalf("ResumeActor: %v", err)
		}

		// 1. SUSPEND_ACTOR_SYNC hits FailedPrecondition while GetActor sees RUNNING -> backs off and succeeds on retry.
		key := store.TaskKey{Atespace: "default", Name: "race-actor"}
		if _, err := mem.Apply(ctx, key, func(_ *store.TaskRow, ts time.Time) (*store.TaskRow, error) {
			st := lifecycle.State{
				Task: &v1alpha1.Task{
					Status: &v1alpha1.TaskStatus{
						State:            v1alpha1.TaskState_TASK_STATE_SUSPENDING,
						AteActorTemplate: "ax-race",
					},
				},
				ActorUID: actor.GetMetadata().GetUid(),
			}
			sus := st.NextAction(lifecycle.ActionSuspendActorSync, ts)
			sus.SuspendReason = lifecycle.SuspendReasonUser
			tag := st.NextAction(lifecycle.ActionCreateTag, ts)
			tag.Tag = "fork-tag-1"
			res := st.NextAction(lifecycle.ActionResumeActor, ts)
			st.Actions = []lifecycle.Action{sus, tag, res}
			row := &store.TaskRow{}
			row.SetLifecycleState(st, ts)
			return row, nil
		}); err != nil {
			t.Fatalf("Apply: %v", err)
		}

		// Attempt 1: SUSPEND_ACTOR_SYNC fails with FailedPrecondition (GetActor sees RUNNING) -> backs off without failing.
		if err := rec.ReconcileInline(ctx, key); err != nil {
			t.Fatalf("ReconcileInline(suspend precondition): %v", err)
		}
		r1, _ := mem.GetTaskRow(ctx, key)
		if r1.Task.GetStatus().GetState() == v1alpha1.TaskState_TASK_STATE_FAILED || len(r1.Actions) != 3 {
			t.Fatalf("expected SUSPEND_ACTOR_SYNC retry without failing, got state=%v actions=%+v", r1.Task.GetStatus().GetState(), r1.Actions)
		}

		// Attempt 2: SUSPEND_ACTOR_SYNC succeeds; CREATE_TAG hits FailedPrecondition (GetActor sees SUSPENDED) -> backs off!
		advance(time.Second)
		if err := rec.ReconcileInline(ctx, key); err != nil {
			t.Fatalf("ReconcileInline(tag precondition): %v", err)
		}
		r2, _ := mem.GetTaskRow(ctx, key)
		if r2.Task.GetStatus().GetState() == v1alpha1.TaskState_TASK_STATE_FAILED || len(r2.Actions) != 2 || r2.Actions[0].Kind != lifecycle.ActionCreateTag {
			t.Fatalf("expected CREATE_TAG retry without failing, got state=%v actions=%+v", r2.Task.GetStatus().GetState(), r2.Actions)
		}

		// Attempt 3: CREATE_TAG succeeds; RESUME_ACTOR hits FailedPrecondition (GetActor sees SUSPENDED) -> backs off!
		advance(time.Second)
		if err := rec.ReconcileInline(ctx, key); err != nil {
			t.Fatalf("ReconcileInline(resume precondition): %v", err)
		}
		r3, _ := mem.GetTaskRow(ctx, key)
		if r3.Task.GetStatus().GetState() == v1alpha1.TaskState_TASK_STATE_FAILED || len(r3.Actions) != 1 || r3.Actions[0].Kind != lifecycle.ActionResumeActor {
			t.Fatalf("expected RESUME_ACTOR retry without failing, got state=%v actions=%+v", r3.Task.GetStatus().GetState(), r3.Actions)
		}

		// Attempt 4: RESUME_ACTOR succeeds cleanly!
		advance(time.Second)
		if err := rec.ReconcileInline(ctx, key); err != nil {
			t.Fatalf("ReconcileInline(resume complete): %v", err)
		}
		r4, _ := mem.GetTaskRow(ctx, key)
		if len(r4.Actions) != 0 || r4.Task.GetStatus().GetState() == v1alpha1.TaskState_TASK_STATE_FAILED {
			t.Fatalf("expected all 3 actions completed cleanly, got state=%v actions=%+v", r4.Task.GetStatus().GetState(), r4.Actions)
		}
	})

	t.Run("ReconcileInline recovers from lease loss during RenewLease outage instead of returning nil prematurely", func(t *testing.T) {
		sub := substratetest.NewServer(t)
		flakyQ := &flakyRenewQueue{TaskQueue: memory.NewStore()}
		flakyQ.failRenew.Store(true)

		baseRT := reconciler.NewSubstrateRuntime(sub.Client(), reconciler.SubstrateRuntimeOptions{})
		var resumeCalls atomic.Int32
		rt := &preconditionRaceRuntime{
			RuntimeClient: baseRT,
			onResume: func(rCtx context.Context, ns, actor string) error {
				if resumeCalls.Add(1) == 1 {
					// Block until lease expiration cancels callCtx, then heal DB!
					<-rCtx.Done()
					flakyQ.failRenew.Store(false)
					return rCtx.Err()
				}
				return baseRT.ResumeActor(rCtx, ns, actor)
			},
		}

		rec := reconciler.New(flakyQ, rt, reconciler.Config{
			Mode:          reconciler.ModeInline,
			Transition:    stubTransition,
			LeaseTTL:      60 * time.Millisecond,
			RenewInterval: 20 * time.Millisecond,
			MutatingQPS:   -1,
		})

		if err := sub.Client().EnsureAtespace(ctx, "default"); err != nil {
			t.Fatalf("EnsureAtespace: %v", err)
		}
		tmpl := substrate.BuildActorTemplate("default", "ax-lease", "us-docker.pkg.dev/p/img@sha256:111", nil, nil, "", nil)
		if _, err := sub.Client().EnsureActorTemplate(ctx, tmpl); err != nil {
			t.Fatalf("EnsureActorTemplate: %v", err)
		}
		actor, err := sub.Client().EnsureActor(ctx, "default", "lease-recover", "default", "ax-lease")
		if err != nil {
			t.Fatalf("EnsureActor: %v", err)
		}
		key := store.TaskKey{Atespace: "default", Name: "lease-recover"}
		if _, err := flakyQ.Apply(ctx, key, func(_ *store.TaskRow, ts time.Time) (*store.TaskRow, error) {
			st := lifecycle.State{
				Task: &v1alpha1.Task{
					Status: &v1alpha1.TaskStatus{
						State:            v1alpha1.TaskState_TASK_STATE_QUEUED,
						AteActorTemplate: "ax-lease",
					},
				},
				ActorUID: actor.GetMetadata().GetUid(),
			}
			st.Actions = []lifecycle.Action{st.NextAction(lifecycle.ActionResumeActor, ts)}
			row := &store.TaskRow{}
			row.SetLifecycleState(st, ts)
			return row, nil
		}); err != nil {
			t.Fatalf("Apply: %v", err)
		}

		if err := rec.ReconcileInline(ctx, key); err != nil {
			t.Fatalf("ReconcileInline(after lease loss): %v", err)
		}
		rowAfter, err := flakyQ.GetTaskRow(ctx, key)
		if err != nil || len(rowAfter.Actions) != 0 {
			t.Fatalf("expected ReconcileInline to re-acquire lease and finish RESUME_ACTOR, got row=%+v err=%v", rowAfter, err)
		}
		if resumeCalls.Load() < 2 {
			t.Fatalf("resumeCalls = %d, want >= 2", resumeCalls.Load())
		}
	})

	t.Run("ActionDeleteActor with empty ActorUID waits for DELETING orphan before deleting task row and ModeQueue cancel does not trip circuit breaker", func(t *testing.T) {
		sub := substratetest.NewServer(t)
		nowMu := sync.Mutex{}
		nowVal := time.Date(2026, 11, 16, 19, 0, 0, 0, time.UTC)
		getNow := func() time.Time {
			nowMu.Lock()
			defer nowMu.Unlock()
			return nowVal
		}
		advance := func(d time.Duration) {
			nowMu.Lock()
			nowVal = nowVal.Add(d)
			nowMu.Unlock()
		}

		mem := memory.NewStore(memory.WithClock(getNow))
		baseRT := reconciler.NewSubstrateRuntime(sub.Client(), reconciler.SubstrateRuntimeOptions{})
		var stillDeleting atomic.Bool
		stillDeleting.Store(true)

		rt := &preconditionRaceRuntime{
			RuntimeClient: baseRT,
			onGetActor: func(gCtx context.Context, ns, actor string) (*ateapipb.Actor, error) {
				if actor == "deleting-orphan" {
					if stillDeleting.Load() {
						return &ateapipb.Actor{
							Metadata: &ateapipb.ResourceMetadata{Atespace: ns, Name: actor, Uid: "11111111-2222-3333-4444-555555555555"},
							Status:   &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_DELETING},
						}, nil
					}
					return nil, status.Error(codes.NotFound, "actor gone")
				}
				return baseRT.GetActor(gCtx, ns, actor)
			},
			onResume: func(rCtx context.Context, _, _ string) error {
				<-rCtx.Done()
				return rCtx.Err()
			},
		}

		rec := reconciler.New(mem, rt, reconciler.Config{
			Mode:           reconciler.ModeInline,
			Transition:     stubTransition,
			InitialBackoff: 10 * time.Millisecond,
			MaxBackoff:     20 * time.Millisecond,
			MutatingQPS:    -1,
		})

		delKey := store.TaskKey{Atespace: "default", Name: "deleting-orphan"}
		if _, err := mem.Apply(ctx, delKey, func(_ *store.TaskRow, ts time.Time) (*store.TaskRow, error) {
			st := lifecycle.State{
				Task: &v1alpha1.Task{
					Metadata: &v1alpha1.ObjectMeta{DeleteTime: timestamppb.New(ts)},
					Status: &v1alpha1.TaskStatus{
						State:            v1alpha1.TaskState_TASK_STATE_QUEUED,
						AteActorTemplate: "ax-orphan",
					},
				},
			}
			st.Actions = []lifecycle.Action{st.NextAction(lifecycle.ActionDeleteActor, ts)}
			row := &store.TaskRow{}
			row.SetLifecycleState(st, ts)
			return row, nil
		}); err != nil {
			t.Fatalf("Apply(delKey): %v", err)
		}

		// While GetActor reports ACTOR_STATE_DELETING, DELETE_ACTOR must back off
		// without deleting the PostgreSQL row prematurely!
		if err := rec.ReconcileInline(ctx, delKey); err != nil {
			t.Fatalf("ReconcileInline(while DELETING): %v", err)
		}
		stillPresent, err := mem.GetTaskRow(ctx, delKey)
		if err != nil || stillPresent == nil || len(stillPresent.Actions) != 1 || stillPresent.Actions[0].Tries != 1 {
			t.Fatalf("task row was prematurely deleted while actor still DELETING: row=%+v err=%v", stillPresent, err)
		}

		// Once GetActor confirms NotFound, DELETE_ACTOR completes and deletes the row.
		stillDeleting.Store(false)
		advance(time.Second)
		if err := rec.ReconcileInline(ctx, delKey); err != nil {
			t.Fatalf("ReconcileInline(once NotFound): %v", err)
		}
		if _, err := mem.GetTaskRow(ctx, delKey); !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("expected task row deleted once NotFound, got %v", err)
		}

		// Also verify in ModeQueue with MinCalls=1 that parent shutdown
		// cancellation does NOT trip the circuit breaker!
		qMem := memory.NewStore()
		started := make(chan struct{})
		qRT := &preconditionRaceRuntime{
			RuntimeClient: baseRT,
			onGetActor: func(_ context.Context, ns, actor string) (*ateapipb.Actor, error) {
				return &ateapipb.Actor{
					Metadata: &ateapipb.ResourceMetadata{Atespace: ns, Name: actor, Uid: "uid-q"},
					Status:   &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
				}, nil
			},
			onResume: func(rCtx context.Context, _, _ string) error {
				close(started)
				<-rCtx.Done()
				return rCtx.Err()
			},
		}
		qRec := reconciler.New(qMem, qRT, reconciler.Config{
			Mode:         reconciler.ModeQueue,
			Transition:   stubTransition,
			PollInterval: 20 * time.Millisecond,
			MutatingQPS:  -1,
			CircuitBreaker: reconciler.CircuitBreakerConfig{
				MinCalls:         1,
				FailureThreshold: 0.5,
			},
		})
		if _, err := qMem.Apply(ctx, store.TaskKey{Atespace: "default", Name: "q-cancel"}, func(_ *store.TaskRow, ts time.Time) (*store.TaskRow, error) {
			st := lifecycle.State{
				Task:     &v1alpha1.Task{Status: &v1alpha1.TaskStatus{State: v1alpha1.TaskState_TASK_STATE_QUEUED}},
				ActorUID: "uid-q",
			}
			st.Actions = []lifecycle.Action{st.NextAction(lifecycle.ActionResumeActor, ts)}
			row := &store.TaskRow{}
			row.SetLifecycleState(st, ts)
			return row, nil
		}); err != nil {
			t.Fatalf("Apply(q-cancel): %v", err)
		}
		runCtx, cancelRun := context.WithCancel(ctx)
		runDone := make(chan error, 1)
		go func() { runDone <- qRec.Run(runCtx) }()
		<-started
		cancelRun()
		<-runDone
		if qRec.CircuitBreakerOpen() {
			t.Fatal("ModeQueue shutdown cancellation with MinCalls=1 must not open circuit breaker")
		}
	})

	t.Run("permanent steady-state FailedPrecondition, external wakeup during CREATE_TAG, WaitWorkspaceTag timeout, and MutatingQPS ActionTimeout", func(t *testing.T) {
		sub := substratetest.NewServer(t)
		nowMu := sync.Mutex{}
		nowVal := time.Date(2026, 11, 16, 20, 0, 0, 0, time.UTC)
		getNow := func() time.Time {
			nowMu.Lock()
			defer nowMu.Unlock()
			return nowVal
		}
		advance := func(d time.Duration) {
			nowMu.Lock()
			nowVal = nowVal.Add(d)
			nowMu.Unlock()
		}

		mem := memory.NewStore(memory.WithClock(getNow))
		baseRT := reconciler.NewSubstrateRuntime(sub.Client(), reconciler.SubstrateRuntimeOptions{})

		var permResumePrecond atomic.Bool
		permResumePrecond.Store(true)
		rt := &preconditionRaceRuntime{
			RuntimeClient: baseRT,
			onResume: func(rCtx context.Context, ns, actor string) error {
				if permResumePrecond.Load() && actor == "perm-precond" {
					return status.Error(codes.FailedPrecondition, "source tag external snapshot permanently gone")
				}
				return baseRT.ResumeActor(rCtx, ns, actor)
			},
		}

		rec := reconciler.New(mem, rt, reconciler.Config{
			Mode:           reconciler.ModeInline,
			Transition:     stubTransition,
			InitialBackoff: 5 * time.Millisecond,
			MaxBackoff:     10 * time.Millisecond,
			MutatingQPS:    -1,
			Clock:          getNow,
			WorkspaceTagResolver: func(context.Context, string, string, string, string) (string, bool, error) {
				return "", false, nil
			},
		})

		if err := sub.Client().EnsureAtespace(ctx, "default"); err != nil {
			t.Fatalf("EnsureAtespace: %v", err)
		}
		tmpl := substrate.BuildActorTemplate("default", "ax-perm", "us-docker.pkg.dev/p/img@sha256:111", nil, nil, "", nil)
		if _, err := sub.Client().EnsureActorTemplate(ctx, tmpl); err != nil {
			t.Fatalf("EnsureActorTemplate: %v", err)
		}

		// 1. Permanent FailedPrecondition on steady-state SUSPENDED actor bounds
		// retries and transitions to TASK_STATE_FAILED instead of looping forever!
		permActor, err := sub.Client().EnsureActor(ctx, "default", "perm-precond", "default", "ax-perm")
		if err != nil {
			t.Fatalf("EnsureActor: %v", err)
		}
		permKey := store.TaskKey{Atespace: "default", Name: "perm-precond"}
		if _, err := mem.Apply(ctx, permKey, func(_ *store.TaskRow, ts time.Time) (*store.TaskRow, error) {
			st := lifecycle.State{
				Task: &v1alpha1.Task{
					Status: &v1alpha1.TaskStatus{
						State:            v1alpha1.TaskState_TASK_STATE_QUEUED,
						AteActorTemplate: "ax-perm",
					},
				},
				ActorUID: permActor.GetMetadata().GetUid(),
			}
			st.Actions = []lifecycle.Action{st.NextAction(lifecycle.ActionResumeActor, ts)}
			row := &store.TaskRow{}
			row.SetLifecycleState(st, ts)
			return row, nil
		}); err != nil {
			t.Fatalf("Apply(permKey): %v", err)
		}
		for range 5 {
			if err := rec.ReconcileInline(ctx, permKey); err != nil {
				t.Fatalf("ReconcileInline(permKey): %v", err)
			}
			row, _ := mem.GetTaskRow(ctx, permKey)
			if row.Task.GetStatus().GetState() == v1alpha1.TaskState_TASK_STATE_FAILED {
				break
			}
			advance(time.Second)
		}
		permRow, _ := mem.GetTaskRow(ctx, permKey)
		if permRow.Task.GetStatus().GetState() != v1alpha1.TaskState_TASK_STATE_FAILED || len(permRow.Actions) != 0 {
			t.Fatalf("expected permanent steady-state FailedPrecondition to fail task, got state=%v actions=%+v",
				permRow.Task.GetStatus().GetState(), permRow.Actions)
		}

		// 2. External wakeup leaving source actor RUNNING during CREATE_TAG
		// triggers synchronous re-suspend and succeeds on retry!
		wokeActor, err := sub.Client().EnsureActor(ctx, "default", "woke-tag-src", "default", "ax-perm")
		if err != nil {
			t.Fatalf("EnsureActor(woke-tag-src): %v", err)
		}
		if _, _, err := sub.Client().ResumeActor(ctx, "default", "woke-tag-src"); err != nil {
			t.Fatalf("ResumeActor(woke-tag-src): %v", err)
		}
		wokeKey := store.TaskKey{Atespace: "default", Name: "woke-tag-src"}
		if _, err := mem.Apply(ctx, wokeKey, func(_ *store.TaskRow, ts time.Time) (*store.TaskRow, error) {
			st := lifecycle.State{
				Task: &v1alpha1.Task{
					Status: &v1alpha1.TaskStatus{
						State:            v1alpha1.TaskState_TASK_STATE_SUSPENDED,
						AteActorTemplate: "ax-perm",
					},
				},
				ActorUID: wokeActor.GetMetadata().GetUid(),
			}
			tagAct := st.NextAction(lifecycle.ActionCreateTag, ts)
			tagAct.Tag = "woke-snap-tag"
			st.Actions = []lifecycle.Action{tagAct}
			row := &store.TaskRow{}
			row.SetLifecycleState(st, ts)
			return row, nil
		}); err != nil {
			t.Fatalf("Apply(wokeKey): %v", err)
		}
		// Attempt 1: CreateTag fails with FailedPrecondition because actor is RUNNING -> reconciler re-suspends and backs off.
		if err := rec.ReconcileInline(ctx, wokeKey); err != nil {
			t.Fatalf("ReconcileInline(wokeKey 1): %v", err)
		}
		advance(time.Second)
		// Attempt 2: Actor is now SUSPENDED -> CreateTag succeeds!
		if err := rec.ReconcileInline(ctx, wokeKey); err != nil {
			t.Fatalf("ReconcileInline(wokeKey 2): %v", err)
		}
		wokeRow, _ := mem.GetTaskRow(ctx, wokeKey)
		if len(wokeRow.Actions) != 0 {
			t.Fatalf("expected CREATE_TAG after re-suspend to complete, got actions=%+v", wokeRow.Actions)
		}
		if _, ok := sub.Tag("default", "woke-snap-tag"); !ok {
			t.Fatal("expected woke-snap-tag created on Substrate")
		}

		// 3. WAIT_WORKSPACE_TAG exceeding DefaultMaxWorkspaceTagWait fails permanently!
		wsTimeoutKey := store.TaskKey{Atespace: "default", Name: "ws-tag-timeout"}
		if _, err := mem.Apply(ctx, wsTimeoutKey, func(_ *store.TaskRow, ts time.Time) (*store.TaskRow, error) {
			st := lifecycle.State{
				Task: &v1alpha1.Task{
					Spec:   &v1alpha1.TaskSpec{Workspace: &v1alpha1.WorkspaceRef{Name: "ws-never"}},
					Status: &v1alpha1.TaskStatus{State: v1alpha1.TaskState_TASK_STATE_QUEUED},
				},
			}
			w := st.NextAction(lifecycle.ActionWaitWorkspaceTag, ts)
			w.Tag = "missing-ws-tag"
			st.Actions = []lifecycle.Action{w}
			row := &store.TaskRow{}
			row.SetLifecycleState(st, ts)
			return row, nil
		}); err != nil {
			t.Fatalf("Apply(wsTimeoutKey): %v", err)
		}
		advance(reconciler.DefaultMaxWorkspaceTagWait + time.Second)
		if err := rec.ReconcileInline(ctx, wsTimeoutKey); err != nil {
			t.Fatalf("ReconcileInline(wsTimeoutKey): %v", err)
		}
		wsTimeoutRow, _ := mem.GetTaskRow(ctx, wsTimeoutKey)
		if wsTimeoutRow.Task.GetStatus().GetState() != v1alpha1.TaskState_TASK_STATE_FAILED || len(wsTimeoutRow.Actions) != 0 {
			t.Fatalf("expected WAIT_WORKSPACE_TAG timeout to fail task, got state=%v actions=%+v",
				wsTimeoutRow.Task.GetStatus().GetState(), wsTimeoutRow.Actions)
		}

		// 4. MutatingQPS limiter wait exceeding ActionTimeout backs off with
		// cbSignalNone (does not open MinCalls=1 circuit breaker)!
		rlRec := reconciler.New(mem, baseRT, reconciler.Config{
			Mode:           reconciler.ModeQueue, // exercise breaker signal path via ReconcileInline
			Transition:     stubTransition,
			ActionTimeout:  15 * time.Millisecond,
			InitialBackoff: 5 * time.Millisecond,
			MaxBackoff:     10 * time.Millisecond,
			MutatingQPS:    0.5, // 1 token per 2s after initial burst=1
			MutatingBurst:  1,
			Clock:          getNow,
			CircuitBreaker: reconciler.CircuitBreakerConfig{
				MinCalls:         1,
				FailureThreshold: 0.5,
			},
		})
		for i := range 2 {
			k := store.TaskKey{Atespace: "default", Name: fmt.Sprintf("rl-task-%d", i)}
			a, err := sub.Client().EnsureActor(ctx, "default", k.Name, "default", "ax-perm")
			if err != nil {
				t.Fatalf("EnsureActor(%v): %v", k, err)
			}
			if _, err := mem.Apply(ctx, k, func(_ *store.TaskRow, ts time.Time) (*store.TaskRow, error) {
				st := lifecycle.State{
					Task:     &v1alpha1.Task{Status: &v1alpha1.TaskStatus{State: v1alpha1.TaskState_TASK_STATE_QUEUED, AteActorTemplate: "ax-perm"}},
					ActorUID: a.GetMetadata().GetUid(),
				}
				st.Actions = []lifecycle.Action{st.NextAction(lifecycle.ActionResumeActor, ts)}
				row := &store.TaskRow{}
				row.SetLifecycleState(st, ts)
				return row, nil
			}); err != nil {
				t.Fatalf("Apply(%v): %v", k, err)
			}
			if err := rlRec.ReconcileInline(ctx, k); err != nil {
				t.Fatalf("ReconcileInline(%v): %v", k, err)
			}
		}
		// Task 0 consumed burst=1; Task 1 timed out in limiter.Wait (2s > 15ms ActionTimeout) and backed off!
		rlRow1, _ := mem.GetTaskRow(ctx, store.TaskKey{Atespace: "default", Name: "rl-task-1"})
		if len(rlRow1.Actions) != 1 || rlRow1.Actions[0].Tries != 1 {
			t.Fatalf("expected rl-task-1 to back off with Tries=1 on limiter timeout, got %+v", rlRow1.Actions)
		}
		if rlRec.CircuitBreakerOpen() {
			t.Fatal("rate-limiter wait timeout must not open circuit breaker")
		}
	})
}
