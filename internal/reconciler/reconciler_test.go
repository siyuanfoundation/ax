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
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/ax/internal/controller"
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

// stubTransition implements a deterministic lifecycle.TransitionFunc covering
// the action, capacity, failure, crash-observation, and watchdog contracts so
// reconciler tests exercise the reconciliation loop independently of the
// state-machine PR.
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
			return next, lifecycle.Output{}, nil
		}
		if ev.ActorState == ateapipb.ActorState_ACTOR_STATE_CRASHED {
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
		}
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

// noIOInTxGuard fails the test if any RuntimeClient RPC executes while a
// store.Apply callback is open.
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
func (g *noIOInTxGuard) DeleteActor(ctx context.Context, ns, actor, uid string) error {
	g.checkNoTx("DeleteActor")
	return g.RuntimeClient.DeleteActor(ctx, ns, actor, uid)
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
func (g *noIOInTxGuard) GetTag(ctx context.Context, ns, tag string) (*ateapipb.Tag, error) {
	g.checkNoTx("GetTag")
	return g.RuntimeClient.GetTag(ctx, ns, tag)
}

type customRuntime struct {
	reconciler.RuntimeClient
	onResume  func(ctx context.Context, ns, actor string) error
	onSuspend func(ctx context.Context, ns, actor string) (int32, error)
}

func (c *customRuntime) ResumeActor(ctx context.Context, ns, actor string) error {
	if c.onResume != nil {
		return c.onResume(ctx, ns, actor)
	}
	return c.RuntimeClient.ResumeActor(ctx, ns, actor)
}

func (c *customRuntime) SuspendActorSync(ctx context.Context, ns, actor string) (int32, error) {
	if c.onSuspend != nil {
		return c.onSuspend(ctx, ns, actor)
	}
	return c.RuntimeClient.SuspendActorSync(ctx, ns, actor)
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out waiting for condition")
}

func TestReconciler_ProvisionSuspendTagRevertAndDelete(t *testing.T) {
	mem := memory.NewStore()
	sub := substratetest.NewServer(t)
	rt := reconciler.NewSubstrateRuntime(sub.Client(), reconciler.SubstrateRuntimeOptions{
		DefaultTaskImage: "us-docker.pkg.dev/acme/agent@sha256:abcdef",
	})
	guard := &noIOInTxGuard{TaskQueue: mem, RuntimeClient: rt, t: t}

	rec := reconciler.New(guard, guard, reconciler.Config{
		WorkerID:     "test-worker",
		Transition:   stubTransition,
		PollInterval: 20 * time.Millisecond,
		ExtraEgressRules: []*ateapipb.EgressRule{{
			Https: &ateapipb.HttpsRule{Hostnames: []string{"ax-server.ax-system.svc"}},
		}},
	})

	ctx := t.Context()
	runCtx, cancelRun := context.WithCancel(ctx)
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		_ = rec.Run(runCtx)
	}()
	t.Cleanup(func() {
		cancelRun()
		<-runDone
	})

	prof := &v1alpha1.Profile{Image: "us-docker.pkg.dev/acme/agent@sha256:abcdef"}
	tmplName, _, err := controller.ResolveProfileTemplate("payments", prof, "", "")
	if err != nil {
		t.Fatalf("ResolveProfileTemplate: %v", err)
	}

	// 1. Provision task: ENSURE_TEMPLATE -> CREATE_ACTOR -> ENSURE_EGRESS -> RESUME_ACTOR.
	key := store.TaskKey{Atespace: "payments", Name: "task-1"}
	if _, err := guard.Apply(ctx, key, func(_ *store.TaskRow, now time.Time) (*store.TaskRow, error) {
		st := lifecycle.State{
			Task: &v1alpha1.Task{
				Status: &v1alpha1.TaskStatus{
					State:            v1alpha1.TaskState_TASK_STATE_QUEUED,
					AteActorTemplate: tmplName,
				},
			},
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
				Https: &ateapipb.HttpsRule{Hostnames: []string{"api.openai.com"}},
			}},
		}
		row.SetLifecycleState(st, now)
		return row, nil
	}); err != nil {
		t.Fatalf("Apply(create): %v", err)
	}

	waitFor(t, 3*time.Second, func() bool {
		r, gErr := mem.GetTaskRow(ctx, key)
		return gErr == nil && len(r.Actions) == 0 && r.ActorUID != "" && r.ClaimDeadline != nil
	})
	actor, ok := sub.Actor("payments", "task-1")
	if !ok || actor.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_RUNNING {
		t.Fatalf("expected RUNNING actor on Substrate, got %+v", actor)
	}
	pol, ok := sub.EgressPolicy("payments", "task-1")
	if !ok || len(pol.GetRules()) != 2 {
		t.Fatalf("expected 2 combined egress rules on actor, got %+v", pol)
	}

	// 2. SUSPEND_ACTOR_SYNC + CREATE_TAG + DELETE_TAG.
	if _, err := guard.Apply(ctx, key, func(cur *store.TaskRow, now time.Time) (*store.TaskRow, error) {
		next := cur.Clone()
		st := next.LifecycleState()
		st.RunnerTokenHash = []byte{1, 2, 3}
		st.RunnerTokenConfirmed = true
		sus := st.NextAction(lifecycle.ActionSuspendActorSync, now)
		sus.SuspendReason = lifecycle.SuspendReasonUser
		mkTag := st.NextAction(lifecycle.ActionCreateTag, now)
		mkTag.Tag = "snap-tag-1"
		st.Actions = []lifecycle.Action{sus, mkTag}
		next.SetLifecycleState(st, now)
		return next, nil
	}); err != nil {
		t.Fatalf("Apply(suspend+tag): %v", err)
	}

	waitFor(t, 3*time.Second, func() bool {
		r, gErr := mem.GetTaskRow(ctx, key)
		return gErr == nil && len(r.Actions) == 0 && r.Task.GetStatus().GetState() == v1alpha1.TaskState_TASK_STATE_SUSPENDED
	})
	rSuspended, _ := mem.GetTaskRow(ctx, key)
	if rSuspended.RunnerTokenGen == 0 {
		t.Fatalf("expected RunnerTokenGen recorded on SUSPEND_ACTOR_SYNC, got %+v", rSuspended.State)
	}
	if _, ok := sub.Tag("payments", "snap-tag-1"); !ok {
		t.Fatal("expected snap-tag-1 created on Substrate")
	}

	if _, err := guard.Apply(ctx, key, func(cur *store.TaskRow, now time.Time) (*store.TaskRow, error) {
		next := cur.Clone()
		st := next.LifecycleState()
		delTag := st.NextAction(lifecycle.ActionDeleteTag, now)
		delTag.Tag = "snap-tag-1"
		st.Actions = []lifecycle.Action{delTag}
		next.SetLifecycleState(st, now)
		return next, nil
	}); err != nil {
		t.Fatalf("Apply(delete tag): %v", err)
	}
	waitFor(t, 3*time.Second, func() bool {
		_, exists := sub.Tag("payments", "snap-tag-1")
		return !exists
	})

	// 3. DELETE_ACTOR removes actor from Substrate and deletes the TaskRow.
	if _, err := guard.Apply(ctx, key, func(cur *store.TaskRow, now time.Time) (*store.TaskRow, error) {
		next := cur.Clone()
		st := next.LifecycleState()
		st.Task.Metadata.DeleteTime = timestamppb.New(now)
		st.Actions = []lifecycle.Action{st.NextAction(lifecycle.ActionDeleteActor, now)}
		next.SetLifecycleState(st, now)
		return next, nil
	}); err != nil {
		t.Fatalf("Apply(delete): %v", err)
	}
	waitFor(t, 3*time.Second, func() bool {
		_, gErr := mem.GetTaskRow(ctx, key)
		return errors.Is(gErr, store.ErrNotFound)
	})
	if _, exists := sub.Actor("payments", "task-1"); exists {
		t.Fatal("expected actor deleted from Substrate")
	}
}

func TestReconciler_WaitTagCapacityWatchdogAndCrashRecovery(t *testing.T) {
	now := time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)
	advance := func(d time.Duration) { now = now.Add(d) }
	mem := memory.NewStore(memory.WithClock(func() time.Time { return now }))
	sub := substratetest.NewServer(t)
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
		onResume: func(callCtx context.Context, ns, actor string) error {
			if hangResume.Load() {
				<-callCtx.Done()
				return callCtx.Err()
			}
			if capExhaust.Load() {
				return status.Error(codes.ResourceExhausted, "no free gVisor workers")
			}
			return baseRT.ResumeActor(callCtx, ns, actor)
		},
	}

	rec := reconciler.New(mem, rt, reconciler.Config{
		Transition:     stubTransition,
		ActionTimeout:  40 * time.Millisecond,
		InitialBackoff: 50 * time.Millisecond,
		MaxBackoff:     100 * time.Millisecond,
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
		Metadata: &ateapipb.ResourceMetadata{Atespace: "default", Name: "warm-tag-1"},
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

	// 1. WAIT_WORKSPACE_TAG backs off until ready, then seeds CREATE_ACTOR;
	// hung ResumeActor times out via ActionTimeout and backs off.
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
		waitAct.Tag = "warm-tag-1"
		st.Actions = []lifecycle.Action{
			waitAct,
			st.NextAction(lifecycle.ActionCreateActor, ts),
			st.NextAction(lifecycle.ActionResumeActor, ts),
		}
		row := &store.TaskRow{EffectiveProfile: prof}
		row.SetLifecycleState(st, ts)
		return row, nil
	}); err != nil {
		t.Fatalf("Apply(warm-task): %v", err)
	}

	if _, err := rec.ProcessOnce(ctx); err != nil {
		t.Fatalf("ProcessOnce(tag warming): %v", err)
	}
	rWarm, _ := mem.GetTaskRow(ctx, wsKey)
	if len(rWarm.Actions) != 3 || rWarm.Actions[0].Tries != 1 {
		t.Fatalf("expected WAIT_WORKSPACE_TAG backed off with Tries=1, got %+v", rWarm.Actions)
	}

	tagReady.Store(true)
	hangResume.Store(true)
	advance(time.Second)
	if _, err := rec.ProcessOnce(ctx); err != nil {
		t.Fatalf("ProcessOnce(hung resume): %v", err)
	}
	rAfterHang, _ := mem.GetTaskRow(ctx, wsKey)
	if len(rAfterHang.Actions) != 1 || rAfterHang.Actions[0].Kind != lifecycle.ActionResumeActor || rAfterHang.Actions[0].Tries != 1 {
		t.Fatalf("expected RESUME_ACTOR timed out and backed off, got %+v", rAfterHang.Actions)
	}
	createdActor, ok := sub.Actor("default", "warm-task")
	if !ok || createdActor.GetSourceTag().GetName() != "warm-tag-1" {
		t.Fatalf("expected actor created from warm-tag-1, got %+v", createdActor)
	}

	// 2. CapacityUnavailable backs off RESUME_ACTOR while staying QUEUED even
	// past 30m, then resumes once capacity recovers.
	hangResume.Store(false)
	capExhaust.Store(true)
	advance(time.Second)
	if _, err := rec.ProcessOnce(ctx); err != nil {
		t.Fatalf("ProcessOnce(capacity 1): %v", err)
	}
	advance(45 * time.Minute)
	if _, err := rec.ProcessOnce(ctx); err != nil {
		t.Fatalf("ProcessOnce(capacity >30m): %v", err)
	}
	rStillQueued, _ := mem.GetTaskRow(ctx, wsKey)
	if rStillQueued.Task.GetStatus().GetState() != v1alpha1.TaskState_TASK_STATE_QUEUED || len(rStillQueued.Actions) != 1 {
		t.Fatalf("expected QUEUED across extended capacity exhaustion, got state=%v actions=%+v",
			rStillQueued.Task.GetStatus().GetState(), rStillQueued.Actions)
	}

	capExhaust.Store(false)
	advance(lifecycle.DefaultActionMaxBackoff + time.Second)
	if _, err := rec.ProcessOnce(ctx); err != nil {
		t.Fatalf("ProcessOnce(capacity recovered): %v", err)
	}
	rResumed, _ := mem.GetTaskRow(ctx, wsKey)
	if len(rResumed.Actions) != 0 || rResumed.ClaimDeadline == nil {
		t.Fatalf("expected RESUME_ACTOR completed with ClaimDeadline set, got %+v", rResumed.State)
	}

	// 3. ClaimDeadline watchdog expires -> TimerExpiredInput fails task and
	// parks actor via terminal SUSPEND_ACTOR_SYNC.
	advance(lifecycle.ClaimWatchdogTimeout + time.Second)
	if _, err := rec.ProcessOnce(ctx); err != nil {
		t.Fatalf("ProcessOnce(ClaimDeadline): %v", err)
	}
	rWatchdog, _ := mem.GetTaskRow(ctx, wsKey)
	if rWatchdog.Task.GetStatus().GetState() != v1alpha1.TaskState_TASK_STATE_FAILED || len(rWatchdog.Actions) != 0 {
		t.Fatalf("expected FAILED and parked after ClaimDeadline expiry, got %+v", rWatchdog.State)
	}
	parkedActor, _ := sub.Actor("default", "warm-task")
	if parkedActor.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
		t.Fatalf("expected actor SUSPENDED after terminal parking, got %v", parkedActor.GetStatus().GetState())
	}

	// 4. Crashed actor during SUSPEND_ACTOR_SYNC routes via ObservedActorInput{CRASHED}
	// -> REVERT_ACTOR + RESUME_ACTOR recovery!
	sub.SetActor(&ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "default", Name: "crash-task", Uid: "uid-crash"},
		Status:   &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_CRASHED},
	})
	crashKey := store.TaskKey{Atespace: "default", Name: "crash-task"}
	if _, err := mem.Apply(ctx, crashKey, func(_ *store.TaskRow, ts time.Time) (*store.TaskRow, error) {
		st := lifecycle.State{
			Task: &v1alpha1.Task{
				Status: &v1alpha1.TaskStatus{
					State:   v1alpha1.TaskState_TASK_STATE_WORKING,
					Attempt: 1,
				},
			},
			ActorUID: "uid-crash",
		}
		sus := st.NextAction(lifecycle.ActionSuspendActorSync, ts)
		sus.SuspendReason = lifecycle.SuspendReasonUser
		st.Actions = []lifecycle.Action{sus}
		row := &store.TaskRow{}
		row.SetLifecycleState(st, ts)
		return row, nil
	}); err != nil {
		t.Fatalf("Apply(crash-task): %v", err)
	}
	if _, err := rec.ProcessOnce(ctx); err != nil {
		t.Fatalf("ProcessOnce(crash-task): %v", err)
	}
	rRecovered, _ := mem.GetTaskRow(ctx, crashKey)
	if rRecovered.Task.GetStatus().GetAttempt() != 2 || len(rRecovered.Actions) != 0 {
		t.Fatalf("expected crash-task reverted and resumed at attempt 2, got %+v", rRecovered.State)
	}
}
