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

package lifecycle_test

import (
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	"github.com/google/ax/internal/lifecycle"
	runnerv1 "github.com/google/ax/pkg/apis/runner/v1"
	"github.com/google/ax/pkg/apis/v1alpha1"
	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/protobuf/proto"
)

var (
	guardT0 = time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)
	// guardToken is the hash of the runner token minted by newWorkingState.
	guardToken = sha256.Sum256([]byte("runner-token"))
)

func mustTransition(t *testing.T, s lifecycle.State, in lifecycle.Input, at time.Time) lifecycle.State {
	t.Helper()
	next, _, err := lifecycle.Transition(s, in, at)
	if err != nil {
		t.Fatalf("Transition(%+v): %v", in, err)
	}
	return next
}

// newCreatedState returns a freshly created QUEUED task whose four
// provisioning actions are all still pending.
func newCreatedState(t *testing.T, maxRetries int32) lifecycle.State {
	t.Helper()
	return mustTransition(t, lifecycle.State{}, lifecycle.Input{Create: &lifecycle.CreateInput{
		Task: &v1alpha1.Task{
			Metadata: &v1alpha1.ObjectMeta{Atespace: "payments", Name: "guarded", Uid: "task-uid"},
			Spec: &v1alpha1.TaskSpec{
				Commands: []string{"pytest"},
				Retry:    &v1alpha1.RetryConfig{MaxRetries: maxRetries},
			},
		},
		AteActorTemplate: "ax-0123456789abcdef",
		WorkspaceReady:   true,
	}}, guardT0)
}

// newClaimedState provisions the task and answers the runner's first claim
// (RUN r-1), leaving it QUEUED with the runner token minted.
func newClaimedState(t *testing.T, maxRetries int32) lifecycle.State {
	t.Helper()
	s := newCreatedState(t, maxRetries)
	for len(s.Actions) > 0 {
		a := s.Actions[0]
		s = mustTransition(t, s, lifecycle.Input{ActionCompleted: &lifecycle.ActionCompletedInput{
			ActionID: a.ID, Kind: a.Kind, ActorUID: "actor-uid-1",
		}}, guardT0)
	}
	return mustTransition(t, s, lifecycle.Input{Claim: &lifecycle.ClaimInput{
		Request:              &runnerv1.ClaimAssignmentRequest{Nonce: "n-1"},
		CandidateRunnerToken: "runner-token",
		CandidateTokenHash:   guardToken[:],
	}}, guardT0)
}

// newWorkingState drives a task to WORKING on run r-1.
func newWorkingState(t *testing.T, maxRetries int32) lifecycle.State {
	t.Helper()
	s := newClaimedState(t, maxRetries)
	s = mustTransition(t, s, lifecycle.Input{
		EventTokenHash: guardToken[:], EventRunID: s.ActiveRunID, EventSeq: 1,
		CommandStarted: &runnerv1.CommandStarted{Pid: 42},
	}, guardT0)
	if s.Task.Status.State != v1alpha1.TaskState_TASK_STATE_WORKING {
		t.Fatalf("setup: state = %s, want WORKING", s.Task.Status.State)
	}
	return s
}

// completeHead reports the head pending action as completed.
func completeHead(t *testing.T, s lifecycle.State) lifecycle.State {
	t.Helper()
	if len(s.Actions) == 0 {
		t.Fatal("setup: no pending action to complete")
	}
	a := s.Actions[0]
	return mustTransition(t, s, lifecycle.Input{ActionCompleted: &lifecycle.ActionCompletedInput{
		ActionID: a.ID, Kind: a.Kind, SnapshotGen: 1,
	}}, guardT0)
}

// newIdleState parks a WORKING task in IDLE via idle_notice and its checkpoint.
func newIdleState(t *testing.T) lifecycle.State {
	t.Helper()
	s := newWorkingState(t, 0)
	s = mustTransition(t, s, lifecycle.Input{
		EventTokenHash: guardToken[:], EventRunID: s.ActiveRunID, EventSeq: 2,
		IdleNotice: &runnerv1.IdleNotice{Output: "turn done"},
	}, guardT0)
	s = completeHead(t, s)
	if s.Task.Status.State != v1alpha1.TaskState_TASK_STATE_IDLE {
		t.Fatalf("setup: state = %s, want IDLE", s.Task.Status.State)
	}
	return s
}

// newSuspendedState checkpoints a WORKING task into SUSPENDED via SuspendTask.
func newSuspendedState(t *testing.T) lifecycle.State {
	t.Helper()
	s := newWorkingState(t, 0)
	s = mustTransition(t, s, lifecycle.Input{Suspend: &lifecycle.SuspendInput{}}, guardT0)
	s = completeHead(t, s)
	if s.Task.Status.State != v1alpha1.TaskState_TASK_STATE_SUSPENDED {
		t.Fatalf("setup: state = %s, want SUSPENDED", s.Task.Status.State)
	}
	return s
}

func actionIDs(actions []lifecycle.Action) []string {
	out := make([]string, 0, len(actions))
	for _, a := range actions {
		out = append(out, string(a.Kind)+":"+a.ID)
	}
	return out
}

func crashObserved() lifecycle.Input {
	return lifecycle.Input{ObservedActor: &lifecycle.ObservedActorInput{
		ActorState: ateapipb.ActorState_ACTOR_STATE_CRASHED, CrashMessage: "oom",
	}}
}

// Every runner event must present the runner token: the untrusted task process
// shares the actor's egress identity and could otherwise forge a successful
// exit. Rejected events leave the state untouched.
func TestRunnerEventsRequireRunnerToken(t *testing.T) {
	working := newWorkingState(t, 0)
	wrong := sha256.Sum256([]byte("not-the-runner-token"))

	events := map[string]lifecycle.Input{
		"SetupFinished":  {EventSeq: 2, SetupFinished: &runnerv1.SetupFinished{Ok: true}},
		"CommandStarted": {EventRunID: "r-1", EventSeq: 2, CommandStarted: &runnerv1.CommandStarted{Pid: 7}},
		"CommandExited":  {EventRunID: "r-1", EventSeq: 2, CommandExited: &runnerv1.CommandExited{ExitCode: 0}},
		"IdleNotice":     {EventRunID: "r-1", EventSeq: 2, IdleNotice: &runnerv1.IdleNotice{}},
		"EvictionNotice": {EventRunID: "r-1", EventSeq: 2, EvictionNotice: &runnerv1.EvictionNotice{}},
	}
	for name, in := range events {
		for label, hash := range map[string][]byte{"missing token": nil, "wrong token": wrong[:]} {
			in.EventTokenHash = hash
			got, out, err := lifecycle.Transition(working, in, guardT0.Add(time.Second))
			if !errors.Is(err, lifecycle.ErrUnauthorizedRunner) {
				t.Errorf("%s with %s: err = %v, want ErrUnauthorizedRunner", name, label, err)
			}
			if out.Assignment != nil || out.DeleteRow {
				t.Errorf("%s with %s: output = %+v, want empty", name, label, out)
			}
			if got.Task.Status.State != v1alpha1.TaskState_TASK_STATE_WORKING ||
				!proto.Equal(got.Task, working.Task) || got.LastAckedSeq != working.LastAckedSeq {
				t.Errorf("%s with %s: state mutated to %s (acked seq %d)", name, label, got.Task.Status.State, got.LastAckedSeq)
			}
		}
	}

	// The same forged exit is accepted only with the token on record.
	done := mustTransition(t, working, lifecycle.Input{
		EventTokenHash: guardToken[:], EventRunID: "r-1", EventSeq: 2,
		CommandExited: &runnerv1.CommandExited{ExitCode: 0},
	}, guardT0.Add(time.Second))
	if done.Task.Status.State != v1alpha1.TaskState_TASK_STATE_COMPLETED {
		t.Fatalf("authenticated CommandExited: state = %s, want COMPLETED", done.Task.Status.State)
	}

	// Before a runner token is minted, Transition rejects every event regardless
	// of the token presented.
	unminted := newCreatedState(t, 0)
	if _, _, err := lifecycle.Transition(unminted, lifecycle.Input{
		EventTokenHash: guardToken[:], EventRunID: "r-1", EventSeq: 1,
		CommandExited: &runnerv1.CommandExited{ExitCode: 0},
	}, guardT0); !errors.Is(err, lifecycle.ErrUnauthorizedRunner) {
		t.Errorf("event before token mint: err = %v, want ErrUnauthorizedRunner", err)
	}
	if _, _, err := lifecycle.Transition(unminted, lifecycle.Input{
		EventRunID: "r-1", EventSeq: 1, CommandExited: &runnerv1.CommandExited{ExitCode: 0},
	}, guardT0); !errors.Is(err, lifecycle.ErrUnauthorizedRunner) {
		t.Errorf("tokenless event before token mint: err = %v, want ErrUnauthorizedRunner", err)
	}
}

// One sandbox crash consumes exactly one retry, however many sweeps observe the
// actor still CRASHED before REVERT_ACTOR finishes.
func TestRepeatedCrashObservationConsumesOneRetry(t *testing.T) {
	s := newWorkingState(t, 1)
	s = mustTransition(t, s, crashObserved(), guardT0.Add(15*time.Second))
	if s.Task.Status.State != v1alpha1.TaskState_TASK_STATE_QUEUED || s.Task.Status.Attempt != 2 {
		t.Fatalf("first observation: state=%s attempt=%d, want QUEUED attempt 2",
			s.Task.Status.State, s.Task.Status.Attempt)
	}
	before := actionIDs(s.Actions)
	if len(before) != 2 {
		t.Fatalf("first observation: actions = %v, want [REVERT_ACTOR, RESUME_ACTOR]", before)
	}

	for i := 2; i <= 4; i++ {
		s = mustTransition(t, s, crashObserved(), guardT0.Add(time.Duration(i)*15*time.Second))
		if s.Task.Status.State != v1alpha1.TaskState_TASK_STATE_QUEUED || s.Task.Status.Attempt != 2 {
			t.Fatalf("observation %d of the same crash: state=%s attempt=%d, want QUEUED attempt 2",
				i, s.Task.Status.State, s.Task.Status.Attempt)
		}
		if got := actionIDs(s.Actions); len(got) != 2 || got[0] != before[0] || got[1] != before[1] {
			t.Fatalf("observation %d replaced pending actions: %v -> %v", i, before, got)
		}
	}

	// Once the revert and resume complete, a new crash is a new failure: with
	// max_retries = 1 already spent it is terminal.
	s = completeHead(t, s)
	s = completeHead(t, s)
	s = mustTransition(t, s, crashObserved(), guardT0.Add(5*time.Minute))
	if s.Task.Status.State != v1alpha1.TaskState_TASK_STATE_FAILED {
		t.Fatalf("second, distinct crash: state=%s, want FAILED", s.Task.Status.State)
	}
}

// A sweep that finds no actor for a task still waiting on CREATE_ACTOR must not
// fail it: the actor is not supposed to exist yet.
func TestMissingActorIgnoredUntilCreated(t *testing.T) {
	missing := lifecycle.Input{ObservedActor: &lifecycle.ObservedActorInput{MissingConfirmed: true}}

	s := newCreatedState(t, 0)
	s = mustTransition(t, s, missing, guardT0.Add(15*time.Second))
	if s.Task.Status.State != v1alpha1.TaskState_TASK_STATE_QUEUED || len(s.Actions) != 4 {
		t.Fatalf("before CREATE_ACTOR: state=%s actions=%v, want QUEUED with 4 provisioning actions",
			s.Task.Status.State, actionIDs(s.Actions))
	}

	// After the actor has been created, a confirmed-missing actor is fatal.
	working := newWorkingState(t, 0)
	working = mustTransition(t, working, missing, guardT0.Add(15*time.Second))
	if working.Task.Status.State != v1alpha1.TaskState_TASK_STATE_FAILED || working.ActorUID != "" {
		t.Fatalf("after CREATE_ACTOR: state=%s actorUID=%q, want FAILED with cleared actor UID",
			working.Task.Status.State, working.ActorUID)
	}
}

// Only a task whose runner is expected to be alive can lose it.
func TestHeartbeatLostOnlyWhenRunnerExpected(t *testing.T) {
	lost := lifecycle.Input{HeartbeatLost: &lifecycle.HeartbeatLostInput{SilentFor: 3 * time.Minute}}
	at := guardT0.Add(time.Hour)

	for name, s := range map[string]lifecycle.State{
		"IDLE":      newIdleState(t),
		"SUSPENDED": newSuspendedState(t),
		"SUSPENDING": mustTransition(t, newWorkingState(t, 0),
			lifecycle.Input{Suspend: &lifecycle.SuspendInput{}}, guardT0),
		"QUEUED provisioning":  newCreatedState(t, 0),
		"QUEUED crash backoff": mustTransition(t, newWorkingState(t, 1), crashObserved(), guardT0),
	} {
		got := mustTransition(t, s, lost, at)
		if got.Task.Status.State != s.Task.Status.State || got.Task.Status.Attempt != s.Task.Status.Attempt {
			t.Errorf("%s + HeartbeatLost: state %s attempt %d -> %s attempt %d, want unchanged", name,
				s.Task.Status.State, s.Task.Status.Attempt, got.Task.Status.State, got.Task.Status.Attempt)
		}
		if a, b := actionIDs(s.Actions), actionIDs(got.Actions); len(a) != len(b) || (len(a) > 0 && a[0] != b[0]) {
			t.Errorf("%s + HeartbeatLost: actions %v -> %v, want unchanged", name, a, b)
		}
	}

	// A WORKING runner going silent is a failure (terminal without retries)...
	if got := mustTransition(t, newWorkingState(t, 0), lost, at); got.Task.Status.State != v1alpha1.TaskState_TASK_STATE_FAILED {
		t.Errorf("WORKING + HeartbeatLost, no retries: state=%s, want FAILED", got.Task.Status.State)
	}
	// ...and so is one that claimed its run but died before command_started.
	claimed := newClaimedState(t, 1)
	got := mustTransition(t, claimed, lost, at)
	if got.Task.Status.State != v1alpha1.TaskState_TASK_STATE_QUEUED || got.Task.Status.Attempt != 2 ||
		len(got.Actions) != 2 || got.Actions[0].Kind != lifecycle.ActionRevertActor {
		t.Errorf("claimed QUEUED + HeartbeatLost, 1 retry: state=%s attempt=%d actions=%v, want QUEUED attempt 2 [REVERT_ACTOR, RESUME_ACTOR]",
			got.Task.Status.State, got.Task.Status.Attempt, actionIDs(got.Actions))
	}
}

// Re-observing the same drift must not replace an equivalent pending action
// with a new ID: workers fence completions on the head action ID.
func TestRepeatedObservationKeepsHeadActionID(t *testing.T) {
	// Terminal task whose actor is still RUNNING: the park is already queued.
	s := newWorkingState(t, 0)
	s = mustTransition(t, s, lifecycle.Input{
		EventTokenHash: guardToken[:], EventRunID: s.ActiveRunID, EventSeq: 2,
		CommandExited: &runnerv1.CommandExited{ExitCode: 0},
	}, guardT0)
	if s.Task.Status.State != v1alpha1.TaskState_TASK_STATE_COMPLETED || len(s.Actions) != 1 {
		t.Fatalf("setup: state=%s actions=%v", s.Task.Status.State, actionIDs(s.Actions))
	}
	head := s.Actions[0].ID
	running := lifecycle.Input{ObservedActor: &lifecycle.ObservedActorInput{
		ActorState: ateapipb.ActorState_ACTOR_STATE_RUNNING,
	}}
	s = mustTransition(t, s, running, guardT0.Add(15*time.Second))
	if len(s.Actions) != 1 || s.Actions[0].ID != head {
		t.Errorf("terminal + RUNNING: actions %v, want head %s unchanged", actionIDs(s.Actions), head)
	}
	// A terminal actor woken after its park completed still gets re-parked.
	s = completeHead(t, s)
	s = mustTransition(t, s, running, guardT0.Add(time.Minute))
	if len(s.Actions) != 1 || s.Actions[0].Kind != lifecycle.ActionSuspendActorSync ||
		s.Actions[0].SuspendReason != lifecycle.SuspendReasonTerminal {
		t.Errorf("terminal woken after park: actions %v, want a new terminal SUSPEND_ACTOR_SYNC", actionIDs(s.Actions))
	}

	// Durably parked tasks whose actor is CRASHED revert in place, once.
	for name, parked := range map[string]lifecycle.State{
		"IDLE":      newIdleState(t),
		"SUSPENDED": newSuspendedState(t),
	} {
		want := parked.Task.Status.State
		parked = mustTransition(t, parked, crashObserved(), guardT0.Add(15*time.Second))
		if parked.Task.Status.State != want || parked.Task.Status.Attempt != 1 ||
			len(parked.Actions) != 1 || parked.Actions[0].Kind != lifecycle.ActionRevertActor {
			t.Fatalf("%s + CRASHED: state=%s attempt=%d actions=%v, want %s attempt 1 [REVERT_ACTOR]",
				name, parked.Task.Status.State, parked.Task.Status.Attempt, actionIDs(parked.Actions), want)
		}
		revertID := parked.Actions[0].ID
		parked = mustTransition(t, parked, crashObserved(), guardT0.Add(30*time.Second))
		if len(parked.Actions) != 1 || parked.Actions[0].ID != revertID {
			t.Errorf("%s + CRASHED twice: actions %v, want head %s unchanged", name, actionIDs(parked.Actions), revertID)
		}
	}
}

// ResumeTask on a parked task whose crashed actor still awaits REVERT_ACTOR
// keeps the revert ahead of the resume.
func TestResumeKeepsPendingRevert(t *testing.T) {
	s := newIdleState(t)
	s = mustTransition(t, s, crashObserved(), guardT0.Add(15*time.Second))
	revertID := s.Actions[0].ID
	s = mustTransition(t, s, lifecycle.Input{Resume: &lifecycle.ResumeInput{}}, guardT0.Add(16*time.Second))
	if s.Task.Status.State != v1alpha1.TaskState_TASK_STATE_QUEUED || len(s.Actions) != 2 ||
		s.Actions[0].ID != revertID || s.Actions[1].Kind != lifecycle.ActionResumeActor {
		t.Fatalf("state=%s actions=%v, want QUEUED [REVERT_ACTOR:%s, RESUME_ACTOR]",
			s.Task.Status.State, actionIDs(s.Actions), revertID)
	}

	// Without a pending revert, resume queues only RESUME_ACTOR.
	plain := mustTransition(t, newIdleState(t), lifecycle.Input{Resume: &lifecycle.ResumeInput{}}, guardT0)
	if len(plain.Actions) != 1 || plain.Actions[0].Kind != lifecycle.ActionResumeActor {
		t.Fatalf("plain resume: actions=%v, want [RESUME_ACTOR]", actionIDs(plain.Actions))
	}
}

// newResumingState returns a provisioned QUEUED task whose only pending action
// is RESUME_ACTOR: the worker is calling ResumeActor and the runner is about to
// claim.
func newResumingState(t *testing.T, workspaceReady bool) lifecycle.State {
	t.Helper()
	s := mustTransition(t, lifecycle.State{}, lifecycle.Input{Create: &lifecycle.CreateInput{
		Task: &v1alpha1.Task{
			Metadata: &v1alpha1.ObjectMeta{Atespace: "payments", Name: "resuming", Uid: "task-uid"},
			Spec:     &v1alpha1.TaskSpec{Commands: []string{"pytest"}},
		},
		AteActorTemplate: "ax-0123456789abcdef",
		WorkspaceReady:   workspaceReady,
	}}, guardT0)
	for len(s.Actions) > 1 {
		a := s.Actions[0]
		s = mustTransition(t, s, lifecycle.Input{ActionCompleted: &lifecycle.ActionCompletedInput{
			ActionID: a.ID, Kind: a.Kind, ActorUID: "actor-uid-1",
		}}, guardT0)
	}
	if len(s.Actions) != 1 || s.Actions[0].Kind != lifecycle.ActionResumeActor {
		t.Fatalf("setup: actions = %v, want [RESUME_ACTOR]", actionIDs(s.Actions))
	}
	return s
}

func firstClaim(nonce string, ws *v1alpha1.Workspace) lifecycle.Input {
	return lifecycle.Input{Claim: &lifecycle.ClaimInput{
		Request:              &runnerv1.ClaimAssignmentRequest{Nonce: nonce},
		CandidateRunnerToken: "runner-token",
		CandidateTokenHash:   guardToken[:],
		Workspace:            ws,
	}}
}

// A permanent failure to park or revert the actor of a terminal task must not
// rewrite the task's state or outcome, and must not be retried forever.
func TestActionFailedOnTerminalTaskKeepsOutcomeAndComesToRest(t *testing.T) {
	failHead := func(s lifecycle.State, msg string) lifecycle.State {
		t.Helper()
		a := s.Actions[0]
		return mustTransition(t, s, lifecycle.Input{ActionFailed: &lifecycle.ActionFailedInput{
			ActionID: a.ID, Kind: a.Kind, Message: msg,
		}}, guardT0.Add(time.Minute))
	}

	// COMPLETED with its park still pending.
	done := newWorkingState(t, 0)
	done = mustTransition(t, done, lifecycle.Input{
		EventTokenHash: guardToken[:], EventRunID: "r-1", EventSeq: 2,
		CommandExited: &runnerv1.CommandExited{ExitCode: 0, Output: "all tests pass"},
	}, guardT0)
	if done.Task.Status.State != v1alpha1.TaskState_TASK_STATE_COMPLETED || len(done.Actions) != 1 {
		t.Fatalf("setup: state=%s actions=%v", done.Task.Status.State, actionIDs(done.Actions))
	}
	got := failHead(done, "actor not found")
	if got.Task.Status.State != v1alpha1.TaskState_TASK_STATE_COMPLETED ||
		got.Task.Status.Outcome.GetResult() != v1alpha1.TaskResult_TASK_RESULT_SUCCEEDED ||
		got.Task.Status.Outcome.GetOutput() != "all tests pass" ||
		got.Task.Status.StatusDetail != nil {
		t.Errorf("COMPLETED + failed park: state=%s outcome=%+v detail=%+v, want COMPLETED/SUCCEEDED with its output",
			got.Task.Status.State, got.Task.Status.Outcome, got.Task.Status.StatusDetail)
	}
	if len(got.Actions) != 0 || lifecycle.NextReconcileAt(got, guardT0.Add(time.Minute)) != nil {
		t.Errorf("COMPLETED + failed park: actions=%v, want none and the row at rest", actionIDs(got.Actions))
	}

	// COMPLETED and parked, then observed CRASHED: the in-place revert fails.
	parked := completeHead(t, done)
	parked = mustTransition(t, parked, crashObserved(), guardT0.Add(30*time.Second))
	if len(parked.Actions) != 1 || parked.Actions[0].Kind != lifecycle.ActionRevertActor {
		t.Fatalf("setup: actions = %v, want [REVERT_ACTOR]", actionIDs(parked.Actions))
	}
	got = failHead(parked, "no snapshot to revert to")
	if got.Task.Status.State != v1alpha1.TaskState_TASK_STATE_COMPLETED ||
		got.Task.Status.Outcome.GetResult() != v1alpha1.TaskResult_TASK_RESULT_SUCCEEDED || len(got.Actions) != 0 {
		t.Errorf("COMPLETED + failed revert: state=%s result=%s actions=%v, want COMPLETED/SUCCEEDED at rest",
			got.Task.Status.State, got.Task.Status.Outcome.GetResult(), actionIDs(got.Actions))
	}

	// A provisioning failure makes the task FAILED and queues one park; when
	// that park also fails permanently the task comes to rest, not into a loop.
	failing := failHead(newResumingState(t, true), "permission denied")
	if failing.Task.Status.State != v1alpha1.TaskState_TASK_STATE_FAILED || len(failing.Actions) != 1 ||
		failing.Actions[0].Kind != lifecycle.ActionSuspendActorSync {
		t.Fatalf("RESUME_ACTOR failed: state=%s actions=%v, want FAILED [SUSPEND_ACTOR_SYNC]",
			failing.Task.Status.State, actionIDs(failing.Actions))
	}
	detail := failing.Task.Status.StatusDetail.GetDetail()
	failing = failHead(failing, "permission denied")
	if failing.Task.Status.State != v1alpha1.TaskState_TASK_STATE_FAILED || len(failing.Actions) != 0 ||
		failing.Task.Status.StatusDetail.GetDetail() != detail {
		t.Errorf("FAILED + failed park: state=%s actions=%v detail=%q, want FAILED at rest with the original detail %q",
			failing.Task.Status.State, actionIDs(failing.Actions), failing.Task.Status.StatusDetail.GetDetail(), detail)
	}
}

// ResumeActor returns when /readyz answers, which is also when the runner
// claims, so the claim and the worker's RESUME_ACTOR completion commit in
// either order. Both orders must leave the task in the same place.
func TestClaimAndResumeCompletionCommute(t *testing.T) {
	ws := &v1alpha1.Workspace{Metadata: &v1alpha1.ObjectMeta{Name: "ws-1"}}
	completeResume := func(s lifecycle.State, id string, at time.Time) lifecycle.State {
		t.Helper()
		return mustTransition(t, s, lifecycle.Input{ActionCompleted: &lifecycle.ActionCompletedInput{
			ActionID: id, Kind: lifecycle.ActionResumeActor,
		}}, at)
	}
	watchdog := lifecycle.Input{TimerExpired: &lifecycle.TimerExpiredInput{}}

	for _, order := range []string{"completion then claim", "claim then completion"} {
		t.Run("long setup is not failed by the claim watchdog/"+order, func(t *testing.T) {
			s := newResumingState(t, false)
			resumeID := s.Actions[0].ID
			var out lifecycle.Output
			var err error
			if order == "completion then claim" {
				s = completeResume(s, resumeID, guardT0.Add(time.Second))
				s, out, err = lifecycle.Transition(s, firstClaim("n-1", ws), guardT0.Add(2*time.Second))
			} else {
				s, out, err = lifecycle.Transition(s, firstClaim("n-1", ws), guardT0.Add(time.Second))
				if err == nil {
					s = completeResume(s, resumeID, guardT0.Add(2*time.Second))
				}
			}
			if err != nil || out.Assignment.GetMode() != runnerv1.AssignmentMode_ASSIGNMENT_MODE_SETUP {
				t.Fatalf("claim = (%v, %v), want SETUP", out.Assignment, err)
			}
			if s.ClaimDeadline != nil || len(s.Actions) != 0 {
				t.Errorf("after claim and completion: ClaimDeadline=%v actions=%v, want no watchdog and no actions",
					s.ClaimDeadline, actionIDs(s.Actions))
			}
			if s.LastResumedAt == nil {
				t.Error("LastResumedAt not recorded")
			}
			// Setup is still running six minutes later.
			s = mustTransition(t, s, watchdog, guardT0.Add(6*time.Minute))
			if s.Task.Status.State != v1alpha1.TaskState_TASK_STATE_QUEUED {
				t.Errorf("TimerExpired during setup: state=%s detail=%q, want QUEUED",
					s.Task.Status.State, s.Task.Status.StatusDetail.GetDetail())
			}
		})

		t.Run("lost first claim response can be retried/"+order, func(t *testing.T) {
			s := newResumingState(t, true)
			resumeID := s.Actions[0].ID
			if order == "completion then claim" {
				s = completeResume(s, resumeID, guardT0.Add(time.Second))
				s = mustTransition(t, s, firstClaim("n-1", nil), guardT0.Add(2*time.Second))
			} else {
				s = mustTransition(t, s, firstClaim("n-1", nil), guardT0.Add(time.Second))
				s = completeResume(s, resumeID, guardT0.Add(2*time.Second))
			}
			// The HTTP response carrying the token was lost; the runner retries
			// with the same nonce and no token.
			retryToken := sha256.Sum256([]byte("runner-token-2"))
			s, out, err := lifecycle.Transition(s, lifecycle.Input{Claim: &lifecycle.ClaimInput{
				Request:              &runnerv1.ClaimAssignmentRequest{Nonce: "n-1"},
				CandidateRunnerToken: "runner-token-2",
				CandidateTokenHash:   retryToken[:],
			}}, guardT0.Add(3*time.Second))
			if err != nil || out.Assignment.GetMode() != runnerv1.AssignmentMode_ASSIGNMENT_MODE_RUN ||
				out.Assignment.GetRunId() != "r-1" || out.Assignment.GetRunnerToken() != "runner-token-2" {
				t.Fatalf("retried claim = (%+v, %v), want RUN r-1 with a re-minted token", out.Assignment, err)
			}
			if s.RunSeq != 1 {
				t.Errorf("RunSeq = %d, want 1 (the retry must not mint a second run)", s.RunSeq)
			}
		})
	}
}

// A claim that wakes a parked task records the resume time that lost-runner
// detection measures silence from.
func TestClaimWakeRecordsResumeTime(t *testing.T) {
	woke := guardT0.Add(2 * time.Hour)
	for name, parked := range map[string]lifecycle.State{
		"IDLE":      newIdleState(t),
		"SUSPENDED": newSuspendedState(t),
	} {
		s := mustTransition(t, parked, lifecycle.Input{Claim: &lifecycle.ClaimInput{
			Request:           &runnerv1.ClaimAssignmentRequest{Nonce: "n-wake", LastStartedRunId: "r-1"},
			ProvidedTokenHash: guardToken[:],
		}}, woke)
		if s.LastResumedAt == nil || !s.LastResumedAt.Equal(woke) {
			t.Errorf("%s woken by a claim: LastResumedAt = %v, want %v", name, s.LastResumedAt, woke)
		}
	}
}

// An IDLE task keeps its run: a wakeup that finds the process alive (MEMORY
// fidelity) resumes it, and a cold restore is given a new run.
func TestIdleWakeResumesLiveRunOrStartsNewOne(t *testing.T) {
	idle := newIdleState(t)
	if idle.ActiveRunID != "r-1" {
		t.Fatalf("IDLE ActiveRunID = %q, want r-1", idle.ActiveRunID)
	}

	live, out, err := lifecycle.Transition(idle, lifecycle.Input{Claim: &lifecycle.ClaimInput{
		Request: &runnerv1.ClaimAssignmentRequest{
			Nonce: "n-live", InMemoryRunId: "r-1", LastStartedRunId: "r-1", ChildAlive: true,
		},
		ProvidedTokenHash: guardToken[:],
	}}, guardT0.Add(time.Hour))
	if err != nil || out.Assignment.GetMode() != runnerv1.AssignmentMode_ASSIGNMENT_MODE_RESUME ||
		out.Assignment.GetRunId() != "r-1" || live.Task.Status.State != v1alpha1.TaskState_TASK_STATE_WORKING {
		t.Errorf("live child: assignment=(%s %q) state=%s err=%v, want RESUME r-1 and WORKING",
			out.Assignment.GetMode(), out.Assignment.GetRunId(), live.Task.Status.State, err)
	}

	cold, out, err := lifecycle.Transition(idle, lifecycle.Input{Claim: &lifecycle.ClaimInput{
		Request:           &runnerv1.ClaimAssignmentRequest{Nonce: "n-cold", LastStartedRunId: "r-1"},
		ProvidedTokenHash: guardToken[:],
	}}, guardT0.Add(time.Hour))
	if err != nil || out.Assignment.GetMode() != runnerv1.AssignmentMode_ASSIGNMENT_MODE_RUN ||
		out.Assignment.GetRunId() != "r-2" || cold.Task.Status.State != v1alpha1.TaskState_TASK_STATE_QUEUED {
		t.Errorf("cold restore: assignment=(%s %q) state=%s err=%v, want RUN r-2 and QUEUED",
			out.Assignment.GetMode(), out.Assignment.GetRunId(), cold.Task.Status.State, err)
	}
}

// A command that exits while its actor is being checkpointed is recorded, so a
// cold restore does not forget the run finished and execute it again.
func TestCommandExitDuringCheckpointIsRecorded(t *testing.T) {
	suspending := func(maxRetries int32) lifecycle.State {
		t.Helper()
		s := mustTransition(t, newWorkingState(t, maxRetries), lifecycle.Input{Suspend: &lifecycle.SuspendInput{}}, guardT0)
		if s.Task.Status.State != v1alpha1.TaskState_TASK_STATE_SUSPENDING || len(s.Actions) != 1 {
			t.Fatalf("setup: state=%s actions=%v", s.Task.Status.State, actionIDs(s.Actions))
		}
		return s
	}
	exit := func(code int32) lifecycle.Input {
		return lifecycle.Input{
			EventTokenHash: guardToken[:], EventRunID: "r-1", EventSeq: 2,
			CommandExited: &runnerv1.CommandExited{ExitCode: code, Output: "result"},
		}
	}

	t.Run("success completes the task and keeps the in-flight checkpoint", func(t *testing.T) {
		s := suspending(0)
		head := s.Actions[0].ID
		s, out, err := lifecycle.Transition(s, exit(0), guardT0.Add(time.Second))
		if err != nil || out.Assignment.GetMode() != runnerv1.AssignmentMode_ASSIGNMENT_MODE_IDLE {
			t.Fatalf("exit 0 = (%v, %v), want next IDLE", out.Assignment, err)
		}
		if s.Task.Status.State != v1alpha1.TaskState_TASK_STATE_COMPLETED ||
			s.Task.Status.Outcome.GetOutput() != "result" || s.LastAckedSeq != 2 {
			t.Errorf("state=%s outcome=%+v acked=%d, want COMPLETED with its output and seq 2 acknowledged",
				s.Task.Status.State, s.Task.Status.Outcome, s.LastAckedSeq)
		}
		if len(s.Actions) != 1 || s.Actions[0].ID != head || s.Actions[0].SuspendReason != lifecycle.SuspendReasonTerminal {
			t.Fatalf("actions = %+v, want the same checkpoint %s retargeted to a terminal park", s.Actions, head)
		}
		s = completeHead(t, s)
		if s.Task.Status.State != v1alpha1.TaskState_TASK_STATE_COMPLETED || len(s.Actions) != 0 {
			t.Errorf("after the checkpoint: state=%s actions=%v, want COMPLETED at rest", s.Task.Status.State, actionIDs(s.Actions))
		}
	})

	t.Run("failure with a retry left is retried after the checkpoint", func(t *testing.T) {
		s := suspending(1)
		head := s.Actions[0].ID
		s, out, err := lifecycle.Transition(s, exit(1), guardT0.Add(time.Second))
		if err != nil || out.Assignment.GetMode() != runnerv1.AssignmentMode_ASSIGNMENT_MODE_IDLE {
			t.Fatalf("exit 1 = (%v, %v), want next IDLE while the actor is checkpointed", out.Assignment, err)
		}
		// The failed run is over and the retry is counted, but its run is only
		// minted when a claim hands it out; the exit stays acknowledged.
		if s.Task.Status.State != v1alpha1.TaskState_TASK_STATE_SUSPENDING || s.Task.Status.Attempt != 2 ||
			s.ActiveRunID != "" || s.RunSeq != 1 || s.LastAckedSeq != 2 || len(s.Actions) != 1 || s.Actions[0].ID != head {
			t.Fatalf("state=%s attempt=%d run=%q runSeq=%d acked=%d actions=%v, want SUSPENDING attempt 2 with no active run, seq 2 acknowledged and checkpoint %s still pending",
				s.Task.Status.State, s.Task.Status.Attempt, s.ActiveRunID, s.RunSeq, s.LastAckedSeq, actionIDs(s.Actions), head)
		}
		s = completeHead(t, s)
		if s.Task.Status.State != v1alpha1.TaskState_TASK_STATE_SUSPENDED {
			t.Fatalf("after the checkpoint: state=%s, want SUSPENDED (the user's suspend is honored)", s.Task.Status.State)
		}
		s = mustTransition(t, s, lifecycle.Input{Resume: &lifecycle.ResumeInput{}}, guardT0.Add(time.Minute))
		s, out, err = lifecycle.Transition(s, lifecycle.Input{Claim: &lifecycle.ClaimInput{
			Request:           &runnerv1.ClaimAssignmentRequest{Nonce: "n-2", LastStartedRunId: "r-1"},
			ProvidedTokenHash: guardToken[:],
		}}, guardT0.Add(2*time.Minute))
		if err != nil || out.Assignment.GetMode() != runnerv1.AssignmentMode_ASSIGNMENT_MODE_RUN ||
			out.Assignment.GetRunId() != "r-2" || out.Assignment.GetGoal().GetPreviousFailure() != "result" || s.RunSeq != 2 {
			t.Errorf("claim after resume = (%+v, %v) RunSeq=%d, want RUN r-2 carrying the previous failure and no third run",
				out.Assignment, err, s.RunSeq)
		}
	})

	t.Run("failure without retries is terminal", func(t *testing.T) {
		s := suspending(0)
		head := s.Actions[0].ID
		s = mustTransition(t, s, exit(3), guardT0.Add(time.Second))
		if s.Task.Status.State != v1alpha1.TaskState_TASK_STATE_FAILED || s.Task.Status.Outcome.GetExitCode() != 3 ||
			s.Task.Status.Outcome.GetOutput() != "result" {
			t.Errorf("state=%s outcome=%+v, want FAILED with exit code 3 and its output", s.Task.Status.State, s.Task.Status.Outcome)
		}
		if len(s.Actions) != 1 || s.Actions[0].ID != head || s.Actions[0].SuspendReason != lifecycle.SuspendReasonTerminal {
			t.Errorf("actions = %+v, want the same checkpoint %s retargeted to a terminal park", s.Actions, head)
		}
	})
}

// Setup precedes every run, so a setup event that arrives after a run was
// assigned is a late duplicate. It must not advance the acknowledged sequence
// (reset for the run) or tell the runner to idle.
func TestLateSetupFinishedIgnoredOnceRunAssigned(t *testing.T) {
	s := newClaimedState(t, 0)
	if s.ActiveRunID != "r-1" || s.LastAckedSeq != 0 {
		t.Fatalf("setup: run=%q acked=%d", s.ActiveRunID, s.LastAckedSeq)
	}
	s, out, err := lifecycle.Transition(s, lifecycle.Input{
		EventTokenHash: guardToken[:], EventSeq: 1,
		SetupFinished: &runnerv1.SetupFinished{Ok: true},
	}, guardT0.Add(time.Second))
	if err != nil || out.Assignment != nil || s.LastAckedSeq != 0 {
		t.Fatalf("late setup_finished = (assignment %v, acked %d, err %v), want it ignored", out.Assignment, s.LastAckedSeq, err)
	}
	s = mustTransition(t, s, lifecycle.Input{
		EventTokenHash: guardToken[:], EventRunID: "r-1", EventSeq: 1,
		CommandStarted: &runnerv1.CommandStarted{Pid: 42},
	}, guardT0.Add(2*time.Second))
	if s.Task.Status.State != v1alpha1.TaskState_TASK_STATE_WORKING {
		t.Errorf("command_started(r-1, seq 1) after the late setup event: state=%s, want WORKING", s.Task.Status.State)
	}
}

// An observation that names a different actor UID than the task's own actor is
// about another incarnation of the name and must not affect the task.
func TestObservationOfAnotherActorIncarnationIgnored(t *testing.T) {
	working := newWorkingState(t, 0)
	if working.ActorUID != "actor-uid-1" {
		t.Fatalf("setup: ActorUID = %q", working.ActorUID)
	}
	for name, obs := range map[string]*lifecycle.ObservedActorInput{
		"crashed": {ActorState: ateapipb.ActorState_ACTOR_STATE_CRASHED, ActorUID: "actor-uid-0"},
		"missing": {MissingConfirmed: true, ActorUID: "actor-uid-0"},
	} {
		got := mustTransition(t, working, lifecycle.Input{ObservedActor: obs}, guardT0.Add(15*time.Second))
		if got.Task.Status.State != v1alpha1.TaskState_TASK_STATE_WORKING || len(got.Actions) != 0 {
			t.Errorf("%s observation of another actor UID: state=%s actions=%v, want WORKING untouched",
				name, got.Task.Status.State, actionIDs(got.Actions))
		}
	}
	// The same observation for this task's actor, or one without a UID, applies.
	for name, uid := range map[string]string{"own UID": "actor-uid-1", "no UID": ""} {
		got := mustTransition(t, working, lifecycle.Input{ObservedActor: &lifecycle.ObservedActorInput{
			ActorState: ateapipb.ActorState_ACTOR_STATE_CRASHED, ActorUID: uid,
		}}, guardT0.Add(15*time.Second))
		if got.Task.Status.State != v1alpha1.TaskState_TASK_STATE_FAILED {
			t.Errorf("crash observed with %s: state=%s, want FAILED", name, got.Task.Status.State)
		}
	}
}

// SuspendTask accepted while workspace setup is running must still end in
// SUSPENDED when setup finishes before the checkpoint does.
func TestSetupFinishedKeepsUserSuspend(t *testing.T) {
	ws := &v1alpha1.Workspace{Metadata: &v1alpha1.ObjectMeta{Name: "ws-1"}}
	s := newResumingState(t, false)
	s = completeHead(t, s)
	s, out, err := lifecycle.Transition(s, firstClaim("n-1", ws), guardT0.Add(time.Second))
	if err != nil || out.Assignment.GetMode() != runnerv1.AssignmentMode_ASSIGNMENT_MODE_SETUP {
		t.Fatalf("claim = (%v, %v), want SETUP", out.Assignment, err)
	}
	s = mustTransition(t, s, lifecycle.Input{Suspend: &lifecycle.SuspendInput{}}, guardT0.Add(2*time.Second))
	if s.Task.Status.State != v1alpha1.TaskState_TASK_STATE_SUSPENDING || len(s.Actions) != 1 {
		t.Fatalf("suspend during setup: state=%s actions=%v", s.Task.Status.State, actionIDs(s.Actions))
	}
	suspendID := s.Actions[0].ID

	s, out, err = lifecycle.Transition(s, lifecycle.Input{
		EventTokenHash: guardToken[:], EventSeq: 1,
		SetupFinished: &runnerv1.SetupFinished{Ok: true},
	}, guardT0.Add(3*time.Second))
	if err != nil || out.Assignment.GetMode() != runnerv1.AssignmentMode_ASSIGNMENT_MODE_IDLE {
		t.Fatalf("setup_finished = (%v, %v), want next IDLE", out.Assignment, err)
	}
	if len(s.Actions) != 1 || s.Actions[0].ID != suspendID || s.Actions[0].SuspendReason != lifecycle.SuspendReasonUser {
		t.Fatalf("actions = %+v, want the user's checkpoint %s kept", s.Actions, suspendID)
	}
	s = completeHead(t, s)
	if s.Task.Status.State != v1alpha1.TaskState_TASK_STATE_SUSPENDED || len(s.Actions) != 0 {
		t.Fatalf("after the checkpoint: state=%s actions=%v, want SUSPENDED at rest", s.Task.Status.State, actionIDs(s.Actions))
	}

	// Resuming starts the first run without repeating setup.
	s = mustTransition(t, s, lifecycle.Input{Resume: &lifecycle.ResumeInput{}}, guardT0.Add(time.Minute))
	_, out, err = lifecycle.Transition(s, lifecycle.Input{Claim: &lifecycle.ClaimInput{
		Request:           &runnerv1.ClaimAssignmentRequest{Nonce: "n-2"},
		ProvidedTokenHash: guardToken[:],
		Workspace:         ws,
	}}, guardT0.Add(2*time.Minute))
	if err != nil || out.Assignment.GetMode() != runnerv1.AssignmentMode_ASSIGNMENT_MODE_RUN || out.Assignment.GetRunId() != "r-1" {
		t.Errorf("claim after resume = (%+v, %v), want RUN r-1", out.Assignment, err)
	}
}

// Events are numbered from 1 within each execution (one setup pass or one run),
// so LastAckedSeq resets to 0 whenever a SETUP or RUN assignment is issued and
// is preserved across RESUME. See RunnerEvent.seq in runner.proto.
func TestEventSequenceRestartsWithEachExecution(t *testing.T) {
	ws := &v1alpha1.Workspace{Metadata: &v1alpha1.ObjectMeta{Name: "ws-1"}}
	event := func(runID string, seq int64) lifecycle.Input {
		return lifecycle.Input{EventTokenHash: guardToken[:], EventRunID: runID, EventSeq: seq}
	}
	claim := func(nonce string, req *runnerv1.ClaimAssignmentRequest, w *v1alpha1.Workspace) lifecycle.Input {
		req.Nonce = nonce
		return lifecycle.Input{Claim: &lifecycle.ClaimInput{Request: req, ProvidedTokenHash: guardToken[:], Workspace: w}}
	}
	// evictAndResume applies an eviction notice at seq, then completes the
	// checkpoint and the resume that follows it.
	evictAndResume := func(s lifecycle.State, runID string, seq int64) lifecycle.State {
		t.Helper()
		in := event(runID, seq)
		in.EvictionNotice = &runnerv1.EvictionNotice{}
		s = mustTransition(t, s, in, guardT0.Add(time.Second))
		if s.Task.Status.State != v1alpha1.TaskState_TASK_STATE_SUSPENDING || s.LastAckedSeq != seq {
			t.Fatalf("eviction_notice(seq %d): state=%s acked=%d, want SUSPENDING with it applied", seq, s.Task.Status.State, s.LastAckedSeq)
		}
		s = completeHead(t, s)    // SUSPEND_ACTOR_SYNC
		return completeHead(t, s) // RESUME_ACTOR
	}

	t.Run("a repeated setup pass starts again at 1", func(t *testing.T) {
		s := completeHead(t, newResumingState(t, false))
		s, out, err := lifecycle.Transition(s, firstClaim("n-1", ws), guardT0)
		if err != nil || out.Assignment.GetMode() != runnerv1.AssignmentMode_ASSIGNMENT_MODE_SETUP {
			t.Fatalf("first claim = (%v, %v), want SETUP", out.Assignment, err)
		}
		s = evictAndResume(s, "", 1)

		s, out, err = lifecycle.Transition(s, claim("n-2", &runnerv1.ClaimAssignmentRequest{}, ws), guardT0.Add(time.Minute))
		if err != nil || out.Assignment.GetMode() != runnerv1.AssignmentMode_ASSIGNMENT_MODE_SETUP || s.LastAckedSeq != 0 {
			t.Fatalf("claim after eviction = (%v, %v) acked=%d, want SETUP with the sequence restarted", out.Assignment, err, s.LastAckedSeq)
		}
		in := event("", 1)
		in.SetupFinished = &runnerv1.SetupFinished{Ok: true}
		s = mustTransition(t, s, in, guardT0.Add(2*time.Minute))
		if s.Task.Status.State != v1alpha1.TaskState_TASK_STATE_SUSPENDING || len(s.Actions) != 2 || s.LastAckedSeq != 1 {
			t.Errorf("setup_finished(seq 1) of the second pass: state=%s actions=%v acked=%d, want it applied (setup checkpoint queued)",
				s.Task.Status.State, actionIDs(s.Actions), s.LastAckedSeq)
		}
	})

	t.Run("an unstarted run can be re-issued with seq starting at 1", func(t *testing.T) {
		s := newClaimedState(t, 0) // RUN r-1 handed out; no event reported for it
		// The actor is suspended and resumed before the runner starts r-1.
		s = mustTransition(t, s, lifecycle.Input{Suspend: &lifecycle.SuspendInput{}}, guardT0.Add(time.Second))
		s = completeHead(t, s)
		s = mustTransition(t, s, lifecycle.Input{Resume: &lifecycle.ResumeInput{}}, guardT0.Add(time.Minute))

		s, out, err := lifecycle.Transition(s, claim("n-2", &runnerv1.ClaimAssignmentRequest{}, nil), guardT0.Add(2*time.Minute))
		if err != nil || out.Assignment.GetMode() != runnerv1.AssignmentMode_ASSIGNMENT_MODE_RUN ||
			out.Assignment.GetRunId() != "r-1" || s.RunSeq != 1 || s.LastAckedSeq != 0 {
			t.Fatalf("claim = (%v, %v) runSeq=%d acked=%d, want the same RUN r-1 again", out.Assignment, err, s.RunSeq, s.LastAckedSeq)
		}
		in := event("r-1", 1)
		in.CommandStarted = &runnerv1.CommandStarted{Pid: 42}
		s = mustTransition(t, s, in, guardT0.Add(3*time.Minute))
		if s.Task.Status.State != v1alpha1.TaskState_TASK_STATE_WORKING {
			t.Errorf("command_started(r-1, seq 1): state=%s, want WORKING", s.Task.Status.State)
		}
	})

	t.Run("a run with an applied event is never handed out again", func(t *testing.T) {
		s := newClaimedState(t, 0) // RUN r-1 handed out
		s = evictAndResume(s, "r-1", 1)

		// The runner does not report r-1 as started (its storage was rolled
		// back), but an event was applied for r-1, so it gets a new run. A
		// late event from the first r-1 is then fenced out by its run ID.
		s, out, err := lifecycle.Transition(s, claim("n-2", &runnerv1.ClaimAssignmentRequest{}, nil), guardT0.Add(time.Minute))
		if err != nil || out.Assignment.GetMode() != runnerv1.AssignmentMode_ASSIGNMENT_MODE_RUN ||
			out.Assignment.GetRunId() != "r-2" || s.LastAckedSeq != 0 {
			t.Fatalf("claim after eviction = (%v, %v) acked=%d, want a new RUN r-2", out.Assignment, err, s.LastAckedSeq)
		}
		late := event("r-1", 2)
		late.CommandExited = &runnerv1.CommandExited{ExitCode: 0}
		s, out, err = lifecycle.Transition(s, late, guardT0.Add(2*time.Minute))
		if err != nil || out.Assignment != nil || s.Task.Status.State != v1alpha1.TaskState_TASK_STATE_QUEUED || s.LastAckedSeq != 0 {
			t.Errorf("late command_exited(r-1) = (next %v, err %v) state=%s acked=%d, want it ignored",
				out.Assignment, err, s.Task.Status.State, s.LastAckedSeq)
		}
	})

	t.Run("an in-place retry starts again at 1", func(t *testing.T) {
		s := newWorkingState(t, 1) // command_started was seq 1
		in := event("r-1", 2)
		in.CommandExited = &runnerv1.CommandExited{ExitCode: 1}
		s, out, err := lifecycle.Transition(s, in, guardT0.Add(time.Second))
		if err != nil || out.Assignment.GetMode() != runnerv1.AssignmentMode_ASSIGNMENT_MODE_RUN ||
			out.Assignment.GetRunId() != "r-2" || s.LastAckedSeq != 0 {
			t.Fatalf("failed exit = (%v, %v) acked=%d, want next RUN r-2 with the sequence restarted", out.Assignment, err, s.LastAckedSeq)
		}
		// A resend of the old run's exit (its response was lost) changes nothing
		// and carries no next assignment; the runner then claims again.
		again, out, err := lifecycle.Transition(s, in, guardT0.Add(2*time.Second))
		if err != nil || out.Assignment != nil || again.Task.Status.Attempt != 2 || again.ActiveRunID != "r-2" || again.LastAckedSeq != 0 {
			t.Errorf("resent exit of r-1 = (next %v, err %v) attempt=%d run=%q acked=%d, want it ignored",
				out.Assignment, err, again.Task.Status.Attempt, again.ActiveRunID, again.LastAckedSeq)
		}
		started := event("r-2", 1)
		started.CommandStarted = &runnerv1.CommandStarted{Pid: 43}
		s = mustTransition(t, s, started, guardT0.Add(3*time.Second))
		if s.LastAckedSeq != 1 || s.Task.Status.State != v1alpha1.TaskState_TASK_STATE_WORKING {
			t.Errorf("command_started(r-2, seq 1): acked=%d state=%s, want it applied", s.LastAckedSeq, s.Task.Status.State)
		}
	})

	t.Run("RESUME preserves the current sequence", func(t *testing.T) {
		s := newIdleState(t) // idle_notice was seq 2
		if s.LastAckedSeq != 2 {
			t.Fatalf("setup: acked=%d, want 2", s.LastAckedSeq)
		}
		s, out, err := lifecycle.Transition(s, claim("n-2", &runnerv1.ClaimAssignmentRequest{
			InMemoryRunId: "r-1", LastStartedRunId: "r-1", ChildAlive: true,
		}, nil), guardT0.Add(time.Hour))
		if err != nil || out.Assignment.GetMode() != runnerv1.AssignmentMode_ASSIGNMENT_MODE_RESUME || s.LastAckedSeq != 2 {
			t.Fatalf("wake = (%v, %v) acked=%d, want RESUME with the sequence kept", out.Assignment, err, s.LastAckedSeq)
		}
		// Resending the idle_notice that parked the task is ignored as a duplicate.
		dup := event("r-1", 2)
		dup.IdleNotice = &runnerv1.IdleNotice{}
		s = mustTransition(t, s, dup, guardT0.Add(time.Hour+time.Second))
		if s.Task.Status.State != v1alpha1.TaskState_TASK_STATE_WORKING {
			t.Fatalf("resent idle_notice(seq 2): state=%s, want WORKING (duplicate ignored)", s.Task.Status.State)
		}
		next := event("r-1", 3)
		next.IdleNotice = &runnerv1.IdleNotice{}
		s = mustTransition(t, s, next, guardT0.Add(2*time.Hour))
		if s.Task.Status.State != v1alpha1.TaskState_TASK_STATE_SUSPENDING || s.LastAckedSeq != 3 {
			t.Errorf("idle_notice(seq 3): state=%s acked=%d, want SUSPENDING with it applied", s.Task.Status.State, s.LastAckedSeq)
		}
	})
}

// An event is applied only in the execution it was sent in. An eviction notice
// is about the worker rather than a run, but one sent during an earlier
// execution must not be counted against the current one: it would advance
// LastAckedSeq past the new run's own events and silently drop them.
func TestEventFromEarlierExecutionIsNotApplied(t *testing.T) {
	event := func(runID string, seq int64) lifecycle.Input {
		return lifecycle.Input{EventTokenHash: guardToken[:], EventRunID: runID, EventSeq: seq}
	}

	// One request for r-1 carries its failed exit and an eviction notice.
	s := newWorkingState(t, 1) // command_started was seq 1
	exit := event("r-1", 2)
	exit.CommandExited = &runnerv1.CommandExited{ExitCode: 1}
	s, out, err := lifecycle.Transition(s, exit, guardT0.Add(time.Second))
	if err != nil || out.Assignment.GetRunId() != "r-2" {
		t.Fatalf("failed exit = (%v, %v), want next RUN r-2", out.Assignment, err)
	}
	stale := event("r-1", 3)
	stale.EvictionNotice = &runnerv1.EvictionNotice{}
	s = mustTransition(t, s, stale, guardT0.Add(time.Second))
	if s.Task.Status.State != v1alpha1.TaskState_TASK_STATE_WORKING || s.LastAckedSeq != 0 || len(s.Actions) != 0 {
		t.Fatalf("eviction_notice sent in r-1, applied during r-2: state=%s acked=%d actions=%v, want it ignored",
			s.Task.Status.State, s.LastAckedSeq, actionIDs(s.Actions))
	}

	// r-2's own events are therefore still applied, and it can complete.
	started := event("r-2", 1)
	started.CommandStarted = &runnerv1.CommandStarted{Pid: 43}
	s = mustTransition(t, s, started, guardT0.Add(2*time.Second))
	done := event("r-2", 2)
	done.CommandExited = &runnerv1.CommandExited{ExitCode: 0}
	completed := mustTransition(t, s, done, guardT0.Add(3*time.Second))
	if completed.Task.Status.State != v1alpha1.TaskState_TASK_STATE_COMPLETED {
		t.Errorf("command_exited(r-2, seq 2, exit 0): state=%s, want COMPLETED", completed.Task.Status.State)
	}

	// The runner reports the eviction again in the new execution, and that
	// one is applied.
	again := event("r-2", 2)
	again.EvictionNotice = &runnerv1.EvictionNotice{}
	evicting := mustTransition(t, s, again, guardT0.Add(3*time.Second))
	if evicting.Task.Status.State != v1alpha1.TaskState_TASK_STATE_SUSPENDING || evicting.LastAckedSeq != 2 {
		t.Errorf("eviction_notice(r-2, seq 2): state=%s acked=%d, want SUSPENDING with it applied",
			evicting.Task.Status.State, evicting.LastAckedSeq)
	}

	// A setup event never carries a run ID.
	setup := completeHead(t, newResumingState(t, false))
	setup = mustTransition(t, setup, firstClaim("n-1", &v1alpha1.Workspace{Metadata: &v1alpha1.ObjectMeta{Name: "ws-1"}}), guardT0)
	withRun := event("r-9", 1)
	withRun.SetupFinished = &runnerv1.SetupFinished{Ok: true}
	got := mustTransition(t, setup, withRun, guardT0.Add(time.Second))
	if got.Task.Status.State != v1alpha1.TaskState_TASK_STATE_QUEUED || got.LastAckedSeq != 0 || len(got.Actions) != 0 {
		t.Errorf("setup_finished carrying a run ID: state=%s acked=%d actions=%v, want it ignored",
			got.Task.Status.State, got.LastAckedSeq, actionIDs(got.Actions))
	}
}

// Every runner event is sequenced from 1. An event without a seq cannot be
// deduplicated, so it is refused instead of being applied on every resend.
func TestRunnerEventWithoutSeqIsRejected(t *testing.T) {
	working := newWorkingState(t, 0)
	for name, in := range map[string]lifecycle.Input{
		"SetupFinished":  {SetupFinished: &runnerv1.SetupFinished{Ok: true}},
		"CommandStarted": {EventRunID: "r-1", CommandStarted: &runnerv1.CommandStarted{Pid: 7}},
		"CommandExited":  {EventRunID: "r-1", CommandExited: &runnerv1.CommandExited{ExitCode: 0}},
		"IdleNotice":     {EventRunID: "r-1", IdleNotice: &runnerv1.IdleNotice{}},
		"EvictionNotice": {EventRunID: "r-1", EvictionNotice: &runnerv1.EvictionNotice{}},
	} {
		in.EventTokenHash = guardToken[:]
		for _, seq := range []int64{0, -1} {
			in.EventSeq = seq
			got, out, err := lifecycle.Transition(working, in, guardT0.Add(time.Second))
			if !errors.Is(err, lifecycle.ErrInvalidEvent) || out.Assignment != nil {
				t.Errorf("%s with seq %d = (next %v, err %v), want ErrInvalidEvent", name, seq, out.Assignment, err)
			}
			if !proto.Equal(got.Task, working.Task) || got.LastAckedSeq != working.LastAckedSeq {
				t.Errorf("%s with seq %d changed the state", name, seq)
			}
		}
	}
}
