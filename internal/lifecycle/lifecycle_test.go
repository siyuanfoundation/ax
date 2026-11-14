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
	"strings"
	"testing"
	"time"

	"github.com/google/ax/internal/lifecycle"
	runnerv1 "github.com/google/ax/pkg/apis/runner/v1"
	"github.com/google/ax/pkg/apis/v1alpha1"
)

func TestTransitionProvisionClaimIdleAndColdRestore(t *testing.T) {
	t0 := time.Date(2026, 11, 13, 20, 0, 0, 0, time.UTC)

	// 1. CreateTask -> QUEUED with client-supplied Status.ActorUid scrubbed.
	s, out, err := lifecycle.Transition(lifecycle.State{}, lifecycle.Input{
		Create: &lifecycle.CreateInput{
			Task: &v1alpha1.Task{
				Metadata: &v1alpha1.ObjectMeta{Atespace: "payments", Name: "fix-bug", Uid: "task-uid-1"},
				Spec: &v1alpha1.TaskSpec{
					Commands:  []string{"pytest", "-q"},
					Env:       []*v1alpha1.EnvVar{{Name: "FOO", Value: "bar"}},
					Goal:      &v1alpha1.Goal{Spec: "Fix rounding bug"},
					SessionId: "sess-1",
				},
				Status: &v1alpha1.TaskStatus{
					Actor:    "spoofed-actor",
					ActorUid: "spoofed-uid",
				},
			},
			AteActorTemplate: "ax-0123456789abcdef",
			SourceTag:        "ws-payments-r1",
			WorkspaceReady:   true,
		},
	}, t0)
	if err != nil || out.DeleteRow {
		t.Fatalf("Create: err=%v, out=%+v", err, out)
	}
	if s.Task.GetStatus().GetActorUid() != "" || s.Task.GetStatus().GetActor() != "" {
		t.Fatalf("expected client status Actor/ActorUid scrubbed, got %+v", s.Task.GetStatus())
	}
	if len(s.Actions) != 4 || s.Actions[1].Tag != "ws-payments-r1" {
		t.Fatalf("actions = %+v, want 4 provisioning actions with source tag", s.Actions)
	}
	if due := lifecycle.NextReconcileAt(s, t0); due == nil || !due.Equal(t0) {
		t.Fatalf("NextReconcileAt = %v, want %v", due, t0)
	}

	// 2. Pop all 4 actions; RESUME_ACTOR sets ClaimDeadline = t0 + 5m.
	for _, step := range []lifecycle.ActionCompletedInput{
		{ActionID: "a-1", Kind: lifecycle.ActionEnsureTemplate},
		{ActionID: "a-2", Kind: lifecycle.ActionCreateActor, ActorUID: "uid-1"},
		{ActionID: "a-3", Kind: lifecycle.ActionEnsureEgress},
		{ActionID: "a-4", Kind: lifecycle.ActionResumeActor},
	} {
		s, _, err = lifecycle.Transition(s, lifecycle.Input{ActionCompleted: &step}, t0)
		if err != nil {
			t.Fatalf("ActionCompleted(%s): %v", step.Kind, err)
		}
	}
	if s.ActorUID != "uid-1" || s.Task.GetStatus().GetActorUid() != "" {
		t.Fatalf("ActorUID = %q, Task.Status.ActorUid = %q; want uid-1 and empty", s.ActorUID, s.Task.GetStatus().GetActorUid())
	}
	wantDeadline := t0.Add(lifecycle.ClaimWatchdogTimeout)
	if due := lifecycle.NextReconcileAt(s, t0); due == nil || !due.Equal(wantDeadline) {
		t.Fatalf("NextReconcileAt after resume = %v, want %v", due, wantDeadline)
	}

	// 3. Initial Claim mints token + r-1 with Exec.Env populated; dropped-response retry re-mints safely.
	tok1 := sha256.Sum256([]byte("tok-1"))
	s, out, err = lifecycle.Transition(s, lifecycle.Input{
		Claim: &lifecycle.ClaimInput{
			Request:              &runnerv1.ClaimAssignmentRequest{Nonce: "n-1"},
			CandidateRunnerToken: "tok-1",
			CandidateTokenHash:   tok1[:],
		},
	}, t0.Add(time.Second))
	if err != nil {
		t.Fatalf("Claim 1: %v", err)
	}
	if out.Assignment.GetMode() != runnerv1.AssignmentMode_ASSIGNMENT_MODE_RUN ||
		out.Assignment.GetRunId() != "r-1" ||
		out.Assignment.GetRunnerToken() != "tok-1" ||
		out.Assignment.GetExec().GetEnv()["FOO"] != "bar" {
		t.Fatalf("Claim 1 assignment = %+v", out.Assignment)
	}
	if s.RunnerTokenConfirmed {
		t.Fatal("expected RunnerTokenConfirmed=false before runner acknowledges token")
	}

	// Simulate lost HTTP response: retry with same nonce and no token before confirmation.
	tok2 := sha256.Sum256([]byte("tok-2"))
	s, out, err = lifecycle.Transition(s, lifecycle.Input{
		Claim: &lifecycle.ClaimInput{
			Request:              &runnerv1.ClaimAssignmentRequest{Nonce: "n-1"},
			CandidateRunnerToken: "tok-2",
			CandidateTokenHash:   tok2[:],
		},
	}, t0.Add(2*time.Second))
	if err != nil {
		t.Fatalf("Claim 1 retry: %v", err)
	}
	if out.Assignment.GetRunId() != "r-1" || out.Assignment.GetRunnerToken() != "tok-2" ||
		out.Assignment.GetExec().GetEnv()["FOO"] != "bar" {
		t.Fatalf("Claim 1 retry assignment = %+v, want r-1 + tok-2 + env", out.Assignment)
	}

	// Event with empty run_id must be ignored (cannot bypass ActiveRunID fencing).
	sUnchanged, _, err := lifecycle.Transition(s, lifecycle.Input{
		EventRunID:     "",
		EventSeq:       99,
		CommandStarted: &runnerv1.CommandStarted{Pid: 42},
	}, t0.Add(2*time.Second))
	if err != nil || sUnchanged.Task.GetStatus().GetState() != v1alpha1.TaskState_TASK_STATE_QUEUED {
		t.Fatalf("empty EventRunID bypassed fencing: state=%v, err=%v", sUnchanged.Task.GetStatus().GetState(), err)
	}

	// 4. CommandStarted with r-1 -> WORKING, confirms token, clears LastClaimNonce/Response.
	s, _, err = lifecycle.Transition(s, lifecycle.Input{
		EventRunID:     "r-1",
		EventSeq:       1,
		CommandStarted: &runnerv1.CommandStarted{Pid: 42},
	}, t0.Add(3*time.Second))
	if err != nil || s.Task.GetStatus().GetState() != v1alpha1.TaskState_TASK_STATE_WORKING {
		t.Fatalf("CommandStarted: state=%v, err=%v", s.Task.GetStatus().GetState(), err)
	}
	if !s.RunnerTokenConfirmed || s.LastClaimNonce != "" || s.LastClaimResponse != nil {
		t.Fatalf("expected token confirmed and claim cache cleared, got confirmed=%v nonce=%q", s.RunnerTokenConfirmed, s.LastClaimNonce)
	}

	// 5. Agent posts IdleNotice -> SUSPENDING -> SUSPEND_ACTOR_SYNC(SnapshotGen=2) -> IDLE.
	s, _, err = lifecycle.Transition(s, lifecycle.Input{
		EventRunID: "r-1",
		EventSeq:   2,
		IdleNotice: &runnerv1.IdleNotice{Output: "turn 1 done"},
	}, t0.Add(10*time.Second))
	if err != nil || s.Task.GetStatus().GetState() != v1alpha1.TaskState_TASK_STATE_SUSPENDING {
		t.Fatalf("IdleNotice: state=%v, err=%v", s.Task.GetStatus().GetState(), err)
	}
	suspendID := s.Actions[0].ID
	s, _, err = lifecycle.Transition(s, lifecycle.Input{
		ActionCompleted: &lifecycle.ActionCompletedInput{
			ActionID:    suspendID,
			Kind:        lifecycle.ActionSuspendActorSync,
			SnapshotGen: 2,
		},
	}, t0.Add(12*time.Second))
	if err != nil || s.Task.GetStatus().GetState() != v1alpha1.TaskState_TASK_STATE_IDLE {
		t.Fatalf("SUSPEND_ACTOR_SYNC complete: state=%v, err=%v", s.Task.GetStatus().GetState(), err)
	}
	if s.RunnerTokenGen != 2 {
		t.Fatalf("RunnerTokenGen = %d, want 2", s.RunnerTokenGen)
	}

	// 6. External wakeup from IDLE under ROOTFS fidelity (child_alive=false)
	// moves IDLE -> QUEUED immediately and mints r-2 via monotonic RunSeq.
	s, out, err = lifecycle.Transition(s, lifecycle.Input{
		Claim: &lifecycle.ClaimInput{
			Request: &runnerv1.ClaimAssignmentRequest{
				LastStartedRunId: "r-1",
				ChildAlive:       false,
				Nonce:            "n-2",
			},
			ProvidedTokenHash: tok2[:],
		},
	}, t0.Add(20*time.Second))
	if err != nil {
		t.Fatalf("Claim after IDLE: %v", err)
	}
	if s.Task.GetStatus().GetState() != v1alpha1.TaskState_TASK_STATE_QUEUED || out.Assignment.GetRunId() != "r-2" {
		t.Fatalf("post-IDLE claim: state=%v, run_id=%q; want QUEUED + r-2",
			s.Task.GetStatus().GetState(), out.Assignment.GetRunId())
	}

	// 7. RevertActor to SnapshotGen=1 (< RunnerTokenGen=2) clears RunnerTokenHash & RunnerTokenConfirmed.
	rev := s.NextAction(lifecycle.ActionRevertActor, t0.Add(25*time.Second))
	s.Actions = []lifecycle.Action{rev}
	s, _, err = lifecycle.Transition(s, lifecycle.Input{
		ActionCompleted: &lifecycle.ActionCompletedInput{
			ActionID:    rev.ID,
			Kind:        lifecycle.ActionRevertActor,
			SnapshotGen: 1,
		},
	}, t0.Add(26*time.Second))
	if err != nil {
		t.Fatalf("RevertActor complete: %v", err)
	}
	if len(s.RunnerTokenHash) != 0 || s.RunnerTokenGen != 0 || s.RunnerTokenConfirmed {
		t.Fatalf("expected RunnerTokenHash cleared after revert below gen 2, got gen=%d confirmed=%v",
			s.RunnerTokenGen, s.RunnerTokenConfirmed)
	}

	// 8. Failure after earlier Turn 1 IdleNotice must overwrite Outcome.Result to FAILED.
	sFailed, _, err := lifecycle.Transition(s, lifecycle.Input{
		HeartbeatLost: &lifecycle.HeartbeatLostInput{SilentFor: 3 * time.Minute},
	}, t0.Add(30*time.Second))
	if err != nil {
		t.Fatalf("HeartbeatLost: %v", err)
	}
	if sFailed.Task.GetStatus().GetState() != v1alpha1.TaskState_TASK_STATE_FAILED ||
		sFailed.Task.GetStatus().GetOutcome().GetResult() != v1alpha1.TaskResult_TASK_RESULT_FAILED {
		t.Fatalf("failed outcome after IdleNotice = %+v, want TASK_RESULT_FAILED", sFailed.Task.GetStatus().GetOutcome())
	}
}

func TestColdBootSetupCheckpointAndDeleteFencing(t *testing.T) {
	t0 := time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)

	// 1. CreateTask with WaitWorkspaceTag=true queues WAIT_WORKSPACE_TAG at head.
	s, _, err := lifecycle.Transition(lifecycle.State{}, lifecycle.Input{
		Create: &lifecycle.CreateInput{
			Task: &v1alpha1.Task{
				Metadata: &v1alpha1.ObjectMeta{Atespace: "default", Name: "cold-setup"},
				Spec: &v1alpha1.TaskSpec{
					Commands:  []string{"pytest"},
					Workspace: &v1alpha1.WorkspaceRef{Name: "ws-1"},
				},
			},
			AteActorTemplate: "ax-override00000000",
			WorkspaceReady:   false,
			WaitWorkspaceTag: true,
		},
	}, t0)
	if err != nil || len(s.Actions) != 5 || s.Actions[0].Kind != lifecycle.ActionWaitWorkspaceTag {
		t.Fatalf("Create with WaitWorkspaceTag: actions=%+v, err=%v", s.Actions, err)
	}

	// Complete provisioning without a warm tag (cold-boot setup).
	for _, a := range append([]lifecycle.Action(nil), s.Actions...) {
		s, _, err = lifecycle.Transition(s, lifecycle.Input{
			ActionCompleted: &lifecycle.ActionCompletedInput{ActionID: a.ID, Kind: a.Kind, ActorUID: "actor-1"},
		}, t0)
		if err != nil {
			t.Fatalf("ActionCompleted(%s): %v", a.Kind, err)
		}
	}

	// 2. Claim returns ASSIGNMENT_MODE_SETUP; SetupFinished{ok: true} queues
	// [SUSPEND_ACTOR_SYNC(setup), RESUME_ACTOR] and returns IDLE so the workspace
	// is durably checkpointed before r-1 starts.
	tok := sha256.Sum256([]byte("tok-setup"))
	ws := &v1alpha1.Workspace{Metadata: &v1alpha1.ObjectMeta{Name: "ws-1"}}
	s, out, err := lifecycle.Transition(s, lifecycle.Input{
		Claim: &lifecycle.ClaimInput{
			Request:              &runnerv1.ClaimAssignmentRequest{Nonce: "n-setup"},
			CandidateRunnerToken: "tok-setup",
			CandidateTokenHash:   tok[:],
			Workspace:            ws,
		},
	}, t0.Add(time.Second))
	if err != nil || out.Assignment.GetMode() != runnerv1.AssignmentMode_ASSIGNMENT_MODE_SETUP {
		t.Fatalf("Claim setup = (%v, %v), want SETUP", out.Assignment, err)
	}

	s, out, err = lifecycle.Transition(s, lifecycle.Input{
		EventSeq:      1,
		SetupFinished: &runnerv1.SetupFinished{Ok: true},
	}, t0.Add(5*time.Second))
	if err != nil {
		t.Fatalf("SetupFinished: %v", err)
	}
	if out.Assignment.GetMode() != runnerv1.AssignmentMode_ASSIGNMENT_MODE_IDLE {
		t.Fatalf("SetupFinished next mode = %v, want IDLE", out.Assignment.GetMode())
	}
	if s.Task.GetStatus().GetState() != v1alpha1.TaskState_TASK_STATE_SUSPENDING ||
		len(s.Actions) != 2 ||
		s.Actions[0].Kind != lifecycle.ActionSuspendActorSync ||
		s.Actions[0].SuspendReason != lifecycle.SuspendReasonSetup ||
		s.Actions[1].Kind != lifecycle.ActionResumeActor {
		t.Fatalf("post-SetupFinished state=%v actions=%+v", s.Task.GetStatus().GetState(), s.Actions)
	}

	// Complete checkpoint + resume -> next Claim gets RUN r-1!
	for _, a := range append([]lifecycle.Action(nil), s.Actions...) {
		s, _, err = lifecycle.Transition(s, lifecycle.Input{
			ActionCompleted: &lifecycle.ActionCompletedInput{ActionID: a.ID, Kind: a.Kind, SnapshotGen: 1},
		}, t0.Add(6*time.Second))
		if err != nil {
			t.Fatalf("post-setup ActionCompleted(%s): %v", a.Kind, err)
		}
	}
	s, out, err = lifecycle.Transition(s, lifecycle.Input{
		Claim: &lifecycle.ClaimInput{
			Request:           &runnerv1.ClaimAssignmentRequest{Nonce: "n-run1"},
			ProvidedTokenHash: tok[:],
			Workspace:         ws,
		},
	}, t0.Add(7*time.Second))
	if err != nil || out.Assignment.GetMode() != runnerv1.AssignmentMode_ASSIGNMENT_MODE_RUN || out.Assignment.GetRunId() != "r-1" {
		t.Fatalf("post-setup Claim = (%+v, %v), want RUN r-1", out.Assignment, err)
	}

	// OOM exit with exit_code=0 must NOT mark COMPLETED; formats OOM message.
	sOOM, _, err := lifecycle.Transition(s, lifecycle.Input{
		EventRunID: "r-1",
		EventSeq:   1,
		CommandExited: &runnerv1.CommandExited{
			ExitCode: 0,
			Cause:    runnerv1.ExitCause_EXIT_CAUSE_OOM_KILLED,
		},
	}, t0.Add(8*time.Second))
	if err != nil || sOOM.Task.GetStatus().GetState() != v1alpha1.TaskState_TASK_STATE_FAILED ||
		!strings.Contains(sOOM.Task.GetStatus().GetStatusDetail().GetDetail(), "out-of-memory") {
		t.Fatalf("OOM exit = (state=%v, detail=%+v, err=%v)", sOOM.Task.GetStatus().GetState(), sOOM.Task.GetStatus().GetStatusDetail(), err)
	}

	// 3. DeleteTask is idempotent and immune to late CommandExited / HeartbeatLost / ActionFailed.
	sDel, _, err := lifecycle.Transition(s, lifecycle.Input{Delete: &lifecycle.DeleteInput{}}, t0.Add(9*time.Second))
	if err != nil || len(sDel.Actions) != 1 || sDel.Actions[0].Kind != lifecycle.ActionDeleteActor {
		t.Fatalf("Delete: actions=%+v, err=%v", sDel.Actions, err)
	}
	delActionID := sDel.Actions[0].ID

	// Duplicate Delete keeps the exact same ActionID so in-flight DeleteActor is not aborted.
	sDel2, _, err := lifecycle.Transition(sDel, lifecycle.Input{Delete: &lifecycle.DeleteInput{}}, t0.Add(10*time.Second))
	if err != nil || sDel2.Actions[0].ID != delActionID {
		t.Fatalf("duplicate Delete mutated ActionID: got %q, want %q", sDel2.Actions[0].ID, delActionID)
	}

	// Late CommandExited, HeartbeatLost, and ActionFailed must NOT drop ActionDeleteActor.
	for name, in := range map[string]lifecycle.Input{
		"CommandExited": {EventRunID: "r-1", EventSeq: 2, CommandExited: &runnerv1.CommandExited{ExitCode: 0}},
		"HeartbeatLost": {HeartbeatLost: &lifecycle.HeartbeatLostInput{SilentFor: 5 * time.Minute}},
		"ActionFailed":  {ActionFailed: &lifecycle.ActionFailedInput{ActionID: delActionID, Kind: lifecycle.ActionDeleteActor, Message: "denied"}},
	} {
		after, _, err := lifecycle.Transition(sDel, in, t0.Add(11*time.Second))
		if err != nil || len(after.Actions) != 1 || after.Actions[0].ID != delActionID {
			t.Fatalf("%s overwrote deleting task actions: got %+v, err=%v", name, after.Actions, err)
		}
	}

	// Suspend on terminal task returns ErrInvalidTransition.
	if _, _, err := lifecycle.Transition(sOOM, lifecycle.Input{Suspend: &lifecycle.SuspendInput{}}, t0.Add(12*time.Second)); !errors.Is(err, lifecycle.ErrInvalidTransition) {
		t.Fatalf("Suspend on FAILED = %v, want ErrInvalidTransition", err)
	}
}
