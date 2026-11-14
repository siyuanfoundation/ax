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

package lifecycle

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"time"

	runnerv1 "github.com/google/ax/pkg/apis/runner/v1"
	"github.com/google/ax/pkg/apis/v1alpha1"
	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
	spb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Transition computes the next task state and complete pending action list for
// input in at timestamp now. It is pure and deterministic and performs no I/O.
//
// TODO: Add multi-attempt spec.retry transitions (in-place and clean-slate)
// when pull-based runner execution is wired up.
func Transition(state State, in Input, now time.Time) (State, Output, error) {
	s := state.Clone()
	var out Output

	switch {
	case in.Create != nil:
		return applyCreate(s, in.Create, now)
	case s.Task == nil:
		return state, out, errors.New("lifecycle: nil task state")
	}

	ensureTaskContainers(s.Task)

	switch {
	case in.Delete != nil:
		return applyDelete(s, now)
	case in.Suspend != nil:
		return applySuspend(s, now)
	case in.Resume != nil:
		return applyResume(s, now)
	case in.ActionCompleted != nil:
		return applyActionCompleted(s, in.ActionCompleted, now)
	case in.ActionFailed != nil:
		return applyActionFailed(s, in.ActionFailed, now)
	case in.CapacityUnavailable != nil:
		return applyCapacityUnavailable(s, in.CapacityUnavailable, now)
	case in.ObservedActor != nil:
		return applyObservedActor(s, in.ObservedActor, now)
	case in.HeartbeatLost != nil:
		if s.Task.Metadata.GetDeleteTime() != nil || IsTerminal(s.Task.Status.State) {
			return s, out, nil
		}
		return applyFailureOrRetry(s, v1alpha1.TaskStatusReason_TASK_STATUS_REASON_UNSPECIFIED,
			fmt.Sprintf("runner heartbeat silent for %s", in.HeartbeatLost.SilentFor), true, now)
	case in.TimerExpired != nil:
		if s.Task.Metadata.GetDeleteTime() == nil &&
			s.Task.Status.State == v1alpha1.TaskState_TASK_STATE_QUEUED &&
			len(s.Actions) == 0 &&
			s.ClaimDeadline != nil && !now.Before(*s.ClaimDeadline) {
			s.ClaimDeadline = nil
			return applyFailureOrRetry(s, v1alpha1.TaskStatusReason_TASK_STATUS_REASON_UNSPECIFIED,
				"runner did not claim assignment before watchdog deadline", s.ActorUID != "", now)
		}
		return s, out, nil
	case in.Claim != nil:
		return applyClaim(s, in.Claim, now)
	case in.SetupFinished != nil:
		return applySetupFinished(s, in.EventSeq, in.SetupFinished, now)
	case in.CommandStarted != nil:
		return applyCommandStarted(s, in.EventRunID, in.EventSeq, now)
	case in.CommandExited != nil:
		return applyCommandExited(s, in.EventRunID, in.EventSeq, in.CommandExited, now)
	case in.IdleNotice != nil:
		return applyIdleNotice(s, in.EventRunID, in.EventSeq, in.IdleNotice, now)
	case in.EvictionNotice != nil:
		return applyEvictionNotice(s, in.EventSeq, now)
	default:
		return s, out, nil
	}
}

func ensureTaskContainers(t *v1alpha1.Task) {
	if t.Metadata == nil {
		t.Metadata = &v1alpha1.ObjectMeta{}
	}
	if t.Spec == nil {
		t.Spec = &v1alpha1.TaskSpec{}
	}
	if t.Status == nil {
		t.Status = &v1alpha1.TaskStatus{}
	}
	// Live Substrate actor identity fields are overlaid on read, never stored.
	t.Status.Actor = ""
	t.Status.ActorUid = ""
}

func applyCreate(s State, c *CreateInput, now time.Time) (State, Output, error) {
	if c.Task == nil {
		return s, Output{}, errors.New("lifecycle: create requires non-nil task")
	}
	t := proto.Clone(c.Task).(*v1alpha1.Task)
	if t.Metadata == nil {
		t.Metadata = &v1alpha1.ObjectMeta{}
	}
	if t.Spec == nil {
		t.Spec = &v1alpha1.TaskSpec{}
	}
	// Replace any client-supplied status completely so read-only fields such as
	// actor_uid, outcome, or conditions cannot be spoofed into storage.
	t.Status = &v1alpha1.TaskStatus{
		State:            v1alpha1.TaskState_TASK_STATE_QUEUED,
		Attempt:          1,
		AteActorTemplate: c.AteActorTemplate,
	}

	ts := timestamppb.New(now)
	if t.Metadata.CreateTime == nil {
		t.Metadata.CreateTime = ts
	}
	t.Metadata.UpdateTime = ts
	t.Metadata.DeleteTime = nil

	if len(c.ResolvedArgv) > 0 {
		t.Status.ResolvedArgv = slices.Clone(c.ResolvedArgv)
	} else if len(t.Spec.Commands) > 0 {
		t.Status.ResolvedArgv = slices.Clone(t.Spec.Commands)
	}
	if c.WorkspaceReady {
		setCondition(t, v1alpha1.ConditionWorkspaceReady, "True", v1alpha1.ReasonSetupComplete, "Workspace ready from warm snapshot", now)
	} else {
		setCondition(t, v1alpha1.ConditionWorkspaceReady, "False", v1alpha1.ReasonInitializing, "Workspace pending initialization", now)
	}
	setCondition(t, v1alpha1.ConditionReady, "False", v1alpha1.ReasonResuming, "Task queued for provisioning", now)

	s.Task = t
	var actions []Action
	if c.WaitWorkspaceTag {
		actions = append(actions, s.NextAction(ActionWaitWorkspaceTag, now))
	}
	ensureTpl := s.NextAction(ActionEnsureTemplate, now)
	createActor := s.NextAction(ActionCreateActor, now)
	createActor.Tag = c.SourceTag
	ensureEgress := s.NextAction(ActionEnsureEgress, now)
	resumeActor := s.NextAction(ActionResumeActor, now)
	s.Actions = append(actions, ensureTpl, createActor, ensureEgress, resumeActor)
	return s, Output{}, nil
}

func applyDelete(s State, now time.Time) (State, Output, error) {
	if s.Task.Metadata.GetDeleteTime() != nil && len(s.Actions) == 1 && s.Actions[0].Kind == ActionDeleteActor {
		return s, Output{}, nil
	}
	ts := timestamppb.New(now)
	if s.Task.Metadata.DeleteTime == nil {
		s.Task.Metadata.DeleteTime = ts
	}
	s.Task.Metadata.UpdateTime = ts
	s.ClaimDeadline = nil
	s.clearClaimCache()
	setCondition(s.Task, v1alpha1.ConditionReady, "False", v1alpha1.ReasonTerminating, "Task is being deleted", now)
	s.Actions = []Action{s.NextAction(ActionDeleteActor, now)}
	return s, Output{}, nil
}

func applySuspend(s State, now time.Time) (State, Output, error) {
	if s.Task.Metadata.GetDeleteTime() != nil || IsTerminal(s.Task.Status.State) {
		return s, Output{}, fmt.Errorf("%w: cannot suspend task in state %s", ErrInvalidTransition, s.Task.Status.State)
	}
	switch s.Task.Status.State {
	case v1alpha1.TaskState_TASK_STATE_SUSPENDING,
		v1alpha1.TaskState_TASK_STATE_SUSPENDED,
		v1alpha1.TaskState_TASK_STATE_IDLE:
		return s, Output{}, nil
	}
	s.Task.Status.State = v1alpha1.TaskState_TASK_STATE_SUSPENDING
	s.Task.Metadata.UpdateTime = timestamppb.New(now)
	s.ClaimDeadline = nil
	s.clearClaimCache()
	setCondition(s.Task, v1alpha1.ConditionReady, "False", v1alpha1.ReasonSuspending, "Suspending task actor", now)

	act := s.NextAction(ActionSuspendActorSync, now)
	act.SuspendReason = SuspendReasonUser
	s.Actions = keepProvisioningBefore(s.Actions, act)
	return s, Output{}, nil
}

func applyResume(s State, now time.Time) (State, Output, error) {
	if s.Task.Metadata.GetDeleteTime() != nil || IsTerminal(s.Task.Status.State) ||
		s.Task.Status.State == v1alpha1.TaskState_TASK_STATE_SUSPENDING {
		return s, Output{}, fmt.Errorf("%w: cannot resume task in state %s", ErrInvalidTransition, s.Task.Status.State)
	}
	switch s.Task.Status.State {
	case v1alpha1.TaskState_TASK_STATE_QUEUED, v1alpha1.TaskState_TASK_STATE_WORKING:
		return s, Output{}, nil
	}
	s.Task.Status.State = v1alpha1.TaskState_TASK_STATE_QUEUED
	s.Task.Status.StatusDetail = nil
	s.Task.Metadata.UpdateTime = timestamppb.New(now)
	s.clearClaimCache()
	setCondition(s.Task, v1alpha1.ConditionReady, "False", v1alpha1.ReasonResuming, "Resuming task actor", now)
	s.Actions = []Action{s.NextAction(ActionResumeActor, now)}
	return s, Output{}, nil
}

func applyActionCompleted(s State, done *ActionCompletedInput, now time.Time) (State, Output, error) {
	if len(s.Actions) == 0 || s.Actions[0].ID != done.ActionID {
		return s, Output{}, nil
	}
	head := s.Actions[0]
	s.Actions = s.Actions[1:]
	s.Task.Metadata.UpdateTime = timestamppb.New(now)

	switch head.Kind {
	case ActionWaitWorkspaceTag:
		for i := range s.Actions {
			if s.Actions[i].Kind == ActionCreateActor && done.SourceTag != "" {
				s.Actions[i].Tag = done.SourceTag
			}
		}
		if done.SourceTag != "" {
			setCondition(s.Task, v1alpha1.ConditionWorkspaceReady, "True", v1alpha1.ReasonSetupComplete, "Workspace ready from warm snapshot", now)
		}
	case ActionCreateActor:
		if done.ActorUID != "" {
			s.ActorUID = done.ActorUID
		}
	case ActionResumeActor:
		resumedAt := now
		s.LastResumedAt = &resumedAt
		s.clearClaimCache()
		if s.Task.Status.State == v1alpha1.TaskState_TASK_STATE_QUEUED && len(s.Actions) == 0 {
			deadline := now.Add(ClaimWatchdogTimeout)
			s.ClaimDeadline = &deadline
		}
	case ActionSuspendActorSync:
		s.clearClaimCache()
		if len(s.RunnerTokenHash) > 0 && s.RunnerTokenConfirmed && s.RunnerTokenGen == 0 && done.SnapshotGen > 0 {
			s.RunnerTokenGen = done.SnapshotGen
		}
		if s.Task.Status.State == v1alpha1.TaskState_TASK_STATE_SUSPENDING {
			switch head.SuspendReason {
			case SuspendReasonTurnIdle:
				s.Task.Status.State = v1alpha1.TaskState_TASK_STATE_IDLE
				s.ActiveRunID = ""
				setCondition(s.Task, v1alpha1.ConditionReady, "False", v1alpha1.ReasonSuspended, "Task parked idle between turns", now)
			case SuspendReasonEviction:
				s.Task.Status.State = v1alpha1.TaskState_TASK_STATE_QUEUED
				setCondition(s.Task, v1alpha1.ConditionReady, "False", v1alpha1.ReasonResuming, "Migrating evicted task actor", now)
				s.Actions = append(s.Actions, s.NextAction(ActionResumeActor, now))
			case SuspendReasonSetup:
				s.Task.Status.State = v1alpha1.TaskState_TASK_STATE_QUEUED
				setCondition(s.Task, v1alpha1.ConditionReady, "False", v1alpha1.ReasonResuming, "Workspace checkpointed; starting task run", now)
			default:
				s.Task.Status.State = v1alpha1.TaskState_TASK_STATE_SUSPENDED
				setCondition(s.Task, v1alpha1.ConditionReady, "False", v1alpha1.ReasonSuspended, "Task suspended", now)
			}
		}
	case ActionRevertActor:
		s.clearClaimCache()
		if len(s.RunnerTokenHash) > 0 && (s.RunnerTokenGen == 0 || done.SnapshotGen < s.RunnerTokenGen) {
			s.RunnerTokenHash = nil
			s.RunnerTokenConfirmed = false
			s.RunnerTokenGen = 0
		}
	case ActionDeleteActor:
		return s, Output{DeleteRow: true}, nil
	}
	return s, Output{}, nil
}

func applyActionFailed(s State, failed *ActionFailedInput, now time.Time) (State, Output, error) {
	if len(s.Actions) == 0 || s.Actions[0].ID != failed.ActionID {
		return s, Output{}, nil
	}
	if s.Task.Metadata.GetDeleteTime() != nil || failed.Kind == ActionDeleteActor {
		setCondition(s.Task, v1alpha1.ConditionReady, "False", v1alpha1.ReasonTerminating, failed.Message, now)
		return s, Output{}, nil
	}
	reason := failed.Reason
	if reason == v1alpha1.TaskStatusReason_TASK_STATUS_REASON_UNSPECIFIED {
		reason = v1alpha1.TaskStatusReason_TASK_STATUS_REASON_PROVISIONING_FAILED
	}
	return markTerminalFailed(s, reason, failed.Message, codes.Internal, false, now), Output{}, nil
}

func applyCapacityUnavailable(s State, capIn *CapacityUnavailableInput, now time.Time) (State, Output, error) {
	if len(s.Actions) == 0 || s.Actions[0].ID != capIn.ActionID {
		return s, Output{}, nil
	}
	head := &s.Actions[0]
	if head.Since.IsZero() {
		head.Since = now
	}
	if now.Sub(head.Since) >= MaxQueueWaitDuration {
		return markTerminalFailed(s, v1alpha1.TaskStatusReason_TASK_STATUS_REASON_PROVISIONING_FAILED,
			"exceeded maximum queue wait for sandbox capacity: "+capIn.Message, codes.ResourceExhausted, false, now), Output{}, nil
	}
	delay := RetryBackoff(head.Tries, DefaultActionInitialBackoff, DefaultActionMaxBackoff, 2.0)
	head.Tries++
	head.NotBefore = now.Add(delay)
	return s, Output{}, nil
}

func applyObservedActor(s State, obs *ObservedActorInput, now time.Time) (State, Output, error) {
	if s.Task.Metadata.GetDeleteTime() != nil {
		return s, Output{}, nil
	}
	if obs.MissingConfirmed {
		if IsTerminal(s.Task.Status.State) {
			return s, Output{}, nil
		}
		s.ActorUID = ""
		return markTerminalFailed(s, v1alpha1.TaskStatusReason_TASK_STATUS_REASON_PROVISIONING_FAILED,
			"task actor no longer exists on Substrate", codes.NotFound, false, now), Output{}, nil
	}

	switch obs.ActorState {
	case ateapipb.ActorState_ACTOR_STATE_CRASHED:
		if IsTerminal(s.Task.Status.State) || s.Task.Status.State == v1alpha1.TaskState_TASK_STATE_IDLE {
			s.Actions = []Action{s.NextAction(ActionRevertActor, now)}
			return s, Output{}, nil
		}
		msg := obs.CrashMessage
		if msg == "" {
			msg = "sandbox crashed"
		}
		return applyFailureOrRetry(s, v1alpha1.TaskStatusReason_TASK_STATUS_REASON_SANDBOX_CRASHED, msg, true, now)
	case ateapipb.ActorState_ACTOR_STATE_RUNNING:
		switch {
		case IsTerminal(s.Task.Status.State):
			act := s.NextAction(ActionSuspendActorSync, now)
			act.SuspendReason = SuspendReasonTerminal
			s.Actions = []Action{act}
		case s.Task.Status.State == v1alpha1.TaskState_TASK_STATE_SUSPENDED ||
			s.Task.Status.State == v1alpha1.TaskState_TASK_STATE_IDLE:
			s.Task.Status.State = v1alpha1.TaskState_TASK_STATE_QUEUED
			resumedAt := now
			s.LastResumedAt = &resumedAt
			deadline := now.Add(ClaimWatchdogTimeout)
			s.ClaimDeadline = &deadline
		}
	case ateapipb.ActorState_ACTOR_STATE_SUSPENDED:
		if s.Task.Status.State == v1alpha1.TaskState_TASK_STATE_WORKING && len(s.Actions) == 0 {
			s.Task.Status.State = v1alpha1.TaskState_TASK_STATE_SUSPENDED
			s.clearClaimCache()
			setCondition(s.Task, v1alpha1.ConditionReady, "False", v1alpha1.ReasonSuspended, "Actor externally suspended", now)
		}
	}
	return s, Output{}, nil
}

func applyClaim(s State, claim *ClaimInput, now time.Time) (State, Output, error) {
	req := claim.Request
	if req == nil {
		req = &runnerv1.ClaimAssignmentRequest{}
	}

	// Nonce replay check before token enforcement on unstarted QUEUED claims so
	// a dropped initial HTTP response can safely re-mint the runner token.
	hasToken := len(s.RunnerTokenHash) > 0
	tokenMatch := hasToken && bytes.Equal(s.RunnerTokenHash, claim.ProvidedTokenHash)
	if req.GetNonce() != "" && req.GetNonce() == s.LastClaimNonce && s.LastClaimResponse != nil {
		if tokenMatch {
			s.RunnerTokenConfirmed = true
			return s, Output{Assignment: proto.Clone(s.LastClaimResponse).(*runnerv1.ClaimAssignmentResponse)}, nil
		}
		if len(claim.ProvidedTokenHash) == 0 &&
			s.Task.Metadata.GetDeleteTime() == nil &&
			s.Task.Status.State == v1alpha1.TaskState_TASK_STATE_QUEUED &&
			!s.RunnerTokenConfirmed && s.LastAckedSeq == 0 && len(claim.CandidateTokenHash) > 0 {
			s.RunnerTokenHash = append([]byte(nil), claim.CandidateTokenHash...)
			s.RunnerTokenConfirmed = false
			s.RunnerTokenGen = 0
			replay := proto.Clone(s.LastClaimResponse).(*runnerv1.ClaimAssignmentResponse)
			replay.RunnerToken = claim.CandidateRunnerToken
			return s, Output{Assignment: replay}, nil
		}
	}

	var mintedToken string
	if !hasToken {
		if len(claim.CandidateTokenHash) > 0 {
			s.RunnerTokenHash = append([]byte(nil), claim.CandidateTokenHash...)
			s.RunnerTokenConfirmed = false
			s.RunnerTokenGen = 0
			mintedToken = claim.CandidateRunnerToken
		}
	} else if !tokenMatch {
		return stateUnchanged(s), Output{}, ErrUnauthorizedRunner
	} else {
		s.RunnerTokenConfirmed = true
	}

	s.ClaimDeadline = nil
	resp := &runnerv1.ClaimAssignmentResponse{
		Attempt:   s.Task.Status.Attempt,
		Task:      proto.Clone(s.Task).(*v1alpha1.Task),
		Workspace: claim.Workspace,
	}

	switch {
	case s.Task.Metadata.GetDeleteTime() != nil || IsTerminal(s.Task.Status.State) ||
		s.Task.Status.State == v1alpha1.TaskState_TASK_STATE_SUSPENDING:
		resp.Mode = runnerv1.AssignmentMode_ASSIGNMENT_MODE_IDLE
	case !conditionTrue(s.Task, v1alpha1.ConditionWorkspaceReady) && claim.Workspace != nil:
		resp.Mode = runnerv1.AssignmentMode_ASSIGNMENT_MODE_SETUP
		if s.Task.Status.State == v1alpha1.TaskState_TASK_STATE_IDLE ||
			s.Task.Status.State == v1alpha1.TaskState_TASK_STATE_SUSPENDED {
			s.Task.Status.State = v1alpha1.TaskState_TASK_STATE_QUEUED
			s.Task.Metadata.UpdateTime = timestamppb.New(now)
			setCondition(s.Task, v1alpha1.ConditionReady, "False", v1alpha1.ReasonResuming, "Running workspace setup", now)
		}
	case req.GetChildAlive() && req.GetInMemoryRunId() != "" && req.GetInMemoryRunId() == s.ActiveRunID:
		resp.Mode = runnerv1.AssignmentMode_ASSIGNMENT_MODE_RESUME
		resp.RunId = s.ActiveRunID
		s.Task.Status.State = v1alpha1.TaskState_TASK_STATE_WORKING
		s.Task.Metadata.UpdateTime = timestamppb.New(now)
		setCondition(s.Task, v1alpha1.ConditionReady, "True", v1alpha1.ReasonTaskRunning, "Task is running", now)
	default:
		if s.Task.Status.State == v1alpha1.TaskState_TASK_STATE_IDLE ||
			s.Task.Status.State == v1alpha1.TaskState_TASK_STATE_SUSPENDED {
			s.Task.Status.State = v1alpha1.TaskState_TASK_STATE_QUEUED
			s.Task.Metadata.UpdateTime = timestamppb.New(now)
			setCondition(s.Task, v1alpha1.ConditionReady, "False", v1alpha1.ReasonResuming, "Starting claimed task run", now)
		}
		runID := s.ActiveRunID
		if runID == "" || runID == req.GetLastStartedRunId() {
			runID = s.NextRunID()
			s.ActiveRunID = runID
			s.LastAckedSeq = 0
		}
		resp.Mode = runnerv1.AssignmentMode_ASSIGNMENT_MODE_RUN
		resp.RunId = runID
		resp.Exec = &runnerv1.ExecSpec{
			Argv:     slices.Clone(s.Task.Status.ResolvedArgv),
			Workdir:  s.Task.Spec.WorkspacePath(),
			Env:      buildExecEnv(s.Task),
			RunAsUid: runnerv1.DefaultTaskUID,
		}
		if s.Task.Spec.GetGoal() != nil || s.Task.Status.PreviousFailure != "" {
			resp.Goal = &runnerv1.GoalPayload{
				Spec:            s.Task.Spec.GetGoal().GetSpec(),
				PreviousFailure: s.Task.Status.PreviousFailure,
			}
		}
	}

	// Keep resp.Task in sync with any state transition applied above.
	resp.Task = proto.Clone(s.Task).(*v1alpha1.Task)

	if req.GetNonce() != "" {
		s.LastClaimNonce = req.GetNonce()
		s.LastClaimResponse = proto.Clone(resp).(*runnerv1.ClaimAssignmentResponse)
	}
	resp.RunnerToken = mintedToken
	return s, Output{Assignment: resp}, nil
}

func applySetupFinished(s State, seq int64, sf *runnerv1.SetupFinished, now time.Time) (State, Output, error) {
	if s.Task.Metadata.GetDeleteTime() != nil || IsTerminal(s.Task.Status.State) {
		return s, Output{
			Assignment: &runnerv1.ClaimAssignmentResponse{Mode: runnerv1.AssignmentMode_ASSIGNMENT_MODE_IDLE},
		}, nil
	}
	if seq > 0 && seq <= s.LastAckedSeq {
		return s, Output{}, nil
	}
	if seq > 0 {
		s.LastAckedSeq = seq
	}
	s.RunnerTokenConfirmed = true
	s.clearClaimCache()

	if !sf.GetOk() {
		return markTerminalFailed(s, v1alpha1.TaskStatusReason_TASK_STATUS_REASON_PROVISIONING_FAILED,
			"workspace setup failed: "+sf.GetMessage(), codes.Internal, false, now), Output{
			Assignment: &runnerv1.ClaimAssignmentResponse{Mode: runnerv1.AssignmentMode_ASSIGNMENT_MODE_IDLE},
		}, nil
	}
	if conditionTrue(s.Task, v1alpha1.ConditionWorkspaceReady) {
		return s, Output{
			Assignment: &runnerv1.ClaimAssignmentResponse{Mode: runnerv1.AssignmentMode_ASSIGNMENT_MODE_IDLE},
		}, nil
	}

	// Checkpoint the initialized workspace before starting r-1 so a later crash
	// revert preserves /workspace setup and wakes /readyz cleanly for RUN r-1.
	setCondition(s.Task, v1alpha1.ConditionWorkspaceReady, "True", v1alpha1.ReasonSetupComplete, "Workspace setup completed", now)
	s.Task.Status.State = v1alpha1.TaskState_TASK_STATE_SUSPENDING
	s.ClaimDeadline = nil
	s.Task.Metadata.UpdateTime = timestamppb.New(now)
	setCondition(s.Task, v1alpha1.ConditionReady, "False", v1alpha1.ReasonSuspending, "Checkpointing initialized workspace", now)

	checkpoint := s.NextAction(ActionSuspendActorSync, now)
	checkpoint.SuspendReason = SuspendReasonSetup
	resume := s.NextAction(ActionResumeActor, now)
	s.Actions = []Action{checkpoint, resume}
	return s, Output{
		Assignment: &runnerv1.ClaimAssignmentResponse{Mode: runnerv1.AssignmentMode_ASSIGNMENT_MODE_IDLE},
	}, nil
}

func applyCommandStarted(s State, runID string, seq int64, now time.Time) (State, Output, error) {
	if s.Task.Metadata.GetDeleteTime() != nil || IsTerminal(s.Task.Status.State) {
		return s, Output{}, nil
	}
	if s.ActiveRunID == "" || runID != s.ActiveRunID {
		return s, Output{}, nil
	}
	if seq > 0 && seq <= s.LastAckedSeq {
		return s, Output{}, nil
	}
	if seq > 0 {
		s.LastAckedSeq = seq
	}
	s.RunnerTokenConfirmed = true
	s.ClaimDeadline = nil
	s.clearClaimCache()
	if s.Task.Status.State == v1alpha1.TaskState_TASK_STATE_QUEUED {
		s.Task.Status.State = v1alpha1.TaskState_TASK_STATE_WORKING
		s.Task.Metadata.UpdateTime = timestamppb.New(now)
		setCondition(s.Task, v1alpha1.ConditionReady, "True", v1alpha1.ReasonTaskRunning, "Task is running", now)
	}
	return s, Output{}, nil
}

func applyCommandExited(s State, runID string, seq int64, exited *runnerv1.CommandExited, now time.Time) (State, Output, error) {
	if s.Task.Metadata.GetDeleteTime() != nil || IsTerminal(s.Task.Status.State) {
		return s, Output{
			Assignment: &runnerv1.ClaimAssignmentResponse{Mode: runnerv1.AssignmentMode_ASSIGNMENT_MODE_IDLE},
		}, nil
	}
	if s.ActiveRunID == "" || runID != s.ActiveRunID {
		return s, Output{}, nil
	}
	if s.Task.Status.State != v1alpha1.TaskState_TASK_STATE_WORKING &&
		s.Task.Status.State != v1alpha1.TaskState_TASK_STATE_QUEUED {
		return s, Output{}, nil
	}
	if seq > 0 && seq <= s.LastAckedSeq {
		return s, Output{}, nil
	}
	if seq > 0 {
		s.LastAckedSeq = seq
	}
	s.RunnerTokenConfirmed = true
	s.clearClaimCache()

	if exited.GetExitCode() == 0 &&
		exited.GetSignal() == "" &&
		exited.GetCause() != runnerv1.ExitCause_EXIT_CAUSE_OOM_KILLED &&
		exited.GetCause() != runnerv1.ExitCause_EXIT_CAUSE_DISK_FULL {
		s.Task.Status.State = v1alpha1.TaskState_TASK_STATE_COMPLETED
		s.Task.Status.StatusDetail = nil
		s.Task.Status.Outcome = &v1alpha1.TaskOutcome{
			Result:           v1alpha1.TaskResult_TASK_RESULT_SUCCEEDED,
			ExitCode:         0,
			Output:           exited.GetOutput(),
			StructuredOutput: exited.GetStructuredOutput(),
		}
		s.ActiveRunID = ""
		s.Task.Metadata.UpdateTime = timestamppb.New(now)
		setCondition(s.Task, v1alpha1.ConditionReady, "False", "Completed", "Task completed successfully", now)
		act := s.NextAction(ActionSuspendActorSync, now)
		act.SuspendReason = SuspendReasonTerminal
		s.Actions = []Action{act}
		return s, Output{
			Assignment: &runnerv1.ClaimAssignmentResponse{Mode: runnerv1.AssignmentMode_ASSIGNMENT_MODE_IDLE},
		}, nil
	}

	failMsg := formatExitFailure(exited)
	summary := exited.GetOutput()
	if summary == "" {
		summary = failMsg
	}
	maxRetries := s.Task.Spec.GetRetry().GetMaxRetries()
	if s.Task.Status.Attempt <= maxRetries {
		retryCfg := s.Task.Spec.GetRetry()
		delay := RetryBackoff(int(s.Task.Status.Attempt-1),
			retryCfg.GetInitialBackoff().AsDuration(),
			retryCfg.GetMaxBackoff().AsDuration(),
			retryCfg.GetBackoffMultiplier())
		s.Task.Status.Attempt++
		s.Task.Status.PreviousFailure = summary
		nextRun := s.NextRunID()
		s.ActiveRunID = nextRun
		s.LastAckedSeq = 0
		s.Task.Metadata.UpdateTime = timestamppb.New(now)
		nextAssign := &runnerv1.ClaimAssignmentResponse{
			Mode:    runnerv1.AssignmentMode_ASSIGNMENT_MODE_RUN,
			RunId:   nextRun,
			Attempt: s.Task.Status.Attempt,
			Task:    proto.Clone(s.Task).(*v1alpha1.Task),
			Exec: &runnerv1.ExecSpec{
				Argv:     slices.Clone(s.Task.Status.ResolvedArgv),
				Workdir:  s.Task.Spec.WorkspacePath(),
				Env:      buildExecEnv(s.Task),
				RunAsUid: runnerv1.DefaultTaskUID,
			},
			Goal: &runnerv1.GoalPayload{
				Spec:            s.Task.Spec.GetGoal().GetSpec(),
				PreviousFailure: summary,
			},
		}
		if delay > 0 {
			nextAssign.StartDelay = durationpb.New(delay)
		}
		return s, Output{Assignment: nextAssign}, nil
	}

	s.Task.Status.Outcome = &v1alpha1.TaskOutcome{
		Result:           v1alpha1.TaskResult_TASK_RESULT_FAILED,
		Status:           &spb.Status{Code: int32(codes.Internal), Message: failMsg},
		ExitCode:         exited.GetExitCode(),
		Output:           exited.GetOutput(),
		StructuredOutput: exited.GetStructuredOutput(),
	}
	s = markTerminalFailed(s, v1alpha1.TaskStatusReason_TASK_STATUS_REASON_UNSPECIFIED, failMsg, codes.Internal, true, now)
	return s, Output{
		Assignment: &runnerv1.ClaimAssignmentResponse{Mode: runnerv1.AssignmentMode_ASSIGNMENT_MODE_IDLE},
	}, nil
}

func applyIdleNotice(s State, runID string, seq int64, idle *runnerv1.IdleNotice, now time.Time) (State, Output, error) {
	if s.Task.Metadata.GetDeleteTime() != nil || IsTerminal(s.Task.Status.State) {
		return s, Output{}, nil
	}
	if s.ActiveRunID == "" || runID != s.ActiveRunID {
		return s, Output{}, nil
	}
	if seq > 0 && seq <= s.LastAckedSeq {
		return s, Output{}, nil
	}
	if seq > 0 {
		s.LastAckedSeq = seq
	}
	s.RunnerTokenConfirmed = true
	if s.Task.Status.State != v1alpha1.TaskState_TASK_STATE_WORKING {
		return s, Output{}, nil
	}
	s.clearClaimCache()
	s.Task.Status.Outcome = &v1alpha1.TaskOutcome{
		Result:           v1alpha1.TaskResult_TASK_RESULT_SUCCEEDED,
		Output:           idle.GetOutput(),
		StructuredOutput: idle.GetStructuredOutput(),
	}
	s.Task.Status.State = v1alpha1.TaskState_TASK_STATE_SUSPENDING
	s.Task.Metadata.UpdateTime = timestamppb.New(now)
	setCondition(s.Task, v1alpha1.ConditionReady, "False", v1alpha1.ReasonSuspending, "Parking idle task between turns", now)
	act := s.NextAction(ActionSuspendActorSync, now)
	act.SuspendReason = SuspendReasonTurnIdle
	s.Actions = []Action{act}
	return s, Output{}, nil
}

func applyEvictionNotice(s State, seq int64, now time.Time) (State, Output, error) {
	if s.Task.Metadata.GetDeleteTime() != nil || IsTerminal(s.Task.Status.State) {
		return s, Output{}, nil
	}
	if seq > 0 && seq <= s.LastAckedSeq {
		return s, Output{}, nil
	}
	if seq > 0 {
		s.LastAckedSeq = seq
	}
	s.RunnerTokenConfirmed = true
	if s.Task.Status.State != v1alpha1.TaskState_TASK_STATE_WORKING &&
		s.Task.Status.State != v1alpha1.TaskState_TASK_STATE_QUEUED {
		return s, Output{}, nil
	}
	s.clearClaimCache()
	s.Task.Status.State = v1alpha1.TaskState_TASK_STATE_SUSPENDING
	s.ClaimDeadline = nil
	s.Task.Metadata.UpdateTime = timestamppb.New(now)
	setCondition(s.Task, v1alpha1.ConditionReady, "False", v1alpha1.ReasonSuspending, "Checkpointing task before worker eviction", now)
	act := s.NextAction(ActionSuspendActorSync, now)
	act.SuspendReason = SuspendReasonEviction
	s.Actions = []Action{act}
	return s, Output{}, nil
}

func applyFailureOrRetry(s State, reason v1alpha1.TaskStatusReason, detail string, revertActor bool, now time.Time) (State, Output, error) {
	s.clearClaimCache()
	maxRetries := s.Task.Spec.GetRetry().GetMaxRetries()
	if s.Task.Status.Attempt <= maxRetries && revertActor {
		retryCfg := s.Task.Spec.GetRetry()
		delay := RetryBackoff(int(s.Task.Status.Attempt-1),
			retryCfg.GetInitialBackoff().AsDuration(),
			retryCfg.GetMaxBackoff().AsDuration(),
			retryCfg.GetBackoffMultiplier())
		s.Task.Status.Attempt++
		s.Task.Status.PreviousFailure = detail
		s.Task.Status.State = v1alpha1.TaskState_TASK_STATE_QUEUED
		s.ActiveRunID = ""
		s.ClaimDeadline = nil
		s.Task.Metadata.UpdateTime = timestamppb.New(now)
		setCondition(s.Task, v1alpha1.ConditionReady, "False", v1alpha1.ReasonResuming, detail, now)

		rev := s.NextAction(ActionRevertActor, now)
		res := s.NextAction(ActionResumeActor, now)
		if delay > 0 {
			res.NotBefore = now.Add(delay)
		}
		s.Actions = []Action{rev, res}
		return s, Output{}, nil
	}
	return markTerminalFailed(s, reason, detail, codes.Internal, false, now), Output{}, nil
}

func markTerminalFailed(s State, reason v1alpha1.TaskStatusReason, detail string, code codes.Code, preserveCommandOutcome bool, now time.Time) State {
	s.clearClaimCache()
	s.Task.Status.State = v1alpha1.TaskState_TASK_STATE_FAILED
	s.Task.Status.StatusDetail = &v1alpha1.StatusDetail{
		Reason: reason,
		Detail: detail,
	}
	if preserveCommandOutcome && s.Task.Status.Outcome != nil {
		s.Task.Status.Outcome.Result = v1alpha1.TaskResult_TASK_RESULT_FAILED
		s.Task.Status.Outcome.Status = &spb.Status{Code: int32(code), Message: detail}
	} else {
		s.Task.Status.Outcome = &v1alpha1.TaskOutcome{
			Result: v1alpha1.TaskResult_TASK_RESULT_FAILED,
			Status: &spb.Status{Code: int32(code), Message: detail},
		}
	}
	s.ActiveRunID = ""
	s.ClaimDeadline = nil
	s.Task.Metadata.UpdateTime = timestamppb.New(now)
	setCondition(s.Task, v1alpha1.ConditionReady, "False", v1alpha1.ReasonFailed, detail, now)
	if s.ActorUID != "" {
		act := s.NextAction(ActionSuspendActorSync, now)
		act.SuspendReason = SuspendReasonTerminal
		s.Actions = []Action{act}
	} else {
		s.Actions = nil
	}
	return s
}

func keepProvisioningBefore(cur []Action, tail Action) []Action {
	var kept []Action
	for _, a := range cur {
		switch a.Kind {
		case ActionWaitWorkspaceTag, ActionEnsureTemplate, ActionCreateActor, ActionEnsureEgress, ActionRevertActor:
			kept = append(kept, a)
		}
	}
	return append(kept, tail)
}

func buildExecEnv(task *v1alpha1.Task) map[string]string {
	envList := task.GetSpec().GetEnv()
	sessionID := task.GetSpec().GetSessionId()
	taskUID := task.GetMetadata().GetUid()
	if len(envList) == 0 && sessionID == "" && taskUID == "" {
		return nil
	}
	out := make(map[string]string, len(envList)+2)
	for _, e := range envList {
		if e.GetName() != "" {
			out[e.GetName()] = e.GetValue()
		}
	}
	if taskUID != "" {
		if _, exists := out["AX_TASK_UID"]; !exists {
			out["AX_TASK_UID"] = taskUID
		}
	}
	if sessionID != "" {
		if _, exists := out["AX_SESSION_ID"]; !exists {
			out["AX_SESSION_ID"] = sessionID
		}
	}
	return out
}

func formatExitFailure(exited *runnerv1.CommandExited) string {
	switch exited.GetCause() {
	case runnerv1.ExitCause_EXIT_CAUSE_OOM_KILLED:
		return fmt.Sprintf("command killed due to out-of-memory (exit code %d)", exited.GetExitCode())
	case runnerv1.ExitCause_EXIT_CAUSE_DISK_FULL:
		return fmt.Sprintf("command failed due to disk full (exit code %d)", exited.GetExitCode())
	}
	if sig := exited.GetSignal(); sig != "" {
		return fmt.Sprintf("command terminated by signal %s (exit code %d)", sig, exited.GetExitCode())
	}
	return fmt.Sprintf("command exited with code %d", exited.GetExitCode())
}

func stateUnchanged(s State) State { return s }

func conditionTrue(task *v1alpha1.Task, condType string) bool {
	for _, c := range task.GetStatus().GetConditions() {
		if c.GetType() == condType {
			return c.GetStatus() == "True"
		}
	}
	return false
}

func setCondition(task *v1alpha1.Task, condType, condStatus, reason, message string, now time.Time) {
	ts := timestamppb.New(now)
	for _, c := range task.Status.Conditions {
		if c.GetType() == condType {
			if c.Status != condStatus || c.Reason != reason || c.Message != message {
				c.Status = condStatus
				c.Reason = reason
				c.Message = message
				c.UpdateTime = ts
			}
			return
		}
	}
	task.Status.Conditions = append(task.Status.Conditions, &v1alpha1.Condition{
		Type:       condType,
		Status:     condStatus,
		Reason:     reason,
		Message:    message,
		UpdateTime: ts,
	})
}
