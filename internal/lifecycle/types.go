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

// Package lifecycle defines the pure, I/O-free Task state machine shared by
// ax-server's gRPC and runner HTTP handlers and the background action queue and
// drift sweep reconciler.
package lifecycle

import (
	"errors"
	"fmt"
	"time"

	runnerv1 "github.com/google/ax/pkg/apis/runner/v1"
	"github.com/google/ax/pkg/apis/v1alpha1"
	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/protobuf/proto"
)

const (
	// ClaimWatchdogTimeout is how long a resumed QUEUED task waits for
	// ax-task-runner to claim its assignment before TimerExpired fires.
	ClaimWatchdogTimeout = 5 * time.Minute

	// MaxQueueWaitDuration bounds how long RESUME_ACTOR backs off on
	// RESOURCE_EXHAUSTED before failing the task.
	MaxQueueWaitDuration = 30 * time.Minute

	// DefaultActionInitialBackoff is the initial delay for capacity backoff.
	DefaultActionInitialBackoff = time.Second

	// DefaultActionMaxBackoff caps exponential capacity backoff.
	DefaultActionMaxBackoff = 60 * time.Second
)

var (
	// ErrInvalidTransition indicates an operation is invalid for the task's
	// current state (maps to gRPC FailedPrecondition).
	ErrInvalidTransition = errors.New("invalid task state transition")

	// ErrUnauthorizedRunner indicates a missing or mismatched X-AX-Runner-Token
	// on a runner control request (maps to HTTP 401/403).
	ErrUnauthorizedRunner = errors.New("unauthorized runner token")
)

// ActionKind identifies one idempotent Agent Substrate operation stored in
// tasks.pending_actions.
type ActionKind string

const (
	ActionWaitWorkspaceTag ActionKind = "WAIT_WORKSPACE_TAG"
	ActionEnsureTemplate   ActionKind = "ENSURE_TEMPLATE"
	ActionCreateActor      ActionKind = "CREATE_ACTOR"
	ActionEnsureEgress     ActionKind = "ENSURE_EGRESS"
	ActionResumeActor      ActionKind = "RESUME_ACTOR"
	ActionSuspendActorSync ActionKind = "SUSPEND_ACTOR_SYNC"
	ActionRevertActor      ActionKind = "REVERT_ACTOR"
	ActionDeleteActor      ActionKind = "DELETE_ACTOR"
	ActionCreateTag        ActionKind = "CREATE_TAG"
	ActionDeleteTag        ActionKind = "DELETE_TAG"
)

// SuspendReason records why ActionSuspendActorSync was queued so SUSPENDING
// transitions to the intended post-checkpoint state without adding internal
// reasons to the public TaskStatusReason proto enum.
type SuspendReason string

const (
	SuspendReasonUser     SuspendReason = "user"      // SuspendTask -> TASK_STATE_SUSPENDED
	SuspendReasonTurnIdle SuspendReason = "turn_idle" // IdleNotice -> TASK_STATE_IDLE
	SuspendReasonEviction SuspendReason = "eviction"  // EvictionNotice -> TASK_STATE_QUEUED + RESUME_ACTOR
	SuspendReasonSetup    SuspendReason = "setup"     // SetupFinished{ok: true} -> durable checkpoint + RESUME_ACTOR
	SuspendReasonTerminal SuspendReason = "terminal"  // Park actor after COMPLETED or FAILED
)

// Action is a single queued Substrate step serialized in tasks.pending_actions.
type Action struct {
	ID            string        `json:"id"`
	Kind          ActionKind    `json:"kind"`
	Tag           string        `json:"tag,omitempty"`
	SuspendReason SuspendReason `json:"suspendReason,omitempty"`
	Tries         int           `json:"tries"`
	Since         time.Time     `json:"since"`
	NotBefore     time.Time     `json:"notBefore,omitempty"`
}

// State bundles the Task resource together with the control-plane runtime
// columns stored alongside it on store.TaskRow. All fields are value-copied or
// cloned by Transition; Transition performs zero I/O.
type State struct {
	Task                 *v1alpha1.Task
	Actions              []Action
	ActorUID             string
	ActiveRunID          string
	RunSeq               int64
	ActionSeq            int64
	RunnerTokenHash      []byte
	RunnerTokenConfirmed bool
	RunnerTokenGen       int32
	LastAckedSeq         int64
	LastClaimNonce       string
	LastClaimResponse    *runnerv1.ClaimAssignmentResponse
	ClaimDeadline        *time.Time
	LastResumedAt        *time.Time
}

// Clone returns a deep copy of s safe to mutate inside Transition.
func (s State) Clone() State {
	out := s
	if s.Task != nil {
		out.Task = proto.Clone(s.Task).(*v1alpha1.Task)
	}
	if len(s.Actions) > 0 {
		out.Actions = make([]Action, len(s.Actions))
		copy(out.Actions, s.Actions)
	} else {
		out.Actions = nil
	}
	if len(s.RunnerTokenHash) > 0 {
		out.RunnerTokenHash = append([]byte(nil), s.RunnerTokenHash...)
	}
	if s.LastClaimResponse != nil {
		out.LastClaimResponse = proto.Clone(s.LastClaimResponse).(*runnerv1.ClaimAssignmentResponse)
	}
	if s.ClaimDeadline != nil {
		t := *s.ClaimDeadline
		out.ClaimDeadline = &t
	}
	if s.LastResumedAt != nil {
		t := *s.LastResumedAt
		out.LastResumedAt = &t
	}
	return out
}

// NextAction allocates a deterministic Action with a monotonic ID ("a-1", ...).
func (s *State) NextAction(kind ActionKind, now time.Time) Action {
	s.ActionSeq++
	return Action{
		ID:    fmt.Sprintf("a-%d", s.ActionSeq),
		Kind:  kind,
		Since: now,
	}
}

// NextRunID increments RunSeq and returns a monotonic run ID ("r-1", ...).
func (s *State) NextRunID() string {
	s.RunSeq++
	return fmt.Sprintf("r-%d", s.RunSeq)
}

func (s *State) clearClaimCache() {
	s.LastClaimNonce = ""
	s.LastClaimResponse = nil
}

// Output carries non-persisted results returned by Transition to the caller.
type Output struct {
	// Assignment is returned to ax-task-runner on Claim (or piggybacked on
	// ReportEventsResponse.next).
	Assignment *runnerv1.ClaimAssignmentResponse
	// DeleteRow instructs store.Apply to delete the task row from storage once
	// actor cleanup has finished.
	DeleteRow bool
}

// TransitionFunc is the signature of [Transition], allowing reconciler unit
// tests to inject deterministic state machine stubs.
type TransitionFunc func(state State, in Input, now time.Time) (State, Output, error)

// Input is the tagged union of every event that can mutate a Task. Exactly one
// pointer field should be non-nil per Transition call.
type Input struct {
	// gRPC API inputs
	Create  *CreateInput
	Suspend *SuspendInput
	Resume  *ResumeInput
	Delete  *DeleteInput

	// Runner HTTP API inputs (:8081)
	EventRunID     string
	EventSeq       int64
	Claim          *ClaimInput
	SetupFinished  *runnerv1.SetupFinished
	CommandStarted *runnerv1.CommandStarted
	CommandExited  *runnerv1.CommandExited
	EvictionNotice *runnerv1.EvictionNotice
	IdleNotice     *runnerv1.IdleNotice

	// Reconciler & drift sweep inputs
	ActionCompleted     *ActionCompletedInput
	ActionFailed        *ActionFailedInput
	CapacityUnavailable *CapacityUnavailableInput
	ObservedActor       *ObservedActorInput
	HeartbeatLost       *HeartbeatLostInput
	TimerExpired        *TimerExpiredInput
}

// CreateInput initializes a new Task in TASK_STATE_QUEUED.
type CreateInput struct {
	Task             *v1alpha1.Task
	AteActorTemplate string
	ResolvedArgv     []string
	SourceTag        string
	WorkspaceReady   bool
	// WaitWorkspaceTag prepends ActionWaitWorkspaceTag when the bound Workspace
	// is still pre-warming its default Profile snapshot.
	WaitWorkspaceTag bool
}

// SuspendInput requests a user-initiated checkpoint (SuspendTask).
type SuspendInput struct{}

// ResumeInput wakes a SUSPENDED or IDLE task (ResumeTask).
type ResumeInput struct{}

// DeleteInput marks a task for deletion and queues DELETE_ACTOR (DeleteTask).
type DeleteInput struct{}

// ClaimInput represents a POST /runner/v1/assignment:claim request.
type ClaimInput struct {
	Request              *runnerv1.ClaimAssignmentRequest
	ProvidedTokenHash    []byte
	CandidateRunnerToken string
	CandidateTokenHash   []byte
	Workspace            *v1alpha1.Workspace
}

// ActionCompletedInput reports that the head pending action succeeded on Substrate.
type ActionCompletedInput struct {
	ActionID string
	Kind     ActionKind
	// ActorUID is populated when CREATE_ACTOR completes.
	ActorUID string
	// SnapshotGen is the active durable Snapshot.generation reported by
	// Substrate when SUSPEND_ACTOR_SYNC or REVERT_ACTOR completes.
	SnapshotGen int32
	// SourceTag is populated when WAIT_WORKSPACE_TAG resolves a warm tag.
	SourceTag string
}

// ActionFailedInput reports a permanent Substrate error on the head action.
type ActionFailedInput struct {
	ActionID string
	Kind     ActionKind
	Reason   v1alpha1.TaskStatusReason
	Message  string
}

// CapacityUnavailableInput reports RESOURCE_EXHAUSTED on RESUME_ACTOR.
type CapacityUnavailableInput struct {
	ActionID string
	Message  string
}

// ObservedActorInput reports actor state drift observed by the periodic sweep
// (or FAILED_PRECONDITION when suspending a crashed actor).
type ObservedActorInput struct {
	ActorState       ateapipb.ActorState
	ActorUID         string
	CrashMessage     string
	MissingConfirmed bool
}

// HeartbeatLostInput reports that a running task's runner exceeded the silence
// threshold.
type HeartbeatLostInput struct {
	SilentFor time.Duration
}

// TimerExpiredInput fires when a due task has no ready pending actions (e.g.
// the 5-minute runner claim watchdog).
type TimerExpiredInput struct{}

// IsTerminal reports whether state is COMPLETED, FAILED, or CANCELLED.
func IsTerminal(state v1alpha1.TaskState) bool {
	switch state {
	case v1alpha1.TaskState_TASK_STATE_COMPLETED,
		v1alpha1.TaskState_TASK_STATE_FAILED,
		v1alpha1.TaskState_TASK_STATE_CANCELLED:
		return true
	default:
		return false
	}
}
