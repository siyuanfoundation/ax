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

package reconciler

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/ax/internal/lifecycle"
	"github.com/google/ax/internal/store"
	"github.com/google/ax/internal/substrate"
	"github.com/google/ax/pkg/apis/v1alpha1"
	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc/codes"
)

// ActionContext bundles task state and execution contexts passed to an [ActionExecutor].
type ActionContext struct {
	ParentCtx context.Context
	Ctx       context.Context
	Key       store.TaskKey
	Row       *store.TaskRow
	Action    lifecycle.Action
	StoreNow  time.Time
}

// ActionExecutor defines the strategy for executing a specific [lifecycle.ActionKind].
type ActionExecutor interface {
	Kind() lifecycle.ActionKind
	Execute(actx ActionContext) actionExecOutcome
}

func defaultExecutors(r *Reconciler) map[lifecycle.ActionKind]ActionExecutor {
	executors := []ActionExecutor{
		&waitWorkspaceTagExecutor{
			tagResolve:          r.tagResolve,
			maxWorkspaceTagWait: r.maxWorkspaceTagWait,
		},
		&ensureTemplateExecutor{runtime: r.runtime},
		&createActorExecutor{runtime: r.runtime},
		&ensureEgressExecutor{
			runtime:    r.runtime,
			extraRules: r.extraRules,
		},
		&resumeActorExecutor{runtime: r.runtime},
		&suspendActorSyncExecutor{runtime: r.runtime},
		&revertActorExecutor{runtime: r.runtime},
		&deleteActorExecutor{runtime: r.runtime},
		&createTagExecutor{runtime: r.runtime},
		&deleteTagExecutor{runtime: r.runtime},
	}
	m := make(map[lifecycle.ActionKind]ActionExecutor, len(executors))
	for _, e := range executors {
		m[e.Kind()] = e
	}
	return m
}

// --- Concrete Action Executors ---

type waitWorkspaceTagExecutor struct {
	tagResolve          WorkspaceTagResolver
	maxWorkspaceTagWait time.Duration
}

func (e *waitWorkspaceTagExecutor) Kind() lifecycle.ActionKind {
	return lifecycle.ActionWaitWorkspaceTag
}

func (e *waitWorkspaceTagExecutor) Execute(actx ActionContext) actionExecOutcome {
	atespace := actx.Key.Atespace
	templateName := actx.Row.Task.GetStatus().GetAteActorTemplate()
	wsName := actx.Row.Task.GetSpec().GetWorkspace().GetName()

	tag, ready, err := e.tagResolve(actx.Ctx, atespace, wsName, templateName, actx.Action.Tag)
	if err != nil {
		return classifyFailureOutcome(actx.ParentCtx, actx.Ctx, actx.Action, err)
	}
	if !ready {
		if !actx.Action.Since.IsZero() && actx.StoreNow.Sub(actx.Action.Since) >= e.maxWorkspaceTagWait {
			return actionExecOutcome{
				kind: outcomePermanentFailure,
				input: &lifecycle.ActionFailedInput{
					ActionID: actx.Action.ID,
					Kind:     actx.Action.Kind,
					Reason:   v1alpha1.TaskStatusReason_TASK_STATUS_REASON_PROVISIONING_FAILED,
					Message:  fmt.Sprintf("timed out after %s waiting for Workspace snapshot tag %q", e.maxWorkspaceTagWait, actx.Action.Tag),
				},
			}
		}
		return actionExecOutcome{kind: outcomeTransientRetry}
	}
	if tag == "" {
		tag = actx.Action.Tag
	}
	if tag == "" {
		return actionExecOutcome{
			kind: outcomePermanentFailure,
			input: &lifecycle.ActionFailedInput{
				ActionID: actx.Action.ID,
				Kind:     actx.Action.Kind,
				Reason:   v1alpha1.TaskStatusReason_TASK_STATUS_REASON_PROVISIONING_FAILED,
				Message:  "WAIT_WORKSPACE_TAG resolved ready=true with an empty snapshot tag",
			},
		}
	}
	return actionExecOutcome{
		kind: outcomeCompleted,
		input: &lifecycle.ActionCompletedInput{
			ActionID:  actx.Action.ID,
			Kind:      actx.Action.Kind,
			SourceTag: tag,
		},
	}
}

type ensureTemplateExecutor struct {
	runtime RuntimeClient
}

func (e *ensureTemplateExecutor) Kind() lifecycle.ActionKind {
	return lifecycle.ActionEnsureTemplate
}

func (e *ensureTemplateExecutor) Execute(actx ActionContext) actionExecOutcome {
	atespace := actx.Key.Atespace
	templateName := actx.Row.Task.GetStatus().GetAteActorTemplate()

	if err := e.runtime.EnsureTemplate(actx.Ctx, atespace, templateName, actx.Row.EffectiveProfile); err != nil {
		return classifyFailureOutcome(actx.ParentCtx, actx.Ctx, actx.Action, err)
	}
	return actionExecOutcome{
		kind: outcomeCompleted,
		input: &lifecycle.ActionCompletedInput{
			ActionID: actx.Action.ID,
			Kind:     actx.Action.Kind,
		},
	}
}

type createActorExecutor struct {
	runtime RuntimeClient
}

func (e *createActorExecutor) Kind() lifecycle.ActionKind {
	return lifecycle.ActionCreateActor
}

func (e *createActorExecutor) Execute(actx ActionContext) actionExecOutcome {
	atespace := actx.Key.Atespace
	actorName := actx.Key.Name
	templateName := actx.Row.Task.GetStatus().GetAteActorTemplate()

	uid, err := e.runtime.EnsureActor(actx.Ctx, atespace, actorName, templateName, actx.Action.Tag)
	if err != nil {
		return classifyFailureOutcome(actx.ParentCtx, actx.Ctx, actx.Action, err)
	}
	return actionExecOutcome{
		kind: outcomeCompleted,
		input: &lifecycle.ActionCompletedInput{
			ActionID: actx.Action.ID,
			Kind:     actx.Action.Kind,
			ActorUID: uid,
		},
	}
}

type ensureEgressExecutor struct {
	runtime    RuntimeClient
	extraRules []*ateapipb.EgressRule
}

func (e *ensureEgressExecutor) Kind() lifecycle.ActionKind {
	return lifecycle.ActionEnsureEgress
}

func (e *ensureEgressExecutor) combinedEgressRules(taskRules []*ateapipb.EgressRule) []*ateapipb.EgressRule {
	if len(e.extraRules) == 0 && len(taskRules) == 0 {
		return nil
	}
	out := make([]*ateapipb.EgressRule, 0, len(e.extraRules)+len(taskRules))
	out = append(out, e.extraRules...)
	out = append(out, taskRules...)
	return out
}

func (e *ensureEgressExecutor) Execute(actx ActionContext) actionExecOutcome {
	atespace := actx.Key.Atespace
	actorName := actx.Key.Name
	rules := e.combinedEgressRules(actx.Row.EgressRules)
	if len(rules) == 0 {
		return actionExecOutcome{
			kind: outcomeCompleted,
			input: &lifecycle.ActionCompletedInput{
				ActionID: actx.Action.ID,
				Kind:     actx.Action.Kind,
			},
		}
	}
	if err := e.runtime.EnsureActorEgress(actx.Ctx, atespace, actorName, rules); err != nil {
		if statusCode(err) == codes.NotFound && actx.Row.ActorUID != "" {
			return actionExecOutcome{
				kind:  outcomeObservedActor,
				input: &lifecycle.ObservedActorInput{MissingConfirmed: true},
			}
		}
		return classifyFailureOutcome(actx.ParentCtx, actx.Ctx, actx.Action, err)
	}
	return actionExecOutcome{
		kind: outcomeCompleted,
		input: &lifecycle.ActionCompletedInput{
			ActionID: actx.Action.ID,
			Kind:     actx.Action.Kind,
		},
	}
}

type resumeActorExecutor struct {
	runtime RuntimeClient
}

func (e *resumeActorExecutor) Kind() lifecycle.ActionKind {
	return lifecycle.ActionResumeActor
}

func (e *resumeActorExecutor) Execute(actx ActionContext) actionExecOutcome {
	atespace := actx.Key.Atespace
	actorName := actx.Key.Name

	if err := e.runtime.ResumeActor(actx.Ctx, atespace, actorName); err != nil {
		switch statusCode(err) {
		case codes.FailedPrecondition:
			return e.resolveResumePrecondition(actx, err)
		case codes.NotFound:
			if actx.Row.ActorUID != "" {
				return actionExecOutcome{
					kind:  outcomeObservedActor,
					input: &lifecycle.ObservedActorInput{MissingConfirmed: true},
				}
			}
		}
		return classifyFailureOutcome(actx.ParentCtx, actx.Ctx, actx.Action, err)
	}
	return actionExecOutcome{
		kind: outcomeCompleted,
		input: &lifecycle.ActionCompletedInput{
			ActionID: actx.Action.ID,
			Kind:     actx.Action.Kind,
		},
	}
}

func (e *resumeActorExecutor) resolveResumePrecondition(actx ActionContext, resumeErr error) actionExecOutcome {
	actor, getErr := e.runtime.GetActor(actx.Ctx, actx.Key.Atespace, actx.Key.Name)
	if getErr != nil {
		if statusCode(getErr) == codes.NotFound && actx.Row.ActorUID != "" {
			return actionExecOutcome{
				kind:  outcomeObservedActor,
				input: &lifecycle.ObservedActorInput{MissingConfirmed: true},
			}
		}
		return classifyFailureOutcome(actx.ParentCtx, actx.Ctx, actx.Action, getErr)
	}
	switch actor.GetStatus().GetState() {
	case ateapipb.ActorState_ACTOR_STATE_RUNNING:
		return actionExecOutcome{
			kind: outcomeCompleted,
			input: &lifecycle.ActionCompletedInput{
				ActionID: actx.Action.ID,
				Kind:     actx.Action.Kind,
			},
		}
	case ateapipb.ActorState_ACTOR_STATE_CRASHED:
		return actionExecOutcome{
			kind: outcomeObservedActor,
			input: &lifecycle.ObservedActorInput{
				ActorState:   ateapipb.ActorState_ACTOR_STATE_CRASHED,
				ActorUID:     actor.GetMetadata().GetUid(),
				CrashMessage: actor.GetStatus().GetCrash().GetMessage(),
			},
		}
	case ateapipb.ActorState_ACTOR_STATE_RESUMING,
		ateapipb.ActorState_ACTOR_STATE_SUSPENDING,
		ateapipb.ActorState_ACTOR_STATE_REVERTING:
		return actionExecOutcome{kind: outcomeTransientRetry}
	default:
		return classifyFailureOutcome(actx.ParentCtx, actx.Ctx, actx.Action, resumeErr)
	}
}

type suspendActorSyncExecutor struct {
	runtime RuntimeClient
}

func (e *suspendActorSyncExecutor) Kind() lifecycle.ActionKind {
	return lifecycle.ActionSuspendActorSync
}

func (e *suspendActorSyncExecutor) Execute(actx ActionContext) actionExecOutcome {
	atespace := actx.Key.Atespace
	actorName := actx.Key.Name

	terminalPark := actx.Action.SuspendReason == lifecycle.SuspendReasonTerminal ||
		lifecycle.IsTerminal(actx.Row.Task.GetStatus().GetState())
	if actx.Row.ActorUID == "" && terminalPark {
		return actionExecOutcome{
			kind: outcomeCompleted,
			input: &lifecycle.ActionCompletedInput{
				ActionID: actx.Action.ID,
				Kind:     actx.Action.Kind,
			},
		}
	}
	gen, err := e.runtime.SuspendActorSync(actx.Ctx, atespace, actorName)
	if err != nil {
		switch statusCode(err) {
		case codes.FailedPrecondition:
			return e.resolveSuspendPrecondition(actx, terminalPark, err)
		case codes.NotFound:
			if terminalPark {
				return actionExecOutcome{
					kind: outcomeCompleted,
					input: &lifecycle.ActionCompletedInput{
						ActionID: actx.Action.ID,
						Kind:     actx.Action.Kind,
					},
				}
			}
			if actx.Row.ActorUID != "" {
				return actionExecOutcome{
					kind:  outcomeObservedActor,
					input: &lifecycle.ObservedActorInput{MissingConfirmed: true},
				}
			}
		}
		return classifyFailureOutcome(actx.ParentCtx, actx.Ctx, actx.Action, err)
	}
	return actionExecOutcome{
		kind: outcomeCompleted,
		input: &lifecycle.ActionCompletedInput{
			ActionID:    actx.Action.ID,
			Kind:        actx.Action.Kind,
			SnapshotGen: gen,
		},
	}
}

func (e *suspendActorSyncExecutor) resolveSuspendPrecondition(actx ActionContext, terminalPark bool, suspendErr error) actionExecOutcome {
	actor, getErr := e.runtime.GetActor(actx.Ctx, actx.Key.Atespace, actx.Key.Name)
	if getErr != nil {
		if statusCode(getErr) == codes.NotFound {
			if terminalPark {
				return actionExecOutcome{
					kind: outcomeCompleted,
					input: &lifecycle.ActionCompletedInput{
						ActionID: actx.Action.ID,
						Kind:     actx.Action.Kind,
					},
				}
			}
			if actx.Row.ActorUID != "" {
				return actionExecOutcome{
					kind:  outcomeObservedActor,
					input: &lifecycle.ObservedActorInput{MissingConfirmed: true},
				}
			}
		}
		return classifyFailureOutcome(actx.ParentCtx, actx.Ctx, actx.Action, getErr)
	}
	switch actor.GetStatus().GetState() {
	case ateapipb.ActorState_ACTOR_STATE_SUSPENDED:
		if substrate.HasPendingSnapshotUpload(actor) {
			return actionExecOutcome{kind: outcomeTransientRetry}
		}
		return actionExecOutcome{
			kind: outcomeCompleted,
			input: &lifecycle.ActionCompletedInput{
				ActionID:    actx.Action.ID,
				Kind:        actx.Action.Kind,
				SnapshotGen: substrate.LatestDurableSnapshotGeneration(actor),
			},
		}
	case ateapipb.ActorState_ACTOR_STATE_CRASHED:
		return actionExecOutcome{
			kind: outcomeObservedActor,
			input: &lifecycle.ObservedActorInput{
				ActorState:   ateapipb.ActorState_ACTOR_STATE_CRASHED,
				ActorUID:     actor.GetMetadata().GetUid(),
				CrashMessage: actor.GetStatus().GetCrash().GetMessage(),
			},
		}
	case ateapipb.ActorState_ACTOR_STATE_RESUMING,
		ateapipb.ActorState_ACTOR_STATE_SUSPENDING,
		ateapipb.ActorState_ACTOR_STATE_REVERTING:
		return actionExecOutcome{kind: outcomeTransientRetry}
	default:
		return classifyFailureOutcome(actx.ParentCtx, actx.Ctx, actx.Action, suspendErr)
	}
}

type revertActorExecutor struct {
	runtime RuntimeClient
}

func (e *revertActorExecutor) Kind() lifecycle.ActionKind {
	return lifecycle.ActionRevertActor
}

func (e *revertActorExecutor) Execute(actx ActionContext) actionExecOutcome {
	atespace := actx.Key.Atespace
	actorName := actx.Key.Name

	gen, err := e.runtime.RevertActor(actx.Ctx, atespace, actorName)
	if err != nil {
		if statusCode(err) == codes.NotFound && actx.Row.ActorUID != "" {
			return actionExecOutcome{
				kind:  outcomeObservedActor,
				input: &lifecycle.ObservedActorInput{MissingConfirmed: true},
			}
		}
		return classifyFailureOutcome(actx.ParentCtx, actx.Ctx, actx.Action, err)
	}
	return actionExecOutcome{
		kind: outcomeCompleted,
		input: &lifecycle.ActionCompletedInput{
			ActionID:    actx.Action.ID,
			Kind:        actx.Action.Kind,
			SnapshotGen: gen,
		},
	}
}

type deleteActorExecutor struct {
	runtime RuntimeClient
}

func (e *deleteActorExecutor) Kind() lifecycle.ActionKind {
	return lifecycle.ActionDeleteActor
}

func (e *deleteActorExecutor) Execute(actx ActionContext) actionExecOutcome {
	atespace := actx.Key.Atespace
	actorName := actx.Key.Name

	if err := e.runtime.DeleteActor(actx.Ctx, atespace, actorName, actx.Row.ActorUID); err != nil {
		return classifyFailureOutcome(actx.ParentCtx, actx.Ctx, actx.Action, err)
	}
	return actionExecOutcome{
		kind: outcomeCompleted,
		input: &lifecycle.ActionCompletedInput{
			ActionID: actx.Action.ID,
			Kind:     actx.Action.Kind,
		},
	}
}

type createTagExecutor struct {
	runtime RuntimeClient
}

func (e *createTagExecutor) Kind() lifecycle.ActionKind {
	return lifecycle.ActionCreateTag
}

func (e *createTagExecutor) Execute(actx ActionContext) actionExecOutcome {
	atespace := actx.Key.Atespace
	actorName := actx.Key.Name

	if err := e.runtime.CreateSnapshotTag(actx.Ctx, atespace, actorName, actx.Action.Tag); err != nil {
		if statusCode(err) == codes.NotFound && actx.Row.ActorUID != "" {
			return actionExecOutcome{
				kind:  outcomeObservedActor,
				input: &lifecycle.ObservedActorInput{MissingConfirmed: true},
			}
		}
		return classifyFailureOutcome(actx.ParentCtx, actx.Ctx, actx.Action, err)
	}
	return actionExecOutcome{
		kind: outcomeCompleted,
		input: &lifecycle.ActionCompletedInput{
			ActionID: actx.Action.ID,
			Kind:     actx.Action.Kind,
		},
	}
}

type deleteTagExecutor struct {
	runtime RuntimeClient
}

func (e *deleteTagExecutor) Kind() lifecycle.ActionKind {
	return lifecycle.ActionDeleteTag
}

func (e *deleteTagExecutor) Execute(actx ActionContext) actionExecOutcome {
	atespace := actx.Key.Atespace

	if err := e.runtime.DeleteSnapshotTag(actx.Ctx, atespace, actx.Action.Tag); err != nil {
		return classifyFailureOutcome(actx.ParentCtx, actx.Ctx, actx.Action, err)
	}
	return actionExecOutcome{
		kind: outcomeCompleted,
		input: &lifecycle.ActionCompletedInput{
			ActionID: actx.Action.ID,
			Kind:     actx.Action.Kind,
		},
	}
}

// classifyFailureOutcome maps an error from an Action attempt into its actionExecOutcome.
func classifyFailureOutcome(parentCtx, callCtx context.Context, action lifecycle.Action, err error) actionExecOutcome {
	if parentCtx.Err() != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
		statusCode(err) == codes.Canceled || statusCode(err) == codes.DeadlineExceeded) {
		return actionExecOutcome{kind: outcomeCanceled}
	}
	switch classifySubstrateError(callCtx, action.Kind, err) {
	case errorClassCanceled:
		return actionExecOutcome{kind: outcomeCanceled}
	case errorClassCapacityUnavailable:
		return actionExecOutcome{
			kind: outcomeCapacityUnavailable,
			input: &lifecycle.CapacityUnavailableInput{
				ActionID: action.ID,
				Message:  substrateMessage(err),
			},
		}
	case errorClassTransient:
		return actionExecOutcome{kind: outcomeTransientRetry}
	default:
		return actionExecOutcome{
			kind: outcomePermanentFailure,
			input: &lifecycle.ActionFailedInput{
				ActionID: action.ID,
				Kind:     action.Kind,
				Reason:   v1alpha1.TaskStatusReason_TASK_STATUS_REASON_PROVISIONING_FAILED,
				Message:  substrateMessage(err),
			},
		}
	}
}
