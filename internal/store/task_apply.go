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

package store

import (
	"context"
	"time"

	"github.com/google/ax/internal/lifecycle"
	runnerv1 "github.com/google/ax/pkg/apis/runner/v1"
	"github.com/google/ax/pkg/apis/v1alpha1"
	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/protobuf/proto"
)

// TaskKey uniquely identifies a Task within an atespace.
type TaskKey struct {
	Atespace string
	Name     string
}

// Normalized returns k with an empty Atespace defaulted to DefaultAtespace.
func (k TaskKey) Normalized() TaskKey {
	if k.Atespace == "" {
		k.Atespace = v1alpha1.DefaultAtespace
	}
	return k
}

// TaskRow holds the persisted Task document together with all control-plane,
// runner authentication, and action-queue columns stored on the tasks row.
type TaskRow struct {
	Task                 *v1alpha1.Task
	EffectiveProfile     *v1alpha1.Profile
	EgressRules          []*ateapipb.EgressRule
	Generation           int64
	SessionID            string
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
	LastHeartbeatAt      *time.Time
	LastResumedAt        *time.Time
	ClaimDeadline        *time.Time
	PendingActions       []lifecycle.Action
	NextReconcileAt      *time.Time
	LeaseOwner           string
	LeaseExpiresAt       *time.Time
}

// Clone returns a deep copy of r safe to mutate inside an ApplyFunc.
func (r *TaskRow) Clone() *TaskRow {
	if r == nil {
		return nil
	}
	out := *r
	if r.Task != nil {
		out.Task = proto.Clone(r.Task).(*v1alpha1.Task)
	}
	if r.EffectiveProfile != nil {
		out.EffectiveProfile = proto.Clone(r.EffectiveProfile).(*v1alpha1.Profile)
	}
	if len(r.EgressRules) > 0 {
		out.EgressRules = make([]*ateapipb.EgressRule, len(r.EgressRules))
		for i, rule := range r.EgressRules {
			if rule != nil {
				out.EgressRules[i] = proto.Clone(rule).(*ateapipb.EgressRule)
			}
		}
	} else {
		out.EgressRules = nil
	}
	if len(r.RunnerTokenHash) > 0 {
		out.RunnerTokenHash = append([]byte(nil), r.RunnerTokenHash...)
	} else {
		out.RunnerTokenHash = nil
	}
	if r.LastClaimResponse != nil {
		out.LastClaimResponse = proto.Clone(r.LastClaimResponse).(*runnerv1.ClaimAssignmentResponse)
	}
	if len(r.PendingActions) > 0 {
		out.PendingActions = make([]lifecycle.Action, len(r.PendingActions))
		copy(out.PendingActions, r.PendingActions)
	} else {
		out.PendingActions = nil
	}
	out.LastHeartbeatAt = cloneTimePtr(r.LastHeartbeatAt)
	out.LastResumedAt = cloneTimePtr(r.LastResumedAt)
	out.ClaimDeadline = cloneTimePtr(r.ClaimDeadline)
	out.NextReconcileAt = cloneTimePtr(r.NextReconcileAt)
	out.LeaseExpiresAt = cloneTimePtr(r.LeaseExpiresAt)
	return &out
}

// LifecycleState extracts the pure [lifecycle.State] view of r.
func (r *TaskRow) LifecycleState() lifecycle.State {
	if r == nil {
		return lifecycle.State{}
	}
	cp := r.Clone()
	return lifecycle.State{
		Task:                 cp.Task,
		Actions:              cp.PendingActions,
		ActorUID:             cp.ActorUID,
		ActiveRunID:          cp.ActiveRunID,
		RunSeq:               cp.RunSeq,
		ActionSeq:            cp.ActionSeq,
		RunnerTokenHash:      cp.RunnerTokenHash,
		RunnerTokenConfirmed: cp.RunnerTokenConfirmed,
		RunnerTokenGen:       cp.RunnerTokenGen,
		LastAckedSeq:         cp.LastAckedSeq,
		LastClaimNonce:       cp.LastClaimNonce,
		LastClaimResponse:    cp.LastClaimResponse,
		ClaimDeadline:        cp.ClaimDeadline,
		LastResumedAt:        cp.LastResumedAt,
	}
}

// SetLifecycleState writes s back onto r and recomputes NextReconcileAt at now.
// Plaintext RunnerToken and read-only Substrate actor identity fields (status.actor
// and status.actor_uid) are scrubbed so they are never persisted.
func (r *TaskRow) SetLifecycleState(s lifecycle.State, now time.Time) {
	c := s.Clone()
	ScrubClaimResponse(c.LastClaimResponse)
	ScrubStoredTask(c.Task)
	r.Task = c.Task
	r.PendingActions = c.Actions
	r.ActorUID = c.ActorUID
	r.ActiveRunID = c.ActiveRunID
	r.RunSeq = c.RunSeq
	r.ActionSeq = c.ActionSeq
	r.RunnerTokenHash = c.RunnerTokenHash
	r.RunnerTokenConfirmed = c.RunnerTokenConfirmed
	r.RunnerTokenGen = c.RunnerTokenGen
	r.LastAckedSeq = c.LastAckedSeq
	r.LastClaimNonce = c.LastClaimNonce
	r.LastClaimResponse = c.LastClaimResponse
	r.ClaimDeadline = c.ClaimDeadline
	r.LastResumedAt = c.LastResumedAt
	if r.Task != nil {
		r.SessionID = r.Task.GetSpec().GetSessionId()
	}
	r.NextReconcileAt = lifecycle.NextReconcileAt(c, now)
}

// ScrubStoredTask clears live Substrate-owned fields on t.Status (actor and
// actor_uid) that must only be overlaid at read time (task.proto:162, 180).
func ScrubStoredTask(t *v1alpha1.Task) {
	if t != nil && t.Status != nil {
		t.Status.Actor = ""
		t.Status.ActorUid = ""
	}
}

// ScrubClaimResponse clears secrets and live Substrate-owned fields from a
// cached ClaimAssignmentResponse before persisting it.
func ScrubClaimResponse(resp *runnerv1.ClaimAssignmentResponse) {
	if resp != nil {
		resp.RunnerToken = ""
		ScrubStoredTask(resp.Task)
	}
}

func cloneTimePtr(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	v := *t
	return &v
}

// ApplyFunc computes the next TaskRow from current under a short row lock at
// database clock time now. ApplyFunc MUST NOT perform network I/O, and MUST be
// safe to invoke again if a concurrent transaction wins an initial row creation
// race (invoking fn(nil, now) followed by fn(winner, now)).
//
// Contract:
//   - When current == nil (row does not exist):
//   - Returning (nil, nil) is a no-op and Apply returns (nil, nil).
//   - Returning (next, nil) inserts a new row with Generation = 1 and saved_at = now.
//   - When current != nil (row exists):
//   - Returning (nil, nil) deletes the row inside the transaction and Apply returns (nil, nil).
//   - Returning (current, nil) (same pointer) is a read-only no-op that leaves Generation unchanged.
//   - Returning (next, nil) (where next != current) increments Generation (+1), mirrors it to
//     next.Task.Status.Generation, preserves saved_at, clears any expired lease
//     (lease_expires_at <= now), and clamps next_reconcile_at >= lease_expires_at while a
//     live lease is held.
type ApplyFunc func(current *TaskRow, now time.Time) (*TaskRow, error)

// TaskApplier is the single transactional write seam for all Task mutations.
type TaskApplier interface {
	Apply(ctx context.Context, key TaskKey, fn ApplyFunc) (*TaskRow, error)
	GetTaskRow(ctx context.Context, key TaskKey) (*TaskRow, error)
	// DueNotify returns a 1-buffered channel signaled after an Apply commits a
	// task row that is immediately due (next_reconcile_at <= now) without an
	// active lease.
	DueNotify() <-chan struct{}
}
