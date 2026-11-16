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
	"errors"
	"time"

	"github.com/google/ax/internal/lifecycle"
	"github.com/google/ax/pkg/apis/v1alpha1"
)

// ErrLeaseLost indicates that a queue worker no longer owns the active lease on
// a task row (because the row was deleted, the lease expired, or another worker
// acquired it). Callers must discard any in-flight action result without
// retrying under the stale lease token.
var ErrLeaseLost = errors.New("task worker lease lost")

// LeasedTask represents time-bounded ownership of a due task row granted to a
// single reconciler worker.
type LeasedTask struct {
	Key        TaskKey
	LeaseToken string
	LeasedAt   time.Time
	ExpiresAt  time.Time
	Row        *TaskRow
}

// HasActiveLease reports whether row currently holds an unexpired worker lease
// matching leaseToken at timestamp now.
func HasActiveLease(row *TaskRow, leaseToken string, now time.Time) bool {
	return row != nil &&
		leaseToken != "" &&
		row.LeaseOwner == leaseToken &&
		row.LeaseExpiresAt != nil &&
		row.LeaseExpiresAt.After(now)
}

// NormalizeTaskForApply scrubs live Substrate actor fields, overwrites metadata
// key fields with key, fills API/Kind/Status defaults, and mirrors generation
// onto Task.Status.Generation.
func NormalizeTaskForApply(task *v1alpha1.Task, key TaskKey, generation int64) error {
	if task == nil {
		return errors.New("task row returned from ApplyFunc has nil Task")
	}
	key = key.Normalized()
	ScrubStoredTask(task)
	if task.Metadata == nil {
		task.Metadata = &v1alpha1.ObjectMeta{}
	}
	task.Metadata.Atespace = key.Atespace
	task.Metadata.Name = key.Name
	if task.ApiVersion == "" {
		task.ApiVersion = v1alpha1.APIVersion
	}
	if task.Kind == "" {
		task.Kind = v1alpha1.KindTask
	}
	if task.Status == nil {
		task.Status = &v1alpha1.TaskStatus{}
	}
	task.Status.Generation = generation
	return nil
}

// LeaseFilter restricts which due task rows [TaskQueue.LeaseDue] claims so
// per-atespace and per-action-kind admission caps on one replica skip saturated
// work directly in storage without head-of-line blocking or lease-release churn.
type LeaseFilter struct {
	// ExcludeAtespaces skips due rows belonging to any listed atespace.
	ExcludeAtespaces []string
	// ExcludeActionKinds skips due rows whose runnable head action (Actions[0]
	// with NotBefore <= now) has a Kind in this list.
	ExcludeActionKinds []lifecycle.ActionKind
	// ExcludeOtherActionKinds, when true, skips due rows whose runnable head
	// action has a Kind outside BucketedActionKinds (used when the shared
	// fallback action-kind bucket is at capacity).
	ExcludeOtherActionKinds bool
	// BucketedActionKinds lists the explicitly bucketed ActionKind values when
	// ExcludeOtherActionKinds is true.
	BucketedActionKinds []lifecycle.ActionKind
}

// MergeLeaseFilters combines zero or more [LeaseFilter] values into one.
func MergeLeaseFilters(filters []LeaseFilter) LeaseFilter {
	switch len(filters) {
	case 0:
		return LeaseFilter{}
	case 1:
		return filters[0]
	default:
		var out LeaseFilter
		for _, f := range filters {
			out.ExcludeAtespaces = append(out.ExcludeAtespaces, f.ExcludeAtespaces...)
			out.ExcludeActionKinds = append(out.ExcludeActionKinds, f.ExcludeActionKinds...)
			if f.ExcludeOtherActionKinds {
				out.ExcludeOtherActionKinds = true
				out.BucketedActionKinds = append(out.BucketedActionKinds, f.BucketedActionKinds...)
			}
		}
		return out
	}
}

// TaskQueue extends [TaskApplier] with leased work-queue operations for
// action-driven task reconciliation.
//
// All timestamp comparisons (due checks, lease expiry, and backoff deadlines)
// are evaluated against the store's own clock so clock skew across ax-server
// replicas never affects correctness.
type TaskQueue interface {
	TaskApplier

	// LeaseDue atomically claims up to limit due task rows
	// (next_reconcile_at <= now), advancing both lease_expires_at and
	// next_reconcile_at to now + leaseTTL so in-flight rows leave the due
	// range while leased. Any saturated atespaces or saturated runnable action
	// kinds supplied via filters are skipped so saturated buckets on one
	// replica cannot head-of-line block other due work or trigger repeated
	// lease-and-release churn. Concurrent callers never block one another.
	LeaseDue(ctx context.Context, workerID string, leaseTTL time.Duration, limit int, filters ...LeaseFilter) ([]LeasedTask, error)

	// LeaseTask attempts to acquire a worker lease on a single task row if it
	// exists and is currently due (next_reconcile_at <= now).
	//
	// When the lease cannot be granted immediately:
	//   - contended reports whether another worker currently holds an unexpired
	//     lease on due work (runnable actions or an expired timer) for key;
	//   - retryIn reports the remaining store-clock duration until
	//     next_reconcile_at when the row is unleased and still has pending
	//     actions waiting on a future backoff (0 when missing, contended, at
	//     rest, or waiting only on a non-action timer such as ClaimDeadline).
	LeaseTask(ctx context.Context, key TaskKey, workerID string, leaseTTL time.Duration) (leased *LeasedTask, contended bool, retryIn time.Duration, err error)

	// RenewLease extends an active worker lease to now + leaseTTL and returns
	// the ID of the current head action (Actions[0].ID, or "" if Actions is
	// empty) so the caller can cancel an in-flight Substrate RPC whose action
	// was superseded. Returns [ErrLeaseLost] if leaseToken is no longer the
	// active, unexpired lease holder.
	RenewLease(ctx context.Context, key TaskKey, leaseToken string, leaseTTL time.Duration) (headActionID string, err error)

	// ReleaseLease clears the worker lease matching leaseToken and recomputes
	// next_reconcile_at from the row's current lifecycle state without bumping
	// Generation or rewriting the Task document. When requeueAfter > 0 and the
	// task still requires reconciliation, next_reconcile_at is scheduled no
	// earlier than now + requeueAfter (used when admission control defers a
	// task without incrementing Action.Tries). Returns [ErrLeaseLost] if
	// leaseToken is no longer active.
	ReleaseLease(ctx context.Context, key TaskKey, leaseToken string, requeueAfter time.Duration) error

	// BackoffAction records a transient execution failure on actionID under
	// leaseToken: if actionID is still at Actions[0], it increments
	// Actions[0].Tries and sets Actions[0].NotBefore = now + backoff; if
	// actionID was superseded by a newer head action, Actions[0] is left
	// untouched so the replacement action runs without delay. In both cases the
	// lease is released and next_reconcile_at is updated without bumping
	// Generation or rewriting the Task document. Returns [ErrLeaseLost] if
	// leaseToken is no longer active.
	BackoffAction(ctx context.Context, key TaskKey, leaseToken, actionID string, backoff time.Duration) error

	// SetQueueWakeup registers an optional callback for the single local
	// ModeQueue poller attached to this store, invoked after Apply,
	// ReleaseLease, or BackoffAction commits a row that is immediately due so
	// local workers pick up work without waiting for the next poll tick.
	SetQueueWakeup(fn func())
}
