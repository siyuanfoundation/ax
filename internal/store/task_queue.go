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

	"github.com/google/ax/pkg/apis/v1alpha1"
)

// DefaultLeaseTTL is the default duration for which a worker leases a due task
// row when leaseTTL <= 0 is passed to LeaseDue or RenewLease.
const DefaultLeaseTTL = 30 * time.Second

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

// TaskQueue extends [TaskApplier] with leased work-queue operations for
// action-driven task reconciliation.
//
// TODO: Once basic reconciliation is wired end-to-end, add optional LeaseDue
// filtering (e.g. skipping saturated atespaces and saturated action kinds) if
// multi-tenant fairness / per-action-kind admission slots are added.
type TaskQueue interface {
	TaskApplier

	// LeaseDue atomically claims up to limit rows with NextReconcileAt <= now,
	// setting LeaseOwner to a unique token prefixed with workerID and advancing
	// both LeaseExpiresAt and NextReconcileAt to now + leaseTTL so no other
	// worker claims the row while leased.
	LeaseDue(ctx context.Context, workerID string, leaseTTL time.Duration, limit int) ([]LeasedTask, error)

	// RenewLease extends an active worker lease to now + leaseTTL and returns
	// the row's current head Action.ID (or "" if Actions is empty), allowing the
	// worker to cancel an in-flight Substrate call whose action was superseded.
	// Returns [ErrLeaseLost] if the row no longer exists or leaseToken is no
	// longer the active unexpired lease owner.
	RenewLease(ctx context.Context, key TaskKey, leaseToken string, leaseTTL time.Duration) (headActionID string, err error)

	// ReleaseLease clears the worker lease on key if leaseToken still owns it,
	// recomputing NextReconcileAt from the stored lifecycle state (pushed out to
	// at least now + requeueAfter when requeueAfter > 0). It is a no-op if the
	// row is missing or owned by another lease.
	ReleaseLease(ctx context.Context, key TaskKey, leaseToken string, requeueAfter time.Duration) error

	// BackoffAction increments Tries and sets NotBefore = now + backoff on the
	// head action matching actionID, releases leaseToken, and updates
	// NextReconcileAt in a single atomic write. It is a no-op if leaseToken no
	// longer owns the lease or actionID is no longer at Actions[0].
	BackoffAction(ctx context.Context, key TaskKey, leaseToken, actionID string, backoff time.Duration) error

	// SetQueueWakeup registers a non-blocking callback invoked after Apply
	// commits a row whose NextReconcileAt is due immediately (<= now). Passing
	// nil clears the callback.
	SetQueueWakeup(fn func())
}
