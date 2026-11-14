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
)

// LeasedTask represents a due task row leased to a single reconciler worker.
type LeasedTask struct {
	Row            *TaskRow
	LeaseToken     string
	LeaseExpiresAt time.Time
}

// HeartbeatEntry is one buffered runner heartbeat flushed in batches to
// PostgreSQL, verified against tasks.runner_token_hash in SQL.
type HeartbeatEntry struct {
	Key             TaskKey
	RunnerTokenHash []byte
	At              time.Time
}

// ReplicaInfo describes a live ax-server replica registered in ax_replicas.
type ReplicaInfo struct {
	ReplicaID   string
	StartedAt   time.Time
	LastFlushAt time.Time
	ExpiresAt   time.Time
}

// TaskQueueStore extends [TaskApplier] with leased queue polling, active-task
// paging for the drift sweep, and batched heartbeat ingestion.
type TaskQueueStore interface {
	TaskApplier

	// LeaseDue claims up to limit due rows (next_reconcile_at <= now()) for
	// leaseTTL (default 30s when <= 0), advancing both lease_expires_at and
	// next_reconcile_at to now() + leaseTTL.
	LeaseDue(ctx context.Context, workerID string, limit int, leaseTTL time.Duration) ([]LeasedTask, error)

	// RenewLease extends an active lease by leaseTTL (default 30s when <= 0) and
	// returns the current head pending action ID so workers can abort obsolete
	// in-flight Substrate calls. Returns ErrLeaseLost if leaseToken no longer owns key.
	RenewLease(ctx context.Context, key TaskKey, leaseToken string, leaseTTL time.Duration) (headActionID string, expiresAt time.Time, err error)

	// ReleaseOrBackoff clears leaseToken on key and recomputes next_reconcile_at.
	// When backoffDelay > 0, next_reconcile_at is set to at least now() + backoffDelay,
	// and when incrementTries is true (transient Substrate error vs. admission
	// cap requeue), PendingActions[0].Tries is incremented.
	ReleaseOrBackoff(ctx context.Context, key TaskKey, leaseToken string, backoffDelay time.Duration, incrementTries bool) error

	// ListActive pages non-terminal task rows (state NOT IN COMPLETED, FAILED,
	// CANCELLED) ordered by (atespace, name) strictly after cursor after, scoped
	// to atespace when non-empty or across all atespaces when atespace == "".
	//
	// Note for drift sweep callers: Because terminal tasks are excluded from
	// tasks_active_idx, sweepers observing an unknown RUNNING actor on Substrate
	// must skip non-Task actors (e.g. runnerv1.IsSetupActorName and Substrate
	// golden-* actors) and check GetTaskRow before re-parking a woken terminal task.
	ListActive(ctx context.Context, atespace string, after TaskKey, limit int) ([]*TaskRow, error)

	// ListActiveAtespaces returns the distinct atespaces that currently have at
	// least one non-terminal task row, used by sweep shards (hash(atespace) mod K)
	// to avoid polling empty atespaces.
	ListActiveAtespaces(ctx context.Context) ([]string, error)

	// FlushHeartbeats batch-updates last_heartbeat_at for entries whose
	// RunnerTokenHash matches tasks.runner_token_hash and refreshes replicaID's
	// last_flush_at in ax_replicas.
	FlushHeartbeats(ctx context.Context, replicaID string, entries []HeartbeatEntry) error
}

// ReplicaStore tracks live ax-server replicas and the heartbeat flush watermark
// used to gate lost-runner detection during database slowness.
type ReplicaStore interface {
	UpsertReplica(ctx context.Context, replicaID string, ttl time.Duration) (dbNow time.Time, err error)
	ListLiveReplicas(ctx context.Context) (replicas []ReplicaInfo, dbNow time.Time, err error)
}
