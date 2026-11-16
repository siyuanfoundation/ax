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

	"github.com/google/ax/pkg/apis/v1alpha1"
)

// HeartbeatEntry represents a runner liveness observation buffered in memory on
// an ax-server replica and flushed in batch to storage.
//
// Age records the monotonic duration elapsed on the receiving replica between
// when the runner's authenticated request arrived and when the batch flush
// started, allowing storage to compute (store_clock_now - Age) without any
// sensitivity to wall-clock skew across replicas.
type HeartbeatEntry struct {
	Key             TaskKey
	RunnerTokenHash []byte
	Age             time.Duration
}

// ReplicaStats summarizes live ax-server replicas and the cluster-wide runner
// heartbeat flush watermark observed from storage's clock.
type ReplicaStats struct {
	// LiveReplicas is the count of replicas in ax_replicas with expires_at > StoreNow.
	LiveReplicas int
	// HasLiveReplicas is true when LiveReplicas > 0.
	HasLiveReplicas bool
	// WatermarkAge is StoreNow minus the oldest last_flush_at among live
	// replicas (0 when HasLiveReplicas is false). Lost-runner checks pause
	// whenever !HasLiveReplicas or WatermarkAge exceeds 60s.
	WatermarkAge time.Duration
	// StoreNow is the storage clock timestamp at which the snapshot was taken.
	StoreNow time.Time
}

// TaskSweepRecord is a compact, TOAST-free projection of a task row read by the
// background actor drift sweep before paging Substrate's ListActors API.
//
// Omitting the large Task JSONB document keeps sweep memory and PostgreSQL read
// bandwidth proportional to narrow index/heap metadata rather than task output
// size; any drift mutation re-reads and fences on Generation under store.Apply.
type TaskSweepRecord struct {
	// Key is the task's normalized (Atespace, Name) identifier.
	Key TaskKey
	// State is the committed TaskState on the row.
	State v1alpha1.TaskState
	// Generation is the monotonic row revision used to fence drift updates.
	Generation int64
	// ActorUID is the Substrate UID of the actor owned by this task ("" before CREATE_ACTOR).
	ActorUID string
	// HasPendingActions reports whether pending_actions is non-empty (worker-owned).
	HasPendingActions bool
	// HasRunnerToken reports whether runner_token_hash is non-empty.
	HasRunnerToken bool
	// HasClaimDeadline reports whether claim_deadline is currently set.
	HasClaimDeadline bool
	// CreatedAt is the storage timestamp when the task row was created, used to
	// lower-bound lost-runner silence so a brand-new task never inherits an
	// older shard-acquisition baseline.
	CreatedAt time.Time
	// LastResumedAt is the store timestamp of the task's latest actor resume.
	LastResumedAt *time.Time
	// LastHeartbeatAt is the store timestamp of the task's latest runner heartbeat.
	LastHeartbeatAt *time.Time
}

// RowExpectsHeartbeats reports whether row is in steady-state runner execution
// where the drift sweep monitors LastHeartbeatAt for silence (WORKING or
// claimed QUEUED with a non-empty RunnerTokenHash and no pending actions).
func RowExpectsHeartbeats(row *TaskRow) bool {
	if row == nil || row.Task == nil || len(row.Actions) > 0 || len(row.RunnerTokenHash) == 0 {
		return false
	}
	switch row.Task.GetStatus().GetState() {
	case v1alpha1.TaskState_TASK_STATE_WORKING:
		return true
	case v1alpha1.TaskState_TASK_STATE_QUEUED:
		return row.ClaimDeadline == nil
	default:
		return false
	}
}

// TaskSweepStore extends [TaskApplier] with narrow active-task snapshots,
// batched runner heartbeat flushes, replica watermark inspection, and
// non-blocking sweep shard leases.
type TaskSweepStore interface {
	TaskApplier

	// ListTaskAtespaces returns all distinct atespaces that currently contain at
	// least one AX task row (active or terminal) in sorted order.
	ListTaskAtespaces(ctx context.Context) ([]string, error)

	// SnapshotActiveTasks returns a point-in-time slice of all non-terminal task
	// records belonging to atespaces (or across all atespaces when atespaces is
	// empty), ordered by (atespace, name), together with the store clock
	// timestamp captured at the start of the snapshot.
	SnapshotActiveTasks(ctx context.Context, atespaces []string) (records []TaskSweepRecord, storeNow time.Time, err error)

	// GetTaskRows loads full TaskRow records by primary key in bounded chunks,
	// clearing any expired worker leases against the store clock. Missing keys
	// are omitted from the returned map. Used for rare candidate woken terminal
	// tasks, observe-only sweep validation, and anomalous heartbeat token checks.
	GetTaskRows(ctx context.Context, keys []TaskKey) (map[TaskKey]*TaskRow, error)

	// FlushHeartbeats applies a batch of buffered runner heartbeats (updating
	// tasks.last_heartbeat_at only where runner_token_hash matches and the new
	// timestamp advances last_heartbeat_at) and refreshes replicaID's liveness
	// and last_flush_at watermark in ax_replicas with TTL replicaTTL.
	//
	// Calling FlushHeartbeats with an empty entries slice still registers or
	// renews replicaID in ax_replicas so idle replicas keep the cluster
	// watermark fresh.
	FlushHeartbeats(ctx context.Context, replicaID string, replicaTTL time.Duration, entries []HeartbeatEntry) error

	// UnregisterReplica removes replicaID from ax_replicas on graceful shutdown
	// after its final heartbeat flush succeeds.
	UnregisterReplica(ctx context.Context, replicaID string) error

	// InspectReplicas returns the current live replica count and heartbeat
	// flush watermark age evaluated against the store's clock.
	InspectReplicas(ctx context.Context) (ReplicaStats, error)

	// TryAcquireShardLease attempts a non-blocking acquisition of shardKey in
	// the distributed locks table for holderToken with duration ttl, succeeding
	// if shardKey is unheld, expired, or already held by holderToken.
	TryAcquireShardLease(ctx context.Context, shardKey, holderToken string, ttl time.Duration) (acquired bool, storeNow time.Time, err error)

	// RenewShardLease extends an active, unexpired shard lease held by
	// holderToken to storeNow + ttl. Returns renewed == false (with err == nil)
	// if shardKey is missing, expired, or held by a different token.
	RenewShardLease(ctx context.Context, shardKey, holderToken string, ttl time.Duration) (renewed bool, storeNow time.Time, err error)

	// ReleaseShardLease deletes shardKey if and only if it is currently held by
	// holderToken.
	ReleaseShardLease(ctx context.Context, shardKey, holderToken string) error
}
