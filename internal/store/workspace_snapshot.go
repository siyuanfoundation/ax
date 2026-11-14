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
	"fmt"
	"math"
	"time"
)

// ErrSnapshotConflict is returned by
// [WorkspaceSnapshotStore.CompareAndSwapWorkspaceSnapshot] when the stored
// snapshot does not match the caller's expected revision because another writer
// published or deleted a snapshot after the caller read it. When this error is
// returned, no changes have been written.
//
// Callers must not blindly retry with the same snapshot; they should either
// abort or re-read the current snapshot to check whether a new snapshot is
// still needed.
var ErrSnapshotConflict = errors.New("workspace snapshot was changed by another writer")

// WorkspaceSnapshot records the warm Substrate snapshot Tag used to seed new
// Task actors for a Workspace on a specific ActorTemplate.
//
// Records are keyed by (Atespace, WorkspaceName, AteActorTemplate). A Workspace
// maintains one snapshot per ActorTemplate used by its tasks because Substrate
// only allows an actor to boot from a Tag created on the same template.
type WorkspaceSnapshot struct {
	// Atespace defaults to "default" when empty.
	Atespace         string
	WorkspaceName    string
	AteActorTemplate string

	// ActiveTag is the name of the Substrate Tag passed as source_tag when
	// creating a warm Task actor. It is never empty in a stored snapshot.
	ActiveTag string

	// Revision orders the snapshots published for this key: each stored
	// snapshot has a strictly greater Revision than the one it replaced, though
	// revisions need not be consecutive. Every stored snapshot has Revision >= 1;
	// 0 is reserved by CompareAndSwapWorkspaceSnapshot to mean "no snapshot
	// currently stored".
	//
	// Monotonicity holds while a snapshot remains stored for the key. After
	// DeleteWorkspaceSnapshots removes the entry, revisions for that key may
	// restart from 1.
	//
	// Revision also forms the numeric suffix of the Substrate Tag name
	// (controller.WorkspaceTagName). Because Substrate Tags are immutable,
	// each newly published snapshot uses a distinct Tag name.
	Revision uint64

	// UpdatedAt is the store timestamp when this snapshot was published. The
	// store populates this field on write and ignores any caller-supplied value.
	UpdatedAt time.Time
}

// WorkspaceSnapshotStore caches pre-warmed Workspace snapshot Tags so
// CreateTask can seed warm Task actors without re-running workspace setup.
//
// # Publishing a snapshot
//
// Multiple provisioners may build a snapshot for the same key concurrently (for
// example across ax-server replicas, or when a retry overlaps a slow earlier
// attempt). Snapshot updates therefore use compare-and-swap on Revision so a
// slower writer cannot overwrite a newer snapshot:
//
//  1. Call GetWorkspaceSnapshot and record the current Revision, or 0 if it
//     returns [ErrNotFound].
//  2. Build the new snapshot and create its Substrate Tag using a strictly
//     greater Revision.
//  3. Call CompareAndSwapWorkspaceSnapshot with the recorded expectedRevision.
//  4. If it returns [ErrSnapshotConflict], another writer updated or deleted
//     the entry first; leave the stored snapshot untouched and either stop or
//     restart from step 1.
//
// When cleaning up after a conflict, a provisioner must delete a Tag only if it
// created that Tag itself and the Tag differs from the stored snapshot's
// ActiveTag. Because Tag names are derived deterministically from the key and
// Revision, two provisioners racing at the same Revision target the same Tag
// name, so the losing writer's Tag name may match the Tag published by the
// winner.
//
// # Workspace recreation limitation
//
// Revision-only compare-and-swap coordinates writers working on the same
// Workspace, but the key currently identifies a Workspace only by name rather
// than by a unique UID. If a Workspace is deleted and recreated under the same
// name while an old provisioner is still building a snapshot:
//
//   - an old provisioner that observed expectedRevision == 0 will succeed if it
//     calls CompareAndSwapWorkspaceSnapshot before the new Workspace publishes
//     its first snapshot;
//   - an old provisioner that observed expectedRevision == N will fail while the
//     key is empty, but could succeed once the recreated Workspace reaches
//     revision N (since revisions restart after DeleteWorkspaceSnapshots).
//
// In either case, tasks in the new Workspace would boot from a snapshot built
// for the deleted Workspace. Until Workspace UIDs exist, provisioners and
// Workspace deletion callers must follow two rules to prevent this in practice:
//
//   - Immediately before calling CompareAndSwapWorkspaceSnapshot, re-read the
//     Workspace and publish only if metadata.create_time still matches the
//     Workspace instance the build started from. Recreating a Workspace assigns
//     a fresh create_time, narrowing the race window to the brief interval
//     between that read and the swap.
//   - Whenever DeleteWorkspaceSnapshots is called, also delete the Substrate
//     Tags referenced by the removed snapshot records. Because revisions
//     restart after deletion, the recreated Workspace's first Tag can share a
//     name with an older Tag, and a provisioner must never adopt a pre-existing
//     Tag instead of creating a new one.
//
// TODO: Include a Workspace UID in WorkspaceSnapshot and workspace_snapshots
// once Workspace metadata carries a UID, allowing the store itself to reject
// stale writes across Workspace deletion and recreation without changing the
// method signatures on this interface.
type WorkspaceSnapshotStore interface {
	// GetWorkspaceSnapshot returns the snapshot stored for the key, or
	// [ErrNotFound] if none exists.
	GetWorkspaceSnapshot(ctx context.Context, atespace, workspaceName, templateName string) (*WorkspaceSnapshot, error)

	// CompareAndSwapWorkspaceSnapshot stores snap for its key only if the
	// snapshot currently stored for that key is at expectedRevision.
	//
	// Pass expectedRevision == 0 when no snapshot was stored ([ErrNotFound]);
	// the call inserts snap and fails with [ErrSnapshotConflict] if a snapshot
	// already exists. Pass expectedRevision > 0 to replace that exact revision;
	// the call fails with [ErrSnapshotConflict] if no snapshot exists or the
	// stored revision differs.
	//
	// The revision check and write are atomic: when concurrent callers race
	// with the same expectedRevision matching the stored revision, one succeeds
	// and the others fail with [ErrSnapshotConflict] without modifying storage.
	//
	// On success, it returns the stored snapshot with UpdatedAt populated by
	// the store clock.
	//
	// Arguments must pass [ValidateSnapshotSwap]; invalid arguments leave
	// storage unchanged and return a validation error distinct from
	// [ErrSnapshotConflict].
	CompareAndSwapWorkspaceSnapshot(ctx context.Context, expectedRevision uint64, snap WorkspaceSnapshot) (*WorkspaceSnapshot, error)

	// ListWorkspaceSnapshots returns all snapshots stored for a Workspace (at
	// most one per ActorTemplate), ordered by template name.
	ListWorkspaceSnapshots(ctx context.Context, atespace, workspaceName string) ([]WorkspaceSnapshot, error)

	// DeleteWorkspaceSnapshots removes all snapshots stored for a Workspace. If
	// none exist, it succeeds without error.
	//
	// Concurrent writers that read a deleted revision will fail with
	// [ErrSnapshotConflict] while the key remains empty; see "Workspace
	// recreation limitation" above for behavior if the Workspace is recreated
	// under the same name.
	DeleteWorkspaceSnapshots(ctx context.Context, atespace, workspaceName string) error
}

// ValidateSnapshotSwap checks the invariants required of arguments to
// [WorkspaceSnapshotStore.CompareAndSwapWorkspaceSnapshot] before any backend
// touches storage:
//
//   - snap.WorkspaceName and snap.AteActorTemplate must be non-empty
//     (an empty snap.Atespace defaults to "default");
//   - snap.ActiveTag must be non-empty, because an actor cannot be seeded
//     without a Substrate Tag name;
//   - snap.Revision must be strictly greater than expectedRevision, ensuring
//     monotonic revisions and guaranteeing that stored revisions are >= 1;
//   - snap.Revision must not exceed math.MaxInt64 so it fits in PostgreSQL's
//     signed bigint column.
func ValidateSnapshotSwap(expectedRevision uint64, snap WorkspaceSnapshot) error {
	if snap.WorkspaceName == "" || snap.AteActorTemplate == "" {
		return errors.New("workspace snapshot: workspace name and actor template are required")
	}
	if snap.ActiveTag == "" {
		return errors.New("workspace snapshot: active tag is required")
	}
	if snap.Revision <= expectedRevision {
		return fmt.Errorf("workspace snapshot: revision %d must be greater than the expected revision %d",
			snap.Revision, expectedRevision)
	}
	if snap.Revision > math.MaxInt64 {
		return fmt.Errorf("workspace snapshot: revision %d is too large to store", snap.Revision)
	}
	return nil
}
