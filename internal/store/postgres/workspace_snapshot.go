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

package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/ax/internal/store"
	"github.com/jackc/pgx/v5"
)

// GetWorkspaceSnapshot returns the cached snapshot record for
// (atespace, workspaceName, templateName), or [store.ErrNotFound] if no
// snapshot is stored for that key.
func (s *Store) GetWorkspaceSnapshot(ctx context.Context, atespace, workspaceName, templateName string) (*store.WorkspaceSnapshot, error) {
	atespace = defaultAtespace(atespace)
	var (
		snap store.WorkspaceSnapshot
		rev  int64
	)
	err := s.pool.QueryRow(ctx, `
		SELECT atespace, workspace_name, ate_actor_template, active_tag, revision, updated_at
		FROM workspace_snapshots
		WHERE atespace = $1 AND workspace_name = $2 AND ate_actor_template = $3`,
		atespace, workspaceName, templateName,
	).Scan(&snap.Atespace, &snap.WorkspaceName, &snap.AteActorTemplate, &snap.ActiveTag, &rev, &snap.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, store.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("getting workspace snapshot: %w", err)
	}
	snap.Revision = uint64(max(rev, 0))
	return &snap, nil
}

// CompareAndSwapWorkspaceSnapshot stores snap only if the current snapshot for
// (snap.Atespace, snap.WorkspaceName, snap.AteActorTemplate) is at
// expectedRevision. See [store.WorkspaceSnapshotStore.CompareAndSwapWorkspaceSnapshot]
// for the full contract.
//
// Each case runs as a single SQL statement so that no concurrent writer can
// interleave between the revision check and the write:
//
//   - When expectedRevision == 0 (no existing snapshot expected), it executes
//     INSERT ... ON CONFLICT DO NOTHING, inserting a row only if none exists
//     for the key.
//   - When expectedRevision > 0, it executes UPDATE ... WHERE revision =
//     expectedRevision, updating the row only if it exists at that revision.
//
// If neither statement inserts or updates a row, CompareAndSwapWorkspaceSnapshot
// returns [store.ErrSnapshotConflict].
func (s *Store) CompareAndSwapWorkspaceSnapshot(ctx context.Context, expectedRevision uint64, snap store.WorkspaceSnapshot) (*store.WorkspaceSnapshot, error) {
	snap.Atespace = defaultAtespace(snap.Atespace)
	if err := store.ValidateSnapshotSwap(expectedRevision, snap); err != nil {
		return nil, err
	}

	var row pgx.Row
	if expectedRevision == 0 {
		row = s.pool.QueryRow(ctx, `
			INSERT INTO workspace_snapshots (atespace, workspace_name, ate_actor_template, active_tag, revision, updated_at)
			VALUES ($1, $2, $3, $4, $5, now())
			ON CONFLICT (atespace, workspace_name, ate_actor_template) DO NOTHING
			RETURNING updated_at`,
			snap.Atespace, snap.WorkspaceName, snap.AteActorTemplate, snap.ActiveTag, int64(snap.Revision))
	} else {
		row = s.pool.QueryRow(ctx, `
			UPDATE workspace_snapshots
			SET active_tag = $4, revision = $5, updated_at = now()
			WHERE atespace = $1 AND workspace_name = $2 AND ate_actor_template = $3 AND revision = $6
			RETURNING updated_at`,
			snap.Atespace, snap.WorkspaceName, snap.AteActorTemplate, snap.ActiveTag, int64(snap.Revision), int64(expectedRevision))
	}
	err := row.Scan(&snap.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("%w: %s/%s on %s is not at revision %d",
			store.ErrSnapshotConflict, snap.Atespace, snap.WorkspaceName, snap.AteActorTemplate, expectedRevision)
	}
	if err != nil {
		return nil, fmt.Errorf("saving workspace snapshot: %w", err)
	}
	return &snap, nil
}

// ListWorkspaceSnapshots returns all cached snapshot records for a Workspace.
func (s *Store) ListWorkspaceSnapshots(ctx context.Context, atespace, workspaceName string) ([]store.WorkspaceSnapshot, error) {
	atespace = defaultAtespace(atespace)
	rows, err := s.pool.Query(ctx, `
		SELECT atespace, workspace_name, ate_actor_template, active_tag, revision, updated_at
		FROM workspace_snapshots
		WHERE atespace = $1 AND workspace_name = $2
		ORDER BY ate_actor_template`,
		atespace, workspaceName,
	)
	if err != nil {
		return nil, fmt.Errorf("listing workspace snapshots: %w", err)
	}
	defer rows.Close()

	var out []store.WorkspaceSnapshot
	for rows.Next() {
		var (
			snap store.WorkspaceSnapshot
			rev  int64
		)
		if err := rows.Scan(&snap.Atespace, &snap.WorkspaceName, &snap.AteActorTemplate, &snap.ActiveTag, &rev, &snap.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scanning workspace snapshot: %w", err)
		}
		snap.Revision = uint64(max(rev, 0))
		out = append(out, snap)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("listing workspace snapshots: %w", err)
	}
	return out, nil
}

// DeleteWorkspaceSnapshots deletes all cached snapshot rows for a Workspace.
func (s *Store) DeleteWorkspaceSnapshots(ctx context.Context, atespace, workspaceName string) error {
	atespace = defaultAtespace(atespace)
	if _, err := s.pool.Exec(ctx,
		`DELETE FROM workspace_snapshots WHERE atespace = $1 AND workspace_name = $2`,
		atespace, workspaceName); err != nil {
		return fmt.Errorf("deleting workspace snapshots: %w", err)
	}
	return nil
}
