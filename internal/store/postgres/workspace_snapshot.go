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
// (atespace, workspaceName, templateName), or store.ErrNotFound when uncached.
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

// PutWorkspaceSnapshot upserts the cached snapshot Tag for
// (snap.Atespace, snap.WorkspaceName, snap.AteActorTemplate).
func (s *Store) PutWorkspaceSnapshot(ctx context.Context, snap store.WorkspaceSnapshot) (*store.WorkspaceSnapshot, error) {
	snap.Atespace = defaultAtespace(snap.Atespace)
	if snap.WorkspaceName == "" || snap.AteActorTemplate == "" {
		return nil, errors.New("workspace_name and ate_actor_template are required")
	}
	var rev int64
	err := s.pool.QueryRow(ctx, `
		INSERT INTO workspace_snapshots (atespace, workspace_name, ate_actor_template, active_tag, revision, updated_at)
		VALUES ($1, $2, $3, $4, $5, now())
		ON CONFLICT (atespace, workspace_name, ate_actor_template) DO UPDATE
		SET active_tag = EXCLUDED.active_tag,
		    revision   = EXCLUDED.revision,
		    updated_at = now()
		RETURNING revision, updated_at`,
		snap.Atespace, snap.WorkspaceName, snap.AteActorTemplate, snap.ActiveTag, int64(snap.Revision),
	).Scan(&rev, &snap.UpdatedAt)
	if err != nil {
		return nil, fmt.Errorf("saving workspace snapshot: %w", err)
	}
	snap.Revision = uint64(max(rev, 0))
	out := snap
	return &out, nil
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
