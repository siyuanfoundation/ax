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

// WorkspaceSnapshot caches the active pre-warmed Substrate snapshot Tag built
// for an (Atespace, WorkspaceName, AteActorTemplate) tuple.
type WorkspaceSnapshot struct {
	Atespace         string
	WorkspaceName    string
	AteActorTemplate string
	ActiveTag        string
	Revision         uint64
	UpdatedAt        time.Time
}

// WorkspaceSnapshotStore caches pre-warmed Workspace snapshot Tags so
// CreateTask can seed warm Task actors without re-running workspace setup.
type WorkspaceSnapshotStore interface {
	GetWorkspaceSnapshot(ctx context.Context, atespace, workspaceName, templateName string) (*WorkspaceSnapshot, error)
	PutWorkspaceSnapshot(ctx context.Context, snap WorkspaceSnapshot) (*WorkspaceSnapshot, error)
	ListWorkspaceSnapshots(ctx context.Context, atespace, workspaceName string) ([]WorkspaceSnapshot, error)
	DeleteWorkspaceSnapshots(ctx context.Context, atespace, workspaceName string) error
}
