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

package memory

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/google/ax/internal/store"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const defaultPageLimit = 50

// clone returns a deep copy of a protobuf message so stored resources are never
// shared with callers. Copying the struct by value is unsafe: generated messages
// embed internal state, including a mutex.
func clone[T proto.Message](m T) T {
	return proto.Clone(m).(T)
}

// MemoryStore is an in-memory implementation of store.Store, store.TaskApplier,
// and store.WorkspaceSnapshotStore for testing and single-node setups.
type MemoryStore struct {
	mu                 sync.RWMutex
	tasks              map[string]*store.TaskRow
	models             map[string]*v1alpha1.Model
	workspaces         map[string]*v1alpha1.Workspace
	workspaceSnapshots map[string]store.WorkspaceSnapshot
	gateways           map[string]*v1alpha1.Gateway
	clock              func() time.Time
	dueCh              chan struct{}
}

var (
	_ store.Store                  = (*MemoryStore)(nil)
	_ store.TaskApplier            = (*MemoryStore)(nil)
	_ store.WorkspaceSnapshotStore = (*MemoryStore)(nil)
)

// NewStore creates a new in-memory Store.
func NewStore() *MemoryStore {
	return &MemoryStore{
		tasks:              make(map[string]*store.TaskRow),
		models:             make(map[string]*v1alpha1.Model),
		workspaces:         make(map[string]*v1alpha1.Workspace),
		workspaceSnapshots: make(map[string]store.WorkspaceSnapshot),
		gateways:           make(map[string]*v1alpha1.Gateway),
		clock:              time.Now,
		dueCh:              make(chan struct{}, 1),
	}
}

// SetClock overrides the wall clock read by Apply and PutWorkspaceSnapshot in
// unit tests.
func (s *MemoryStore) SetClock(clock func() time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if clock == nil {
		s.clock = time.Now
		return
	}
	s.clock = clock
}

// DueNotify returns a 1-buffered channel signaled when Apply commits a due task
// row without an active worker lease.
func (s *MemoryStore) DueNotify() <-chan struct{} {
	return s.dueCh
}

func (s *MemoryStore) notifyDue() {
	select {
	case s.dueCh <- struct{}{}:
	default:
	}
}

// sortByKey orders resources by atespace and name, so that lists, and pages
// of them, come back in a stable order.
func sortByKey[T interface{ GetMetadata() *v1alpha1.ObjectMeta }](items []T) {
	sort.Slice(items, func(i, j int) bool {
		return metaKey(items[i].GetMetadata()) < metaKey(items[j].GetMetadata())
	})
}

func paginate[T any](items []T, limit, offset int64) []T {
	if offset < 0 {
		offset = 0
	}
	if offset >= int64(len(items)) {
		return []T{}
	}
	if limit <= 0 {
		limit = defaultPageLimit
	}
	remaining := int64(len(items)) - offset
	if limit > remaining {
		limit = remaining
	}
	return items[offset : offset+limit]
}

func metaKey(m *v1alpha1.ObjectMeta) string {
	return taskKey(m.GetAtespace(), m.GetName())
}

func taskKey(atespace, name string) string {
	if atespace == "" {
		atespace = "default"
	}
	return fmt.Sprintf("%s:%s", atespace, name)
}

func wsSnapKey(atespace, workspaceName, templateName string) string {
	if atespace == "" {
		atespace = "default"
	}
	return fmt.Sprintf("%s:%s:%s", atespace, workspaceName, templateName)
}

func normalizeTask(task *v1alpha1.Task) error {
	if task.Metadata == nil {
		task.Metadata = &v1alpha1.ObjectMeta{}
	}
	if task.Metadata.Name == "" {
		return errors.New("task name is required")
	}
	if task.Metadata.Atespace == "" {
		task.Metadata.Atespace = "default"
	}
	if task.ApiVersion == "" {
		task.ApiVersion = v1alpha1.APIVersion
	}
	if task.Kind == "" {
		task.Kind = v1alpha1.KindTask
	}
	if task.Status == nil {
		task.Status = &v1alpha1.TaskStatus{}
	}
	return nil
}

func (s *MemoryStore) SaveTask(ctx context.Context, task *v1alpha1.Task) error {
	cp := clone(task)
	if err := normalizeTask(cp); err != nil {
		return err
	}
	store.ScrubStoredTask(cp)
	if cp.Status.State == v1alpha1.TaskState_TASK_STATE_UNSPECIFIED && cp.Metadata.GetDeleteTime() == nil {
		cp.Status.State = v1alpha1.TaskState_TASK_STATE_QUEUED
	}
	gen := max(cp.Status.Generation, int64(1))
	cp.Status.Generation = gen

	key := taskKey(cp.Metadata.Atespace, cp.Metadata.Name)

	s.mu.Lock()
	if existing, ok := s.tasks[key]; ok {
		next := existing.Clone()
		next.Task = cp
		next.Generation = gen
		next.SessionID = cp.GetSpec().GetSessionId()
		s.tasks[key] = next
	} else {
		s.tasks[key] = &store.TaskRow{
			Task:       cp,
			Generation: gen,
			SessionID:  cp.GetSpec().GetSessionId(),
		}
	}
	s.mu.Unlock()

	return nil
}

func (s *MemoryStore) GetTask(ctx context.Context, atespace, name string) (*v1alpha1.Task, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	key := taskKey(atespace, name)
	row, ok := s.tasks[key]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := clone(row.Task)
	if row.Generation > 0 {
		if cp.Status == nil {
			cp.Status = &v1alpha1.TaskStatus{}
		}
		cp.Status.Generation = row.Generation
	}
	return cp, nil
}

func (s *MemoryStore) ListTasks(ctx context.Context, atespace string, limit, offset int64) ([]*v1alpha1.Task, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []*v1alpha1.Task
	for _, row := range s.tasks {
		t := row.Task
		if atespace == "" || atespace == "*" || t.Metadata.Atespace == atespace {
			cp := clone(t)
			if row.Generation > 0 {
				if cp.Status == nil {
					cp.Status = &v1alpha1.TaskStatus{}
				}
				cp.Status.Generation = row.Generation
			}
			result = append(result, cp)
		}
	}
	sortByKey(result)
	return paginate(result, limit, offset), nil
}

func (s *MemoryStore) UpdateTaskStatus(ctx context.Context, atespace, name string, status *v1alpha1.TaskStatus) error {
	key := taskKey(atespace, name)

	s.mu.Lock()
	defer s.mu.Unlock()

	row, ok := s.tasks[key]
	if !ok {
		return store.ErrNotFound
	}
	st := &v1alpha1.TaskStatus{}
	if status != nil {
		st = clone(status)
	}
	gen := max(st.Generation, row.Generation, int64(1))
	st.Generation = gen
	row.Task.Status = st
	row.Generation = gen
	return nil
}

func (s *MemoryStore) SetTaskDeleteTime(ctx context.Context, atespace, name string, deleteTime *timestamppb.Timestamp) error {
	key := taskKey(atespace, name)

	s.mu.Lock()
	defer s.mu.Unlock()

	row, ok := s.tasks[key]
	if !ok {
		return store.ErrNotFound
	}
	if row.Task.Metadata == nil {
		row.Task.Metadata = &v1alpha1.ObjectMeta{}
	}
	if deleteTime != nil {
		row.Task.Metadata.DeleteTime = clone(deleteTime)
	} else {
		row.Task.Metadata.DeleteTime = nil
	}
	return nil
}

func (s *MemoryStore) DeleteTask(ctx context.Context, atespace, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.tasks, taskKey(atespace, name))
	return nil
}

// GetTaskRow returns a deep copy of the full TaskRow for key.
func (s *MemoryStore) GetTaskRow(ctx context.Context, key store.TaskKey) (*store.TaskRow, error) {
	key = key.Normalized()
	s.mu.RLock()
	defer s.mu.RUnlock()

	row, ok := s.tasks[taskKey(key.Atespace, key.Name)]
	if !ok {
		return nil, store.ErrNotFound
	}
	return row.Clone(), nil
}

// Apply atomically mutates the TaskRow for key under MemoryStore's write lock,
// enforcing the exact same generation, deletion, lease-clamping, and DueNotify
// semantics as postgres.Store.Apply.
func (s *MemoryStore) Apply(ctx context.Context, key store.TaskKey, fn store.ApplyFunc) (*store.TaskRow, error) {
	key = key.Normalized()
	if key.Name == "" {
		return nil, errors.New("task name is required")
	}
	mapKey := taskKey(key.Atespace, key.Name)

	s.mu.Lock()
	now := s.clock()
	var current *store.TaskRow
	if existing, ok := s.tasks[mapKey]; ok {
		current = existing.Clone()
	}

	next, err := fn(current, now)
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}

	if current == nil && next == nil {
		s.mu.Unlock()
		return nil, nil
	}
	if current != nil && next == nil {
		delete(s.tasks, mapKey)
		s.mu.Unlock()
		return nil, nil
	}
	if current != nil && next == current {
		s.mu.Unlock()
		return current.Clone(), nil
	}

	gen := int64(1)
	if current != nil {
		gen = max(current.Generation, int64(1)) + 1
	}

	stored := next.Clone()
	if stored.Task == nil {
		s.mu.Unlock()
		return nil, errors.New("task row requires non-nil Task")
	}
	if stored.Task.Metadata == nil {
		stored.Task.Metadata = &v1alpha1.ObjectMeta{}
	}
	stored.Task.Metadata.Atespace = key.Atespace
	stored.Task.Metadata.Name = key.Name
	if err := normalizeTask(stored.Task); err != nil {
		s.mu.Unlock()
		return nil, err
	}
	store.ScrubStoredTask(stored.Task)
	store.ScrubClaimResponse(stored.LastClaimResponse)
	stored.Generation = gen
	stored.Task.Status.Generation = gen
	stored.SessionID = stored.Task.GetSpec().GetSessionId()

	if stored.LeaseOwner != "" && stored.LeaseExpiresAt != nil && stored.LeaseExpiresAt.After(now) {
		if stored.NextReconcileAt == nil || stored.NextReconcileAt.Before(*stored.LeaseExpiresAt) {
			exp := *stored.LeaseExpiresAt
			stored.NextReconcileAt = &exp
		}
	} else {
		stored.LeaseOwner = ""
		stored.LeaseExpiresAt = nil
	}

	due := stored.LeaseOwner == "" && stored.NextReconcileAt != nil && !stored.NextReconcileAt.After(now)
	s.tasks[mapKey] = stored
	out := stored.Clone()
	s.mu.Unlock()

	if due {
		s.notifyDue()
	}
	return out, nil
}

func (s *MemoryStore) SaveModel(ctx context.Context, model *v1alpha1.Model) error {
	if model.Metadata == nil {
		model.Metadata = &v1alpha1.ObjectMeta{}
	}
	if model.Metadata.Name == "" {
		return errors.New("model name is required")
	}
	if model.Metadata.Atespace == "" {
		model.Metadata.Atespace = "default"
	}
	if model.ApiVersion == "" {
		model.ApiVersion = v1alpha1.APIVersion
	}
	if model.Kind == "" {
		model.Kind = v1alpha1.KindModel
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := clone(model)
	s.models[metaKey(model.Metadata)] = cp
	return nil
}

func (s *MemoryStore) GetModel(ctx context.Context, atespace, name string) (*v1alpha1.Model, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	m, ok := s.models[taskKey(atespace, name)]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := clone(m)
	return cp, nil
}

func (s *MemoryStore) ListModels(ctx context.Context, atespace string) ([]*v1alpha1.Model, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []*v1alpha1.Model
	for _, m := range s.models {
		if atespace == "" || atespace == "*" || m.Metadata.Atespace == atespace {
			cp := clone(m)
			result = append(result, cp)
		}
	}
	sortByKey(result)
	return result, nil
}

func (s *MemoryStore) DeleteModel(ctx context.Context, atespace, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.models, taskKey(atespace, name))
	return nil
}

func (s *MemoryStore) SaveWorkspace(ctx context.Context, ws *v1alpha1.Workspace) error {
	if ws.Metadata == nil {
		ws.Metadata = &v1alpha1.ObjectMeta{}
	}
	if ws.Metadata.Name == "" {
		return errors.New("workspace name is required")
	}
	if ws.Metadata.Atespace == "" {
		ws.Metadata.Atespace = "default"
	}
	if ws.ApiVersion == "" {
		ws.ApiVersion = v1alpha1.APIVersion
	}
	if ws.Kind == "" {
		ws.Kind = v1alpha1.KindWorkspace
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := clone(ws)
	s.workspaces[metaKey(ws.Metadata)] = cp
	return nil
}

func (s *MemoryStore) GetWorkspace(ctx context.Context, atespace, name string) (*v1alpha1.Workspace, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	ws, ok := s.workspaces[taskKey(atespace, name)]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := clone(ws)
	return cp, nil
}

func (s *MemoryStore) ListWorkspaces(ctx context.Context, atespace string, limit, offset int64) ([]*v1alpha1.Workspace, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []*v1alpha1.Workspace
	for _, w := range s.workspaces {
		if atespace == "" || atespace == "*" || w.Metadata.Atespace == atespace {
			cp := clone(w)
			result = append(result, cp)
		}
	}
	sortByKey(result)
	return paginate(result, limit, offset), nil
}

func (s *MemoryStore) UpdateWorkspaceStatus(ctx context.Context, atespace, name string, status *v1alpha1.WorkspaceStatus) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	ws, ok := s.workspaces[taskKey(atespace, name)]
	if !ok {
		return store.ErrNotFound
	}
	if status != nil {
		ws.Status = clone(status)
	} else {
		ws.Status = nil
	}
	return nil
}

func (s *MemoryStore) DeleteWorkspace(ctx context.Context, atespace, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.workspaces, taskKey(atespace, name))
	return nil
}

func (s *MemoryStore) GetWorkspaceSnapshot(ctx context.Context, atespace, workspaceName, templateName string) (*store.WorkspaceSnapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	snap, ok := s.workspaceSnapshots[wsSnapKey(atespace, workspaceName, templateName)]
	if !ok {
		return nil, store.ErrNotFound
	}
	out := snap
	return &out, nil
}

func (s *MemoryStore) PutWorkspaceSnapshot(ctx context.Context, snap store.WorkspaceSnapshot) (*store.WorkspaceSnapshot, error) {
	if snap.Atespace == "" {
		snap.Atespace = "default"
	}
	if snap.WorkspaceName == "" || snap.AteActorTemplate == "" {
		return nil, errors.New("workspace_name and ate_actor_template are required")
	}
	s.mu.Lock()
	snap.UpdatedAt = s.clock()
	s.workspaceSnapshots[wsSnapKey(snap.Atespace, snap.WorkspaceName, snap.AteActorTemplate)] = snap
	out := snap
	s.mu.Unlock()
	return &out, nil
}

func (s *MemoryStore) ListWorkspaceSnapshots(ctx context.Context, atespace, workspaceName string) ([]store.WorkspaceSnapshot, error) {
	if atespace == "" {
		atespace = "default"
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	var out []store.WorkspaceSnapshot
	for _, snap := range s.workspaceSnapshots {
		if snap.Atespace == atespace && snap.WorkspaceName == workspaceName {
			out = append(out, snap)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].AteActorTemplate < out[j].AteActorTemplate
	})
	return out, nil
}

func (s *MemoryStore) DeleteWorkspaceSnapshots(ctx context.Context, atespace, workspaceName string) error {
	if atespace == "" {
		atespace = "default"
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	for k, snap := range s.workspaceSnapshots {
		if snap.Atespace == atespace && snap.WorkspaceName == workspaceName {
			delete(s.workspaceSnapshots, k)
		}
	}
	return nil
}

func (s *MemoryStore) SaveGateway(ctx context.Context, gw *v1alpha1.Gateway) error {
	if gw.Metadata == nil {
		gw.Metadata = &v1alpha1.ObjectMeta{}
	}
	if gw.Metadata.Name == "" {
		return errors.New("gateway name is required")
	}
	if gw.Metadata.Atespace == "" {
		gw.Metadata.Atespace = "default"
	}
	if gw.ApiVersion == "" {
		gw.ApiVersion = v1alpha1.APIVersion
	}
	if gw.Kind == "" {
		gw.Kind = v1alpha1.KindGateway
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := clone(gw)
	s.gateways[metaKey(gw.Metadata)] = cp
	return nil
}

func (s *MemoryStore) GetGateway(ctx context.Context, atespace, name string) (*v1alpha1.Gateway, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	gw, ok := s.gateways[taskKey(atespace, name)]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := clone(gw)
	return cp, nil
}

func (s *MemoryStore) ListGateways(ctx context.Context, atespace string) ([]*v1alpha1.Gateway, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []*v1alpha1.Gateway
	for _, gw := range s.gateways {
		if atespace == "" || atespace == "*" || gw.Metadata.Atespace == atespace {
			cp := clone(gw)
			result = append(result, cp)
		}
	}
	sortByKey(result)
	return result, nil
}

func (s *MemoryStore) DeleteGateway(ctx context.Context, atespace, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.gateways, taskKey(atespace, name))
	return nil
}

func (s *MemoryStore) Close() error {
	return nil
}
