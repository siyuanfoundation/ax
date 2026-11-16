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
	"fmt"
	"sort"
	"time"

	"github.com/google/ax/internal/lifecycle"
	"github.com/google/ax/internal/store"
	"github.com/google/uuid"
)

// SetQueueWakeup registers a non-blocking wakeup callback fired when Apply
// commits a row whose NextReconcileAt is due immediately (<= now).
func (s *MemoryStore) SetQueueWakeup(fn func()) {
	s.wakeupMu.Lock()
	s.wakeup = fn
	s.wakeupMu.Unlock()
}

func (s *MemoryStore) notifyWakeup() {
	s.wakeupMu.RLock()
	fn := s.wakeup
	s.wakeupMu.RUnlock()
	if fn != nil {
		fn()
	}
}

func scrubExpiredLease(row *store.TaskRow, now time.Time) {
	if row != nil && row.LeaseExpiresAt != nil && !row.LeaseExpiresAt.After(now) {
		row.LeaseOwner = ""
		row.LeaseExpiresAt = nil
	}
}

// GetTaskRow returns a deep copy of the stored TaskRow for key, or
// store.ErrNotFound if no matching row exists.
func (s *MemoryStore) GetTaskRow(ctx context.Context, key store.TaskKey) (*store.TaskRow, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	key = key.Normalized()

	s.mu.RLock()
	defer s.mu.RUnlock()

	row, ok := s.tasks[taskKey(key.Atespace, key.Name)]
	if !ok || row == nil || row.Task == nil {
		return nil, store.ErrNotFound
	}
	cp := row.Clone()
	scrubExpiredLease(cp, s.now())
	return cp, nil
}

// Apply executes fn atomically under s.mu for key and commits the resulting
// insert, update, delete, or no-op.
func (s *MemoryStore) Apply(ctx context.Context, key store.TaskKey, fn store.ApplyFunc) (*store.TaskRow, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	key = key.Normalized()
	mapKey := taskKey(key.Atespace, key.Name)

	var (
		ret        *store.TaskRow
		shouldWake bool
	)

	s.mu.Lock()
	now := s.now()
	var current *store.TaskRow
	if stored, ok := s.tasks[mapKey]; ok && stored != nil && stored.Task != nil {
		current = stored.Clone()
		scrubExpiredLease(current, now)
	}

	next, err := fn(current, now)
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}

	switch {
	case next == store.DeleteTaskRow:
		delete(s.tasks, mapKey)
	case next == nil || (current != nil && next == current):
		if stored, ok := s.tasks[mapKey]; ok && stored != nil && stored.Task != nil {
			ret = stored.Clone()
			scrubExpiredLease(ret, now)
		}
	default:
		written := next.Clone()
		gen := int64(1)
		if current != nil {
			gen = current.Generation + 1
		}
		if err := store.NormalizeTaskForApply(written.Task, key, gen); err != nil {
			s.mu.Unlock()
			return nil, err
		}
		written.Generation = gen
		scrubExpiredLease(written, now)
		if written.LeaseOwner != "" && written.LeaseExpiresAt != nil && written.NextReconcileAt != nil &&
			written.NextReconcileAt.Before(*written.LeaseExpiresAt) {
			exp := *written.LeaseExpiresAt
			written.NextReconcileAt = &exp
		}
		s.tasks[mapKey] = written
		ret = written.Clone()
		shouldWake = written.NextReconcileAt != nil && !written.NextReconcileAt.After(now)
	}
	s.mu.Unlock()

	if shouldWake {
		s.notifyWakeup()
	}
	return ret, nil
}

// LeaseDue atomically claims up to limit rows with NextReconcileAt <= now.
func (s *MemoryStore) LeaseDue(ctx context.Context, workerID string, leaseTTL time.Duration, limit int) ([]store.LeasedTask, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit <= 0 {
		return nil, nil
	}
	if leaseTTL <= 0 {
		leaseTTL = store.DefaultLeaseTTL
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()
	type candidate struct {
		key store.TaskKey
		row *store.TaskRow
		due time.Time
	}
	var due []candidate
	for _, row := range s.tasks {
		if row == nil || row.Task == nil || row.NextReconcileAt == nil || row.NextReconcileAt.After(now) {
			continue
		}
		due = append(due, candidate{
			key: store.TaskKey{
				Atespace: row.Task.GetMetadata().GetAtespace(),
				Name:     row.Task.GetMetadata().GetName(),
			}.Normalized(),
			row: row,
			due: *row.NextReconcileAt,
		})
	}

	sort.Slice(due, func(i, j int) bool {
		if !due[i].due.Equal(due[j].due) {
			return due[i].due.Before(due[j].due)
		}
		if due[i].key.Atespace != due[j].key.Atespace {
			return due[i].key.Atespace < due[j].key.Atespace
		}
		return due[i].key.Name < due[j].key.Name
	})
	if len(due) > limit {
		due = due[:limit]
	}

	exp := now.Add(leaseTTL)
	out := make([]store.LeasedTask, 0, len(due))
	for _, item := range due {
		token := fmt.Sprintf("%s:%s", workerID, uuid.NewString())
		expCopy := exp
		dueCopy := exp
		item.row.LeaseOwner = token
		item.row.LeaseExpiresAt = &expCopy
		item.row.NextReconcileAt = &dueCopy
		out = append(out, store.LeasedTask{
			Key:        item.key,
			LeaseToken: token,
			LeasedAt:   now,
			ExpiresAt:  exp,
			Row:        item.row.Clone(),
		})
	}
	return out, nil
}

// RenewLease extends an active worker lease and returns the current head Action.ID.
func (s *MemoryStore) RenewLease(ctx context.Context, key store.TaskKey, leaseToken string, leaseTTL time.Duration) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if leaseTTL <= 0 {
		leaseTTL = store.DefaultLeaseTTL
	}
	key = key.Normalized()

	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()
	row, ok := s.tasks[taskKey(key.Atespace, key.Name)]
	if !ok || !store.HasActiveLease(row, leaseToken, now) {
		return "", store.ErrLeaseLost
	}
	exp := now.Add(leaseTTL)
	due := exp
	if row.NextReconcileAt != nil && row.NextReconcileAt.After(exp) {
		due = *row.NextReconcileAt
	}
	row.LeaseExpiresAt = &exp
	row.NextReconcileAt = &due
	if len(row.Actions) > 0 {
		return row.Actions[0].ID, nil
	}
	return "", nil
}

// ReleaseLease clears leaseToken if still active and recomputes NextReconcileAt.
func (s *MemoryStore) ReleaseLease(ctx context.Context, key store.TaskKey, leaseToken string, requeueAfter time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	key = key.Normalized()

	var shouldWake bool
	s.mu.Lock()
	now := s.now()
	row, ok := s.tasks[taskKey(key.Atespace, key.Name)]
	if ok && store.HasActiveLease(row, leaseToken, now) {
		row.LeaseOwner = ""
		row.LeaseExpiresAt = nil
		next := lifecycle.NextReconcileAt(row.State, now)
		if requeueAfter > 0 && next != nil {
			floor := now.Add(requeueAfter)
			if next.Before(floor) {
				next = &floor
			}
		}
		row.NextReconcileAt = next
		shouldWake = next != nil && !next.After(now)
	}
	s.mu.Unlock()

	if shouldWake {
		s.notifyWakeup()
	}
	return nil
}

// BackoffAction increments Tries and advances NotBefore on the head action if
// leaseToken still owns the row and actionID still matches Actions[0].ID.
func (s *MemoryStore) BackoffAction(ctx context.Context, key store.TaskKey, leaseToken, actionID string, backoff time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	key = key.Normalized()

	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()
	row, ok := s.tasks[taskKey(key.Atespace, key.Name)]
	if !ok || !store.HasActiveLease(row, leaseToken, now) {
		return nil
	}
	if len(row.Actions) == 0 || row.Actions[0].ID != actionID {
		return nil
	}
	row.Actions[0].Tries++
	row.Actions[0].NotBefore = now.Add(max(backoff, 0))
	row.LeaseOwner = ""
	row.LeaseExpiresAt = nil
	nextGen := row.Generation + 1
	_ = store.NormalizeTaskForApply(row.Task, key, nextGen)
	row.Generation = nextGen
	row.NextReconcileAt = lifecycle.NextReconcileAt(row.State, now)
	return nil
}
