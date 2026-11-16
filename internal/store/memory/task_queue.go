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
	"sort"
	"time"

	"github.com/google/ax/internal/lifecycle"
	"github.com/google/ax/internal/store"
	"github.com/google/uuid"
)

// SetQueueWakeup registers a callback invoked after a task row becomes due.
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

// GetTaskRow returns a deep copy of the stored row for key (with any expired
// worker lease cleared), or store.ErrNotFound.
func (s *MemoryStore) GetTaskRow(ctx context.Context, key store.TaskKey) (*store.TaskRow, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	key = key.Normalized()
	s.mu.RLock()
	defer s.mu.RUnlock()

	now := s.now()
	row, ok := s.tasks[taskKey(key.Atespace, key.Name)]
	if !ok || row == nil {
		return nil, store.ErrNotFound
	}
	cp := row.Clone()
	overlayTaskGeneration(cp.Task, cp.Generation)
	if cp.LeaseExpiresAt != nil && !cp.LeaseExpiresAt.After(now) {
		cp.LeaseOwner = ""
		cp.LeaseExpiresAt = nil
	}
	return cp, nil
}

// Apply executes fn atomically under the store mutex and commits the returned mutation.
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
	stored := s.tasks[mapKey]

	var passedCurrent *store.TaskRow
	if stored != nil {
		passedCurrent = stored.Clone()
		overlayTaskGeneration(passedCurrent.Task, passedCurrent.Generation)
		if passedCurrent.LeaseExpiresAt != nil && !passedCurrent.LeaseExpiresAt.After(now) {
			passedCurrent.LeaseOwner = ""
			passedCurrent.LeaseExpiresAt = nil
		}
	}

	next, err := fn(passedCurrent, now)
	switch {
	case err != nil:
		s.mu.Unlock()
		return nil, err

	case next == store.DeleteTaskRow:
		delete(s.tasks, mapKey)
		delete(s.taskSavedAt, mapKey)
		s.mu.Unlock()
		return nil, nil

	case next == nil || (passedCurrent != nil && next == passedCurrent):
		if stored != nil {
			ret = stored.Clone()
			overlayTaskGeneration(ret.Task, ret.Generation)
			if ret.LeaseExpiresAt != nil && !ret.LeaseExpiresAt.After(now) {
				ret.LeaseOwner = ""
				ret.LeaseExpiresAt = nil
			}
		}
		s.mu.Unlock()
		return ret, nil
	}

	written := next.Clone()
	nextGen := int64(1)
	if stored != nil {
		baseGen := max(stored.Generation, stored.Task.GetStatus().GetGeneration())
		nextGen = max(baseGen+1, 1)
	}
	if err := store.NormalizeTaskForApply(written.Task, key, nextGen); err != nil {
		s.mu.Unlock()
		return nil, err
	}
	written.Generation = nextGen
	if stored != nil && !store.RowExpectsHeartbeats(stored) && store.RowExpectsHeartbeats(written) {
		if written.LastHeartbeatAt == nil || written.LastHeartbeatAt.Before(now) {
			hb := now
			written.LastHeartbeatAt = &hb
		}
	}
	written.NextReconcileAt = lifecycle.NextReconcileAt(written.LifecycleState(), now)

	// Preserve an active worker lease from stored and clamp NextReconcileAt so
	// another worker cannot claim the row before the lease expires; clear any
	// expired or unheld lease.
	if stored != nil && stored.LeaseOwner != "" && stored.LeaseExpiresAt != nil && stored.LeaseExpiresAt.After(now) {
		exp := *stored.LeaseExpiresAt
		written.LeaseOwner = stored.LeaseOwner
		written.LeaseExpiresAt = &exp
		if written.NextReconcileAt != nil && written.NextReconcileAt.Before(exp) {
			clamped := exp
			written.NextReconcileAt = &clamped
		}
	} else {
		written.LeaseOwner = ""
		written.LeaseExpiresAt = nil
	}

	if stored == nil {
		s.taskSavedAt[mapKey] = now
	}
	s.tasks[mapKey] = written
	ret = written.Clone()
	shouldWake = written.NextReconcileAt != nil && !written.NextReconcileAt.After(now)
	s.mu.Unlock()

	if shouldWake {
		s.notifyWakeup()
	}
	return ret, nil
}

// LeaseDue claims up to limit due task rows ordered by NextReconcileAt,
// skipping saturated atespaces and saturated runnable action kinds specified by
// filters.
func (s *MemoryStore) LeaseDue(ctx context.Context, workerID string, leaseTTL time.Duration, limit int, filters ...store.LeaseFilter) ([]store.LeasedTask, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit <= 0 {
		return nil, nil
	}
	if leaseTTL <= 0 {
		leaseTTL = 30 * time.Second
	}
	filter := store.MergeLeaseFilters(filters)
	var excludedAtespaces map[string]struct{}
	if len(filter.ExcludeAtespaces) > 0 {
		excludedAtespaces = make(map[string]struct{}, len(filter.ExcludeAtespaces))
		for _, ns := range filter.ExcludeAtespaces {
			excludedAtespaces[(store.TaskKey{Atespace: ns}).Normalized().Atespace] = struct{}{}
		}
	}
	var excludedKinds map[lifecycle.ActionKind]struct{}
	if len(filter.ExcludeActionKinds) > 0 {
		excludedKinds = make(map[lifecycle.ActionKind]struct{}, len(filter.ExcludeActionKinds))
		for _, k := range filter.ExcludeActionKinds {
			excludedKinds[k] = struct{}{}
		}
	}
	var bucketedKinds map[lifecycle.ActionKind]struct{}
	if filter.ExcludeOtherActionKinds {
		bucketedKinds = make(map[lifecycle.ActionKind]struct{}, len(filter.BucketedActionKinds))
		for _, k := range filter.BucketedActionKinds {
			bucketedKinds[k] = struct{}{}
		}
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
		k := store.TaskKey{
			Atespace: row.Task.GetMetadata().GetAtespace(),
			Name:     row.Task.GetMetadata().GetName(),
		}.Normalized()
		if _, skip := excludedAtespaces[k.Atespace]; skip {
			continue
		}
		if len(row.Actions) > 0 && (row.Actions[0].NotBefore.IsZero() || !row.Actions[0].NotBefore.After(now)) {
			headKind := row.Actions[0].Kind
			if _, skip := excludedKinds[headKind]; skip {
				continue
			}
			if filter.ExcludeOtherActionKinds {
				if _, explicit := bucketedKinds[headKind]; !explicit {
					continue
				}
			}
		}
		due = append(due, candidate{
			key: k,
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

	expiresAt := now.Add(leaseTTL)
	leased := make([]store.LeasedTask, 0, len(due))
	for _, c := range due {
		token := workerID + ":" + uuid.NewString()
		exp := expiresAt
		nextDue := expiresAt
		c.row.LeaseOwner = token
		c.row.LeaseExpiresAt = &exp
		c.row.NextReconcileAt = &nextDue
		cp := c.row.Clone()
		overlayTaskGeneration(cp.Task, cp.Generation)
		leased = append(leased, store.LeasedTask{
			Key:        c.key,
			LeaseToken: token,
			LeasedAt:   now,
			ExpiresAt:  expiresAt,
			Row:        cp,
		})
	}
	return leased, nil
}

// LeaseTask claims a single due task row if it is currently due, or reports
// contended == true when another active worker holds the lease on due work and
// retryIn > 0 when an unleased row has pending actions waiting on a future
// store-clock backoff.
func (s *MemoryStore) LeaseTask(ctx context.Context, key store.TaskKey, workerID string, leaseTTL time.Duration) (*store.LeasedTask, bool, time.Duration, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, 0, err
	}
	if leaseTTL <= 0 {
		leaseTTL = 30 * time.Second
	}
	key = key.Normalized()

	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()
	row := s.tasks[taskKey(key.Atespace, key.Name)]
	if row == nil || row.Task == nil || row.NextReconcileAt == nil {
		return nil, false, 0, nil
	}
	if row.NextReconcileAt.After(now) {
		activeLease := row.LeaseOwner != "" && row.LeaseExpiresAt != nil && row.LeaseExpiresAt.After(now)
		unclamped := lifecycle.NextReconcileAt(row.LifecycleState(), now)
		dueNow := unclamped != nil && !unclamped.After(now)
		var retryIn time.Duration
		if !activeLease && len(row.Actions) > 0 {
			retryIn = row.NextReconcileAt.Sub(now)
		}
		return nil, activeLease && dueNow, retryIn, nil
	}
	expiresAt := now.Add(leaseTTL)
	token := workerID + ":" + uuid.NewString()
	exp := expiresAt
	nextDue := expiresAt
	row.LeaseOwner = token
	row.LeaseExpiresAt = &exp
	row.NextReconcileAt = &nextDue

	cp := row.Clone()
	overlayTaskGeneration(cp.Task, cp.Generation)
	return &store.LeasedTask{
		Key:        key,
		LeaseToken: token,
		LeasedAt:   now,
		ExpiresAt:  expiresAt,
		Row:        cp,
	}, false, 0, nil
}

// RenewLease extends an active lease and returns the current head Action.ID.
func (s *MemoryStore) RenewLease(ctx context.Context, key store.TaskKey, leaseToken string, leaseTTL time.Duration) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if leaseTTL <= 0 {
		leaseTTL = 30 * time.Second
	}
	key = key.Normalized()

	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()
	row := s.tasks[taskKey(key.Atespace, key.Name)]
	if !store.HasActiveLease(row, leaseToken, now) {
		return "", store.ErrLeaseLost
	}
	exp := now.Add(leaseTTL)
	row.LeaseExpiresAt = &exp
	if row.NextReconcileAt != nil && row.NextReconcileAt.Before(exp) {
		clamped := exp
		row.NextReconcileAt = &clamped
	}
	if len(row.Actions) > 0 {
		return row.Actions[0].ID, nil
	}
	return "", nil
}

// ReleaseLease clears the worker lease and recomputes NextReconcileAt without bumping Generation.
func (s *MemoryStore) ReleaseLease(ctx context.Context, key store.TaskKey, leaseToken string, requeueAfter time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	key = key.Normalized()

	var shouldWake bool
	s.mu.Lock()
	now := s.now()
	row := s.tasks[taskKey(key.Atespace, key.Name)]
	if !store.HasActiveLease(row, leaseToken, now) {
		s.mu.Unlock()
		return store.ErrLeaseLost
	}
	row.LeaseOwner = ""
	row.LeaseExpiresAt = nil
	due := lifecycle.NextReconcileAt(row.LifecycleState(), now)
	if due != nil && requeueAfter > 0 {
		delayed := now.Add(requeueAfter)
		if due.Before(delayed) {
			due = &delayed
		}
	}
	row.NextReconcileAt = due
	shouldWake = due != nil && !due.After(now)
	s.mu.Unlock()

	if shouldWake {
		s.notifyWakeup()
	}
	return nil
}

// BackoffAction records a transient retry backoff on actionID (if still at head) and releases the lease.
func (s *MemoryStore) BackoffAction(ctx context.Context, key store.TaskKey, leaseToken, actionID string, backoff time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	key = key.Normalized()

	var shouldWake bool
	s.mu.Lock()
	now := s.now()
	row := s.tasks[taskKey(key.Atespace, key.Name)]
	if !store.HasActiveLease(row, leaseToken, now) {
		s.mu.Unlock()
		return store.ErrLeaseLost
	}
	if len(row.Actions) > 0 && row.Actions[0].ID == actionID {
		row.Actions[0].Tries++
		if backoff > 0 {
			row.Actions[0].NotBefore = now.Add(backoff)
		} else {
			row.Actions[0].NotBefore = now
		}
	}
	row.LeaseOwner = ""
	row.LeaseExpiresAt = nil
	row.NextReconcileAt = lifecycle.NextReconcileAt(row.LifecycleState(), now)
	shouldWake = row.NextReconcileAt != nil && !row.NextReconcileAt.After(now)
	s.mu.Unlock()

	if shouldWake {
		s.notifyWakeup()
	}
	return nil
}
