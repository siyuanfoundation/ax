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
	"bytes"
	"context"
	"errors"
	"sort"
	"time"

	"github.com/google/ax/internal/lifecycle"
	"github.com/google/ax/internal/store"
)

const (
	defaultReplicaTTL    = 45 * time.Second
	defaultShardLeaseTTL = 45 * time.Second
)

// ListTaskAtespaces returns all distinct atespaces with at least one task in sorted order.
func (s *MemoryStore) ListTaskAtespaces(ctx context.Context) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	seen := make(map[string]struct{})
	for _, row := range s.tasks {
		if row == nil || row.Task == nil {
			continue
		}
		ns := (store.TaskKey{Atespace: row.Task.GetMetadata().GetAtespace()}).Normalized().Atespace
		seen[ns] = struct{}{}
	}
	out := make([]string, 0, len(seen))
	for ns := range seen {
		out = append(out, ns)
	}
	sort.Strings(out)
	return out, nil
}

// SnapshotActiveTasks returns a sorted point-in-time slice of all non-terminal
// task sweep records in atespaces (or across all atespaces when empty).
func (s *MemoryStore) SnapshotActiveTasks(ctx context.Context, atespaces []string) ([]store.TaskSweepRecord, time.Time, error) {
	if err := ctx.Err(); err != nil {
		return nil, time.Time{}, err
	}
	var allowed map[string]struct{}
	if len(atespaces) > 0 {
		allowed = make(map[string]struct{}, len(atespaces))
		for _, ns := range atespaces {
			allowed[(store.TaskKey{Atespace: ns}).Normalized().Atespace] = struct{}{}
		}
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	now := s.now()
	var records []store.TaskSweepRecord
	for mapKey, row := range s.tasks {
		if row == nil || row.Task == nil {
			continue
		}
		st := row.Task.GetStatus().GetState()
		if lifecycle.IsTerminal(st) {
			continue
		}
		k := store.TaskKey{
			Atespace: row.Task.GetMetadata().GetAtespace(),
			Name:     row.Task.GetMetadata().GetName(),
		}.Normalized()
		if allowed != nil {
			if _, ok := allowed[k.Atespace]; !ok {
				continue
			}
		}
		var resumedAt, hbAt *time.Time
		if row.LastResumedAt != nil {
			t := *row.LastResumedAt
			resumedAt = &t
		}
		if row.LastHeartbeatAt != nil {
			t := *row.LastHeartbeatAt
			hbAt = &t
		}
		var createdAt time.Time
		if createTs := row.Task.GetMetadata().GetCreateTime(); createTs != nil {
			createdAt = createTs.AsTime()
		} else {
			createdAt = s.taskSavedAt[mapKey]
		}
		records = append(records, store.TaskSweepRecord{
			Key:               k,
			State:             st,
			Generation:        row.Generation,
			ActorUID:          row.ActorUID,
			HasPendingActions: len(row.Actions) > 0,
			HasRunnerToken:    len(row.RunnerTokenHash) > 0,
			HasClaimDeadline:  row.ClaimDeadline != nil,
			CreatedAt:         createdAt,
			LastResumedAt:     resumedAt,
			LastHeartbeatAt:   hbAt,
		})
	}

	sort.Slice(records, func(i, j int) bool {
		if records[i].Key.Atespace != records[j].Key.Atespace {
			return records[i].Key.Atespace < records[j].Key.Atespace
		}
		return records[i].Key.Name < records[j].Key.Name
	})
	return records, now, nil
}

// GetTaskRows batch-loads TaskRow copies for keys, scrubbing expired leases.
func (s *MemoryStore) GetTaskRows(ctx context.Context, keys []store.TaskKey) (map[store.TaskKey]*store.TaskRow, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	out := make(map[store.TaskKey]*store.TaskRow, len(keys))
	if len(keys) == 0 {
		return out, nil
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	now := s.now()
	for _, k := range keys {
		if k.Name == "" {
			continue
		}
		nk := k.Normalized()
		row, ok := s.tasks[taskKey(nk.Atespace, nk.Name)]
		if !ok || row == nil {
			continue
		}
		cp := row.Clone()
		overlayTaskGeneration(cp.Task, cp.Generation)
		if cp.LeaseExpiresAt != nil && !cp.LeaseExpiresAt.After(now) {
			cp.LeaseOwner = ""
			cp.LeaseExpiresAt = nil
		}
		out[nk] = cp
	}
	return out, nil
}

// FlushHeartbeats applies matching runner heartbeats without bumping Generation
// and updates replicaID's watermark in replicas.
func (s *MemoryStore) FlushHeartbeats(ctx context.Context, replicaID string, replicaTTL time.Duration, entries []store.HeartbeatEntry) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if replicaID == "" {
		return errors.New("replicaID is required for FlushHeartbeats")
	}
	if replicaTTL <= 0 {
		replicaTTL = defaultReplicaTTL
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()
	for _, e := range entries {
		if e.Key.Name == "" || len(e.RunnerTokenHash) == 0 {
			continue
		}
		nk := e.Key.Normalized()
		row := s.tasks[taskKey(nk.Atespace, nk.Name)]
		if row == nil || len(row.RunnerTokenHash) == 0 || !bytes.Equal(row.RunnerTokenHash, e.RunnerTokenHash) {
			continue
		}
		age := max(e.Age, time.Duration(0))
		ts := now.Add(-age)
		if row.LastHeartbeatAt == nil || ts.After(*row.LastHeartbeatAt) {
			row.LastHeartbeatAt = &ts
		}
	}

	for id, rec := range s.replicas {
		if id != replicaID && !rec.expiresAt.After(now.Add(-5*time.Minute)) {
			delete(s.replicas, id)
		}
	}
	startedAt := now
	if prev, ok := s.replicas[replicaID]; ok {
		startedAt = prev.startedAt
	}
	s.replicas[replicaID] = replicaEntry{
		startedAt:   startedAt,
		lastFlushAt: now,
		expiresAt:   now.Add(replicaTTL),
	}
	return nil
}

// UnregisterReplica removes replicaID from the live replica registry.
func (s *MemoryStore) UnregisterReplica(ctx context.Context, replicaID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if replicaID == "" {
		return nil
	}
	s.mu.Lock()
	delete(s.replicas, replicaID)
	s.mu.Unlock()
	return nil
}

// InspectReplicas computes live replica count and watermark age against s.now().
func (s *MemoryStore) InspectReplicas(ctx context.Context) (store.ReplicaStats, error) {
	if err := ctx.Err(); err != nil {
		return store.ReplicaStats{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	now := s.now()
	var (
		liveCount int
		oldest    time.Time
	)
	for _, rec := range s.replicas {
		if rec.expiresAt.After(now) {
			if liveCount == 0 || rec.lastFlushAt.Before(oldest) {
				oldest = rec.lastFlushAt
			}
			liveCount++
		}
	}
	var watermarkAge time.Duration
	if liveCount > 0 {
		watermarkAge = max(now.Sub(oldest), 0)
	}
	return store.ReplicaStats{
		LiveReplicas:    liveCount,
		HasLiveReplicas: liveCount > 0,
		WatermarkAge:    watermarkAge,
		StoreNow:        now,
	}, nil
}

// TryAcquireShardLease attempts a non-blocking shard lease acquisition.
func (s *MemoryStore) TryAcquireShardLease(ctx context.Context, shardKey, holderToken string, ttl time.Duration) (bool, time.Time, error) {
	if err := ctx.Err(); err != nil {
		return false, time.Time{}, err
	}
	if shardKey == "" || holderToken == "" {
		return false, time.Time{}, errors.New("shardKey and holderToken are required")
	}
	if ttl <= 0 {
		ttl = defaultShardLeaseTTL
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()
	cur, exists := s.shardLeases[shardKey]
	if exists && cur.expiresAt.After(now) && cur.token != holderToken {
		return false, now, nil
	}
	s.shardLeases[shardKey] = shardLeaseEntry{
		token:     holderToken,
		expiresAt: now.Add(ttl),
	}
	return true, now, nil
}

// RenewShardLease extends an active, unexpired shard lease held by holderToken.
func (s *MemoryStore) RenewShardLease(ctx context.Context, shardKey, holderToken string, ttl time.Duration) (bool, time.Time, error) {
	if err := ctx.Err(); err != nil {
		return false, time.Time{}, err
	}
	if shardKey == "" || holderToken == "" {
		return false, time.Time{}, nil
	}
	if ttl <= 0 {
		ttl = defaultShardLeaseTTL
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()
	cur, exists := s.shardLeases[shardKey]
	if !exists || cur.token != holderToken || !cur.expiresAt.After(now) {
		return false, now, nil
	}
	cur.expiresAt = now.Add(ttl)
	s.shardLeases[shardKey] = cur
	return true, now, nil
}

// ReleaseShardLease deletes shardKey if held by holderToken.
func (s *MemoryStore) ReleaseShardLease(ctx context.Context, shardKey, holderToken string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if shardKey == "" || holderToken == "" {
		return nil
	}
	s.mu.Lock()
	if cur, ok := s.shardLeases[shardKey]; ok && cur.token == holderToken {
		delete(s.shardLeases, shardKey)
	}
	s.mu.Unlock()
	return nil
}
