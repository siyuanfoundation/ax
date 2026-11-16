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
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/google/ax/internal/store"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"github.com/jackc/pgx/v5"
)

var sweepActivePageSize = 2000

const (
	getTaskRowsChunkSize   = 1000
	maxHeartbeatFlushChunk = 5000
	defaultReplicaTTL      = 45 * time.Second
	defaultShardLeaseTTL   = 45 * time.Second

	activeSweepSelectPrefix = `
		SELECT atespace, name, state, generation, actor_uid,
		       CASE
		           WHEN jsonb_typeof(pending_actions) = 'array' THEN jsonb_array_length(pending_actions) > 0
		           ELSE false
		       END,
		       (runner_token_hash IS NOT NULL AND octet_length(runner_token_hash) > 0),
		       (claim_deadline IS NOT NULL),
		       saved_at, last_resumed_at, last_heartbeat_at
		FROM tasks
		WHERE state NOT IN (4, 5, 6)`
)

func (s *Store) sweepTimeoutsMs() (lockMs, stmtMs int64) {
	lockMs = max(s.opts.ApplyLockTimeout.Milliseconds(), 1)
	stmtMs = max(s.opts.ApplyStatementTimeout.Milliseconds(), 1)
	return lockMs, stmtMs
}

// ListTaskAtespaces returns all distinct atespaces currently present in tasks
// (both active and terminal) using a recursive loose index skip scan over
// tasks_pkey (atespace, name) so discovery takes O(distinct atespaces) B-tree
// probes rather than scanning every row.
func (s *Store) ListTaskAtespaces(ctx context.Context) ([]string, error) {
	_, stmtMs := s.sweepTimeoutsMs()
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, classifyTxError(ctx, err)
	}
	defer rollbackTx(tx)

	if _, err := tx.Exec(ctx, fmt.Sprintf("SET LOCAL statement_timeout = '%dms'", stmtMs)); err != nil {
		return nil, classifyTxError(ctx, err)
	}

	rows, err := tx.Query(ctx, `
		WITH RECURSIVE distinct_atespaces(atespace) AS (
			(SELECT tasks.atespace FROM tasks ORDER BY tasks.atespace ASC LIMIT 1)
			UNION ALL
			SELECT (
				SELECT tasks.atespace
				FROM tasks
				WHERE tasks.atespace > d.atespace
				ORDER BY tasks.atespace ASC
				LIMIT 1
			)
			FROM distinct_atespaces d
			WHERE d.atespace IS NOT NULL
		)
		SELECT atespace FROM distinct_atespaces WHERE atespace IS NOT NULL`,
	)
	if err != nil {
		return nil, classifyTxError(ctx, err)
	}

	var out []string
	for rows.Next() {
		var ns string
		if err := rows.Scan(&ns); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scanning task atespace: %w", err)
		}
		out = append(out, ns)
	}
	rowsErr := rows.Err()
	rows.Close()
	if rowsErr != nil {
		return nil, classifyTxError(ctx, rowsErr)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, classifyTxError(ctx, err)
	}
	return out, nil
}

// SnapshotActiveTasks captures a consistent point-in-time snapshot of all
// non-terminal task sweep metadata in atespaces (or across all atespaces when
// empty) without reading TOASTed Task JSONB documents, together with
// PostgreSQL's clock_timestamp() at snapshot start.
func (s *Store) SnapshotActiveTasks(ctx context.Context, atespaces []string) ([]store.TaskSweepRecord, time.Time, error) {
	normalized := make([]string, 0, len(atespaces))
	for _, ns := range atespaces {
		normalized = append(normalized, defaultAtespace(ns))
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return nil, time.Time{}, classifyTxError(ctx, err)
	}
	defer rollbackTx(tx)

	_, stmtMs := s.sweepTimeoutsMs()
	if _, err := tx.Exec(ctx, fmt.Sprintf("SET LOCAL statement_timeout = '%dms'", stmtMs)); err != nil {
		return nil, time.Time{}, classifyTxError(ctx, err)
	}

	var storeNow time.Time
	if err := tx.QueryRow(ctx, `SELECT date_trunc('microsecond', clock_timestamp())`).Scan(&storeNow); err != nil {
		return nil, time.Time{}, classifyTxError(ctx, err)
	}
	storeNow = storeNow.UTC()

	var (
		records   []store.TaskSweepRecord
		hasCursor bool
		afterNs   string
		afterName string
	)
	for {
		// Use specialized query shapes without OR predicates so PostgreSQL
		// generic plans push atespace = ANY($1) and keyset bounds directly into
		// tasks_active_idx Index Cond.
		var (
			rows pgx.Rows
			qErr error
		)
		switch {
		case len(normalized) == 0 && !hasCursor:
			rows, qErr = tx.Query(ctx,
				activeSweepSelectPrefix+` ORDER BY atespace ASC, name ASC LIMIT $1`,
				sweepActivePageSize,
			)
		case len(normalized) == 0 && hasCursor:
			rows, qErr = tx.Query(ctx,
				activeSweepSelectPrefix+` AND (atespace, name) > ($1, $2) ORDER BY atespace ASC, name ASC LIMIT $3`,
				afterNs, afterName, sweepActivePageSize,
			)
		case len(normalized) > 0 && !hasCursor:
			rows, qErr = tx.Query(ctx,
				activeSweepSelectPrefix+` AND atespace = ANY($1::text[]) ORDER BY atespace ASC, name ASC LIMIT $2`,
				normalized, sweepActivePageSize,
			)
		default:
			rows, qErr = tx.Query(ctx,
				activeSweepSelectPrefix+` AND atespace = ANY($1::text[]) AND (atespace, name) > ($2, $3) ORDER BY atespace ASC, name ASC LIMIT $4`,
				normalized, afterNs, afterName, sweepActivePageSize,
			)
		}
		if qErr != nil {
			return nil, time.Time{}, classifyTxError(ctx, qErr)
		}

		pageCount := 0
		for rows.Next() {
			var (
				ns               string
				name             string
				stateCol         int16
				generation       int64
				actorUID         string
				hasActions       bool
				hasRunnerToken   bool
				hasClaimDeadline bool
				savedAt          time.Time
				lastResumedAt    *time.Time
				lastHeartbeatAt  *time.Time
			)
			if scanErr := rows.Scan(
				&ns, &name, &stateCol, &generation, &actorUID,
				&hasActions, &hasRunnerToken, &hasClaimDeadline,
				&savedAt, &lastResumedAt, &lastHeartbeatAt,
			); scanErr != nil {
				rows.Close()
				return nil, time.Time{}, fmt.Errorf("scanning active task sweep record: %w", scanErr)
			}
			records = append(records, store.TaskSweepRecord{
				Key:               store.TaskKey{Atespace: ns, Name: name}.Normalized(),
				State:             v1alpha1.TaskState(stateCol),
				Generation:        generation,
				ActorUID:          actorUID,
				HasPendingActions: hasActions,
				HasRunnerToken:    hasRunnerToken,
				HasClaimDeadline:  hasClaimDeadline,
				CreatedAt:         savedAt.UTC(),
				LastResumedAt:     utcTimePtr(lastResumedAt),
				LastHeartbeatAt:   utcTimePtr(lastHeartbeatAt),
			})
			hasCursor = true
			afterNs = ns
			afterName = name
			pageCount++
		}
		rowsErr := rows.Err()
		rows.Close()
		if rowsErr != nil {
			return nil, time.Time{}, classifyTxError(ctx, rowsErr)
		}
		if pageCount < sweepActivePageSize {
			break
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, time.Time{}, classifyTxError(ctx, err)
	}
	return records, storeNow, nil
}

// GetTaskRows batch-loads TaskRow records by primary key, scrubbing any expired
// worker leases against PostgreSQL's real-time clock.
func (s *Store) GetTaskRows(ctx context.Context, keys []store.TaskKey) (map[store.TaskKey]*store.TaskRow, error) {
	out := make(map[store.TaskKey]*store.TaskRow, len(keys))
	if len(keys) == 0 {
		return out, nil
	}

	seen := make(map[store.TaskKey]struct{}, len(keys))
	dedup := make([]store.TaskKey, 0, len(keys))
	for _, k := range keys {
		if k.Name == "" {
			continue
		}
		nk := k.Normalized()
		if _, dup := seen[nk]; dup {
			continue
		}
		seen[nk] = struct{}{}
		dedup = append(dedup, nk)
	}

	_, stmtMs := s.sweepTimeoutsMs()
	for start := 0; start < len(dedup); start += getTaskRowsChunkSize {
		chunk := dedup[start:min(start+getTaskRowsChunkSize, len(dedup))]
		atespaces := make([]string, len(chunk))
		names := make([]string, len(chunk))
		for i, k := range chunk {
			atespaces[i] = k.Atespace
			names[i] = k.Name
		}

		tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
		if err != nil {
			return nil, classifyTxError(ctx, err)
		}
		if _, err := tx.Exec(ctx, fmt.Sprintf("SET LOCAL statement_timeout = '%dms'", stmtMs)); err != nil {
			rollbackTx(tx)
			return nil, classifyTxError(ctx, err)
		}

		rows, err := tx.Query(ctx, `
			WITH clock AS (
				SELECT clock_timestamp() AS now
			)
			SELECT clock.now, `+qualifiedTaskRowColumns("t")+`
			FROM unnest($1::text[], $2::text[]) AS k(atespace, name)
			JOIN tasks t ON t.atespace = k.atespace AND t.name = k.name
			CROSS JOIN clock`,
			atespaces, names,
		)
		if err != nil {
			rollbackTx(tx)
			return nil, classifyTxError(ctx, err)
		}
		for rows.Next() {
			now, tr, scanErr := scanTaskRowWithClock(rows)
			if scanErr != nil {
				rows.Close()
				rollbackTx(tx)
				return nil, scanErr
			}
			if tr.LeaseExpiresAt != nil && !tr.LeaseExpiresAt.After(now) {
				tr.LeaseOwner = ""
				tr.LeaseExpiresAt = nil
			}
			k := store.TaskKey{
				Atespace: tr.Task.GetMetadata().GetAtespace(),
				Name:     tr.Task.GetMetadata().GetName(),
			}.Normalized()
			out[k] = tr
		}
		rowsErr := rows.Err()
		rows.Close()
		if rowsErr != nil {
			rollbackTx(tx)
			return nil, classifyTxError(ctx, rowsErr)
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, classifyTxError(ctx, err)
		}
	}
	return out, nil
}

// FlushHeartbeats writes buffered runner heartbeats in chunks of up to 5,000
// rows (locking matching rows in deterministic primary-key order and updating
// only the unindexed last_heartbeat_at column so PostgreSQL can apply HOT
// updates) and then refreshes replicaID's liveness and last_flush_at watermark
// in ax_replicas.
func (s *Store) FlushHeartbeats(ctx context.Context, replicaID string, replicaTTL time.Duration, entries []store.HeartbeatEntry) error {
	if replicaID == "" {
		return errors.New("replicaID is required for FlushHeartbeats")
	}
	if replicaTTL <= 0 {
		replicaTTL = defaultReplicaTTL
	}
	ttlMs := max(replicaTTL.Milliseconds(), 1)
	lockMs, stmtMs := s.sweepTimeoutsMs()

	dedup := dedupHeartbeatEntries(entries)
	for start := 0; start < len(dedup); start += maxHeartbeatFlushChunk {
		chunk := dedup[start:min(start+maxHeartbeatFlushChunk, len(dedup))]
		atespaces := make([]string, len(chunk))
		names := make([]string, len(chunk))
		hashes := make([][]byte, len(chunk))
		agesUs := make([]int64, len(chunk))
		for i, e := range chunk {
			atespaces[i] = e.Key.Atespace
			names[i] = e.Key.Name
			hashes[i] = e.RunnerTokenHash
			agesUs[i] = max(e.Age.Microseconds(), 0)
		}

		if err := s.execBoundedStmt(ctx, lockMs, stmtMs, `
			WITH clock AS (
				SELECT date_trunc('microsecond', clock_timestamp()) AS now
			),
			locked AS (
				SELECT t.atespace,
				       t.name,
				       b.token_hash,
				       date_trunc('microsecond', clock.now - (b.age_us * interval '1 microsecond')) AS hb_ts
				FROM unnest($1::text[], $2::text[], $3::bytea[], $4::bigint[])
				     AS b(atespace, name, token_hash, age_us)
				CROSS JOIN clock
				JOIN tasks t ON t.atespace = b.atespace AND t.name = b.name
				WHERE t.runner_token_hash IS NOT NULL
				  AND octet_length(b.token_hash) > 0
				  AND t.runner_token_hash = b.token_hash
				  AND (
				      t.last_heartbeat_at IS NULL
				      OR date_trunc('microsecond', clock.now - (b.age_us * interval '1 microsecond')) > t.last_heartbeat_at
				  )
				ORDER BY t.atespace ASC, t.name ASC
				FOR NO KEY UPDATE OF t
			)
			UPDATE tasks t
			SET last_heartbeat_at = GREATEST(
				COALESCE(t.last_heartbeat_at, '-infinity'::timestamptz),
				locked.hb_ts
			)
			FROM locked
			WHERE t.atespace = locked.atespace
			  AND t.name = locked.name
			  AND t.runner_token_hash = locked.token_hash`,
			atespaces, names, hashes, agesUs,
		); err != nil {
			if ctx.Err() == nil {
				// Best-effort renew only expires_at on ax_replicas without
				// advancing last_flush_at so a live replica experiencing tasks
				// contention stays registered while aging the cluster watermark
				// past 60s to pause lost-runner checks.
				_ = s.execBoundedStmt(ctx, lockMs, stmtMs, `
					WITH now_cte AS (
						SELECT date_trunc('microsecond', clock_timestamp()) AS now
					)
					INSERT INTO ax_replicas (replica_id, started_at, last_flush_at, expires_at)
					SELECT $1, now_cte.now, 'epoch'::timestamptz, now_cte.now + ($2 * interval '1 millisecond')
					FROM now_cte
					ON CONFLICT (replica_id) DO UPDATE
					SET expires_at = EXCLUDED.expires_at`,
					replicaID, ttlMs,
				)
			}
			return err
		}
	}

	// Update ax_replicas AFTER all heartbeat chunks succeed so last_flush_at
	// strictly bounds every heartbeat included in this flush.
	return s.execBoundedStmt(ctx, lockMs, stmtMs, `
		WITH now_cte AS (
			SELECT date_trunc('microsecond', clock_timestamp()) AS now
		),
		pruned AS (
			DELETE FROM ax_replicas
			USING now_cte
			WHERE ax_replicas.replica_id <> $1
			  AND ax_replicas.expires_at <= now_cte.now - interval '5 minutes'
		)
		INSERT INTO ax_replicas (replica_id, started_at, last_flush_at, expires_at)
		SELECT $1, now_cte.now, now_cte.now, now_cte.now + ($2 * interval '1 millisecond')
		FROM now_cte
		ON CONFLICT (replica_id) DO UPDATE
		SET last_flush_at = EXCLUDED.last_flush_at,
		    expires_at    = EXCLUDED.expires_at`,
		replicaID, ttlMs,
	)
}

func (s *Store) execBoundedStmt(ctx context.Context, lockMs, stmtMs int64, query string, args ...any) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return classifyTxError(ctx, err)
	}
	defer rollbackTx(tx)

	if lockMs > 0 {
		if _, err := tx.Exec(ctx, fmt.Sprintf("SET LOCAL lock_timeout = '%dms'", lockMs)); err != nil {
			return classifyTxError(ctx, err)
		}
	}
	if _, err := tx.Exec(ctx, fmt.Sprintf("SET LOCAL statement_timeout = '%dms'", stmtMs)); err != nil {
		return classifyTxError(ctx, err)
	}
	if _, err := tx.Exec(ctx, query, args...); err != nil {
		return classifyTxError(ctx, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return classifyTxError(ctx, err)
	}
	return nil
}

type heartbeatDedupKey struct {
	key       store.TaskKey
	tokenHash string
}

func dedupHeartbeatEntries(entries []store.HeartbeatEntry) []store.HeartbeatEntry {
	if len(entries) == 0 {
		return nil
	}
	// Deduplicate by (TaskKey, RunnerTokenHash) rather than TaskKey alone so a
	// stale or mismatched token entry can never shadow a valid token entry for
	// the same task in the same flush batch; WHERE t.runner_token_hash = b.token_hash
	// ensures at most one unnest row matches any given tasks row.
	byKey := make(map[heartbeatDedupKey]int, len(entries))
	out := make([]store.HeartbeatEntry, 0, len(entries))
	for _, e := range entries {
		if e.Key.Name == "" || len(e.RunnerTokenHash) == 0 {
			continue
		}
		nk := e.Key.Normalized()
		dk := heartbeatDedupKey{key: nk, tokenHash: string(e.RunnerTokenHash)}
		age := max(e.Age, time.Duration(0))
		if idx, exists := byKey[dk]; exists {
			if age <= out[idx].Age {
				out[idx].Age = age
			}
			continue
		}
		byKey[dk] = len(out)
		out = append(out, store.HeartbeatEntry{
			Key:             nk,
			RunnerTokenHash: e.RunnerTokenHash,
			Age:             age,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Key.Atespace != out[j].Key.Atespace {
			return out[i].Key.Atespace < out[j].Key.Atespace
		}
		if out[i].Key.Name != out[j].Key.Name {
			return out[i].Key.Name < out[j].Key.Name
		}
		return bytes.Compare(out[i].RunnerTokenHash, out[j].RunnerTokenHash) < 0
	})
	return out
}

// UnregisterReplica removes replicaID from ax_replicas on clean shutdown.
func (s *Store) UnregisterReplica(ctx context.Context, replicaID string) error {
	if replicaID == "" {
		return nil
	}
	lockMs, stmtMs := s.sweepTimeoutsMs()
	return s.execBoundedStmt(ctx, lockMs, stmtMs, `DELETE FROM ax_replicas WHERE replica_id = $1`, replicaID)
}

// InspectReplicas computes live replica count and heartbeat flush watermark age
// directly on PostgreSQL's clock.
func (s *Store) InspectReplicas(ctx context.Context) (store.ReplicaStats, error) {
	_, stmtMs := s.sweepTimeoutsMs()
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return store.ReplicaStats{}, classifyTxError(ctx, err)
	}
	defer rollbackTx(tx)

	if _, err := tx.Exec(ctx, fmt.Sprintf("SET LOCAL statement_timeout = '%dms'", stmtMs)); err != nil {
		return store.ReplicaStats{}, classifyTxError(ctx, err)
	}

	var (
		storeNow    time.Time
		liveCount   int
		oldestFlush *time.Time
	)
	if err := tx.QueryRow(ctx, `
		WITH clock AS (
			SELECT date_trunc('microsecond', clock_timestamp()) AS now
		)
		SELECT
			clock.now,
			COUNT(r.replica_id)::int,
			MIN(r.last_flush_at)
		FROM clock
		LEFT JOIN ax_replicas r ON r.expires_at > clock.now
		GROUP BY clock.now`,
	).Scan(&storeNow, &liveCount, &oldestFlush); err != nil {
		return store.ReplicaStats{}, classifyTxError(ctx, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return store.ReplicaStats{}, classifyTxError(ctx, err)
	}

	storeNow = storeNow.UTC()
	var watermarkAge time.Duration
	if liveCount > 0 && oldestFlush != nil {
		watermarkAge = max(storeNow.Sub(oldestFlush.UTC()), 0)
	}
	return store.ReplicaStats{
		LiveReplicas:    liveCount,
		HasLiveReplicas: liveCount > 0,
		WatermarkAge:    watermarkAge,
		StoreNow:        storeNow,
	}, nil
}

// TryAcquireShardLease attempts a non-blocking shard lease acquisition in locks.
func (s *Store) TryAcquireShardLease(ctx context.Context, shardKey, holderToken string, ttl time.Duration) (bool, time.Time, error) {
	if shardKey == "" || holderToken == "" {
		return false, time.Time{}, errors.New("shardKey and holderToken are required")
	}
	if ttl <= 0 {
		ttl = defaultShardLeaseTTL
	}
	ttlMs := max(ttl.Milliseconds(), 1)
	lockMs, stmtMs := s.sweepTimeoutsMs()

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, time.Time{}, classifyTxError(ctx, err)
	}
	defer rollbackTx(tx)

	if _, err := tx.Exec(ctx, fmt.Sprintf("SET LOCAL lock_timeout = '%dms'", lockMs)); err != nil {
		return false, time.Time{}, classifyTxError(ctx, err)
	}
	if _, err := tx.Exec(ctx, fmt.Sprintf("SET LOCAL statement_timeout = '%dms'", stmtMs)); err != nil {
		return false, time.Time{}, classifyTxError(ctx, err)
	}

	var (
		storeNow time.Time
		acquired bool
	)
	if err := tx.QueryRow(ctx, `
		WITH clock AS (
			SELECT date_trunc('microsecond', clock_timestamp()) AS now
		),
		upserted AS (
			INSERT INTO locks (key, token, expires_at)
			SELECT $1, $2, clock.now + ($3 * interval '1 millisecond')
			FROM clock
			ON CONFLICT (key) DO UPDATE
			SET token      = EXCLUDED.token,
			    expires_at = EXCLUDED.expires_at
			WHERE locks.expires_at <= (SELECT now FROM clock)
			   OR locks.token = EXCLUDED.token
			RETURNING 1
		)
		SELECT clock.now, EXISTS(SELECT 1 FROM upserted) FROM clock`,
		shardKey, holderToken, ttlMs,
	).Scan(&storeNow, &acquired); err != nil {
		return false, time.Time{}, classifyTxError(ctx, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return false, time.Time{}, classifyTxError(ctx, err)
	}
	return acquired, storeNow.UTC(), nil
}

// RenewShardLease extends an unexpired shard lease held by holderToken.
func (s *Store) RenewShardLease(ctx context.Context, shardKey, holderToken string, ttl time.Duration) (bool, time.Time, error) {
	if shardKey == "" || holderToken == "" {
		return false, time.Time{}, nil
	}
	if ttl <= 0 {
		ttl = defaultShardLeaseTTL
	}
	ttlMs := max(ttl.Milliseconds(), 1)
	lockMs, stmtMs := s.sweepTimeoutsMs()

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, time.Time{}, classifyTxError(ctx, err)
	}
	defer rollbackTx(tx)

	if _, err := tx.Exec(ctx, fmt.Sprintf("SET LOCAL lock_timeout = '%dms'", lockMs)); err != nil {
		return false, time.Time{}, classifyTxError(ctx, err)
	}
	if _, err := tx.Exec(ctx, fmt.Sprintf("SET LOCAL statement_timeout = '%dms'", stmtMs)); err != nil {
		return false, time.Time{}, classifyTxError(ctx, err)
	}

	var (
		storeNow time.Time
		renewed  bool
	)
	if err := tx.QueryRow(ctx, `
		WITH clock AS (
			SELECT date_trunc('microsecond', clock_timestamp()) AS now
		),
		updated AS (
			UPDATE locks
			SET expires_at = clock.now + ($3 * interval '1 millisecond')
			FROM clock
			WHERE locks.key = $1
			  AND locks.token = $2
			  AND locks.expires_at > clock.now
			RETURNING 1
		)
		SELECT clock.now, EXISTS(SELECT 1 FROM updated) FROM clock`,
		shardKey, holderToken, ttlMs,
	).Scan(&storeNow, &renewed); err != nil {
		return false, time.Time{}, classifyTxError(ctx, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return false, time.Time{}, classifyTxError(ctx, err)
	}
	return renewed, storeNow.UTC(), nil
}

// ReleaseShardLease deletes shardKey if it is held by holderToken.
func (s *Store) ReleaseShardLease(ctx context.Context, shardKey, holderToken string) error {
	if shardKey == "" || holderToken == "" {
		return nil
	}
	lockMs, stmtMs := s.sweepTimeoutsMs()
	return s.execBoundedStmt(ctx, lockMs, stmtMs, `DELETE FROM locks WHERE key = $1 AND token = $2`, shardKey, holderToken)
}
