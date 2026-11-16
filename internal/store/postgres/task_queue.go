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
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/ax/internal/lifecycle"
	"github.com/google/ax/internal/store"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/protobuf/encoding/protojson"
)

const (
	maxInsertConflictRetries = 3

	taskRowColumns = `atespace, name, data, generation, ` +
		`actor_uid, active_run_id, run_seq, action_seq, ` +
		`effective_profile, egress_rules, ` +
		`runner_token_hash, runner_token_confirmed, runner_token_gen, ` +
		`last_acked_seq, claim_deadline, last_resumed_at, last_heartbeat_at, ` +
		`pending_actions, next_reconcile_at, lease_owner, lease_expires_at`
)

// SetQueueWakeup registers a non-blocking wakeup callback fired when Apply or
// ReleaseLease commits a row whose NextReconcileAt is due immediately (<= now).
func (s *Store) SetQueueWakeup(fn func()) {
	s.wakeupMu.Lock()
	s.wakeup = fn
	s.wakeupMu.Unlock()
}

func (s *Store) notifyWakeup() {
	s.wakeupMu.RLock()
	fn := s.wakeup
	s.wakeupMu.RUnlock()
	if fn != nil {
		fn()
	}
}

func rollbackTx(tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = tx.Rollback(ctx)
}

func (s *Store) setApplyTimeouts(ctx context.Context, tx pgx.Tx) error {
	lockMs := max(s.opts.ApplyLockTimeout.Milliseconds(), 1)
	stmtMs := max(s.opts.ApplyStatementTimeout.Milliseconds(), 1)
	if _, err := tx.Exec(ctx, fmt.Sprintf("SET LOCAL lock_timeout = '%dms'", lockMs)); err != nil {
		return classifyTxError(ctx, err)
	}
	if _, err := tx.Exec(ctx, fmt.Sprintf("SET LOCAL statement_timeout = '%dms'", stmtMs)); err != nil {
		return classifyTxError(ctx, err)
	}
	return nil
}

func queryClockTimestamp(ctx context.Context, tx pgx.Tx) (time.Time, error) {
	var now time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return time.Time{}, classifyTxError(ctx, err)
	}
	return now.UTC(), nil
}

func classifyTxError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if ctx.Err() != nil {
		return err
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "55P03", "57014": // lock_not_available, query_canceled (statement_timeout)
			return fmt.Errorf("%w: %s", store.ErrLockTimeout, pgErr.Message)
		}
	}
	return err
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

func qualifiedTaskRowColumns(alias string) string {
	cols := strings.Split(taskRowColumns, ", ")
	for i, c := range cols {
		cols[i] = alias + "." + c
	}
	return strings.Join(cols, ", ")
}

func utcTimePtr(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	v := t.UTC()
	return &v
}

func scanTaskRow(row pgx.Row) (*store.TaskRow, error) {
	var (
		atespace, name       string
		data                 []byte
		generation           int64
		actorUID             string
		activeRunID          string
		runSeq, actionSeq    int64
		effectiveProfileJSON []byte
		egressRulesJSON      []byte
		runnerTokenHash      []byte
		runnerTokenConfirmed bool
		runnerTokenGen       int32
		lastAckedSeq         int64
		claimDeadline        *time.Time
		lastResumedAt        *time.Time
		lastHeartbeatAt      *time.Time
		pendingActionsJSON   []byte
		nextReconcileAt      *time.Time
		leaseOwner           *string
		leaseExpiresAt       *time.Time
	)

	err := row.Scan(
		&atespace, &name, &data, &generation,
		&actorUID, &activeRunID, &runSeq, &actionSeq,
		&effectiveProfileJSON, &egressRulesJSON,
		&runnerTokenHash, &runnerTokenConfirmed, &runnerTokenGen,
		&lastAckedSeq, &claimDeadline, &lastResumedAt, &lastHeartbeatAt,
		&pendingActionsJSON, &nextReconcileAt, &leaseOwner, &leaseExpiresAt,
	)
	if err != nil {
		return nil, err
	}
	return buildScannedTaskRow(
		atespace, name, data, generation,
		actorUID, activeRunID, runSeq, actionSeq,
		effectiveProfileJSON, egressRulesJSON,
		runnerTokenHash, runnerTokenConfirmed, runnerTokenGen,
		lastAckedSeq, claimDeadline, lastResumedAt, lastHeartbeatAt,
		pendingActionsJSON, nextReconcileAt, leaseOwner, leaseExpiresAt,
	)
}

func scanTaskRowWithClock(row pgx.Row) (time.Time, *store.TaskRow, error) {
	var (
		clockNow             time.Time
		atespace, name       string
		data                 []byte
		generation           int64
		actorUID             string
		activeRunID          string
		runSeq, actionSeq    int64
		effectiveProfileJSON []byte
		egressRulesJSON      []byte
		runnerTokenHash      []byte
		runnerTokenConfirmed bool
		runnerTokenGen       int32
		lastAckedSeq         int64
		claimDeadline        *time.Time
		lastResumedAt        *time.Time
		lastHeartbeatAt      *time.Time
		pendingActionsJSON   []byte
		nextReconcileAt      *time.Time
		leaseOwner           *string
		leaseExpiresAt       *time.Time
	)

	err := row.Scan(
		&clockNow,
		&atespace, &name, &data, &generation,
		&actorUID, &activeRunID, &runSeq, &actionSeq,
		&effectiveProfileJSON, &egressRulesJSON,
		&runnerTokenHash, &runnerTokenConfirmed, &runnerTokenGen,
		&lastAckedSeq, &claimDeadline, &lastResumedAt, &lastHeartbeatAt,
		&pendingActionsJSON, &nextReconcileAt, &leaseOwner, &leaseExpiresAt,
	)
	if err != nil {
		return time.Time{}, nil, err
	}
	tr, err := buildScannedTaskRow(
		atespace, name, data, generation,
		actorUID, activeRunID, runSeq, actionSeq,
		effectiveProfileJSON, egressRulesJSON,
		runnerTokenHash, runnerTokenConfirmed, runnerTokenGen,
		lastAckedSeq, claimDeadline, lastResumedAt, lastHeartbeatAt,
		pendingActionsJSON, nextReconcileAt, leaseOwner, leaseExpiresAt,
	)
	if err != nil {
		return time.Time{}, nil, err
	}
	return clockNow.UTC(), tr, nil
}

func buildScannedTaskRow(
	atespace, name string,
	data []byte,
	generation int64,
	actorUID, activeRunID string,
	runSeq, actionSeq int64,
	effectiveProfileJSON, egressRulesJSON []byte,
	runnerTokenHash []byte,
	runnerTokenConfirmed bool,
	runnerTokenGen int32,
	lastAckedSeq int64,
	claimDeadline, lastResumedAt, lastHeartbeatAt *time.Time,
	pendingActionsJSON []byte,
	nextReconcileAt *time.Time,
	leaseOwner *string,
	leaseExpiresAt *time.Time,
) (*store.TaskRow, error) {
	var task v1alpha1.Task
	if err := unmarshalOpts.Unmarshal(data, &task); err != nil {
		return nil, fmt.Errorf("unmarshaling task %s/%s: %w", atespace, name, err)
	}
	if generation > task.GetStatus().GetGeneration() {
		if task.Status == nil {
			task.Status = &v1alpha1.TaskStatus{}
		}
		task.Status.Generation = generation
	}

	var effectiveProfile *v1alpha1.Profile
	if len(effectiveProfileJSON) > 0 && string(effectiveProfileJSON) != "null" {
		var p v1alpha1.Profile
		if err := unmarshalOpts.Unmarshal(effectiveProfileJSON, &p); err != nil {
			return nil, fmt.Errorf("unmarshaling effective_profile for %s/%s: %w", atespace, name, err)
		}
		effectiveProfile = &p
	}

	egressRules, err := unmarshalEgressRules(egressRulesJSON)
	if err != nil {
		return nil, fmt.Errorf("unmarshaling egress_rules for %s/%s: %w", atespace, name, err)
	}

	var actions []lifecycle.Action
	if len(pendingActionsJSON) > 0 && string(pendingActionsJSON) != "null" {
		if err := json.Unmarshal(pendingActionsJSON, &actions); err != nil {
			return nil, fmt.Errorf("unmarshaling pending_actions for %s/%s: %w", atespace, name, err)
		}
		if len(actions) == 0 {
			actions = nil
		}
	}

	var owner string
	if leaseOwner != nil {
		owner = *leaseOwner
	}

	var tokenHashCopy []byte
	if len(runnerTokenHash) > 0 {
		tokenHashCopy = append([]byte(nil), runnerTokenHash...)
	}

	return &store.TaskRow{
		State: lifecycle.State{
			Task:                 &task,
			Actions:              actions,
			ActorUID:             actorUID,
			ActiveRunID:          activeRunID,
			RunSeq:               runSeq,
			ActionSeq:            actionSeq,
			RunnerTokenHash:      tokenHashCopy,
			RunnerTokenConfirmed: runnerTokenConfirmed,
			RunnerTokenGen:       runnerTokenGen,
			LastAckedRunnerSeq:   lastAckedSeq,
			ClaimDeadline:        utcTimePtr(claimDeadline),
			LastResumedAt:        utcTimePtr(lastResumedAt),
		},
		EffectiveProfile: effectiveProfile,
		EgressRules:      egressRules,
		Generation:       generation,
		LastHeartbeatAt:  utcTimePtr(lastHeartbeatAt),
		NextReconcileAt:  utcTimePtr(nextReconcileAt),
		LeaseOwner:       owner,
		LeaseExpiresAt:   utcTimePtr(leaseExpiresAt),
	}, nil
}

func marshalEffectiveProfile(p *v1alpha1.Profile) (*string, error) {
	if p == nil {
		return nil, nil
	}
	b, err := protojson.Marshal(p)
	if err != nil {
		return nil, err
	}
	s := string(b)
	return &s, nil
}

func marshalEgressRules(rules []*ateapipb.EgressRule) (string, error) {
	if len(rules) == 0 {
		return "[]", nil
	}
	items := make([]json.RawMessage, 0, len(rules))
	for _, r := range rules {
		if r == nil {
			continue
		}
		b, err := protojson.Marshal(r)
		if err != nil {
			return "", err
		}
		items = append(items, json.RawMessage(b))
	}
	out, err := json.Marshal(items)
	if err != nil {
		return "", err
	}
	return string(out), nil
}

func unmarshalEgressRules(data []byte) ([]*ateapipb.EgressRule, error) {
	if len(data) == 0 || string(data) == "null" || string(data) == "[]" {
		return nil, nil
	}
	var items []json.RawMessage
	if err := json.Unmarshal(data, &items); err != nil {
		return nil, err
	}
	if len(items) == 0 {
		return nil, nil
	}
	out := make([]*ateapipb.EgressRule, 0, len(items))
	for _, raw := range items {
		var r ateapipb.EgressRule
		if err := unmarshalOpts.Unmarshal(raw, &r); err != nil {
			return nil, err
		}
		out = append(out, &r)
	}
	return out, nil
}

func marshalPendingActions(actions []lifecycle.Action) (string, error) {
	if len(actions) == 0 {
		return "[]", nil
	}
	b, err := json.Marshal(actions)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// GetTaskRow loads the full TaskRow for key without locking it, or returns
// store.ErrNotFound if no matching row exists.
func (s *Store) GetTaskRow(ctx context.Context, key store.TaskKey) (*store.TaskRow, error) {
	key = key.Normalized()
	now, row, err := scanTaskRowWithClock(s.pool.QueryRow(ctx,
		`SELECT clock_timestamp(), `+taskRowColumns+` FROM tasks WHERE atespace = $1 AND name = $2`,
		key.Atespace, key.Name,
	))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, store.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("getting task row %s/%s: %w", key.Atespace, key.Name, err)
	}
	if row.LeaseExpiresAt != nil && !row.LeaseExpiresAt.After(now) {
		row.LeaseOwner = ""
		row.LeaseExpiresAt = nil
	}
	return row, nil
}

// Apply locks key's row, reads PostgreSQL's post-lock clock_timestamp(), invokes
// fn, and commits the resulting insert, update, delete, or no-op in a single
// bounded transaction.
func (s *Store) Apply(ctx context.Context, key store.TaskKey, fn store.ApplyFunc) (*store.TaskRow, error) {
	key = key.Normalized()

	for attempt := 0; attempt <= maxInsertConflictRetries; attempt++ {
		written, shouldWake, retryConflict, err := s.applyOnce(ctx, key, fn)
		if retryConflict {
			continue
		}
		if err != nil {
			return nil, err
		}
		if shouldWake {
			s.notifyWakeup()
		}
		return written, nil
	}
	return nil, fmt.Errorf("%w: concurrent task creation conflict on %s/%s", store.ErrLockTimeout, key.Atespace, key.Name)
}

func (s *Store) applyOnce(ctx context.Context, key store.TaskKey, fn store.ApplyFunc) (*store.TaskRow, bool, bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, false, false, classifyTxError(ctx, err)
	}
	defer rollbackTx(tx)

	if err := s.setApplyTimeouts(ctx, tx); err != nil {
		return nil, false, false, err
	}

	stored, err := scanTaskRow(tx.QueryRow(ctx,
		`SELECT `+taskRowColumns+` FROM tasks WHERE atespace = $1 AND name = $2 FOR UPDATE`,
		key.Atespace, key.Name,
	))
	if errors.Is(err, pgx.ErrNoRows) {
		stored = nil
	} else if err != nil {
		return nil, false, false, classifyTxError(ctx, err)
	}

	now, err := queryClockTimestamp(ctx, tx)
	if err != nil {
		return nil, false, false, err
	}

	var current *store.TaskRow
	if stored != nil {
		current = stored.Clone()
		if current.LeaseExpiresAt != nil && !current.LeaseExpiresAt.After(now) {
			current.LeaseOwner = ""
			current.LeaseExpiresAt = nil
		}
	}

	next, err := fn(current, now)
	if err != nil {
		return nil, false, false, err
	}

	switch {
	case next == store.DeleteTaskRow:
		if stored != nil {
			if _, err := tx.Exec(ctx, `DELETE FROM tasks WHERE atespace = $1 AND name = $2`, key.Atespace, key.Name); err != nil {
				return nil, false, false, classifyTxError(ctx, err)
			}
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, false, false, classifyTxError(ctx, err)
		}
		return nil, false, false, nil

	case next == nil || (current != nil && next == current):
		if err := tx.Commit(ctx); err != nil {
			return nil, false, false, classifyTxError(ctx, err)
		}
		if stored == nil {
			return nil, false, false, nil
		}
		ret := stored.Clone()
		if ret.LeaseExpiresAt != nil && !ret.LeaseExpiresAt.After(now) {
			ret.LeaseOwner = ""
			ret.LeaseExpiresAt = nil
		}
		return ret, false, false, nil
	}

	written := next.Clone()
	gen := int64(1)
	if stored != nil {
		gen = stored.Generation + 1
	}
	if err := store.NormalizeTaskForApply(written.Task, key, gen); err != nil {
		return nil, false, false, err
	}
	written.Generation = gen
	if written.LeaseExpiresAt != nil && !written.LeaseExpiresAt.After(now) {
		written.LeaseOwner = ""
		written.LeaseExpiresAt = nil
	}
	if written.LeaseOwner == "" {
		written.LeaseExpiresAt = nil
	}
	if written.LeaseOwner != "" && written.LeaseExpiresAt != nil && written.NextReconcileAt != nil &&
		written.NextReconcileAt.Before(*written.LeaseExpiresAt) {
		exp := *written.LeaseExpiresAt
		written.NextReconcileAt = &exp
	}

	dataBytes, err := protojson.Marshal(written.Task)
	if err != nil {
		return nil, false, false, fmt.Errorf("marshaling task: %w", err)
	}
	effProfileJSON, err := marshalEffectiveProfile(written.EffectiveProfile)
	if err != nil {
		return nil, false, false, fmt.Errorf("marshaling effective_profile: %w", err)
	}
	egressJSON, err := marshalEgressRules(written.EgressRules)
	if err != nil {
		return nil, false, false, fmt.Errorf("marshaling egress_rules: %w", err)
	}
	actionsJSON, err := marshalPendingActions(written.Actions)
	if err != nil {
		return nil, false, false, fmt.Errorf("marshaling pending_actions: %w", err)
	}
	var leaseOwnerPtr *string
	if written.LeaseOwner != "" {
		owner := written.LeaseOwner
		leaseOwnerPtr = &owner
	}

	if stored == nil {
		_, err = tx.Exec(ctx, `
			INSERT INTO tasks (
				atespace, name, data, saved_at, state, generation, session_id,
				actor_uid, active_run_id, run_seq, action_seq,
				effective_profile, egress_rules,
				runner_token_hash, runner_token_confirmed, runner_token_gen,
				last_acked_seq, claim_deadline, last_resumed_at, last_heartbeat_at,
				pending_actions, next_reconcile_at, lease_owner, lease_expires_at
			) VALUES (
				$1, $2, $3::jsonb, $4, $5, $6, $7,
				$8, $9, $10, $11,
				$12::jsonb, $13::jsonb,
				$14, $15, $16,
				$17, $18, $19, $20,
				$21::jsonb, $22, $23, $24
			)`,
			key.Atespace, key.Name, string(dataBytes), now,
			int16(written.Task.GetStatus().GetState()), written.Generation, written.Task.GetSpec().GetSessionId(),
			written.ActorUID, written.ActiveRunID, written.RunSeq, written.ActionSeq,
			effProfileJSON, egressJSON,
			written.RunnerTokenHash, written.RunnerTokenConfirmed, written.RunnerTokenGen,
			written.LastAckedRunnerSeq, written.ClaimDeadline, written.LastResumedAt, written.LastHeartbeatAt,
			actionsJSON, written.NextReconcileAt, leaseOwnerPtr, written.LeaseExpiresAt,
		)
		if isUniqueViolation(err) {
			return nil, false, true, nil
		}
		if err != nil {
			return nil, false, false, classifyTxError(ctx, err)
		}
	} else {
		_, err = tx.Exec(ctx, `
			UPDATE tasks SET
				data                   = $3::jsonb,
				state                  = $4,
				generation             = $5,
				session_id             = $6,
				actor_uid              = $7,
				active_run_id          = $8,
				run_seq                = $9,
				action_seq             = $10,
				effective_profile      = $11::jsonb,
				egress_rules           = $12::jsonb,
				runner_token_hash      = $13,
				runner_token_confirmed = $14,
				runner_token_gen       = $15,
				last_acked_seq         = $16,
				claim_deadline         = $17,
				last_resumed_at        = $18,
				last_heartbeat_at      = $19,
				pending_actions        = $20::jsonb,
				next_reconcile_at      = $21,
				lease_owner            = $22,
				lease_expires_at       = $23
			WHERE atespace = $1 AND name = $2`,
			key.Atespace, key.Name, string(dataBytes),
			int16(written.Task.GetStatus().GetState()), written.Generation, written.Task.GetSpec().GetSessionId(),
			written.ActorUID, written.ActiveRunID, written.RunSeq, written.ActionSeq,
			effProfileJSON, egressJSON,
			written.RunnerTokenHash, written.RunnerTokenConfirmed, written.RunnerTokenGen,
			written.LastAckedRunnerSeq, written.ClaimDeadline, written.LastResumedAt, written.LastHeartbeatAt,
			actionsJSON, written.NextReconcileAt, leaseOwnerPtr, written.LeaseExpiresAt,
		)
		if err != nil {
			return nil, false, false, classifyTxError(ctx, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, false, false, classifyTxError(ctx, err)
	}
	shouldWake := written.NextReconcileAt != nil && !written.NextReconcileAt.After(now)
	return written.Clone(), shouldWake, false, nil
}

// LeaseDue atomically claims up to limit due rows using FOR UPDATE SKIP LOCKED
// against clock_timestamp() and advances both lease_expires_at and
// next_reconcile_at to now + leaseTTL.
func (s *Store) LeaseDue(ctx context.Context, workerID string, leaseTTL time.Duration, limit int) ([]store.LeasedTask, error) {
	if limit <= 0 {
		return nil, nil
	}
	if leaseTTL <= 0 {
		leaseTTL = store.DefaultLeaseTTL
	}
	ttlMs := max(leaseTTL.Milliseconds(), 1)

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, classifyTxError(ctx, err)
	}
	defer rollbackTx(tx)

	if err := s.setApplyTimeouts(ctx, tx); err != nil {
		return nil, err
	}

	now, err := queryClockTimestamp(ctx, tx)
	if err != nil {
		return nil, err
	}
	exp := now.Add(time.Duration(ttlMs) * time.Millisecond)

	rows, err := tx.Query(ctx, `
		UPDATE tasks t
		SET lease_owner       = $3 || ':' || gen_random_uuid()::text,
		    lease_expires_at  = $4,
		    next_reconcile_at = $4
		FROM (
			SELECT tasks.atespace, tasks.name
			FROM tasks
			WHERE tasks.next_reconcile_at IS NOT NULL
			  AND tasks.next_reconcile_at <= $1
			ORDER BY tasks.next_reconcile_at ASC, tasks.atespace ASC, tasks.name ASC
			LIMIT $2
			FOR UPDATE OF tasks SKIP LOCKED
		) AS due
		WHERE t.atespace = due.atespace AND t.name = due.name
		RETURNING `+qualifiedTaskRowColumns("t"),
		now, limit, workerID, exp,
	)
	if err != nil {
		return nil, classifyTxError(ctx, err)
	}

	var leased []store.LeasedTask
	for rows.Next() {
		tr, scanErr := scanTaskRow(rows)
		if scanErr != nil {
			rows.Close()
			return nil, scanErr
		}
		leased = append(leased, store.LeasedTask{
			Key: store.TaskKey{
				Atespace: tr.Task.GetMetadata().GetAtespace(),
				Name:     tr.Task.GetMetadata().GetName(),
			}.Normalized(),
			LeaseToken: tr.LeaseOwner,
			LeasedAt:   now,
			ExpiresAt:  exp,
			Row:        tr,
		})
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, classifyTxError(ctx, err)
	}
	rows.Close()

	if err := tx.Commit(ctx); err != nil {
		return nil, classifyTxError(ctx, err)
	}
	return leased, nil
}

// RenewLease extends an active worker lease and returns the row's current head
// Action.ID (or "" if pending_actions is empty).
func (s *Store) RenewLease(ctx context.Context, key store.TaskKey, leaseToken string, leaseTTL time.Duration) (string, error) {
	if leaseToken == "" {
		return "", store.ErrLeaseLost
	}
	if leaseTTL <= 0 {
		leaseTTL = store.DefaultLeaseTTL
	}
	key = key.Normalized()
	ttlMs := max(leaseTTL.Milliseconds(), 1)

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return "", classifyTxError(ctx, err)
	}
	defer rollbackTx(tx)

	if err := s.setApplyTimeouts(ctx, tx); err != nil {
		return "", err
	}

	var headActionID string
	err = tx.QueryRow(ctx, `
		UPDATE tasks
		SET lease_expires_at  = clock_timestamp() + ($4 * interval '1 millisecond'),
		    next_reconcile_at = GREATEST(next_reconcile_at, clock_timestamp() + ($4 * interval '1 millisecond'))
		WHERE atespace = $1
		  AND name = $2
		  AND lease_owner = $3
		  AND lease_expires_at > clock_timestamp()
		RETURNING COALESCE(pending_actions->0->>'id', '')`,
		key.Atespace, key.Name, leaseToken, ttlMs,
	).Scan(&headActionID)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", store.ErrLeaseLost
	}
	if err != nil {
		return "", classifyTxError(ctx, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return "", classifyTxError(ctx, err)
	}
	return headActionID, nil
}

// ReleaseLease clears leaseToken if still active and recomputes next_reconcile_at.
func (s *Store) ReleaseLease(ctx context.Context, key store.TaskKey, leaseToken string, requeueAfter time.Duration) error {
	if leaseToken == "" {
		return nil
	}
	key = key.Normalized()

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return classifyTxError(ctx, err)
	}
	defer rollbackTx(tx)

	if err := s.setApplyTimeouts(ctx, tx); err != nil {
		return err
	}

	stored, err := scanTaskRow(tx.QueryRow(ctx,
		`SELECT `+taskRowColumns+` FROM tasks WHERE atespace = $1 AND name = $2 FOR UPDATE`,
		key.Atespace, key.Name,
	))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return classifyTxError(ctx, err)
	}

	now, err := queryClockTimestamp(ctx, tx)
	if err != nil {
		return err
	}
	if !store.HasActiveLease(stored, leaseToken, now) {
		return nil
	}

	next := lifecycle.NextReconcileAt(stored.State, now)
	if requeueAfter > 0 && next != nil {
		floor := now.Add(requeueAfter)
		if next.Before(floor) {
			next = &floor
		}
	}
	if _, err := tx.Exec(ctx, `
		UPDATE tasks
		SET lease_owner = NULL, lease_expires_at = NULL, next_reconcile_at = $3
		WHERE atespace = $1 AND name = $2`,
		key.Atespace, key.Name, next,
	); err != nil {
		return classifyTxError(ctx, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return classifyTxError(ctx, err)
	}
	if next != nil && !next.After(now) {
		s.notifyWakeup()
	}
	return nil
}

// BackoffAction increments Tries and advances NotBefore on the head action if
// leaseToken still owns the row and actionID still matches Actions[0].ID.
func (s *Store) BackoffAction(ctx context.Context, key store.TaskKey, leaseToken, actionID string, backoff time.Duration) error {
	if leaseToken == "" || actionID == "" {
		return nil
	}
	key = key.Normalized()

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return classifyTxError(ctx, err)
	}
	defer rollbackTx(tx)

	if err := s.setApplyTimeouts(ctx, tx); err != nil {
		return err
	}

	stored, err := scanTaskRow(tx.QueryRow(ctx,
		`SELECT `+taskRowColumns+` FROM tasks WHERE atespace = $1 AND name = $2 FOR UPDATE`,
		key.Atespace, key.Name,
	))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return classifyTxError(ctx, err)
	}

	now, err := queryClockTimestamp(ctx, tx)
	if err != nil {
		return err
	}
	if !store.HasActiveLease(stored, leaseToken, now) {
		return nil
	}
	if len(stored.Actions) == 0 || stored.Actions[0].ID != actionID {
		return nil
	}

	stored.Actions[0].Tries++
	stored.Actions[0].NotBefore = now.Add(max(backoff, 0))
	nextGen := stored.Generation + 1
	if err := store.NormalizeTaskForApply(stored.Task, key, nextGen); err != nil {
		return err
	}
	dataBytes, err := protojson.Marshal(stored.Task)
	if err != nil {
		return fmt.Errorf("marshaling task: %w", err)
	}
	actionsJSON, err := marshalPendingActions(stored.Actions)
	if err != nil {
		return fmt.Errorf("marshaling pending_actions: %w", err)
	}
	nextDue := lifecycle.NextReconcileAt(stored.State, now)

	if _, err := tx.Exec(ctx, `
		UPDATE tasks
		SET data              = $3::jsonb,
		    generation        = $4,
		    pending_actions   = $5::jsonb,
		    next_reconcile_at = $6,
		    lease_owner       = NULL,
		    lease_expires_at  = NULL
		WHERE atespace = $1 AND name = $2`,
		key.Atespace, key.Name, string(dataBytes), nextGen, actionsJSON, nextDue,
	); err != nil {
		return classifyTxError(ctx, err)
	}
	return classifyTxError(ctx, tx.Commit(ctx))
}
