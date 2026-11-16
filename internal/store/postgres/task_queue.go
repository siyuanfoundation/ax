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
	"time"

	"github.com/google/ax/internal/lifecycle"
	"github.com/google/ax/internal/store"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const (
	pgCodeUniqueViolation  = "23505"
	pgCodeLockNotAvailable = "55P03"
	pgCodeQueryCanceled    = "57014"

	maxInsertConflictRetries = 3
	rollbackTimeout          = 2 * time.Second

	taskRowColumns = `
		atespace, name, data, state, generation, session_id,
		actor_uid, active_run_id, run_seq, action_seq,
		effective_profile, egress_rules,
		runner_token_hash, runner_token_confirmed, runner_token_gen,
		last_acked_seq, claim_deadline, last_resumed_at, last_heartbeat_at,
		pending_actions, next_reconcile_at, lease_owner, lease_expires_at`
)

// SetQueueWakeup registers a callback invoked after a committed row is due.
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

// rollbackTx aborts an uncommitted transaction using an independent bounded
// context so a canceled caller context still sends ROLLBACK cleanly instead of
// forcing pgxpool to destroy and reconnect the underlying TCP session.
func rollbackTx(tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), rollbackTimeout)
	defer cancel()
	_ = tx.Rollback(ctx)
}

func (s *Store) setApplyTimeouts(ctx context.Context, tx pgx.Tx) error {
	lockMs := max(s.opts.ApplyLockTimeout.Milliseconds(), 1)
	stmtMs := max(s.opts.ApplyStatementTimeout.Milliseconds(), 1)
	// SET LOCAL does not accept bind parameters in PostgreSQL; both values are
	// positive integer millisecond counts derived from time.Duration.
	if _, err := tx.Exec(ctx, fmt.Sprintf("SET LOCAL lock_timeout = '%dms'", lockMs)); err != nil {
		return classifyTxError(ctx, err)
	}
	if _, err := tx.Exec(ctx, fmt.Sprintf("SET LOCAL statement_timeout = '%dms'", stmtMs)); err != nil {
		return classifyTxError(ctx, err)
	}
	return nil
}

// queryClockTimestamp reads PostgreSQL's real-time clock_timestamp() (rather
// than transaction_timestamp() / now(), which freezes at BEGIN before any
// SELECT ... FOR UPDATE wait completes). Always call this AFTER acquiring any
// row lock so due-time and lease-expiry checks reflect post-wait reality.
func queryClockTimestamp(ctx context.Context, tx pgx.Tx) (time.Time, error) {
	var now time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return time.Time{}, classifyTxError(ctx, err)
	}
	return now.UTC(), nil
}

// classifyTxError maps PostgreSQL lock_timeout (55P03) and statement_timeout
// (57014 when the caller's context was not itself canceled) to
// store.ErrLockTimeout so callers can treat contention as retryable.
func classifyTxError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case pgCodeLockNotAvailable:
			return fmt.Errorf("%w: %w", store.ErrLockTimeout, err)
		case pgCodeQueryCanceled:
			if ctxErr := ctx.Err(); ctxErr != nil {
				return fmt.Errorf("%w: %w", ctxErr, err)
			}
			return fmt.Errorf("%w: %w", store.ErrLockTimeout, err)
		}
	}
	if ctxErr := ctx.Err(); ctxErr != nil && errors.Is(err, ctxErr) {
		return ctxErr
	}
	return err
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == pgCodeUniqueViolation
}

func scanTaskRow(row pgx.Row) (*store.TaskRow, error) {
	var (
		atespace             string
		name                 string
		data                 []byte
		stateCol             int16
		generation           int64
		sessionID            string
		actorUID             string
		activeRunID          string
		runSeq               int64
		actionSeq            int64
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
		&atespace, &name, &data, &stateCol, &generation, &sessionID,
		&actorUID, &activeRunID, &runSeq, &actionSeq,
		&effectiveProfileJSON, &egressRulesJSON,
		&runnerTokenHash, &runnerTokenConfirmed, &runnerTokenGen,
		&lastAckedSeq, &claimDeadline, &lastResumedAt, &lastHeartbeatAt,
		&pendingActionsJSON, &nextReconcileAt, &leaseOwner, &leaseExpiresAt,
	)
	if err != nil {
		return nil, err
	}
	_ = stateCol
	_ = sessionID

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
		atespace             string
		name                 string
		data                 []byte
		stateCol             int16
		generation           int64
		sessionID            string
		actorUID             string
		activeRunID          string
		runSeq               int64
		actionSeq            int64
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
		&atespace, &name, &data, &stateCol, &generation, &sessionID,
		&actorUID, &activeRunID, &runSeq, &actionSeq,
		&effectiveProfileJSON, &egressRulesJSON,
		&runnerTokenHash, &runnerTokenConfirmed, &runnerTokenGen,
		&lastAckedSeq, &claimDeadline, &lastResumedAt, &lastHeartbeatAt,
		&pendingActionsJSON, &nextReconcileAt, &leaseOwner, &leaseExpiresAt,
	)
	if err != nil {
		return time.Time{}, nil, err
	}
	_ = stateCol
	_ = sessionID

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
	overlayTaskGeneration(&task, generation)

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

func utcTimePtr(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	u := t.UTC()
	return &u
}

func marshalEffectiveProfile(p *v1alpha1.Profile) ([]byte, error) {
	if p == nil {
		return nil, nil
	}
	return protojson.Marshal(p)
}

func marshalEgressRules(rules []*ateapipb.EgressRule) ([]byte, error) {
	if len(rules) == 0 {
		return []byte("[]"), nil
	}
	raw := make([]json.RawMessage, 0, len(rules))
	for _, r := range rules {
		if r == nil {
			continue
		}
		b, err := protojson.Marshal(r)
		if err != nil {
			return nil, err
		}
		raw = append(raw, json.RawMessage(b))
	}
	return json.Marshal(raw)
}

func unmarshalEgressRules(data []byte) ([]*ateapipb.EgressRule, error) {
	if len(data) == 0 || string(data) == "null" || string(data) == "[]" {
		return nil, nil
	}
	var raw []json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, err
	}
	if len(raw) == 0 {
		return nil, nil
	}
	out := make([]*ateapipb.EgressRule, 0, len(raw))
	for _, elem := range raw {
		var r ateapipb.EgressRule
		if err := unmarshalOpts.Unmarshal(elem, &r); err != nil {
			return nil, err
		}
		out = append(out, &r)
	}
	return out, nil
}

func marshalPendingActions(actions []lifecycle.Action) ([]byte, error) {
	if len(actions) == 0 {
		return []byte("[]"), nil
	}
	return json.Marshal(actions)
}

// GetTaskRow loads the full TaskRow for key without locking it (clearing any
// expired worker lease against PostgreSQL's real-time clock in a single query),
// or returns store.ErrNotFound if no matching row exists.
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
// bounded transaction. If concurrent callers race to insert the same missing
// key, the losing caller retries up to maxInsertConflictRetries times against
// the winner's committed row.
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

	// Acquire the row lock BEFORE reading clock_timestamp() so any time spent
	// waiting on a contended FOR UPDATE lock is reflected in now.
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

	var passedCurrent *store.TaskRow
	if stored != nil {
		passedCurrent = stored.Clone()
		if passedCurrent.LeaseExpiresAt != nil && !passedCurrent.LeaseExpiresAt.After(now) {
			passedCurrent.LeaseOwner = ""
			passedCurrent.LeaseExpiresAt = nil
		}
	}

	next, fnErr := fn(passedCurrent, now)
	switch {
	case fnErr != nil:
		return nil, false, false, fnErr

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

	case next == nil || (passedCurrent != nil && next == passedCurrent):
		// Read-only transaction; commit releases the row lock cleanly.
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
	nextGen := int64(1)
	if stored != nil {
		baseGen := max(stored.Generation, stored.Task.GetStatus().GetGeneration())
		nextGen = max(baseGen+1, 1)
	}
	if err := store.NormalizeTaskForApply(written.Task, key, nextGen); err != nil {
		return nil, false, false, err
	}
	written.Generation = nextGen
	// Align non-zero NotBefore and ClaimDeadline timestamps to PostgreSQL's
	// microsecond timestamptz resolution so pending_actions JSONB and
	// next_reconcile_at never diverge by sub-microsecond nanoseconds.
	for i := range written.Actions {
		if !written.Actions[i].NotBefore.IsZero() {
			written.Actions[i].NotBefore = written.Actions[i].NotBefore.UTC().Truncate(time.Microsecond)
		}
	}
	if written.ClaimDeadline != nil {
		cd := written.ClaimDeadline.UTC().Truncate(time.Microsecond)
		written.ClaimDeadline = &cd
	}
	written.NextReconcileAt = lifecycle.NextReconcileAt(written.LifecycleState(), now)

	// Preserve an active worker lease from stored and clamp NextReconcileAt to
	// at least LeaseExpiresAt; clear any expired lease.
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

	effProfileBytes, err := marshalEffectiveProfile(written.EffectiveProfile)
	if err != nil {
		return nil, false, false, fmt.Errorf("marshaling effective_profile: %w", err)
	}
	egressBytes, err := marshalEgressRules(written.EgressRules)
	if err != nil {
		return nil, false, false, fmt.Errorf("marshaling egress_rules: %w", err)
	}
	actionsBytes, err := marshalPendingActions(written.Actions)
	if err != nil {
		return nil, false, false, fmt.Errorf("marshaling pending_actions: %w", err)
	}

	var leaseOwnerArg *string
	if written.LeaseOwner != "" {
		owner := written.LeaseOwner
		leaseOwnerArg = &owner
	}
	var tokenHashArg []byte
	if len(written.RunnerTokenHash) > 0 {
		tokenHashArg = written.RunnerTokenHash
	}

	if stored == nil {
		dataBytes, err := protojson.Marshal(written.Task)
		if err != nil {
			return nil, false, false, fmt.Errorf("marshaling task: %w", err)
		}
		_, err = tx.Exec(ctx, `
			INSERT INTO tasks (
				atespace, name, data, saved_at,
				state, generation, session_id,
				actor_uid, active_run_id, run_seq, action_seq,
				effective_profile, egress_rules,
				runner_token_hash, runner_token_confirmed, runner_token_gen,
				last_acked_seq, claim_deadline, last_resumed_at, last_heartbeat_at,
				pending_actions, next_reconcile_at, lease_owner, lease_expires_at
			) VALUES (
				$1, $2, $3, $4,
				$5, $6, $7,
				$8, $9, $10, $11,
				$12, $13,
				$14, $15, $16,
				$17, $18, $19, $20,
				$21, $22, $23, $24
			)`,
			key.Atespace, key.Name, string(dataBytes), now,
			int16(written.Task.GetStatus().GetState()), written.Generation, written.Task.GetSpec().GetSessionId(),
			written.ActorUID, written.ActiveRunID, written.RunSeq, written.ActionSeq,
			nullableJSON(effProfileBytes), string(egressBytes),
			tokenHashArg, written.RunnerTokenConfirmed, written.RunnerTokenGen,
			written.LastAckedRunnerSeq, written.ClaimDeadline, written.LastResumedAt, written.LastHeartbeatAt,
			string(actionsBytes), written.NextReconcileAt, leaseOwnerArg, written.LeaseExpiresAt,
		)
		if err != nil {
			if isUniqueViolation(err) {
				return nil, false, true, nil
			}
			return nil, false, false, classifyTxError(ctx, err)
		}
	} else {
		// Omit rewriting the potentially large TOASTed data JSONB column when
		// nothing on the Task protobuf changed other than Status.Generation.
		taskDocUnchanged := taskEqualAtGeneration(stored.Task, written.Task, written.Generation)
		if taskDocUnchanged {
			_, err = tx.Exec(ctx, `
				UPDATE tasks SET
					state                  = $3,
					generation             = $4,
					session_id             = $5,
					actor_uid              = $6,
					active_run_id          = $7,
					run_seq                = $8,
					action_seq             = $9,
					effective_profile      = $10,
					egress_rules           = $11,
					runner_token_hash      = $12,
					runner_token_confirmed = $13,
					runner_token_gen       = $14,
					last_acked_seq         = $15,
					claim_deadline         = $16,
					last_resumed_at        = $17,
					last_heartbeat_at      = $18,
					pending_actions        = $19,
					next_reconcile_at      = $20,
					lease_owner            = $21,
					lease_expires_at       = $22
				WHERE atespace = $1 AND name = $2`,
				key.Atespace, key.Name,
				int16(written.Task.GetStatus().GetState()),
				written.Generation,
				written.Task.GetSpec().GetSessionId(),
				written.ActorUID,
				written.ActiveRunID,
				written.RunSeq,
				written.ActionSeq,
				nullableJSON(effProfileBytes),
				string(egressBytes),
				tokenHashArg,
				written.RunnerTokenConfirmed,
				written.RunnerTokenGen,
				written.LastAckedRunnerSeq,
				written.ClaimDeadline,
				written.LastResumedAt,
				written.LastHeartbeatAt,
				string(actionsBytes),
				written.NextReconcileAt,
				leaseOwnerArg,
				written.LeaseExpiresAt,
			)
		} else {
			dataBytes, marshalErr := protojson.Marshal(written.Task)
			if marshalErr != nil {
				return nil, false, false, fmt.Errorf("marshaling task: %w", marshalErr)
			}
			_, err = tx.Exec(ctx, `
				UPDATE tasks SET
					data                   = $3,
					state                  = $4,
					generation             = $5,
					session_id             = $6,
					actor_uid              = $7,
					active_run_id          = $8,
					run_seq                = $9,
					action_seq             = $10,
					effective_profile      = $11,
					egress_rules           = $12,
					runner_token_hash      = $13,
					runner_token_confirmed = $14,
					runner_token_gen       = $15,
					last_acked_seq         = $16,
					claim_deadline         = $17,
					last_resumed_at        = $18,
					last_heartbeat_at      = $19,
					pending_actions        = $20,
					next_reconcile_at      = $21,
					lease_owner            = $22,
					lease_expires_at       = $23
				WHERE atespace = $1 AND name = $2`,
				key.Atespace, key.Name,
				string(dataBytes),
				int16(written.Task.GetStatus().GetState()),
				written.Generation,
				written.Task.GetSpec().GetSessionId(),
				written.ActorUID,
				written.ActiveRunID,
				written.RunSeq,
				written.ActionSeq,
				nullableJSON(effProfileBytes),
				string(egressBytes),
				tokenHashArg,
				written.RunnerTokenConfirmed,
				written.RunnerTokenGen,
				written.LastAckedRunnerSeq,
				written.ClaimDeadline,
				written.LastResumedAt,
				written.LastHeartbeatAt,
				string(actionsBytes),
				written.NextReconcileAt,
				leaseOwnerArg,
				written.LeaseExpiresAt,
			)
		}
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

func nullableJSON(b []byte) any {
	if len(b) == 0 {
		return nil
	}
	return string(b)
}

func taskEqualAtGeneration(stored, next *v1alpha1.Task, nextGen int64) bool {
	if stored == nil || next == nil {
		return false
	}
	cmp := proto.Clone(stored).(*v1alpha1.Task)
	if cmp.Status == nil {
		cmp.Status = &v1alpha1.TaskStatus{}
	}
	cmp.Status.Generation = nextGen
	return proto.Equal(cmp, next)
}

// LeaseDue atomically claims up to limit due rows using FOR UPDATE SKIP LOCKED
// against clock_timestamp() and advances both lease_expires_at and
// next_reconcile_at to now + leaseTTL, skipping saturated atespaces and
// saturated runnable action kinds during the streaming index scan.
func (s *Store) LeaseDue(ctx context.Context, workerID string, leaseTTL time.Duration, limit int, filters ...store.LeaseFilter) ([]store.LeasedTask, error) {
	if limit <= 0 {
		return nil, nil
	}
	if leaseTTL <= 0 {
		leaseTTL = 30 * time.Second
	}
	ttlMs := max(leaseTTL.Milliseconds(), 1)
	filter := store.MergeLeaseFilters(filters)
	excludedAtespaces := make([]string, 0, len(filter.ExcludeAtespaces))
	for _, ns := range filter.ExcludeAtespaces {
		excludedAtespaces = append(excludedAtespaces, defaultAtespace(ns))
	}
	excludedKinds := make([]string, 0, len(filter.ExcludeActionKinds))
	for _, k := range filter.ExcludeActionKinds {
		excludedKinds = append(excludedKinds, string(k))
	}
	bucketedKinds := make([]string, 0, len(filter.BucketedActionKinds))
	for _, k := range filter.BucketedActionKinds {
		bucketedKinds = append(bucketedKinds, string(k))
	}

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

	// Passing now as a bound parameter ($1) and joining on the primary key lets
	// PostgreSQL execute a streaming Limit -> LockRows (SKIP LOCKED) -> Index
	// Scan on tasks_queue_idx with zero Sort while following concurrent row
	// updates cleanly under READ COMMITTED EvalPlanQual.
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
			  AND (cardinality($5::text[]) = 0 OR NOT (tasks.atespace = ANY($5::text[])))
			  AND (
			      (cardinality($6::text[]) = 0 AND NOT $7::boolean)
			      OR jsonb_array_length(tasks.pending_actions) = 0
			      OR ((tasks.pending_actions->0 ? 'notBefore') AND (tasks.pending_actions->0->>'notBefore')::timestamptz > $1)
			      OR (
			          (cardinality($6::text[]) = 0 OR NOT ((tasks.pending_actions->0->>'kind') = ANY($6::text[])))
			          AND (NOT $7::boolean OR (tasks.pending_actions->0->>'kind') = ANY($8::text[]))
			      )
			  )
			ORDER BY tasks.next_reconcile_at ASC, tasks.atespace ASC, tasks.name ASC
			LIMIT $2
			FOR UPDATE OF tasks SKIP LOCKED
		) AS due
		WHERE t.atespace = due.atespace AND t.name = due.name
		RETURNING `+qualifiedTaskRowColumns("t"),
		now, limit, workerID, exp, excludedAtespaces, excludedKinds, filter.ExcludeOtherActionKinds, bucketedKinds,
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
		var rowExp time.Time
		if tr.LeaseExpiresAt != nil {
			rowExp = *tr.LeaseExpiresAt
		}
		leased = append(leased, store.LeasedTask{
			Key: store.TaskKey{
				Atespace: tr.Task.GetMetadata().GetAtespace(),
				Name:     tr.Task.GetMetadata().GetName(),
			}.Normalized(),
			LeaseToken: tr.LeaseOwner,
			LeasedAt:   now,
			ExpiresAt:  rowExp,
			Row:        tr,
		})
	}
	rowsErr := rows.Err()
	rows.Close()
	if rowsErr != nil {
		return nil, classifyTxError(ctx, rowsErr)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, classifyTxError(ctx, err)
	}
	return leased, nil
}

// LeaseTask acquires a worker lease on key when due, waiting up to
// ApplyLockTimeout on short concurrent Apply transactions and sampling
// clock_timestamp() after acquiring the row lock so inline callers never miss
// newly committed actions. When the row is not yet due, contended is true if
// and only if another worker holds an unexpired lease on due work (runnable
// actions or an expired timer) for key, and retryIn reports the remaining
// store-clock wait on an unleased row with pending actions.
func (s *Store) LeaseTask(ctx context.Context, key store.TaskKey, workerID string, leaseTTL time.Duration) (*store.LeasedTask, bool, time.Duration, error) {
	if leaseTTL <= 0 {
		leaseTTL = 30 * time.Second
	}
	ttlMs := max(leaseTTL.Milliseconds(), 1)
	key = key.Normalized()

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, false, 0, classifyTxError(ctx, err)
	}
	defer rollbackTx(tx)

	if err := s.setApplyTimeouts(ctx, tx); err != nil {
		return nil, false, 0, err
	}

	stored, err := scanTaskRow(tx.QueryRow(ctx,
		`SELECT `+taskRowColumns+` FROM tasks WHERE atespace = $1 AND name = $2 FOR UPDATE`,
		key.Atespace, key.Name,
	))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, 0, nil
	}
	if err != nil {
		return nil, false, 0, classifyTxError(ctx, err)
	}

	now, err := queryClockTimestamp(ctx, tx)
	if err != nil {
		return nil, false, 0, err
	}
	if stored.NextReconcileAt == nil {
		return nil, false, 0, nil
	}
	if stored.NextReconcileAt.After(now) {
		activeLease := stored.LeaseOwner != "" && stored.LeaseExpiresAt != nil && stored.LeaseExpiresAt.After(now)
		unclamped := lifecycle.NextReconcileAt(stored.LifecycleState(), now)
		dueNow := unclamped != nil && !unclamped.After(now)
		var retryIn time.Duration
		if !activeLease && len(stored.Actions) > 0 {
			retryIn = stored.NextReconcileAt.Sub(now)
		}
		return nil, activeLease && dueNow, retryIn, nil
	}

	exp := now.Add(time.Duration(ttlMs) * time.Millisecond)
	tr, err := scanTaskRow(tx.QueryRow(ctx, `
		UPDATE tasks t
		SET lease_owner       = $3 || ':' || gen_random_uuid()::text,
		    lease_expires_at  = $4,
		    next_reconcile_at = $4
		WHERE t.atespace = $1 AND t.name = $2
		RETURNING `+qualifiedTaskRowColumns("t"),
		key.Atespace, key.Name, workerID, exp,
	))
	if err != nil {
		return nil, false, 0, classifyTxError(ctx, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, false, 0, classifyTxError(ctx, err)
	}
	return &store.LeasedTask{
		Key:        key,
		LeaseToken: tr.LeaseOwner,
		LeasedAt:   now,
		ExpiresAt:  exp,
		Row:        tr,
	}, false, 0, nil
}

func qualifiedTaskRowColumns(alias string) string {
	return fmt.Sprintf(`
		%[1]s.atespace, %[1]s.name, %[1]s.data, %[1]s.state, %[1]s.generation, %[1]s.session_id,
		%[1]s.actor_uid, %[1]s.active_run_id, %[1]s.run_seq, %[1]s.action_seq,
		%[1]s.effective_profile, %[1]s.egress_rules,
		%[1]s.runner_token_hash, %[1]s.runner_token_confirmed, %[1]s.runner_token_gen,
		%[1]s.last_acked_seq, %[1]s.claim_deadline, %[1]s.last_resumed_at, %[1]s.last_heartbeat_at,
		%[1]s.pending_actions, %[1]s.next_reconcile_at, %[1]s.lease_owner, %[1]s.lease_expires_at`, alias)
}

// RenewLease locks key's row, verifies against post-lock clock_timestamp() that
// leaseToken is still active, extends the lease, and returns the current head
// Action.ID.
func (s *Store) RenewLease(ctx context.Context, key store.TaskKey, leaseToken string, leaseTTL time.Duration) (string, error) {
	if leaseToken == "" {
		return "", store.ErrLeaseLost
	}
	if leaseTTL <= 0 {
		leaseTTL = 30 * time.Second
	}
	ttlMs := max(leaseTTL.Milliseconds(), 1)
	key = key.Normalized()

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return "", classifyTxError(ctx, err)
	}
	defer rollbackTx(tx)

	if err := s.setApplyTimeouts(ctx, tx); err != nil {
		return "", err
	}

	var (
		leaseOwner         *string
		leaseExpiresAt     *time.Time
		pendingActionsJSON []byte
	)
	err = tx.QueryRow(ctx, `
		SELECT lease_owner, lease_expires_at, pending_actions
		FROM tasks
		WHERE atespace = $1 AND name = $2
		FOR UPDATE`,
		key.Atespace, key.Name,
	).Scan(&leaseOwner, &leaseExpiresAt, &pendingActionsJSON)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", store.ErrLeaseLost
	}
	if err != nil {
		return "", classifyTxError(ctx, err)
	}

	now, err := queryClockTimestamp(ctx, tx)
	if err != nil {
		return "", err
	}
	if leaseOwner == nil || *leaseOwner != leaseToken || leaseExpiresAt == nil || !leaseExpiresAt.After(now) {
		return "", store.ErrLeaseLost
	}

	exp := now.Add(time.Duration(ttlMs) * time.Millisecond)
	if _, err := tx.Exec(ctx, `
		UPDATE tasks
		SET lease_expires_at  = $3,
		    next_reconcile_at = CASE
		        WHEN next_reconcile_at IS NULL THEN NULL
		        ELSE GREATEST(next_reconcile_at, $3)
		    END
		WHERE atespace = $1 AND name = $2`,
		key.Atespace, key.Name, exp,
	); err != nil {
		return "", classifyTxError(ctx, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return "", classifyTxError(ctx, err)
	}

	var actions []lifecycle.Action
	if len(pendingActionsJSON) > 0 && string(pendingActionsJSON) != "null" {
		if err := json.Unmarshal(pendingActionsJSON, &actions); err != nil {
			return "", fmt.Errorf("unmarshaling pending_actions on lease renewal: %w", err)
		}
	}
	if len(actions) > 0 {
		return actions[0].ID, nil
	}
	return "", nil
}

// ReleaseLease clears an active worker lease and recomputes next_reconcile_at
// without rewriting the Task document or incrementing Generation.
func (s *Store) ReleaseLease(ctx context.Context, key store.TaskKey, leaseToken string, requeueAfter time.Duration) error {
	if leaseToken == "" {
		return store.ErrLeaseLost
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
		return store.ErrLeaseLost
	}
	if err != nil {
		return classifyTxError(ctx, err)
	}

	now, err := queryClockTimestamp(ctx, tx)
	if err != nil {
		return err
	}
	if !store.HasActiveLease(stored, leaseToken, now) {
		return store.ErrLeaseLost
	}

	due := lifecycle.NextReconcileAt(stored.LifecycleState(), now)
	if due != nil && requeueAfter > 0 {
		delayed := now.Add(requeueAfter).UTC().Truncate(time.Microsecond)
		if due.Before(delayed) {
			due = &delayed
		}
	}

	if _, err := tx.Exec(ctx, `
		UPDATE tasks
		SET lease_owner       = NULL,
		    lease_expires_at  = NULL,
		    next_reconcile_at = $3
		WHERE atespace = $1 AND name = $2`,
		key.Atespace, key.Name, due,
	); err != nil {
		return classifyTxError(ctx, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return classifyTxError(ctx, err)
	}
	if due != nil && !due.After(now) {
		s.notifyWakeup()
	}
	return nil
}

// BackoffAction records a transient retry delay on actionID (when still at head)
// and clears the worker lease without touching the Task document or Generation.
func (s *Store) BackoffAction(ctx context.Context, key store.TaskKey, leaseToken, actionID string, backoff time.Duration) error {
	if leaseToken == "" {
		return store.ErrLeaseLost
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
		return store.ErrLeaseLost
	}
	if err != nil {
		return classifyTxError(ctx, err)
	}

	now, err := queryClockTimestamp(ctx, tx)
	if err != nil {
		return err
	}
	if !store.HasActiveLease(stored, leaseToken, now) {
		return store.ErrLeaseLost
	}

	st := stored.LifecycleState()
	if len(st.Actions) > 0 && st.Actions[0].ID == actionID {
		st.Actions[0].Tries++
		if backoff > 0 {
			st.Actions[0].NotBefore = now.Add(backoff).UTC().Truncate(time.Microsecond)
		} else {
			st.Actions[0].NotBefore = now.UTC().Truncate(time.Microsecond)
		}
	}
	actionsBytes, err := marshalPendingActions(st.Actions)
	if err != nil {
		return fmt.Errorf("marshaling pending_actions: %w", err)
	}
	due := lifecycle.NextReconcileAt(st, now)

	if _, err := tx.Exec(ctx, `
		UPDATE tasks
		SET pending_actions   = $3,
		    lease_owner       = NULL,
		    lease_expires_at  = NULL,
		    next_reconcile_at = $4
		WHERE atespace = $1 AND name = $2`,
		key.Atespace, key.Name, string(actionsBytes), due,
	); err != nil {
		return classifyTxError(ctx, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return classifyTxError(ctx, err)
	}
	if due != nil && !due.After(now) {
		s.notifyWakeup()
	}
	return nil
}
