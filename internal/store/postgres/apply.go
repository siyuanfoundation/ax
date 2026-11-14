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
	runnerv1 "github.com/google/ax/pkg/apis/runner/v1"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const (
	maxCreateContentionRetries = 3

	taskRowColumns = `
	data,
	generation,
	session_id,
	actor_uid,
	active_run_id,
	run_seq,
	action_seq,
	effective_profile,
	egress_rules,
	runner_token_hash,
	runner_token_confirmed,
	runner_token_gen,
	last_acked_seq,
	last_claim_nonce,
	last_claim_response,
	last_heartbeat_at,
	last_resumed_at,
	claim_deadline,
	pending_actions,
	next_reconcile_at,
	lease_owner,
	lease_expires_at`
)

// GetTaskRow loads the full TaskRow for key without acquiring a row lock.
func (s *Store) GetTaskRow(ctx context.Context, key store.TaskKey) (*store.TaskRow, error) {
	key = key.Normalized()
	row, err := scanTaskRow(s.pool.QueryRow(ctx,
		`SELECT `+taskRowColumns+` FROM tasks WHERE atespace = $1 AND name = $2`,
		key.Atespace, key.Name))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, store.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("getting task row %s/%s: %w", key.Atespace, key.Name, err)
	}
	return row, nil
}

// Apply executes fn under a short row-level lock (2s lock_timeout, 5s
// statement_timeout) using PostgreSQL's transaction clock now().
func (s *Store) Apply(ctx context.Context, key store.TaskKey, fn store.ApplyFunc) (*store.TaskRow, error) {
	key = key.Normalized()
	if key.Name == "" {
		return nil, errors.New("task name is required")
	}

	var (
		committed *store.TaskRow
		due       bool
	)

	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SET LOCAL lock_timeout = '2s'`); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `SET LOCAL statement_timeout = '5s'`); err != nil {
			return err
		}

		var dbNow time.Time
		if err := tx.QueryRow(ctx, `SELECT now()`).Scan(&dbNow); err != nil {
			return fmt.Errorf("reading database clock: %w", err)
		}

		var current *store.TaskRow
		for attempt := range maxCreateContentionRetries {
			var err error
			current, err = scanTaskRow(tx.QueryRow(ctx,
				`SELECT `+taskRowColumns+` FROM tasks WHERE atespace = $1 AND name = $2 FOR UPDATE`,
				key.Atespace, key.Name))
			if err == nil {
				break
			}
			if !errors.Is(err, pgx.ErrNoRows) {
				return err
			}

			next, fnErr := fn(nil, dbNow)
			if fnErr != nil || next == nil {
				committed = nil
				due = false
				return fnErr
			}
			insertRow, prepErr := prepareTaskRowForWrite(key, next, 1, dbNow)
			if prepErr != nil {
				return prepErr
			}

			// Guard against concurrent CreateTask calls racing when no row existed yet.
			sp, spErr := tx.Begin(ctx)
			if spErr != nil {
				return spErr
			}
			insErr := execInsertTaskRow(ctx, sp, key, insertRow, dbNow)
			if insErr == nil {
				if err := sp.Commit(ctx); err != nil {
					return err
				}
				committed = insertRow.Clone()
				due = isImmediatelyDue(insertRow, dbNow)
				return nil
			}
			_ = sp.Rollback(ctx)
			if !isUniqueViolation(insErr) {
				return insErr
			}
			if attempt == maxCreateContentionRetries-1 {
				return insErr
			}
		}

		next, fnErr := fn(current, dbNow)
		if fnErr != nil {
			return fnErr
		}
		if next == nil {
			if _, err := tx.Exec(ctx, `DELETE FROM tasks WHERE atespace = $1 AND name = $2`, key.Atespace, key.Name); err != nil {
				return fmt.Errorf("deleting task row: %w", err)
			}
			committed = nil
			due = false
			return nil
		}
		if next == current {
			committed = current.Clone()
			due = false
			return nil
		}

		nextGen := max(current.Generation, int64(1)) + 1
		updateRow, prepErr := prepareTaskRowForWrite(key, next, nextGen, dbNow)
		if prepErr != nil {
			return prepErr
		}
		skipData := taskDocumentUnchanged(current.Task, updateRow.Task)
		if err := execUpdateTaskRow(ctx, tx, key, updateRow, skipData); err != nil {
			return err
		}
		committed = updateRow.Clone()
		due = isImmediatelyDue(updateRow, dbNow)
		return nil
	})
	if err != nil {
		return nil, wrapLockTimeout(err)
	}
	if due {
		s.notifyDue()
	}
	return committed, nil
}

// taskDocumentUnchanged reports whether a and b have identical persisted fields
// once Status.Generation (which lives in its own column and is merged on read)
// is normalized, allowing Apply to skip rewriting large TOASTed data JSONB on
// queue/runtime-only updates.
func taskDocumentUnchanged(a, b *v1alpha1.Task) bool {
	if a == nil || b == nil {
		return false
	}
	ca := proto.Clone(a).(*v1alpha1.Task)
	cb := proto.Clone(b).(*v1alpha1.Task)
	store.ScrubStoredTask(ca)
	store.ScrubStoredTask(cb)
	if ca.Status != nil {
		ca.Status.Generation = 0
	}
	if cb.Status != nil {
		cb.Status.Generation = 0
	}
	return proto.Equal(ca, cb)
}

func prepareTaskRowForWrite(key store.TaskKey, in *store.TaskRow, gen int64, now time.Time) (*store.TaskRow, error) {
	out := in.Clone()
	if out.Task == nil {
		return nil, errors.New("task row requires non-nil Task")
	}
	if out.Task.Metadata == nil {
		out.Task.Metadata = &v1alpha1.ObjectMeta{}
	}
	out.Task.Metadata.Atespace = key.Atespace
	out.Task.Metadata.Name = key.Name
	if err := normalizeTaskForStore(out.Task); err != nil {
		return nil, err
	}
	store.ScrubStoredTask(out.Task)
	store.ScrubClaimResponse(out.LastClaimResponse)
	out.Generation = gen
	out.Task.Status.Generation = gen
	out.SessionID = out.Task.GetSpec().GetSessionId()

	// Reconcile lease state: clear expired leases, or clamp next_reconcile_at
	// to at least lease_expires_at while a live lease is held.
	if out.LeaseOwner != "" && out.LeaseExpiresAt != nil && out.LeaseExpiresAt.After(now) {
		if out.NextReconcileAt == nil || out.NextReconcileAt.Before(*out.LeaseExpiresAt) {
			exp := *out.LeaseExpiresAt
			out.NextReconcileAt = &exp
		}
	} else {
		out.LeaseOwner = ""
		out.LeaseExpiresAt = nil
	}
	return out, nil
}

func isImmediatelyDue(row *store.TaskRow, now time.Time) bool {
	return row.LeaseOwner == "" && row.NextReconcileAt != nil && !row.NextReconcileAt.After(now)
}

type serializedTaskRow struct {
	data              string
	state             int16
	effectiveProfile  *string
	egressRules       string
	lastClaimResponse *string
	pendingActions    string
	leaseOwner        *string
}

func serializeTaskRow(row *store.TaskRow, includeData bool) (*serializedTaskRow, error) {
	var dataStr string
	if includeData {
		dataBytes, err := protojson.Marshal(row.Task)
		if err != nil {
			return nil, fmt.Errorf("marshaling task: %w", err)
		}
		dataStr = string(dataBytes)
	}

	var effProfile *string
	if row.EffectiveProfile != nil {
		b, err := protojson.Marshal(row.EffectiveProfile)
		if err != nil {
			return nil, fmt.Errorf("marshaling effective_profile: %w", err)
		}
		s := string(b)
		effProfile = &s
	}

	egressJSON, err := marshalEgressRules(row.EgressRules)
	if err != nil {
		return nil, err
	}

	var claimResp *string
	if row.LastClaimResponse != nil {
		b, err := protojson.Marshal(row.LastClaimResponse)
		if err != nil {
			return nil, fmt.Errorf("marshaling last_claim_response: %w", err)
		}
		s := string(b)
		claimResp = &s
	}

	actions := row.PendingActions
	if actions == nil {
		actions = []lifecycle.Action{}
	}
	actionsBytes, err := json.Marshal(actions)
	if err != nil {
		return nil, fmt.Errorf("marshaling pending_actions: %w", err)
	}

	var leaseOwner *string
	if row.LeaseOwner != "" {
		owner := row.LeaseOwner
		leaseOwner = &owner
	}

	return &serializedTaskRow{
		data:              dataStr,
		state:             int16(row.Task.GetStatus().GetState()),
		effectiveProfile:  effProfile,
		egressRules:       egressJSON,
		lastClaimResponse: claimResp,
		pendingActions:    string(actionsBytes),
		leaseOwner:        leaseOwner,
	}, nil
}

func execInsertTaskRow(ctx context.Context, tx pgx.Tx, key store.TaskKey, row *store.TaskRow, savedAt time.Time) error {
	ser, err := serializeTaskRow(row, true)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO tasks (
			atespace, name, data, saved_at,
			state, generation, session_id, actor_uid, active_run_id,
			run_seq, action_seq, effective_profile, egress_rules,
			runner_token_hash, runner_token_confirmed, runner_token_gen, last_acked_seq,
			last_claim_nonce, last_claim_response,
			last_heartbeat_at, last_resumed_at, claim_deadline,
			pending_actions, next_reconcile_at, lease_owner, lease_expires_at
		) VALUES (
			$1, $2, $3, $4,
			$5, $6, $7, $8, $9,
			$10, $11, $12, $13,
			$14, $15, $16, $17,
			$18, $19,
			$20, $21, $22,
			$23, $24, $25, $26
		)`,
		key.Atespace, key.Name, ser.data, savedAt,
		ser.state, row.Generation, row.SessionID, row.ActorUID, row.ActiveRunID,
		row.RunSeq, row.ActionSeq, ser.effectiveProfile, ser.egressRules,
		nullableBytes(row.RunnerTokenHash), row.RunnerTokenConfirmed, row.RunnerTokenGen, row.LastAckedSeq,
		row.LastClaimNonce, ser.lastClaimResponse,
		row.LastHeartbeatAt, row.LastResumedAt, row.ClaimDeadline,
		ser.pendingActions, row.NextReconcileAt, ser.leaseOwner, row.LeaseExpiresAt,
	)
	if err != nil {
		return fmt.Errorf("inserting task row: %w", err)
	}
	return nil
}

func execUpdateTaskRow(ctx context.Context, tx pgx.Tx, key store.TaskKey, row *store.TaskRow, skipData bool) error {
	ser, err := serializeTaskRow(row, !skipData)
	if err != nil {
		return err
	}
	if skipData {
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
				last_claim_nonce       = $16,
				last_claim_response    = $17,
				last_heartbeat_at      = $18,
				last_resumed_at        = $19,
				claim_deadline         = $20,
				pending_actions        = $21,
				next_reconcile_at      = $22,
				lease_owner            = $23,
				lease_expires_at       = $24
			WHERE atespace = $1 AND name = $2`,
			key.Atespace, key.Name,
			ser.state,
			row.Generation,
			row.SessionID,
			row.ActorUID,
			row.ActiveRunID,
			row.RunSeq,
			row.ActionSeq,
			ser.effectiveProfile,
			ser.egressRules,
			nullableBytes(row.RunnerTokenHash),
			row.RunnerTokenConfirmed,
			row.RunnerTokenGen,
			row.LastAckedSeq,
			row.LastClaimNonce,
			ser.lastClaimResponse,
			row.LastHeartbeatAt,
			row.LastResumedAt,
			row.ClaimDeadline,
			ser.pendingActions,
			row.NextReconcileAt,
			ser.leaseOwner,
			row.LeaseExpiresAt,
		)
	} else {
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
				last_claim_nonce       = $17,
				last_claim_response    = $18,
				last_heartbeat_at      = $19,
				last_resumed_at        = $20,
				claim_deadline         = $21,
				pending_actions        = $22,
				next_reconcile_at      = $23,
				lease_owner            = $24,
				lease_expires_at       = $25
			WHERE atespace = $1 AND name = $2`,
			key.Atespace, key.Name,
			ser.data,
			ser.state,
			row.Generation,
			row.SessionID,
			row.ActorUID,
			row.ActiveRunID,
			row.RunSeq,
			row.ActionSeq,
			ser.effectiveProfile,
			ser.egressRules,
			nullableBytes(row.RunnerTokenHash),
			row.RunnerTokenConfirmed,
			row.RunnerTokenGen,
			row.LastAckedSeq,
			row.LastClaimNonce,
			ser.lastClaimResponse,
			row.LastHeartbeatAt,
			row.LastResumedAt,
			row.ClaimDeadline,
			ser.pendingActions,
			row.NextReconcileAt,
			ser.leaseOwner,
			row.LeaseExpiresAt,
		)
	}
	if err != nil {
		return fmt.Errorf("updating task row: %w", err)
	}
	return nil
}

func scanTaskRow(r pgx.Row) (*store.TaskRow, error) {
	var (
		data             []byte
		row              store.TaskRow
		effProfileBytes  []byte
		egressBytes      []byte
		claimRespBytes   []byte
		pendingBytes     []byte
		nullableLeaseOwn *string
	)
	err := r.Scan(
		&data,
		&row.Generation,
		&row.SessionID,
		&row.ActorUID,
		&row.ActiveRunID,
		&row.RunSeq,
		&row.ActionSeq,
		&effProfileBytes,
		&egressBytes,
		&row.RunnerTokenHash,
		&row.RunnerTokenConfirmed,
		&row.RunnerTokenGen,
		&row.LastAckedSeq,
		&row.LastClaimNonce,
		&claimRespBytes,
		&row.LastHeartbeatAt,
		&row.LastResumedAt,
		&row.ClaimDeadline,
		&pendingBytes,
		&row.NextReconcileAt,
		&nullableLeaseOwn,
		&row.LeaseExpiresAt,
	)
	if err != nil {
		return nil, err
	}

	var task v1alpha1.Task
	if err := unmarshalOpts.Unmarshal(data, &task); err != nil {
		return nil, fmt.Errorf("unmarshaling task: %w", err)
	}
	if task.Status == nil {
		task.Status = &v1alpha1.TaskStatus{}
	}
	store.ScrubStoredTask(&task)
	if row.Generation > 0 {
		task.Status.Generation = row.Generation
	}
	row.Task = &task

	if len(effProfileBytes) > 0 {
		var prof v1alpha1.Profile
		if err := unmarshalOpts.Unmarshal(effProfileBytes, &prof); err != nil {
			return nil, fmt.Errorf("unmarshaling effective_profile: %w", err)
		}
		row.EffectiveProfile = &prof
	}
	if len(egressBytes) > 0 {
		rules, err := unmarshalEgressRules(egressBytes)
		if err != nil {
			return nil, err
		}
		row.EgressRules = rules
	}
	if len(claimRespBytes) > 0 {
		var resp runnerv1.ClaimAssignmentResponse
		if err := unmarshalOpts.Unmarshal(claimRespBytes, &resp); err != nil {
			return nil, fmt.Errorf("unmarshaling last_claim_response: %w", err)
		}
		row.LastClaimResponse = &resp
	}
	if len(pendingBytes) > 0 {
		if err := json.Unmarshal(pendingBytes, &row.PendingActions); err != nil {
			return nil, fmt.Errorf("unmarshaling pending_actions: %w", err)
		}
	}
	if nullableLeaseOwn != nil {
		row.LeaseOwner = *nullableLeaseOwn
	}
	return &row, nil
}

func marshalEgressRules(rules []*ateapipb.EgressRule) (string, error) {
	if len(rules) == 0 {
		return "[]", nil
	}
	raw := make([]json.RawMessage, 0, len(rules))
	for _, r := range rules {
		if r == nil {
			continue
		}
		b, err := protojson.Marshal(r)
		if err != nil {
			return "", fmt.Errorf("marshaling egress rule: %w", err)
		}
		raw = append(raw, json.RawMessage(b))
	}
	out, err := json.Marshal(raw)
	if err != nil {
		return "", fmt.Errorf("marshaling egress_rules array: %w", err)
	}
	return string(out), nil
}

func unmarshalEgressRules(data []byte) ([]*ateapipb.EgressRule, error) {
	var raw []json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("unmarshaling egress_rules array: %w", err)
	}
	if len(raw) == 0 {
		return nil, nil
	}
	out := make([]*ateapipb.EgressRule, 0, len(raw))
	for _, item := range raw {
		var rule ateapipb.EgressRule
		if err := unmarshalOpts.Unmarshal(item, &rule); err != nil {
			return nil, fmt.Errorf("unmarshaling egress rule: %w", err)
		}
		out = append(out, &rule)
	}
	return out, nil
}

func nullableBytes(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	return b
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

func wrapLockTimeout(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && (pgErr.Code == "55P03" || pgErr.Code == "57014") {
		return fmt.Errorf("%w: %v", store.ErrLockTimeout, err)
	}
	return err
}
