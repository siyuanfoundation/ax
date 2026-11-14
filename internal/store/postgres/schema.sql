-- Resources are stored as protojson documents. Only the columns needed to
-- address and order them are broken out, so adding fields to the API does not
-- require a schema change. saved_at changes on every Save but not on status
-- updates; lists are ordered by it, newest first.

CREATE TABLE IF NOT EXISTS tasks (
    atespace text        NOT NULL,
    name     text        NOT NULL,
    data     jsonb       NOT NULL,
    saved_at timestamptz NOT NULL,
    PRIMARY KEY (atespace, name)
);
CREATE INDEX IF NOT EXISTS tasks_saved_at_idx ON tasks (saved_at DESC, atespace, name);
CREATE INDEX IF NOT EXISTS tasks_atespace_saved_at_idx ON tasks (atespace, saved_at DESC, name);

-- Task queue, runner protocol, and drift-sweep columns.
-- Hot liveness/lease columns (last_heartbeat_at, lease_owner, lease_expires_at)
-- remain unindexed so frequent updates qualify as PostgreSQL HOT updates.
--
-- The columns are added once, guarded by a catalog check, for two reasons.
-- First, ALTER TABLE ... ADD COLUMN acquires an ACCESS EXCLUSIVE lock on tasks
-- even with IF NOT EXISTS, so running it on every startup would stall task
-- reads behind any open transaction. Second, rows written before these columns
-- existed store state, generation, and session_id only inside the data JSONB
-- document, so they are backfilled in the same transaction: generation is
-- merged onto status.generation on read and must never move backward, while
-- state and session_id back tasks_active_idx and tasks_session_idx.
--
-- Because this block is skipped once the generation column exists, do not
-- append future columns here; add a new guarded block instead (or introduce
-- versioned migrations; see schema.go).
--
-- TODO: Replace this one-time block with versioned migrations to address two
-- limitations:
--   - Backfilling rewrites every existing task row within ax-server's
--     one-minute store-open timeout, which could time out on tables with
--     hundreds of thousands of rows.
--   - Rolling back to an older binary that writes only the data JSONB leaves
--     these extracted columns stale on any rows it touches, because this
--     backfill runs only once.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = current_schema() AND table_name = 'tasks' AND column_name = 'generation'
    ) THEN
        RETURN;
    END IF;

    ALTER TABLE tasks
        ADD COLUMN state                  smallint    NOT NULL DEFAULT 0,
        ADD COLUMN generation             bigint      NOT NULL DEFAULT 1,
        ADD COLUMN session_id             text        NOT NULL DEFAULT '',
        ADD COLUMN actor_uid              text        NOT NULL DEFAULT '',
        ADD COLUMN active_run_id          text        NOT NULL DEFAULT '',
        ADD COLUMN run_seq                bigint      NOT NULL DEFAULT 0,
        ADD COLUMN action_seq             bigint      NOT NULL DEFAULT 0,
        ADD COLUMN effective_profile      jsonb,
        ADD COLUMN egress_rules           jsonb       NOT NULL DEFAULT '[]',
        ADD COLUMN runner_token_hash      bytea,
        ADD COLUMN runner_token_confirmed boolean     NOT NULL DEFAULT false,
        ADD COLUMN runner_token_gen       integer     NOT NULL DEFAULT 0,
        ADD COLUMN last_acked_seq         bigint      NOT NULL DEFAULT 0,
        ADD COLUMN last_claim_nonce       text        NOT NULL DEFAULT '',
        ADD COLUMN last_claim_response    jsonb,
        ADD COLUMN last_heartbeat_at      timestamptz,
        ADD COLUMN last_resumed_at        timestamptz,
        ADD COLUMN claim_deadline         timestamptz,
        ADD COLUMN pending_actions        jsonb       NOT NULL DEFAULT '[]',
        ADD COLUMN next_reconcile_at      timestamptz,
        ADD COLUMN lease_owner            text,
        ADD COLUMN lease_expires_at       timestamptz;

    -- protojson writes int64 as a decimal string and enums by name.
    UPDATE tasks SET
        generation = CASE
            WHEN data->'status'->>'generation' ~ '^[0-9]{1,18}$'
            THEN GREATEST((data->'status'->>'generation')::bigint, 1)
            ELSE 1
        END,
        session_id = COALESCE(data->'spec'->>'sessionId', ''),
        state = CASE data->'status'->>'state'
            WHEN 'TASK_STATE_IDLE'            THEN 1
            WHEN 'TASK_STATE_WORKING'         THEN 2
            WHEN 'TASK_STATE_REQUIRES_ACTION' THEN 3
            WHEN 'TASK_STATE_COMPLETED'       THEN 4
            WHEN 'TASK_STATE_FAILED'          THEN 5
            WHEN 'TASK_STATE_CANCELLED'       THEN 6
            WHEN 'TASK_STATE_QUEUED'          THEN 7
            WHEN 'TASK_STATE_SUSPENDED'       THEN 8
            WHEN 'TASK_STATE_SUSPENDING'      THEN 9
            ELSE 0
        END;

    ALTER TABLE tasks SET (
        fillfactor = 70,
        autovacuum_vacuum_scale_factor = 0.02,
        autovacuum_analyze_scale_factor = 0.02
    );
END
$$;

CREATE INDEX IF NOT EXISTS tasks_queue_idx
    ON tasks (next_reconcile_at) WHERE next_reconcile_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS tasks_active_idx
    ON tasks (atespace, name) WHERE state NOT IN (4, 5, 6);
CREATE INDEX IF NOT EXISTS tasks_session_idx
    ON tasks (atespace, session_id) WHERE session_id <> '';

CREATE TABLE IF NOT EXISTS workspaces (
    atespace text        NOT NULL,
    name     text        NOT NULL,
    data     jsonb       NOT NULL,
    saved_at timestamptz NOT NULL,
    PRIMARY KEY (atespace, name)
);
CREATE INDEX IF NOT EXISTS workspaces_atespace_saved_at_idx ON workspaces (atespace, saved_at DESC, name);

CREATE TABLE IF NOT EXISTS gateways (
    atespace text        NOT NULL,
    name     text        NOT NULL,
    data     jsonb       NOT NULL,
    saved_at timestamptz NOT NULL,
    PRIMARY KEY (atespace, name)
);
CREATE INDEX IF NOT EXISTS gateways_atespace_saved_at_idx ON gateways (atespace, saved_at DESC, name);

CREATE TABLE IF NOT EXISTS models (
    atespace text        NOT NULL,
    name     text        NOT NULL,
    data     jsonb       NOT NULL,
    saved_at timestamptz NOT NULL,
    PRIMARY KEY (atespace, name)
);
CREATE INDEX IF NOT EXISTS models_atespace_saved_at_idx ON models (atespace, saved_at DESC, name);

-- Leases backing the distributed Locker. A row whose expires_at has passed is
-- free and may be taken over by the next caller.
CREATE TABLE IF NOT EXISTS locks (
    key        text        PRIMARY KEY,
    token      text        NOT NULL,
    expires_at timestamptz NOT NULL
);

-- Live ax-server replica registry and heartbeat flush watermark.
CREATE TABLE IF NOT EXISTS ax_replicas (
    replica_id    text        PRIMARY KEY,
    started_at    timestamptz NOT NULL,
    last_flush_at timestamptz NOT NULL,
    expires_at    timestamptz NOT NULL
);

-- Pre-warmed Workspace snapshot Tag cache keyed by (atespace, workspace_name, ate_actor_template).
-- Writers publish updates via compare-and-swap on revision (see
-- store.WorkspaceSnapshotStore), advancing revision monotonically on each update.
-- Revision 0 is reserved by callers to mean "no existing row", so stored rows
-- always have revision >= 1 (enforced by the CHECK constraint below).
CREATE TABLE IF NOT EXISTS workspace_snapshots (
    atespace           text        NOT NULL,
    workspace_name     text        NOT NULL,
    ate_actor_template text        NOT NULL,
    active_tag         text        NOT NULL CHECK (active_tag <> ''),
    revision           bigint      NOT NULL CHECK (revision >= 1),
    updated_at         timestamptz NOT NULL,
    PRIMARY KEY (atespace, workspace_name, ate_actor_template)
);
