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

-- Additive Task lifecycle, runner, and leased action-queue columns.
-- Batched heartbeat flushes update only the unindexed last_heartbeat_at column,
-- allowing PostgreSQL to apply them as heap-only tuple (HOT) updates alongside
-- fillfactor = 70 without index maintenance. Keeping lease_owner and
-- lease_expires_at unindexed also avoids maintaining extra secondary indexes on
-- queue lease transitions (which update next_reconcile_at).
ALTER TABLE tasks
    ADD COLUMN IF NOT EXISTS state                  smallint    NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS generation             bigint      NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS session_id             text        NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS actor_uid              text        NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS active_run_id          text        NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS run_seq                bigint      NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS action_seq             bigint      NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS effective_profile      jsonb,
    ADD COLUMN IF NOT EXISTS egress_rules           jsonb       NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN IF NOT EXISTS runner_token_hash      bytea,
    ADD COLUMN IF NOT EXISTS runner_token_confirmed boolean     NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS runner_token_gen       integer     NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS last_acked_seq         bigint      NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS claim_deadline         timestamptz,
    ADD COLUMN IF NOT EXISTS last_resumed_at        timestamptz,
    ADD COLUMN IF NOT EXISTS last_heartbeat_at      timestamptz,
    ADD COLUMN IF NOT EXISTS pending_actions        jsonb       NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN IF NOT EXISTS next_reconcile_at      timestamptz,
    ADD COLUMN IF NOT EXISTS lease_owner            text,
    ADD COLUMN IF NOT EXISTS lease_expires_at       timestamptz;

-- Queue index: covers only tasks with pending actions or active timers;
-- steady-state tasks have next_reconcile_at IS NULL and stay out of this index.
-- Including (atespace, name) lets LeaseDue stream due rows in exact order via
-- Index Scan + LockRows (SKIP LOCKED) without sorting the full due set.
CREATE INDEX IF NOT EXISTS tasks_queue_idx
    ON tasks (next_reconcile_at, atespace, name) WHERE next_reconcile_at IS NOT NULL;

-- Active-task sweep index: covers all non-terminal tasks by excluding terminal
-- TaskState enum values from task.proto (4 = TASK_STATE_COMPLETED,
-- 5 = TASK_STATE_FAILED, 6 = TASK_STATE_CANCELLED).
CREATE INDEX IF NOT EXISTS tasks_active_idx
    ON tasks (atespace, name) WHERE state NOT IN (4, 5, 6);

-- Session correlation index: supports filtering tasks by TaskSpec.session_id.
CREATE INDEX IF NOT EXISTS tasks_session_idx
    ON tasks (atespace, session_id) WHERE session_id <> '';

ALTER TABLE tasks SET (
    fillfactor = 70,
    autovacuum_vacuum_scale_factor = 0.02,
    autovacuum_analyze_scale_factor = 0.02
);

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

-- Replica registry: tracks live ax-server replicas and their latest runner
-- heartbeat flush timestamps so the drift sweep can scale mutating QPS across
-- live replicas, balance sweep shards, and gate lost-runner detection on the
-- cluster-wide heartbeat watermark.
CREATE TABLE IF NOT EXISTS ax_replicas (
    replica_id    text        PRIMARY KEY,
    started_at    timestamptz NOT NULL,
    last_flush_at timestamptz NOT NULL,
    expires_at    timestamptz NOT NULL
);
