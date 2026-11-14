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
ALTER TABLE tasks
    ADD COLUMN IF NOT EXISTS state               smallint    NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS generation          bigint      NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS session_id          text        NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS actor_uid           text        NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS active_run_id       text        NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS run_seq             bigint      NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS action_seq          bigint      NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS effective_profile   jsonb,
    ADD COLUMN IF NOT EXISTS egress_rules        jsonb       NOT NULL DEFAULT '[]',
    ADD COLUMN IF NOT EXISTS runner_token_hash      bytea,
    ADD COLUMN IF NOT EXISTS runner_token_confirmed boolean     NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS runner_token_gen       integer     NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS last_acked_seq      bigint      NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS last_claim_nonce    text        NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS last_claim_response jsonb,
    ADD COLUMN IF NOT EXISTS last_heartbeat_at   timestamptz,
    ADD COLUMN IF NOT EXISTS last_resumed_at     timestamptz,
    ADD COLUMN IF NOT EXISTS claim_deadline      timestamptz,
    ADD COLUMN IF NOT EXISTS pending_actions     jsonb       NOT NULL DEFAULT '[]',
    ADD COLUMN IF NOT EXISTS next_reconcile_at   timestamptz,
    ADD COLUMN IF NOT EXISTS lease_owner         text,
    ADD COLUMN IF NOT EXISTS lease_expires_at    timestamptz;

CREATE INDEX IF NOT EXISTS tasks_queue_idx
    ON tasks (next_reconcile_at) WHERE next_reconcile_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS tasks_active_idx
    ON tasks (atespace, name) WHERE state NOT IN (4, 5, 6);
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

-- Live ax-server replica registry and heartbeat flush watermark.
CREATE TABLE IF NOT EXISTS ax_replicas (
    replica_id    text        PRIMARY KEY,
    started_at    timestamptz NOT NULL,
    last_flush_at timestamptz NOT NULL,
    expires_at    timestamptz NOT NULL
);

-- Pre-warmed Workspace snapshot Tag cache per (atespace, workspace, template).
CREATE TABLE IF NOT EXISTS workspace_snapshots (
    atespace           text        NOT NULL,
    workspace_name     text        NOT NULL,
    ate_actor_template text        NOT NULL,
    active_tag         text        NOT NULL DEFAULT '',
    revision           bigint      NOT NULL DEFAULT 0,
    updated_at         timestamptz NOT NULL,
    PRIMARY KEY (atespace, workspace_name, ate_actor_template)
);
