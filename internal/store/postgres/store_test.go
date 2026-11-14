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
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/ax/internal/store"
	"github.com/google/ax/internal/store/storetest"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// testURL returns the database URL from $AX_TEST_POSTGRES_URL, skipping the
// test when it is unset. Tests drop and recreate all AX tables in it.
func testURL(t *testing.T) string {
	t.Helper()
	u := os.Getenv("AX_TEST_POSTGRES_URL")
	if u == "" {
		t.Skip("AX_TEST_POSTGRES_URL not set; skipping Postgres tests")
	}
	return u
}

// openTest opens a Store on an empty schema.
func openTest(t *testing.T, opts Options) *Store {
	t.Helper()
	ctx := context.Background()
	s, err := Open(ctx, testURL(t), opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := s.pool.Exec(ctx, `TRUNCATE tasks, workspaces, gateways, models, locks, ax_replicas, workspace_snapshots`); err != nil {
		t.Fatalf("truncating tables: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestCreateSchemaConcurrent(t *testing.T) {
	s := openTest(t, Options{})
	ctx := context.Background()
	if _, err := s.pool.Exec(ctx, `DROP TABLE tasks, workspaces, gateways, models, locks, ax_replicas, workspace_snapshots`); err != nil {
		t.Fatal(err)
	}
	// Simulate several replicas starting at once.
	errs := make(chan error, 4)
	for range 4 {
		go func() { errs <- createSchema(ctx, s.pool) }()
	}
	for range 4 {
		if err := <-errs; err != nil {
			t.Fatalf("createSchema: %v", err)
		}
	}
	if err := s.SaveTask(ctx, &v1alpha1.Task{Metadata: &v1alpha1.ObjectMeta{Name: "after-create"}}); err != nil {
		t.Fatalf("SaveTask after createSchema: %v", err)
	}
}

// TestListNewestFirst checks that lists are ordered by most recent save, as
// they were with Redis: re-saving a resource moves it to the front, and a
// status update does not.
func TestListNewestFirst(t *testing.T) {
	s := openTest(t, Options{})
	ctx := context.Background()
	save := func(atespace, name string) {
		t.Helper()
		task := &v1alpha1.Task{Metadata: &v1alpha1.ObjectMeta{Atespace: atespace, Name: name}}
		if err := s.SaveTask(ctx, task); err != nil {
			t.Fatalf("SaveTask: %v", err)
		}
	}
	save("a", "a1")
	save("a", "a2")
	save("a", "a3")
	save("b", "b1")
	save("a", "a1")
	if err := s.UpdateTaskStatus(ctx, "a", "a2", &v1alpha1.TaskStatus{State: v1alpha1.TaskState_TASK_STATE_FAILED}); err != nil {
		t.Fatalf("UpdateTaskStatus: %v", err)
	}
	dt := timestamppb.Now()
	if err := s.SetTaskDeleteTime(ctx, "a", "a2", dt); err != nil {
		t.Fatalf("SetTaskDeleteTime: %v", err)
	}
	if a2, err := s.GetTask(ctx, "a", "a2"); err != nil || a2.GetMetadata().GetDeleteTime() == nil {
		t.Fatalf("GetTask(a/a2) delete_time = %v, err = %v; want non-nil", a2.GetMetadata().GetDeleteTime(), err)
	}
	if err := s.SetTaskDeleteTime(ctx, "a", "a2", nil); err != nil {
		t.Fatalf("SetTaskDeleteTime(nil): %v", err)
	}

	check := func(atespace string, limit, offset int64, want ...string) {
		t.Helper()
		tasks, err := s.ListTasks(ctx, atespace, limit, offset)
		if err != nil {
			t.Fatalf("ListTasks(%q, %d, %d): %v", atespace, limit, offset, err)
		}
		var got []string
		for _, task := range tasks {
			got = append(got, task.GetMetadata().GetAtespace()+"/"+task.GetMetadata().GetName())
		}
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Errorf("ListTasks(%q, %d, %d) = %v, want %v", atespace, limit, offset, got, want)
		}
	}
	check("a", 10, 0, "a/a1", "a/a3", "a/a2")
	check("*", 10, 0, "a/a1", "b/b1", "a/a3", "a/a2")
	check("*", 2, 2, "a/a3", "a/a2")
	check("a", 1, 1, "a/a3")

	for _, w := range []string{"w1", "w2"} {
		if err := s.SaveWorkspace(ctx, &v1alpha1.Workspace{Metadata: &v1alpha1.ObjectMeta{Name: w}}); err != nil {
			t.Fatalf("SaveWorkspace: %v", err)
		}
	}
	if list, err := s.ListWorkspaces(ctx, "*", 50, 0); err != nil || len(list) != 2 || list[0].GetMetadata().GetName() != "w2" {
		t.Errorf("ListWorkspaces = %v, %v; want w2 first", list, err)
	}
	if page, err := s.ListWorkspaces(ctx, "*", 1, 1); err != nil || len(page) != 1 || page[0].GetMetadata().GetName() != "w1" {
		t.Errorf("ListWorkspaces(limit=1, offset=1) = %v, %v; want [w1]", page, err)
	}
	for _, g := range []string{"g1", "g2"} {
		if err := s.SaveGateway(ctx, &v1alpha1.Gateway{Metadata: &v1alpha1.ObjectMeta{Name: g}}); err != nil {
			t.Fatalf("SaveGateway: %v", err)
		}
	}
	if list, err := s.ListGateways(ctx, "*"); err != nil || len(list) != 2 || list[0].GetMetadata().GetName() != "g2" {
		t.Errorf("ListGateways = %v, %v; want g2 first", list, err)
	}
	for _, m := range []string{"m1", "m2"} {
		if err := s.SaveModel(ctx, &v1alpha1.Model{Metadata: &v1alpha1.ObjectMeta{Name: m}}); err != nil {
			t.Fatalf("SaveModel: %v", err)
		}
	}
	if list, err := s.ListModels(ctx, "*"); err != nil || len(list) != 2 || list[0].GetMetadata().GetName() != "m2" {
		t.Errorf("ListModels = %v, %v; want m2 first", list, err)
	}
}

// TestLockRenewsLeaseWhileHeld verifies that a held lock outlives its TTL
// (because it is renewed) and that waiters acquire it only after release.
func TestLockRenewsLeaseWhileHeld(t *testing.T) {
	ttl := 300 * time.Millisecond
	s := openTest(t, Options{LockTTL: ttl, LockRetryInterval: 20 * time.Millisecond})
	ctx := context.Background()

	unlock, err := s.Lock(ctx, "task", "default", "renew")
	if err != nil {
		t.Fatalf("Lock: %v", err)
	}
	waitCtx, cancel := context.WithTimeout(ctx, 4*ttl)
	defer cancel()
	if _, err := s.Lock(waitCtx, "task", "default", "renew"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected contender deadline while lease was held, got %v", err)
	}
	unlock()

	acquireCtx, cancel2 := context.WithTimeout(ctx, ttl)
	defer cancel2()
	unlock2, err := s.Lock(acquireCtx, "task", "default", "renew")
	if err != nil {
		t.Fatalf("Lock after release: %v", err)
	}
	unlock2()
}

// TestLockTakesOverExpiredLease verifies that a lock orphaned by a crashed
// holder becomes available once its lease expires.
func TestLockTakesOverExpiredLease(t *testing.T) {
	ttl := 200 * time.Millisecond
	s := openTest(t, Options{LockTTL: ttl, LockRetryInterval: 20 * time.Millisecond})
	ctx := context.Background()

	// Start the clock before the INSERT. The lease expires ttl after the
	// database evaluates clock_timestamp(), which happens somewhere during the
	// INSERT's round trip, so timing from after it returns would undercount
	// by however long the round trip and commit took.
	start := time.Now()
	if _, err := s.pool.Exec(ctx, `INSERT INTO locks (key, token, expires_at) VALUES ($1, 'crashed', clock_timestamp() + $2 * interval '1 millisecond')`,
		"lock:task:default:orphan", ttl.Milliseconds()); err != nil {
		t.Fatal(err)
	}
	acquireCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	unlock, err := s.Lock(acquireCtx, "task", "default", "orphan")
	if err != nil {
		t.Fatalf("Lock: %v", err)
	}
	defer unlock()
	if elapsed := time.Since(start); elapsed < ttl {
		t.Fatalf("acquired orphaned lock after %v, before its %v lease expired", elapsed, ttl)
	}
}

// TestRestartKeepsTasksAndReleasesLocks simulates a server crash. A task is
// saved and a lock is taken, then the store is closed without unlocking and
// reopened on the same database. The task must still be there, and the lock
// must be takeable once its lease runs out.
func TestRestartKeepsTasksAndReleasesLocks(t *testing.T) {
	ttl := 300 * time.Millisecond
	opts := Options{LockTTL: ttl, LockRetryInterval: 20 * time.Millisecond}
	s1 := openTest(t, opts)
	ctx := context.Background()
	task := &v1alpha1.Task{Metadata: &v1alpha1.ObjectMeta{Atespace: "default", Name: "survivor"}}
	if err := s1.SaveTask(ctx, task); err != nil {
		t.Fatalf("SaveTask: %v", err)
	}
	if _, err := s1.Lock(ctx, "task", "default", "survivor"); err != nil {
		t.Fatalf("Lock before restart: %v", err)
	}
	// Crash: close without calling unlock, so the lease is never released.
	if err := s1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen without truncating, so the data from before the restart is kept.
	s2, err := Open(ctx, testURL(t), opts)
	if err != nil {
		t.Fatalf("Open after restart: %v", err)
	}
	t.Cleanup(func() { _ = s2.Close() })
	got, err := s2.GetTask(ctx, "default", "survivor")
	if err != nil {
		t.Fatalf("GetTask after restart: %v", err)
	}
	if got.GetMetadata().GetName() != "survivor" {
		t.Fatalf("GetTask name = %q, want survivor", got.GetMetadata().GetName())
	}

	acquireCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	unlock, err := s2.Lock(acquireCtx, "task", "default", "survivor")
	if err != nil {
		t.Fatalf("Lock after restart: %v", err)
	}
	unlock()
}

func TestStoreContract(t *testing.T) {
	s := openTest(t, Options{})
	storetest.RunContract(t, s)
}

func TestApplyConcurrentCreateAndLockTimeout(t *testing.T) {
	s := openTest(t, Options{})
	ctx := context.Background()
	key := store.TaskKey{Atespace: "default", Name: "race-create"}

	// 1. Concurrent Apply calls when current == nil must all succeed without
	// leaking 23505 unique_violation errors, serializing from generation 1..N.
	const workers = 6
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for range workers {
		wg.Go(func() {
			_, err := s.Apply(ctx, key, func(cur *store.TaskRow, _ time.Time) (*store.TaskRow, error) {
				if cur == nil {
					return &store.TaskRow{
						Task: &v1alpha1.Task{
							Metadata: &v1alpha1.ObjectMeta{Atespace: key.Atespace, Name: key.Name},
						},
						RunSeq: 1,
					}, nil
				}
				next := cur.Clone()
				next.RunSeq++
				return next, nil
			})
			errs <- err
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent Apply failed: %v", err)
		}
	}
	row, err := s.GetTaskRow(ctx, key)
	if err != nil {
		t.Fatalf("GetTaskRow: %v", err)
	}
	if row.Generation != workers || row.RunSeq != workers {
		t.Fatalf("final row = (gen=%d, runSeq=%d), want (%d, %d)", row.Generation, row.RunSeq, workers, workers)
	}

	// 2. Holding FOR UPDATE longer than Apply's 2s lock_timeout returns store.ErrLockTimeout.
	locked := make(chan struct{})
	release := make(chan struct{})
	holdDone := make(chan error, 1)
	go func() {
		holdDone <- pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
			var gen int64
			if err := tx.QueryRow(ctx, `SELECT generation FROM tasks WHERE atespace = $1 AND name = $2 FOR UPDATE`,
				key.Atespace, key.Name).Scan(&gen); err != nil {
				return err
			}
			close(locked)
			<-release
			return nil
		})
	}()
	<-locked
	_, lockErr := s.Apply(ctx, key, func(cur *store.TaskRow, _ time.Time) (*store.TaskRow, error) {
		next := cur.Clone()
		next.RunSeq++
		return next, nil
	})
	close(release)
	if err := <-holdDone; err != nil {
		t.Fatalf("holder tx: %v", err)
	}
	if !errors.Is(lockErr, store.ErrLockTimeout) {
		t.Fatalf("Apply under lock contention = %v, want store.ErrLockTimeout", lockErr)
	}
}

// tasksTableBeforeQueueColumns is the tasks table as created before the queue,
// runner, and lease columns existed: the task document was the only place that
// held status.state, status.generation, and spec.session_id.
const tasksTableBeforeQueueColumns = `
CREATE TABLE tasks (
    atespace text        NOT NULL,
    name     text        NOT NULL,
    data     jsonb       NOT NULL,
    saved_at timestamptz NOT NULL,
    PRIMARY KEY (atespace, name)
);
CREATE INDEX tasks_saved_at_idx ON tasks (saved_at DESC, atespace, name);
CREATE INDEX tasks_atespace_saved_at_idx ON tasks (atespace, saved_at DESC, name);`

// Upgrading a database that predates the task queue columns must backfill
// generation, state, and session_id from each row's JSON document, and
// subsequent startups must skip the ALTER TABLE so readers are not blocked.
func TestCreateSchemaUpgradesRowsWrittenBeforeQueueColumns(t *testing.T) {
	s := openTest(t, Options{})
	ctx := context.Background()

	if _, err := s.pool.Exec(ctx, `DROP TABLE tasks`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(ctx, tasksTableBeforeQueueColumns); err != nil {
		t.Fatal(err)
	}
	insertOld := func(task *v1alpha1.Task) {
		t.Helper()
		data, err := protojson.Marshal(task)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.pool.Exec(ctx,
			`INSERT INTO tasks (atespace, name, data, saved_at) VALUES ($1, $2, $3, clock_timestamp())`,
			task.Metadata.Atespace, task.Metadata.Name, string(data)); err != nil {
			t.Fatal(err)
		}
	}
	insertOld(&v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Atespace: "payments", Name: "old-queued"},
		Spec:     &v1alpha1.TaskSpec{SessionId: "sess-7"},
		Status:   &v1alpha1.TaskStatus{State: v1alpha1.TaskState_TASK_STATE_QUEUED, Generation: 7},
	})
	insertOld(&v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Atespace: "payments", Name: "old-failed"},
		Status:   &v1alpha1.TaskStatus{State: v1alpha1.TaskState_TASK_STATE_FAILED, Generation: 5},
	})
	// A row that never recorded a generation or a state.
	insertOld(&v1alpha1.Task{Metadata: &v1alpha1.ObjectMeta{Atespace: "payments", Name: "old-bare"}})

	if err := createSchema(ctx, s.pool); err != nil {
		t.Fatalf("createSchema on a pre-queue-columns database: %v", err)
	}

	columns := func(name string) (state int16, gen int64, session string) {
		t.Helper()
		if err := s.pool.QueryRow(ctx,
			`SELECT state, generation, session_id FROM tasks WHERE atespace = 'payments' AND name = $1`, name,
		).Scan(&state, &gen, &session); err != nil {
			t.Fatalf("reading columns of %s: %v", name, err)
		}
		return state, gen, session
	}
	for _, want := range []struct {
		name    string
		state   v1alpha1.TaskState
		gen     int64
		session string
	}{
		{"old-queued", v1alpha1.TaskState_TASK_STATE_QUEUED, 7, "sess-7"},
		{"old-failed", v1alpha1.TaskState_TASK_STATE_FAILED, 5, ""},
		{"old-bare", v1alpha1.TaskState_TASK_STATE_UNSPECIFIED, 1, ""},
	} {
		state, gen, session := columns(want.name)
		if v1alpha1.TaskState(state) != want.state || gen != want.gen || session != want.session {
			t.Errorf("%s columns after upgrade = (state=%s, generation=%d, session_id=%q), want (%s, %d, %q)",
				want.name, v1alpha1.TaskState(state), gen, session, want.state, want.gen, want.session)
		}
		got, err := s.GetTask(ctx, "payments", want.name)
		if err != nil {
			t.Fatalf("GetTask(%s): %v", want.name, err)
		}
		if got.GetStatus().GetGeneration() != want.gen {
			t.Errorf("%s: status.generation read after upgrade = %d, want %d (must not move backwards)",
				want.name, got.GetStatus().GetGeneration(), want.gen)
		}
	}

	// The next write continues from the carried-over generation.
	if err := s.UpdateTaskStatus(ctx, "payments", "old-queued", &v1alpha1.TaskStatus{
		State: v1alpha1.TaskState_TASK_STATE_QUEUED,
	}); err != nil {
		t.Fatalf("UpdateTaskStatus: %v", err)
	}
	if _, gen, _ := columns("old-queued"); gen != 8 {
		t.Errorf("old-queued generation after the next write = %d, want 8", gen)
	}

	// Terminal rows written before the upgrade are not in the active index.
	var active int
	if err := s.pool.QueryRow(ctx,
		`SELECT count(*) FROM tasks WHERE atespace = 'payments' AND state NOT IN (4, 5, 6)`).Scan(&active); err != nil {
		t.Fatal(err)
	}
	if active != 2 {
		t.Errorf("non-terminal rows by state column = %d, want 2 (old-queued, old-bare)", active)
	}

	// Subsequent startups are a no-op: they neither rewrite generations nor
	// block behind a session holding a row-level FOR UPDATE lock.
	locked := make(chan struct{})
	release := make(chan struct{})
	holdDone := make(chan error, 1)
	go func() {
		holdDone <- pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
			var gen int64
			if err := tx.QueryRow(ctx,
				`SELECT generation FROM tasks WHERE atespace = 'payments' AND name = 'old-failed' FOR UPDATE`,
			).Scan(&gen); err != nil {
				return err
			}
			close(locked)
			<-release
			return nil
		})
	}()
	<-locked
	startCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	restartErr := createSchema(startCtx, s.pool)
	cancel()
	close(release)
	if err := <-holdDone; err != nil {
		t.Fatalf("row-lock holder: %v", err)
	}
	if restartErr != nil {
		t.Errorf("createSchema on an upgraded database blocked behind a row lock: %v", restartErr)
	}
	if _, gen, _ := columns("old-queued"); gen != 8 {
		t.Errorf("old-queued generation after restart = %d, want 8 (restart must not backfill again)", gen)
	}
}
