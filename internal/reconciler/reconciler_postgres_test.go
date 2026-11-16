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

package reconciler_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"net/url"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/ax/internal/controller"
	"github.com/google/ax/internal/lifecycle"
	"github.com/google/ax/internal/reconciler"
	"github.com/google/ax/internal/store"
	"github.com/google/ax/internal/store/postgres"
	"github.com/google/ax/internal/substrate/substratetest"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"github.com/jackc/pgx/v5/pgxpool"
	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func openIsolatedPostgresStore(t *testing.T) *postgres.Store {
	t.Helper()
	baseURL := os.Getenv("AX_TEST_POSTGRES_URL")
	if baseURL == "" {
		t.Skip("AX_TEST_POSTGRES_URL not set; skipping Postgres reconciler integration test")
	}

	ctx := context.Background()
	var suffix [6]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		t.Fatalf("rand: %v", err)
	}
	schemaName := "reconciler_test_" + hex.EncodeToString(suffix[:])

	adminPool, err := pgxpool.New(ctx, baseURL)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	if _, err := adminPool.Exec(ctx, "CREATE SCHEMA "+schemaName); err != nil {
		adminPool.Close()
		t.Fatalf("CREATE SCHEMA: %v", err)
	}
	t.Cleanup(func() {
		_, _ = adminPool.Exec(context.Background(), "DROP SCHEMA IF EXISTS "+schemaName+" CASCADE")
		adminPool.Close()
	})

	u, err := url.Parse(baseURL)
	if err != nil {
		t.Fatalf("url.Parse: %v", err)
	}
	q := u.Query()
	q.Set("search_path", schemaName)
	u.RawQuery = q.Encode()

	pgStore, err := postgres.Open(ctx, u.String(), postgres.Options{
		ApplyLockTimeout:      500 * time.Millisecond,
		ApplyStatementTimeout: 3 * time.Second,
	})
	if err != nil {
		t.Fatalf("postgres.Open: %v", err)
	}
	t.Cleanup(func() { _ = pgStore.Close() })
	return pgStore
}

func TestReconciler_PostgresIntegrationLeaseFailoverAndObsoleteCancel(t *testing.T) {
	pgStore := openIsolatedPostgresStore(t)
	sub := substratetest.NewServer(t)
	baseRT := reconciler.NewSubstrateRuntime(sub.Client(), reconciler.SubstrateRuntimeOptions{
		DefaultTaskImage: "us-docker.pkg.dev/acme/agent@sha256:abcdef",
	})

	ctx := t.Context()
	prof := &v1alpha1.Profile{Image: "us-docker.pkg.dev/acme/agent@sha256:abcdef"}
	tmplName, _, err := controller.ResolveProfileTemplate("payments", prof, "", "")
	if err != nil {
		t.Fatalf("ResolveProfileTemplate: %v", err)
	}

	// 1. Multi-step provisioning + obsolete ResumeActor cancellation via RenewLease on real PostgreSQL.
	resumeBlocked := make(chan struct{})
	resumeCanceled := make(chan struct{})
	var blockOnce atomic.Bool
	blockOnce.Store(true)

	rtCancel := &customRuntime{
		RuntimeClient: baseRT,
		onResume: func(callCtx context.Context, ns, actor string) error {
			if blockOnce.CompareAndSwap(true, false) {
				close(resumeBlocked)
				<-callCtx.Done()
				close(resumeCanceled)
				return callCtx.Err()
			}
			return baseRT.ResumeActor(callCtx, ns, actor)
		},
	}

	rec := reconciler.New(pgStore, rtCancel, reconciler.Config{
		WorkerID:      "pg-worker-a",
		Transition:    stubTransition,
		LeaseTTL:      300 * time.Millisecond,
		RenewInterval: 60 * time.Millisecond,
		PollInterval:  25 * time.Millisecond,
	})
	runCtx, cancelRun := context.WithCancel(ctx)
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		_ = rec.Run(runCtx)
	}()
	t.Cleanup(func() {
		cancelRun()
		<-runDone
	})

	cancelKey := store.TaskKey{Atespace: "payments", Name: "pg-cancel-task"}
	if _, err := pgStore.Apply(ctx, cancelKey, func(_ *store.TaskRow, now time.Time) (*store.TaskRow, error) {
		st := lifecycle.State{
			Task: &v1alpha1.Task{
				Status: &v1alpha1.TaskStatus{
					State:            v1alpha1.TaskState_TASK_STATE_QUEUED,
					AteActorTemplate: tmplName,
				},
			},
		}
		st.Actions = []lifecycle.Action{
			st.NextAction(lifecycle.ActionEnsureTemplate, now),
			st.NextAction(lifecycle.ActionCreateActor, now),
			st.NextAction(lifecycle.ActionResumeActor, now),
		}
		row := &store.TaskRow{EffectiveProfile: prof}
		row.SetLifecycleState(st, now)
		return row, nil
	}); err != nil {
		t.Fatalf("Apply(pg-cancel-task): %v", err)
	}

	<-resumeBlocked
	if _, err := pgStore.Apply(ctx, cancelKey, func(cur *store.TaskRow, now time.Time) (*store.TaskRow, error) {
		next := cur.Clone()
		st := next.LifecycleState()
		st.Task.Metadata.DeleteTime = timestamppb.New(now)
		st.Actions = []lifecycle.Action{st.NextAction(lifecycle.ActionDeleteActor, now)}
		next.SetLifecycleState(st, now)
		return next, nil
	}); err != nil {
		t.Fatalf("Apply(DeleteTask on Postgres): %v", err)
	}

	select {
	case <-resumeCanceled:
	case <-time.After(3 * time.Second):
		t.Fatal("RenewLease on Postgres did not cancel superseded ResumeActor")
	}

	waitFor(t, 3*time.Second, func() bool {
		_, gErr := pgStore.GetTaskRow(ctx, cancelKey)
		return errors.Is(gErr, store.ErrNotFound)
	})
	if _, exists := sub.Actor("payments", "pg-cancel-task"); exists {
		t.Fatal("expected actor deleted from Substrate after DELETE_ACTOR on Postgres")
	}

	// 2. Cross-worker lease failover and DB HasActiveLease fencing on real PostgreSQL.
	cancelRun()
	<-runDone

	sub.SetActor(&ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: "payments", Name: "pg-failover", Uid: "uid-pg-fo"},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: "payments", Name: tmplName},
		Status:        &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
	})
	foKey := store.TaskKey{Atespace: "payments", Name: "pg-failover"}
	if _, err := pgStore.Apply(ctx, foKey, func(_ *store.TaskRow, now time.Time) (*store.TaskRow, error) {
		st := lifecycle.State{
			Task: &v1alpha1.Task{
				Status: &v1alpha1.TaskStatus{
					State:            v1alpha1.TaskState_TASK_STATE_QUEUED,
					AteActorTemplate: tmplName,
				},
			},
			ActorUID: "uid-pg-fo",
		}
		st.Actions = []lifecycle.Action{st.NextAction(lifecycle.ActionResumeActor, now)}
		row := &store.TaskRow{EffectiveProfile: prof}
		row.SetLifecycleState(st, now)
		return row, nil
	}); err != nil {
		t.Fatalf("Apply(pg-failover): %v", err)
	}

	var resumes atomic.Int32
	var transitions atomic.Int32
	w1Hold := make(chan struct{})
	w1Release := make(chan struct{})
	rtFO := &customRuntime{
		RuntimeClient: baseRT,
		onResume: func(callCtx context.Context, ns, actor string) error {
			if resumes.Add(1) == 1 {
				if err := baseRT.ResumeActor(callCtx, ns, actor); err != nil {
					return err
				}
				close(w1Hold)
				<-w1Release
				return nil
			}
			return baseRT.ResumeActor(callCtx, ns, actor)
		},
	}
	countTrans := func(state lifecycle.State, in lifecycle.Input, now time.Time) (lifecycle.State, lifecycle.Output, error) {
		transitions.Add(1)
		return stubTransition(state, in, now)
	}
	w1 := reconciler.New(pgStore, rtFO, reconciler.Config{
		WorkerID:      "pg-w1-stalled",
		Transition:    countTrans,
		LeaseTTL:      100 * time.Millisecond,
		RenewInterval: 5 * time.Second,
	})
	w2 := reconciler.New(pgStore, rtFO, reconciler.Config{
		WorkerID:   "pg-w2-healthy",
		Transition: countTrans,
		LeaseTTL:   5 * time.Second,
	})

	w1Done := make(chan error, 1)
	go func() {
		_, err := w1.ProcessOnce(ctx)
		w1Done <- err
	}()
	<-w1Hold
	time.Sleep(150 * time.Millisecond) // allow w1's 100ms PostgreSQL lease to expire

	if _, err := w2.ProcessOnce(ctx); err != nil {
		t.Fatalf("w2 ProcessOnce on Postgres: %v", err)
	}
	close(w1Release)
	if err := <-w1Done; err != nil {
		t.Fatalf("w1 ProcessOnce after lease loss on Postgres = %v, want nil", err)
	}
	if got := transitions.Load(); got != 1 {
		t.Fatalf("Postgres HasActiveLease transitions = %d, want 1", got)
	}
}
