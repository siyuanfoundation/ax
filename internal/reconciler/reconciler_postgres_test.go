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
		Mode:          reconciler.ModeQueue,
		WorkerID:      "pg-worker-a",
		Transition:    stubTransition,
		LeaseTTL:      300 * time.Millisecond,
		RenewInterval: 60 * time.Millisecond,
		PollInterval:  25 * time.Millisecond,
		MutatingQPS:   -1,
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
		Mode:          reconciler.ModeInline,
		WorkerID:      "pg-w1-stalled",
		Transition:    countTrans,
		LeaseTTL:      100 * time.Millisecond,
		RenewInterval: 5 * time.Second,
		MutatingQPS:   -1,
	})
	w2 := reconciler.New(pgStore, rtFO, reconciler.Config{
		Mode:        reconciler.ModeInline,
		WorkerID:    "pg-w2-healthy",
		Transition:  countTrans,
		LeaseTTL:    5 * time.Second,
		MutatingQPS: -1,
	})

	cancelRun()
	<-runDone

	w1ErrCh := make(chan error, 1)
	go func() {
		w1ErrCh <- w1.ReconcileInline(ctx, foKey)
	}()
	<-w1Hold
	time.Sleep(150 * time.Millisecond) // allow w1's 100ms PostgreSQL lease to expire

	if err := w2.ReconcileInline(ctx, foKey); err != nil {
		t.Fatalf("w2 ReconcileInline on Postgres: %v", err)
	}
	close(w1Release)
	if err := <-w1ErrCh; err != nil {
		t.Fatalf("w1 ReconcileInline after lease loss on Postgres = %v, want nil", err)
	}
	if got := transitions.Load(); got != 1 {
		t.Fatalf("Postgres HasActiveLease transitions = %d, want 1", got)
	}
}

func TestReconciler_PostgresEndToEndSweepAndHeartbeats(t *testing.T) {
	pgStore := openIsolatedPostgresStore(t)
	sub := substratetest.NewServer(t)
	rt := reconciler.NewSubstrateRuntime(sub.Client(), reconciler.SubstrateRuntimeOptions{
		DefaultTaskImage: "us-docker.pkg.dev/acme/agent@sha256:abcdef",
	})
	ctx := t.Context()

	prof := &v1alpha1.Profile{Image: "us-docker.pkg.dev/acme/agent@sha256:abcdef"}
	tmplName, _, err := controller.ResolveProfileTemplate("payments", prof, "", "")
	if err != nil {
		t.Fatalf("ResolveProfileTemplate: %v", err)
	}

	hb := reconciler.NewHeartbeatBuffer(pgStore, reconciler.HeartbeatConfig{
		ReplicaID:     "pg-rep-1",
		FlushInterval: 40 * time.Millisecond,
		ReplicaTTL:    45 * time.Second,
	})
	rec := reconciler.New(pgStore, rt, reconciler.Config{
		Mode:         reconciler.ModeQueue,
		WorkerID:     "pg-worker-1",
		Transition:   stubTransition,
		PollInterval: 20 * time.Millisecond,
		MutatingQPS:  -1,
	})
	sweeper := reconciler.NewSweeper(pgStore, rt, reconciler.SweepConfig{
		ReplicaID:            "pg-rep-1",
		Reconciler:           rec,
		HeartbeatBuffer:      hb,
		Transition:           stubTransition,
		SweepInterval:        50 * time.Millisecond,
		NumShards:            4,
		RunnerSilenceTimeout: 750 * time.Millisecond,
		MaxWatermarkAge:      250 * time.Millisecond,
		MonitoringQPS:        -1,
	})

	runCtx, cancel := context.WithCancel(ctx)
	doneCh := make(chan struct{}, 3)
	go func() { _ = hb.Run(runCtx); doneCh <- struct{}{} }()
	go func() { _ = rec.Run(runCtx); doneCh <- struct{}{} }()
	go func() { _ = sweeper.Run(runCtx); doneCh <- struct{}{} }()
	t.Cleanup(func() {
		cancel()
		for range 3 {
			<-doneCh
		}
	})

	provisionTask := func(name string, tok []byte) store.TaskKey {
		key := store.TaskKey{Atespace: "payments", Name: name}
		if _, err := pgStore.Apply(ctx, key, func(_ *store.TaskRow, now time.Time) (*store.TaskRow, error) {
			st := lifecycle.State{
				Task: &v1alpha1.Task{
					Metadata: &v1alpha1.ObjectMeta{CreateTime: timestamppb.New(now)},
					Status: &v1alpha1.TaskStatus{
						State:            v1alpha1.TaskState_TASK_STATE_WORKING,
						AteActorTemplate: tmplName,
						Attempt:          1,
					},
				},
				RunnerTokenHash:      tok,
				RunnerTokenConfirmed: true,
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
			t.Fatalf("Apply(%s): %v", name, err)
		}
		waitFor(t, 3*time.Second, func() bool {
			r, gErr := pgStore.GetTaskRow(ctx, key)
			return gErr == nil && len(r.Actions) == 0 && r.ActorUID != ""
		})
		return key
	}

	tokAlive := []byte{0x11, 0x22}
	tokSilent := []byte{0x33, 0x44}
	aliveKey := provisionTask("pg-alive-task", tokAlive)
	silentKey := provisionTask("pg-silent-task", tokSilent)

	// Keep aliveKey heartbeating every 25ms in the background while silentKey goes silent.
	hbStop := make(chan struct{})
	hbDone := make(chan struct{})
	go func() {
		defer close(hbDone)
		ticker := time.NewTicker(25 * time.Millisecond)
		defer ticker.Stop()
		for {
			hb.Record(aliveKey, tokAlive)
			select {
			case <-hbStop:
				return
			case <-ticker.C:
			}
		}
	}()
	defer func() {
		close(hbStop)
		<-hbDone
	}()

	// Wait for silentKey to be detected lost via watermark-gated sweep and parked on Substrate.
	waitFor(t, 4*time.Second, func() bool {
		r, gErr := pgStore.GetTaskRow(ctx, silentKey)
		if gErr != nil || r.Task.GetStatus().GetState() != v1alpha1.TaskState_TASK_STATE_FAILED || len(r.Actions) != 0 {
			return false
		}
		actor, ok := sub.Actor("payments", "pg-silent-task")
		return ok && actor.GetStatus().GetState() == ateapipb.ActorState_ACTOR_STATE_SUSPENDED
	})

	// Verify aliveKey remained WORKING throughout!
	aliveRow, err := pgStore.GetTaskRow(ctx, aliveKey)
	if err != nil || aliveRow.Task.GetStatus().GetState() != v1alpha1.TaskState_TASK_STATE_WORKING {
		t.Fatalf("alive heartbeating task state = %v (err=%v), want WORKING", aliveRow.Task.GetStatus().GetState(), err)
	}

	// Simulate sandbox crash on aliveKey -> Sweeper enqueues REVERT_ACTOR + RESUME_ACTOR -> Reconciler recovers it to RUNNING!
	sub.SetActor(&ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: "payments", Name: "pg-alive-task", Uid: aliveRow.ActorUID},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: "payments", Name: tmplName},
		Status: &ateapipb.ActorStatus{
			State:                  ateapipb.ActorState_ACTOR_STATE_CRASHED,
			LastAssignedGeneration: 1,
			Snapshots: []*ateapipb.Snapshot{{
				Generation:      1,
				Owner:           ateapipb.SnapshotOwner_SNAPSHOT_OWNER_TAG,
				DurableSnapshot: &ateapipb.SnapshotStorage{State: ateapipb.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_COMPLETED},
			}},
		},
	})
	waitFor(t, 4*time.Second, func() bool {
		r, gErr := pgStore.GetTaskRow(ctx, aliveKey)
		if gErr != nil || r.Task.GetStatus().GetAttempt() < 2 || len(r.Actions) != 0 {
			return false
		}
		actor, ok := sub.Actor("payments", "pg-alive-task")
		return ok && actor.GetStatus().GetState() == ateapipb.ActorState_ACTOR_STATE_RUNNING
	})
}
