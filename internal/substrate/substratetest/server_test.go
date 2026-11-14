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

package substratetest_test

import (
	"testing"

	"github.com/google/ax/internal/substrate"
	"github.com/google/ax/internal/substrate/substratetest"
	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRuntimeClientSnapshotGenCrashAndPaging(t *testing.T) {
	ctx := t.Context()
	srv := substratetest.NewServer(t)
	var rc substrate.RuntimeClient = srv.Client()

	if err := rc.EnsureAtespace(ctx, "payments"); err != nil {
		t.Fatalf("EnsureAtespace: %v", err)
	}
	tmpl := substrate.BuildActorTemplate("payments", "ax-0123456789abcdef", "img@sha256:abc", nil, nil, "", nil)
	if _, err := rc.EnsureActorTemplate(ctx, tmpl); err != nil {
		t.Fatalf("EnsureActorTemplate: %v", err)
	}

	// Create actor -> assigns non-empty actor UID.
	actor, err := rc.EnsureActorFromTag(ctx, "payments", "task-1", "payments", "ax-0123456789abcdef", "")
	if err != nil || actor.GetMetadata().GetUid() == "" {
		t.Fatalf("EnsureActorFromTag = (%v, %v), want non-empty actor UID", actor, err)
	}

	// Resume + SuspendActorSync increments durable Snapshot.generation to 1.
	if _, _, err := rc.ResumeActor(ctx, "payments", "task-1"); err != nil {
		t.Fatalf("ResumeActor: %v", err)
	}
	_, gen1, err := rc.SuspendActorSync(ctx, "payments", "task-1")
	if err != nil || gen1 != 1 {
		t.Fatalf("SuspendActorSync gen = %d, err = %v; want gen 1", gen1, err)
	}

	// Suspending a CRASHED actor returns FailedPrecondition; RevertActor restores gen 1.
	crashed, _ := srv.Actor("payments", "task-1")
	crashed.Status.State = ateapipb.ActorState_ACTOR_STATE_CRASHED
	srv.SetActor(crashed)

	if _, _, err := rc.SuspendActorSync(ctx, "payments", "task-1"); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("SuspendActorSync on CRASHED = %v, want FailedPrecondition", err)
	}
	_, revGen, err := rc.RevertActor(ctx, "payments", "task-1")
	if err != nil || revGen != 1 {
		t.Fatalf("RevertActor gen = %d, err = %v; want gen 1", revGen, err)
	}
	// Repeat RevertActor on the now-SUSPENDED durable actor: substratetest returns
	// FailedPrecondition, and Client.RevertActor resolves it idempotently.
	_, revGenRepeat, err := rc.RevertActor(ctx, "payments", "task-1")
	if err != nil || revGenRepeat != 1 {
		t.Fatalf("repeat RevertActor gen = %d, err = %v; want idempotent gen 1", revGenRepeat, err)
	}

	// PENDING snapshots must be ignored by ActiveSnapshotGeneration.
	withPending, _ := srv.Actor("payments", "task-1")
	withPending.Status.Snapshots = append(withPending.Status.Snapshots, &ateapipb.Snapshot{
		Uuid:       "snap-pending",
		Generation: 99,
		Locality:   "node-1",
		DurableSnapshot: &ateapipb.SnapshotStorage{
			State: ateapipb.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_PENDING,
		},
	})
	if gotGen := substrate.ActiveSnapshotGeneration(withPending); gotGen != 1 {
		t.Fatalf("ActiveSnapshotGeneration with PENDING gen 99 = %d, want 1", gotGen)
	}
	if !substrate.IsRetryableActionError(status.Error(codes.Aborted, "busy")) ||
		!substrate.IsRetryableActionError(status.Error(codes.Internal, "transient internal")) ||
		substrate.IsRetryableActionError(status.Error(codes.InvalidArgument, "bad arg")) {
		t.Fatal("unexpected IsRetryableActionError classification")
	}

	// EnsureActorFromTag rejects mismatched shared Profile templates on an existing actor,
	// while remaining idempotent for the matching template.
	if _, err := rc.EnsureActorFromTag(ctx, "payments", "task-1", "payments", "ax-0123456789abcdef", ""); err != nil {
		t.Fatalf("idempotent EnsureActorFromTag: %v", err)
	}
	if _, err := rc.EnsureActorFromTag(ctx, "payments", "task-1", "payments", "ax-ffffffffffffffff", ""); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("EnsureActorFromTag template mismatch = %v, want FailedPrecondition", err)
	}

	// Paged ListActorsPage iterates deterministically across pages.
	for _, name := range []string{"task-2", "task-3"} {
		if _, err := rc.EnsureActorFromTag(ctx, "payments", name, "payments", "ax-0123456789abcdef", ""); err != nil {
			t.Fatalf("EnsureActorFromTag(%s): %v", name, err)
		}
	}
	page1, tok, err := rc.ListActorsPage(ctx, "payments", 2, "")
	if err != nil || len(page1) != 2 || tok == "" {
		t.Fatalf("ListActorsPage page 1 = (len=%d, tok=%q, err=%v), want 2 items + token", len(page1), tok, err)
	}
	page2, tok2, err := rc.ListActorsPage(ctx, "payments", 2, tok)
	if err != nil || len(page2) != 1 || tok2 != "" {
		t.Fatalf("ListActorsPage page 2 = (len=%d, tok=%q, err=%v), want 1 item + empty token", len(page2), tok2, err)
	}
}
