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

package server

import (
	"context"
	"time"

	"github.com/google/ax/internal/store"
	"github.com/google/ax/internal/substrate"
	runnerv1 "github.com/google/ax/pkg/apis/runner/v1"
)

// TaskReconcileTrigger is an optional hook invoked synchronously after a task
// mutation commits via store.Apply. Inline reconcile mode implements this
// interface to execute pending actions immediately; in background queue mode it
// is nil because store.Apply already signals DueNotify.
type TaskReconcileTrigger interface {
	OnTaskApplied(ctx context.Context, key store.TaskKey) error
}

// HeartbeatRecorder ingests runner liveness signals from the :8081 runner HTTP
// server into an in-memory heartbeat buffer for batched SQL flushing.
type HeartbeatRecorder interface {
	RecordHeartbeat(key store.TaskKey, runnerTokenHash []byte, at time.Time)
}

// ImageConfigResolver resolves OCI image digests and default ENTRYPOINT + CMD /
// WORKDIR when TaskSpec.commands is empty.
type ImageConfigResolver interface {
	ResolveImageConfig(ctx context.Context, imageRef string) (*substrate.ImageConfig, error)
}

// SetupRunnerHandler handles :8081 runner claims and events for temporary
// Workspace setup actors (prefixed with [runnerv1.SetupActorPrefix]), keeping
// temporary setup actors separate from the tasks table.
//
// HTTP status mapping contract for the :8081 runner server:
//   - Actor JWT actorUID mismatch (e.g. 300s gateway JWT cache on recreated name): HTTP 401
//   - [github.com/google/ax/internal/lifecycle.ErrUnauthorizedRunner]: HTTP 401/403
//   - [store.ErrNotFound]: HTTP 404
//   - [store.ErrLockTimeout] or transient store unavailability: HTTP 503 (retried by runner)
type SetupRunnerHandler interface {
	ClaimSetup(ctx context.Context, atespace, actorName, actorUID string, providedTokenHash []byte, req *runnerv1.ClaimAssignmentRequest) (*runnerv1.ClaimAssignmentResponse, error)
	ReportSetupEvents(ctx context.Context, atespace, actorName, actorUID string, providedTokenHash []byte, req *runnerv1.ReportEventsRequest) (*runnerv1.ReportEventsResponse, error)
}
