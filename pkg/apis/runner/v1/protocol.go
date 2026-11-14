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

// Package runnerv1 defines the HTTP + protojson wire contract between
// ax-task-runner (PID 1 inside Agent Substrate sandboxes) and ax-server's
// runner control endpoint (:8081).
//
// # Sandbox State & Snapshot Identity Contract
//
// Root-only runner state under [RunnerStateDir] (/var/lib/ax, mode 0700) is
// persisted on a durable volume so that run_id and runner_token survive cold
// restores of the same actor. Because a Task actor can also be seeded from a
// pre-warmed Workspace snapshot Tag created by a temporary setup actor (which
// has a different Substrate actor UID), every file persisted under
// [RunnerStateDir] (including [RunnerTokenFile] and [RunnerStateFile]) MUST
// bind and verify the current sandbox's actor_uid read from [ActorUIDPath] on
// each boot/wakeup, discarding any inherited state whose actor_uid does not
// match.
package runnerv1

import "strings"

const (
	// ClaimAssignmentPath is the HTTP POST endpoint where ax-task-runner pulls
	// its next assignment on boot and wakeup.
	ClaimAssignmentPath = "/runner/v1/assignment:claim"

	// ReportEventsPath is the HTTP POST endpoint where ax-task-runner reports
	// sequenced lifecycle events and heartbeats.
	ReportEventsPath = "/runner/v1/events"

	// RunnerTokenHeader carries the root-only per-actor secret minted on the
	// actor's initial claim, distinguishing PID 1 (uid 0) from the untrusted
	// task process (DefaultTaskUID).
	RunnerTokenHeader = "X-AX-Runner-Token"

	// RunnerAudience is the OIDC JWT audience injected by Substrate's egress
	// gateway on requests to the runner service.
	RunnerAudience = "ax-runner"

	// RunnerServiceHost is the in-cluster DNS host(:port) targeted by the
	// built-in runner egress rule.
	RunnerServiceHost = "ax-runner.ax-system.svc.cluster.local:8081"

	// DefaultTaskUID is the non-root UID under which ax-task-runner executes
	// the task command.
	DefaultTaskUID int32 = 1000

	// RunnerStateDir is the root-only (0700) durable directory storing runner
	// state across cold restores.
	RunnerStateDir = "/var/lib/ax"

	// RunnerTokenFile stores the current actor_uid and runner_token under
	// RunnerStateDir.
	RunnerTokenFile = "/var/lib/ax/runner_token"

	// RunnerStateFile stores the current actor_uid and last_started_run_id
	// under RunnerStateDir.
	RunnerStateFile = "/var/lib/ax/state.json"

	// ActorUIDPath is the SystemInfo actor metadata file exposing the live
	// sandbox's actor UID; re-read on every wakeup.
	//
	// Note for ActorTemplate synthesis: Agent Substrate rejects nested volume
	// MountPaths ("mounts must not nest"), so ActorMetadata (Path: "actor-uid")
	// and TrustBundle (Path: "egress-trust-bundle/trust-bundle.pem") must be
	// combined as two DataSources inside a single SystemInfo volume mounted at
	// "/run/ate".
	ActorUIDPath = "/run/ate/actor-uid"

	// SetupActorPrefix prefixes temporary Workspace setup actor names so the
	// :8081 runner server routes their claims and events to the workspace setup
	// handler instead of looking up a Task row.
	SetupActorPrefix = "ax-ws-setup-"
)

// IsSetupActorName reports whether actorName belongs to a temporary Workspace
// setup actor rather than a Task actor.
func IsSetupActorName(actorName string) bool {
	return strings.HasPrefix(actorName, SetupActorPrefix)
}
