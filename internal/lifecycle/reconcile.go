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

package lifecycle

import (
	"math"
	"time"

	"github.com/google/ax/pkg/apis/v1alpha1"
)

// NextReconcileAt returns when state next requires background worker attention,
// or nil when the task is at rest and must remain out of the queue.
//
// Rules:
//  1. If pending actions exist, the row is due at max(now, actions[0].NotBefore).
//  2. Otherwise, if the task is in QUEUED (not deleting) with a ClaimDeadline,
//     the row is due at that exact ClaimDeadline (no periodic polling).
//  3. Steady-state tasks (WORKING, IDLE, SUSPENDED, terminal) return nil.
func NextReconcileAt(state State, now time.Time) *time.Time {
	if len(state.Actions) > 0 {
		due := now
		if nb := state.Actions[0].NotBefore; !nb.IsZero() && nb.After(now) {
			due = nb
		}
		return &due
	}
	if state.Task.GetMetadata().GetDeleteTime() == nil &&
		state.Task.GetStatus().GetState() == v1alpha1.TaskState_TASK_STATE_QUEUED &&
		state.ClaimDeadline != nil {
		deadline := *state.ClaimDeadline
		return &deadline
	}
	return nil
}

// RetryBackoff computes a deterministic exponential backoff delay for retry
// index tries (0-based: tries=0 returns initial).
func RetryBackoff(tries int, initial, maxBackoff time.Duration, multiplier float64) time.Duration {
	if initial <= 0 {
		return 0
	}
	if maxBackoff <= 0 {
		maxBackoff = 5 * time.Minute
	}
	if multiplier < 1.0 {
		multiplier = 2.0
	}
	if tries <= 0 {
		if initial > maxBackoff {
			return maxBackoff
		}
		return initial
	}
	scaled := float64(initial) * math.Pow(multiplier, float64(tries))
	if math.IsNaN(scaled) || math.IsInf(scaled, 0) || scaled >= float64(maxBackoff) {
		return maxBackoff
	}
	return time.Duration(scaled)
}
