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

package substrate_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/ax/internal/substrate"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// IsRetryableActionError distinguishes retryable Substrate errors from
// permanent failures. Context cancellation says nothing about the action
// itself, so a worker canceled during ax-server shutdown must treat the
// cancellation as retryable rather than permanent.
func TestIsRetryableActionError(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"context.Canceled", context.Canceled, true},
		{"wrapped context.Canceled", fmt.Errorf("resuming actor: %w", context.Canceled), true},
		{"grpc Canceled", status.Error(codes.Canceled, "context canceled"), true},
		{"wrapped grpc Canceled", fmt.Errorf("resuming actor: %w", status.Error(codes.Canceled, "x")), true},
		{"context.DeadlineExceeded", context.DeadlineExceeded, true},
		{"grpc DeadlineExceeded", status.Error(codes.DeadlineExceeded, "x"), true},
		{"grpc Unavailable", status.Error(codes.Unavailable, "x"), true},
		{"grpc Aborted", status.Error(codes.Aborted, "x"), true},
		{"grpc Internal", status.Error(codes.Internal, "x"), true},
		{"wrapped grpc Internal", fmt.Errorf("creating tag: %w", status.Error(codes.Internal, "x")), true},
		{"grpc NotFound", status.Error(codes.NotFound, "x"), false},
		{"grpc FailedPrecondition", status.Error(codes.FailedPrecondition, "x"), false},
		{"grpc InvalidArgument", status.Error(codes.InvalidArgument, "x"), false},
		{"grpc PermissionDenied", status.Error(codes.PermissionDenied, "x"), false},
		// Capacity is reported to the state machine as CapacityUnavailable,
		// which has its own bounded backoff, not retried as a transient error.
		{"grpc ResourceExhausted", status.Error(codes.ResourceExhausted, "x"), false},
		{"plain error", fmt.Errorf("boom"), false},
	} {
		if got := substrate.IsRetryableActionError(tc.err); got != tc.want {
			t.Errorf("IsRetryableActionError(%s) = %v, want %v", tc.name, got, tc.want)
		}
	}
}
