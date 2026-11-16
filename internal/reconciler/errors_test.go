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

package reconciler

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/google/ax/internal/lifecycle"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestClassifySubstrateErrorAndMessageUnwrap(t *testing.T) {
	bg := context.Background()
	canceledCtx, cancel := context.WithCancel(bg)
	cancel()

	tests := []struct {
		name string
		ctx  context.Context
		kind lifecycle.ActionKind
		err  error
		want errorClass
	}{
		{
			name: "nil error",
			ctx:  bg,
			kind: lifecycle.ActionResumeActor,
			err:  nil,
			want: errorClassNone,
		},
		{
			name: "local context canceled",
			ctx:  canceledCtx,
			kind: lifecycle.ActionResumeActor,
			err:  fmt.Errorf("resuming: %w", status.Error(codes.Canceled, "client canceled")),
			want: errorClassCanceled,
		},
		{
			name: "server-originated Canceled when callCtx active is transient",
			ctx:  bg,
			kind: lifecycle.ActionResumeActor,
			err:  status.Error(codes.Canceled, "upstream connection reset"),
			want: errorClassTransient,
		},
		{
			name: "ResourceExhausted on RESUME_ACTOR is CapacityUnavailable",
			ctx:  bg,
			kind: lifecycle.ActionResumeActor,
			err:  fmt.Errorf("resuming: %w", status.Error(codes.ResourceExhausted, "no worker room")),
			want: errorClassCapacityUnavailable,
		},
		{
			name: "ResourceExhausted on non-resume action is transient",
			ctx:  bg,
			kind: lifecycle.ActionCreateActor,
			err:  status.Error(codes.ResourceExhausted, "api rate limit"),
			want: errorClassTransient,
		},
		{
			name: "Unauthenticated SA token rotation is transient",
			ctx:  bg,
			kind: lifecycle.ActionEnsureTemplate,
			err:  status.Error(codes.Unauthenticated, "token expired"),
			want: errorClassTransient,
		},
		{
			name: "EOF transport error is transient",
			ctx:  bg,
			kind: lifecycle.ActionSuspendActorSync,
			err:  io.ErrUnexpectedEOF,
			want: errorClassTransient,
		},
		{
			name: "InvalidArgument wrapped error is permanent",
			ctx:  bg,
			kind: lifecycle.ActionEnsureTemplate,
			err:  fmt.Errorf("creating template: %w", status.Error(codes.InvalidArgument, "image must be digest-pinned")),
			want: errorClassPermanent,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := classifySubstrateError(tc.ctx, tc.kind, tc.err); got != tc.want {
				t.Fatalf("classifySubstrateError() = %v, want %v", got, tc.want)
			}
		})
	}

	wrapped := fmt.Errorf("creating actor: %w", status.Error(codes.InvalidArgument, "must be pinned by digest"))
	if msg := substrateMessage(wrapped); msg != "must be pinned by digest" {
		t.Fatalf("substrateMessage() = %q, want clean gRPC description", msg)
	}
}
