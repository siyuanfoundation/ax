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
	"errors"

	"github.com/google/ax/internal/lifecycle"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// errorClass categorizes an error returned by an Agent Substrate RPC so the
// worker knows whether to retry with backoff, feed CapacityUnavailable into the
// state machine, fail the task permanently, or stop cleanly after cancellation.
type errorClass int

const (
	errorClassNone errorClass = iota
	errorClassCanceled
	errorClassCapacityUnavailable
	errorClassTransient
	errorClassPermanent
)

// classifySubstrateError maps a Substrate RPC error for actionKind into its
// [errorClass].
//
// Classification rules:
//   - Local context cancellation (when callCtx.Err() == context.Canceled, such
//     as replica shutdown or cancellation of an obsolete action) ->
//     errorClassCanceled (neither fails the task nor burns a retry).
//   - RESOURCE_EXHAUSTED on RESUME_ACTOR -> errorClassCapacityUnavailable
//     (lifecycle.CapacityUnavailableInput, backing off while keeping the task
//     in TASK_STATE_QUEUED).
//     RESOURCE_EXHAUSTED on any other action (e.g. control-plane rate limiting)
//     is treated as transient.
//   - UNAVAILABLE, DEADLINE_EXCEEDED, INTERNAL, ABORTED, UNAUTHENTICATED
//     (projected SA token rotation / IDP hiccups), UNKNOWN, and network
//     transport errors -> errorClassTransient (jittered backoff, never fails a
//     task).
//   - INVALID_ARGUMENT, PERMISSION_DENIED, NOT_FOUND, ALREADY_EXISTS,
//     FAILED_PRECONDITION, UNIMPLEMENTED, OUT_OF_RANGE, and DATA_LOSS ->
//     errorClassPermanent (lifecycle.ActionFailedInput).
func classifySubstrateError(callCtx context.Context, kind lifecycle.ActionKind, err error) errorClass {
	if err == nil {
		return errorClassNone
	}
	if errors.Is(callCtx.Err(), context.Canceled) && (errors.Is(err, context.Canceled) || statusCode(err) == codes.Canceled) {
		return errorClassCanceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return errorClassTransient
	}

	code := statusCode(err)
	switch code {
	case codes.OK:
		return errorClassNone
	case codes.Canceled:
		if errors.Is(callCtx.Err(), context.Canceled) {
			return errorClassCanceled
		}
		return errorClassTransient
	case codes.ResourceExhausted:
		if kind == lifecycle.ActionResumeActor {
			return errorClassCapacityUnavailable
		}
		return errorClassTransient
	case codes.Unavailable, codes.DeadlineExceeded, codes.Internal, codes.Aborted, codes.Unauthenticated:
		return errorClassTransient
	case codes.InvalidArgument,
		codes.PermissionDenied,
		codes.NotFound,
		codes.AlreadyExists,
		codes.FailedPrecondition,
		codes.Unimplemented,
		codes.OutOfRange,
		codes.DataLoss:
		return errorClassPermanent
	case codes.Unknown:
		// Non-gRPC transport errors and unknown RPC failures are retried
		// transiently so temporary connectivity blips never fail tasks.
		return errorClassTransient
	default:
		return errorClassTransient
	}
}

// statusCode extracts the gRPC status code from err, unwrapping fmt.Errorf
// error chains when present.
func statusCode(err error) codes.Code {
	if err == nil {
		return codes.OK
	}
	var gs interface{ GRPCStatus() *status.Status }
	if errors.As(err, &gs) {
		if st := gs.GRPCStatus(); st != nil {
			return st.Code()
		}
	}
	return codes.Unknown
}

// substrateMessage extracts the clean gRPC status description from a possibly
// wrapped error so task failure details omit "rpc error: code = ... desc = "
// boilerplate.
func substrateMessage(err error) string {
	if err == nil {
		return ""
	}
	var gs interface{ GRPCStatus() *status.Status }
	if errors.As(err, &gs) {
		if st := gs.GRPCStatus(); st != nil && st.Message() != "" {
			return st.Message()
		}
	}
	return err.Error()
}
