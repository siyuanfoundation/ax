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

package substrate

import (
	"context"
	"errors"
	"testing"

	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// revertStub scripts RevertActor results call by call and serves one actor
// (or one error) from GetActor.
type revertStub struct {
	ateapipb.ControlClient

	revertErrs  []error // result of each RevertActor call in order; nil means success
	revertCalls int
	actor       *ateapipb.Actor
	getErr      error
}

func (s *revertStub) RevertActor(_ context.Context, _ *ateapipb.RevertActorRequest, _ ...grpc.CallOption) (*ateapipb.RevertActorResponse, error) {
	i := s.revertCalls
	s.revertCalls++
	if i < len(s.revertErrs) && s.revertErrs[i] != nil {
		return nil, s.revertErrs[i]
	}
	return &ateapipb.RevertActorResponse{Actor: &ateapipb.Actor{
		Status: &ateapipb.ActorStatus{
			State:     ateapipb.ActorState_ACTOR_STATE_SUSPENDED,
			Snapshots: []*ateapipb.Snapshot{durableSnapshot(3)},
		},
	}}, nil
}

func (s *revertStub) GetActor(_ context.Context, _ *ateapipb.GetActorRequest, _ ...grpc.CallOption) (*ateapipb.Actor, error) {
	if s.getErr != nil {
		return nil, s.getErr
	}
	return s.actor, nil
}

func durableSnapshot(gen int32) *ateapipb.Snapshot {
	return &ateapipb.Snapshot{
		Generation: gen,
		DurableSnapshot: &ateapipb.SnapshotStorage{
			State: ateapipb.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_COMPLETED,
		},
	}
}

func actorIn(state ateapipb.ActorState, snaps ...*ateapipb.Snapshot) *ateapipb.Actor {
	return &ateapipb.Actor{Status: &ateapipb.ActorStatus{State: state, Snapshots: snaps}}
}

// When the initial RevertActor RPC returns FailedPrecondition, Client.RevertActor
// inspects the actor and retries if appropriate. It must return the error from
// that follow-up step rather than the initial FailedPrecondition so queue
// workers classify transient errors accurately via IsRetryableActionError.
func TestClient_RevertActor_AfterFailedPrecondition(t *testing.T) {
	notRevertable := status.Error(codes.FailedPrecondition, "actor is not in a revertable state")
	localSnapshot := durableSnapshot(2)
	localSnapshot.Locality = "node-a"

	for _, tc := range []struct {
		name          string
		stub          *revertStub
		wantCode      codes.Code
		wantRetryable bool
		wantCalls     int
		wantGen       int32
	}{
		{
			name:      "already reverted to its durable snapshot is a no-op success",
			stub:      &revertStub{revertErrs: []error{notRevertable}, actor: actorIn(ateapipb.ActorState_ACTOR_STATE_SUSPENDED, durableSnapshot(2))},
			wantCode:  codes.OK,
			wantCalls: 1,
			wantGen:   2,
		},
		{
			name:      "now revertable and the second attempt succeeds",
			stub:      &revertStub{revertErrs: []error{notRevertable, nil}, actor: actorIn(ateapipb.ActorState_ACTOR_STATE_RUNNING)},
			wantCode:  codes.OK,
			wantCalls: 2,
			wantGen:   3,
		},
		{
			name:      "suspended with a node-local snapshot is reverted again",
			stub:      &revertStub{revertErrs: []error{notRevertable, nil}, actor: actorIn(ateapipb.ActorState_ACTOR_STATE_SUSPENDED, localSnapshot)},
			wantCode:  codes.OK,
			wantCalls: 2,
			wantGen:   3,
		},
		{
			name: "second attempt fails transiently",
			stub: &revertStub{
				revertErrs: []error{notRevertable, status.Error(codes.Internal, "atelet restarting")},
				actor:      actorIn(ateapipb.ActorState_ACTOR_STATE_CRASHED),
			},
			wantCode:      codes.Internal,
			wantRetryable: true,
			wantCalls:     2,
		},
		{
			name: "second attempt is canceled",
			stub: &revertStub{
				revertErrs: []error{notRevertable, status.Error(codes.Canceled, "context canceled")},
				actor:      actorIn(ateapipb.ActorState_ACTOR_STATE_RUNNING),
			},
			wantCode:      codes.Canceled,
			wantRetryable: true,
			wantCalls:     2,
		},
		{
			name:          "reading the actor fails transiently",
			stub:          &revertStub{revertErrs: []error{notRevertable}, getErr: status.Error(codes.Internal, "store hiccup")},
			wantCode:      codes.Internal,
			wantRetryable: true,
			wantCalls:     1,
		},
		{
			name:      "actor no longer exists",
			stub:      &revertStub{revertErrs: []error{notRevertable}, getErr: status.Error(codes.NotFound, "actor not found")},
			wantCode:  codes.NotFound,
			wantCalls: 1,
		},
		{
			name:          "actor in a transitional state",
			stub:          &revertStub{revertErrs: []error{notRevertable}, actor: actorIn(ateapipb.ActorState_ACTOR_STATE_RESUMING)},
			wantCode:      codes.Aborted,
			wantRetryable: true,
			wantCalls:     1,
		},
		{
			name:      "actor in a state that can never be reverted",
			stub:      &revertStub{revertErrs: []error{notRevertable}, actor: actorIn(ateapipb.ActorState_ACTOR_STATE_DELETING)},
			wantCode:  codes.FailedPrecondition,
			wantCalls: 1,
		},
		{
			name:      "second attempt is still not revertable",
			stub:      &revertStub{revertErrs: []error{notRevertable, notRevertable}, actor: actorIn(ateapipb.ActorState_ACTOR_STATE_RUNNING)},
			wantCode:  codes.FailedPrecondition,
			wantCalls: 2,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &Client{control: tc.stub}
			actor, gen, err := c.RevertActor(context.Background(), "payments", "task-1")

			var code codes.Code
			if err != nil {
				var gs interface{ GRPCStatus() *status.Status }
				if !errors.As(err, &gs) {
					t.Fatalf("error %v carries no gRPC status", err)
				}
				code = gs.GRPCStatus().Code()
			}
			if code != tc.wantCode {
				t.Errorf("code = %s (err: %v), want %s", code, err, tc.wantCode)
			}
			if got := IsRetryableActionError(err); got != tc.wantRetryable {
				t.Errorf("IsRetryableActionError = %v (err: %v), want %v", got, err, tc.wantRetryable)
			}
			if tc.stub.revertCalls != tc.wantCalls {
				t.Errorf("RevertActor calls = %d, want %d", tc.stub.revertCalls, tc.wantCalls)
			}
			if tc.wantCode == codes.OK {
				if actor.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_SUSPENDED || gen != tc.wantGen {
					t.Errorf("result = (state %s, generation %d), want (SUSPENDED, %d)", actor.GetStatus().GetState(), gen, tc.wantGen)
				}
			} else if actor != nil || gen != 0 {
				t.Errorf("result on error = (%v, %d), want (nil, 0)", actor, gen)
			}
		})
	}
}
