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

	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ImageConfig describes the resolved OCI image digest and default container
// execution metadata (ENTRYPOINT + CMD and WORKDIR).
type ImageConfig struct {
	Digest  string
	Argv    []string
	WorkDir string
}

// IsRetryableActionError reports whether err is a transient Substrate error
// that background action execution should back off and retry without failing
// the task: UNAVAILABLE, DEADLINE_EXCEEDED, ABORTED, INTERNAL, CANCELLED, and
// context deadline or cancellation errors.
//
// Cancellation is retryable because it says nothing about the action itself: a
// worker whose context is canceled during ax-server shutdown must leave the
// action queued for the next lease holder rather than failing the task
// permanently.
func IsRetryableActionError(err error) bool {
	if err == nil {
		return false
	}
	if IsTransientError(err) || errors.Is(err, context.Canceled) {
		return true
	}
	var gs interface{ GRPCStatus() *status.Status }
	if errors.As(err, &gs) && gs.GRPCStatus() != nil {
		switch gs.GRPCStatus().Code() {
		case codes.Internal, codes.Canceled:
			return true
		}
	}
	return false
}

// RuntimeClient is the Agent Substrate adapter interface used by background
// task action execution, drift sweeping, and workspace snapshot pre-warming.
// [*Client] implements RuntimeClient directly.
type RuntimeClient interface {
	EnsureAtespace(ctx context.Context, atespace string) error
	GetActorTemplate(ctx context.Context, atespace, templateName string) (*ateapipb.ActorTemplate, error)
	EnsureActorTemplate(ctx context.Context, tmpl *ateapipb.ActorTemplate) (*ateapipb.ActorTemplate, error)
	EnsureActorFromTag(ctx context.Context, atespace, actorName, templateAtespace, templateName, sourceTagName string) (*ateapipb.Actor, error)
	EnsureActorEgress(ctx context.Context, atespace, actorName string, gatewayRules []*ateapipb.EgressRule) error
	ResumeActor(ctx context.Context, atespace, actorName string) (*ateapipb.Actor, string, error)
	SuspendActorSync(ctx context.Context, atespace, actorName string) (*ateapipb.Actor, int32, error)
	RevertActor(ctx context.Context, atespace, actorName string) (*ateapipb.Actor, int32, error)
	DeleteActor(ctx context.Context, atespace, actorName string) error
	GetActor(ctx context.Context, atespace, actorName string) (*ateapipb.Actor, error)
	ListActorsPage(ctx context.Context, atespace string, pageSize int32, pageToken string) ([]*ateapipb.Actor, string, error)
	CreateTag(ctx context.Context, atespace, tagName, sourceActorName string) (*ateapipb.Tag, error)
	GetTag(ctx context.Context, atespace, tagName string) (*ateapipb.Tag, error)
	DeleteTag(ctx context.Context, atespace, tagName string) error
}

var _ RuntimeClient = (*Client)(nil)
