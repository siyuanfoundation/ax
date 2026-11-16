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

	"github.com/google/ax/internal/apivalidation"
	"github.com/google/ax/internal/controller"
	"github.com/google/ax/internal/substrate"
	"github.com/google/ax/pkg/apis/v1alpha1"
	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// RuntimeClient abstracts the idempotent Agent Substrate operations executed by
// the task reconciliation loop.
//
// In production, [NewSubstrateRuntime] adapts [*substrate.Client] so that
// internal/substrate remains the sole package speaking directly to the
// Substrate Control gRPC API. Unit tests may supply either a
// [NewSubstrateRuntime] backed by substratetest.Server or a custom fake.
type RuntimeClient interface {
	EnsureTemplate(ctx context.Context, atespace, templateName string, profile *v1alpha1.Profile) error
	EnsureActor(ctx context.Context, atespace, actorName, templateName, sourceTag string) (actorUID string, err error)
	EnsureActorEgress(ctx context.Context, atespace, actorName string, rules []*ateapipb.EgressRule) error
	ResumeActor(ctx context.Context, atespace, actorName string) error
	SuspendActorSync(ctx context.Context, atespace, actorName string) (snapshotGen int32, err error)
	RevertActor(ctx context.Context, atespace, actorName string) (snapshotGen int32, err error)
	DeleteActor(ctx context.Context, atespace, actorName, expectedUID string) error
	CreateSnapshotTag(ctx context.Context, atespace, actorName, tagName string) error
	DeleteSnapshotTag(ctx context.Context, atespace, tagName string) error
	GetActor(ctx context.Context, atespace, actorName string) (*ateapipb.Actor, error)
	GetTag(ctx context.Context, atespace, tagName string) (*ateapipb.Tag, error)
}

// SubstrateRuntimeOptions configures [NewSubstrateRuntime].
type SubstrateRuntimeOptions struct {
	// DefaultTaskImage is used when an effective Profile omits both image and
	// ate_actor_template (defaults to v1alpha1.DefaultTaskImage).
	DefaultTaskImage string
	// RunnerImage is the version-pinned ax-task-runner image included in the
	// content hash of AX-managed Profile ActorTemplates.
	RunnerImage string
}

// SubstrateRuntime implements [RuntimeClient] on top of [*substrate.Client].
//
// TODO: Add rolling-upgrade template-skew retry handling and pre-existing
// foreign-actor orphan guards once basic end-to-end reconciliation is in place.
type SubstrateRuntime struct {
	client           *substrate.Client
	defaultTaskImage string
	runnerImage      string
}

// NewSubstrateRuntime wraps client as a [RuntimeClient].
func NewSubstrateRuntime(client *substrate.Client, opts SubstrateRuntimeOptions) *SubstrateRuntime {
	if client == nil {
		panic("reconciler: NewSubstrateRuntime requires a non-nil *substrate.Client")
	}
	defaultImage := opts.DefaultTaskImage
	if defaultImage == "" {
		defaultImage = v1alpha1.DefaultTaskImage
	}
	return &SubstrateRuntime{
		client:           client,
		defaultTaskImage: defaultImage,
		runnerImage:      opts.RunnerImage,
	}
}

// EnsureTemplate ensures the atespace and ActorTemplate exist on Substrate.
func (r *SubstrateRuntime) EnsureTemplate(ctx context.Context, atespace, templateName string, profile *v1alpha1.Profile) error {
	ctx = substrate.WithSingleAttempt(ctx)
	if atespace == "" {
		atespace = v1alpha1.DefaultAtespace
	}
	if err := r.client.EnsureAtespace(ctx, atespace); err != nil {
		return err
	}

	var eff *v1alpha1.Profile
	if profile != nil {
		eff = proto.Clone(profile).(*v1alpha1.Profile)
	} else {
		eff = &v1alpha1.Profile{}
	}
	if eff.GetAteActorTemplate() == "" {
		if err := apivalidation.ValidateResources(eff.GetResources()); err != nil {
			return status.Error(codes.InvalidArgument, err.Error())
		}
	}

	resolvedName, tmpl, err := controller.ResolveProfileTemplate(atespace, eff, r.defaultTaskImage, r.runnerImage)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	if tmpl == nil {
		target := resolvedName
		if templateName != "" {
			target = templateName
		}
		_, err := r.client.GetActorTemplate(ctx, atespace, target)
		return err
	}
	_, err = r.client.EnsureActorTemplate(ctx, tmpl)
	return err
}

// EnsureActor creates or reuses the task's Actor on Substrate and returns its UID.
func (r *SubstrateRuntime) EnsureActor(ctx context.Context, atespace, actorName, templateName, sourceTag string) (string, error) {
	ctx = substrate.WithSingleAttempt(ctx)
	if templateName == "" {
		return "", status.Errorf(codes.InvalidArgument, "actor %s/%s requires a non-empty ActorTemplate name", atespace, actorName)
	}
	if atespace == "" {
		atespace = v1alpha1.DefaultAtespace
	}
	if err := r.client.EnsureAtespace(ctx, atespace); err != nil {
		return "", err
	}
	var (
		actor *ateapipb.Actor
		err   error
	)
	if sourceTag != "" {
		actor, err = r.client.EnsureActorFromTag(ctx, atespace, actorName, atespace, templateName, sourceTag)
	} else {
		actor, err = r.client.EnsureActor(ctx, atespace, actorName, atespace, templateName)
	}
	if err != nil {
		return "", err
	}
	uid := actor.GetMetadata().GetUid()
	if uid == "" {
		return "", status.Errorf(codes.Internal, "substrate returned empty UID for actor %s/%s", atespace, actorName)
	}
	return uid, nil
}

// EnsureActorEgress writes the Actor's singleton EgressPolicy idempotently.
func (r *SubstrateRuntime) EnsureActorEgress(ctx context.Context, atespace, actorName string, rules []*ateapipb.EgressRule) error {
	ctx = substrate.WithSingleAttempt(ctx)
	if len(rules) == 0 {
		return nil
	}
	if atespace == "" {
		atespace = v1alpha1.DefaultAtespace
	}
	return r.client.EnsureActorEgressPolicy(ctx, atespace, actorName, rules)
}

// ResumeActor resumes the Actor on Substrate.
func (r *SubstrateRuntime) ResumeActor(ctx context.Context, atespace, actorName string) error {
	ctx = substrate.WithSingleAttempt(ctx)
	if atespace == "" {
		atespace = v1alpha1.DefaultAtespace
	}
	_, _, err := r.client.ResumeActor(ctx, atespace, actorName)
	return err
}

// SuspendActorSync synchronously checkpoints and suspends the Actor.
func (r *SubstrateRuntime) SuspendActorSync(ctx context.Context, atespace, actorName string) (int32, error) {
	ctx = substrate.WithSingleAttempt(ctx)
	if atespace == "" {
		atespace = v1alpha1.DefaultAtespace
	}
	return r.client.SuspendActorSync(ctx, atespace, actorName)
}

// RevertActor rolls the Actor back to its latest durable snapshot.
func (r *SubstrateRuntime) RevertActor(ctx context.Context, atespace, actorName string) (int32, error) {
	ctx = substrate.WithSingleAttempt(ctx)
	if atespace == "" {
		atespace = v1alpha1.DefaultAtespace
	}
	return r.client.RevertActor(ctx, atespace, actorName)
}

// DeleteActor deletes the Actor matching expectedUID from Substrate.
func (r *SubstrateRuntime) DeleteActor(ctx context.Context, atespace, actorName, expectedUID string) error {
	ctx = substrate.WithSingleAttempt(ctx)
	if atespace == "" {
		atespace = v1alpha1.DefaultAtespace
	}
	return r.client.DeleteActorWithUID(ctx, atespace, actorName, expectedUID)
}

// CreateSnapshotTag idempotently creates a durable snapshot Tag from actorName.
func (r *SubstrateRuntime) CreateSnapshotTag(ctx context.Context, atespace, actorName, tagName string) error {
	ctx = substrate.WithSingleAttempt(ctx)
	if tagName == "" {
		return status.Error(codes.InvalidArgument, "snapshot tag name is required")
	}
	if atespace == "" {
		atespace = v1alpha1.DefaultAtespace
	}
	_, err := r.client.EnsureSnapshotTag(ctx, atespace, tagName, actorName)
	return err
}

// DeleteSnapshotTag deletes tagName from Substrate.
func (r *SubstrateRuntime) DeleteSnapshotTag(ctx context.Context, atespace, tagName string) error {
	ctx = substrate.WithSingleAttempt(ctx)
	if tagName == "" {
		return status.Error(codes.InvalidArgument, "snapshot tag name is required")
	}
	if atespace == "" {
		atespace = v1alpha1.DefaultAtespace
	}
	return r.client.DeleteTag(ctx, atespace, tagName)
}

// GetActor fetches an Actor from Substrate.
func (r *SubstrateRuntime) GetActor(ctx context.Context, atespace, actorName string) (*ateapipb.Actor, error) {
	ctx = substrate.WithSingleAttempt(ctx)
	if atespace == "" {
		atespace = v1alpha1.DefaultAtespace
	}
	return r.client.GetActor(ctx, atespace, actorName)
}

// GetTag fetches a Tag from Substrate.
func (r *SubstrateRuntime) GetTag(ctx context.Context, atespace, tagName string) (*ateapipb.Tag, error) {
	ctx = substrate.WithSingleAttempt(ctx)
	if atespace == "" {
		atespace = v1alpha1.DefaultAtespace
	}
	return r.client.GetTag(ctx, atespace, tagName)
}
