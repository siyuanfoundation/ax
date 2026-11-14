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

package controller

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/google/ax/internal/resources"
	"github.com/google/ax/internal/substrate"
	"github.com/google/ax/pkg/apis/v1alpha1"
	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/protobuf/proto"
)

// EffectiveProfile selects the compute Profile for a Task as a whole unit,
// without field-level merging, without mutating either input, and without
// filling server defaults.
//
// Workspace.spec.profile exists to pre-warm a default workspace snapshot for
// fast warm task startup; it does not constrain or bleed fields into an
// explicit Task.spec.profile:
//   - When taskProfile is non-empty (sets image, ate_actor_template, or any
//     CPU/memory limit), EffectiveProfile returns a copy of taskProfile as-is
//     and ignores workspaceProfile completely. Omitted fields within
//     taskProfile fall back to server/Substrate defaults, never to
//     workspaceProfile. If the resulting profile differs from workspaceProfile,
//     the task simply misses the pre-warmed snapshot and cold-boots workspace
//     setup in its own sandbox profile.
//   - When taskProfile is nil or empty, EffectiveProfile falls back to a copy
//     of workspaceProfile (if non-empty) so the task runs on the Workspace's
//     pre-warmed profile, or returns an empty Profile when both are empty.
func EffectiveProfile(workspaceProfile, taskProfile *v1alpha1.Profile) *v1alpha1.Profile {
	if !isProfileEmpty(taskProfile) {
		return proto.Clone(taskProfile).(*v1alpha1.Profile)
	}
	if !isProfileEmpty(workspaceProfile) {
		return proto.Clone(workspaceProfile).(*v1alpha1.Profile)
	}
	return &v1alpha1.Profile{}
}

func isProfileEmpty(p *v1alpha1.Profile) bool {
	return p.GetAteActorTemplate() == "" &&
		p.GetImage() == "" &&
		p.GetResources().GetLimits().GetCpu() == "" &&
		p.GetResources().GetLimits().GetMemory() == ""
}

// DefaultProfileSnapshotFidelity is the Substrate snapshot fidelity configured
// on AX-managed Profile ActorTemplates so rootfs package installs from
// workspace setup commands survive snapshots.
const DefaultProfileSnapshotFidelity = ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS

type profile struct {
	image         string
	cpuMillicores int64
	memoryBytes   int64
	// Defaults to DefaultProfileSnapshotFidelity (SNAPSHOT_FIDELITY_ROOTFS).
	snapshotFidelity ateapipb.SnapshotFidelity
}

func (p profile) templateName(runnerImage string) string {
	h := sha256.New()
	_, _ = fmt.Fprintf(h, "%s\x00%s\x00%d\x00%d\x00%s",
		p.image,
		runnerImage,
		p.cpuMillicores,
		p.memoryBytes,
		p.snapshotFidelity.String(),
	)
	sum := h.Sum(nil)
	return "ax-" + hex.EncodeToString(sum[:8])
}

// ResolveProfileTemplate resolves p into a deterministic Substrate
// ActorTemplate name and specification using [v1alpha1.DefaultTaskImage] when p
// omits image. Callers that configure a custom server default task image should
// call [ResolveProfileTemplateWithDefaults] so workspace snapshot pre-warming
// and task provisioning always hash to the exact same template name.
func ResolveProfileTemplate(atespace string, p *v1alpha1.Profile, runnerImage string) (string, *ateapipb.ActorTemplate, error) {
	return ResolveProfileTemplateWithDefaults(atespace, p, v1alpha1.DefaultTaskImage, runnerImage)
}

// ResolveProfileTemplateWithDefaults resolves p into a deterministic Substrate
// ActorTemplate name and specification, falling back to defaultTaskImage (or
// [v1alpha1.DefaultTaskImage] when empty) when p omits image.
//
// When p sets ate_actor_template, it returns that template name with a nil
// *ateapipb.ActorTemplate so the caller looks up the existing user-managed
// template on Substrate rather than creating an AX-managed template.
func ResolveProfileTemplateWithDefaults(atespace string, p *v1alpha1.Profile, defaultTaskImage, runnerImage string) (string, *ateapipb.ActorTemplate, error) {
	if atespace == "" {
		atespace = v1alpha1.DefaultAtespace
	}
	if tpl := p.GetAteActorTemplate(); tpl != "" {
		return tpl, nil, nil
	}

	image := p.GetImage()
	if image == "" {
		if defaultTaskImage != "" {
			image = defaultTaskImage
		} else {
			image = v1alpha1.DefaultTaskImage
		}
	}

	cpuMillicores := substrate.DefaultCPUMillicores
	if cpu := p.GetResources().GetLimits().GetCpu(); cpu != "" {
		parsed, err := resources.ParseCPUMillicores(cpu)
		if err != nil {
			return "", nil, fmt.Errorf("profile cpu limit: %w", err)
		}
		cpuMillicores = parsed
	}

	memoryBytes := substrate.DefaultMemoryBytes
	if mem := p.GetResources().GetLimits().GetMemory(); mem != "" {
		parsed, err := resources.ParseMemoryBytes(mem)
		if err != nil {
			return "", nil, fmt.Errorf("profile memory limit: %w", err)
		}
		memoryBytes = parsed
	}

	resolved := profile{
		image:            image,
		cpuMillicores:    cpuMillicores,
		memoryBytes:      memoryBytes,
		snapshotFidelity: DefaultProfileSnapshotFidelity,
	}
	name := resolved.templateName(runnerImage)

	tmpl := substrate.BuildActorTemplate(atespace, name, image, nil, nil, "", &ateapipb.ResourceRequirements{
		Limits: &ateapipb.Resources{
			CpuMillicores: cpuMillicores,
			MemoryBytes:   memoryBytes,
		},
	})
	tmpl.SnapshotConfig.PreferredFidelity = resolved.snapshotFidelity
	return name, tmpl, nil
}
