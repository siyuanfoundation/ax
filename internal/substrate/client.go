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
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"path"
	"slices"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v5"
	"github.com/google/ax/internal/resources"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"github.com/google/uuid"
	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// ClientOptions configures connection to the Substrate Control API server.
type ClientOptions struct {
	Target      string
	Authority   string
	TokenFile   string
	CAFile      string
	InsecureTLS bool
	Plaintext   bool
}

type tokenAuth struct {
	tokenFile string
}

func (t tokenAuth) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	if t.tokenFile == "" {
		return nil, nil
	}
	b, err := os.ReadFile(t.tokenFile)
	if err != nil {
		return nil, fmt.Errorf("reading token file %q: %w", t.tokenFile, err)
	}
	return map[string]string{
		"authorization": "Bearer " + strings.TrimSpace(string(b)),
	}, nil
}

func (t tokenAuth) RequireTransportSecurity() bool {
	return false
}

// Client wraps the Substrate Control API client.
type Client struct {
	conn    *grpc.ClientConn
	control ateapipb.ControlClient
}

// NewClientWithOptions connects to Substrate with full TLS and auth configuration.
func NewClientWithOptions(opts ClientOptions, extraDialOpts ...grpc.DialOption) (*Client, error) {
	if opts.Target == "" {
		opts.Target = "api.ate-system.svc.cluster.local:443"
	}

	// Auto-detect token file if not specified
	if opts.TokenFile == "" {
		if envToken := os.Getenv("SUBSTRATE_TOKEN_FILE"); envToken != "" {
			opts.TokenFile = envToken
		} else if _, err := os.Stat("/var/run/secrets/ateapi/token"); err == nil {
			opts.TokenFile = "/var/run/secrets/ateapi/token"
		}
	}

	// Auto-detect CA file if not specified
	if opts.CAFile == "" {
		if envCA := os.Getenv("SUBSTRATE_CA_FILE"); envCA != "" {
			opts.CAFile = envCA
		} else if _, err := os.Stat("/run/servicedns-ca/trust-bundle.pem"); err == nil {
			opts.CAFile = "/run/servicedns-ca/trust-bundle.pem"
		} else if _, err := os.Stat("/run/servicedns-ca/ca.pem"); err == nil {
			opts.CAFile = "/run/servicedns-ca/ca.pem"
		}
	}

	dialOpts := append([]grpc.DialOption{}, extraDialOpts...)

	if opts.Plaintext {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	} else {
		tlsConfig := &tls.Config{}
		if opts.InsecureTLS {
			tlsConfig.InsecureSkipVerify = true
		}
		if opts.Authority != "" {
			tlsConfig.ServerName = opts.Authority
		} else if strings.Contains(opts.Target, "api.ate-system.svc") {
			tlsConfig.ServerName = "api.ate-system.svc"
		}

		if opts.CAFile != "" {
			caCert, err := os.ReadFile(opts.CAFile)
			if err != nil {
				return nil, fmt.Errorf("reading CA file %q: %w", opts.CAFile, err)
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(caCert) {
				return nil, fmt.Errorf("failed to parse CA certificate from %q", opts.CAFile)
			}
			tlsConfig.RootCAs = pool
		}

		dialOpts = append(dialOpts, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
	}

	authority := opts.Authority
	if authority == "" && strings.Contains(opts.Target, "api.ate-system.svc") {
		authority = "api.ate-system.svc"
	}
	if authority != "" {
		dialOpts = append(dialOpts, grpc.WithAuthority(authority))
	}

	if opts.TokenFile != "" {
		dialOpts = append(dialOpts, grpc.WithPerRPCCredentials(tokenAuth{tokenFile: opts.TokenFile}))
	}

	conn, err := grpc.NewClient(opts.Target, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Substrate control service at %s: %w", opts.Target, err)
	}
	return &Client{
		conn:    conn,
		control: ateapipb.NewControlClient(conn),
	}, nil
}

// NewClient establishes a connection to the Substrate Control API server.
func NewClient(target string, opts ...grpc.DialOption) (*Client, error) {
	if len(opts) > 0 {
		if target == "" {
			target = "api.ate-system.svc.cluster.local:443"
		}
		conn, err := grpc.NewClient(target, opts...)
		if err != nil {
			return nil, fmt.Errorf("failed to connect to Substrate control service at %s: %w", target, err)
		}
		return &Client{
			conn:    conn,
			control: ateapipb.NewControlClient(conn),
		}, nil
	}

	// Auto-configure options when no explicit dial options given
	return NewClientWithOptions(ClientOptions{Target: target})
}

// Close closes the gRPC connection.
func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// EnsureAtespace creates the atespace if it does not already exist.
func (c *Client) EnsureAtespace(ctx context.Context, atespace string) error {
	req := &ateapipb.CreateAtespaceRequest{
		Atespace: &ateapipb.Atespace{
			Metadata: &ateapipb.ResourceMetadata{
				Name: atespace,
			},
		},
	}
	_, err := callWithRetry(ctx, func() (*ateapipb.Atespace, error) {
		return c.control.CreateAtespace(ctx, req)
	})
	if err != nil && status.Code(err) != codes.AlreadyExists {
		return fmt.Errorf("creating atespace %q: %w", atespace, err)
	}
	return nil
}

// GetActorTemplate fetches an ActorTemplate by name.
func (c *Client) GetActorTemplate(ctx context.Context, atespace, templateName string) (*ateapipb.ActorTemplate, error) {
	req := &ateapipb.GetActorTemplateRequest{
		ActorTemplate: &ateapipb.ObjectRef{
			Atespace: atespace,
			Name:     templateName,
		},
	}
	return callWithRetry(ctx, func() (*ateapipb.ActorTemplate, error) {
		return c.control.GetActorTemplate(ctx, req)
	})
}

const (
	DefaultGuestCommand = "/usr/local/bin/ax-task-runner"

	// defaultRetryAttempts is the maximum number of calls retryOnCodes makes. With
	// exponential backoff from defaultRetryInterval capped at maxRetryInterval
	// (±50% jitter), the total wait across all attempts is roughly 6s-20s.
	defaultRetryAttempts = 10

	// retryBackoffFactor is the multiplier applied to the backoff after each failed attempt.
	retryBackoffFactor = 2

	// actorDeletingWaitAttempts bounds how many times EnsureActor retries creation
	// while a previous actor with the same name is still being deleted.
	actorDeletingWaitAttempts = 20

	// deleteFinalizePollAttempts bounds how many GetActor / GetTag polls
	// DeleteActor and DeleteTag perform before returning a retryable Unavailable
	// status so a wedged resource cannot stall a caller indefinitely.
	deleteFinalizePollAttempts = 150
)

var (
	// defaultRetryInterval is the initial backoff for retryOnCodes and the poll
	// interval used while waiting for actor deletion.
	defaultRetryInterval = 200 * time.Millisecond

	// maxRetryInterval caps the exponential backoff in retryOnCodes.
	maxRetryInterval = 2 * time.Second

	// actorDeletingWaitInterval is the interval between creation attempts while a
	// previous actor is still deleting (actorDeletingWaitAttempts * interval ~= 10s).
	actorDeletingWaitInterval = 500 * time.Millisecond
)

type (
	singleAttemptKey  struct{}
	taskCreateTimeKey struct{}
)

// WithSingleAttempt returns a derived context instructing [*Client] methods to
// make a single outbound RPC attempt per call instead of running internal
// exponential-backoff loops. Queue-based reconcilers pass this option so
// outbound attempts stay 1:1 with rate-limiter tokens and outage
// circuit-breaker observations.
func WithSingleAttempt(ctx context.Context) context.Context {
	return context.WithValue(ctx, singleAttemptKey{}, true)
}

func isSingleAttempt(ctx context.Context) bool {
	v, _ := ctx.Value(singleAttemptKey{}).(bool)
	return v
}

// WithTaskCreateTime attaches a Task's creation timestamp to ctx so
// EnsureTaskActor can reject pre-existing foreign Actors that predate the task.
func WithTaskCreateTime(ctx context.Context, createdAt time.Time) context.Context {
	if createdAt.IsZero() {
		return ctx
	}
	return context.WithValue(ctx, taskCreateTimeKey{}, createdAt)
}

func taskCreateTimeFromContext(ctx context.Context) time.Time {
	t, _ := ctx.Value(taskCreateTimeKey{}).(time.Time)
	return t
}

var (
	// ErrHealthyRetry matches transient resource-level retry conditions on a
	// healthy Substrate control plane (pending durable snapshot uploads,
	// deleting resources, transitional actor workflows, or rolling-upgrade
	// template skew) so callers back off without tripping outage circuit
	// breakers.
	ErrHealthyRetry = errors.New("substrate resource operation requires healthy retry")

	// ErrSnapshotUploadPending indicates that an Actor is already in
	// ACTOR_STATE_SUSPENDED on Substrate while its latest snapshot upload to
	// durable object storage is still in SNAPSHOT_STORAGE_STATE_PENDING.
	ErrSnapshotUploadPending = errors.New("actor snapshot upload is still pending")

	// ErrResourceDeleting indicates that an Actor or Tag is currently finalizing
	// asynchronous deletion on Substrate.
	ErrResourceDeleting = errors.New("substrate resource is still deleting")

	// ErrTemplateSkew indicates that a rolling ax-server upgrade produced a
	// different content-addressed ActorTemplate name on this replica while the
	// task's recorded ActorTemplate has not yet been published on Substrate.
	ErrTemplateSkew = errors.New("actor template skew during rolling upgrade")

	// ErrRevertPreconditionSettled indicates that RevertActor failed with
	// FailedPrecondition while an actor was mid-transition and the follow-up
	// GetActor observed the actor settled in RUNNING or CRASHED state.
	ErrRevertPreconditionSettled = errors.New("actor settled after revert precondition failure")
)

type healthyRetryError struct {
	st       *status.Status
	sentinel error
}

func (e *healthyRetryError) Error() string              { return e.st.Message() }
func (e *healthyRetryError) GRPCStatus() *status.Status { return e.st }
func (e *healthyRetryError) Is(target error) bool {
	return target == ErrHealthyRetry || (e.sentinel != nil && target == e.sentinel)
}

// NewHealthyRetryError returns a gRPC Unavailable error tagged with
// [ErrHealthyRetry] (and optional extra sentinel) so single-attempt reconcilers
// schedule a normal retry backoff without counting the call toward Substrate
// control-plane outage thresholds.
func NewHealthyRetryError(sentinel error, format string, args ...any) error {
	return &healthyRetryError{
		st:       status.Newf(codes.Unavailable, format, args...),
		sentinel: sentinel,
	}
}

// IsHealthyRetry reports whether err represents a healthy-control-plane
// resource retry condition ([ErrHealthyRetry]).
func IsHealthyRetry(err error) bool {
	return errors.Is(err, ErrHealthyRetry)
}

func newSnapshotUploadPendingError(atespace, actorName string, gen int32) error {
	return NewHealthyRetryError(ErrSnapshotUploadPending,
		"actor %s/%s snapshot upload (generation %d) is still pending", atespace, actorName, gen)
}

// IsTransientError reports whether err represents a transient Substrate
// control-plane unavailability or timeout rather than a permanent failure.
func IsTransientError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	if st, ok := status.FromError(err); ok {
		switch st.Code() {
		case codes.Unavailable, codes.DeadlineExceeded, codes.Aborted:
			return true
		}
	}
	return false
}

// retryOnCodes calls fn until it succeeds, returns an error whose gRPC code is not in
// retryable, defaultRetryAttempts is exhausted, or ctx is done. Between attempts it
// waits with jittered exponential backoff starting at defaultRetryInterval and
// capped at maxRetryInterval. When WithSingleAttempt(ctx) is set, fn runs once.
func retryOnCodes(ctx context.Context, fn func() error, retryable ...codes.Code) error {
	if isSingleAttempt(ctx) {
		return fn()
	}
	b := backoff.NewExponentialBackOff()
	b.InitialInterval = defaultRetryInterval
	b.MaxInterval = maxRetryInterval
	b.Multiplier = retryBackoffFactor

	_, err := backoff.Retry(ctx, func() (struct{}, error) {
		err := fn()
		if err != nil && !slices.Contains(retryable, status.Code(err)) {
			return struct{}{}, backoff.Permanent(err)
		}
		return struct{}{}, err
	}, backoff.WithBackOff(b), backoff.WithMaxTries(defaultRetryAttempts))

	// backoff.Retry returns the *PermanentError wrapper as-is when the attempt limit
	// is hit on the same try, so unwrap it to always surface the original error.
	var permanent *backoff.PermanentError
	if errors.As(err, &permanent) {
		return permanent.Unwrap()
	}
	return err
}

// callWithRetry runs fn with retryOnCodes, always retrying codes.Unavailable
// plus any additional retryable codes (such as codes.Aborted).
func callWithRetry[T any](ctx context.Context, fn func() (T, error), extraRetryable ...codes.Code) (T, error) {
	var out T
	retryable := append([]codes.Code{codes.Unavailable}, extraRetryable...)
	err := retryOnCodes(ctx, func() error {
		var callErr error
		out, callErr = fn()
		return callErr
	}, retryable...)
	return out, err
}

const (
	// DefaultCPUMillicores is the default CPU limit (1 core) applied to an
	// ActorTemplate when a Task omits spec.resources.limits.cpu.
	DefaultCPUMillicores int64 = 1000
	// DefaultMemoryBytes is the default memory limit (512 MiB) applied to an
	// ActorTemplate when a Task omits spec.resources.limits.memory.
	DefaultMemoryBytes int64 = 512 << 20
)

// ResourceLimits translates a Task's resource limits into the Substrate
// ActorTemplate resources block. Substrate sizes a sandbox by limits alone. It
// returns nil when no limit is set so the template uses the default sandbox
// limits.
func ResourceLimits(reqs *v1alpha1.ResourcesSpec) *ateapipb.ResourceRequirements {
	limits := reqs.GetLimits()
	cpuStr := limits.GetCpu()
	memStr := limits.GetMemory()
	if cpuStr == "" && memStr == "" {
		return nil
	}
	out := &ateapipb.Resources{
		CpuMillicores: DefaultCPUMillicores,
		MemoryBytes:   DefaultMemoryBytes,
	}
	if cpuStr != "" {
		if cpu, err := resources.ParseCPUMillicores(cpuStr); err == nil {
			out.CpuMillicores = cpu
		}
	}
	if memStr != "" {
		if mem, err := resources.ParseMemoryBytes(memStr); err == nil {
			out.MemoryBytes = mem
		}
	}
	return &ateapipb.ResourceRequirements{Limits: out}
}

// BuildActorTemplate constructs a Substrate ActorTemplate based on the standard ate-env specification.
// A nil resources applies DefaultCPUMillicores and DefaultMemoryBytes.
// snapshotsBucket sets the template-level snapshot storage location; note that Substrate rejects
// a non-empty storage_location with InvalidArgument (Forbidden) unless the Substrate cluster has
// the PerTemplateSnapshotLocation preview feature gate enabled. Leaving snapshotsBucket empty uses
// Substrate's cluster-wide snapshot storage location.
func BuildActorTemplate(atespace, name, image string, envMap map[string]string, command []string, snapshotsBucket string, resources *ateapipb.ResourceRequirements) *ateapipb.ActorTemplate {
	if atespace == "" {
		atespace = v1alpha1.DefaultAtespace
	}
	if image == "" {
		image = v1alpha1.DefaultTaskImage
	}
	if len(command) == 0 {
		command = []string{DefaultGuestCommand}
	}
	var envList []*ateapipb.EnvVar
	for k, v := range envMap {
		envList = append(envList, &ateapipb.EnvVar{
			Name:  k,
			Value: v,
		})
	}

	cpuMillicores := DefaultCPUMillicores
	memoryBytes := DefaultMemoryBytes
	if limits := resources.GetLimits(); limits != nil {
		if limits.GetCpuMillicores() > 0 {
			cpuMillicores = limits.GetCpuMillicores()
		}
		if limits.GetMemoryBytes() > 0 {
			memoryBytes = limits.GetMemoryBytes()
		}
	}

	snapCfg := &ateapipb.SnapshotConfig{
		PreferredFidelity: ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES,
	}
	if snapshotsBucket != "" {
		snapCfg.StorageLocation = snapshotsBucket
	}
	trustBundleDir, trustBundleFile := path.Split(v1alpha1.EgressTrustBundlePath)

	return &ateapipb.ActorTemplate{
		Metadata: &ateapipb.ResourceMetadata{
			Name:     name,
			Atespace: atespace,
		},
		Containers: []*ateapipb.Container{{
			Name:    "guest",
			Image:   image,
			Command: command,
			Env:     envList,
			WakeupProbe: &ateapipb.WakeupProbe{
				HttpGet: &ateapipb.HttpGetAction{
					Path: "/readyz",
					Port: 80,
				},
			},
			VolumeMounts: []*ateapipb.VolumeMount{{
				Name:      "workspace",
				MountPath: "/workspace",
			}, {
				Name:      "egress-trust-bundle",
				MountPath: path.Clean(trustBundleDir),
			}},
		}},
		Volumes: []*ateapipb.Volume{{
			Name:       "workspace",
			DurableDir: &ateapipb.DurableDirVolumeSource{},
		}, {
			// Mounted for every task, with or without a gateway, so that TLS
			// verifies whether or not something on the way out intercepts it.
			Name: "egress-trust-bundle",
			SystemInfo: &ateapipb.SystemInfoVolumeSource{
				DataSources: []*ateapipb.SystemInfoDataSource{{
					TrustBundle: &ateapipb.TrustBundleDataSource{
						Names: []string{"egress-mitm.ate.dev", "system-roots.ate.dev"},
						Path:  trustBundleFile,
					},
				}},
			},
		}},
		SnapshotConfig: snapCfg,
		Resources: &ateapipb.ResourceRequirements{
			Limits: &ateapipb.Resources{
				CpuMillicores: cpuMillicores,
				MemoryBytes:   memoryBytes,
			},
		},
	}
}

// EnsureActorTemplate returns the named ActorTemplate if it already exists, or
// creates it from tmpl idempotently.
func (c *Client) EnsureActorTemplate(ctx context.Context, tmpl *ateapipb.ActorTemplate) (*ateapipb.ActorTemplate, error) {
	atespace := tmpl.GetMetadata().GetAtespace()
	name := tmpl.GetMetadata().GetName()
	existing, err := c.GetActorTemplate(ctx, atespace, name)
	if err == nil && existing != nil {
		if existing.GetMetadata().GetDeleteTime() == nil {
			return existing, nil
		}
		return c.recreateDeletingTemplate(ctx, tmpl, atespace, name)
	}
	if err != nil && status.Code(err) != codes.NotFound {
		return nil, fmt.Errorf("getting actor template %s/%s: %w", atespace, name, err)
	}

	req := &ateapipb.CreateActorTemplateRequest{ActorTemplate: tmpl}
	created, err := callWithRetry(ctx, func() (*ateapipb.ActorTemplate, error) {
		return c.control.CreateActorTemplate(ctx, req)
	})
	if err != nil && status.Code(err) != codes.AlreadyExists {
		return nil, fmt.Errorf("creating actor template %s/%s: %w", atespace, name, err)
	}
	if created != nil {
		return created, nil
	}
	// Another caller created (or began deleting) the same template concurrently
	// (AlreadyExists); fetch the winner's template so metadata/UID is populated.
	fetched, err := c.GetActorTemplate(ctx, atespace, name)
	if status.Code(err) == codes.NotFound {
		retryCreated, retryErr := callWithRetry(ctx, func() (*ateapipb.ActorTemplate, error) {
			return c.control.CreateActorTemplate(ctx, req)
		})
		if retryErr != nil {
			return nil, fmt.Errorf("creating actor template %s/%s: %w", atespace, name, retryErr)
		}
		return retryCreated, nil
	}
	if err != nil {
		return nil, err
	}
	if fetched.GetMetadata().GetDeleteTime() != nil {
		return c.recreateDeletingTemplate(ctx, tmpl, atespace, name)
	}
	return fetched, nil
}

func (c *Client) recreateDeletingTemplate(ctx context.Context, tmpl *ateapipb.ActorTemplate, atespace, name string) (*ateapipb.ActorTemplate, error) {
	if isSingleAttempt(ctx) {
		return nil, NewHealthyRetryError(ErrResourceDeleting, "actor template %s/%s is still deleting; please retry", atespace, name)
	}
	req := &ateapipb.CreateActorTemplateRequest{ActorTemplate: tmpl}
	for range actorDeletingWaitAttempts {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(actorDeletingWaitInterval):
		}
		created, err := callWithRetry(ctx, func() (*ateapipb.ActorTemplate, error) {
			return c.control.CreateActorTemplate(ctx, req)
		})
		if err == nil {
			return created, nil
		}
		if status.Code(err) != codes.AlreadyExists {
			return nil, fmt.Errorf("creating actor template %s/%s: %w", atespace, name, err)
		}
	}
	return nil, NewHealthyRetryError(ErrResourceDeleting, "actor template %s/%s is still deleting; please retry", atespace, name)
}

// EnsureActorTemplateWithImage creates an ActorTemplate using the specified container image,
// resource limits (nil for default limits), and optional environment variables.
//
// TODO: Remove once TaskReconciler migrates from per-task templates to shared
// Profile ActorTemplates via EnsureActorTemplate.
func (c *Client) EnsureActorTemplateWithImage(ctx context.Context, baseAtespace, baseTemplate, targetAtespace, targetTemplate, image string, resources *ateapipb.ResourceRequirements, extraEnv ...map[string]string) (*ateapipb.ActorTemplate, error) {
	envMap := make(map[string]string)
	for _, envs := range extraEnv {
		maps.Copy(envMap, envs)
	}
	tmpl := BuildActorTemplate(targetAtespace, targetTemplate, image, envMap, nil, "", resources)
	return c.EnsureActorTemplate(ctx, tmpl)
}

// EnsureActor creates an Actor in the specified atespace deriving from an ActorTemplate.
func (c *Client) EnsureActor(ctx context.Context, atespace, actorName, templateAtespace, templateName string) (*ateapipb.Actor, error) {
	return c.EnsureActorFromTag(ctx, atespace, actorName, templateAtespace, templateName, "")
}

// EnsureActorFromTag creates an Actor in atespace deriving from an ActorTemplate,
// seeded from sourceTagName (a Workspace snapshot Tag) when non-empty.
func (c *Client) EnsureActorFromTag(ctx context.Context, atespace, actorName, templateAtespace, templateName, sourceTagName string) (*ateapipb.Actor, error) {
	return c.ensureActor(ctx, atespace, actorName, templateAtespace, templateName, sourceTagName, false)
}

// EnsureTaskActor idempotently creates or adopts a Task's Actor on Substrate.
//
// On AlreadyExists, EnsureTaskActor adopts an existing non-deleting Actor ONLY
// if it matches an uncommitted orphan from an earlier CREATE_ACTOR attempt on
// this task (matching template and source tag, still SUSPENDED with zero
// actor-owned checkpoints, no pre-existing EgressPolicy, and not predating
// WithTaskCreateTime) so pre-existing foreign Actors are never hijacked,
// reverted, or deleted.
func (c *Client) EnsureTaskActor(ctx context.Context, atespace, actorName, templateName, sourceTagName string) (*ateapipb.Actor, error) {
	return c.ensureActor(ctx, atespace, actorName, atespace, templateName, sourceTagName, true)
}

// IsUncommittedTaskActorOrphan reports whether existing matches the exact state
// of a freshly created, never-resumed Actor left behind when a previous worker
// crashed or was canceled right after CreateActor succeeded on Substrate but
// before recording ActorUID in storage.
func IsUncommittedTaskActorOrphan(
	existing *ateapipb.Actor,
	templateName, sourceTagName string,
	checkSourceTag bool,
	taskCreatedAt time.Time,
) bool {
	if existing == nil || templateName == "" {
		return false
	}
	if existing.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
		return false
	}
	if existing.GetActorTemplate().GetName() != templateName {
		return false
	}
	haveTag := existing.GetSourceTag().GetName()
	if checkSourceTag && haveTag != sourceTagName {
		return false
	}
	snaps := existing.GetStatus().GetSnapshots()
	lastGen := existing.GetStatus().GetLastAssignedGeneration()
	// Both explicit source_tag actors and golden-snapshot actors (where
	// Actor.source_tag is empty) are born with at most one Tag-owned snapshot
	// (Owner == SNAPSHOT_OWNER_TAG, Locality == "") and LastAssignedGeneration <= 1.
	// Any subsequent actor checkpoint increments generation above 1 or records
	// SNAPSHOT_OWNER_ACTOR / non-empty Locality.
	if lastGen > 1 || len(snaps) > 1 {
		return false
	}
	for _, s := range snaps {
		if s.GetLocality() != "" ||
			(s.GetOwner() != ateapipb.SnapshotOwner_SNAPSHOT_OWNER_UNSPECIFIED &&
				s.GetOwner() != ateapipb.SnapshotOwner_SNAPSHOT_OWNER_TAG) {
			return false
		}
	}
	if !taskCreatedAt.IsZero() && existing.GetMetadata().GetCreateTime() != nil {
		if existing.GetMetadata().GetCreateTime().AsTime().Before(taskCreatedAt.Add(-30 * time.Second)) {
			return false
		}
	}
	return true
}

func (c *Client) ensureActor(ctx context.Context, atespace, actorName, templateAtespace, templateName, sourceTagName string, verifySpecOnExists bool) (*ateapipb.Actor, error) {
	actorSpec := &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: atespace, Name: actorName},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: templateAtespace, Name: templateName},
	}
	if sourceTagName != "" {
		actorSpec.SourceTag = &ateapipb.ObjectRef{Atespace: atespace, Name: sourceTagName}
	}
	req := &ateapipb.CreateActorRequest{Actor: actorSpec}

	actor, err := callWithRetry(ctx, func() (*ateapipb.Actor, error) {
		return c.control.CreateActor(ctx, req)
	})
	if err == nil {
		return actor, nil
	}
	if status.Code(err) == codes.AlreadyExists {
		return c.resolveExistingActor(ctx, req, atespace, actorName, verifySpecOnExists, err)
	}
	return nil, fmt.Errorf("creating actor %s/%s: %w", atespace, actorName, err)
}

// resolveExistingActor handles AlreadyExists when EnsureActor/EnsureActorFromTag
// is called on subsequent Suspend/Resume cycles or retries for an actor that was
// already provisioned on Substrate.
func (c *Client) resolveExistingActor(ctx context.Context, req *ateapipb.CreateActorRequest, atespace, actorName string, verifySpec bool, createErr error) (*ateapipb.Actor, error) {
	existing, err := c.GetActor(ctx, atespace, actorName)
	if status.Code(err) == codes.NotFound {
		// Previous deleting actor finished removal right after CreateActor returned AlreadyExists.
		actor, retryErr := callWithRetry(ctx, func() (*ateapipb.Actor, error) {
			return c.control.CreateActor(ctx, req)
		})
		if retryErr != nil {
			return nil, fmt.Errorf("creating actor %s/%s: %w", atespace, actorName, retryErr)
		}
		return actor, nil
	}
	if err != nil || existing == nil {
		return existing, err
	}
	if existing.GetStatus().GetState() == ateapipb.ActorState_ACTOR_STATE_DELETING {
		return c.recreateDeletingActor(ctx, req, atespace, actorName)
	}
	if verifySpec {
		wantTmpl := req.GetActor().GetActorTemplate().GetName()
		wantTag := req.GetActor().GetSourceTag().GetName()
		if !IsUncommittedTaskActorOrphan(existing, wantTmpl, wantTag, true, taskCreateTimeFromContext(ctx)) {
			return nil, fmt.Errorf("creating actor %s/%s: refusing to adopt pre-existing actor (state=%s template=%q tag=%q lastGen=%d): %w",
				atespace, actorName,
				existing.GetStatus().GetState(),
				existing.GetActorTemplate().GetName(),
				existing.GetSourceTag().GetName(),
				existing.GetStatus().GetLastAssignedGeneration(),
				createErr)
		}
		if _, polErr := c.GetActorEgressPolicy(ctx, atespace, actorName); polErr == nil {
			return nil, fmt.Errorf("creating actor %s/%s: refusing to adopt pre-existing actor that already carries an egress policy: %w",
				atespace, actorName, createErr)
		} else if status.Code(polErr) != codes.NotFound {
			return nil, fmt.Errorf("checking egress policy of existing actor %s/%s: %w", atespace, actorName, polErr)
		}
		return existing, nil
	}
	if existing.GetStatus().GetState() == ateapipb.ActorState_ACTOR_STATE_CRASHED {
		return c.recoverCrashedActor(ctx, req, atespace, actorName)
	}
	return existing, nil
}

// recoverCrashedActor rolls a crashed actor back to its last snapshot via
// RevertActor so the workspace volume survives the crash. If RevertActor fails
// because no durable snapshot exists yet (FailedPrecondition or NotFound), it
// deletes and recreates the actor from scratch; transient control-plane errors
// propagate to the caller without destroying the actor.
func (c *Client) recoverCrashedActor(ctx context.Context, req *ateapipb.CreateActorRequest, atespace, actorName string) (*ateapipb.Actor, error) {
	// Revert to the last snapshot so the workspace survives the crash.
	reverted, revertErr := callWithRetry(ctx, func() (*ateapipb.RevertActorResponse, error) {
		return c.control.RevertActor(ctx, &ateapipb.RevertActorRequest{
			Actor: &ateapipb.ObjectRef{Atespace: atespace, Name: actorName},
		})
	}, codes.Aborted)
	if revertErr == nil {
		slog.Warn("existing actor is crashed, reverted to its last snapshot", "actor", actorName)
		return reverted.GetActor(), nil
	}
	if code := status.Code(revertErr); code != codes.FailedPrecondition && code != codes.NotFound {
		return nil, fmt.Errorf("reverting crashed actor %s/%s: %w", atespace, actorName, revertErr)
	}

	slog.Warn("existing actor is crashed and could not be reverted, deleting and recreating", "actor", actorName, "error", revertErr)
	if err := c.DeleteActor(ctx, atespace, actorName); err != nil {
		return nil, err
	}
	actor, err := callWithRetry(ctx, func() (*ateapipb.Actor, error) {
		return c.control.CreateActor(ctx, req)
	})
	if err != nil {
		return nil, fmt.Errorf("recreating actor %s/%s after crash: %w", atespace, actorName, err)
	}
	return actor, nil
}

// recreateDeletingActor waits briefly for a previous actor of the same name to
// finish deleting before recreating it (or returns Unavailable immediately when
// WithSingleAttempt(ctx) delegates retry scheduling to the caller).
func (c *Client) recreateDeletingActor(ctx context.Context, req *ateapipb.CreateActorRequest, atespace, actorName string) (*ateapipb.Actor, error) {
	if isSingleAttempt(ctx) {
		return nil, NewHealthyRetryError(ErrResourceDeleting, "actor %s/%s is still deleting; please retry", atespace, actorName)
	}
	// Wait briefly for previous actor deletion to finalize before recreating.
	for i := 0; i < actorDeletingWaitAttempts; i++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(actorDeletingWaitInterval):
		}
		actor, err := callWithRetry(ctx, func() (*ateapipb.Actor, error) {
			return c.control.CreateActor(ctx, req)
		})
		if err == nil {
			return actor, nil
		}
		if status.Code(err) != codes.AlreadyExists {
			return nil, fmt.Errorf("creating actor %s/%s: %w", atespace, actorName, err)
		}
	}
	return nil, NewHealthyRetryError(ErrResourceDeleting, "actor %s/%s is still deleting; please retry", atespace, actorName)
}

// ResumeActor resumes the specified actor onto a worker and returns the worker details.
func (c *Client) ResumeActor(ctx context.Context, atespace, actorName string) (*ateapipb.Actor, string, error) {
	req := &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{
			Atespace: atespace,
			Name:     actorName,
		},
	}
	resp, err := callWithRetry(ctx, func() (*ateapipb.ResumeActorResponse, error) {
		return c.control.ResumeActor(ctx, req)
	}, codes.Aborted)
	if err != nil {
		return nil, "", fmt.Errorf("resuming actor %s/%s: %w", atespace, actorName, err)
	}
	if resp.GetActor() == nil {
		return nil, "", fmt.Errorf("nil actor returned when resuming %s/%s", atespace, actorName)
	}
	return resp.GetActor(), WorkerIP(resp.GetActor()), nil
}

// WorkerIP returns the pod IP of the worker hosting the actor, or "" when the
// actor has no worker (for example while it is suspended).
func WorkerIP(actor *ateapipb.Actor) string {
	if ips := actor.GetStatus().GetWorkerAssignment().GetWorkerPodIps(); len(ips) > 0 {
		return ips[0]
	}
	return ""
}

// SuspendActor suspends the specified actor. When SuspendOptions is omitted,
// Substrate defaults to DURABILITY_MODE_ASYNCHRONOUS (the actor transitions to
// SUSPENDED immediately with a node-local snapshot while atelet uploads the
// durable copy in the background; see workflow_suspend.go in Substrate).
func (c *Client) SuspendActor(ctx context.Context, atespace, actorName string) error {
	return c.suspendActor(ctx, atespace, actorName, nil)
}

// SuspendActorAndWaitForSnapshot suspends the specified actor with
// DURABILITY_MODE_SYNCHRONOUS so the RPC blocks until atelet finishes uploading
// a COMPLETED durable snapshot. Setup actors use this before CreateTag and
// DeleteActor so the snapshot is guaranteed durable before teardown.
func (c *Client) SuspendActorAndWaitForSnapshot(ctx context.Context, atespace, actorName string) error {
	return c.suspendActor(ctx, atespace, actorName, &ateapipb.SuspendOptions{
		Mode: ateapipb.DurabilityMode_DURABILITY_MODE_SYNCHRONOUS,
	})
}

func (c *Client) suspendActor(ctx context.Context, atespace, actorName string, opts *ateapipb.SuspendOptions) error {
	req := &ateapipb.SuspendActorRequest{
		Actor: &ateapipb.ObjectRef{
			Atespace: atespace,
			Name:     actorName,
		},
		Options: opts,
	}
	_, err := callWithRetry(ctx, func() (*ateapipb.SuspendActorResponse, error) {
		return c.control.SuspendActor(ctx, req)
	}, codes.Aborted)
	if err != nil {
		return fmt.Errorf("suspending actor %s/%s: %w", atespace, actorName, err)
	}
	return nil
}

// SuspendActorSync synchronously checkpoints and suspends the specified actor,
// succeeding only once its latest snapshot has reached COMPLETED state in
// durable object storage.
//
// In Substrate 1.0, DURABILITY_MODE_SYNCHRONOUS blocks on upload only when
// checkpointing a RUNNING actor; if an actor is already SUSPENDED (for example
// after an asynchronous node suspend or an earlier sync call that timed out
// after node checkpointing), SuspendActor returns immediately while the
// durable upload may still be PENDING. SuspendActorSync detects pending uploads
// via HasPendingSnapshotUpload: single-attempt reconciler callers receive
// ErrSnapshotUploadPending (Unavailable) so they release their admission slot
// and back off cleanly, while direct callers poll GetActor until the upload
// completes.
func (c *Client) SuspendActorSync(ctx context.Context, atespace, actorName string) (int32, error) {
	req := &ateapipb.SuspendActorRequest{
		Actor: &ateapipb.ObjectRef{
			Atespace: atespace,
			Name:     actorName,
		},
		Options: &ateapipb.SuspendOptions{
			Mode: ateapipb.DurabilityMode_DURABILITY_MODE_SYNCHRONOUS,
		},
	}
	resp, err := callWithRetry(ctx, func() (*ateapipb.SuspendActorResponse, error) {
		return c.control.SuspendActor(ctx, req)
	}, codes.Aborted)
	if err != nil {
		return 0, fmt.Errorf("synchronously suspending actor %s/%s: %w", atespace, actorName, err)
	}
	actor := resp.GetActor()
	if !HasPendingSnapshotUpload(actor) {
		return LatestDurableSnapshotGeneration(actor), nil
	}
	if isSingleAttempt(ctx) {
		return 0, newSnapshotUploadPendingError(atespace, actorName, actor.GetStatus().GetLastAssignedGeneration())
	}

	var durableGen int32
	waitErr := retryOnCodes(ctx, func() error {
		cur, getErr := c.control.GetActor(ctx, &ateapipb.GetActorRequest{
			Actor: &ateapipb.ObjectRef{Atespace: atespace, Name: actorName},
		})
		if getErr != nil {
			return getErr
		}
		if cur.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
			return status.Errorf(codes.FailedPrecondition, "actor %s/%s left SUSPENDED state (%s) while waiting for durable upload",
				atespace, actorName, cur.GetStatus().GetState())
		}
		if HasPendingSnapshotUpload(cur) {
			return newSnapshotUploadPendingError(atespace, actorName, cur.GetStatus().GetLastAssignedGeneration())
		}
		durableGen = LatestDurableSnapshotGeneration(cur)
		return nil
	}, codes.Unavailable)
	if waitErr != nil {
		return 0, fmt.Errorf("waiting for durable snapshot upload of actor %s/%s: %w", atespace, actorName, waitErr)
	}
	return durableGen, nil
}

// RevertActor rolls an actor back to its latest completed durable snapshot,
// discarding any crashed, running, or node-local execution and leaving the
// actor SUSPENDED.
//
// Substrate returns FailedPrecondition when RevertActor is called on an actor
// that is already SUSPENDED at its durable snapshot with no node-local or
// pending upload left to discard (for example when a worker repeats an
// idempotent REVERT_ACTOR action after a crash). RevertActor inspects GetActor
// in that case and treats an already-reverted SUSPENDED actor as a clean
// idempotent success.
func (c *Client) RevertActor(ctx context.Context, atespace, actorName string) (int32, error) {
	req := &ateapipb.RevertActorRequest{
		Actor: &ateapipb.ObjectRef{
			Atespace: atespace,
			Name:     actorName,
		},
	}
	resp, err := callWithRetry(ctx, func() (*ateapipb.RevertActorResponse, error) {
		return c.control.RevertActor(ctx, req)
	}, codes.Aborted)
	if err == nil {
		return LatestDurableSnapshotGeneration(resp.GetActor()), nil
	}
	if status.Code(err) == codes.FailedPrecondition {
		existing, getErr := c.GetActor(ctx, atespace, actorName)
		if getErr != nil {
			if status.Code(getErr) == codes.NotFound {
				return 0, fmt.Errorf("reverting actor %s/%s: %w", atespace, actorName, getErr)
			}
			return 0, fmt.Errorf("checking actor %s/%s after revert precondition failure: %w", atespace, actorName, getErr)
		}
		switch existing.GetStatus().GetState() {
		case ateapipb.ActorState_ACTOR_STATE_SUSPENDED:
			if HasPendingSnapshotUpload(existing) {
				return 0, newSnapshotUploadPendingError(atespace, actorName, existing.GetStatus().GetLastAssignedGeneration())
			}
			if allSnapshotsDurable(existing) {
				return LatestDurableSnapshotGeneration(existing), nil
			}
		case ateapipb.ActorState_ACTOR_STATE_REVERTING,
			ateapipb.ActorState_ACTOR_STATE_SUSPENDING,
			ateapipb.ActorState_ACTOR_STATE_RESUMING,
			ateapipb.ActorState_ACTOR_STATE_DELETING:
			return 0, NewHealthyRetryError(nil, "actor %s/%s is %s; retry revert", atespace, actorName, existing.GetStatus().GetState())
		case ateapipb.ActorState_ACTOR_STATE_RUNNING,
			ateapipb.ActorState_ACTOR_STATE_CRASHED:
			if LatestDurableSnapshotGeneration(existing) > 0 {
				return 0, NewHealthyRetryError(ErrRevertPreconditionSettled,
					"actor %s/%s settled in %s after revert precondition; retry revert",
					atespace, actorName, existing.GetStatus().GetState())
			}
		}
	}
	return 0, fmt.Errorf("reverting actor %s/%s: %w", atespace, actorName, err)
}

// LatestDurableSnapshotGeneration returns the highest generation among
// COMPLETED durable snapshots recorded on actor, or 0 when the actor has none.
func LatestDurableSnapshotGeneration(actor *ateapipb.Actor) int32 {
	var maxGen int32
	for _, snap := range actor.GetStatus().GetSnapshots() {
		if snap.GetDurableSnapshot().GetState() == ateapipb.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_COMPLETED &&
			snap.GetGeneration() > maxGen {
			maxGen = snap.GetGeneration()
		}
	}
	return maxGen
}

// HasPendingSnapshotUpload reports whether actor holds any snapshot whose
// upload to durable object storage has not yet completed (either explicitly in
// SNAPSHOT_STORAGE_STATE_PENDING or present on a node via Locality without a
// COMPLETED durable copy).
//
// Note: ActorStatus.last_assigned_generation is monotonic in Substrate 1.0 and
// does not decrease on RevertActor, so pending uploads are determined directly
// from status.snapshots rather than comparing LastAssignedGeneration.
func HasPendingSnapshotUpload(actor *ateapipb.Actor) bool {
	if actor == nil {
		return false
	}
	for _, snap := range actor.GetStatus().GetSnapshots() {
		state := snap.GetDurableSnapshot().GetState()
		if state == ateapipb.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_PENDING {
			return true
		}
		if snap.GetLocality() != "" && state != ateapipb.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_COMPLETED {
			return true
		}
	}
	return false
}

func allSnapshotsDurable(actor *ateapipb.Actor) bool {
	for _, snap := range actor.GetStatus().GetSnapshots() {
		if snap.GetDurableSnapshot().GetState() != ateapipb.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_COMPLETED {
			return false
		}
	}
	return true
}

// GetActor fetches an Actor by name.
func (c *Client) GetActor(ctx context.Context, atespace, actorName string) (*ateapipb.Actor, error) {
	req := &ateapipb.GetActorRequest{
		Actor: &ateapipb.ObjectRef{
			Atespace: atespace,
			Name:     actorName,
		},
	}
	return callWithRetry(ctx, func() (*ateapipb.Actor, error) {
		return c.control.GetActor(ctx, req)
	})
}

const (
	// DefaultListActorsPageSize is the page size (1,000 actors) used by the
	// background actor drift sweep so 100,000 actors across a cluster require
	// ~100 pages per 15s pass (~6.7 QPS, well within the 25 QPS budget).
	DefaultListActorsPageSize int32 = 1000
)

// ListActorsPage fetches a single page of Actors in atespace (or across all
// atespaces when atespace is empty), returning the page items and
// nextPageToken (empty when no further pages remain).
func (c *Client) ListActorsPage(ctx context.Context, atespace string, pageSize int32, pageToken string) ([]*ateapipb.Actor, string, error) {
	if pageSize <= 0 {
		pageSize = DefaultListActorsPageSize
	}
	opts := &ateapipb.ListOptions{
		PageSize:  pageSize,
		PageToken: pageToken,
	}
	req := &ateapipb.ListActorsRequest{
		Atespace: atespace,
		Options:  opts,
	}
	resp, err := callWithRetry(ctx, func() (*ateapipb.ListActorsResponse, error) {
		return c.control.ListActors(ctx, req)
	})
	if err != nil {
		return nil, "", fmt.Errorf("listing actors in %q: %w", atespace, err)
	}
	return resp.GetActors(), resp.GetNextPageToken(), nil
}

// ListActors returns every Actor in the given atespace, or in all atespaces
// when atespace is empty.
func (c *Client) ListActors(ctx context.Context, atespace string) ([]*ateapipb.Actor, error) {
	var (
		actors    []*ateapipb.Actor
		pageToken string
	)
	seenTokens := map[string]struct{}{"": {}}
	for {
		page, nextToken, err := c.ListActorsPage(ctx, atespace, 0, pageToken)
		if err != nil {
			return nil, err
		}
		actors = append(actors, page...)
		if nextToken == "" {
			return actors, nil
		}
		if _, dup := seenTokens[nextToken]; dup {
			return nil, fmt.Errorf("listing actors in %q returned cyclic page token %q", atespace, nextToken)
		}
		seenTokens[nextToken] = struct{}{}
		pageToken = nextToken
	}
}

// egressPolicyName is the name Substrate requires for an actor's egress
// policy. An actor has at most one.
const egressPolicyName = "default"

// GetActorEgressPolicy fetches the actor's egress policy. The error has code
// NotFound when the actor has none.
func (c *Client) GetActorEgressPolicy(ctx context.Context, atespace, actorName string) (*ateapipb.EgressPolicy, error) {
	req := &ateapipb.GetActorEgressPolicyRequest{
		Actor: &ateapipb.ObjectRef{
			Atespace: atespace,
			Name:     actorName,
		},
		Name: egressPolicyName,
	}
	return callWithRetry(ctx, func() (*ateapipb.EgressPolicy, error) {
		return c.control.GetActorEgressPolicy(ctx, req)
	})
}

// EnsureActorEgressPolicy creates the actor's egress policy with the given
// rules if it does not already exist.
func (c *Client) EnsureActorEgressPolicy(ctx context.Context, atespace, actorName string, rules []*ateapipb.EgressRule) error {
	req := &ateapipb.CreateActorEgressPolicyRequest{
		Actor: &ateapipb.ObjectRef{
			Atespace: atespace,
			Name:     actorName,
		},
		EgressPolicy: &ateapipb.EgressPolicy{
			Metadata: &ateapipb.ResourceMetadata{
				Atespace: atespace,
				Name:     egressPolicyName,
			},
			Rules: rules,
		},
	}
	_, err := callWithRetry(ctx, func() (*ateapipb.EgressPolicy, error) {
		return c.control.CreateActorEgressPolicy(ctx, req)
	}, codes.Aborted)
	if err != nil && status.Code(err) != codes.AlreadyExists {
		return fmt.Errorf("creating egress policy of actor %s/%s: %w", atespace, actorName, err)
	}
	return nil
}

// DeleteActor deletes the specified actor from Substrate and blocks until the actor
// is fully deleted.
func (c *Client) DeleteActor(ctx context.Context, atespace, actorName string) error {
	return c.DeleteActorWithUID(ctx, atespace, actorName, "")
}

// DeleteActorWithUID deletes the named actor only if its Metadata.Uid matches
// expectedUID (when expectedUID is non-empty), ensuring a stale delete never
// terminates a foreign or replacement actor incarnation under the same name.
// If the expected incarnation is already gone (NotFound or replaced by a
// different UID), DeleteActorWithUID returns nil immediately.
func (c *Client) DeleteActorWithUID(ctx context.Context, atespace, actorName, expectedUID string) error {
	req := &ateapipb.DeleteActorRequest{
		Actor: &ateapipb.ObjectRef{
			Atespace: atespace,
			Name:     actorName,
		},
	}
	if expectedUID != "" {
		if uuid.Validate(expectedUID) == nil {
			req.Options = &ateapipb.DeleteOptions{Uid: expectedUID}
		} else {
			// Synthetic non-UUID test identifier: verify ownership via GetActor
			// without sending a non-UUID string that real Substrate's
			// DeleteOptions validator would reject with InvalidArgument.
			existing, getErr := c.GetActor(ctx, atespace, actorName)
			if status.Code(getErr) == codes.NotFound {
				return nil
			}
			if getErr != nil {
				return fmt.Errorf("checking actor %s/%s before delete: %w", atespace, actorName, getErr)
			}
			if gotUID := existing.GetMetadata().GetUid(); gotUID != expectedUID {
				return nil
			}
		}
	}

	var uidGone bool
	deleted, err := callWithRetry(ctx, func() (*ateapipb.Actor, error) {
		res, delErr := c.control.DeleteActor(ctx, req)
		if delErr != nil && expectedUID != "" && status.Code(delErr) == codes.Aborted {
			// Substrate returns Aborted both on optimistic version conflicts
			// and when DeleteOptions.Uid does not match the live actor's UID.
			// Distinguish UID mismatch so we return nil immediately instead of
			// retrying against a replacement actor.
			cur, getErr := c.control.GetActor(ctx, &ateapipb.GetActorRequest{Actor: req.Actor})
			if status.Code(getErr) == codes.NotFound {
				uidGone = true
				return nil, nil
			}
			if getErr == nil && cur.GetMetadata().GetUid() != "" && cur.GetMetadata().GetUid() != expectedUID {
				uidGone = true
				return nil, nil
			}
		}
		return res, delErr
	}, codes.Aborted)
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return nil
		}
		return fmt.Errorf("deleting actor %s/%s: %w", atespace, actorName, err)
	}
	if uidGone {
		return nil
	}

	targetUID := expectedUID
	if targetUID == "" {
		targetUID = deleted.GetMetadata().GetUid()
	}

	getReq := &ateapipb.GetActorRequest{
		Actor: &ateapipb.ObjectRef{
			Atespace: atespace,
			Name:     actorName,
		},
	}

	actorGone := func(cur *ateapipb.Actor, getErr error) (done bool, fatalErr error) {
		if getErr == nil {
			if gotUID := cur.GetMetadata().GetUid(); targetUID != "" && gotUID != "" && gotUID != targetUID {
				return true, nil
			}
			return false, nil
		}
		if status.Code(getErr) == codes.NotFound {
			return true, nil
		}
		if ctx.Err() != nil {
			return false, fmt.Errorf("waiting for actor %s/%s deletion: %w", atespace, actorName, ctx.Err())
		}
		if isSingleAttempt(ctx) || !isRetryablePollError(getErr) {
			return false, fmt.Errorf("waiting for actor %s/%s deletion: %w", atespace, actorName, getErr)
		}
		return false, nil
	}

	// Fast path: check if the actor was deleted synchronously.
	if done, fatalErr := actorGone(c.control.GetActor(ctx, getReq)); fatalErr != nil || done {
		return fatalErr
	}
	if isSingleAttempt(ctx) {
		return NewHealthyRetryError(ErrResourceDeleting, "actor %s/%s is still deleting; please retry", atespace, actorName)
	}

	ticker := time.NewTicker(defaultRetryInterval)
	defer ticker.Stop()

	for attempt := 0; attempt < deleteFinalizePollAttempts; attempt++ {
		select {
		case <-ctx.Done():
			return fmt.Errorf("waiting for actor %s/%s deletion: %w", atespace, actorName, ctx.Err())
		case <-ticker.C:
			if done, fatalErr := actorGone(c.control.GetActor(ctx, getReq)); fatalErr != nil || done {
				return fatalErr
			}
		}
	}
	return NewHealthyRetryError(ErrResourceDeleting, "actor %s/%s is still deleting; please retry", atespace, actorName)
}

func isRetryablePollError(err error) bool {
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.Aborted, codes.Internal, codes.ResourceExhausted, codes.Unauthenticated:
		return true
	default:
		return false
	}
}

// ListActorTemplates returns all ActorTemplates in the given atespace.
func (c *Client) ListActorTemplates(ctx context.Context, atespace string) ([]*ateapipb.ActorTemplate, error) {
	resp, err := callWithRetry(ctx, func() (*ateapipb.ListActorTemplatesResponse, error) {
		return c.control.ListActorTemplates(ctx, &ateapipb.ListActorTemplatesRequest{Atespace: atespace})
	})
	if err != nil {
		return nil, fmt.Errorf("listing actor templates in %s: %w", atespace, err)
	}
	if resp.GetNextPageToken() != "" {
		slog.Warn("actor template listing was truncated", "atespace", atespace)
	}
	return resp.GetActorTemplates(), nil
}

// DeleteActorTemplate deletes the specified ActorTemplate. A missing template is not an error.
func (c *Client) DeleteActorTemplate(ctx context.Context, atespace, templateName string) error {
	req := &ateapipb.DeleteActorTemplateRequest{
		ActorTemplate: &ateapipb.ObjectRef{
			Atespace: atespace,
			Name:     templateName,
		},
	}
	_, err := callWithRetry(ctx, func() (*ateapipb.ActorTemplate, error) {
		return c.control.DeleteActorTemplate(ctx, req)
	}, codes.Aborted)
	if err != nil && status.Code(err) != codes.NotFound {
		return fmt.Errorf("deleting actor template %s/%s: %w", atespace, templateName, err)
	}
	return nil
}

// -----------------------------------------------------------------------------
// Snapshot Tags
// -----------------------------------------------------------------------------

// CreateTag creates an atespace-scoped Tag capturing the snapshot of a
// suspended source actor. If a tag of the same name already exists from an
// earlier incomplete attempt, callers should DeleteTag before retrying.
func (c *Client) CreateTag(ctx context.Context, atespace, tagName, sourceActorName string) (*ateapipb.Tag, error) {
	req := &ateapipb.CreateTagRequest{
		Tag: &ateapipb.Tag{
			Metadata:    &ateapipb.ResourceMetadata{Atespace: atespace, Name: tagName},
			Scope:       ateapipb.TagScope_TAG_SCOPE_ATESPACE,
			SourceActor: &ateapipb.ObjectRef{Atespace: atespace, Name: sourceActorName},
		},
	}
	tag, err := callWithRetry(ctx, func() (*ateapipb.Tag, error) {
		return c.control.CreateTag(ctx, req)
	}, codes.Aborted)
	if err != nil {
		return nil, fmt.Errorf("creating tag %s/%s from actor %s: %w", atespace, tagName, sourceActorName, err)
	}
	return tag, nil
}

// GetTag fetches a Tag by atespace and name.
func (c *Client) GetTag(ctx context.Context, atespace, tagName string) (*ateapipb.Tag, error) {
	req := &ateapipb.GetTagRequest{
		Tag: &ateapipb.ObjectRef{Atespace: atespace, Name: tagName},
	}
	return callWithRetry(ctx, func() (*ateapipb.Tag, error) {
		return c.control.GetTag(ctx, req)
	})
}

// ListTags returns every Tag in the given atespace, or in all atespaces when
// atespace is empty.
func (c *Client) ListTags(ctx context.Context, atespace string) ([]*ateapipb.Tag, error) {
	var tags []*ateapipb.Tag
	req := &ateapipb.ListTagsRequest{Atespace: atespace, Options: &ateapipb.ListOptions{}}
	for {
		resp, err := callWithRetry(ctx, func() (*ateapipb.ListTagsResponse, error) {
			return c.control.ListTags(ctx, req)
		})
		if err != nil {
			return nil, fmt.Errorf("listing tags in %q: %w", atespace, err)
		}
		tags = append(tags, resp.GetTags()...)
		if resp.GetNextPageToken() == "" {
			return tags, nil
		}
		req.Options.PageToken = resp.GetNextPageToken()
	}
}

// EnsureSnapshotTag idempotently ensures that tagName exists in a COMPLETED
// durable state from sourceActorName.
//
// Because Substrate's CreateTag reserves the tag name before copying objects, a
// previous worker crash mid-upload leaves an incomplete Tag that causes
// subsequent CreateTag calls to fail with AlreadyExists. EnsureSnapshotTag
// resolves AlreadyExists by reading GetTag:
//   - If the existing Tag is complete, not deleting, and belongs to
//     sourceActorName, it is returned directly.
//   - If the existing Tag is incomplete or deleting (and does not belong to a
//     different completed source actor), it is deleted and recreated once.
func (c *Client) EnsureSnapshotTag(ctx context.Context, atespace, tagName, sourceActorName string) (*ateapipb.Tag, error) {
	tag, err := c.CreateTag(ctx, atespace, tagName, sourceActorName)
	if err == nil {
		return tag, nil
	}
	if status.Code(err) != codes.AlreadyExists {
		return nil, err
	}

	existing, getErr := c.GetTag(ctx, atespace, tagName)
	if getErr != nil && status.Code(getErr) != codes.NotFound {
		return nil, fmt.Errorf("checking existing tag %s/%s: %w", atespace, tagName, getErr)
	}
	if existing != nil {
		sameSource := sourceActorName == "" || existing.GetSourceActor().GetName() == sourceActorName
		completed := existing.GetMetadata().GetDeleteTime() == nil &&
			existing.GetStatus().GetSnapshot().GetDurableSnapshot().GetState() == ateapipb.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_COMPLETED
		if completed {
			if sameSource {
				return existing, nil
			}
			// Never delete a completed Tag owned by a different source actor.
			return nil, err
		}
		if existing.GetMetadata().GetDeleteTime() != nil {
			if isSingleAttempt(ctx) {
				return nil, NewHealthyRetryError(ErrResourceDeleting, "tag %s/%s is still deleting; please retry", atespace, tagName)
			}
			if !sameSource {
				// Wait for the other source actor's deleting tag to disappear
				// without sending a mutating DeleteTag against it.
				for range actorDeletingWaitAttempts {
					select {
					case <-ctx.Done():
						return nil, ctx.Err()
					case <-time.After(actorDeletingWaitInterval):
					}
					if _, pollErr := c.GetTag(ctx, atespace, tagName); status.Code(pollErr) == codes.NotFound {
						break
					}
				}
			} else if delErr := c.DeleteTagWithUID(ctx, atespace, tagName, existing.GetMetadata().GetUid()); delErr != nil {
				return nil, delErr
			}
		} else {
			if !sameSource {
				return nil, err
			}
			if delErr := c.DeleteTagWithUID(ctx, atespace, tagName, existing.GetMetadata().GetUid()); delErr != nil {
				return nil, delErr
			}
		}
	}
	created, createErr := c.CreateTag(ctx, atespace, tagName, sourceActorName)
	if createErr == nil {
		return created, nil
	}
	if status.Code(createErr) == codes.AlreadyExists {
		// A concurrent caller may have recreated and completed the same tag
		// immediately after DeleteTagWithUID finished.
		if raced, raceErr := c.GetTag(ctx, atespace, tagName); raceErr == nil && raced != nil {
			sameSource := sourceActorName == "" || raced.GetSourceActor().GetName() == sourceActorName
			completed := raced.GetMetadata().GetDeleteTime() == nil &&
				raced.GetStatus().GetSnapshot().GetDurableSnapshot().GetState() == ateapipb.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_COMPLETED
			if completed && sameSource {
				return raced, nil
			}
		}
	}
	return nil, createErr
}

// DeleteTag deletes a Tag and the external snapshot it owns, polling until the
// Tag disappears so callers can immediately recreate a Tag of the same name.
// Deleting a missing Tag is not an error.
func (c *Client) DeleteTag(ctx context.Context, atespace, tagName string) error {
	return c.DeleteTagWithUID(ctx, atespace, tagName, "")
}

// DeleteTagWithUID deletes tagName only if its Metadata.Uid matches expectedUID
// (when expectedUID is non-empty), returning nil immediately if the expected
// Tag incarnation is already gone or has been replaced by a newer UID.
func (c *Client) DeleteTagWithUID(ctx context.Context, atespace, tagName, expectedUID string) error {
	req := &ateapipb.DeleteTagRequest{
		Tag: &ateapipb.ObjectRef{Atespace: atespace, Name: tagName},
	}
	if expectedUID != "" {
		if uuid.Validate(expectedUID) == nil {
			req.Options = &ateapipb.DeleteOptions{Uid: expectedUID}
		} else {
			existing, getErr := c.GetTag(ctx, atespace, tagName)
			if status.Code(getErr) == codes.NotFound {
				return nil
			}
			if getErr != nil {
				return fmt.Errorf("checking tag %s/%s before delete: %w", atespace, tagName, getErr)
			}
			if gotUID := existing.GetMetadata().GetUid(); gotUID != expectedUID {
				return nil
			}
		}
	}

	var uidGone bool
	deleted, err := callWithRetry(ctx, func() (*ateapipb.Tag, error) {
		res, delErr := c.control.DeleteTag(ctx, req)
		if delErr != nil && expectedUID != "" && status.Code(delErr) == codes.Aborted {
			cur, getErr := c.control.GetTag(ctx, &ateapipb.GetTagRequest{Tag: req.Tag})
			if status.Code(getErr) == codes.NotFound {
				uidGone = true
				return nil, nil
			}
			if getErr == nil && cur.GetMetadata().GetUid() != "" && cur.GetMetadata().GetUid() != expectedUID {
				uidGone = true
				return nil, nil
			}
		}
		return res, delErr
	}, codes.Aborted)
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return nil
		}
		return fmt.Errorf("deleting tag %s/%s: %w", atespace, tagName, err)
	}
	if uidGone {
		return nil
	}

	targetUID := expectedUID
	if targetUID == "" {
		targetUID = deleted.GetMetadata().GetUid()
	}

	getReq := &ateapipb.GetTagRequest{
		Tag: &ateapipb.ObjectRef{Atespace: atespace, Name: tagName},
	}

	tagGone := func(cur *ateapipb.Tag, getErr error) (done bool, fatalErr error) {
		if getErr == nil {
			if gotUID := cur.GetMetadata().GetUid(); targetUID != "" && gotUID != "" && gotUID != targetUID {
				return true, nil
			}
			return false, nil
		}
		if status.Code(getErr) == codes.NotFound {
			return true, nil
		}
		if ctx.Err() != nil {
			return false, fmt.Errorf("waiting for tag %s/%s deletion: %w", atespace, tagName, ctx.Err())
		}
		if isSingleAttempt(ctx) || !isRetryablePollError(getErr) {
			return false, fmt.Errorf("waiting for tag %s/%s deletion: %w", atespace, tagName, getErr)
		}
		return false, nil
	}

	if done, fatalErr := tagGone(c.control.GetTag(ctx, getReq)); fatalErr != nil || done {
		return fatalErr
	}
	if isSingleAttempt(ctx) {
		return NewHealthyRetryError(ErrResourceDeleting, "tag %s/%s is still deleting; please retry", atespace, tagName)
	}

	ticker := time.NewTicker(defaultRetryInterval)
	defer ticker.Stop()

	for attempt := 0; attempt < deleteFinalizePollAttempts; attempt++ {
		select {
		case <-ctx.Done():
			return fmt.Errorf("waiting for tag %s/%s deletion: %w", atespace, tagName, ctx.Err())
		case <-ticker.C:
			if done, fatalErr := tagGone(c.control.GetTag(ctx, getReq)); fatalErr != nil || done {
				return fatalErr
			}
		}
	}
	return NewHealthyRetryError(ErrResourceDeleting, "tag %s/%s is still deleting; please retry", atespace, tagName)
}
