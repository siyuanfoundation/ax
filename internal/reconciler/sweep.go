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
	"bytes"
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"math"
	"os"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/ax/internal/apivalidation"
	"github.com/google/ax/internal/lifecycle"
	"github.com/google/ax/internal/store"
	"github.com/google/ax/internal/substrate"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"github.com/google/uuid"
	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
)

// SweepMode controls how the background actor drift sweep operates.
type SweepMode string

const (
	// SweepModeActive detects drift and lost runners and applies transitions
	// through store.Apply.
	SweepModeActive SweepMode = "active"
	// SweepModeObserveOnly runs the full sharded sweep and logs every drift or
	// lost-runner condition it would act on without mutating storage (used for
	// rollout canary validation).
	SweepModeObserveOnly SweepMode = "observe"
	// SweepModeOff disables background sweep passes (operational rollback via
	// --sweep=off).
	SweepModeOff SweepMode = "off"
)

const (
	// DefaultSweepInterval is the nominal cadence (15s) at which owned shards
	// compare AX task state against Substrate's ListActors API.
	DefaultSweepInterval = 15 * time.Second
	// DefaultSweepCallTimeout bounds every individual storage query and
	// Substrate RPC issued by Sweeper so a hung TCP socket on one shard cannot
	// block all sweep passes indefinitely.
	DefaultSweepCallTimeout = 15 * time.Second
	// DefaultNumSweepShards is the default shard count (K = 8) across which
	// atespaces are partitioned via hash(atespace) mod K.
	DefaultNumSweepShards = 8
	// DefaultShardLeaseTTL is the lease duration (45s, renewed every 15s) for
	// each sweep shard lock in PostgreSQL's locks table.
	DefaultShardLeaseTTL = 45 * time.Second
	// DefaultMonitoringQPS is the deployment-wide rate budget (25 QPS) for
	// sweep ListActorsPage and missing-actor GetActor calls, divided evenly
	// across all K shards.
	DefaultMonitoringQPS = 25.0
	// DefaultRunnerSilenceTimeout is the silence duration (150s) after which a
	// WORKING or claimed QUEUED task with no pending actions triggers
	// HeartbeatLostInput.
	DefaultRunnerSilenceTimeout = 150 * time.Second
	// DefaultMaxWatermarkAge is the maximum age (60s) of the oldest live
	// replica's last_flush_at in ax_replicas under which lost-runner checks run.
	DefaultMaxWatermarkAge = 60 * time.Second
	// DefaultGlobalListAtespaceThreshold is the per-shard atespace count (200)
	// above which a shard switches from per-atespace ListActorsPage calls to a
	// single global ListActorsPage("", ...) filtered client-side.
	DefaultGlobalListAtespaceThreshold = 200

	sweepApplyTimeout    = 10 * time.Second
	sweepShutdownTimeout = 5 * time.Second
)

// SweepConfig configures a [Sweeper]. Zero values use production defaults.
type SweepConfig struct {
	// Mode selects SweepModeActive (default), SweepModeObserveOnly, or SweepModeOff.
	Mode SweepMode
	// ReplicaID identifies this replica when acquiring shard leases (defaults to
	// HeartbeatBuffer.ReplicaID() when set, or "<hostname>-<short-uuid>").
	ReplicaID string
	// Transition computes pure task state transitions inside store.Apply
	// (required when Mode is SweepModeActive).
	Transition lifecycle.TransitionFunc
	// Reconciler optionally links the local action Reconciler so live replica
	// counts observed in ax_replicas dynamically scale MutatingQPS.
	Reconciler *Reconciler
	// HeartbeatBuffer optionally links this replica's local HeartbeatBuffer so
	// lost-runner checks also verify that the local replica itself has flushed
	// within MaxWatermarkAge.
	HeartbeatBuffer *HeartbeatBuffer

	// SweepInterval is how often owned shards are swept and renewed (default 15s ± 20% jitter).
	SweepInterval time.Duration
	// CallTimeout bounds each individual store call and Substrate RPC issued
	// during a sweep pass (default 15s).
	CallTimeout time.Duration
	// NumShards is the total number of sweep shards K (default 8).
	NumShards int
	// ShardLeaseTTL is the TTL of each shard lease in locks (default 45s).
	ShardLeaseTTL time.Duration
	// ListPageSize is the page size passed to ListActorsPage (default 1,000).
	ListPageSize int32
	// GlobalListThreshold is the shard atespace count above which global
	// ListActorsPage("", ...) is used (default 200).
	GlobalListThreshold int
	// MonitoringQPS is the deployment-wide monitoring QPS cap divided evenly
	// across NumShards (default 25.0; set < 0 to disable in unit tests).
	MonitoringQPS float64

	// RunnerSilenceTimeout is the silence threshold for lost-runner detection
	// (default 150s).
	RunnerSilenceTimeout time.Duration
	// MaxWatermarkAge is the maximum cluster-wide heartbeat flush watermark age
	// allowed for lost-runner checks (default 60s).
	MaxWatermarkAge time.Duration
}

type ownedShard struct {
	id                   int
	key                  string
	token                string
	acquiredAt           time.Time
	lastConfirmedMono    time.Time
	lastRenewAttemptMono time.Time
	lost                 bool
	limiter              *rateLimiter
	missingOnce          map[store.TaskKey]string // TaskKey -> ActorUID observed missing once
}

var errShardLeaseLost = fmt.Errorf("sweep shard lease lost")

// ShardForAtespace deterministically maps atespace into [0, numShards) using
// 64-bit FNV-1a.
func ShardForAtespace(atespace string, numShards int) int {
	if numShards <= 1 {
		return 0
	}
	ns := (store.TaskKey{Atespace: atespace}).Normalized().Atespace
	h := fnv.New64a()
	_, _ = h.Write([]byte(ns))
	return int(h.Sum64() % uint64(numShards))
}

func sweepShardLockKey(shardID int) string {
	return fmt.Sprintf("lock:sweep-shard:%d", shardID)
}

// Sweeper runs the sharded background actor drift sweep and watermark-gated
// lost-runner detector.
type Sweeper struct {
	store      store.TaskSweepStore
	runtime    RuntimeClient
	transition lifecycle.TransitionFunc
	reconciler *Reconciler
	heartbeats *HeartbeatBuffer

	mode                 SweepMode
	replicaID            string
	sweepInterval        time.Duration
	callTimeout          time.Duration
	numShards            int
	shardLeaseTTL        time.Duration
	listPageSize         int32
	globalListThreshold  int
	perShardQPS          float64
	runnerSilenceTimeout time.Duration
	maxWatermarkAge      time.Duration
	startShardOffset     int

	passMu  sync.Mutex
	owned   map[int]*ownedShard
	idsMu   sync.RWMutex
	lastIDs []int
}

// NewSweeper constructs a [Sweeper] over sweepStore and runtime.
func NewSweeper(sweepStore store.TaskSweepStore, runtime RuntimeClient, cfg SweepConfig) *Sweeper {
	if sweepStore == nil {
		panic("reconciler: NewSweeper requires a non-nil store.TaskSweepStore")
	}
	if runtime == nil {
		panic("reconciler: NewSweeper requires a non-nil RuntimeClient")
	}

	mode := cfg.Mode
	if mode == "" {
		mode = SweepModeActive
	}
	if mode != SweepModeActive && mode != SweepModeObserveOnly && mode != SweepModeOff {
		panic(fmt.Sprintf("reconciler: unsupported SweepMode %q", mode))
	}

	replicaID := cfg.ReplicaID
	if replicaID == "" && cfg.HeartbeatBuffer != nil {
		replicaID = cfg.HeartbeatBuffer.ReplicaID()
	}
	if replicaID == "" {
		host, _ := os.Hostname()
		if host == "" {
			host = "ax-server"
		}
		replicaID = fmt.Sprintf("%s-%s", host, uuid.NewString()[:8])
	}

	transition := cfg.Transition
	if mode == SweepModeActive && transition == nil {
		panic("reconciler: NewSweeper requires a non-nil SweepConfig.Transition in active mode")
	}
	sweepInterval := cfg.SweepInterval
	if sweepInterval <= 0 {
		sweepInterval = DefaultSweepInterval
	}
	callTimeout := cfg.CallTimeout
	if callTimeout <= 0 {
		callTimeout = DefaultSweepCallTimeout
	}
	numShards := cfg.NumShards
	if numShards <= 0 {
		numShards = DefaultNumSweepShards
	}
	shardLeaseTTL := cfg.ShardLeaseTTL
	if shardLeaseTTL <= 0 {
		shardLeaseTTL = DefaultShardLeaseTTL
	}
	listPageSize := cfg.ListPageSize
	if listPageSize <= 0 {
		listPageSize = substrate.DefaultListActorsPageSize
	}
	globalThreshold := cfg.GlobalListThreshold
	if globalThreshold <= 0 {
		globalThreshold = DefaultGlobalListAtespaceThreshold
	}
	monitoringQPS := cfg.MonitoringQPS
	if monitoringQPS == 0 {
		monitoringQPS = DefaultMonitoringQPS
	}
	var perShardQPS float64
	if monitoringQPS > 0 {
		perShardQPS = monitoringQPS / float64(numShards)
	}
	silenceTimeout := cfg.RunnerSilenceTimeout
	if silenceTimeout <= 0 {
		silenceTimeout = DefaultRunnerSilenceTimeout
	}
	maxWatermarkAge := cfg.MaxWatermarkAge
	if maxWatermarkAge <= 0 {
		maxWatermarkAge = DefaultMaxWatermarkAge
	}

	h := fnv.New64a()
	_, _ = h.Write([]byte(replicaID))
	startOffset := int(h.Sum64() % uint64(numShards))

	return &Sweeper{
		store:                sweepStore,
		runtime:              runtime,
		transition:           transition,
		reconciler:           cfg.Reconciler,
		heartbeats:           cfg.HeartbeatBuffer,
		mode:                 mode,
		replicaID:            replicaID,
		sweepInterval:        sweepInterval,
		callTimeout:          callTimeout,
		numShards:            numShards,
		shardLeaseTTL:        shardLeaseTTL,
		listPageSize:         listPageSize,
		globalListThreshold:  globalThreshold,
		perShardQPS:          perShardQPS,
		runnerSilenceTimeout: silenceTimeout,
		maxWatermarkAge:      maxWatermarkAge,
		startShardOffset:     startOffset,
		owned:                make(map[int]*ownedShard),
	}
}

// Mode returns the sweeper's configured [SweepMode].
func (s *Sweeper) Mode() SweepMode {
	return s.mode
}

// OwnedShardIDs returns a sorted snapshot of shard IDs currently leased by this
// replica without blocking on an in-flight sweep pass.
func (s *Sweeper) OwnedShardIDs() []int {
	s.idsMu.RLock()
	defer s.idsMu.RUnlock()
	return slices.Clone(s.lastIDs)
}

func (s *Sweeper) publishOwnedIDsLocked() {
	ids := make([]int, 0, len(s.owned))
	for id := range s.owned {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	s.idsMu.Lock()
	s.lastIDs = ids
	s.idsMu.Unlock()
}

func (s *Sweeper) withCallTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, s.callTimeout)
}

func (s *Sweeper) withRenewTimeout(ctx context.Context, lastConfirmedMono time.Time) (context.Context, context.CancelFunc) {
	remaining := max(s.shardLeaseTTL-time.Since(lastConfirmedMono), time.Millisecond)
	return context.WithTimeout(ctx, min(s.callTimeout, remaining))
}

// Run executes the periodic sharded sweep loop until ctx is canceled, releasing
// all owned shard leases cleanly on shutdown. When Mode is SweepModeOff, Run
// performs no actor sweeps or shard leasing, while still keeping Reconciler's
// live-replica MutatingQPS scaled if a Reconciler is attached.
func (s *Sweeper) Run(ctx context.Context) error {
	if s.mode == SweepModeOff && s.reconciler == nil {
		<-ctx.Done()
		return nil
	}

	if s.mode != SweepModeOff {
		defer s.releaseAllShards()
	}

	if err := s.SweepOnce(ctx); err != nil && ctx.Err() == nil {
		slog.Warn("initial actor drift sweep pass failed", "replica", s.replicaID, "error", err)
	}

	timer := time.NewTimer(jitter20Pct(s.sweepInterval))
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
			if err := s.SweepOnce(ctx); err != nil && ctx.Err() == nil {
				slog.Warn("actor drift sweep pass failed", "replica", s.replicaID, "error", err)
			}
			timer.Reset(jitter20Pct(s.sweepInterval))
		}
	}
}

func (s *Sweeper) releaseAllShards() {
	s.passMu.Lock()
	defer s.passMu.Unlock()

	shutCtx, cancel := context.WithTimeout(context.Background(), sweepShutdownTimeout)
	defer cancel()

	for id, sh := range s.owned {
		callCtx, callCancel := context.WithTimeout(shutCtx, s.callTimeout)
		err := s.store.ReleaseShardLease(callCtx, sh.key, sh.token)
		callCancel()
		if err != nil {
			slog.Warn("releasing sweep shard lease on shutdown failed",
				"replica", s.replicaID,
				"shard", id,
				"error", err,
			)
		}
		delete(s.owned, id)
	}
	s.publishOwnedIDsLocked()
}

// SweepOnce executes one complete shard-rebalancing and drift-sweep cycle across
// all shards owned by this replica. When Mode is SweepModeOff, SweepOnce skips
// shard leasing and Substrate listing while still refreshing live-replica QPS
// scaling if a Reconciler is linked. Safe for deterministic single-pass testing.
func (s *Sweeper) SweepOnce(ctx context.Context) error {
	if s.mode == SweepModeOff && s.reconciler == nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	s.passMu.Lock()
	defer s.passMu.Unlock()

	inspectCtx, cancelInspect := s.withCallTimeout(ctx)
	stats, err := s.store.InspectReplicas(inspectCtx)
	cancelInspect()
	if err != nil {
		return fmt.Errorf("inspecting live replicas: %w", err)
	}
	if s.reconciler != nil && stats.HasLiveReplicas {
		s.reconciler.ScaleForLiveReplicas(stats.LiveReplicas)
	}
	if s.mode == SweepModeOff {
		return nil
	}

	activeShards := s.rebalanceShards(ctx, stats.LiveReplicas)
	s.publishOwnedIDsLocked()
	if len(activeShards) == 0 {
		return nil
	}

	listNsCtx, cancelListNs := s.withCallTimeout(ctx)
	allAtespaces, err := s.store.ListTaskAtespaces(listNsCtx)
	cancelListNs()
	if err != nil {
		return fmt.Errorf("listing task atespaces: %w", err)
	}

	byShard := make(map[int][]string, len(activeShards))
	for _, ns := range allAtespaces {
		sid := ShardForAtespace(ns, s.numShards)
		byShard[sid] = append(byShard[sid], ns)
	}

	var (
		wg       sync.WaitGroup
		errMu    sync.Mutex
		firstErr error
	)
	for _, sh := range activeShards {
		shardAtespaces := byShard[sh.id]
		wg.Add(1)
		go func(shard *ownedShard, atespaces []string) {
			defer wg.Done()
			sweepErr := s.sweepShard(ctx, shard, atespaces, stats)
			if sweepErr != nil && !errors.Is(sweepErr, errShardLeaseLost) && ctx.Err() == nil {
				errMu.Lock()
				if firstErr == nil {
					firstErr = sweepErr
				}
				errMu.Unlock()
			}
		}(sh, shardAtespaces)
	}
	wg.Wait()
	droppedAny := false
	for _, sh := range activeShards {
		if sh.lost {
			delete(s.owned, sh.id)
			droppedAny = true
		}
	}
	if droppedAny {
		s.publishOwnedIDsLocked()
	}
	return firstErr
}

func (s *Sweeper) rebalanceShards(ctx context.Context, liveReplicas int) []*ownedShard {
	live := max(liveReplicas, 1)
	targetOwned := (s.numShards + live - 1) / live // ceil(K / live)

	// 1. Renew existing shard leases; drop any expired or stolen shard immediately
	// (discarding its per-shard missingOnce state) and skip sweeping any shard
	// whose renewal encountered a transient DB error.
	confirmed := make([]*ownedShard, 0, len(s.owned))
	ownedIDs := make([]int, 0, len(s.owned))
	for id := range s.owned {
		ownedIDs = append(ownedIDs, id)
	}
	sort.Ints(ownedIDs)

	for _, id := range ownedIDs {
		sh := s.owned[id]
		attemptMono := time.Now()
		renewCtx, cancelRenew := s.withRenewTimeout(ctx, sh.lastConfirmedMono)
		renewed, _, err := s.store.RenewShardLease(renewCtx, sh.key, sh.token, s.shardLeaseTTL)
		cancelRenew()
		if err != nil {
			if time.Since(sh.lastConfirmedMono) >= s.shardLeaseTTL {
				delete(s.owned, id)
			} else if ctx.Err() == nil {
				slog.Warn("renewing sweep shard lease failed; skipping shard on this pass",
					"replica", s.replicaID,
					"shard", id,
					"error", err,
				)
			}
			continue
		}
		if !renewed {
			delete(s.owned, id)
			continue
		}
		sh.lastConfirmedMono = attemptMono
		sh.lastRenewAttemptMono = attemptMono
		confirmed = append(confirmed, sh)
	}

	// 2. Release excess shards when new replicas have joined the cluster so
	// ownership stays balanced at ceil(K / liveReplicas).
	for len(s.owned) > targetOwned && len(confirmed) > 0 {
		drop := confirmed[len(confirmed)-1]
		confirmed = confirmed[:len(confirmed)-1]
		delete(s.owned, drop.id)
		relCtx, cancelRel := s.withCallTimeout(ctx)
		err := s.store.ReleaseShardLease(relCtx, drop.key, drop.token)
		cancelRel()
		if err != nil && ctx.Err() == nil {
			slog.Warn("releasing excess sweep shard lease failed",
				"replica", s.replicaID,
				"shard", drop.id,
				"error", err,
			)
		}
	}

	// 3. Acquire unheld shards up to targetOwned starting from this replica's
	// hashed offset to avoid startup contention on shard 0.
	for i := 0; i < s.numShards && len(s.owned) < targetOwned; i++ {
		if ctx.Err() != nil {
			break
		}
		sid := (s.startShardOffset + i) % s.numShards
		if _, already := s.owned[sid]; already {
			continue
		}
		key := sweepShardLockKey(sid)
		token := fmt.Sprintf("%s:%s", s.replicaID, uuid.NewString())
		attemptMono := time.Now()
		acqCtx, cancelAcq := s.withCallTimeout(ctx)
		acquired, storeNow, err := s.store.TryAcquireShardLease(acqCtx, key, token, s.shardLeaseTTL)
		cancelAcq()
		if err != nil {
			if ctx.Err() == nil {
				slog.Warn("acquiring sweep shard lease failed",
					"replica", s.replicaID,
					"shard", sid,
					"error", err,
				)
			}
			continue
		}
		if !acquired {
			continue
		}
		var lim *rateLimiter
		if s.perShardQPS > 0 {
			burst := max(1, int(math.Ceil(s.perShardQPS)))
			lim = newRateLimiter(s.perShardQPS, burst)
		}
		sh := &ownedShard{
			id:                   sid,
			key:                  key,
			token:                token,
			acquiredAt:           storeNow,
			lastConfirmedMono:    attemptMono,
			lastRenewAttemptMono: attemptMono,
			limiter:              lim,
			missingOnce:          make(map[store.TaskKey]string),
		}
		s.owned[sid] = sh
		confirmed = append(confirmed, sh)
	}

	return confirmed
}

func (s *Sweeper) renewShardIfDue(ctx context.Context, shard *ownedShard) error {
	if shard.lost {
		return errShardLeaseLost
	}
	if time.Since(shard.lastConfirmedMono) >= s.shardLeaseTTL {
		shard.lost = true
		return errShardLeaseLost
	}
	renewEvery := max(s.shardLeaseTTL/3, time.Millisecond)
	if time.Since(shard.lastConfirmedMono) < renewEvery {
		return nil
	}
	retryBackoff := min(max(renewEvery/3, time.Millisecond), time.Second)
	if !shard.lastRenewAttemptMono.IsZero() && time.Since(shard.lastRenewAttemptMono) < retryBackoff {
		return nil
	}
	attemptMono := time.Now()
	shard.lastRenewAttemptMono = attemptMono
	renewCtx, cancelRenew := s.withRenewTimeout(ctx, shard.lastConfirmedMono)
	renewed, _, err := s.store.RenewShardLease(renewCtx, shard.key, shard.token, s.shardLeaseTTL)
	cancelRenew()
	if err != nil {
		if time.Since(shard.lastConfirmedMono) >= s.shardLeaseTTL {
			shard.lost = true
			return errShardLeaseLost
		}
		return nil
	}
	if !renewed {
		shard.lost = true
		return errShardLeaseLost
	}
	shard.lastConfirmedMono = attemptMono
	return nil
}

func (s *Sweeper) sweepShard(ctx context.Context, shard *ownedShard, shardAtespaces []string, replicaStats store.ReplicaStats) error {
	if len(shardAtespaces) == 0 {
		clear(shard.missingOnce)
		return nil
	}

	// Read active task (key, state, generation) snapshots from storage BEFORE
	// paging Substrate's ListActors API so any concurrent state transition while
	// ListActors is in flight advances Generation and fences out stale sweep
	// observations (ws1_one_pager.md §4.5).
	snapCtx, cancelSnap := s.withCallTimeout(ctx)
	activeRecords, preListStoreNow, err := s.store.SnapshotActiveTasks(snapCtx, shardAtespaces)
	cancelSnap()
	if err != nil {
		return fmt.Errorf("snapshotting active tasks for shard %d: %w", shard.id, err)
	}
	activeByKey := make(map[store.TaskKey]store.TaskSweepRecord, len(activeRecords))
	for _, rec := range activeRecords {
		activeByKey[rec.Key] = rec
	}

	observed, completedAtespaces, err := s.listShardActors(ctx, shard, shardAtespaces)
	if errors.Is(err, errShardLeaseLost) || (err != nil && ctx.Err() != nil) {
		return err
	}

	// Inspect RUNNING actors on Substrate that were not in activeRecords so
	// parked terminal (COMPLETED/FAILED/CANCELLED) tasks woken externally (for
	// example via ax ssh or HTTP ingress) are re-parked without scanning all
	// terminal rows on every pass.
	var unknownRunningKeys []store.TaskKey
	for key, actor := range observed {
		if actor.GetStatus().GetState() == ateapipb.ActorState_ACTOR_STATE_RUNNING {
			if _, isActive := activeByKey[key]; !isActive {
				unknownRunningKeys = append(unknownRunningKeys, key)
			}
		}
	}
	if len(unknownRunningKeys) > 0 {
		sort.Slice(unknownRunningKeys, func(i, j int) bool {
			if unknownRunningKeys[i].Atespace != unknownRunningKeys[j].Atespace {
				return unknownRunningKeys[i].Atespace < unknownRunningKeys[j].Atespace
			}
			return unknownRunningKeys[i].Name < unknownRunningKeys[j].Name
		})
		getRowsCtx, cancelGetRows := s.withCallTimeout(ctx)
		rows, getErr := s.store.GetTaskRows(getRowsCtx, unknownRunningKeys)
		cancelGetRows()
		if getErr != nil {
			if ctx.Err() != nil {
				return getErr
			}
			slog.Warn("loading candidate woken terminal tasks failed", "shard", shard.id, "error", getErr)
		} else {
			for _, key := range unknownRunningKeys {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				if renewErr := s.renewShardIfDue(ctx, shard); renewErr != nil {
					return renewErr
				}
				row := rows[key]
				if row == nil || row.Task == nil {
					continue
				}
				state := row.Task.GetStatus().GetState()
				if !lifecycle.IsTerminal(state) || len(row.Actions) > 0 || row.ActorUID == "" {
					continue
				}
				if row.Task.GetMetadata().GetDeleteTime() != nil || row.Task.GetStatus().GetAteActorTemplate() == "" {
					continue
				}
				if updTs := row.Task.GetMetadata().GetUpdateTime(); updTs != nil && updTs.AsTime().After(preListStoreNow) {
					continue
				}
				if createTs := row.Task.GetMetadata().GetCreateTime(); createTs != nil && createTs.AsTime().After(preListStoreNow) {
					continue
				}
				listActor := observed[key]
				if gotUID := listActor.GetMetadata().GetUid(); gotUID != "" && gotUID != row.ActorUID {
					continue
				}
				// Confirm with GetActor AFTER reading row.Generation (rare path:
				// zero calls in steady state). If a terminal task's in-flight
				// SUSPEND_ACTOR_SYNC completed while ListActorsPage was paging,
				// GetActor now observes ACTOR_STATE_SUSPENDED and avoids
				// enqueueing a duplicate terminal suspend.
				if shard.limiter != nil {
					if waitErr := shard.limiter.Wait(ctx); waitErr != nil {
						return waitErr
					}
				}
				getCtx, cancelGet := s.withCallTimeout(ctx)
				liveActor, liveErr := s.runtime.GetActor(getCtx, key.Atespace, key.Name)
				cancelGet()
				if liveErr != nil || liveActor.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_RUNNING {
					continue
				}
				if liveUID := liveActor.GetMetadata().GetUid(); liveUID != "" && liveUID != row.ActorUID {
					continue
				}
				s.applyDriftInput(ctx, key, row.Generation, row.ActorUID, shard.acquiredAt, &lifecycle.ObservedActorInput{
					ActorState: ateapipb.ActorState_ACTOR_STATE_RUNNING,
					ActorUID:   row.ActorUID,
				}, "re-suspend woken terminal actor")
			}
		}
	}

	localFlushOK := s.heartbeats == nil || s.heartbeats.HasFreshFlush(s.maxWatermarkAge)
	checkLostRunners := replicaStats.HasLiveReplicas &&
		replicaStats.WatermarkAge <= s.maxWatermarkAge &&
		localFlushOK

	nextMissingOnce := make(map[store.TaskKey]string)
	for _, rec := range activeRecords {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if renewErr := s.renewShardIfDue(ctx, shard); renewErr != nil {
			return renewErr
		}
		// Tasks with pending actions are owned by their queue worker; tasks
		// without ActorUID have not completed CREATE_ACTOR yet (or are legacy
		// non-Apply rows).
		if rec.HasPendingActions || rec.ActorUID == "" {
			continue
		}

		actor, present := observed[rec.Key]
		if present {
			if gotUID := actor.GetMetadata().GetUid(); gotUID != "" && gotUID != rec.ActorUID {
				// Foreign or replacement actor incarnation under the same name:
				// treat as absent for this task and never modify the actor.
				present = false
				actor = nil
			}
		}

		// Even if a later ListActorsPage failed mid-atespace, act immediately
		// on drift (such as CRASHED) for actors already observed on earlier pages.
		if present && s.evaluatePresentActorDrift(ctx, rec, actor, shard.acquiredAt) {
			continue
		}

		if !present && !completedAtespaces[rec.Key.Atespace] {
			if prevUID := shard.missingOnce[rec.Key]; prevUID == rec.ActorUID {
				nextMissingOnce[rec.Key] = rec.ActorUID
			}
			continue
		}

		if !present {
			if prevUID := shard.missingOnce[rec.Key]; prevUID != rec.ActorUID {
				nextMissingOnce[rec.Key] = rec.ActorUID
				continue
			}
			// Absent in two consecutive completed passes for the same ActorUID:
			// confirm with targeted GetActor under the shard's monitoring QPS budget.
			if shard.limiter != nil {
				if waitErr := shard.limiter.Wait(ctx); waitErr != nil {
					return waitErr
				}
			}
			getCtx, cancelGet := s.withCallTimeout(ctx)
			liveActor, getErr := s.runtime.GetActor(getCtx, rec.Key.Atespace, rec.Key.Name)
			cancelGet()
			switch {
			case statusCode(getErr) == codes.NotFound ||
				(getErr == nil && liveActor.GetMetadata().GetUid() != "" && liveActor.GetMetadata().GetUid() != rec.ActorUID):
				s.applyDriftInput(ctx, rec.Key, rec.Generation, rec.ActorUID, shard.acquiredAt, &lifecycle.ObservedActorInput{
					MissingConfirmed: true,
				}, "actor confirmed missing on Substrate")
				continue
			case getErr != nil:
				// Transient confirmation error: retain missingOnce so the next
				// pass retries GetActor without failing the task.
				nextMissingOnce[rec.Key] = rec.ActorUID
				continue
			default:
				actor = liveActor
				if s.evaluatePresentActorDrift(ctx, rec, actor, shard.acquiredAt) {
					continue
				}
			}
		}

		// Watermark-gated lost-runner detection: only WORKING tasks and claimed
		// QUEUED tasks whose actor is currently RUNNING on Substrate are
		// expected to send heartbeats. Unclaimed QUEUED tasks waiting for
		// runner startup or cached JWT expiry are governed by the 5-minute
		// ClaimDeadline timer in Reconciler.
		if checkLostRunners &&
			actor.GetStatus().GetState() == ateapipb.ActorState_ACTOR_STATE_RUNNING &&
			expectsRunnerHeartbeats(rec.State, rec.HasRunnerToken, rec.HasClaimDeadline) {
			baseline := silenceBaseline(rec.LastHeartbeatAt, rec.LastResumedAt, rec.CreatedAt, shard.acquiredAt)
			if preListStoreNow.Sub(baseline) > s.runnerSilenceTimeout {
				s.applyDriftInput(ctx, rec.Key, rec.Generation, rec.ActorUID, shard.acquiredAt, &lifecycle.HeartbeatLostInput{
					SilentFor: preListStoreNow.Sub(baseline),
				}, "runner exceeded heartbeat silence threshold")
			}
		}
	}

	shard.missingOnce = nextMissingOnce
	return err
}

func expectsRunnerHeartbeats(state v1alpha1.TaskState, hasRunnerToken, hasClaimDeadline bool) bool {
	if !hasRunnerToken {
		return false
	}
	switch state {
	case v1alpha1.TaskState_TASK_STATE_WORKING:
		return true
	case v1alpha1.TaskState_TASK_STATE_QUEUED:
		return !hasClaimDeadline
	default:
		return false
	}
}

func (s *Sweeper) listShardActors(
	ctx context.Context,
	shard *ownedShard,
	shardAtespaces []string,
) (observed map[store.TaskKey]*ateapipb.Actor, completedAtespaces map[string]bool, firstErr error) {
	observed = make(map[store.TaskKey]*ateapipb.Actor)
	completedAtespaces = make(map[string]bool, len(shardAtespaces))

	if len(shardAtespaces) > s.globalListThreshold {
		shardSet := make(map[string]struct{}, len(shardAtespaces))
		for _, ns := range shardAtespaces {
			shardSet[ns] = struct{}{}
		}
		var pageToken string
		seenTokens := map[string]struct{}{"": {}}
		for {
			if err := s.renewShardIfDue(ctx, shard); err != nil {
				return observed, completedAtespaces, err
			}
			if shard.limiter != nil {
				if err := shard.limiter.Wait(ctx); err != nil {
					return observed, completedAtespaces, err
				}
			}
			listCtx, cancelList := s.withCallTimeout(ctx)
			actors, nextToken, err := s.runtime.ListActorsPage(listCtx, "", s.listPageSize, pageToken)
			cancelList()
			if err != nil {
				if ctx.Err() == nil {
					slog.Warn("global ListActorsPage failed during sweep", "shard", shard.id, "error", err)
				}
				return observed, completedAtespaces, err
			}
			for _, actor := range actors {
				if actor == nil || actor.Metadata == nil || actor.Metadata.Name == "" ||
					strings.HasPrefix(actor.Metadata.Name, apivalidation.SetupActorPrefix) {
					continue
				}
				k := store.TaskKey{
					Atespace: actor.Metadata.Atespace,
					Name:     actor.Metadata.Name,
				}.Normalized()
				if _, match := shardSet[k.Atespace]; match {
					observed[k] = actor
				}
			}
			if nextToken == "" {
				for _, ns := range shardAtespaces {
					completedAtespaces[ns] = true
				}
				return observed, completedAtespaces, nil
			}
			if _, dup := seenTokens[nextToken]; dup {
				nonAdvErr := fmt.Errorf("global ListActorsPage returned cyclic page token %q", nextToken)
				slog.Warn("global ListActorsPage failed during sweep", "shard", shard.id, "error", nonAdvErr)
				return observed, completedAtespaces, nonAdvErr
			}
			seenTokens[nextToken] = struct{}{}
			pageToken = nextToken
		}
	}

	for _, ns := range shardAtespaces {
		var pageToken string
		seenTokens := map[string]struct{}{"": {}}
		for {
			if err := s.renewShardIfDue(ctx, shard); err != nil {
				return observed, completedAtespaces, err
			}
			if shard.limiter != nil {
				if err := shard.limiter.Wait(ctx); err != nil {
					return observed, completedAtespaces, err
				}
			}
			listCtx, cancelList := s.withCallTimeout(ctx)
			actors, nextToken, err := s.runtime.ListActorsPage(listCtx, ns, s.listPageSize, pageToken)
			cancelList()
			if err != nil {
				if firstErr == nil {
					firstErr = err
				}
				if ctx.Err() != nil {
					return observed, completedAtespaces, err
				}
				slog.Warn("atespace ListActorsPage failed during sweep",
					"shard", shard.id,
					"atespace", ns,
					"error", err,
				)
				break
			}
			for _, actor := range actors {
				if actor == nil || actor.Metadata == nil || actor.Metadata.Name == "" ||
					strings.HasPrefix(actor.Metadata.Name, apivalidation.SetupActorPrefix) {
					continue
				}
				k := store.TaskKey{
					Atespace: actor.Metadata.Atespace,
					Name:     actor.Metadata.Name,
				}.Normalized()
				observed[k] = actor
			}
			if nextToken == "" {
				completedAtespaces[ns] = true
				break
			}
			if _, dup := seenTokens[nextToken]; dup {
				nonAdvErr := fmt.Errorf("atespace %q ListActorsPage returned cyclic page token %q", ns, nextToken)
				if firstErr == nil {
					firstErr = nonAdvErr
				}
				slog.Warn("atespace ListActorsPage failed during sweep", "shard", shard.id, "atespace", ns, "error", nonAdvErr)
				break
			}
			seenTokens[nextToken] = struct{}{}
			pageToken = nextToken
		}
	}
	return observed, completedAtespaces, firstErr
}

func (s *Sweeper) evaluatePresentActorDrift(
	ctx context.Context,
	rec store.TaskSweepRecord,
	actor *ateapipb.Actor,
	shardAcquiredAt time.Time,
) bool {
	if actor == nil {
		return false
	}
	actorState := actor.GetStatus().GetState()
	switch actorState {
	case ateapipb.ActorState_ACTOR_STATE_CRASHED:
		if !lifecycle.IsTerminal(rec.State) {
			s.applyDriftInput(ctx, rec.Key, rec.Generation, rec.ActorUID, shardAcquiredAt, &lifecycle.ObservedActorInput{
				ActorState:   ateapipb.ActorState_ACTOR_STATE_CRASHED,
				ActorUID:     rec.ActorUID,
				CrashMessage: actor.GetStatus().GetCrash().GetMessage(),
			}, "actor crashed on Substrate")
			return true
		}

	case ateapipb.ActorState_ACTOR_STATE_RUNNING:
		if rec.State == v1alpha1.TaskState_TASK_STATE_SUSPENDED || rec.State == v1alpha1.TaskState_TASK_STATE_IDLE {
			s.applyDriftInput(ctx, rec.Key, rec.Generation, rec.ActorUID, shardAcquiredAt, &lifecycle.ObservedActorInput{
				ActorState: ateapipb.ActorState_ACTOR_STATE_RUNNING,
				ActorUID:   rec.ActorUID,
			}, "parked task actor observed RUNNING on Substrate")
			return true
		}

	case ateapipb.ActorState_ACTOR_STATE_SUSPENDED:
		if rec.State == v1alpha1.TaskState_TASK_STATE_WORKING || rec.State == v1alpha1.TaskState_TASK_STATE_QUEUED {
			s.applyDriftInput(ctx, rec.Key, rec.Generation, rec.ActorUID, shardAcquiredAt, &lifecycle.ObservedActorInput{
				ActorState: ateapipb.ActorState_ACTOR_STATE_SUSPENDED,
				ActorUID:   rec.ActorUID,
			}, "active task actor observed SUSPENDED on Substrate")
			return true
		}
	}
	return false
}

func (s *Sweeper) validateDriftCandidate(
	cur *store.TaskRow,
	now time.Time,
	expectedGen int64,
	expectedActorUID string,
	shardAcquiredAt time.Time,
	input lifecycle.Input,
) (lifecycle.State, lifecycle.Output, bool, error) {
	if cur == nil || cur.Task == nil {
		return lifecycle.State{}, lifecycle.Output{}, false, nil
	}
	if cur.Generation != expectedGen || len(cur.Actions) > 0 {
		return lifecycle.State{}, lifecycle.Output{}, false, nil
	}
	if cur.ActorUID == "" || cur.ActorUID != expectedActorUID {
		return lifecycle.State{}, lifecycle.Output{}, false, nil
	}
	if cur.Task.GetMetadata().GetDeleteTime() != nil || cur.Task.GetStatus().GetAteActorTemplate() == "" {
		return lifecycle.State{}, lifecycle.Output{}, false, nil
	}

	callInput := input
	if _, isHB := input.(*lifecycle.HeartbeatLostInput); isHB {
		curState := cur.Task.GetStatus().GetState()
		if !expectsRunnerHeartbeats(curState, len(cur.RunnerTokenHash) > 0, cur.ClaimDeadline != nil) {
			return lifecycle.State{}, lifecycle.Output{}, false, nil
		}
		// Re-verify silence against cur.LastHeartbeatAt (which FlushHeartbeats
		// updates without bumping Generation) so a concurrent heartbeat flush
		// is never missed.
		var createdAt time.Time
		if createTs := cur.Task.GetMetadata().GetCreateTime(); createTs != nil {
			createdAt = createTs.AsTime()
		}
		freshBaseline := silenceBaseline(cur.LastHeartbeatAt, cur.LastResumedAt, createdAt, shardAcquiredAt)
		silentFor := now.Sub(freshBaseline)
		if silentFor <= s.runnerSilenceTimeout {
			return lifecycle.State{}, lifecycle.Output{}, false, nil
		}
		callInput = &lifecycle.HeartbeatLostInput{SilentFor: silentFor}
	}

	curLC := cur.LifecycleState()
	nextState, out, err := s.transition(curLC, callInput, now)
	if err != nil {
		return lifecycle.State{}, lifecycle.Output{}, false, err
	}
	if !out.DeleteRow && lifecycleStateEqual(curLC, nextState) {
		return lifecycle.State{}, lifecycle.Output{}, false, nil
	}
	return nextState, out, true, nil
}

func (s *Sweeper) applyDriftInput(
	ctx context.Context,
	key store.TaskKey,
	expectedGen int64,
	expectedActorUID string,
	shardAcquiredAt time.Time,
	input lifecycle.Input,
	reason string,
) {
	applyCtx, cancel := context.WithTimeout(ctx, min(s.callTimeout, sweepApplyTimeout))
	defer cancel()

	if s.mode == SweepModeObserveOnly {
		rows, getErr := s.store.GetTaskRows(applyCtx, []store.TaskKey{key})
		if getErr != nil {
			if ctx.Err() == nil {
				slog.Warn("loading task row for observe-only sweep validation failed",
					"atespace", key.Atespace,
					"task", key.Name,
					"actorUID", expectedActorUID,
					"reason", reason,
					"error", getErr,
				)
			}
			return
		}
		stats, statsErr := s.store.InspectReplicas(applyCtx)
		if statsErr != nil {
			if ctx.Err() == nil {
				slog.Warn("inspecting store clock for observe-only sweep validation failed",
					"atespace", key.Atespace,
					"task", key.Name,
					"actorUID", expectedActorUID,
					"reason", reason,
					"error", statsErr,
				)
			}
			return
		}
		_, _, wouldMutate, valErr := s.validateDriftCandidate(
			rows[key], stats.StoreNow, expectedGen, expectedActorUID, shardAcquiredAt, input,
		)
		if valErr != nil {
			if ctx.Err() == nil {
				slog.Warn("validating observe-only drift sweep candidate failed",
					"atespace", key.Atespace,
					"task", key.Name,
					"actorUID", expectedActorUID,
					"reason", reason,
					"error", valErr,
				)
			}
			return
		}
		if wouldMutate {
			slog.Info("drift sweep observation (observe-only)",
				"atespace", key.Atespace,
				"task", key.Name,
				"actorUID", expectedActorUID,
				"generation", expectedGen,
				"reason", reason,
			)
		}
		return
	}

	_, err := s.store.Apply(applyCtx, key, func(cur *store.TaskRow, now time.Time) (*store.TaskRow, error) {
		nextState, out, wouldMutate, transErr := s.validateDriftCandidate(
			cur, now, expectedGen, expectedActorUID, shardAcquiredAt, input,
		)
		if transErr != nil {
			return nil, transErr
		}
		if !wouldMutate {
			return nil, nil
		}
		if out.DeleteRow {
			return store.DeleteTaskRow, nil
		}
		next := cur.Clone()
		next.SetLifecycleState(nextState, now)
		return next, nil
	})
	if err != nil && ctx.Err() == nil {
		slog.Warn("applying drift sweep observation failed",
			"atespace", key.Atespace,
			"task", key.Name,
			"actorUID", expectedActorUID,
			"reason", reason,
			"error", err,
		)
	}
}

func silenceBaseline(lastHeartbeatAt, lastResumedAt *time.Time, createdAt, shardAcquiredAt time.Time) time.Time {
	baseline := shardAcquiredAt
	if !createdAt.IsZero() && createdAt.After(baseline) {
		baseline = createdAt
	}
	if lastResumedAt != nil && lastResumedAt.After(baseline) {
		baseline = *lastResumedAt
	}
	if lastHeartbeatAt != nil && lastHeartbeatAt.After(baseline) {
		baseline = *lastHeartbeatAt
	}
	return baseline
}

func lifecycleStateEqual(a, b lifecycle.State) bool {
	if a.ActorUID != b.ActorUID ||
		a.ActiveRunID != b.ActiveRunID ||
		a.RunSeq != b.RunSeq ||
		a.ActionSeq != b.ActionSeq ||
		a.RunnerTokenConfirmed != b.RunnerTokenConfirmed ||
		a.RunnerTokenGen != b.RunnerTokenGen ||
		a.LastAckedRunnerSeq != b.LastAckedRunnerSeq ||
		!bytes.Equal(a.RunnerTokenHash, b.RunnerTokenHash) ||
		!timePtrEqual(a.ClaimDeadline, b.ClaimDeadline) ||
		!timePtrEqual(a.LastResumedAt, b.LastResumedAt) ||
		len(a.Actions) != len(b.Actions) {
		return false
	}
	for i := range a.Actions {
		if a.Actions[i] != b.Actions[i] {
			return false
		}
	}
	return proto.Equal(a.Task, b.Task)
}

func timePtrEqual(a, b *time.Time) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.Equal(*b)
}
