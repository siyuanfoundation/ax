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
	"log/slog"
	"math/rand/v2"
	"os"
	"sync"
	"time"

	"github.com/google/ax/internal/store"
	"github.com/google/uuid"
)

const (
	// DefaultHeartbeatFlushInterval is the nominal interval at which each
	// replica flushes its in-memory runner heartbeat map and renews its
	// ax_replicas watermark. Actual sleeps draw uniformly from [0.8x, 1.0x] of
	// this bound (24s..30s) so multi-replica flushes are desynchronized while
	// healthy replicas never exceed the 30s flush bound.
	DefaultHeartbeatFlushInterval = 30 * time.Second

	// DefaultReplicaTTL is how long a replica's row in ax_replicas remains live
	// without a successful heartbeat flush (45s).
	DefaultReplicaTTL = 45 * time.Second

	// DefaultHeartbeatFlushTimeout bounds the duration of any individual
	// FlushHeartbeats call driven by HeartbeatBuffer.Run so a blackholed TCP
	// connection fails promptly and enters the fast retry loop.
	DefaultHeartbeatFlushTimeout = 10 * time.Second

	defaultFlushErrorRetryInterval = 3 * time.Second
	heartbeatShutdownTimeout       = 5 * time.Second
	maxBufferedHeartbeatAge        = 10 * time.Minute
	maxTokensPerTask               = 4
	tokenVerifyCooldown            = 5 * time.Second
	tokenVerifyTimeout             = 2 * time.Second
	maxPinnedTasks                 = 32768
)

// HeartbeatConfig configures a [HeartbeatBuffer]. Zero values use production defaults.
type HeartbeatConfig struct {
	// ReplicaID uniquely identifies this ax-server replica instance in
	// ax_replicas (defaults to "<hostname>-<short-uuid>").
	ReplicaID string
	// FlushInterval is the upper bound between periodic batch flushes
	// (default 30s, jittered in [0.8x, 1.0x]).
	FlushInterval time.Duration
	// FlushTimeout bounds each periodic Flush call inside Run (defaults to
	// min(FlushInterval, 10s)).
	FlushTimeout time.Duration
	// ReplicaTTL is the lease TTL written to ax_replicas.expires_at on each
	// flush (default 45s).
	ReplicaTTL time.Duration
	// RetryInterval is how quickly Run retries after a transient Flush failure
	// so the replica renews ax_replicas before ReplicaTTL expires (default 3s).
	RetryInterval time.Duration
	// Clock overrides local time reads in unit tests (defaults to time.Now,
	// which carries monotonic clock readings).
	Clock func() time.Time
}

type bufferedHeartbeat struct {
	tokenHash  []byte
	receivedAt time.Time
}

type pinnedRunnerToken struct {
	tokenKey      string
	knownNoToken  bool
	lastCheckedAt time.Time
	checking      bool
	seq           uint64
}

// HeartbeatBuffer coalesces per-request runner heartbeats in replica memory and
// flushes them to storage in a single batched write every ~24-30 seconds,
// simultaneously maintaining this replica's liveness and last_flush_at
// watermark in ax_replicas.
//
// Each recorded entry stores its receipt time on the replica's monotonic clock
// and sends only the elapsed Age to storage, which computes
// (store_clock_now - Age). Replica wall-clock skew therefore has zero effect on
// persisted heartbeat timestamps.
//
// Health-gating contract for `:8081` runner HTTP handlers: before accepting an
// incoming heartbeat request and returning HTTP 200 OK, the handler must check
// [HeartbeatBuffer.CanAcceptHeartbeats] and return HTTP 503 Service Unavailable
// when this replica has not completed a flush within ReplicaTTL (45s). That
// forces runners to fail over to a healthy replica before this replica's row in
// ax_replicas expires and surviving sweepers resume lost-runner checks.
type HeartbeatBuffer struct {
	store         store.TaskSweepStore
	replicaID     string
	flushInterval time.Duration
	flushTimeout  time.Duration
	replicaTTL    time.Duration
	retryInterval time.Duration
	clock         func() time.Time

	flushMu sync.Mutex

	mu            sync.Mutex
	pending       map[store.TaskKey]map[string]bufferedHeartbeat
	pinned        map[store.TaskKey]pinnedRunnerToken
	checkSeq      uint64
	lastSuccessAt time.Time
}

// NewHeartbeatBuffer constructs a [HeartbeatBuffer] backed by sweepStore.
func NewHeartbeatBuffer(sweepStore store.TaskSweepStore, cfg HeartbeatConfig) *HeartbeatBuffer {
	if sweepStore == nil {
		panic("reconciler: NewHeartbeatBuffer requires a non-nil store.TaskSweepStore")
	}
	replicaID := cfg.ReplicaID
	if replicaID == "" {
		host, _ := os.Hostname()
		if host == "" {
			host = "ax-server"
		}
		replicaID = fmt.Sprintf("%s-%s", host, uuid.NewString()[:8])
	}
	flushInterval := cfg.FlushInterval
	if flushInterval <= 0 {
		flushInterval = DefaultHeartbeatFlushInterval
	}
	flushTimeout := cfg.FlushTimeout
	if flushTimeout <= 0 {
		flushTimeout = min(flushInterval, DefaultHeartbeatFlushTimeout)
	}
	replicaTTL := cfg.ReplicaTTL
	if replicaTTL <= 0 {
		replicaTTL = DefaultReplicaTTL
	}
	retryInterval := cfg.RetryInterval
	if retryInterval <= 0 {
		retryInterval = min(defaultFlushErrorRetryInterval, max(flushInterval/4, 5*time.Millisecond))
	}
	clock := cfg.Clock
	if clock == nil {
		clock = time.Now
	}
	return &HeartbeatBuffer{
		store:         sweepStore,
		replicaID:     replicaID,
		flushInterval: flushInterval,
		flushTimeout:  flushTimeout,
		replicaTTL:    replicaTTL,
		retryInterval: retryInterval,
		clock:         clock,
		pending:       make(map[store.TaskKey]map[string]bufferedHeartbeat),
		pinned:        make(map[store.TaskKey]pinnedRunnerToken),
	}
}

// ReplicaID returns the replica identifier registered in ax_replicas.
func (b *HeartbeatBuffer) ReplicaID() string {
	return b.replicaID
}

// ConfirmToken records a storage-verified runnerTokenHash for key (for example
// after a successful ClaimAssignment or ReportEvents transaction on this
// replica) so bogus X-AX-Runner-Token headers sent by untrusted uid 1000
// sandbox processes are immediately ignored in memory.
func (b *HeartbeatBuffer) ConfirmToken(key store.TaskKey, runnerTokenHash []byte) {
	if key.Name == "" || len(runnerTokenHash) == 0 {
		return
	}
	key = key.Normalized()
	tokKey := string(runnerTokenHash)
	now := b.clock()

	b.mu.Lock()
	b.pinTokenLocked(key, tokKey, false, now)
	b.mu.Unlock()
}

func (b *HeartbeatBuffer) ensurePinCapacityLocked(key store.TaskKey, now time.Time) {
	if len(b.pinned) < maxPinnedTasks {
		return
	}
	if _, exists := b.pinned[key]; exists {
		return
	}
	for k, p := range b.pinned {
		if !p.checking && now.Sub(p.lastCheckedAt) > tokenVerifyCooldown {
			delete(b.pinned, k)
		}
	}
	if len(b.pinned) >= maxPinnedTasks {
		evictBudget := max(maxPinnedTasks/16, 1)
		for k, p := range b.pinned {
			if !p.checking {
				delete(b.pinned, k)
				if evictBudget--; evictBudget == 0 {
					break
				}
			}
		}
	}
}

func (b *HeartbeatBuffer) pinTokenLocked(key store.TaskKey, tokKey string, knownNoToken bool, now time.Time) {
	b.ensurePinCapacityLocked(key, now)
	b.pinned[key] = pinnedRunnerToken{
		tokenKey:      tokKey,
		knownNoToken:  knownNoToken,
		lastCheckedAt: now,
		checking:      false,
	}
	if knownNoToken {
		delete(b.pending, key)
		return
	}
	if byTok := b.pending[key]; byTok != nil && tokKey != "" {
		for k := range byTok {
			if k != tokKey {
				delete(byTok, k)
			}
		}
		if len(byTok) == 0 {
			delete(b.pending, key)
		}
	}
}

func (b *HeartbeatBuffer) freshPinLocked(key store.TaskKey, now time.Time) (pinnedTok string, knownNoToken bool, fresh bool) {
	pin, ok := b.pinned[key]
	if !ok || pin.lastCheckedAt.IsZero() || now.Sub(pin.lastCheckedAt) > tokenVerifyCooldown {
		return "", false, false
	}
	return pin.tokenKey, pin.knownNoToken, true
}

// Record records a runner heartbeat in memory. Callers serving `:8081` HTTP
// requests should verify [HeartbeatBuffer.CanAcceptHeartbeats] first and return
// HTTP 503 when this replica's flush watermark has expired.
//
// In normal operation (fewer than 4 distinct token hashes for the same task per
// 30s flush window), Record performs zero storage reads. If an untrusted uid
// 1000 process inside a sandbox floods bogus X-AX-Runner-Token values (>= 4
// distinct hashes for the same task), Record resolves and caches the task's
// authoritative RunnerTokenHash from storage (at most once per 5s per task,
// including negative/missing-token and transient-error outcomes) so bogus
// tokens are dropped without ever evicting PID 1's valid heartbeat or
// amplifying PostgreSQL load.
func (b *HeartbeatBuffer) Record(key store.TaskKey, runnerTokenHash []byte) {
	if key.Name == "" || len(runnerTokenHash) == 0 {
		return
	}
	key = key.Normalized()
	hashCopy := append([]byte(nil), runnerTokenHash...)
	tokKey := string(hashCopy)
	receivedAt := b.clock()

	b.mu.Lock()
	if pinnedTok, knownNoToken, fresh := b.freshPinLocked(key, receivedAt); fresh {
		if knownNoToken || (pinnedTok != "" && tokKey != pinnedTok) {
			b.mu.Unlock()
			return
		}
	}

	byTok := b.pending[key]
	if byTok == nil {
		byTok = make(map[string]bufferedHeartbeat, 1)
		b.pending[key] = byTok
	}
	if prev, exists := byTok[tokKey]; exists {
		if !prev.receivedAt.After(receivedAt) {
			byTok[tokKey] = bufferedHeartbeat{tokenHash: hashCopy, receivedAt: receivedAt}
		}
		b.mu.Unlock()
		return
	}
	if len(byTok) < maxTokensPerTask {
		byTok[tokKey] = bufferedHeartbeat{tokenHash: hashCopy, receivedAt: receivedAt}
		b.mu.Unlock()
		return
	}

	// >= 4 distinct tokens for this task: allow at most ONE per-key storage
	// verification per tokenVerifyCooldown without holding any cross-task lock.
	curPin := b.pinned[key]
	if curPin.checking || (!curPin.lastCheckedAt.IsZero() && receivedAt.Sub(curPin.lastCheckedAt) <= tokenVerifyCooldown) {
		b.mu.Unlock()
		return
	}
	b.ensurePinCapacityLocked(key, receivedAt)
	b.checkSeq++
	seq := b.checkSeq
	curPin.checking = true
	curPin.seq = seq
	b.pinned[key] = curPin
	b.mu.Unlock()

	verifyCtx, cancel := context.WithTimeout(context.Background(), tokenVerifyTimeout)
	rows, err := b.store.GetTaskRows(verifyCtx, []store.TaskKey{key})
	cancel()

	checkedAt := b.clock()
	b.mu.Lock()
	defer b.mu.Unlock()
	latestPin := b.pinned[key]
	if !latestPin.checking || latestPin.seq != seq {
		// A concurrent ConfirmToken call established a newer pin while
		// GetTaskRows was in flight; do not overwrite it with stale state.
		return
	}
	if err != nil {
		// Record the failed attempt timestamp so transient DB errors enforce the
		// full 5s per-task cooldown instead of retrying on every request.
		latestPin.checking = false
		latestPin.tokenKey = ""
		latestPin.knownNoToken = false
		latestPin.lastCheckedAt = checkedAt
		b.pinned[key] = latestPin
		return
	}

	var authTok string
	if row := rows[key]; row != nil && len(row.RunnerTokenHash) > 0 {
		authTok = string(row.RunnerTokenHash)
	}
	b.pinTokenLocked(key, authTok, authTok == "", checkedAt)
	if authTok != "" && tokKey == authTok {
		byTok = b.pending[key]
		if byTok == nil {
			byTok = make(map[string]bufferedHeartbeat, 1)
			b.pending[key] = byTok
		}
		if prev, exists := byTok[tokKey]; !exists || !prev.receivedAt.After(receivedAt) {
			byTok[tokKey] = bufferedHeartbeat{tokenHash: hashCopy, receivedAt: receivedAt}
		}
	}
}

// PendingCount returns the number of distinct tasks currently buffered in memory.
func (b *HeartbeatBuffer) PendingCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.pending)
}

// HasFreshFlush reports whether this buffer has completed at least one
// successful Flush within maxAge according to its monotonic clock.
func (b *HeartbeatBuffer) HasFreshFlush(maxAge time.Duration) bool {
	b.mu.Lock()
	last := b.lastSuccessAt
	b.mu.Unlock()
	if last.IsZero() {
		return false
	}
	return b.clock().Sub(last) <= maxAge
}

// CanAcceptHeartbeats reports whether this replica currently holds an active
// flush lease in ax_replicas (i.e. has completed a successful Flush within
// ReplicaTTL). Runner HTTP handlers on `:8081` must return HTTP 503 Service
// Unavailable when CanAcceptHeartbeats returns false so runners fail over to a
// replica with working storage connectivity.
func (b *HeartbeatBuffer) CanAcceptHeartbeats() bool {
	return b.HasFreshFlush(b.replicaTTL)
}

// Flush atomically snapshots buffered heartbeats, writes them to storage, and
// updates this replica's ax_replicas watermark. If storage returns an error,
// un-flushed entries are merged back into the buffer (preserving newer entries
// recorded while the flush was in flight, honoring fresh token pins, and
// keeping each entry's original receipt timestamp) so transient database errors
// never drop heartbeats or fabricate fresher timestamps.
func (b *HeartbeatBuffer) Flush(ctx context.Context) error {
	b.flushMu.Lock()
	defer b.flushMu.Unlock()

	b.mu.Lock()
	flushStart := b.clock()
	batch := b.pending
	b.pending = make(map[store.TaskKey]map[string]bufferedHeartbeat)
	b.mu.Unlock()

	entries := make([]store.HeartbeatEntry, 0, len(batch))
	for key, byTok := range batch {
		for _, item := range byTok {
			age := max(flushStart.Sub(item.receivedAt), time.Duration(0))
			entries = append(entries, store.HeartbeatEntry{
				Key:             key,
				RunnerTokenHash: item.tokenHash,
				Age:             age,
			})
		}
	}

	if err := b.store.FlushHeartbeats(ctx, b.replicaID, b.replicaTTL, entries); err != nil {
		if len(batch) > 0 {
			mergeNow := b.clock()
			b.mu.Lock()
			for key, byTok := range batch {
				pinnedTok, knownNoToken, fresh := b.freshPinLocked(key, mergeNow)
				if fresh && knownNoToken {
					continue
				}
				for tokKey, item := range byTok {
					if mergeNow.Sub(item.receivedAt) > maxBufferedHeartbeatAge {
						continue
					}
					if fresh && pinnedTok != "" && tokKey != pinnedTok {
						continue
					}
					dst := b.pending[key]
					if dst == nil {
						dst = make(map[string]bufferedHeartbeat, len(byTok))
						b.pending[key] = dst
					}
					if cur, exists := dst[tokKey]; exists {
						if item.receivedAt.After(cur.receivedAt) {
							dst[tokKey] = item
						}
						continue
					}
					if fresh && pinnedTok != "" && tokKey == pinnedTok && len(dst) >= maxTokensPerTask {
						for k := range dst {
							if k != pinnedTok {
								delete(dst, k)
								break
							}
						}
					}
					if len(dst) < maxTokensPerTask {
						dst[tokKey] = item
					}
				}
			}
			b.mu.Unlock()
		}
		return err
	}

	b.mu.Lock()
	b.lastSuccessAt = b.clock()
	b.mu.Unlock()
	return nil
}

func (b *HeartbeatBuffer) flushBounded(ctx context.Context) error {
	flushCtx, cancel := context.WithTimeout(ctx, b.flushTimeout)
	defer cancel()
	return b.Flush(flushCtx)
}

// Run registers the replica in ax_replicas via an immediate Flush, periodically
// flushes buffered heartbeats until ctx is canceled, and performs a bounded
// final Flush on shutdown. If and only if the final Flush succeeds, Run removes
// the replica's row from ax_replicas so live-replica scaling adjusts
// immediately without hiding unflushed heartbeats on a failed shutdown flush.
func (b *HeartbeatBuffer) Run(ctx context.Context) error {
	nextWait := jitter80To100Pct(b.flushInterval)
	if err := b.flushBounded(ctx); err != nil {
		if ctx.Err() == nil {
			slog.Warn("initial runner heartbeat flush failed; will retry",
				"replica", b.replicaID,
				"error", err,
			)
		}
		nextWait = b.retryInterval
	}

	timer := time.NewTimer(nextWait)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			shutCtx, cancel := context.WithTimeout(context.Background(), heartbeatShutdownTimeout)
			defer cancel()
			if err := b.Flush(shutCtx); err != nil {
				slog.Warn("final runner heartbeat flush on shutdown failed; leaving replica lease to expire naturally",
					"replica", b.replicaID,
					"error", err,
				)
				return nil
			}
			if err := b.store.UnregisterReplica(shutCtx, b.replicaID); err != nil {
				slog.Warn("unregistering replica on shutdown failed",
					"replica", b.replicaID,
					"error", err,
				)
			}
			return nil

		case <-timer.C:
			if err := b.flushBounded(ctx); err != nil {
				if ctx.Err() == nil {
					slog.Warn("flushing runner heartbeats failed; scheduling fast retry",
						"replica", b.replicaID,
						"error", err,
					)
				}
				timer.Reset(b.retryInterval)
				continue
			}
			timer.Reset(jitter80To100Pct(b.flushInterval))
		}
	}
}

func jitter80To100Pct(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	factor := 0.8 + 0.2*rand.Float64() // [0.8, 1.0)
	return max(time.Duration(float64(d)*factor), time.Millisecond)
}
