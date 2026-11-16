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
	"sync"
	"testing"
	"time"

	"github.com/google/ax/internal/lifecycle"
)

func TestAdmissionController_TaskAtespaceAndKindLimits(t *testing.T) {
	ac := newAdmissionController(3, 2, 1, map[lifecycle.ActionKind]int{
		lifecycle.ActionResumeActor: 2,
	})

	// 1. Task slot cap and full-to-non-full transition indicator.
	if got := ac.AcquireTaskSlots(5); got != 3 {
		t.Fatalf("AcquireTaskSlots(5) = %d, want 3", got)
	}
	if got := ac.AcquireTaskSlots(1); got != 0 {
		t.Fatalf("AcquireTaskSlots when full = %d, want 0", got)
	}
	if wasFull := ac.ReleaseTaskSlots(1); !wasFull {
		t.Fatal("expected wasFull == true when releasing from capacity")
	}
	if wasFull := ac.ReleaseTaskSlots(2); wasFull {
		t.Fatal("expected wasFull == false when releasing below capacity")
	}

	// 2. Per-atespace and per-kind action slot caps, plus map cleanup on release.
	r1, ok1 := ac.TryAcquireAction("tenant-a", lifecycle.ActionResumeActor)
	r2, ok2 := ac.TryAcquireAction("tenant-a", lifecycle.ActionResumeActor)
	if !ok1 || !ok2 {
		t.Fatal("expected first two ResumeActor slots for tenant-a to succeed")
	}
	if _, ok := ac.TryAcquireAction("tenant-a", lifecycle.ActionCreateActor); ok {
		t.Fatal("expected third concurrent action for tenant-a rejected by MaxInFlightPerAtespace=2")
	}
	if f := ac.LeaseFilter(); len(f.ExcludeAtespaces) != 1 || f.ExcludeAtespaces[0] != "tenant-a" ||
		len(f.ExcludeActionKinds) != 1 || f.ExcludeActionKinds[0] != lifecycle.ActionResumeActor ||
		f.RestrictActionKinds {
		t.Fatalf("LeaseFilter() = %+v, want ExcludeAtespaces=[tenant-a], ExcludeActionKinds=[RESUME_ACTOR], RestrictActionKinds=false", f)
	}
	if _, ok := ac.TryAcquireAction("tenant-b", lifecycle.ActionResumeActor); ok {
		t.Fatal("expected third ResumeActor rejected by ActionResumeActor limit=2")
	}
	rOther, okOther := ac.TryAcquireAction("tenant-b", lifecycle.ActionCreateActor)
	if !okOther {
		t.Fatal("expected tenant-b CreateActor to succeed under fallback limit")
	}
	if _, ok := ac.TryAcquireAction("tenant-c", lifecycle.ActionDeleteActor); ok {
		t.Fatal("expected second fallback action rejected by otherKindLimit=1")
	}
	if f := ac.LeaseFilter(); !f.RestrictActionKinds || len(f.AllowedActionKinds) != 3 {
		t.Fatalf("LeaseFilter() with saturated fallback pool = %+v, want RestrictActionKinds=true and 3 unsaturated dedicated kinds", f)
	} else {
		for _, k := range f.AllowedActionKinds {
			if k == lifecycle.ActionResumeActor {
				t.Fatalf("LeaseFilter() AllowedActionKinds included saturated RESUME_ACTOR: %+v", f.AllowedActionKinds)
			}
		}
	}

	// WaitForActionSlot wakes immediately once r1 frees a slot on tenant-a / ResumeActor.
	woke := make(chan error, 1)
	go func() {
		woke <- ac.WaitForActionSlot(context.Background(), "tenant-a", lifecycle.ActionResumeActor)
	}()
	time.Sleep(10 * time.Millisecond)
	if !r1() {
		t.Fatal("expected r1() to report freeing a saturated bucket")
	}
	if err := <-woke; err != nil {
		t.Fatalf("WaitForActionSlot = %v", err)
	}
	if r1() {
		t.Fatal("idempotent second r1() must return false")
	}
	r2()
	rOther()
	ac.mu.Lock()
	leakedAtespaces := len(ac.inFlightAtespace)
	leakedKinds := len(ac.inFlightKind)
	ac.mu.Unlock()
	if leakedAtespaces != 0 || leakedKinds != 0 {
		t.Fatalf("leaked map entries: atespaces=%d kinds=%d", leakedAtespaces, leakedKinds)
	}
}

func TestRateLimiter_CancellationRefundsAtMostOneToken(t *testing.T) {
	// 10 QPS, burst 1 -> 100ms between calls after initial burst token.
	lim := newRateLimiter(10, 1)

	// Consume the burst token immediately.
	if err := lim.Wait(context.Background()); err != nil {
		t.Fatalf("initial Wait: %v", err)
	}

	// Queue 3 waiters; cancel the 3rd immediately so it refunds only its own
	// single token (~100ms), NOT all 300ms of queued debt.
	var wg sync.WaitGroup
	wg.Add(2)
	for range 2 {
		go func() {
			defer wg.Done()
			_ = lim.Wait(context.Background())
		}()
	}
	// Give the 2 background waiters a moment to reserve their slots.
	time.Sleep(10 * time.Millisecond)

	cancelCtx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(5 * time.Millisecond)
		cancel()
	}()
	if err := lim.Wait(cancelCtx); err == nil {
		t.Fatal("expected canceled Wait to return error")
	}

	// A 4th waiter arriving right after the 3rd canceled must still wait behind
	// the 2 active waiters (~180ms+ total from start), proving the canceled
	// waiter did not wipe out their token debt.
	startFourth := time.Now()
	if err := lim.Wait(context.Background()); err != nil {
		t.Fatalf("fourth Wait: %v", err)
	}
	elapsed := time.Since(startFourth)
	if elapsed < 120*time.Millisecond {
		t.Fatalf("fourth Wait completed in %v (<120ms); canceled waiter over-refunded tokens", elapsed)
	}
	wg.Wait()

	// Doomed deadline pre-check: a caller whose context deadline is shorter than
	// the required token wait fails immediately with DeadlineExceeded without
	// deducting a token or delaying subsequent callers.
	slowLim := newRateLimiter(0.5, 1) // 2s per token after initial burst
	if err := slowLim.Wait(context.Background()); err != nil {
		t.Fatalf("slowLim initial Wait: %v", err)
	}
	shortCtx, cancelShort := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancelShort()
	startDoomed := time.Now()
	if err := slowLim.Wait(shortCtx); err != context.DeadlineExceeded {
		t.Fatalf("doomed Wait = %v, want DeadlineExceeded", err)
	}
	if d := time.Since(startDoomed); d >= 200*time.Millisecond {
		t.Fatalf("doomed Wait slept %v instead of failing fast", d)
	}
}

func TestCircuitBreaker_BrownoutIgnoresPreOpenDrainersAndPreservesProbeCooldown(t *testing.T) {
	now := time.Date(2026, 11, 16, 15, 0, 0, 0, time.UTC)
	cb := newCircuitBreaker(CircuitBreakerConfig{
		Window:           30 * time.Second,
		FailureThreshold: 0.5,
		MinCalls:         4,
		ProbeInterval:    5 * time.Second,
	}, func() time.Time { return now })

	// 3 transient failures + 1 success = 75% failure rate across 4 calls -> opens!
	cb.Record(true, false)
	cb.Record(true, false)
	cb.Record(false, false)
	cb.Record(true, false)
	if !cb.IsOpen() {
		t.Fatal("expected circuit breaker to open at 75% transient failure rate")
	}

	// Pre-open draining workers completing (success or failure) with isProbe=false
	// MUST NOT close the breaker or disturb probe state.
	if closed := cb.Record(false, false); closed || !cb.IsOpen() {
		t.Fatal("pre-open draining worker falsely closed an open circuit breaker")
	}

	// Before 5s ProbeInterval elapses, PlanLease returns 0.
	now = now.Add(4 * time.Second)
	if limit, probe := cb.PlanLease(10); limit != 0 || probe {
		t.Fatalf("PlanLease before ProbeInterval = (%d, %v), want (0, false)", limit, probe)
	}

	// At +5s, PlanLease grants 1 probe; if LeaseDue finds 0 tasks and calls
	// CancelProbe, the very next poll (100ms later) must STILL get a probe
	// immediately without waiting another 5s!
	now = now.Add(time.Second)
	limit, probe := cb.PlanLease(10)
	if limit != 1 || !probe {
		t.Fatalf("PlanLease at ProbeInterval = (%d, %v), want (1, true)", limit, probe)
	}
	if l2, p2 := cb.PlanLease(10); l2 != 0 || p2 {
		t.Fatalf("concurrent PlanLease while probe in flight = (%d, %v), want (0, false)", l2, p2)
	}
	cb.CancelProbe()

	now = now.Add(100 * time.Millisecond)
	limit, probe = cb.PlanLease(10)
	if limit != 1 || !probe {
		t.Fatalf("PlanLease after CancelProbe = (%d, %v), want (1, true)", limit, probe)
	}

	// While probe is in flight, a pre-open failing worker must NOT clear probeInFlight.
	cb.Record(true, false)
	if l3, p3 := cb.PlanLease(10); l3 != 0 || p3 {
		t.Fatalf("pre-open failing worker cleared probeInFlight: got (%d, %v)", l3, p3)
	}

	// Failed probe resets cooldown for 5s; subsequent healthy probe closes breaker.
	cb.Record(true, true)
	now = now.Add(2 * time.Second)
	if l4, _ := cb.PlanLease(10); l4 != 0 {
		t.Fatalf("expected 5s cooldown after failed probe, got limit=%d", l4)
	}
	now = now.Add(4 * time.Second)
	if l5, p5 := cb.PlanLease(10); l5 != 1 || !p5 {
		t.Fatalf("PlanLease after failed-probe cooldown = (%d, %v), want (1, true)", l5, p5)
	}
	if closed := cb.Record(false, true); !closed || cb.IsOpen() {
		t.Fatal("healthy probe should close circuit breaker")
	}
}
