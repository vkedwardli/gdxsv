package main

import (
	"fmt"
	"math"
	"testing"
	"time"
)

type legacySpectatorTest struct {
	t   *testing.T
	s   *SpectatorSession
	now time.Time
}

func newLegacySpectatorTest(t *testing.T) *legacySpectatorTest {
	t.Helper()
	return &legacySpectatorTest{t, newTestSpectatorSession(), time.Now()}
}

func (h *legacySpectatorTest) inputs(peer string, frame int32, values ...uint64) int32 {
	h.t.Helper()
	ack, _ := h.s.pushLegacyInputsAt(peer, frame, values, h.now)
	return ack
}

func (h *legacySpectatorTest) start(peer string, frame int32, seed uint64) {
	h.t.Helper()
	if !h.s.pushLegacyRoundEventAt(peer, frame, seed, h.now) {
		h.t.Fatalf("start rejected: peer=%s frame=%d", peer, frame)
	}
}

func (h *legacySpectatorTest) advance(d time.Duration) {
	h.now = h.now.Add(d)
	h.s.mtx.Lock()
	h.s.flushLegacyLocked(h.now)
	h.s.mtx.Unlock()
}

func TestSpectatorLegacy_HoldbackAndRetryDeadline(t *testing.T) {
	h := newLegacySpectatorTest(t)
	h.start("P1", 0, 111)
	assertEq(t, int32(2), h.inputs("P1", 0, 10, 11))
	assertEq(t, 0, len(h.s.log.Inputs))
	h.advance(499 * time.Millisecond)
	assertEq(t, 0, len(h.s.log.Inputs))
	assertEq(t, int32(2), h.inputs("P1", 0, 99, 99))
	h.advance(time.Millisecond)
	assertEq(t, []uint64{10, 11}, h.s.log.Inputs)
	assertEq(t, 0, len(h.s.legacy.publishers[0].inputs))
	// Each new entry has its own age, not a one-off session startup delay.
	h.inputs("P1", 2, 12)
	assertEq(t, []uint64{10, 11}, h.s.log.Inputs)
	h.advance(spectatorLegacyHoldback)
	assertEq(t, []uint64{10, 11, 12}, h.s.log.Inputs)
}

func TestSpectatorLegacy_InputsBeforeFirstMarker(t *testing.T) {
	h := newLegacySpectatorTest(t)
	h.inputs("P1", 0, 1, 2)
	h.advance(5 * time.Second / 60)
	h.start("P1", 0, 111)
	assertEq(t, 0, len(h.s.log.Inputs))
	h.advance(spectatorLegacyHoldback)
	assertEq(t, []uint64{1, 2}, h.s.log.Inputs)
}

func TestSpectatorLegacy_DelayedMarkersNormalizeAllPublishers(t *testing.T) {
	for _, delayFrames := range []int{0, 5, 15, 29} {
		for _, offset := range []int{1, 2} {
			t.Run(fmt.Sprintf("delay_%d_frames_offset_%d", delayFrames, offset), func(t *testing.T) {
				h := newLegacySpectatorTest(t)
				for i := 1; i <= 4; i++ {
					h.start(fmt.Sprint(i), 0, 111)
				}
				// Round 1 ends at 99 in the first reporter's timeline.
				prefix := make([]uint64, 99)
				for i := 1; i <= 4; i++ {
					h.inputs(fmt.Sprint(i), 0, prefix...)
				}
				h.advance(spectatorLegacyHoldback)
				// The next inputs differ immediately, exposing even a one-entry
				// shift. P1 has only the first input: the rest MUST use redundancy.
				round := []uint64{0x10, 0, 8, 4, 0x20, 0x4000, 0, 0x2000}
				h.inputs("1", 99, round[0])
				for i := 2; i <= 4; i++ {
					tail := append(make([]uint64, offset*(i-1)), round...)
					h.inputs(fmt.Sprint(i), 99, tail...)
				}
				h.advance(time.Duration(delayFrames) * time.Second / 60)
				h.start("1", 99, 222)
				// Different arrival order, same round ordinal. No extra UI rounds.
				for _, i := range []int{4, 2, 3} {
					h.start(fmt.Sprint(i), int32(99+offset*(i-1)), 222)
				}
				h.advance(spectatorLegacyHoldback)
				assertEq(t, append(prefix, round...), h.s.log.Inputs)
				assertEq(t, []int32{0, 99}, h.s.log.StartMsgIndexes)
				assertEq(t, []uint64{111, 222}, h.s.log.StartMsgRandoms)
			})
		}
	}
}

func TestSpectatorLegacy_GatesPublisherUntilItsOwnMarker(t *testing.T) {
	h := newLegacySpectatorTest(t)
	h.start("P1", 0, 111)
	h.start("P2", 0, 111)
	h.inputs("P1", 0, 10, 11, 12, 20)
	h.inputs("P2", 0, 10, 11, 12, 0, 20, 21, 22)
	h.start("P1", 3, 222)
	h.advance(spectatorLegacyHoldback)
	assertEq(t, []uint64{10, 11, 12, 20}, h.s.log.Inputs)
	// More than the holdback has elapsed, but a known missing marker is
	// never replaced by a guessed offset. P2's raw data remains buffered.
	h.advance(time.Second)
	assertEq(t, 4, len(h.s.log.Inputs))
	h.start("P2", 4, 222)
	assertEq(t, []uint64{10, 11, 12, 20, 21, 22}, h.s.log.Inputs)
	assertEq(t, int32(7), h.inputs("P2", 4, 20, 21, 22))
}

func TestSpectatorLegacy_FirstReporterCanChangeAcrossRounds(t *testing.T) {
	h := newLegacySpectatorTest(t)
	h.start("P1", 0, 7)
	h.start("P2", 0, 7)
	// P2 reports first but has the LONGER previous round.
	h.start("P2", 4, 7)
	h.start("P1", 3, 7)
	h.inputs("P1", 0, 10, 11, 12, 20, 21, 30, 31)
	h.inputs("P2", 0, 10, 11, 12, 0, 20, 21, 0, 30, 31)
	// P1 reports Round 3 first: local 5 maps to master 6, not 5.
	h.start("P1", 5, 8)
	h.start("P2", 7, 8)
	h.advance(spectatorLegacyHoldback)
	assertEq(t, []int32{0, 4, 6}, h.s.log.StartMsgIndexes)
	assertEq(t, []uint64{7, 7, 8}, h.s.log.StartMsgRandoms)
	assertEq(t, []uint64{10, 11, 12, 0, 20, 21, 30, 31}, h.s.log.Inputs)
	// Repeated seeds are legitimate; retries do not add rounds or refresh ages.
	h.start("P2", 4, 999)
	assertEq(t, 3, len(h.s.log.StartMsgIndexes))
}

func TestSpectatorLegacy_RedundancyFillsGapWithoutInventingReceiveACK(t *testing.T) {
	h := newLegacySpectatorTest(t)
	h.start("P1", 0, 111)
	h.start("P2", 0, 111)
	assertEq(t, int32(1), h.inputs("P1", 0, 10))
	assertEq(t, int32(1), h.inputs("P1", 3, 13))
	assertEq(t, int32(4), h.inputs("P2", 0, 10, 11, 12, 13))
	h.advance(spectatorLegacyHoldback)
	assertEq(t, []uint64{10, 11, 12, 13}, h.s.log.Inputs)
	assertEq(t, int32(1), h.inputs("P1", 3, 13))
	assertEq(t, int32(4), h.inputs("P1", 1, 11, 12))
	assertEq(t, 0, len(h.s.legacy.publisher("P1").inputs))
}

func TestSpectatorLegacy_RejectsInvalidAndBoundsResources(t *testing.T) {
	for _, tc := range []struct {
		name  string
		peer  string
		frame int32
		count int
	}{
		{"empty publisher", "", 0, 1},
		{"negative", "P1", -1, 1},
		{"empty inputs", "P1", 0, 0},
		{"oversized packet", "P1", 0, 129},
		{"beyond window", "P1", 256, 1},
		{"overflow", "P1", math.MaxInt32, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newLegacySpectatorTest(t)
			assertEq(t, int32(0), h.inputs(tc.peer, tc.frame, make([]uint64, tc.count)...))
			assertEq(t, 0, len(h.s.legacy.publishers))
		})
	}
	t.Run("publisher ceiling", func(t *testing.T) {
		h := newLegacySpectatorTest(t)
		for i := 0; i < maxSpectatorLegacyPublishers; i++ {
			h.start(fmt.Sprint(i), 0, 111)
		}
		assertEq(t, int32(0), h.inputs("extra", 0, 99))
		assertEq(t, false, h.s.pushLegacyRoundEventAt("extra", 0, 111, h.now))
		assertEq(t, maxSpectatorLegacyPublishers, len(h.s.legacy.publishers))
	})
	t.Run("unmapped input ceiling", func(t *testing.T) {
		h := newLegacySpectatorTest(t)
		for i := 0; i < maxSpectatorLegacyBufferedFrames; i += 128 {
			assertEq(t, int32(i+128), h.inputs("P1", int32(i), make([]uint64, 128)...))
		}
		assertEq(t, int32(maxSpectatorLegacyBufferedFrames), h.inputs("P1", maxSpectatorLegacyBufferedFrames, 1))
		assertEq(t, maxSpectatorLegacyBufferedFrames, len(h.s.legacy.publisher("P1").inputs))
		h.advance(spectatorLegacyHoldback)
		h.start("P1", 0, 111)
		assertEq(t, maxSpectatorLegacyBufferedFrames, len(h.s.log.Inputs))
		assertEq(t, int32(maxSpectatorLegacyBufferedFrames+1), h.inputs("P1", maxSpectatorLegacyBufferedFrames, 1))
	})
}

func TestSpectatorLegacy_OutOfOrderReceiveWindow(t *testing.T) {
	h := newLegacySpectatorTest(t)
	h.start("P1", 0, 111)
	assertEq(t, int32(0), h.inputs("P1", 1, testSpectatorInputs(1, 128)...))
	assertEq(t, int32(0), h.inputs("P1", 129, testSpectatorInputs(129, 127)...))
	assertEq(t, 255, len(h.s.legacy.publisher("P1").inputs))
	assertEq(t, int32(0), h.inputs("P1", 256, 256))
	assertEq(t, int32(256), h.inputs("P1", 0, 0))
	h.advance(spectatorLegacyHoldback)
	assertEq(t, testSpectatorInputs(0, 256), h.s.log.Inputs)
}

func TestSpectatorLegacy_MismatchedSeedCannotSupplyRound(t *testing.T) {
	h := newLegacySpectatorTest(t)
	h.start("P1", 0, 111)
	h.start("P2", 0, 222)
	h.inputs("P2", 0, 99, 98)
	h.inputs("P1", 0, 10)
	h.advance(spectatorLegacyHoldback)
	assertEq(t, []uint64{10}, h.s.log.Inputs)
}

func TestSpectatorLegacy_CloseWaitsForBufferedTail(t *testing.T) {
	h := newLegacySpectatorTest(t)
	h.start("P1", 0, 111)
	h.inputs("P1", 0, 10, 11)
	addr := testAddr(43210)
	subscribeTestSpectator(h.s, addr, 0)
	sub := h.s.downlinks[addr.String()]
	sub.sentHeader = true
	sub.ackedRoundStateVersion = h.s.roundStateVersion
	h.s.Close("game_end", -1)
	h.s.closedAt = h.now // Deterministic clock for close and receipt aging.
	_, ok := h.s.buildPush(sub)
	assertEq(t, false, ok) // No premature close for an empty published prefix.
	h.advance(100 * time.Millisecond)
	assertEq(t, int32(3), h.inputs("P1", 2, 12)) // TCP close overtook final UDP.
	h.advance(400 * time.Millisecond)
	push, ok := h.s.buildPush(sub)
	assertEq(t, true, ok)
	assertEq(t, []uint64{10, 11}, push.Inputs)
	assertEq(t, "", push.CloseReason)
	h.s.Ack(addr.String(), 2, 0, h.s.roundStateVersion)
	h.advance(100 * time.Millisecond)
	push, ok = h.s.buildPush(sub)
	assertEq(t, true, ok)
	assertEq(t, []uint64{12}, push.Inputs)
	assertEq(t, "", push.CloseReason)
	h.s.Ack(addr.String(), 3, 0, h.s.roundStateVersion)
	push, ok = h.s.buildPush(sub)
	assertEq(t, true, ok)
	assertEq(t, "game_end", push.CloseReason)
	assertEq(t, int32(3), h.inputs("P1", 3, 99))
	h.start("P1", 0, 111) // A lost round ACK can still be recovered.
}

func TestSpectatorLegacy_ExpiredHoldbackDoesNotRewritePublishedInputs(t *testing.T) {
	h := newLegacySpectatorTest(t)
	h.start("P1", 0, 111)
	h.start("P2", 0, 111)
	h.inputs("P1", 0, 10, 11, 12, 20)
	h.inputs("P2", 0, 10, 11, 12, 0, 20, 21)
	h.advance(spectatorLegacyHoldback)
	before := append([]uint64(nil), h.s.log.Inputs...)
	// This intentionally exceeds the legacy compatibility allowance. The
	// late-boundary warning cannot retract bytes already sent to spectators.
	h.advance(time.Millisecond)
	h.start("P1", 3, 222)
	h.start("P2", 4, 222)
	assertEq(t, before, h.s.log.Inputs)
	assertEq(t, []int32{0, 3}, h.s.log.StartMsgIndexes)
}

func TestSpectatorCanonicalInputsHaveNoLegacyHoldback(t *testing.T) {
	s := newTestSpectatorSession()
	s.PushRoundEvent(0, 111)
	ack, advanced := s.PushInputs(0, []uint64{10, 11})
	assertEq(t, int32(2), ack)
	assertEq(t, true, advanced)
	assertEq(t, []uint64{10, 11}, s.log.Inputs)
	assertEq(t, 0, len(s.legacy.publishers))
	s.Close("game_end", -1)
	assertEq(t, true, s.legacy.finished())
}

func TestSpectatorLegacy_FanoutTimerReleasesWithoutNewPacketsOrViewers(t *testing.T) {
	r := newTestSpectatorRegistry()
	s := newTestSpectatorSession()
	r.sessions[s.battleCode] = s
	received := time.Now().Add(-spectatorLegacyHoldback - time.Millisecond)
	s.pushLegacyRoundEventAt("P1", 0, 111, received)
	s.pushLegacyInputsAt("P1", 0, []uint64{10, 11}, received)
	assertEq(t, 0, len(s.log.Inputs))
	r.fanoutOnce(nil) // No subscribers means no socket writes.
	assertEq(t, []uint64{10, 11}, s.log.Inputs)
	live, _ := r.LiveStatus(s.battleCode)
	assertEq(t, true, live)
}

func TestSpectatorCanonicalInputBypassesLegacyQueue(t *testing.T) {
	h := newLegacySpectatorTest(t)
	h.start("legacy", 0, 111)
	h.inputs("legacy", 0, 99, 98)
	assertEq(t, 0, len(h.s.log.Inputs))
	// Already-normalized input need not wait for a legacy publisher's timer,
	// and the later legacy flush cannot overwrite the canonical prefix.
	ack, advanced := h.s.PushInputs(0, []uint64{10, 11})
	assertEq(t, int32(2), ack)
	assertEq(t, true, advanced)
	assertEq(t, []uint64{10, 11}, h.s.log.Inputs)
	h.advance(spectatorLegacyHoldback)
	assertEq(t, []uint64{10, 11}, h.s.log.Inputs)
	assertEq(t, 0, len(h.s.legacy.publisher("legacy").inputs))
}

func TestSpectatorLegacy_CloseWithMissingMarkerDoesNotWaitForever(t *testing.T) {
	h := newLegacySpectatorTest(t)
	h.inputs("P1", 0, 10, 11)
	h.s.Close("game_end", -1)
	h.s.closedAt = h.now
	h.advance(spectatorLegacyHoldback)
	assertEq(t, true, h.s.legacy.finished())
	assertEq(t, 0, len(h.s.legacy.publisher("P1").inputs))
	assertEq(t, false, h.s.pushLegacyRoundEventAt("P1", 0, 111, h.now))
}
