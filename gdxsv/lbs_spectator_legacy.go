package main

import (
	"math"
	"time"

	"go.uber.org/zap"
)

// Legacy input packets carry participant-local absolute indexes, with round
// starts on a separate, independently retried channel. Hold raw inputs BEFORE
// translating/committing them, giving starts time to overtake their inputs.
// This is best-effort compatibility, not a guarantee against arbitrarily late
// markers. A future round-tagged input handler should use the canonical append
// path directly; neither that path nor the downlink imposes this delay.
const spectatorLegacyHoldback = 500 * time.Millisecond

// Normally only ~30 inputs per publisher await release. Bound storage even if
// a publisher never supplies its markers, or a missing input stalls the log.
const maxSpectatorLegacyBufferedFrames = 4096
const maxSpectatorLegacyPublishers = 4

type spectatorLegacyInput struct {
	value      uint64
	receivedAt time.Time // First receipt; retries must not postpone release.
}

type spectatorLegacyStart struct {
	frame int32
	seed  uint64
}

type spectatorLegacyPublisher struct {
	address          string // Observed IP:port identifies a timeline, not a user.
	inputs           map[int32]spectatorLegacyInput
	ackedFrame       int32 // Own contiguous received position, NOT master position.
	starts           []spectatorLegacyStart
	publishedThrough int32
}

type spectatorLegacyRecorder struct {
	// At most four entries; preserve admission order for deterministic ties.
	publishers  []*spectatorLegacyPublisher
	lastInputAt time.Time
	drained     bool
}

func (l *spectatorLegacyRecorder) publisher(address string) *spectatorLegacyPublisher {
	for _, p := range l.publishers {
		if p.address == address {
			return p
		}
	}
	return nil
}

func (l *spectatorLegacyRecorder) finished() bool {
	return len(l.publishers) == 0 || l.drained
}

func (s *SpectatorSession) addLegacyPublisherLocked(address string) *spectatorLegacyPublisher {
	limit := maxSpectatorLegacyPublishers
	if n := len(s.log.Users); n > 0 && n < limit {
		limit = n
	}
	if s.closed || len(s.legacy.publishers) >= limit {
		return nil
	}
	p := &spectatorLegacyPublisher{address: address, inputs: make(map[int32]spectatorLegacyInput)}
	s.legacy.publishers = append(s.legacy.publishers, p)
	return p
}

// A TCP close report can overtake another publisher's last UDP packets. Give
// admitted publishers one holdback interval to finish; then age the final
// inputs normally before exposing close. Never reopen a finished recording.
func (s *SpectatorSession) acceptsLegacyLocked(now time.Time) bool {
	return !s.closed || (!s.legacy.drained && now.Before(s.closedAt.Add(spectatorLegacyHoldback)))
}

// PushLegacyInputs is the current absolute-index wire protocol's entry point.
// ACK reception immediately, without waiting for normalization/publication.
func (s *SpectatorSession) PushLegacyInputs(publisher string, startFrame int32, inputs []uint64) (int32, bool) {
	return s.pushLegacyInputsAt(publisher, startFrame, inputs, time.Now())
}

func (s *SpectatorSession) pushLegacyInputsAt(publisher string, startFrame int32, inputs []uint64, now time.Time) (int32, bool) {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	p := s.legacy.publisher(publisher)
	var ack int32
	if p != nil {
		ack = p.ackedFrame
	}
	end := int64(startFrame) + int64(len(inputs))
	if publisher == "" || startFrame < 0 || len(inputs) == 0 || len(inputs) > maxSpectatorPushFrames ||
		end > math.MaxInt32 || !s.acceptsLegacyLocked(now) ||
		(startFrame > ack && end > int64(ack)+maxSpectatorPendingFrames) {
		return ack, false
	}
	if p == nil {
		p = s.addLegacyPublisherLocked(publisher)
		if p == nil {
			return 0, false
		}
	}
	// Reject atomically if the bounded raw buffer would overflow. Existing
	// entries and their receipt times survive retries, even when it is full.
	additional := 0
	for i := range inputs {
		frame := startFrame + int32(i)
		if _, exists := p.inputs[frame]; frame >= ack && !exists {
			additional++
		}
	}
	if len(p.inputs)+additional > maxSpectatorLegacyBufferedFrames {
		return ack, false
	}
	for i, value := range inputs {
		frame := startFrame + int32(i)
		if _, exists := p.inputs[frame]; frame >= ack && !exists {
			p.inputs[frame] = spectatorLegacyInput{value, now}
		}
	}
	for {
		if _, exists := p.inputs[p.ackedFrame]; !exists {
			break
		}
		p.ackedFrame++
	}
	if additional > 0 {
		s.legacy.lastInputAt = now
	}
	if !s.closed {
		s.lastPushAt = now
	}
	return p.ackedFrame, s.flushLegacyLocked(now)
}

// Legacy clients send only their oldest unacknowledged start. Its ordinal in
// THIS publisher's ordered list identifies the round; absolute indexes and
// seeds alone cannot deduplicate reports from different participants.
func (s *SpectatorSession) PushLegacyRoundEvent(publisher string, frame int32, seed uint64) bool {
	return s.pushLegacyRoundEventAt(publisher, frame, seed, time.Now())
}

func (s *SpectatorSession) pushLegacyRoundEventAt(publisher string, frame int32, seed uint64, now time.Time) bool {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	if publisher == "" || frame < 0 {
		return false
	}
	p := s.legacy.publisher(publisher)
	if p != nil {
		for _, start := range p.starts {
			if start.frame == frame {
				return true // Recover a lost ACK, including after close.
			}
		}
		if len(p.starts) >= maxSpectatorRounds ||
			(len(p.starts) > 0 && frame <= p.starts[len(p.starts)-1].frame) {
			return false
		}
	}
	if !s.acceptsLegacyLocked(now) {
		return false
	}
	if p == nil {
		p = s.addLegacyPublisherLocked(publisher)
		if p == nil {
			return false
		}
	}
	round := len(p.starts)
	if round == len(s.log.StartMsgIndexes) {
		// The first report sets this boundary, in the PREVIOUS round's
		// master coordinates. The next round may be led by a different peer.
		canonical := int64(frame)
		if round > 0 {
			canonical += int64(s.log.StartMsgIndexes[round-1]) - int64(p.starts[round-1].frame)
		}
		if canonical < 0 || canonical >= math.MaxInt32 ||
			!s.appendRoundStartLocked(int32(canonical), seed) {
			return false
		}
		if canonical < int64(len(s.log.Inputs)) {
			logger.Warn("legacy spectator round marker exceeded publication holdback",
				zap.String("battle_code", s.battleCode), zap.String("publisher", publisher),
				zap.Int("round", round+1), zap.Int64("start_frame", canonical),
				zap.Int("published_inputs", len(s.log.Inputs)))
		}
	} else if seed != s.log.StartMsgRandoms[round] {
		// Keep the ordinal so following rounds still line up, but this
		// publisher cannot supply inputs for a round with a different seed.
		logger.Warn("legacy spectator round seed disagrees; gating publisher for round",
			zap.String("battle_code", s.battleCode), zap.String("publisher", publisher),
			zap.Int("round", round+1))
	}
	if frame < p.publishedThrough {
		logger.Warn("legacy spectator publisher marker arrived after its inputs were used",
			zap.String("battle_code", s.battleCode), zap.String("publisher", publisher),
			zap.Int("round", round+1), zap.Int32("start_frame", frame))
	}
	p.starts = append(p.starts, spectatorLegacyStart{frame, seed})
	if !s.closed {
		s.lastPushAt = now
	}
	s.flushLegacyLocked(now)
	return true
}

// flushLegacyLocked releases aged inputs using the markers known NOW, not the
// mapping at packet receipt. Called by the fanout timer even with no viewers.
func (s *SpectatorSession) flushLegacyLocked(now time.Time) bool {
	if s.legacy.finished() {
		return false
	}
	before := len(s.log.Inputs)
	cutoff := now.Add(-spectatorLegacyHoldback)
	starts := s.log.StartMsgIndexes
	for len(starts) > 0 {
		next := int32(len(s.log.Inputs))
		round := len(starts) - 1
		for round > 0 && next < starts[round] {
			round--
		}
		var chosen *spectatorLegacyPublisher
		var chosenFrame int32
		var chosenInput spectatorLegacyInput
		for _, p := range s.legacy.publishers {
			// As soon as ANY peer announces a new round, freeze peers whose
			// own marker has not arrived. Their last-known offset is stale.
			if len(p.starts) < len(starts) || p.starts[round].seed != s.log.StartMsgRandoms[round] {
				continue
			}
			local := int64(p.starts[round].frame) + int64(next) - int64(starts[round])
			if local < 0 || local >= math.MaxInt32 ||
				(round+1 < len(p.starts) && local >= int64(p.starts[round+1].frame)) {
				continue // This peer's shorter previous round cannot fill its extra tail.
			}
			input, exists := p.inputs[int32(local)]
			if !exists || input.receivedAt.After(cutoff) {
				continue
			}
			if chosen == nil || input.receivedAt.Before(chosenInput.receivedAt) {
				chosen, chosenFrame, chosenInput = p, int32(local), input
			}
		}
		if chosen == nil {
			break
		}
		if _, advanced := s.appendInputsLocked(next, []uint64{chosenInput.value}); !advanced {
			break
		}
		if chosenFrame >= chosen.publishedThrough {
			chosen.publishedThrough = chosenFrame + 1
		}
	}
	// Retire redundant copies only after their own receive ACK has passed and
	// their mapping is known. Preserve inputs behind a missing marker or gap.
	for _, p := range s.legacy.publishers {
		for frame := range p.inputs {
			if frame >= p.ackedFrame || len(p.starts) == 0 {
				continue
			}
			round := len(p.starts) - 1
			for round > 0 && frame < p.starts[round].frame {
				round--
			}
			if round == len(p.starts)-1 && len(p.starts) < len(starts) {
				continue
			}
			mapped := int64(starts[round]) + int64(frame) - int64(p.starts[round].frame)
			if mapped < int64(len(s.log.Inputs)) ||
				(round+1 < len(p.starts) && mapped >= int64(starts[round+1])) {
				delete(p.inputs, frame)
			}
		}
	}
	if s.closed && !now.Before(s.closedAt.Add(spectatorLegacyHoldback)) &&
		!now.Before(s.legacy.lastInputAt.Add(spectatorLegacyHoldback)) {
		remaining := 0
		for _, p := range s.legacy.publishers {
			remaining += len(p.inputs)
			clear(p.inputs)
		}
		if remaining > 0 {
			logger.Warn("legacy spectator closed with unmappable or gapped inputs",
				zap.String("battle_code", s.battleCode), zap.Int("buffered_inputs", remaining))
		}
		s.legacy.drained = true
	}
	return len(s.log.Inputs) > before
}
