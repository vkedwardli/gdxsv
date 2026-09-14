package main

import (
	"testing"

	"gdxsv/gdxsv/proto"
	pb "google.golang.org/protobuf/proto"
)

func TestRoundOutcomeReconciliation(t *testing.T) {
	for _, outcomes := range [][]int32{
		{1, 2}, {2, 1}, {-1, 1}, {1, -1}, {-1, 2}, {2, -1},
		{1, 1, 2, 1, 2}, {-1, 1, 2, -1}, {2, 0, 1},
	} {
		var result int32
		for _, outcome := range outcomes {
			result = mergeRoundOutcome(result, outcome)
		}
		assertEq(t, roundOutcomeDraw, result)
	}
	assertEq(t, int32(0), mergeRoundOutcome(0, 0))
	assertEq(t, int32(1), mergeRoundOutcome(1, 1))
	assertEq(t, int32(2), mergeRoundOutcome(2, 0))
	assertEq(t, "-1,2,-1", mergeBattleRoundWin("1,2,0", []*proto.BattleLogRound{
		{WinTeam: 2}, {WinTeam: 2}, {WinTeam: -1},
	}))
	assertEq(t, "-1,2,-1", mergeBattleRoundWin("-1,2,-1", []*proto.BattleLogRound{
		{WinTeam: 1},
	}))
	assertEq(t, "0,-1", mergeBattleRoundWin("", []*proto.BattleLogRound{nil, {WinTeam: -1}}))
}

func TestSpectatorSession_DrawCorrectionResendsUntilAcked(t *testing.T) {
	for _, initial := range []int32{1, 2} {
		s := newTestSpectatorSession()
		s.PushRoundEvent(0, 123)
		input := &proto.BattleLogRound{WinTeam: initial, UsedMs: []int32{1, 6}}
		assertEq(t, true, s.PushRoundResult(0, input))
		input.UsedMs[0] = 99
		assertEq(t, int32(1), s.log.RoundData[0].UsedMs[0])
		previous := s.log.RoundData[0]
		version := s.roundStateVersion
		addr := testAddr(40000)
		subscribeTestSpectator(s, addr, 0)
		sub := s.downlinks[addr.String()]
		sub.sentHeader = true
		sub.ackedRoundStateVersion = version
		assertEq(t, true, s.PushRoundResult(0, &proto.BattleLogRound{WinTeam: 3 - initial}))
		assertEq(t, initial, previous.WinTeam) // Previously built pushes remain immutable.
		assertEq(t, version+1, s.roundStateVersion)
		push, ok := s.buildPush(sub)
		assertEq(t, true, ok)
		assertEq(t, roundOutcomeDraw, push.RoundData[0].WinTeam)
		retry, ok := s.buildPush(sub)
		assertEq(t, true, ok)
		assertEq(t, true, pb.Equal(push, retry))
		s.Ack(addr.String(), 0, 0, push.RoundStateVersion)
		_, ok = s.buildPush(sub)
		assertEq(t, false, ok)
		s.Close("game_end", -1)
		assertEq(t, true, s.PushRoundResult(0, &proto.BattleLogRound{WinTeam: initial}))
		assertEq(t, roundOutcomeDraw, s.log.RoundData[0].WinTeam)
		assertEq(t, false, s.PushRoundResult(9, &proto.BattleLogRound{WinTeam: -1}))
	}
}

func TestSpectatorSession_LateClosedRoundCanReconcile(t *testing.T) {
	s := newTestSpectatorSession()
	s.PushRoundEvent(0, 123)
	s.PushRoundResult(0, &proto.BattleLogRound{WinTeam: 1})
	s.Close("game_end", -1)
	version := s.roundStateVersion
	assertEq(t, true, s.PushRoundResult(0, &proto.BattleLogRound{WinTeam: 2}))
	assertEq(t, version+1, s.roundStateVersion)
	assertEq(t, roundOutcomeDraw, s.log.RoundData[0].WinTeam)
}

func TestSpectatorSession_DrawMetadataFitsInputDatagram(t *testing.T) {
	s := newTestSpectatorSession()
	s.PushInputs(0, make([]uint64, maxSpectatorPushFrames))
	for i := 0; i < maxSpectatorRounds; i++ {
		s.PushRoundEvent(int32(i*10000), 65535)
		s.PushRoundResult(int32(i), &proto.BattleLogRound{
			WinTeam: roundOutcomeDraw,
			UsedMs:  []int32{41, 41, 41, 41},
		})
	}
	push, ok := s.buildPush(&downlinkSubscriber{sentHeader: true})
	assertEq(t, true, ok)
	assertEq(t, maxSpectatorRounds, len(push.RoundData))
	packet := &proto.Packet{
		Type:                   proto.MessageType_SpectatorInputPushType,
		SpectatorInputPushData: push,
	}
	if size := pb.Size(packet); size > 1400 {
		t.Fatalf("draw metadata plus 128 inputs exceeds UDP budget: %d bytes", size)
	}
}
