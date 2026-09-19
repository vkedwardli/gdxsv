package main

import (
	"net"
	"testing"
	"time"

	"gdxsv/gdxsv/proto"
	pb "google.golang.org/protobuf/proto"
)

type spectatorPublisherTest struct {
	t       *testing.T
	session *SpectatorSession
	server  *net.UDPConn
	peers   [2]*net.UDPConn
}

func newSpectatorPublisherTest(t *testing.T) *spectatorPublisherTest {
	t.Helper()
	listen := func() *net.UDPConn {
		conn, err := net.ListenUDP("udp4", testAddr(0))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { conn.Close() })
		return conn
	}
	r := newTestSpectatorRegistry()
	saved := spectatorRegistry
	spectatorRegistry = r
	t.Cleanup(func() { spectatorRegistry = saved })
	s := newTestSpectatorSession()
	r.sessions[s.battleCode] = s
	return &spectatorPublisherTest{t, s, listen(), [2]*net.UDPConn{listen(), listen()}}
}

func (h *spectatorPublisherTest) ack(peer int) *proto.SpectatorInputAck {
	h.t.Helper()
	conn := h.peers[peer]
	if err := conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		h.t.Fatal(err)
	}
	buf := make([]byte, 256)
	n, _, err := conn.ReadFromUDP(buf)
	if err != nil {
		h.t.Fatal(err)
	}
	var packet proto.Packet
	if err := pb.Unmarshal(buf[:n], &packet); err != nil {
		h.t.Fatal(err)
	}
	assertEq(h.t, proto.MessageType_SpectatorInputAckType, packet.Type)
	assertEq(h.t, h.session.battleCode, packet.GetSpectatorInputAckData().GetBattleCode())
	return packet.GetSpectatorInputAckData()
}

func (h *spectatorPublisherTest) inputs(peer int, frame int32, values ...uint64) int32 {
	h.t.Helper()
	handleSpectatorInputPush(h.server, h.peers[peer].LocalAddr().(*net.UDPAddr), &proto.SpectatorInputPush{
		BattleCode: h.session.battleCode, SessionId: h.session.sessionID,
		StartFrame: frame, Inputs: values,
	})
	return h.ack(peer).AckFrame
}

func (h *spectatorPublisherTest) start(peer int, frame int32, seed uint64) {
	h.t.Helper()
	handleSpectatorRoundEvent(h.server, h.peers[peer].LocalAddr().(*net.UDPAddr), &proto.SpectatorRoundEvent{
		BattleCode: h.session.battleCode, SessionId: h.session.sessionID,
		Frame: frame, RandomValue: seed,
	})
	assertEq(h.t, []int32{frame}, h.ack(peer).RoundEventAck)
}

func TestSpectatorUDP_LegacyPublisherCoordinates(t *testing.T) {
	h := newSpectatorPublisherTest(t)
	h.start(0, 0, 111)
	h.start(1, 0, 111)
	// P2's previous round has an extra neutral input. Round 2 is the
	// same sequence at different absolute indexes.
	assertEq(t, int32(5), h.inputs(0, 0, 10, 11, 12, 20, 21))
	assertEq(t, int32(6), h.inputs(1, 0, 10, 11, 12, 0, 20, 21))
	assertEq(t, 0, len(h.session.log.Inputs)) // ACK reception, not publication.
	h.start(0, 3, 222)
	h.start(1, 4, 222)
	h.session.mtx.Lock()
	h.session.flushLegacyLocked(time.Now().Add(spectatorLegacyHoldback))
	h.session.mtx.Unlock()
	assertEq(t, []uint64{10, 11, 12, 20, 21}, h.session.log.Inputs)
	assertEq(t, []int32{0, 3}, h.session.log.StartMsgIndexes)
	assertEq(t, []uint64{111, 222}, h.session.log.StartMsgRandoms)
	// A retransmission gets the publisher's own ACK, not master length.
	assertEq(t, int32(6), h.inputs(1, 4, 20, 21))
}

func TestSpectatorUDP_InvalidInputDoesNotAdmitPublisher(t *testing.T) {
	h := newSpectatorPublisherTest(t)
	assertEq(t, int32(0), h.inputs(0, -1, 99))
	assertEq(t, 0, len(h.session.legacy.publishers))
	h.inputs(1, 0, 10)
	assertEq(t, 1, len(h.session.legacy.publishers))
}

func TestSpectatorUDP_RoundResultsStillUseAllParticipants(t *testing.T) {
	h := newSpectatorPublisherTest(t)
	for peer, outcome := range []int32{1, 2} {
		handleSpectatorRoundResult(h.server, h.peers[peer].LocalAddr().(*net.UDPAddr), &proto.SpectatorRoundResult{
			BattleCode: h.session.battleCode, SessionId: h.session.sessionID,
			RoundIndex: 0, Round: &proto.BattleLogRound{WinTeam: outcome, UsedMs: []int32{1, 2, 3, 4}},
		})
		assertEq(t, []int32{0}, h.ack(peer).RoundResultAck)
	}
	assertEq(t, 0, len(h.session.legacy.publishers))
	h.start(0, 0, 111)
	assertEq(t, int32(-1), h.session.log.RoundData[0].WinTeam)
}
