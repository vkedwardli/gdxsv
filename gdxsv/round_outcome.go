package main

import (
	"strconv"
	"strings"

	"gdxsv/gdxsv/proto"
)

// Reuse BattleLogRound.win_team without changing the protobuf schema.
// Zero stays unknown; 1/2 are winners. -1 is an explicit draw, not a team mask.
const roundOutcomeDraw int32 = -1

func mergeRoundOutcome(current, incoming int32) int32 {
	if incoming == 0 {
		return current
	}
	if current == 0 || incoming == roundOutcomeDraw {
		return incoming
	}
	// Legacy timeout clients each report their opponent as the winner.
	// Reconcile that disagreement as a draw; duplicates cannot undo it.
	// This is a legacy compatibility policy, not proof against desyncs.
	if (current == 1 && incoming == 2) || (current == 2 && incoming == 1) {
		return roundOutcomeDraw
	}
	return current
}

func mergeBattleRoundWin(existing string, rounds []*proto.BattleLogRound) string {
	values := strings.Split(existing, ",")
	if existing == "" {
		values = nil
	}
	if len(values) > maxSpectatorRounds {
		values = values[:maxSpectatorRounds]
	}
	for i, round := range rounds {
		if i >= maxSpectatorRounds {
			break
		}
		if i == len(values) {
			values = append(values, "0")
		}
		previous, _ := strconv.ParseInt(values[i], 10, 32)
		values[i] = strconv.Itoa(int(mergeRoundOutcome(int32(previous), round.GetWinTeam())))
	}
	return strings.Join(values, ",")
}
