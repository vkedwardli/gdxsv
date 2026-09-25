package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func replayFilterTestServer(t *testing.T) (SQLiteDB, *http.ServeMux) {
	t.Helper()
	db := newFreshTestDB(t)
	must(t, db.Init())
	savedDB, savedMux := defaultdb, http.DefaultServeMux
	defaultdb = db
	mux := http.NewServeMux()
	http.DefaultServeMux = mux
	t.Cleanup(func() {
		defaultdb, http.DefaultServeMux = savedDB, savedMux
	})
	NewLbs().RegisterHTTPHandlers()
	return db, mux
}

func insertReplayFilterBattle(t *testing.T, db SQLiteDB, code, disk string, lobby int, created int64, users ...ReplayUser) {
	t.Helper()
	for i, user := range users {
		_, err := db.Exec(`INSERT INTO battle_record
  (battle_code, disk, lobby_id, players, aggregate, user_id, user_name,
   pilot_name, pos, team, created, replay_url, used_ms_mask)
VALUES (?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?)`,
			code, disk, lobby, len(users), user.UserID, user.UserName,
			user.PilotName, i+1, i%2+1, time.Unix(created, 0), code+".pb", 1<<i)
		must(t, err)
	}
}

func requestReplayFilters(t *testing.T, mux *http.ServeMux, query url.Values, status int) []*FoundReplay {
	t.Helper()
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/lbs/replay?"+query.Encode(), nil))
	if rec.Code != status {
		t.Fatalf("GET %s: status %d, want %d; body: %s", query.Encode(), rec.Code, status, rec.Body.String())
	}
	if status != http.StatusOK {
		return nil
	}
	var replays []*FoundReplay
	must(t, json.Unmarshal(rec.Body.Bytes(), &replays))
	return replays
}

func TestReplayPlayerFilters(t *testing.T) {
	db, mux := replayFilterTestServer(t)
	a := ReplayUser{UserID: "AAA111", UserName: "Alice", PilotName: "Ace"}
	b := ReplayUser{UserID: "BBB222", UserName: "Bob", PilotName: "Bravo"}
	c := ReplayUser{UserID: "CCC333", UserName: "Carol", PilotName: "パイロット + A&B, One"}
	d := ReplayUser{UserID: "DDD444", UserName: "Dave", PilotName: "Delta"}
	insertReplayFilterBattle(t, db, "ab", "dc2", 2, 1, a, b)
	insertReplayFilterBattle(t, db, "ac", "dc2", 2, 2, a, c)
	insertReplayFilterBattle(t, db, "bc", "dc1", 4, 3, b, c)
	insertReplayFilterBattle(t, db, "abcd", "dc2", 2, 4, a, b, c, d)
	allUsers := map[string][]string{
		"ab":   {a.UserID, b.UserID},
		"ac":   {a.UserID, c.UserID},
		"bc":   {b.UserID, c.UserID},
		"abcd": {a.UserID, b.UserID, c.UserID, d.UserID},
	}

	for _, tt := range []struct {
		name  string
		query url.Values
		want  []string
	}{
		{"no_filters", nil, []string{"abcd", "bc", "ac", "ab"}},
		{"empty_filters", url.Values{"user_id": {""}, "user_name": {""}, "pilot_name": {""}}, []string{"abcd", "bc", "ac", "ab"}},
		{"legacy_single_id", url.Values{"user_id": {a.UserID}}, []string{"abcd", "ac", "ab"}},
		{"legacy_single_hn_pattern", url.Values{"user_name": {"%lic%"}}, []string{"abcd", "ac", "ab"}},
		{"legacy_single_pn_pattern", url.Values{"pilot_name": {"%Bra%"}}, []string{"abcd", "bc", "ab"}},
		{"two_ids", url.Values{"user_id": {a.UserID, b.UserID}}, []string{"abcd", "ab"}},
		{"three_ids", url.Values{"user_id": {a.UserID, b.UserID, c.UserID}}, []string{"abcd"}},
		{"four_ids", url.Values{"user_id": {a.UserID, b.UserID, c.UserID, d.UserID}}, []string{"abcd"}},
		{"two_handle_names", url.Values{"user_name": {"%Ali%", "%Bo%"}}, []string{"abcd", "ab"}},
		{"two_pilot_names", url.Values{"pilot_name": {"%Ace%", "%Bravo%"}}, []string{"abcd", "ab"}},
		{"id_and_other_players_pn", url.Values{"user_id": {a.UserID}, "pilot_name": {b.PilotName}}, []string{"abcd", "ab"}},
		{"all_fields_different_players", url.Values{"user_id": {a.UserID}, "user_name": {b.UserName}, "pilot_name": {c.PilotName}}, []string{"abcd"}},
		{"all_fields_same_player", url.Values{"user_id": {a.UserID}, "user_name": {a.UserName}, "pilot_name": {a.PilotName}}, []string{"abcd", "ac", "ab"}},
		{"overlapping_name_patterns", url.Values{"user_name": {"A%", "%ice"}}, []string{"abcd", "ac", "ab"}},
		{"url_encoded_pn", url.Values{"pilot_name": {c.PilotName}}, []string{"abcd", "bc", "ac"}},
		{"duplicates_and_empty", url.Values{"user_id": {a.UserID, "", a.UserID, a.UserID, a.UserID, a.UserID}}, []string{"abcd", "ac", "ab"}},
		{"duplicates_at_limit", url.Values{"user_id": {a.UserID, b.UserID, c.UserID, d.UserID, a.UserID}}, []string{"abcd"}},
		{"missing_player", url.Values{"user_id": {a.UserID, "ZZZ999"}}, nil},
		{"id_is_exact", url.Values{"user_id": {"AAA%"}}, nil},
		{"comma_is_not_a_separator", url.Values{"user_id": {a.UserID + "," + b.UserID}}, nil},
		{"sql_is_bound", url.Values{"user_name": {"' OR 1=1 --"}}, nil},
		{"disk", url.Values{"user_id": {b.UserID, c.UserID}, "disk": {"dc1"}}, []string{"bc"}},
		{"lobby", url.Values{"user_id": {b.UserID, c.UserID}, "lobby_id": {"4"}}, []string{"bc"}},
		{"player_count", url.Values{"user_id": {a.UserID, b.UserID}, "players": {"2"}}, []string{"ab"}},
		{"battle_code", url.Values{"user_id": {a.UserID, b.UserID}, "battle_code": {"ab"}}, []string{"ab"}},
		{"aggregate", url.Values{"user_id": {a.UserID, b.UserID}, "aggregate": {"0"}}, nil},
		{"used_ms", url.Values{"user_id": {a.UserID, b.UserID}, "used_ms": {"7"}}, nil},
		{"reverse", url.Values{"user_id": {a.UserID, b.UserID}, "reverse": {"1"}}, []string{"ab", "abcd"}},
		{"empty_page", url.Values{"user_id": {a.UserID, b.UserID}, "page": {"1"}}, nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			status := http.StatusOK
			if len(tt.want) == 0 {
				status = http.StatusNoContent
			}
			replays := requestReplayFilters(t, mux, tt.query, status)
			var got []string
			for _, replay := range replays {
				code := strings.TrimSuffix(replay.ReplayURL, ".pb")
				got = append(got, code)
				var ids []string
				for _, user := range replay.Users {
					ids = append(ids, user.UserID)
				}
				// Matching must not remove the other participants from a replay.
				assertEq(t, allUsers[code], ids)
			}
			assertEq(t, tt.want, got)
		})
	}

	for _, field := range []string{"user_id", "user_name", "pilot_name"} {
		t.Run(field+"_limits", func(t *testing.T) {
			requestReplayFilters(t, mux, url.Values{field: {"1", "2", "3", "4", "5"}}, http.StatusBadRequest)
			requestReplayFilters(t, mux, url.Values{field: {strings.Repeat("x", maxReplayFilterBytes+1)}}, http.StatusBadRequest)
			requestReplayFilters(t, mux, url.Values{field: {strings.Repeat("x", maxReplayFilterBytes)}}, http.StatusNoContent)
		})
	}
	t.Run("malformed_query", func(t *testing.T) {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/lbs/replay?user_name=%", nil))
		assertEq(t, http.StatusBadRequest, rec.Code)
	})
}

func TestReplayPlayerFilterPagination(t *testing.T) {
	db, mux := replayFilterTestServer(t)
	a := ReplayUser{UserID: "AAA111", UserName: "Alice", PilotName: "Ace"}
	b := ReplayUser{UserID: "BBB222", UserName: "Bob", PilotName: "Bravo"}
	c := ReplayUser{UserID: "CCC333", UserName: "Carol", PilotName: "Charlie"}
	for i := 0; i <= 100; i++ {
		insertReplayFilterBattle(t, db, fmt.Sprintf("match-%03d", i), "dc2", 2, int64(i*2), a, b)
		insertReplayFilterBattle(t, db, fmt.Sprintf("other-%03d", i), "dc2", 2, int64(i*2+1), a, c)
	}
	for _, tt := range []struct {
		page, reverse, count, first, last int
	}{
		{0, 0, 100, 100, 1},
		{1, 0, 1, 0, 0},
		{0, 1, 100, 0, 99},
		{1, 1, 1, 100, 100},
	} {
		t.Run(fmt.Sprintf("page_%d_reverse_%d", tt.page, tt.reverse), func(t *testing.T) {
			replays := requestReplayFilters(t, mux, url.Values{
				"user_id": {a.UserID, b.UserID},
				"page":    {fmt.Sprint(tt.page)},
				"reverse": {fmt.Sprint(tt.reverse)},
			}, http.StatusOK)
			assertEq(t, tt.count, len(replays))
			assertEq(t, fmt.Sprintf("match-%03d.pb", tt.first), replays[0].ReplayURL)
			assertEq(t, fmt.Sprintf("match-%03d.pb", tt.last), replays[len(replays)-1].ReplayURL)
			for _, replay := range replays {
				if !strings.HasPrefix(replay.ReplayURL, "match-") {
					t.Fatalf("unexpected replay %s", replay.ReplayURL)
				}
				assertEq(t, 2, len(replay.Users))
			}
		})
	}
}
