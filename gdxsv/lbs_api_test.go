package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestStatus_PilotName(t *testing.T) {
	savedMux := http.DefaultServeMux
	http.DefaultServeMux = http.NewServeMux()
	t.Cleanup(func() { http.DefaultServeMux = savedMux })

	lbs := NewLbs()
	peer := &LbsPeer{
		DBUser:    DBUser{UserID: "status-pn-user", Name: "Handle name"},
		PilotName: "パイロット",
	}
	lbs.userPeers[peer.UserID] = peer
	done := make(chan struct{})
	go func() {
		defer close(done)
		lbs.eventLoop()
	}()
	t.Cleanup(func() {
		lbs.Locked(func(lbs *Lbs) { delete(lbs.userPeers, peer.UserID) })
		lbs.Quit()
		<-done
	})
	lbs.RegisterHTTPHandlers()

	check := func(t *testing.T, section string) {
		t.Helper()
		rec := httptest.NewRecorder()
		http.DefaultServeMux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/lbs/status", nil))
		assertEq(t, http.StatusOK, rec.Code)
		var response map[string][]struct {
			UserID    string `json:"user_id"`
			Name      string `json:"name"`
			PilotName string `json:"pilot_name"`
		}
		must(t, json.Unmarshal(rec.Body.Bytes(), &response))
		count := 0
		for _, key := range []string{"lobby_users", "battle_users"} {
			for _, user := range response[key] {
				if user.UserID == peer.UserID {
					count++
					assertEq(t, section, key)
					assertEq(t, peer.Name, user.Name)
					assertEq(t, peer.PilotName, user.PilotName)
				}
			}
		}
		assertEq(t, 1, count)
	}

	t.Run("lobby", func(t *testing.T) { check(t, "lobby_users") })

	const sessionID = "status-pn-session"
	sharedData.ShareMcsUser(&McsUser{
		SessionID:  sessionID,
		UserID:     peer.UserID,
		Name:       peer.Name,
		PilotName:  peer.PilotName,
		BattleCode: "status-pn-battle",
		Pos:        1,
	})
	t.Cleanup(func() {
		sharedData.Lock()
		defer sharedData.Unlock()
		delete(sharedData.mcsUsers, sessionID)
	})
	lbs.Locked(func(lbs *Lbs) {
		peer.logout = true
		peer.Battle = &LbsBattle{
			BattleCode: "status-pn-battle",
			Users:      []*DBUser{&peer.DBUser},
		}
	})
	t.Run("battle_with_lbs_and_mcs_entries", func(t *testing.T) { check(t, "battle_users") })

	lbs.Locked(func(lbs *Lbs) { delete(lbs.userPeers, peer.UserID) })
	t.Run("battle_with_only_mcs_entry", func(t *testing.T) { check(t, "battle_users") })
}
