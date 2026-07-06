package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"reservoir/internal/store"
)

// TestCRDTStatsEndpoint verifies the /api/crdt endpoint reports the store's CRDT
// gauges (and omits cluster sections when standalone).
func TestCRDTStatsEndpoint(t *testing.T) {
	st := store.NewStore(&store.Limits{MaxKeySize: 1024, MaxValueSize: 1 << 20, MaxMemoryUsage: 1 << 30})
	st.SetLocalOrigin(1)
	defer st.Stop()

	st.SAddLWW("s", 100, 0, 1, "a", "b")
	st.SRemLWW("s", 200, 0, 1, "b")
	st.IncrBy("c", 5)

	srv := StartUIServer("127.0.0.1:0", st, nil)
	defer srv.Close()

	// Exercise the handler directly via httptest against the same mux logic:
	// build a request and run it through a recorder using the server handler.
	req := httptest.NewRequest(http.MethodGet, "/api/crdt", nil)
	rec := httptest.NewRecorder()
	srv.Handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}

	var resp struct {
		Store       store.CRDTStats        `json:"store"`
		AntiEntropy map[string]interface{} `json:"anti_entropy"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v (body=%s)", err, rec.Body.String())
	}
	if resp.Store.CRDTSets != 1 || resp.Store.SetElements != 1 || resp.Store.SetTombstones != 1 {
		t.Errorf("set gauges = {%d %d %d}, want {1 1 1}", resp.Store.CRDTSets, resp.Store.SetElements, resp.Store.SetTombstones)
	}
	if resp.Store.Counters != 1 {
		t.Errorf("Counters = %d, want 1", resp.Store.Counters)
	}
	// Standalone: no cluster manager, so anti_entropy is omitted.
	if resp.AntiEntropy != nil {
		t.Errorf("anti_entropy present without a cluster manager: %v", resp.AntiEntropy)
	}
}
