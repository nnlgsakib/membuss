package metrics

import (
	"io"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMetrics_Noop(t *testing.T) {
	m := Noop()
	if m == nil {
		t.Fatal("Noop returned nil")
	}
	// Calls into the no-op value must not panic.
	m.SetStoredMIDs(0)
	m.SetStoredBytes(0)
	m.SetPeersConnected(0)
	m.IncDHTProvide()
	m.IncMemexBlocksSent(0)
	m.IncMemexBlocksReceived(0)
	m.IncGCRuns()
	m.ObserveMemexTransfer(1.0)
	m.ObserveAddRequest(1.0)
}

func TestMetrics_RecordsAndExposes(t *testing.T) {
	m := New()
	m.SetStoredMIDs(42)
	m.SetStoredBytes(1234)
	m.SetPeersConnected(7)
	m.IncDHTProvide()
	m.IncDHTProvide()
	m.IncMemexBlocksSent(3)
	m.IncMemexBlocksReceived(5)
	m.IncGCRuns()
	m.ObserveMemexTransfer(0.1)
	m.ObserveAddRequest(0.2)

	srv := httptest.NewServer(m.Handler())
	defer srv.Close()
	resp, err := srv.Client().Get(srv.URL)
	if err != nil { t.Fatalf("get: %v", err) }
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	text := string(body)
	for _, want := range []string{
		"membuss_stored_mids_total 42",
		"membuss_stored_bytes_total 1234",
		"membuss_peers_connected 7",
		"membuss_dht_provides_total 2",
		"membuss_memex_blocks_sent_total 3",
		"membuss_memex_blocks_received_total 5",
		"membuss_gc_runs_total 1",
		"membuss_memex_transfer_duration_seconds",
		"membuss_add_request_duration_seconds",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("metric %q missing from /metrics", want)
		}
	}
}
