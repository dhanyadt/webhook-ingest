package httpapi_test

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/convin/webhook-ingest/internal/testutil"
)

func post(t *testing.T, url, body string) *http.Response {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

func TestHealthz(t *testing.T) {
	srv, _ := testutil.NewServer(t)

	resp, err := http.Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d, want 200", resp.StatusCode)
	}
}

func TestWebhookRejectsMalformedJSON(t *testing.T) {
	srv, _ := testutil.NewServer(t)

	resp := post(t, srv.URL+"/webhooks/calls", `{not json`)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("got %d, want 400", resp.StatusCode)
	}
}

func TestWebhookRejectsMissingEventID(t *testing.T) {
	srv, st := testutil.NewServer(t)
	_, callID, accountID := testutil.IDs(t, st)

	body := fmt.Sprintf(
		`{"call_id":%q,"account_id":%q,"status":"completed","duration_sec":10}`,
		callID, accountID)
	resp := post(t, srv.URL+"/webhooks/calls", body)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("got %d, want 400", resp.StatusCode)
	}
}

func TestWebhookRejectsUnknownStatus(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)

	body := fmt.Sprintf(
		`{"event_id":%q,"call_id":%q,"account_id":%q,"status":"exploded","duration_sec":10}`,
		eventID, callID, accountID)
	resp := post(t, srv.URL+"/webhooks/calls", body)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("got %d, want 400", resp.StatusCode)
	}
}

func TestAccountStatsEndpointRespondsJSON(t *testing.T) {
	srv, st := testutil.NewServer(t)
	_, _, accountID := testutil.IDs(t, st)

	resp, err := http.Get(srv.URL + "/accounts/" + accountID + "/stats")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Fatalf("Content-Type is %q, want application/json", ct)
	}
}

func TestConcurrentDuplicateDeliveryIsIgnored(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)

	body := fmt.Sprintf(`{
		"event_id": %q,
		"call_id": %q,
		"account_id": %q,
		"status": "completed",
		"duration_sec": 143
	}`, eventID, callID, accountID)

	const deliveries = 20

	errs := make(chan error, deliveries)

	for i := 0; i < deliveries; i++ {
		go func() {
			resp, err := http.Post(
				srv.URL+"/webhooks/calls",
				"application/json",
				strings.NewReader(body),
			)
			if err != nil {
				errs <- err
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				errs <- fmt.Errorf("got status %d", resp.StatusCode)
				return
			}

			errs <- nil
		}()
	}

	for i := 0; i < deliveries; i++ {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}

	ctx := context.Background()

	var eventCount int
	err := st.Pool().QueryRow(
		ctx,
		`SELECT count(*) FROM events WHERE event_id = $1`,
		eventID,
	).Scan(&eventCount)
	if err != nil {
		t.Fatalf("count events: %v", err)
	}

	if eventCount != 1 {
		t.Fatalf("stored %d copies, want 1", eventCount)
	}

	stats, err := st.AccountStats(ctx, accountID)
	if err != nil {
		t.Fatalf("account stats: %v", err)
	}

	if stats.CallCount != 1 {
		t.Fatalf("got call count %d, want 1", stats.CallCount)
	}

	if stats.TotalDurationSec != 143 {
		t.Fatalf(
			"got total duration %d, want 143",
			stats.TotalDurationSec,
		)
	}

	_ = callID // kept as part of the realistic event payload
}