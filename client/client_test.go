package client

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func testServer(handler http.HandlerFunc) (*httptest.Server, *Client) {
	srv := httptest.NewServer(handler)
	c := New(srv.URL)
	return srv, c
}

func TestEnqueue(t *testing.T) {
	srv, c := testServer(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/enqueue" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		if r.Method != "POST" {
			t.Fatalf("unexpected method: %s", r.Method)
		}
		var body map[string]interface{}
		json.NewDecoder(r.Body).Decode(&body)
		if body["queue"] != "default" {
			t.Fatalf("unexpected queue: %v", body["queue"])
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"job_id": "job-1",
			"status": "queued",
		})
	})
	defer srv.Close()

	result, err := c.Enqueue("default", map[string]string{"key": "value"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.JobID != "job-1" {
		t.Fatalf("unexpected job_id: %s", result.JobID)
	}
}

func TestFetch(t *testing.T) {
	srv, c := testServer(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"job_id":  "job-2",
			"queue":   "default",
			"payload": map[string]string{"key": "value"},
			"attempt": 1,
		})
	})
	defer srv.Close()

	result, err := c.Fetch(context.Background(), []string{"default"}, "w1", "host", 5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil || result.JobID != "job-2" {
		t.Fatalf("unexpected result: %+v", result)
	}
}

func TestFetchEmpty(t *testing.T) {
	srv, c := testServer(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{})
	})
	defer srv.Close()

	result, err := c.Fetch(context.Background(), []string{"default"}, "w1", "host", 5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != nil {
		t.Fatalf("expected nil, got: %+v", result)
	}
}

func TestAck(t *testing.T) {
	srv, c := testServer(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/ack/job-1" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{}`))
	})
	defer srv.Close()

	err := c.Ack("job-1", AckBody{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestFail(t *testing.T) {
	srv, c := testServer(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status":  "retry",
			"attempt": 2,
		})
	})
	defer srv.Close()

	result, err := c.Fail("job-1", "something broke", "stack trace")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != "retry" {
		t.Fatalf("unexpected status: %s", result.Status)
	}
}

func TestHeartbeat(t *testing.T) {
	srv, c := testServer(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"acked":    []string{"job-1"},
			"unknown":  []string{},
			"canceled": []string{},
		})
	})
	defer srv.Close()

	result, err := c.Heartbeat(map[string]HeartbeatJob{"job-1": {}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Acked) != 1 || result.Acked[0] != "job-1" {
		t.Fatalf("unexpected acked: %v", result.Acked)
	}
}

func TestPayloadTooLargeError(t *testing.T) {
	srv, c := testServer(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusRequestEntityTooLarge)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"code":  "PAYLOAD_TOO_LARGE",
			"error": "payload exceeds 1MB limit",
		})
	})
	defer srv.Close()

	_, err := c.Enqueue("default", map[string]string{"key": "value"})
	if !IsPayloadTooLargeError(err) {
		t.Fatalf("expected PayloadTooLargeError, got: %v", err)
	}
}

func TestWithBearerToken(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		if auth != "Bearer test-token" {
			t.Fatalf("unexpected auth header: %s", auth)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"job_id": "j1", "status": "queued"})
	}))
	defer srv.Close()

	c := NewWithOptions(srv.URL, WithBearerToken("test-token"))
	_, err := c.Enqueue("q", "payload")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestWithAPIKey(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get("X-API-Key")
		if key != "my-key" {
			t.Fatalf("unexpected API key: %s", key)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"job_id": "j1", "status": "queued"})
	}))
	defer srv.Close()

	c := NewWithOptions(srv.URL, WithAPIKey("my-key"))
	_, err := c.Enqueue("q", "payload")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSearch(t *testing.T) {
	srv, c := testServer(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"jobs":     []interface{}{},
			"total":    0,
			"has_more": false,
		})
	})
	defer srv.Close()

	result, err := c.Search(SearchFilter{Queue: "default"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Total != 0 {
		t.Fatalf("unexpected total: %d", result.Total)
	}
}

func TestServerTiming(t *testing.T) {
	srv, c := testServer(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Server-Timing", "proc;dur=12.5")
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"job_id": "j1", "status": "queued"})
	})
	defer srv.Close()

	c.Enqueue("q", "payload")
	if c.ServerDuration.Milliseconds() != 12 {
		t.Fatalf("unexpected server duration: %v", c.ServerDuration)
	}
}
