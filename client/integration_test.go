//go:build integration

package client

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"
)

func corvoURL() string {
	if u := os.Getenv("CORVO_URL"); u != "" {
		return u
	}
	return "http://localhost:8080"
}

func TestIntegrationJobLifecycle(t *testing.T) {
	c := New(corvoURL())

	result, err := c.Enqueue("go-test-lc", map[string]string{"hello": "world"})
	if err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}
	if result.JobID == "" {
		t.Fatal("expected non-empty job_id")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	job, err := c.Fetch(ctx, []string{"go-test-lc"}, "integration-worker", "test-host", 5)
	if err != nil {
		t.Fatalf("fetch failed: %v", err)
	}
	if job == nil {
		t.Fatal("expected a job, got nil")
	}
	if job.JobID != result.JobID {
		t.Fatalf("expected job_id %s, got %s", result.JobID, job.JobID)
	}

	var payload map[string]string
	if err := json.Unmarshal(job.Payload, &payload); err != nil {
		t.Fatalf("failed to decode payload: %v", err)
	}
	if payload["hello"] != "world" {
		t.Fatalf("unexpected payload: %v", payload)
	}

	if err := c.Ack(job.JobID, AckBody{}); err != nil {
		t.Fatalf("ack failed: %v", err)
	}

	got, err := c.GetJob(result.JobID)
	if err != nil {
		t.Fatalf("get_job failed: %v", err)
	}
	if got.State != "completed" {
		t.Fatalf("expected state=completed, got %s", got.State)
	}
	t.Logf("job %s completed", result.JobID)
}

func TestIntegrationEnqueueOptions(t *testing.T) {
	c := New(corvoURL())

	result, err := c.Enqueue("go-opts-test",
		map[string]string{"key": "value"},
		WithPriority("high"),
		WithMaxRetries(5),
		WithTags(map[string]string{"env": "test", "sdk": "go"}),
	)
	if err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	got, err := c.GetJob(result.JobID)
	if err != nil {
		t.Fatalf("get_job failed: %v", err)
	}
	if got.Priority != 1 {
		t.Fatalf("expected priority=1 (high), got %d", got.Priority)
	}
	if got.MaxRetries != 5 {
		t.Fatalf("expected max_retries=5, got %d", got.MaxRetries)
	}
	t.Logf("enqueued with options: priority=%d, max_retries=%d", got.Priority, got.MaxRetries)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	job, _ := c.Fetch(ctx, []string{"go-opts-test"}, "w1", "host", 3)
	if job != nil {
		c.Ack(job.JobID, AckBody{})
	}
}

func TestIntegrationSearch(t *testing.T) {
	c := New(corvoURL())

	c.Enqueue("go-search-test", map[string]string{"n": "1"}, WithTags(map[string]string{"batch": "search-test"}))
	c.Enqueue("go-search-test", map[string]string{"n": "2"}, WithTags(map[string]string{"batch": "search-test"}))

	result, err := c.Search(SearchFilter{
		Queue: "go-search-test",
		State: []string{"pending"},
	})
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}
	if result.Total < 2 {
		t.Fatalf("expected at least 2 jobs, got %d", result.Total)
	}
	t.Logf("found %d jobs in search", result.Total)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for i := 0; i < 2; i++ {
		job, _ := c.Fetch(ctx, []string{"go-search-test"}, "w1", "host", 1)
		if job != nil {
			c.Ack(job.JobID, AckBody{})
		}
	}
}

func TestIntegrationFailAndRetry(t *testing.T) {
	c := New(corvoURL())

	result, err := c.Enqueue("go-fail-test", map[string]string{"x": "1"}, WithMaxRetries(2))
	if err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	job, err := c.Fetch(ctx, []string{"go-fail-test"}, "w1", "host", 5)
	if err != nil || job == nil {
		t.Fatalf("fetch failed: %v", err)
	}

	failResult, err := c.Fail(job.JobID, "test error", "")
	if err != nil {
		t.Fatalf("fail failed: %v", err)
	}
	t.Logf("failed job, status=%s attempt=%d", failResult.Status, failResult.Attempt)

	time.Sleep(6 * time.Second)

	job2, err := c.Fetch(ctx, []string{"go-fail-test"}, "w1", "host", 5)
	if err != nil {
		t.Fatalf("fetch retry failed: %v", err)
	}
	if job2 == nil {
		t.Fatal("expected retry job, got nil")
	}
	if job2.Attempt != 2 {
		t.Fatalf("expected attempt=2, got %d", job2.Attempt)
	}

	c.Ack(job2.JobID, AckBody{})

	got, err := c.GetJob(result.JobID)
	if err != nil {
		t.Fatalf("get_job failed: %v", err)
	}
	if got.State != "completed" {
		t.Fatalf("expected state=completed, got %s", got.State)
	}
	t.Logf("job completed after retry")
}

func TestIntegrationWorker(t *testing.T) {
	c := New(corvoURL())

	result, err := c.Enqueue("go-worker-test2", map[string]string{"task": "process"})
	if err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	processed := make(chan string, 1)
	w := NewWorker(WorkerConfig{
		URL:             corvoURL(),
		Queues:          []string{"go-worker-test2"},
		WorkerID:        "integration-test-worker",
		Concurrency:     1,
		ShutdownTimeout: 5 * time.Second,
	})
	w.Register("go-worker-test2", func(job FetchedJob, ctx *JobContext) error {
		processed <- job.JobID
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	workerDone := make(chan error, 1)
	go func() {
		workerDone <- w.Start(ctx)
	}()

	select {
	case jobID := <-processed:
		if jobID != result.JobID {
			t.Fatalf("expected job_id %s, got %s", result.JobID, jobID)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for job to be processed")
	}

	got, err := c.GetJob(result.JobID)
	if err != nil {
		t.Fatalf("get_job failed: %v", err)
	}
	if got.State != "completed" {
		t.Fatalf("expected state=completed, got %s", got.State)
	}

	cancel()
	select {
	case <-workerDone:
	case <-time.After(10 * time.Second):
		t.Fatal("worker didn't stop")
	}
}
