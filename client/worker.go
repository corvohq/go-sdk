package client

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// FetchedJob is a job received by a worker from the server.
type FetchedJob struct {
	JobID         string          `json:"job_id"`
	Queue         string          `json:"queue"`
	Payload       json.RawMessage `json:"payload"`
	Attempt       int             `json:"attempt"`
	MaxRetries    int             `json:"max_retries"`
	LeaseDuration int             `json:"lease_duration"`
	Checkpoint    json.RawMessage `json:"checkpoint,omitempty"`
	Tags          json.RawMessage `json:"tags,omitempty"`
	Agent         *AgentState     `json:"agent,omitempty"`
}

// JobContext provides helper methods for the job handler.
type JobContext struct {
	Job       FetchedJob
	client    *Client
	cancelled bool
	mu        sync.Mutex
}

// Checkpoint saves a checkpoint for this job via heartbeat.
func (c *JobContext) Checkpoint(data interface{}) error {
	_, err := c.client.heartbeat(c.Job.JobID, nil, data)
	return err
}

// Progress reports progress for this job via heartbeat.
func (c *JobContext) Progress(current, total int, message string) error {
	progress := map[string]interface{}{
		"current": current,
		"total":   total,
		"message": message,
	}
	_, err := c.client.heartbeat(c.Job.JobID, progress, nil)
	return err
}

// Cancelled returns whether this job has been cancelled.
func (c *JobContext) Cancelled() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cancelled
}

func (c *JobContext) setCancelled() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cancelled = true
}

// Handler processes a job. Return nil for success, error for failure.
type Handler func(job FetchedJob, ctx *JobContext) error

// Worker manages concurrent fetch loops and heartbeating.
type Worker struct {
	client          *Client
	queues          []string
	workerID        string
	hostname        string
	concurrency     int
	shutdownTimeout time.Duration
	handlers        map[string]Handler
	activeJobs      map[string]*JobContext
	mu              sync.Mutex
	draining        bool
	stopFetch       chan struct{}  // closed to stop fetch loops
	handlerWg       sync.WaitGroup // tracks in-flight handler goroutines
}

// WorkerConfig configures a worker.
type WorkerConfig struct {
	URL             string
	Queues          []string
	WorkerID        string
	Concurrency     int
	ShutdownTimeout time.Duration // default 30s
	ClientOptions   []ClientOption
}

// NewWorker creates a new Worker.
func NewWorker(cfg WorkerConfig) *Worker {
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 10
	}
	if cfg.ShutdownTimeout <= 0 {
		cfg.ShutdownTimeout = 30 * time.Second
	}
	hostname, _ := os.Hostname()
	return &Worker{
		client:          NewWithOptions(cfg.URL, cfg.ClientOptions...),
		queues:          cfg.Queues,
		workerID:        cfg.WorkerID,
		hostname:        hostname,
		concurrency:     cfg.Concurrency,
		shutdownTimeout: cfg.ShutdownTimeout,
		handlers:        make(map[string]Handler),
		activeJobs:      make(map[string]*JobContext),
		stopFetch:       make(chan struct{}),
	}
}

// Register registers a handler for a queue.
func (w *Worker) Register(queue string, handler Handler) {
	w.handlers[queue] = handler
}

// Start runs the worker. It blocks until the context is cancelled or
// SIGTERM/SIGINT is received, then drains in-flight jobs gracefully.
func (w *Worker) Start(ctx context.Context) error {
	// fetchCtx cancellation interrupts in-flight long-poll HTTP requests.
	fetchCtx, fetchCancel := context.WithCancel(ctx)
	defer fetchCancel()

	// heartbeatCtx stays alive during drain so leases don't expire.
	// It uses context.Background() so it's independent of the parent ctx.
	heartbeatCtx, heartbeatCancel := context.WithCancel(context.Background())
	defer heartbeatCancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	defer signal.Stop(sigCh)

	// Start heartbeat loop (stays alive until we explicitly stop it after drain).
	heartbeatDone := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		w.heartbeatLoop(heartbeatCtx)
	}()

	// Start fetch loops.
	for range w.concurrency {
		go w.fetchLoop(fetchCtx)
	}

	slog.Info("worker started", "queues", w.queues, "concurrency", w.concurrency)

	// Wait for shutdown signal.
	select {
	case <-sigCh:
		slog.Info("worker received shutdown signal")
	case <-ctx.Done():
		slog.Info("worker context cancelled")
	}

	// Phase 1: Enter drain mode.
	w.mu.Lock()
	w.draining = true
	activeCount := len(w.activeJobs)
	for _, jc := range w.activeJobs {
		jc.setCancelled()
	}
	w.mu.Unlock()

	close(w.stopFetch) // fetch loops stop starting new fetches
	fetchCancel()      // cancel in-flight long-poll requests

	slog.Info("draining active jobs", "count", activeCount, "timeout", w.shutdownTimeout)

	// Phase 2: Wait for in-flight handlers to finish.
	done := make(chan struct{})
	go func() {
		w.handlerWg.Wait()
		close(done)
	}()

	select {
	case <-done:
		slog.Info("all handlers finished")
	case <-time.After(w.shutdownTimeout):
		slog.Warn("shutdown timeout reached, failing remaining jobs")
		w.failActiveJobs()
	}

	// Stop heartbeat loop.
	heartbeatCancel()
	<-heartbeatDone

	slog.Info("worker stopped")
	return nil
}

func (w *Worker) fetchLoop(fetchCtx context.Context) {
	for {
		select {
		case <-w.stopFetch:
			return
		default:
		}

		job, err := w.fetchJob(fetchCtx)
		if err != nil {
			slog.Debug("fetch error", "error", err)
			select {
			case <-w.stopFetch:
				return
			case <-time.After(1 * time.Second):
			}
			continue
		}
		if job == nil {
			continue
		}

		handler, ok := w.handlers[job.Queue]
		if !ok {
			slog.Warn("no handler for queue", "queue", job.Queue)
			if err := w.client.ack(job.JobID, nil); err != nil {
				slog.Error("failed to ack job", "job_id", job.JobID, "error", err)
			}
			continue
		}

		jc := &JobContext{Job: *job, client: w.client}
		w.mu.Lock()
		w.activeJobs[job.JobID] = jc
		w.mu.Unlock()

		w.handlerWg.Add(1)
		err = handler(*job, jc)
		w.handlerWg.Done()

		w.mu.Lock()
		delete(w.activeJobs, job.JobID)
		w.mu.Unlock()

		if err != nil {
			if ferr := w.client.fail(job.JobID, err.Error(), ""); ferr != nil {
				slog.Error("failed to fail job", "job_id", job.JobID, "error", ferr)
			}
		} else {
			if aerr := w.client.ack(job.JobID, nil); aerr != nil {
				slog.Error("failed to ack job", "job_id", job.JobID, "error", aerr)
			}
		}
	}
}

func (w *Worker) fetchJob(ctx context.Context) (*FetchedJob, error) {
	body := map[string]interface{}{
		"queues":    w.queues,
		"worker_id": w.workerID,
		"hostname":  w.hostname,
		"timeout":   30,
	}

	var result FetchedJob
	err := w.client.postWithContext(ctx, "/api/v1/fetch", body, &result)
	if err != nil {
		return nil, err
	}
	if result.JobID == "" {
		return nil, nil
	}
	return &result, nil
}

func (w *Worker) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.sendHeartbeat()
		}
	}
}

func (w *Worker) sendHeartbeat() {
	w.mu.Lock()
	if len(w.activeJobs) == 0 {
		w.mu.Unlock()
		return
	}

	jobs := make(map[string]interface{})
	for id := range w.activeJobs {
		jobs[id] = map[string]interface{}{}
	}
	w.mu.Unlock()

	body := map[string]interface{}{"jobs": jobs}
	var result struct {
		Jobs map[string]struct {
			Status string `json:"status"`
		} `json:"jobs"`
	}

	if err := w.client.post("/api/v1/heartbeat", body, &result); err != nil {
		slog.Debug("heartbeat error", "error", err)
		return
	}

	// Process cancel signals from server.
	for jobID, resp := range result.Jobs {
		if resp.Status == "cancel" {
			w.mu.Lock()
			if jc, ok := w.activeJobs[jobID]; ok {
				jc.setCancelled()
			}
			w.mu.Unlock()
		}
	}
}

// failActiveJobs sends a final heartbeat (to preserve checkpoint data) and
// then fails each remaining in-flight job with "worker_shutdown".
func (w *Worker) failActiveJobs() {
	w.mu.Lock()
	remaining := make(map[string]*JobContext, len(w.activeJobs))
	for id, jc := range w.activeJobs {
		remaining[id] = jc
	}
	w.mu.Unlock()

	for jobID := range remaining {
		// Best-effort: heartbeat to preserve checkpoint, then fail.
		if _, err := w.client.heartbeat(jobID, nil, nil); err != nil {
			slog.Error("failed to heartbeat job", "job_id", jobID, "error", err)
		}
		if err := w.client.fail(jobID, "worker_shutdown", ""); err != nil {
			slog.Error("failed to fail job", "job_id", jobID, "error", err)
		}
	}
}

// Internal client helpers for worker use

func (c *Client) ack(jobID string, result interface{}) error {
	body := map[string]interface{}{}
	if result != nil {
		body["result"] = result
	}
	return c.post("/api/v1/ack/"+jobID, body, nil)
}

func (c *Client) fail(jobID string, errMsg string, backtrace string) error {
	body := map[string]interface{}{
		"error":     errMsg,
		"backtrace": backtrace,
	}
	return c.post("/api/v1/fail/"+jobID, body, nil)
}

func (c *Client) heartbeat(jobID string, progress, checkpoint interface{}) (string, error) {
	job := map[string]interface{}{}
	if progress != nil {
		job["progress"] = progress
	}
	if checkpoint != nil {
		job["checkpoint"] = checkpoint
	}

	body := map[string]interface{}{
		"jobs": map[string]interface{}{
			jobID: job,
		},
	}

	var result struct {
		Jobs map[string]struct {
			Status string `json:"status"`
		} `json:"jobs"`
	}

	if err := c.post("/api/v1/heartbeat", body, &result); err != nil {
		return "", err
	}

	if r, ok := result.Jobs[jobID]; ok {
		return r.Status, nil
	}
	return "ok", nil
}
