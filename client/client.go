package client

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// PayloadTooLargeError is returned when a job payload exceeds the server's configured limit.
// This error is not retryable; the payload must be reduced before re-enqueuing.
type PayloadTooLargeError struct {
	Message string
}

func (e *PayloadTooLargeError) Error() string { return e.Message }

// IsPayloadTooLargeError reports whether err is a PayloadTooLargeError.
func IsPayloadTooLargeError(err error) bool {
	var e *PayloadTooLargeError
	return errors.As(err, &e)
}

// Client is a thin HTTP wrapper for the Corvo API.
type Client struct {
	URL            string
	HTTPClient     *http.Client
	ServerDuration time.Duration // last server-side processing duration from Server-Timing header
	auth           authConfig
}

type TokenProvider func(ctx context.Context) (string, error)

type ClientOption func(*Client)

type authConfig struct {
	headers      map[string]string
	bearerToken  string
	apiKey       string
	apiKeyHeader string
	tokenSource  TokenProvider
}

// WithHeader adds a static header to every request.
func WithHeader(key, value string) ClientOption {
	return func(c *Client) {
		if c.auth.headers == nil {
			c.auth.headers = map[string]string{}
		}
		c.auth.headers[key] = value
	}
}

// WithBearerToken adds Authorization: Bearer <token> to every request.
func WithBearerToken(token string) ClientOption {
	return func(c *Client) { c.auth.bearerToken = token }
}

// WithTokenProvider adds dynamic bearer token lookup per request.
func WithTokenProvider(provider TokenProvider) ClientOption {
	return func(c *Client) { c.auth.tokenSource = provider }
}

// WithAPIKey adds an API key header to every request. Header defaults to X-API-Key.
func WithAPIKey(key string) ClientOption {
	return WithAPIKeyHeader("X-API-Key", key)
}

// WithAPIKeyHeader adds an API key value using a custom header.
func WithAPIKeyHeader(header, key string) ClientOption {
	return func(c *Client) {
		c.auth.apiKeyHeader = header
		c.auth.apiKey = key
	}
}

// New creates a new Corvo client.
func New(url string) *Client {
	return NewWithOptions(url)
}

// NewWithOptions creates a new Corvo client with optional auth/header behavior.
func NewWithOptions(url string, opts ...ClientOption) *Client {
	c := &Client{
		URL: strings.TrimRight(url, "/"),
		HTTPClient: &http.Client{
			Timeout: 60 * time.Second,
		},
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// EnqueueOption configures an enqueue request.
type EnqueueOption func(map[string]interface{})

type AgentConfig struct {
	MaxIterations    int     `json:"max_iterations,omitempty"`
	MaxCostUSD       float64 `json:"max_cost_usd,omitempty"`
	IterationTimeout string  `json:"iteration_timeout,omitempty"`
}

type AgentState struct {
	MaxIterations    int     `json:"max_iterations,omitempty"`
	MaxCostUSD       float64 `json:"max_cost_usd,omitempty"`
	IterationTimeout string  `json:"iteration_timeout,omitempty"`
	Iteration        int     `json:"iteration,omitempty"`
	TotalCostUSD     float64 `json:"total_cost_usd,omitempty"`
}

func WithPriority(p string) EnqueueOption {
	return func(m map[string]interface{}) { m["priority"] = p }
}

func WithUniqueKey(key string, period int) EnqueueOption {
	return func(m map[string]interface{}) {
		m["unique_key"] = key
		if period > 0 {
			m["unique_period"] = period
		}
	}
}

func WithMaxRetries(n int) EnqueueOption {
	return func(m map[string]interface{}) { m["max_retries"] = n }
}

func WithScheduledAt(t time.Time) EnqueueOption {
	return func(m map[string]interface{}) { m["scheduled_at"] = t.Format(time.RFC3339) }
}

func WithTags(tags map[string]string) EnqueueOption {
	return func(m map[string]interface{}) { m["tags"] = tags }
}

func WithExpireAfter(d time.Duration) EnqueueOption {
	return func(m map[string]interface{}) { m["expire_after"] = d.String() }
}

func WithRetryBackoff(strategy, baseDelay, maxDelay string) EnqueueOption {
	return func(m map[string]interface{}) {
		m["retry_backoff"] = strategy
		m["retry_base_delay"] = baseDelay
		m["retry_max_delay"] = maxDelay
	}
}

func WithAgent(cfg AgentConfig) EnqueueOption {
	return func(m map[string]interface{}) { m["agent"] = cfg }
}

// ChainConfig defines a job chain for sequential execution.
type ChainConfig struct {
	Steps     []ChainStep `json:"steps"`
	OnFailure string      `json:"on_failure,omitempty"`
	OnExit    *ChainStep  `json:"on_exit,omitempty"`
}

// ChainStep is a single step in a chain.
type ChainStep struct {
	Queue   string      `json:"queue"`
	Payload interface{} `json:"payload"`
}

func WithChain(steps []ChainStep, onFailure string, onExit *ChainStep) EnqueueOption {
	return func(m map[string]interface{}) {
		chain := ChainConfig{Steps: steps, OnFailure: onFailure, OnExit: onExit}
		m["chain"] = chain
	}
}

// EnqueueResult is the response from enqueuing a job.
type EnqueueResult struct {
	JobID          string `json:"job_id"`
	Status         string `json:"status"`
	UniqueExisting bool   `json:"unique_existing"`
}

// Enqueue enqueues a job.
func (c *Client) Enqueue(queue string, payload interface{}, opts ...EnqueueOption) (*EnqueueResult, error) {
	body := map[string]interface{}{
		"queue":   queue,
		"payload": payload,
	}
	for _, opt := range opts {
		opt(body)
	}

	var result EnqueueResult
	if err := c.post("/api/v1/enqueue", body, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// BatchRequest is the request for batch enqueue.
type BatchRequest struct {
	Jobs  []BatchJob   `json:"jobs"`
	Batch *BatchConfig `json:"batch,omitempty"`
}

// BatchJob is a single job in a batch.
type BatchJob struct {
	Queue   string      `json:"queue"`
	Payload interface{} `json:"payload"`
}

// BatchConfig configures batch completion callback.
type BatchConfig struct {
	CallbackQueue   string      `json:"callback_queue"`
	CallbackPayload interface{} `json:"callback_payload,omitempty"`
}

// BatchResult is the response from batch enqueue.
type BatchResult struct {
	JobIDs  []string `json:"job_ids"`
	BatchID string   `json:"batch_id"`
}

// EnqueueBatch enqueues multiple jobs.
func (c *Client) EnqueueBatch(req BatchRequest) (*BatchResult, error) {
	var result BatchResult
	if err := c.post("/api/v1/enqueue/batch", req, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// Job is a job returned from the API.
type Job struct {
	ID          string          `json:"id"`
	Queue       string          `json:"queue"`
	State       string          `json:"state"`
	Payload     json.RawMessage `json:"payload"`
	Priority    int             `json:"priority"`
	Attempt     int             `json:"attempt"`
	MaxRetries  int             `json:"max_retries"`
	Tags        json.RawMessage `json:"tags,omitempty"`
	Checkpoint  json.RawMessage `json:"checkpoint,omitempty"`
	Agent       *AgentState     `json:"agent,omitempty"`
	HoldReason  *string         `json:"hold_reason,omitempty"`
	CreatedAt   string          `json:"created_at"`
	StartedAt   *string         `json:"started_at,omitempty"`
	CompletedAt *string         `json:"completed_at,omitempty"`
}

// GetJob returns a job by ID.
func (c *Client) GetJob(id string) (*Job, error) {
	var job Job
	if err := c.get("/api/v1/jobs/"+id, &job); err != nil {
		return nil, err
	}
	return &job, nil
}

// SearchFilter matches the server search filter.
type SearchFilter struct {
	Queue           string            `json:"queue,omitempty"`
	State           []string          `json:"state,omitempty"`
	Priority        string            `json:"priority,omitempty"`
	Tags            map[string]string `json:"tags,omitempty"`
	PayloadContains string            `json:"payload_contains,omitempty"`
	ErrorContains   string            `json:"error_contains,omitempty"`
	Sort            string            `json:"sort,omitempty"`
	Order           string            `json:"order,omitempty"`
	Limit           int               `json:"limit,omitempty"`
	Cursor          string            `json:"cursor,omitempty"`
}

// SearchResult is the response from a search.
type SearchResult struct {
	Jobs       []json.RawMessage `json:"jobs"`
	Total      int               `json:"total"`
	Cursor     string            `json:"cursor,omitempty"`
	HasMore    bool              `json:"has_more"`
	DurationMs int64             `json:"duration_ms"`
}

// Search searches for jobs.
func (c *Client) Search(filter SearchFilter) (*SearchResult, error) {
	var result SearchResult
	if err := c.post("/api/v1/jobs/search", filter, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// Queue management

func (c *Client) ListQueues() (json.RawMessage, error) {
	var result json.RawMessage
	if err := c.get("/api/v1/queues", &result); err != nil {
		return nil, err
	}
	return result, nil
}

func (c *Client) PauseQueue(name string) error {
	return c.post("/api/v1/queues/"+name+"/pause", nil, nil)
}

func (c *Client) ResumeQueue(name string) error {
	return c.post("/api/v1/queues/"+name+"/resume", nil, nil)
}

func (c *Client) RetryJob(id string) error {
	return c.post("/api/v1/jobs/"+id+"/retry", nil, nil)
}

func (c *Client) CancelJob(id string) error {
	return c.post("/api/v1/jobs/"+id+"/cancel", nil, nil)
}

func (c *Client) DeleteJob(id string) error {
	return c.doRequest("DELETE", "/api/v1/jobs/"+id, nil, nil)
}

// MoveJob moves a job to a different queue.
func (c *Client) MoveJob(id, targetQueue string) error {
	return c.post("/api/v1/jobs/"+id+"/move", map[string]string{"queue": targetQueue}, nil)
}

// FetchResult is a job returned by long-poll fetch.
type FetchResult struct {
	JobID   string          `json:"job_id"`
	Queue   string          `json:"queue"`
	Payload json.RawMessage `json:"payload"`
	Attempt int             `json:"attempt"`
}

// Fetch long-polls for a job from the given queues.
func (c *Client) Fetch(ctx context.Context, queues []string, workerID, hostname string, timeout int) (*FetchResult, error) {
	body := map[string]interface{}{
		"queues":    queues,
		"worker_id": workerID,
		"hostname":  hostname,
		"timeout":   timeout,
	}
	var result FetchResult
	if err := c.postWithContext(ctx, "/api/v1/fetch", body, &result); err != nil {
		return nil, err
	}
	if result.JobID == "" {
		return nil, nil
	}
	return &result, nil
}

// AckBody is the request body for acknowledging a job.
type AckBody struct {
	Result     interface{} `json:"result,omitempty"`
	StepStatus string      `json:"step_status,omitempty"`
}

// Ack acknowledges a job as complete.
func (c *Client) Ack(jobID string, body AckBody) error {
	return c.post("/api/v1/ack/"+jobID, body, nil)
}

// FailResult is the response from failing a job.
type FailResult struct {
	Status  string `json:"status"`
	Attempt int    `json:"attempt"`
}

// Fail marks a job as failed.
func (c *Client) Fail(jobID, errMsg, backtrace string) (*FailResult, error) {
	body := map[string]string{"error": errMsg, "backtrace": backtrace}
	var result FailResult
	if err := c.post("/api/v1/fail/"+jobID, body, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// HeartbeatJob is a single job entry in a heartbeat request.
type HeartbeatJob struct {
	Progress   *int        `json:"progress,omitempty"`
	Checkpoint interface{} `json:"checkpoint,omitempty"`
}

// HeartbeatResult is the response from a heartbeat.
type HeartbeatResult struct {
	Acked    []string `json:"acked"`
	Unknown  []string `json:"unknown"`
	Canceled []string `json:"canceled"`
}

// Heartbeat sends a batched heartbeat for active jobs.
func (c *Client) Heartbeat(jobs map[string]HeartbeatJob) (*HeartbeatResult, error) {
	body := map[string]interface{}{"jobs": jobs}
	var result HeartbeatResult
	if err := c.post("/api/v1/heartbeat", body, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// BulkRequest is the request for a bulk operation.
type BulkRequest struct {
	JobIDs      []string      `json:"job_ids,omitempty"`
	Filter      *SearchFilter `json:"filter,omitempty"`
	Action      string        `json:"action"`
	MoveToQueue string        `json:"move_to_queue,omitempty"`
	Priority    string        `json:"priority,omitempty"`
	Async       bool          `json:"async,omitempty"`
}

// BulkResult is the response from a synchronous bulk operation.
type BulkResult struct {
	Affected   int    `json:"affected"`
	Errors     int    `json:"errors"`
	DurationMs int64  `json:"duration_ms"`
	BulkOpID   string `json:"bulk_operation_id,omitempty"`
	Status     string `json:"status,omitempty"`
}

// Bulk performs a bulk operation on jobs.
func (c *Client) Bulk(req BulkRequest) (*BulkResult, error) {
	var result BulkResult
	if err := c.post("/api/v1/jobs/bulk", req, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// BulkTask is the status of an async bulk operation.
type BulkTask struct {
	ID         string `json:"id"`
	Status     string `json:"status"`
	Action     string `json:"action"`
	Total      int    `json:"total"`
	Processed  int    `json:"processed"`
	Affected   int    `json:"affected"`
	Errors     int    `json:"errors"`
	Error      string `json:"error,omitempty"`
	CreatedAt  string `json:"created_at"`
	UpdatedAt  string `json:"updated_at"`
	FinishedAt string `json:"finished_at,omitempty"`
}

// BulkStatus checks the status of an async bulk operation.
func (c *Client) BulkStatus(id string) (*BulkTask, error) {
	var result BulkTask
	if err := c.get("/api/v1/bulk/"+id, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// ServerInfo contains version information about the Corvo server.
type ServerInfo struct {
	ServerVersion string `json:"server_version"`
	APIVersion    string `json:"api_version"`
}

// GetServerInfo returns version information about the Corvo server.
func (c *Client) GetServerInfo() (*ServerInfo, error) {
	var info ServerInfo
	if err := c.get("/api/v1/info", &info); err != nil {
		return nil, err
	}
	return &info, nil
}

// BulkGetJobs returns jobs for the given IDs. Missing jobs are skipped.
func (c *Client) BulkGetJobs(ids []string) ([]*Job, error) {
	var result struct {
		Jobs []*Job `json:"jobs"`
	}
	if err := c.post("/api/v1/jobs/bulk-get", map[string]interface{}{"job_ids": ids}, &result); err != nil {
		return nil, err
	}
	return result.Jobs, nil
}

// Event is a lifecycle event from the SSE stream.
type Event struct {
	Type     string          `json:"type"`
	Seq      uint64          `json:"seq"`
	JobID    string          `json:"job_id,omitempty"`
	Queue    string          `json:"queue,omitempty"`
	AtNs     uint64          `json:"at_ns"`
	Data     json.RawMessage `json:"data,omitempty"`
}

// SubscribeOptions configures the SSE event stream.
type SubscribeOptions struct {
	Queues      []string
	JobIDs      []string
	Types       []string
	LastEventID uint64
}

// Subscribe opens an SSE event stream and sends events to the returned channel.
// The channel is closed when the context is canceled or the stream ends.
func (c *Client) Subscribe(ctx context.Context, opts SubscribeOptions) (<-chan Event, error) {
	params := make([]string, 0)
	if len(opts.Queues) > 0 {
		params = append(params, "queues="+strings.Join(opts.Queues, ","))
	}
	if len(opts.JobIDs) > 0 {
		params = append(params, "job_ids="+strings.Join(opts.JobIDs, ","))
	}
	if len(opts.Types) > 0 {
		params = append(params, "types="+strings.Join(opts.Types, ","))
	}
	if opts.LastEventID > 0 {
		params = append(params, "last_event_id="+strconv.FormatUint(opts.LastEventID, 10))
	}

	path := "/api/v1/events"
	if len(params) > 0 {
		path += "?" + strings.Join(params, "&")
	}

	req, err := http.NewRequestWithContext(ctx, "GET", c.URL+path, nil)
	if err != nil {
		return nil, err
	}
	if err := c.applyAuthHeaders(ctx, req); err != nil {
		return nil, err
	}

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("subscribe request failed: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		_ = resp.Body.Close()
		return nil, fmt.Errorf("subscribe: HTTP %d", resp.StatusCode)
	}

	ch := make(chan Event, 64)
	go func() {
		defer close(ch)
		defer func() { _ = resp.Body.Close() }()

		scanner := bufio.NewScanner(resp.Body)
		var eventType, eventID string
		var dataLines []string

		for scanner.Scan() {
			line := scanner.Text()
			switch {
			case strings.HasPrefix(line, "event: "):
				eventType = line[7:]
			case strings.HasPrefix(line, "id: "):
				eventID = line[4:]
			case strings.HasPrefix(line, "data: "):
				dataLines = append(dataLines, line[6:])
			case line == "":
				if len(dataLines) > 0 {
					raw := strings.Join(dataLines, "\n")
					var ev Event
					if err := json.Unmarshal([]byte(raw), &ev); err == nil {
						ev.Type = eventType
						select {
						case ch <- ev:
						case <-ctx.Done():
							return
						}
					}
				}
				eventType = ""
				eventID = ""
				_ = eventID
				dataLines = dataLines[:0]
			}
		}
	}()

	return ch, nil
}

// HTTP helpers

func (c *Client) get(path string, result interface{}) error {
	return c.doRequest("GET", path, nil, result)
}

func (c *Client) post(path string, body interface{}, result interface{}) error {
	return c.doRequest("POST", path, body, result)
}

func (c *Client) postWithContext(ctx context.Context, path string, body interface{}, result interface{}) error {
	return c.doRequestWithContext(ctx, "POST", path, body, result)
}

func (c *Client) doRequest(method, path string, body interface{}, result interface{}) error {
	return c.doRequestWithContext(context.Background(), method, path, body, result)
}

func (c *Client) doRequestWithContext(ctx context.Context, method, path string, body interface{}, result interface{}) error {
	var reader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("marshal body: %w", err)
		}
		reader = bytes.NewReader(b)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.URL+path, reader)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if err := c.applyAuthHeaders(ctx, req); err != nil {
		return err
	}

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	c.parseServerTiming(resp)

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode >= 400 {
		var apiErr struct {
			Error string `json:"error"`
			Code  string `json:"code"`
		}
		_ = json.Unmarshal(data, &apiErr)
		if apiErr.Code == "PAYLOAD_TOO_LARGE" {
			return &PayloadTooLargeError{Message: apiErr.Error}
		}
		return fmt.Errorf("%s: %s", apiErr.Code, apiErr.Error)
	}

	if result != nil {
		return json.Unmarshal(data, result)
	}
	return nil
}

func (c *Client) applyAuthHeaders(ctx context.Context, req *http.Request) error {
	for k, v := range c.auth.headers {
		req.Header.Set(k, v)
	}
	if c.auth.apiKey != "" {
		header := c.auth.apiKeyHeader
		if header == "" {
			header = "X-API-Key"
		}
		req.Header.Set(header, c.auth.apiKey)
	}
	token := strings.TrimSpace(c.auth.bearerToken)
	if c.auth.tokenSource != nil {
		dyn, err := c.auth.tokenSource(ctx)
		if err != nil {
			return err
		}
		token = strings.TrimSpace(dyn)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	return nil
}

// parseServerTiming extracts the processing duration from the Server-Timing header.
// Expected format: proc;dur=1.234 (milliseconds).
func (c *Client) parseServerTiming(resp *http.Response) {
	c.ServerDuration = 0
	v := resp.Header.Get("Server-Timing")
	if v == "" {
		return
	}
	// Find dur= value.
	for _, part := range strings.Split(v, ";") {
		part = strings.TrimSpace(part)
		if strings.HasPrefix(part, "dur=") {
			ms, err := strconv.ParseFloat(part[4:], 64)
			if err == nil {
				c.ServerDuration = time.Duration(ms * float64(time.Millisecond))
			}
			return
		}
	}
}
