package rpc

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"connectrpc.com/connect"
	corvov1 "github.com/corvohq/go-sdk/internal/gen/corvo/v1"
	"github.com/corvohq/go-sdk/internal/gen/corvo/v1/corvov1connect"
	"golang.org/x/net/http2"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Client is a typed worker lifecycle client over Connect RPC.
type Client struct {
	rpc corvov1connect.WorkerServiceClient
}

// Option configures a Client.
type Option func(*config)

type config struct {
	httpClient   *http.Client
	useJSON      bool
	headers      map[string]string
	bearerToken  string
	apiKey       string
	apiKeyHeader string
	tokenSource  func(context.Context) (string, error)
}

// WithHTTPClient overrides the HTTP client used by Connect.
func WithHTTPClient(c *http.Client) Option {
	return func(cfg *config) {
		if c != nil {
			cfg.httpClient = c
		}
	}
}

// WithProtoJSON forces JSON payload encoding for requests/responses.
func WithProtoJSON() Option {
	return func(cfg *config) { cfg.useJSON = true }
}

// WithHeader adds a static header to every RPC request.
func WithHeader(key, value string) Option {
	return func(cfg *config) {
		if cfg.headers == nil {
			cfg.headers = map[string]string{}
		}
		cfg.headers[key] = value
	}
}

// WithBearerToken adds Authorization: Bearer <token> to every RPC request.
func WithBearerToken(token string) Option {
	return func(cfg *config) { cfg.bearerToken = token }
}

// WithTokenProvider adds dynamic bearer token lookup per RPC request.
func WithTokenProvider(provider func(context.Context) (string, error)) Option {
	return func(cfg *config) { cfg.tokenSource = provider }
}

// WithAPIKey adds API key header with default header X-API-Key.
func WithAPIKey(key string) Option {
	return WithAPIKeyHeader("X-API-Key", key)
}

// WithAPIKeyHeader adds API key header with a custom header name.
func WithAPIKeyHeader(header, key string) Option {
	return func(cfg *config) {
		cfg.apiKeyHeader = header
		cfg.apiKey = key
	}
}

// New creates a worker client for a Corvo server base URL.
func New(baseURL string, opts ...Option) *Client {
	cfg := config{
		httpClient: defaultHTTPClient(),
	}
	for _, opt := range opts {
		opt(&cfg)
	}

	var clientOpts []connect.ClientOption
	if cfg.useJSON {
		clientOpts = append(clientOpts, connect.WithProtoJSON())
	}
	cfg.httpClient = withAuthHTTPClient(cfg.httpClient, &cfg)

	rpc := corvov1connect.NewWorkerServiceClient(cfg.httpClient, strings.TrimRight(baseURL, "/"), clientOpts...)
	return &Client{rpc: rpc}
}

type authRoundTripper struct {
	base http.RoundTripper
	cfg  *config
}

func (a authRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	cloned := req.Clone(req.Context())
	for k, v := range a.cfg.headers {
		cloned.Header.Set(k, v)
	}
	if a.cfg.apiKey != "" {
		h := a.cfg.apiKeyHeader
		if h == "" {
			h = "X-API-Key"
		}
		cloned.Header.Set(h, a.cfg.apiKey)
	}
	token := strings.TrimSpace(a.cfg.bearerToken)
	if a.cfg.tokenSource != nil {
		dyn, err := a.cfg.tokenSource(cloned.Context())
		if err != nil {
			return nil, err
		}
		token = strings.TrimSpace(dyn)
	}
	if token != "" {
		cloned.Header.Set("Authorization", "Bearer "+token)
	}
	return a.base.RoundTrip(cloned)
}

func withAuthHTTPClient(httpClient *http.Client, cfg *config) *http.Client {
	if httpClient == nil {
		return httpClient
	}
	if len(cfg.headers) == 0 && cfg.bearerToken == "" && cfg.apiKey == "" && cfg.tokenSource == nil {
		return httpClient
	}
	base := httpClient.Transport
	if base == nil {
		base = http.DefaultTransport
	}
	cp := *httpClient
	cp.Transport = authRoundTripper{base: base, cfg: cfg}
	return &cp
}

func defaultHTTPClient() *http.Client {
	dialer := &net.Dialer{
		Timeout:   5 * time.Second,
		KeepAlive: 30 * time.Second,
	}
	tr := &http2.Transport{
		AllowHTTP: true,
		DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
			return dialer.DialContext(ctx, network, addr)
		},
		ReadIdleTimeout: 30 * time.Second,
		PingTimeout:     10 * time.Second,
	}
	return &http.Client{
		// Streaming lifecycle RPCs are long-lived; rely on per-request contexts
		// instead of a global client timeout that can terminate active streams.
		Timeout:   0,
		Transport: tr,
	}
}

type EnqueueRequest struct {
	Queue   string
	Payload json.RawMessage
	Agent   *AgentConfig
}

type EnqueueResponse struct {
	JobID          string
	Status         string
	UniqueExisting bool
}

func (c *Client) Enqueue(ctx context.Context, req EnqueueRequest) (*EnqueueResponse, error) {
	if req.Queue == "" {
		return nil, fmt.Errorf("queue is required")
	}
	payload := strings.TrimSpace(string(req.Payload))
	if payload == "" {
		payload = `{}`
	}

	resp, err := c.rpc.Enqueue(ctx, connect.NewRequest(&corvov1.EnqueueRequest{
		Queue:       req.Queue,
		PayloadJson: payload,
		Agent:       agentConfigToPB(req.Agent),
	}))
	if err != nil {
		return nil, err
	}
	return &EnqueueResponse{
		JobID:          resp.Msg.GetJobId(),
		Status:         resp.Msg.GetStatus(),
		UniqueExisting: resp.Msg.GetUniqueExisting(),
	}, nil
}

type FetchRequest struct {
	Queues        []string
	WorkerID      string
	Hostname      string
	LeaseDuration int
}

type FetchedJob struct {
	JobID         string
	Queue         string
	Payload       json.RawMessage
	Attempt       int
	MaxRetries    int
	LeaseDuration int
	Checkpoint    json.RawMessage
	Tags          json.RawMessage
	Agent         *AgentState
}

// Fetch returns nil,nil when no job is available.
func (c *Client) Fetch(ctx context.Context, req FetchRequest) (*FetchedJob, error) {
	resp, err := c.rpc.Fetch(ctx, connect.NewRequest(&corvov1.FetchRequest{
		Queues:        req.Queues,
		WorkerId:      req.WorkerID,
		Hostname:      req.Hostname,
		LeaseDuration: int32(req.LeaseDuration),
	}))
	if err != nil {
		return nil, err
	}
	if !resp.Msg.GetFound() {
		return nil, nil
	}
	return &FetchedJob{
		JobID:         resp.Msg.GetJobId(),
		Queue:         resp.Msg.GetQueue(),
		Payload:       json.RawMessage(resp.Msg.GetPayloadJson()),
		Attempt:       int(resp.Msg.GetAttempt()),
		MaxRetries:    int(resp.Msg.GetMaxRetries()),
		LeaseDuration: int(resp.Msg.GetLeaseDuration()),
		Checkpoint:    json.RawMessage(resp.Msg.GetCheckpointJson()),
		Tags:          json.RawMessage(resp.Msg.GetTagsJson()),
		Agent:         agentStateFromPB(resp.Msg.GetAgent()),
	}, nil
}

func (c *Client) FetchBatch(ctx context.Context, req FetchRequest, count int) ([]FetchedJob, error) {
	if count <= 0 {
		count = 1
	}
	resp, err := c.rpc.FetchBatch(ctx, connect.NewRequest(&corvov1.FetchBatchRequest{
		Queues:        req.Queues,
		WorkerId:      req.WorkerID,
		Hostname:      req.Hostname,
		LeaseDuration: int32(req.LeaseDuration),
		Count:         int32(count),
	}))
	if err != nil {
		return nil, err
	}
	jobs := make([]FetchedJob, 0, len(resp.Msg.GetJobs()))
	for _, j := range resp.Msg.GetJobs() {
		jobs = append(jobs, FetchedJob{
			JobID:         j.GetJobId(),
			Queue:         j.GetQueue(),
			Payload:       json.RawMessage(j.GetPayloadJson()),
			Attempt:       int(j.GetAttempt()),
			MaxRetries:    int(j.GetMaxRetries()),
			LeaseDuration: int(j.GetLeaseDuration()),
			Checkpoint:    json.RawMessage(j.GetCheckpointJson()),
			Tags:          json.RawMessage(j.GetTagsJson()),
			Agent:         agentStateFromPB(j.GetAgent()),
		})
	}
	return jobs, nil
}

func (c *Client) Ack(ctx context.Context, jobID string, result json.RawMessage) error {
	return c.AckWithUsage(ctx, jobID, result, nil)
}

func (c *Client) AckWithUsage(ctx context.Context, jobID string, result json.RawMessage, usage *UsageReport) error {
	return c.AckWithOptions(ctx, AckOptions{
		JobID:  jobID,
		Result: result,
		Usage:  usage,
	})
}

type AckOptions struct {
	JobID       string
	Result      json.RawMessage
	Checkpoint  json.RawMessage
	Trace       json.RawMessage
	Usage       *UsageReport
	AgentStatus string
	HoldReason  string
}

func (c *Client) AckWithOptions(ctx context.Context, req AckOptions) error {
	jobID := strings.TrimSpace(req.JobID)
	if jobID == "" {
		return fmt.Errorf("job_id is required")
	}
	resultJSON := strings.TrimSpace(string(req.Result))
	if resultJSON == "" {
		resultJSON = `{}`
	}
	checkpointJSON := strings.TrimSpace(string(req.Checkpoint))
	traceJSON := strings.TrimSpace(string(req.Trace))
	_, err := c.rpc.Ack(ctx, connect.NewRequest(&corvov1.AckRequest{
		JobId:          jobID,
		ResultJson:     resultJSON,
		Usage:          usageToPB(req.Usage),
		CheckpointJson: checkpointJSON,
		AgentStatus:    req.AgentStatus,
		HoldReason:     req.HoldReason,
		TraceJson:      traceJSON,
	}))
	return err
}

type AckBatchItem struct {
	JobID  string
	Result json.RawMessage
	Usage  *UsageReport
}

func (c *Client) AckBatch(ctx context.Context, items []AckBatchItem) (int, error) {
	if len(items) == 0 {
		return 0, fmt.Errorf("items is required")
	}
	reqItems := make([]*corvov1.AckBatchItem, 0, len(items))
	for _, item := range items {
		jobID := strings.TrimSpace(item.JobID)
		if jobID == "" {
			return 0, fmt.Errorf("job_id is required")
		}
		resultJSON := strings.TrimSpace(string(item.Result))
		if resultJSON == "" {
			resultJSON = `{}`
		}
		reqItems = append(reqItems, &corvov1.AckBatchItem{
			JobId:      jobID,
			ResultJson: resultJSON,
			Usage:      usageToPB(item.Usage),
		})
	}

	resp, err := c.rpc.AckBatch(ctx, connect.NewRequest(&corvov1.AckBatchRequest{Items: reqItems}))
	if err != nil {
		return 0, err
	}
	return int(resp.Msg.GetAcked()), nil
}

type LifecycleRequest struct {
	RequestID    uint64
	Queues       []string
	WorkerID     string
	Hostname     string
	LeaseSeconds int
	FetchCount   int
	Acks         []AckBatchItem
	Enqueues     []LifecycleEnqueueItem
}

type LifecycleEnqueueItem struct {
	Queue   string
	Payload json.RawMessage
	Agent   *AgentConfig
}

type LifecycleResponse struct {
	RequestID      uint64
	Jobs           []FetchedJob
	Acked          int
	EnqueuedJobIDs []string
	Error          string
	LeaderAddr     string
}

// ErrNotLeader is returned when a lifecycle stream hits a non-leader node.
// LeaderAddr contains the HTTP URL of the current leader, or "" if unknown.
type ErrNotLeader struct {
	LeaderAddr string
}

func (e *ErrNotLeader) Error() string {
	if e.LeaderAddr != "" {
		return "NOT_LEADER: leader is at " + e.LeaderAddr
	}
	return "NOT_LEADER: leader unknown"
}

type LifecycleStream struct {
	stream *connect.BidiStreamForClient[corvov1.LifecycleStreamRequest, corvov1.LifecycleStreamResponse]
	mu     sync.Mutex
	closed bool
}

func (c *Client) OpenLifecycleStream(ctx context.Context) *LifecycleStream {
	return &LifecycleStream{stream: c.rpc.StreamLifecycle(ctx)}
}

func (s *LifecycleStream) Exchange(req LifecycleRequest) (*LifecycleResponse, error) {
	if s == nil || s.stream == nil {
		return nil, fmt.Errorf("lifecycle stream is not open")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, io.EOF
	}

	items := make([]*corvov1.AckBatchItem, 0, len(req.Acks))
	for _, ack := range req.Acks {
		jobID := strings.TrimSpace(ack.JobID)
		if jobID == "" {
			return nil, fmt.Errorf("job_id is required")
		}
		resultJSON := strings.TrimSpace(string(ack.Result))
		if resultJSON == "" {
			resultJSON = `{}`
		}
		items = append(items, &corvov1.AckBatchItem{
			JobId:      jobID,
			ResultJson: resultJSON,
			Usage:      usageToPB(ack.Usage),
		})
	}
	enqueues := make([]*corvov1.LifecycleEnqueueItem, 0, len(req.Enqueues))
	for _, enq := range req.Enqueues {
		queue := strings.TrimSpace(enq.Queue)
		if queue == "" {
			return nil, fmt.Errorf("enqueue queue is required")
		}
		payload := strings.TrimSpace(string(enq.Payload))
		if payload == "" {
			payload = `{}`
		}
		enqueues = append(enqueues, &corvov1.LifecycleEnqueueItem{
			Queue:       queue,
			PayloadJson: payload,
			Agent:       agentConfigToPB(enq.Agent),
		})
	}

	if err := s.stream.Send(&corvov1.LifecycleStreamRequest{
		RequestId:     req.RequestID,
		Queues:        req.Queues,
		WorkerId:      req.WorkerID,
		Hostname:      req.Hostname,
		LeaseDuration: int32(req.LeaseSeconds),
		FetchCount:    int32(req.FetchCount),
		Acks:          items,
		Enqueues:      enqueues,
	}); err != nil {
		return nil, err
	}

	msg, err := s.stream.Receive()
	if err != nil {
		if err == io.EOF {
			return nil, io.EOF
		}
		return nil, err
	}
	if msg.GetError() == "NOT_LEADER" {
		return nil, &ErrNotLeader{LeaderAddr: msg.GetLeaderAddr()}
	}

	resp := &LifecycleResponse{
		RequestID:  msg.GetRequestId(),
		Acked:      int(msg.GetAcked()),
		Error:      msg.GetError(),
		LeaderAddr: msg.GetLeaderAddr(),
	}
	resp.EnqueuedJobIDs = append(resp.EnqueuedJobIDs, msg.GetEnqueuedJobIds()...)
	resp.Jobs = make([]FetchedJob, 0, len(msg.GetJobs()))
	for _, j := range msg.GetJobs() {
		resp.Jobs = append(resp.Jobs, FetchedJob{
			JobID:         j.GetJobId(),
			Queue:         j.GetQueue(),
			Payload:       json.RawMessage(j.GetPayloadJson()),
			Attempt:       int(j.GetAttempt()),
			MaxRetries:    int(j.GetMaxRetries()),
			LeaseDuration: int(j.GetLeaseDuration()),
			Checkpoint:    json.RawMessage(j.GetCheckpointJson()),
			Tags:          json.RawMessage(j.GetTagsJson()),
			Agent:         agentStateFromPB(j.GetAgent()),
		})
	}
	return resp, nil
}

func (s *LifecycleStream) Close() error {
	if s == nil || s.stream == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	return s.stream.CloseRequest()
}

// ResilientLifecycleStream wraps a LifecycleStream with automatic
// reconnection on NOT_LEADER errors. When the server indicates a different
// leader, the stream reconnects directly to that node.
type ResilientLifecycleStream struct {
	ctx     context.Context
	baseURL string
	opts    []Option
	stream  *LifecycleStream
}

// OpenResilientLifecycleStream creates a lifecycle stream that automatically
// reconnects to the leader on NOT_LEADER errors.
func OpenResilientLifecycleStream(ctx context.Context, baseURL string, opts ...Option) *ResilientLifecycleStream {
	client := New(baseURL, opts...)
	return &ResilientLifecycleStream{
		ctx:     ctx,
		baseURL: baseURL,
		opts:    opts,
		stream:  client.OpenLifecycleStream(ctx),
	}
}

// Exchange sends a request and receives a response, reconnecting to the
// leader if a NOT_LEADER error is received with a known leader address.
// Returns ErrNotLeader (with empty LeaderAddr) if the leader is unknown.
// Returns other errors (io.EOF, transport) directly for caller backoff.
func (r *ResilientLifecycleStream) Exchange(req LifecycleRequest) (*LifecycleResponse, error) {
	resp, err := r.stream.Exchange(req)
	if err == nil {
		return resp, nil
	}

	var notLeader *ErrNotLeader
	if !errors.As(err, &notLeader) {
		return nil, err
	}
	if notLeader.LeaderAddr == "" {
		return nil, err
	}

	// Reconnect to the leader.
	_ = r.stream.Close()
	client := New(notLeader.LeaderAddr, r.opts...)
	r.stream = client.OpenLifecycleStream(r.ctx)
	r.baseURL = notLeader.LeaderAddr

	return r.stream.Exchange(req)
}

// Close closes the underlying stream.
func (r *ResilientLifecycleStream) Close() error {
	if r.stream == nil {
		return nil
	}
	return r.stream.Close()
}

type FailResponse struct {
	Status            string
	NextAttemptAt     *time.Time
	AttemptsRemaining int
}

func (c *Client) Fail(ctx context.Context, jobID, errMsg, backtrace string) (*FailResponse, error) {
	if jobID == "" {
		return nil, fmt.Errorf("job_id is required")
	}
	resp, err := c.rpc.Fail(ctx, connect.NewRequest(&corvov1.FailRequest{
		JobId:     jobID,
		Error:     errMsg,
		Backtrace: backtrace,
	}))
	if err != nil {
		return nil, err
	}

	return &FailResponse{
		Status:            resp.Msg.GetStatus(),
		NextAttemptAt:     fromProtoTime(resp.Msg.GetNextAttemptAt()),
		AttemptsRemaining: int(resp.Msg.GetAttemptsRemaining()),
	}, nil
}

type HeartbeatJobUpdate struct {
	Progress    map[string]any
	Checkpoint  map[string]any
	StreamDelta string
	Usage       *UsageReport
}

// Heartbeat returns per-job status (e.g. "ok", "cancel").
func (c *Client) Heartbeat(ctx context.Context, jobs map[string]HeartbeatJobUpdate) (map[string]string, error) {
	reqJobs := make(map[string]*corvov1.HeartbeatJobUpdate, len(jobs))
	for jobID, update := range jobs {
		progressJSON, err := marshalMap(update.Progress)
		if err != nil {
			return nil, fmt.Errorf("marshal progress for %s: %w", jobID, err)
		}
		checkpointJSON, err := marshalMap(update.Checkpoint)
		if err != nil {
			return nil, fmt.Errorf("marshal checkpoint for %s: %w", jobID, err)
		}
		reqJobs[jobID] = &corvov1.HeartbeatJobUpdate{
			ProgressJson:   progressJSON,
			CheckpointJson: checkpointJSON,
			StreamDelta:    update.StreamDelta,
			Usage:          usageToPB(update.Usage),
		}
	}

	resp, err := c.rpc.Heartbeat(ctx, connect.NewRequest(&corvov1.HeartbeatRequest{Jobs: reqJobs}))
	if err != nil {
		return nil, err
	}

	result := make(map[string]string, len(resp.Msg.GetJobs()))
	for jobID, r := range resp.Msg.GetJobs() {
		result[jobID] = r.GetStatus()
	}
	return result, nil
}

func marshalMap(m map[string]any) (string, error) {
	if m == nil {
		return "", nil
	}
	b, err := json.Marshal(m)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func fromProtoTime(ts *timestamppb.Timestamp) *time.Time {
	if ts == nil {
		return nil
	}
	t := ts.AsTime()
	return &t
}

type UsageReport struct {
	InputTokens         int64
	OutputTokens        int64
	CacheCreationTokens int64
	CacheReadTokens     int64
	Model               string
	Provider            string
	CostUSD             float64
}

type AgentConfig struct {
	MaxIterations    int
	MaxCostUSD       float64
	IterationTimeout string
}

type AgentState struct {
	MaxIterations    int
	MaxCostUSD       float64
	IterationTimeout string
	Iteration        int
	TotalCostUSD     float64
}

func usageToPB(u *UsageReport) *corvov1.UsageReport {
	if u == nil {
		return nil
	}
	return &corvov1.UsageReport{
		InputTokens:         u.InputTokens,
		OutputTokens:        u.OutputTokens,
		CacheCreationTokens: u.CacheCreationTokens,
		CacheReadTokens:     u.CacheReadTokens,
		Model:               strings.TrimSpace(u.Model),
		Provider:            strings.TrimSpace(u.Provider),
		CostUsd:             u.CostUSD,
	}
}

func agentConfigToPB(a *AgentConfig) *corvov1.AgentConfig {
	if a == nil {
		return nil
	}
	return &corvov1.AgentConfig{
		MaxIterations:    int32(a.MaxIterations),
		MaxCostUsd:       a.MaxCostUSD,
		IterationTimeout: strings.TrimSpace(a.IterationTimeout),
	}
}

func agentStateFromPB(a *corvov1.AgentState) *AgentState {
	if a == nil {
		return nil
	}
	return &AgentState{
		MaxIterations:    int(a.GetMaxIterations()),
		MaxCostUSD:       a.GetMaxCostUsd(),
		IterationTimeout: a.GetIterationTimeout(),
		Iteration:        int(a.GetIteration()),
		TotalCostUSD:     a.GetTotalCostUsd(),
	}
}
