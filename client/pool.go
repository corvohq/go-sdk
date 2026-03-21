package client

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/corvohq/go-sdk/rpc"
)

// PooledClient wraps N internal HTTP clients (lanes) for connection pooling
// and auto-batches individual Enqueue calls into batch HTTP requests.
// Multiple goroutines calling Enqueue concurrently have their jobs grouped
// into a single HTTP request (group-commit pattern). Other operations are
// passed through to internal clients via round-robin.
//
// When RPCHost/RPCPort are configured, hot-path operations (EnqueueRPC,
// FetchRPC, AckRPC, FailRPC, PingRPC) use binary RPC connections instead
// of HTTP, while management operations still use HTTP.
type PooledClient struct {
	lanes    []*lane
	clients  []*Client
	conns    []*rpc.Conn // binary RPC connections, nil when not configured
	next     atomic.Uint64
	numLanes int
}

// PoolOptions configures a PooledClient.
type PoolOptions struct {
	BaseURL       string
	Lanes         int           // default 8
	MaxBatch      int           // default 256
	FlushInterval time.Duration // default 1ms
	ClientOptions []ClientOption

	// RPCHost and RPCPort enable binary RPC connections for hot-path
	// operations. When set, the pool creates one rpc.Conn per lane.
	// Management operations (search, queues, bulk, etc.) still use HTTP.
	RPCHost string
	RPCPort int
}

func (o *PoolOptions) defaults() {
	if o.Lanes <= 0 {
		o.Lanes = 8
	}
	if o.MaxBatch <= 0 {
		o.MaxBatch = 256
	}
	if o.FlushInterval <= 0 {
		o.FlushInterval = 1 * time.Millisecond
	}
}

// EnqueueRequest describes a single job to enqueue via the pooled client.
type EnqueueRequest struct {
	Queue       string
	Payload     interface{}
	Priority    *string
	MaxRetries  *int
	UniqueKey   *string
	UniquePeriod *int
	Tags        map[string]string
	ScheduledAt *time.Time
	ExpireAfter *time.Duration
	BatchID     *string
}

// enqueueEntry is an in-flight enqueue waiting to be flushed.
type enqueueEntry struct {
	req    EnqueueRequest
	result *EnqueueResult
	err    error
	done   chan struct{}
}

// lane is a single batching lane with its own HTTP client, buffer, and flush goroutine.
type lane struct {
	client        *Client
	mu            sync.Mutex
	buffer        []*enqueueEntry
	maxBatch      int
	flushInterval time.Duration
	closed        bool
	flushCh       chan struct{} // signalled when buffer hits maxBatch
	stopCh        chan struct{}
	wg            sync.WaitGroup
}

func newLane(client *Client, maxBatch int, flushInterval time.Duration) *lane {
	l := &lane{
		client:        client,
		buffer:        make([]*enqueueEntry, 0, maxBatch),
		maxBatch:      maxBatch,
		flushInterval: flushInterval,
		flushCh:       make(chan struct{}, 1),
		stopCh:        make(chan struct{}),
	}
	l.wg.Add(1)
	go l.flushLoop()
	return l
}

// submit adds an enqueue entry to the lane buffer and blocks until flushed.
func (l *lane) submit(entry *enqueueEntry) {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		entry.err = fmt.Errorf("pooled client is closed")
		close(entry.done)
		return
	}
	l.buffer = append(l.buffer, entry)
	needsFlush := len(l.buffer) >= l.maxBatch
	l.mu.Unlock()

	if needsFlush {
		// Non-blocking send: if flushCh already has a pending signal, skip.
		select {
		case l.flushCh <- struct{}{}:
		default:
		}
	}
}

// flushLoop runs in a background goroutine, flushing the buffer periodically
// or when it reaches maxBatch.
func (l *lane) flushLoop() {
	defer l.wg.Done()

	timer := time.NewTimer(l.flushInterval)
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			// Periodic flush.
		case <-l.flushCh:
			// Immediate flush (buffer hit maxBatch).
		case <-l.stopCh:
			// Shutting down: flush remaining entries.
			l.mu.Lock()
			entries := l.drainLocked()
			l.mu.Unlock()
			if len(entries) > 0 {
				l.flushEntries(entries)
			}
			return
		}

		l.mu.Lock()
		entries := l.drainLocked()
		l.mu.Unlock()

		if len(entries) > 0 {
			l.flushEntries(entries)
		}

		// Drain the timer channel before resetting to avoid double-fires.
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(l.flushInterval)
	}
}

// drainLocked swaps out the buffer and returns the old entries. Must be called with mu held.
func (l *lane) drainLocked() []*enqueueEntry {
	if len(l.buffer) == 0 {
		return nil
	}
	entries := l.buffer
	l.buffer = make([]*enqueueEntry, 0, l.maxBatch)
	return entries
}

// flushEntries sends a batch enqueue request and notifies all waiting callers.
func (l *lane) flushEntries(entries []*enqueueEntry) {
	// Build the batch request body.
	jobs := make([]map[string]interface{}, len(entries))
	for i, e := range entries {
		job := map[string]interface{}{
			"queue":   e.req.Queue,
			"payload": e.req.Payload,
		}
		if e.req.Priority != nil {
			job["priority"] = *e.req.Priority
		}
		if e.req.MaxRetries != nil {
			job["max_retries"] = *e.req.MaxRetries
		}
		if e.req.UniqueKey != nil {
			job["unique_key"] = *e.req.UniqueKey
		}
		if e.req.UniquePeriod != nil {
			job["unique_period"] = *e.req.UniquePeriod
		}
		if e.req.Tags != nil {
			job["tags"] = e.req.Tags
		}
		if e.req.ScheduledAt != nil {
			job["scheduled_at"] = e.req.ScheduledAt.Format(time.RFC3339)
		}
		if e.req.ExpireAfter != nil {
			job["expire_after"] = e.req.ExpireAfter.String()
		}
		if e.req.BatchID != nil {
			job["batch_id"] = *e.req.BatchID
		}
		jobs[i] = job
	}

	body := map[string]interface{}{"jobs": jobs}
	var result struct {
		JobIDs []string `json:"job_ids"`
	}

	err := l.client.post("/api/v1/enqueue/batch", body, &result)
	if err != nil {
		for _, e := range entries {
			e.err = err
			close(e.done)
		}
		return
	}

	// Match results back to callers.
	for i, e := range entries {
		if i < len(result.JobIDs) {
			e.result = &EnqueueResult{
				JobID: result.JobIDs[i],
			}
		} else {
			e.err = fmt.Errorf("server returned fewer job IDs than submitted (%d < %d)", len(result.JobIDs), len(entries))
		}
		close(e.done)
	}
}

// stop signals the flush loop to exit and waits for it.
func (l *lane) stop() {
	l.mu.Lock()
	l.closed = true
	l.mu.Unlock()
	close(l.stopCh)
	l.wg.Wait()
}

// NewPooledClient creates a new auto-batching pooled client.
func NewPooledClient(opts PoolOptions) *PooledClient {
	opts.defaults()

	clients := make([]*Client, opts.Lanes)
	lanes := make([]*lane, opts.Lanes)
	for i := range opts.Lanes {
		clients[i] = NewWithOptions(opts.BaseURL, opts.ClientOptions...)
		lanes[i] = newLane(clients[i], opts.MaxBatch, opts.FlushInterval)
	}

	var conns []*rpc.Conn
	if opts.RPCHost != "" && opts.RPCPort > 0 {
		conns = make([]*rpc.Conn, opts.Lanes)
		for i := range opts.Lanes {
			conns[i] = rpc.NewConn(opts.RPCHost, opts.RPCPort)
		}
	}

	return &PooledClient{
		lanes:    lanes,
		clients:  clients,
		conns:    conns,
		numLanes: opts.Lanes,
	}
}

// pick returns the next lane/client index via round-robin.
func (p *PooledClient) pick() int {
	return int(p.next.Add(1)-1) % p.numLanes
}

// pickClient returns the next internal client via round-robin.
func (p *PooledClient) pickClient() *Client {
	return p.clients[p.pick()]
}

// pickConn returns the next binary RPC connection via round-robin.
// Returns nil if binary RPC is not configured.
func (p *PooledClient) pickConn() *rpc.Conn {
	if len(p.conns) == 0 {
		return nil
	}
	return p.conns[p.pick()]
}

// HasRPC reports whether binary RPC connections are configured.
func (p *PooledClient) HasRPC() bool {
	return len(p.conns) > 0
}

// Close shuts down all lane flush goroutines, drains remaining entries,
// and closes any binary RPC connections.
// After Close returns, further Enqueue calls will return an error.
func (p *PooledClient) Close() {
	for _, l := range p.lanes {
		l.stop()
	}
	for _, c := range p.conns {
		c.Close()
	}
}

// --- Binary RPC methods ---
// These methods use the binary wire protocol for hot-path operations.
// They return an error if binary RPC is not configured.

// PingRPC sends a ping over the binary RPC connection.
func (p *PooledClient) PingRPC() error {
	conn := p.pickConn()
	if conn == nil {
		return fmt.Errorf("binary RPC not configured")
	}
	return conn.Ping()
}

// EnqueueRPC enqueues a batch of jobs using the binary RPC connection.
// Returns the number of successfully enqueued jobs.
func (p *PooledClient) EnqueueRPC(jobs []rpc.EnqueueJob) (int, error) {
	conn := p.pickConn()
	if conn == nil {
		return 0, fmt.Errorf("binary RPC not configured")
	}
	return conn.EnqueueBatch(jobs)
}

// FetchRPC subscribes and blocks until jobs are pushed by the server.
// Internally it sends a subscribe with the given credits, then reads
// the pushed response. This is a blocking call.
func (p *PooledClient) FetchRPC(queues []string, workerID string, count int) ([]rpc.FetchedJob, error) {
	conn := p.pickConn()
	if conn == nil {
		return nil, fmt.Errorf("binary RPC not configured")
	}
	if err := conn.Subscribe(queues, workerID, count); err != nil {
		return nil, err
	}
	return conn.ReadPushedJobs()
}

// AckRPC acknowledges a batch of jobs using the binary RPC connection.
func (p *PooledClient) AckRPC(acks []rpc.AckJob) error {
	conn := p.pickConn()
	if conn == nil {
		return fmt.Errorf("binary RPC not configured")
	}
	return conn.AckBatch(acks)
}

// FailRPC marks a batch of jobs as failed using the binary RPC connection.
func (p *PooledClient) FailRPC(jobs []rpc.FailJob) error {
	conn := p.pickConn()
	if conn == nil {
		return fmt.Errorf("binary RPC not configured")
	}
	return conn.FailBatch(jobs)
}

// --- Auto-batched enqueue ---

// Enqueue enqueues a single job, auto-batching it with other concurrent callers.
// Blocks until the batch containing this job is flushed to the server.
// Returns the job ID assigned by the server.
func (p *PooledClient) Enqueue(ctx context.Context, req EnqueueRequest) (string, error) {
	entry := &enqueueEntry{
		req:  req,
		done: make(chan struct{}),
	}

	idx := p.pick()
	p.lanes[idx].submit(entry)

	select {
	case <-entry.done:
		if entry.err != nil {
			return "", entry.err
		}
		return entry.result.JobID, nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

// --- Passthrough methods (round-robin across internal clients) ---

// EnqueueDirect enqueues a single job directly (bypasses auto-batching).
func (p *PooledClient) EnqueueDirect(queue string, payload interface{}, opts ...EnqueueOption) (*EnqueueResult, error) {
	return p.pickClient().Enqueue(queue, payload, opts...)
}

// EnqueueBatch enqueues multiple jobs in a single request.
func (p *PooledClient) EnqueueBatch(req BatchRequest) (*BatchResult, error) {
	return p.pickClient().EnqueueBatch(req)
}

// GetJob returns a job by ID.
func (p *PooledClient) GetJob(id string) (*Job, error) {
	return p.pickClient().GetJob(id)
}

// Search searches for jobs.
func (p *PooledClient) Search(filter SearchFilter) (*SearchResult, error) {
	return p.pickClient().Search(filter)
}

// ListQueues lists all queues.
func (p *PooledClient) ListQueues() (json.RawMessage, error) {
	return p.pickClient().ListQueues()
}

// PauseQueue pauses a queue.
func (p *PooledClient) PauseQueue(name string) error {
	return p.pickClient().PauseQueue(name)
}

// ResumeQueue resumes a queue.
func (p *PooledClient) ResumeQueue(name string) error {
	return p.pickClient().ResumeQueue(name)
}

// Fetch long-polls for a job from the given queues.
func (p *PooledClient) Fetch(ctx context.Context, queues []string, workerID, hostname string, timeout int) (*FetchResult, error) {
	return p.pickClient().Fetch(ctx, queues, workerID, hostname, timeout)
}

// Ack acknowledges a job as complete.
func (p *PooledClient) Ack(jobID string, body AckBody) error {
	return p.pickClient().Ack(jobID, body)
}

// Fail marks a job as failed.
func (p *PooledClient) Fail(jobID, errMsg, backtrace string) (*FailResult, error) {
	return p.pickClient().Fail(jobID, errMsg, backtrace)
}

// Heartbeat sends a batched heartbeat for active jobs.
func (p *PooledClient) Heartbeat(jobs map[string]HeartbeatJob) (*HeartbeatResult, error) {
	return p.pickClient().Heartbeat(jobs)
}

// CancelJob cancels a job.
func (p *PooledClient) CancelJob(id string) error {
	return p.pickClient().CancelJob(id)
}

// DeleteJob deletes a job.
func (p *PooledClient) DeleteJob(id string) error {
	return p.pickClient().DeleteJob(id)
}

// MoveJob moves a job to a different queue.
func (p *PooledClient) MoveJob(id, targetQueue string) error {
	return p.pickClient().MoveJob(id, targetQueue)
}

// Bulk performs a bulk operation on jobs.
func (p *PooledClient) Bulk(req BulkRequest) (*BulkResult, error) {
	return p.pickClient().Bulk(req)
}

// BulkStatus checks the status of an async bulk operation.
func (p *PooledClient) BulkStatus(id string) (*BulkTask, error) {
	return p.pickClient().BulkStatus(id)
}

// BulkGetJobs returns jobs for the given IDs.
func (p *PooledClient) BulkGetJobs(ids []string) ([]*Job, error) {
	return p.pickClient().BulkGetJobs(ids)
}

// CreateBatch creates a new open batch with the given callback queue.
func (p *PooledClient) CreateBatch(callbackQueue string, callbackPayload interface{}) (*CreateBatchResult, error) {
	return p.pickClient().CreateBatch(callbackQueue, callbackPayload)
}

// SealBatch seals a batch so no more jobs can be added.
func (p *PooledClient) SealBatch(batchID string) (*SealBatchResult, error) {
	return p.pickClient().SealBatch(batchID)
}

// GetServerInfo returns version information about the Corvo server.
func (p *PooledClient) GetServerInfo() (*ServerInfo, error) {
	return p.pickClient().GetServerInfo()
}

// Subscribe opens an SSE event stream.
func (p *PooledClient) Subscribe(ctx context.Context, opts SubscribeOptions) (<-chan Event, error) {
	return p.pickClient().Subscribe(ctx, opts)
}
