package rpc

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

// Wire protocol constants.
const (
	headerSize = 9  // msg_type(1) + req_id(4) + payload_len(4)
	bufSize    = 65536 // 64KB send/recv buffers

	// Request message types.
	msgEnqueueBatch = 0x01
	msgFetchBatch   = 0x02
	msgAckBatch     = 0x03
	msgPing         = 0x04
	msgHeartbeat    = 0x06
	msgFailBatch    = 0x07

	// Response message types (request | 0x80).
	msgEnqueueBatchResp = 0x81
	msgFetchBatchResp   = 0x82
	msgAckBatchResp     = 0x83
	msgPingResp         = 0x84
	msgHeartbeatResp    = 0x86
	msgFailBatchResp    = 0x87

	// Error response.
	msgError = 0xFF
)

// Backoff strategy constants.
const (
	BackoffNone        = 0
	BackoffFixed       = 1
	BackoffLinear      = 2
	BackoffExponential = 3
)

// AckStatus constants.
const (
	AckDone = 0
	AckHold = 1
)

// EnqueueJob describes a job to enqueue via the binary RPC protocol.
type EnqueueJob struct {
	Queue         string
	JobID         string
	Priority      uint8
	MaxRetries    uint16
	Backoff       uint8
	BaseDelayMs   uint32
	MaxDelayMs    uint32
	UniquePeriodS uint32
	ScheduledAtNs uint64
	ExpireAfterMs uint32
	ChainStep     uint16
	// Optional fields (included if non-empty/non-nil).
	Payload     []byte
	UniqueKey   string
	Tags        string
	BatchID     string
	ChainID     string
	ChainConfig string
	Group       string
	ParentID    string
}

// AckJob describes an ack to send via the binary RPC protocol.
type AckJob struct {
	JobID      string
	Queue      string
	AckStatus  uint8  // AckDone or AckHold
	Result     string // optional
	Checkpoint string // optional
	HoldReason string // optional
}

// HeartbeatJob describes a heartbeat update for a single job.
type HeartbeatJob struct {
	JobID      string
	Queue      string
	Progress   string // optional
	Checkpoint string // optional
}

// FailJob describes a job failure to report via the binary RPC protocol.
type FailJob struct {
	JobID     string
	Queue     string
	Error     string
	Backtrace string
}

// FetchedJob is a job returned from a fetch subscription push.
type FetchedJob struct {
	JobID      string
	Queue      string
	Attempt    int
	MaxRetries int
	Checkpoint json.RawMessage
	Tags       json.RawMessage
	Payload    json.RawMessage
}

// Conn is a binary RPC connection to a Corvo server.
// It is NOT safe for concurrent use; callers must synchronize externally
// or use one Conn per goroutine.
type Conn struct {
	conn    net.Conn
	host    string
	port    int
	reqID   uint32
	sendBuf []byte
	recvBuf []byte
	mu      sync.Mutex
}

// NewConn creates a new binary RPC connection. It does not connect immediately;
// the connection is established lazily on the first RPC call.
func NewConn(host string, port int) *Conn {
	return &Conn{
		host:    host,
		port:    port,
		sendBuf: make([]byte, bufSize),
		recvBuf: make([]byte, bufSize),
	}
}

// Close closes the underlying TCP connection.
func (c *Conn) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
}

// connect establishes the TCP connection with TCP_NODELAY.
func (c *Conn) connect() error {
	if c.conn != nil {
		return nil
	}
	addr := fmt.Sprintf("%s:%d", c.host, c.port)
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return fmt.Errorf("connect %s: %w", addr, err)
	}
	tc := conn.(*net.TCPConn)
	if err := tc.SetNoDelay(true); err != nil {
		conn.Close()
		return fmt.Errorf("set TCP_NODELAY: %w", err)
	}
	c.conn = conn
	return nil
}

// reconnect drops the current connection and establishes a new one.
func (c *Conn) reconnect() error {
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
	return c.connect()
}

// nextReqID returns the next request ID, wrapping at 2^32.
func (c *Conn) nextReqID() uint32 {
	c.reqID++
	return c.reqID
}

// writeHeader writes the 9-byte frame header into buf at the given offset.
func writeHeader(buf []byte, msgType uint8, reqID uint32, payloadLen uint32) {
	buf[0] = msgType
	binary.LittleEndian.PutUint32(buf[1:5], reqID)
	binary.LittleEndian.PutUint32(buf[5:9], payloadLen)
}

// putLenPrefixed writes a length-prefixed string (u8 len + bytes) into buf
// at offset, returning the new offset.
func putLenPrefixed(buf []byte, off int, s string) int {
	n := len(s)
	if n > 255 {
		n = 255
	}
	buf[off] = byte(n)
	off++
	copy(buf[off:off+n], s)
	return off + n
}

// putLenPrefixedBytes writes a length-prefixed byte slice (u8 len + bytes).
func putLenPrefixedBytes(buf []byte, off int, b []byte) int {
	n := len(b)
	if n > 255 {
		n = 255
	}
	buf[off] = byte(n)
	off++
	copy(buf[off:off+n], b)
	return off + n
}

// putU16Prefixed writes a u16-length-prefixed byte slice (u16 LE len + bytes).
func putU16Prefixed(buf []byte, off int, b []byte) int {
	n := len(b)
	if n > 65535 {
		n = 65535
	}
	binary.LittleEndian.PutUint16(buf[off:], uint16(n))
	off += 2
	copy(buf[off:off+n], b)
	return off + n
}

// readLenPrefixed reads a length-prefixed string from buf at offset.
func readLenPrefixed(buf []byte, off int) (string, int) {
	n := int(buf[off])
	off++
	s := string(buf[off : off+n])
	return s, off + n
}

// readLenPrefixedBytes reads a length-prefixed byte slice from buf at offset.
func readLenPrefixedBytes(buf []byte, off int) ([]byte, int) {
	n := int(buf[off])
	off++
	b := make([]byte, n)
	copy(b, buf[off:off+n])
	return b, off + n
}

// ServerError is returned when the server sends an error response frame (0xFF).
// This is a protocol-level error, not an I/O error, and does not trigger reconnect.
type ServerError struct {
	Message string
}

func (e *ServerError) Error() string {
	return "rpc error: " + e.Message
}

// sendAndRecv writes a complete frame and reads the response frame.
// Returns the response message type and response payload.
// On I/O error (not ServerError), it attempts one reconnect and retry.
func (c *Conn) sendAndRecv(msgType uint8, payload []byte) (uint8, []byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	resp, err := c.doSendAndRecv(msgType, payload)
	if err != nil {
		// ServerError is a protocol-level error; do not reconnect.
		var serverErr *ServerError
		if errors.As(err, &serverErr) {
			return 0, nil, err
		}
		// Reconnect and retry once on I/O error.
		if reconnErr := c.reconnect(); reconnErr != nil {
			return 0, nil, fmt.Errorf("reconnect failed: %w (original: %v)", reconnErr, err)
		}
		resp, err = c.doSendAndRecv(msgType, payload)
		if err != nil {
			return 0, nil, err
		}
	}
	return resp.msgType, resp.payload, nil
}

// sendOnly writes a frame without waiting for a response. Used for
// subscription messages where the server pushes responses asynchronously.
// On I/O error, it attempts one reconnect and retry.
func (c *Conn) sendOnly(msgType uint8, payload []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	err := c.doSendOnly(msgType, payload)
	if err != nil {
		if reconnErr := c.reconnect(); reconnErr != nil {
			return fmt.Errorf("reconnect failed: %w (original: %v)", reconnErr, err)
		}
		err = c.doSendOnly(msgType, payload)
	}
	return err
}

func (c *Conn) doSendOnly(msgType uint8, payload []byte) error {
	if err := c.connect(); err != nil {
		return err
	}

	reqID := c.nextReqID()

	frameLen := headerSize + len(payload)
	var frame []byte
	if frameLen <= len(c.sendBuf) {
		frame = c.sendBuf[:frameLen]
	} else {
		frame = make([]byte, frameLen)
	}
	writeHeader(frame, msgType, reqID, uint32(len(payload)))
	copy(frame[headerSize:], payload)

	if _, err := c.conn.Write(frame); err != nil {
		return fmt.Errorf("write: %w", err)
	}
	return nil
}

// readFrame reads a single frame from the connection (blocking).
// Returns the message type and payload. Does NOT handle reconnect;
// the caller is expected to re-subscribe on error.
func (c *Conn) readFrame() (uint8, []byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn == nil {
		return 0, nil, fmt.Errorf("not connected")
	}

	// Read response header.
	if _, err := io.ReadFull(c.conn, c.recvBuf[:headerSize]); err != nil {
		return 0, nil, fmt.Errorf("read header: %w", err)
	}

	respType := c.recvBuf[0]
	respPayloadLen := binary.LittleEndian.Uint32(c.recvBuf[5:9])

	// Read response payload.
	var respPayload []byte
	if respPayloadLen > 0 {
		if int(respPayloadLen) <= len(c.recvBuf) {
			respPayload = c.recvBuf[:respPayloadLen]
		} else {
			respPayload = make([]byte, respPayloadLen)
		}
		if _, err := io.ReadFull(c.conn, respPayload); err != nil {
			return 0, nil, fmt.Errorf("read payload: %w", err)
		}
		// Copy out of recvBuf so data persists after unlock.
		if int(respPayloadLen) <= len(c.recvBuf) {
			cp := make([]byte, respPayloadLen)
			copy(cp, respPayload)
			respPayload = cp
		}
	}

	return respType, respPayload, nil
}

type response struct {
	msgType uint8
	payload []byte
}

func (c *Conn) doSendAndRecv(msgType uint8, payload []byte) (response, error) {
	if err := c.connect(); err != nil {
		return response{}, err
	}

	reqID := c.nextReqID()

	// Build frame: header + payload.
	frameLen := headerSize + len(payload)
	var frame []byte
	if frameLen <= len(c.sendBuf) {
		frame = c.sendBuf[:frameLen]
	} else {
		frame = make([]byte, frameLen)
	}
	writeHeader(frame, msgType, reqID, uint32(len(payload)))
	copy(frame[headerSize:], payload)

	// Send.
	if _, err := c.conn.Write(frame); err != nil {
		return response{}, fmt.Errorf("write: %w", err)
	}

	// Read response header.
	if _, err := io.ReadFull(c.conn, c.recvBuf[:headerSize]); err != nil {
		return response{}, fmt.Errorf("read header: %w", err)
	}

	respType := c.recvBuf[0]
	respReqID := binary.LittleEndian.Uint32(c.recvBuf[1:5])
	respPayloadLen := binary.LittleEndian.Uint32(c.recvBuf[5:9])

	_ = respReqID // Could validate reqID matches; server echoes it back.

	// Read response payload.
	var respPayload []byte
	if respPayloadLen > 0 {
		if int(respPayloadLen) <= len(c.recvBuf) {
			respPayload = c.recvBuf[:respPayloadLen]
		} else {
			respPayload = make([]byte, respPayloadLen)
		}
		if _, err := io.ReadFull(c.conn, respPayload); err != nil {
			return response{}, fmt.Errorf("read payload: %w", err)
		}
		// If we used recvBuf, make a copy so it persists after unlock.
		if int(respPayloadLen) <= len(c.recvBuf) {
			cp := make([]byte, respPayloadLen)
			copy(cp, respPayload)
			respPayload = cp
		}
	}

	// Check for error response.
	if respType == msgError {
		errMsg := "server error"
		if len(respPayload) > 0 {
			errMsg = string(respPayload)
		}
		return response{}, &ServerError{Message: errMsg}
	}

	return response{msgType: respType, payload: respPayload}, nil
}

// Ping sends a ping and waits for the pong response.
func (c *Conn) Ping() error {
	respType, _, err := c.sendAndRecv(msgPing, nil)
	if err != nil {
		return err
	}
	if respType != msgPingResp {
		return fmt.Errorf("unexpected response type 0x%02x for Ping", respType)
	}
	return nil
}

// EnqueueBatch enqueues a batch of jobs.
// Returns the count of successfully enqueued jobs.
//
// Wire format:
//
//	[count:u16][now_ns:u64]
//	per job: [queue:lenPrefixed][id:lenPrefixed][priority:u8][max_retries:u16]
//	         [backoff:u8][base_delay_ms:u32][max_delay_ms:u32]
//	         [unique_period_s:u32][scheduled_at_ns:u64][expire_after_ms:u32]
//	         [chain_step:u16][flags:u16]
//	         if flags & 0x0001: [payload:u16Prefixed]
//	         if flags & 0x0002: [unique_key:lenPrefixed]
//	         if flags & 0x0004: [tags:lenPrefixed]
//	         if flags & 0x0008: [batch_id:lenPrefixed]
//	         if flags & 0x0010: [chain_id:lenPrefixed]
//	         if flags & 0x0020: [chain_config:lenPrefixed]
//	         if flags & 0x0040: [group:lenPrefixed]
//	         if flags & 0x0080: [parent_id:lenPrefixed]
func (c *Conn) EnqueueBatch(jobs []EnqueueJob) (int, error) {
	count := len(jobs)
	if count == 0 {
		return 0, nil
	}
	if count > 65535 {
		return 0, fmt.Errorf("batch size %d exceeds u16 max", count)
	}

	// Estimate payload size: header + per job fixed + optional fields.
	estSize := 2 + 8 // count + now_ns
	for i := range jobs {
		j := &jobs[i]
		// Fixed fields: queue(1+len) + id(1+len) + priority(1) + max_retries(2)
		//   + backoff(1) + base_delay_ms(4) + max_delay_ms(4)
		//   + unique_period_s(4) + scheduled_at_ns(8) + expire_after_ms(4)
		//   + chain_step(2) + flags(2)
		estSize += 1 + len(j.Queue) + 1 + len(j.JobID) + 1 + 2 + 1 + 4 + 4 + 4 + 8 + 4 + 2 + 2
		// Optional fields.
		if len(j.Payload) > 0 {
			estSize += 2 + len(j.Payload)
		}
		if j.UniqueKey != "" {
			estSize += 1 + len(j.UniqueKey)
		}
		if j.Tags != "" {
			estSize += 1 + len(j.Tags)
		}
		if j.BatchID != "" {
			estSize += 1 + len(j.BatchID)
		}
		if j.ChainID != "" {
			estSize += 1 + len(j.ChainID)
		}
		if j.ChainConfig != "" {
			estSize += 1 + len(j.ChainConfig)
		}
		if j.Group != "" {
			estSize += 1 + len(j.Group)
		}
		if j.ParentID != "" {
			estSize += 1 + len(j.ParentID)
		}
	}

	buf := make([]byte, estSize)
	off := 0

	binary.LittleEndian.PutUint16(buf[off:], uint16(count))
	off += 2
	binary.LittleEndian.PutUint64(buf[off:], uint64(time.Now().UnixNano()))
	off += 8

	for i := range jobs {
		j := &jobs[i]

		off = putLenPrefixed(buf, off, j.Queue)
		off = putLenPrefixed(buf, off, j.JobID)
		buf[off] = j.Priority
		off++
		binary.LittleEndian.PutUint16(buf[off:], j.MaxRetries)
		off += 2
		buf[off] = j.Backoff
		off++
		binary.LittleEndian.PutUint32(buf[off:], j.BaseDelayMs)
		off += 4
		binary.LittleEndian.PutUint32(buf[off:], j.MaxDelayMs)
		off += 4
		binary.LittleEndian.PutUint32(buf[off:], j.UniquePeriodS)
		off += 4
		binary.LittleEndian.PutUint64(buf[off:], j.ScheduledAtNs)
		off += 8
		binary.LittleEndian.PutUint32(buf[off:], j.ExpireAfterMs)
		off += 4
		binary.LittleEndian.PutUint16(buf[off:], j.ChainStep)
		off += 2

		// Compute flags from non-empty optional fields.
		var flags uint16
		if len(j.Payload) > 0 {
			flags |= 0x0001
		}
		if j.UniqueKey != "" {
			flags |= 0x0002
		}
		if j.Tags != "" {
			flags |= 0x0004
		}
		if j.BatchID != "" {
			flags |= 0x0008
		}
		if j.ChainID != "" {
			flags |= 0x0010
		}
		if j.ChainConfig != "" {
			flags |= 0x0020
		}
		if j.Group != "" {
			flags |= 0x0040
		}
		if j.ParentID != "" {
			flags |= 0x0080
		}
		binary.LittleEndian.PutUint16(buf[off:], flags)
		off += 2

		// Write optional fields in flag order.
		if flags&0x0001 != 0 {
			off = putU16Prefixed(buf, off, j.Payload)
		}
		if flags&0x0002 != 0 {
			off = putLenPrefixed(buf, off, j.UniqueKey)
		}
		if flags&0x0004 != 0 {
			off = putLenPrefixed(buf, off, j.Tags)
		}
		if flags&0x0008 != 0 {
			off = putLenPrefixed(buf, off, j.BatchID)
		}
		if flags&0x0010 != 0 {
			off = putLenPrefixed(buf, off, j.ChainID)
		}
		if flags&0x0020 != 0 {
			off = putLenPrefixed(buf, off, j.ChainConfig)
		}
		if flags&0x0040 != 0 {
			off = putLenPrefixed(buf, off, j.Group)
		}
		if flags&0x0080 != 0 {
			off = putLenPrefixed(buf, off, j.ParentID)
		}
	}

	respType, respPayload, err := c.sendAndRecv(msgEnqueueBatch, buf[:off])
	if err != nil {
		return 0, err
	}
	if respType != msgEnqueueBatchResp {
		return 0, fmt.Errorf("unexpected response type 0x%02x for EnqueueBatch", respType)
	}

	// Response: [count:u16][err_code:u8]
	if len(respPayload) < 3 {
		return 0, fmt.Errorf("EnqueueBatch response too short: %d bytes", len(respPayload))
	}
	enqueued := int(binary.LittleEndian.Uint16(respPayload[0:2]))
	errCode := respPayload[2]
	if errCode != 0 {
		return enqueued, fmt.Errorf("EnqueueBatch error code %d", errCode)
	}
	return enqueued, nil
}

// Subscribe sends a fetch subscription with the given number of credits.
// The server will push MSG_FETCH_BATCH_RESP frames asynchronously as jobs
// become available. Use ReadPushedJobs to receive them.
//
// Wire format:
//
//	[now_ns:u64][count:u16][lease_ms:u32][worker_id:lenPrefixed][queue_count:u8][queues:lenPrefixed...]
func (c *Conn) Subscribe(queues []string, workerID string, credits int) error {
	if credits <= 0 {
		credits = 1
	}
	if len(queues) == 0 {
		return fmt.Errorf("at least one queue is required")
	}
	if len(queues) > 255 {
		return fmt.Errorf("queue count %d exceeds u8 max", len(queues))
	}

	// Default lease: 30 seconds.
	leaseMs := uint32(30000)

	// Estimate payload size.
	estSize := 8 + 2 + 4 + 1 + len(workerID) + 1
	for _, q := range queues {
		estSize += 1 + len(q)
	}

	buf := make([]byte, estSize)
	off := 0

	binary.LittleEndian.PutUint64(buf[off:], uint64(time.Now().UnixNano()))
	off += 8
	binary.LittleEndian.PutUint16(buf[off:], uint16(credits))
	off += 2
	binary.LittleEndian.PutUint32(buf[off:], leaseMs)
	off += 4
	off = putLenPrefixed(buf, off, workerID)
	buf[off] = byte(len(queues))
	off++
	for _, q := range queues {
		off = putLenPrefixed(buf, off, q)
	}

	return c.sendOnly(msgFetchBatch, buf[:off])
}

// ReadPushedJobs reads a single pushed MSG_FETCH_BATCH_RESP frame from the
// connection. This call blocks until the server pushes jobs or an error occurs.
// Returns the decoded jobs, or an error if the server sent an error frame or
// an unexpected message type was received.
func (c *Conn) ReadPushedJobs() ([]FetchedJob, error) {
	msgType, payload, err := c.readFrame()
	if err != nil {
		return nil, err
	}

	if msgType == msgError {
		errMsg := "server error"
		if len(payload) > 0 {
			errMsg = string(payload)
		}
		return nil, &ServerError{Message: errMsg}
	}

	if msgType != msgFetchBatchResp {
		return nil, fmt.Errorf("unexpected message type 0x%02x, expected MSG_FETCH_BATCH_RESP (0x%02x)", msgType, msgFetchBatchResp)
	}

	return parseFetchResponse(payload)
}

// parseFetchResponse decodes the fetch response wire format.
//
// Response: [count:u16] per job: [id:lenPrefixed][queue:lenPrefixed][attempt:u16][max_retries:u16]
//
//	[checkpoint:lenPrefixed][tags:lenPrefixed][payload_len:u16][payload_bytes]
func parseFetchResponse(data []byte) ([]FetchedJob, error) {
	if len(data) < 2 {
		return nil, nil
	}

	count := int(binary.LittleEndian.Uint16(data[0:2]))
	if count == 0 {
		return nil, nil
	}

	off := 2
	jobs := make([]FetchedJob, count)

	for i := range count {
		if off >= len(data) {
			return nil, fmt.Errorf("FetchBatch response truncated at job %d", i)
		}

		var job FetchedJob

		job.JobID, off = readLenPrefixed(data, off)
		job.Queue, off = readLenPrefixed(data, off)

		job.Attempt = int(binary.LittleEndian.Uint16(data[off:]))
		off += 2
		job.MaxRetries = int(binary.LittleEndian.Uint16(data[off:]))
		off += 2

		var checkpoint, tags []byte
		checkpoint, off = readLenPrefixedBytes(data, off)
		tags, off = readLenPrefixedBytes(data, off)
		if len(checkpoint) > 0 {
			job.Checkpoint = json.RawMessage(checkpoint)
		}
		if len(tags) > 0 {
			job.Tags = json.RawMessage(tags)
		}

		payloadLen := int(binary.LittleEndian.Uint16(data[off:]))
		off += 2
		payload := make([]byte, payloadLen)
		copy(payload, data[off:off+payloadLen])
		off += payloadLen
		if payloadLen > 0 {
			job.Payload = json.RawMessage(payload)
		}

		jobs[i] = job
	}

	return jobs, nil
}

// encodeAckSection encodes ack jobs into buf at the given offset.
//
// Wire format per ack:
//
//	[job_id:lenPrefixed][queue:lenPrefixed][ack_status:u8][flags:u8]
//	if flags & 0x01: [result:lenPrefixed]
//	if flags & 0x02: [checkpoint:lenPrefixed]
//	if flags & 0x04: [hold_reason:lenPrefixed]
func encodeAckSection(buf []byte, off int, acks []AckJob) int {
	for i := range acks {
		a := &acks[i]
		off = putLenPrefixed(buf, off, a.JobID)
		off = putLenPrefixed(buf, off, a.Queue)
		buf[off] = a.AckStatus
		off++

		var flags uint8
		if a.Result != "" {
			flags |= 0x01
		}
		if a.Checkpoint != "" {
			flags |= 0x02
		}
		if a.HoldReason != "" {
			flags |= 0x04
		}
		buf[off] = flags
		off++

		if flags&0x01 != 0 {
			off = putLenPrefixed(buf, off, a.Result)
		}
		if flags&0x02 != 0 {
			off = putLenPrefixed(buf, off, a.Checkpoint)
		}
		if flags&0x04 != 0 {
			off = putLenPrefixed(buf, off, a.HoldReason)
		}
	}
	return off
}

// ackSectionSize estimates the byte size for encoding a slice of AckJob.
func ackSectionSize(acks []AckJob) int {
	n := 0
	for i := range acks {
		a := &acks[i]
		n += 1 + len(a.JobID) + 1 + len(a.Queue) + 1 + 1 // id + queue + status + flags
		if a.Result != "" {
			n += 1 + len(a.Result)
		}
		if a.Checkpoint != "" {
			n += 1 + len(a.Checkpoint)
		}
		if a.HoldReason != "" {
			n += 1 + len(a.HoldReason)
		}
	}
	return n
}

// AckBatch acknowledges a batch of jobs.
//
// Wire format:
//
//	[now_ns:u64][count:u16]
//	per ack: [job_id:lenPrefixed][queue:lenPrefixed][ack_status:u8][flags:u8]
//	         if flags & 0x01: [result:lenPrefixed]
//	         if flags & 0x02: [checkpoint:lenPrefixed]
//	         if flags & 0x04: [hold_reason:lenPrefixed]
func (c *Conn) AckBatch(acks []AckJob) error {
	count := len(acks)
	if count == 0 {
		return nil
	}
	if count > 65535 {
		return fmt.Errorf("batch size %d exceeds u16 max", count)
	}

	estSize := 8 + 2 + ackSectionSize(acks)

	buf := make([]byte, estSize)
	off := 0

	binary.LittleEndian.PutUint64(buf[off:], uint64(time.Now().UnixNano()))
	off += 8
	binary.LittleEndian.PutUint16(buf[off:], uint16(count))
	off += 2

	off = encodeAckSection(buf, off, acks)

	respType, respPayload, err := c.sendAndRecv(msgAckBatch, buf[:off])
	if err != nil {
		return err
	}
	if respType != msgAckBatchResp {
		return fmt.Errorf("unexpected response type 0x%02x for AckBatch", respType)
	}

	// Response: [affected:u16][err_code:u8]
	if len(respPayload) < 3 {
		return fmt.Errorf("AckBatch response too short: %d bytes", len(respPayload))
	}
	errCode := respPayload[2]
	if errCode != 0 {
		return fmt.Errorf("AckBatch error code %d", errCode)
	}
	return nil
}

// FailBatch marks a batch of jobs as failed.
//
// Wire format:
//
//	[now_ns:u64][count:u16]
//	per job: [id:lenPrefixed][queue:lenPrefixed][error:lenPrefixed][backtrace:lenPrefixed]
func (c *Conn) FailBatch(jobs []FailJob) error {
	count := len(jobs)
	if count == 0 {
		return nil
	}
	if count > 65535 {
		return fmt.Errorf("batch size %d exceeds u16 max", count)
	}

	// Estimate payload size.
	estSize := 8 + 2 // now_ns + count
	for i := range jobs {
		j := &jobs[i]
		estSize += 1 + len(j.JobID) + 1 + len(j.Queue) + 1 + len(j.Error) + 1 + len(j.Backtrace)
	}

	buf := make([]byte, estSize)
	off := 0

	binary.LittleEndian.PutUint64(buf[off:], uint64(time.Now().UnixNano()))
	off += 8
	binary.LittleEndian.PutUint16(buf[off:], uint16(count))
	off += 2

	for i := range jobs {
		j := &jobs[i]
		off = putLenPrefixed(buf, off, j.JobID)
		off = putLenPrefixed(buf, off, j.Queue)
		off = putLenPrefixed(buf, off, j.Error)
		off = putLenPrefixed(buf, off, j.Backtrace)
	}

	respType, respPayload, err := c.sendAndRecv(msgFailBatch, buf[:off])
	if err != nil {
		return err
	}
	if respType != msgFailBatchResp {
		return fmt.Errorf("unexpected response type 0x%02x for FailBatch", respType)
	}

	// Response: [affected:u16][err_code:u8]
	if len(respPayload) < 3 {
		return fmt.Errorf("FailBatch response too short: %d bytes", len(respPayload))
	}
	errCode := respPayload[2]
	if errCode != 0 {
		return fmt.Errorf("FailBatch error code %d", errCode)
	}
	return nil
}

// HeartbeatBatch sends heartbeats for a batch of active jobs.
//
// Wire format:
//
//	[worker_id:lenPrefixed][count:u16]
//	per job: [job_id:lenPrefixed][queue:lenPrefixed][flags:u8]
//	         if flags & 0x01: [progress:lenPrefixed]
//	         if flags & 0x02: [checkpoint:lenPrefixed]
func (c *Conn) HeartbeatBatch(workerID string, jobs []HeartbeatJob) error {
	count := len(jobs)
	if count == 0 {
		return nil
	}
	if count > 65535 {
		return fmt.Errorf("batch size %d exceeds u16 max", count)
	}

	// Estimate payload size.
	estSize := 1 + len(workerID) + 2
	for i := range jobs {
		j := &jobs[i]
		estSize += 1 + len(j.JobID) + 1 + len(j.Queue) + 1 // id + queue + flags
		if j.Progress != "" {
			estSize += 1 + len(j.Progress)
		}
		if j.Checkpoint != "" {
			estSize += 1 + len(j.Checkpoint)
		}
	}

	buf := make([]byte, estSize)
	off := 0

	off = putLenPrefixed(buf, off, workerID)
	binary.LittleEndian.PutUint16(buf[off:], uint16(count))
	off += 2

	for i := range jobs {
		j := &jobs[i]
		off = putLenPrefixed(buf, off, j.JobID)
		off = putLenPrefixed(buf, off, j.Queue)

		var flags uint8
		if j.Progress != "" {
			flags |= 0x01
		}
		if j.Checkpoint != "" {
			flags |= 0x02
		}
		buf[off] = flags
		off++

		if flags&0x01 != 0 {
			off = putLenPrefixed(buf, off, j.Progress)
		}
		if flags&0x02 != 0 {
			off = putLenPrefixed(buf, off, j.Checkpoint)
		}
	}

	respType, respPayload, err := c.sendAndRecv(msgHeartbeat, buf[:off])
	if err != nil {
		return err
	}
	if respType != msgHeartbeatResp {
		return fmt.Errorf("unexpected response type 0x%02x for HeartbeatBatch", respType)
	}

	// Response: [affected:u16][err_code:u8]
	if len(respPayload) < 3 {
		return fmt.Errorf("HeartbeatBatch response too short: %d bytes", len(respPayload))
	}
	errCode := respPayload[2]
	if errCode != 0 {
		return fmt.Errorf("HeartbeatBatch error code %d", errCode)
	}
	return nil
}
