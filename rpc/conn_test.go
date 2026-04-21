package rpc

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net"
	"testing"
)

// mockServer creates a TCP listener that handles one connection.
// The handler receives the raw connection to read/write frames.
func mockServer(t *testing.T, handler func(net.Conn)) (string, int, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().(*net.TCPAddr)

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		handler(conn)
	}()

	return addr.IP.String(), addr.Port, func() { ln.Close() }
}

// readFrame reads a complete frame from conn and returns msgType, reqID, payload.
func readFrame(t *testing.T, conn net.Conn) (uint8, uint32, []byte) {
	t.Helper()
	hdr := make([]byte, headerSize)
	if _, err := io.ReadFull(conn, hdr); err != nil {
		t.Fatalf("readFrame header: %v", err)
	}
	msgType := hdr[0]
	reqID := binary.LittleEndian.Uint32(hdr[1:5])
	payloadLen := binary.LittleEndian.Uint32(hdr[5:9])

	var payload []byte
	if payloadLen > 0 {
		payload = make([]byte, payloadLen)
		if _, err := io.ReadFull(conn, payload); err != nil {
			t.Fatalf("readFrame payload: %v", err)
		}
	}
	return msgType, reqID, payload
}

// writeFrame writes a response frame to conn.
func writeFrame(t *testing.T, conn net.Conn, msgType uint8, reqID uint32, payload []byte) {
	t.Helper()
	hdr := make([]byte, headerSize)
	writeHeader(hdr, msgType, reqID, uint32(len(payload)))
	if _, err := conn.Write(hdr); err != nil {
		t.Fatalf("writeFrame header: %v", err)
	}
	if len(payload) > 0 {
		if _, err := conn.Write(payload); err != nil {
			t.Fatalf("writeFrame payload: %v", err)
		}
	}
}

func TestPing(t *testing.T) {
	host, port, cleanup := mockServer(t, func(conn net.Conn) {
		msgType, reqID, payload := readFrame(t, conn)
		if msgType != msgPing {
			t.Errorf("expected msgPing (0x%02x), got 0x%02x", msgPing, msgType)
		}
		if len(payload) != 0 {
			t.Errorf("expected empty payload, got %d bytes", len(payload))
		}
		writeFrame(t, conn, msgPingResp, reqID, nil)
	})
	defer cleanup()

	c := NewConn(host, port)
	defer c.Close()

	if err := c.Ping(); err != nil {
		t.Fatalf("Ping failed: %v", err)
	}
}

func TestEnqueueBatch(t *testing.T) {
	host, port, cleanup := mockServer(t, func(conn net.Conn) {
		msgType, reqID, payload := readFrame(t, conn)
		if msgType != msgEnqueueBatch {
			t.Errorf("expected msgEnqueueBatch, got 0x%02x", msgType)
		}

		// Parse the request payload.
		off := 0
		count := binary.LittleEndian.Uint16(payload[off:])
		off += 2
		if count != 2 {
			t.Errorf("expected count=2, got %d", count)
		}

		// Read job 1: queue, id, priority, max_retries, backoff, base_delay_ms,
		// max_delay_ms, unique_period_s, scheduled_at_ns, expire_after_ms,
		// chain_step, flags.
		queue1, off := readLenPrefixed(payload, off)
		id1, off := readLenPrefixed(payload, off)
		priority1 := payload[off]
		off++
		maxRetries1 := binary.LittleEndian.Uint16(payload[off:])
		off += 2
		off++ // backoff
		off += 4 // base_delay_ms
		off += 4 // max_delay_ms
		off += 4 // unique_period_s
		off += 8 // scheduled_at_ns
		off += 4 // expire_after_ms
		off += 2 // chain_step
		flags1 := binary.LittleEndian.Uint16(payload[off:])
		off += 2

		if queue1 != "emails" {
			t.Errorf("job1 queue = %q, want %q", queue1, "emails")
		}
		if id1 != "job-a" {
			t.Errorf("job1 id = %q, want %q", id1, "job-a")
		}
		if priority1 != 3 {
			t.Errorf("job1 priority = %d, want 3", priority1)
		}
		if maxRetries1 != 5 {
			t.Errorf("job1 maxRetries = %d, want 5", maxRetries1)
		}
		if flags1 != 0 {
			t.Errorf("job1 flags = 0x%04x, want 0", flags1)
		}

		// Read job 2.
		queue2, off := readLenPrefixed(payload, off)
		id2, off := readLenPrefixed(payload, off)
		_ = off

		if queue2 != "emails" {
			t.Errorf("job2 queue = %q, want %q", queue2, "emails")
		}
		if id2 != "job-b" {
			t.Errorf("job2 id = %q, want %q", id2, "job-b")
		}

		// Send response: count=2, err_code=0.
		resp := make([]byte, 3)
		binary.LittleEndian.PutUint16(resp[0:], 2)
		resp[2] = 0
		writeFrame(t, conn, msgEnqueueBatchResp, reqID, resp)
	})
	defer cleanup()

	c := NewConn(host, port)
	defer c.Close()

	n, err := c.EnqueueBatch([]EnqueueJob{
		{Queue: "emails", JobID: "job-a", Priority: 3, MaxRetries: 5},
		{Queue: "emails", JobID: "job-b", Priority: 3, MaxRetries: 5},
	})
	if err != nil {
		t.Fatalf("EnqueueBatch failed: %v", err)
	}
	if n != 2 {
		t.Fatalf("expected 2 enqueued, got %d", n)
	}
}

func TestEnqueueBatchWithPayload(t *testing.T) {
	host, port, cleanup := mockServer(t, func(conn net.Conn) {
		msgType, reqID, payload := readFrame(t, conn)
		if msgType != msgEnqueueBatch {
			t.Errorf("expected msgEnqueueBatch, got 0x%02x", msgType)
		}

		off := 0
		count := binary.LittleEndian.Uint16(payload[off:])
		off += 2
		if count != 1 {
			t.Errorf("expected count=1, got %d", count)
		}

		// queue, id
		_, off = readLenPrefixed(payload, off)
		_, off = readLenPrefixed(payload, off)
		off++     // priority
		off += 2  // max_retries
		off++     // backoff
		off += 4  // base_delay_ms
		off += 4  // max_delay_ms
		off += 4  // unique_period_s
		off += 8  // scheduled_at_ns
		off += 4  // expire_after_ms
		off += 2  // chain_step
		flags := binary.LittleEndian.Uint16(payload[off:])
		off += 2

		if flags&0x0001 == 0 {
			t.Errorf("expected payload flag set, flags=0x%04x", flags)
		}

		// Read u16-prefixed payload.
		payloadLen := int(binary.LittleEndian.Uint16(payload[off:]))
		off += 2
		jobPayload := string(payload[off : off+payloadLen])
		if jobPayload != `{"key":"val"}` {
			t.Errorf("payload = %q, want %q", jobPayload, `{"key":"val"}`)
		}

		resp := make([]byte, 3)
		binary.LittleEndian.PutUint16(resp[0:], 1)
		resp[2] = 0
		writeFrame(t, conn, msgEnqueueBatchResp, reqID, resp)
	})
	defer cleanup()

	c := NewConn(host, port)
	defer c.Close()

	n, err := c.EnqueueBatch([]EnqueueJob{
		{Queue: "q", JobID: "j1", Payload: []byte(`{"key":"val"}`)},
	})
	if err != nil {
		t.Fatalf("EnqueueBatch failed: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected 1 enqueued, got %d", n)
	}
}

func TestSubscribeAndReadPushedJobs(t *testing.T) {
	host, port, cleanup := mockServer(t, func(conn net.Conn) {
		msgType, reqID, payload := readFrame(t, conn)
		if msgType != msgFetchBatch {
			t.Errorf("expected msgFetchBatch (0x%02x), got 0x%02x", msgFetchBatch, msgType)
		}

		// Parse request: count(2) + lease_ms(4) + worker_id(lenPrefixed) + queue_count(1) + queues.
		off := 0
		credits := binary.LittleEndian.Uint16(payload[off:])
		off += 2
		if credits != 10 {
			t.Errorf("credits = %d, want 10", credits)
		}
		off += 4 // skip lease_ms
		workerID, off := readLenPrefixed(payload, off)
		if workerID != "w1" {
			t.Errorf("worker_id = %q, want %q", workerID, "w1")
		}
		queueCount := payload[off]
		off++
		if queueCount != 2 {
			t.Errorf("queue_count = %d, want 2", queueCount)
		}
		q1, off := readLenPrefixed(payload, off)
		q2, _ := readLenPrefixed(payload, off)
		if q1 != "default" || q2 != "urgent" {
			t.Errorf("queues = [%q, %q], want [default, urgent]", q1, q2)
		}

		// Server pushes a response asynchronously with 1 job.
		resp := make([]byte, 256)
		roff := 0
		binary.LittleEndian.PutUint16(resp[roff:], 1)
		roff += 2

		roff = putLenPrefixed(resp, roff, "job-123")  // id
		roff = putLenPrefixed(resp, roff, "default")   // queue
		binary.LittleEndian.PutUint16(resp[roff:], 1)  // attempt
		roff += 2
		binary.LittleEndian.PutUint16(resp[roff:], 3)  // max_retries
		roff += 2
		roff = putLenPrefixedBytes(resp, roff, nil)    // checkpoint
		roff = putLenPrefixedBytes(resp, roff, nil)    // tags
		payloadBytes := []byte(`{"key":"value"}`)
		binary.LittleEndian.PutUint16(resp[roff:], uint16(len(payloadBytes)))
		roff += 2
		copy(resp[roff:], payloadBytes)
		roff += len(payloadBytes)
		binary.LittleEndian.PutUint64(resp[roff:], 0xDEADBEEF42) // lease_token
		roff += 8

		writeFrame(t, conn, msgFetchBatchResp, reqID, resp[:roff])
	})
	defer cleanup()

	c := NewConn(host, port)
	defer c.Close()

	// Subscribe (fire-and-forget, no response expected).
	if err := c.Subscribe([]string{"default", "urgent"}, "w1", 10); err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	// Read the pushed jobs.
	jobs, err := c.ReadPushedJobs()
	if err != nil {
		t.Fatalf("ReadPushedJobs failed: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("expected 1 job, got %d", len(jobs))
	}
	if jobs[0].JobID != "job-123" {
		t.Errorf("job.JobID = %q, want %q", jobs[0].JobID, "job-123")
	}
	if jobs[0].Queue != "default" {
		t.Errorf("job.Queue = %q, want %q", jobs[0].Queue, "default")
	}
	if jobs[0].Attempt != 1 {
		t.Errorf("job.Attempt = %d, want 1", jobs[0].Attempt)
	}
	if jobs[0].MaxRetries != 3 {
		t.Errorf("job.MaxRetries = %d, want 3", jobs[0].MaxRetries)
	}
	if jobs[0].LeaseToken != 0xDEADBEEF42 {
		t.Errorf("job.LeaseToken = 0x%x, want 0xDEADBEEF42", jobs[0].LeaseToken)
	}

	var p map[string]string
	if err := json.Unmarshal(jobs[0].Payload, &p); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if p["key"] != "value" {
		t.Errorf("payload[key] = %q, want %q", p["key"], "value")
	}
}

func TestAckBatch(t *testing.T) {
	host, port, cleanup := mockServer(t, func(conn net.Conn) {
		msgType, reqID, payload := readFrame(t, conn)
		if msgType != msgAckBatch {
			t.Errorf("expected msgAckBatch, got 0x%02x", msgType)
		}

		off := 0
		count := binary.LittleEndian.Uint16(payload[off:])
		off += 2
		if count != 2 {
			t.Errorf("ack count = %d, want 2", count)
		}

		// Ack 1.
		id1, off := readLenPrefixed(payload, off)
		q1, off := readLenPrefixed(payload, off)
		status1 := payload[off]
		off++
		flags1 := payload[off]
		off++
		if id1 != "j1" || q1 != "work" {
			t.Errorf("ack1 data mismatch: id=%q queue=%q", id1, q1)
		}
		if status1 != AckDone {
			t.Errorf("ack1 status = %d, want AckDone", status1)
		}
		if flags1 != 0x08 {
			t.Errorf("ack1 flags = 0x%02x, want 0x08", flags1)
		}
		lt1 := binary.LittleEndian.Uint64(payload[off:])
		off += 8
		if lt1 != 0xABCD1234 {
			t.Errorf("ack1 lease_token = 0x%x, want 0xABCD1234", lt1)
		}

		// Ack 2.
		id2, off := readLenPrefixed(payload, off)
		q2, off := readLenPrefixed(payload, off)
		status2 := payload[off]
		off++
		flags2 := payload[off]
		off++
		if id2 != "j2" || q2 != "work" {
			t.Errorf("ack2 data mismatch: id=%q queue=%q", id2, q2)
		}
		if status2 != AckDone {
			t.Errorf("ack2 status = %d, want AckDone", status2)
		}
		if flags2 != 0x08 {
			t.Errorf("ack2 flags = 0x%02x, want 0x08", flags2)
		}
		lt2 := binary.LittleEndian.Uint64(payload[off:])
		off += 8
		if lt2 != 0x5678EFAB {
			t.Errorf("ack2 lease_token = 0x%x, want 0x5678EFAB", lt2)
		}

		resp := make([]byte, 3)
		binary.LittleEndian.PutUint16(resp[0:], 2)
		resp[2] = 0
		writeFrame(t, conn, msgAckBatchResp, reqID, resp)
	})
	defer cleanup()

	c := NewConn(host, port)
	defer c.Close()

	err := c.AckBatch([]AckJob{
		{JobID: "j1", Queue: "work", AckStatus: AckDone, LeaseToken: 0xABCD1234},
		{JobID: "j2", Queue: "work", AckStatus: AckDone, LeaseToken: 0x5678EFAB},
	})
	if err != nil {
		t.Fatalf("AckBatch failed: %v", err)
	}
}

func TestAckBatchWithOptionalFields(t *testing.T) {
	host, port, cleanup := mockServer(t, func(conn net.Conn) {
		msgType, reqID, payload := readFrame(t, conn)
		if msgType != msgAckBatch {
			t.Errorf("expected msgAckBatch, got 0x%02x", msgType)
		}

		off := 0
		count := binary.LittleEndian.Uint16(payload[off:])
		off += 2
		if count != 1 {
			t.Errorf("ack count = %d, want 1", count)
		}

		id, off := readLenPrefixed(payload, off)
		queue, off := readLenPrefixed(payload, off)
		status := payload[off]
		off++
		flags := payload[off]
		off++

		if id != "j1" || queue != "work" {
			t.Errorf("ack data mismatch: id=%q queue=%q", id, queue)
		}
		if status != AckHold {
			t.Errorf("ack status = %d, want AckHold", status)
		}
		if flags != 0x0D { // result(0x01) + hold_reason(0x04) + lease_token(0x08)
			t.Errorf("ack flags = 0x%02x, want 0x0D", flags)
		}

		// Read result (flag 0x01).
		result, off := readLenPrefixed(payload, off)
		if result != `{"ok":true}` {
			t.Errorf("result = %q", result)
		}
		// Read hold_reason (flag 0x04).
		holdReason, off := readLenPrefixed(payload, off)
		if holdReason != "needs review" {
			t.Errorf("hold_reason = %q", holdReason)
		}
		// Read lease_token (flag 0x08).
		lt := binary.LittleEndian.Uint64(payload[off:])
		off += 8
		if lt != 0x999 {
			t.Errorf("lease_token = 0x%x, want 0x999", lt)
		}

		resp := make([]byte, 3)
		binary.LittleEndian.PutUint16(resp[0:], 1)
		resp[2] = 0
		writeFrame(t, conn, msgAckBatchResp, reqID, resp)
	})
	defer cleanup()

	c := NewConn(host, port)
	defer c.Close()

	err := c.AckBatch([]AckJob{
		{JobID: "j1", Queue: "work", AckStatus: AckHold, Result: `{"ok":true}`, HoldReason: "needs review", LeaseToken: 0x999},
	})
	if err != nil {
		t.Fatalf("AckBatch failed: %v", err)
	}
}

func TestFailBatch(t *testing.T) {
	host, port, cleanup := mockServer(t, func(conn net.Conn) {
		msgType, reqID, payload := readFrame(t, conn)
		if msgType != msgFailBatch {
			t.Errorf("expected msgFailBatch, got 0x%02x", msgType)
		}

		off := 0
		count := binary.LittleEndian.Uint16(payload[off:])
		off += 2
		if count != 1 {
			t.Errorf("fail count = %d, want 1", count)
		}

		id, off := readLenPrefixed(payload, off)
		queue, off := readLenPrefixed(payload, off)
		errMsg, off := readLenPrefixed(payload, off)
		backtrace, off := readLenPrefixed(payload, off)

		if id != "j1" || queue != "work" || errMsg != "boom" || backtrace != "" {
			t.Errorf("fail data: id=%q queue=%q err=%q bt=%q", id, queue, errMsg, backtrace)
		}

		// Read flags byte.
		flags := payload[off]
		off++
		if flags != 0x01 {
			t.Errorf("fail flags = 0x%02x, want 0x01", flags)
		}
		// Read lease_token.
		lt := binary.LittleEndian.Uint64(payload[off:])
		off += 8
		if lt != 0xFEED {
			t.Errorf("fail lease_token = 0x%x, want 0xFEED", lt)
		}

		resp := make([]byte, 3)
		binary.LittleEndian.PutUint16(resp[0:], 1)
		resp[2] = 0
		writeFrame(t, conn, msgFailBatchResp, reqID, resp)
	})
	defer cleanup()

	c := NewConn(host, port)
	defer c.Close()

	err := c.FailBatch([]FailJob{
		{JobID: "j1", Queue: "work", Error: "boom", LeaseToken: 0xFEED},
	})
	if err != nil {
		t.Fatalf("FailBatch failed: %v", err)
	}
}

func TestFailBatchNoLeaseToken(t *testing.T) {
	host, port, cleanup := mockServer(t, func(conn net.Conn) {
		msgType, reqID, payload := readFrame(t, conn)
		if msgType != msgFailBatch {
			t.Errorf("expected msgFailBatch, got 0x%02x", msgType)
		}

		off := 0
		count := binary.LittleEndian.Uint16(payload[off:])
		off += 2
		if count != 1 {
			t.Errorf("fail count = %d, want 1", count)
		}

		id, off := readLenPrefixed(payload, off)
		queue, off := readLenPrefixed(payload, off)
		errMsg, off := readLenPrefixed(payload, off)
		backtrace, off := readLenPrefixed(payload, off)

		if id != "j1" || queue != "work" || errMsg != "oops" || backtrace != "" {
			t.Errorf("fail data: id=%q queue=%q err=%q bt=%q", id, queue, errMsg, backtrace)
		}

		// Read flags byte — should be 0 (no lease_token).
		flags := payload[off]
		off++
		if flags != 0 {
			t.Errorf("fail flags = 0x%02x, want 0x00", flags)
		}

		resp := make([]byte, 3)
		binary.LittleEndian.PutUint16(resp[0:], 1)
		resp[2] = 0
		writeFrame(t, conn, msgFailBatchResp, reqID, resp)
	})
	defer cleanup()

	c := NewConn(host, port)
	defer c.Close()

	err := c.FailBatch([]FailJob{
		{JobID: "j1", Queue: "work", Error: "oops"},
	})
	if err != nil {
		t.Fatalf("FailBatch failed: %v", err)
	}
}

func TestReadPushedJobsError(t *testing.T) {
	host, port, cleanup := mockServer(t, func(conn net.Conn) {
		// Read the subscribe frame (client sends it, we just consume).
		readFrame(t, conn)

		// Push an error frame.
		writeFrame(t, conn, msgError, 1, []byte("queue not found"))
	})
	defer cleanup()

	c := NewConn(host, port)
	defer c.Close()

	if err := c.Subscribe([]string{"missing"}, "w1", 5); err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	_, err := c.ReadPushedJobs()
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var serverErr *ServerError
	if !errors.As(err, &serverErr) {
		t.Fatalf("expected ServerError, got %T: %v", err, err)
	}
	if serverErr.Message != "queue not found" {
		t.Errorf("error message = %q, want %q", serverErr.Message, "queue not found")
	}
}

func TestHeartbeatBatch(t *testing.T) {
	host, port, cleanup := mockServer(t, func(conn net.Conn) {
		msgType, reqID, payload := readFrame(t, conn)
		if msgType != msgHeartbeat {
			t.Errorf("expected msgHeartbeat (0x%02x), got 0x%02x", msgHeartbeat, msgType)
		}

		off := 0
		workerID, off := readLenPrefixed(payload, off)
		if workerID != "w1" {
			t.Errorf("worker_id = %q, want %q", workerID, "w1")
		}
		count := binary.LittleEndian.Uint16(payload[off:])
		off += 2
		if count != 2 {
			t.Errorf("count = %d, want 2", count)
		}

		// Job 1: no optional fields.
		id1, off := readLenPrefixed(payload, off)
		q1, off := readLenPrefixed(payload, off)
		flags1 := payload[off]
		off++
		if id1 != "j1" || q1 != "work" {
			t.Errorf("job1: id=%q queue=%q", id1, q1)
		}
		if flags1 != 0 {
			t.Errorf("job1 flags = 0x%02x, want 0", flags1)
		}

		// Job 2: with progress.
		id2, off := readLenPrefixed(payload, off)
		q2, off := readLenPrefixed(payload, off)
		flags2 := payload[off]
		off++
		if id2 != "j2" || q2 != "work" {
			t.Errorf("job2: id=%q queue=%q", id2, q2)
		}
		if flags2 != 0x01 {
			t.Errorf("job2 flags = 0x%02x, want 0x01", flags2)
		}
		progress, _ := readLenPrefixed(payload, off)
		if progress != `{"pct":50}` {
			t.Errorf("job2 progress = %q", progress)
		}

		resp := make([]byte, 3)
		binary.LittleEndian.PutUint16(resp[0:], 2)
		resp[2] = 0
		writeFrame(t, conn, msgHeartbeatResp, reqID, resp)
	})
	defer cleanup()

	c := NewConn(host, port)
	defer c.Close()

	err := c.HeartbeatBatch("w1", []HeartbeatJob{
		{JobID: "j1", Queue: "work"},
		{JobID: "j2", Queue: "work", Progress: `{"pct":50}`},
	})
	if err != nil {
		t.Fatalf("HeartbeatBatch failed: %v", err)
	}
}

func TestErrorResponse(t *testing.T) {
	host, port, cleanup := mockServer(t, func(conn net.Conn) {
		_, reqID, _ := readFrame(t, conn)
		writeFrame(t, conn, msgError, reqID, []byte("something went wrong"))
	})
	defer cleanup()

	c := NewConn(host, port)
	defer c.Close()

	err := c.Ping()
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if err.Error() != "rpc error: something went wrong" {
		t.Errorf("error = %q", err.Error())
	}
}

func TestEnqueueBatchEmpty(t *testing.T) {
	c := NewConn("127.0.0.1", 1) // no server needed
	defer c.Close()

	n, err := c.EnqueueBatch(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 0 {
		t.Fatalf("expected 0, got %d", n)
	}
}

func TestAckBatchEmpty(t *testing.T) {
	c := NewConn("127.0.0.1", 1)
	defer c.Close()

	err := c.AckBatch(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestFailBatchEmpty(t *testing.T) {
	c := NewConn("127.0.0.1", 1)
	defer c.Close()

	err := c.FailBatch(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestParseFetchResponseMultipleJobs(t *testing.T) {
	// Build a response with 2 jobs.
	buf := make([]byte, 512)
	off := 0
	binary.LittleEndian.PutUint16(buf[off:], 2)
	off += 2

	// Job 1.
	off = putLenPrefixed(buf, off, "j1")
	off = putLenPrefixed(buf, off, "q1")
	binary.LittleEndian.PutUint16(buf[off:], 1) // attempt
	off += 2
	binary.LittleEndian.PutUint16(buf[off:], 3) // max_retries
	off += 2
	off = putLenPrefixedBytes(buf, off, []byte(`{"step":1}`)) // checkpoint
	off = putLenPrefixedBytes(buf, off, []byte(`{"env":"prod"}`)) // tags
	p1 := []byte(`{"action":"build"}`)
	binary.LittleEndian.PutUint16(buf[off:], uint16(len(p1)))
	off += 2
	copy(buf[off:], p1)
	off += len(p1)
	binary.LittleEndian.PutUint64(buf[off:], 111) // lease_token
	off += 8

	// Job 2.
	off = putLenPrefixed(buf, off, "j2")
	off = putLenPrefixed(buf, off, "q2")
	binary.LittleEndian.PutUint16(buf[off:], 4)  // attempt
	off += 2
	binary.LittleEndian.PutUint16(buf[off:], 10) // max_retries
	off += 2
	off = putLenPrefixedBytes(buf, off, nil) // no checkpoint
	off = putLenPrefixedBytes(buf, off, nil) // no tags
	binary.LittleEndian.PutUint16(buf[off:], 0)  // no payload
	off += 2
	binary.LittleEndian.PutUint64(buf[off:], 222) // lease_token
	off += 8

	jobs, err := parseFetchResponse(buf[:off])
	if err != nil {
		t.Fatalf("parseFetchResponse: %v", err)
	}
	if len(jobs) != 2 {
		t.Fatalf("expected 2 jobs, got %d", len(jobs))
	}

	// Job 1 checks.
	if jobs[0].JobID != "j1" {
		t.Errorf("job[0].JobID = %q", jobs[0].JobID)
	}
	if jobs[0].Attempt != 1 || jobs[0].MaxRetries != 3 {
		t.Errorf("job[0] attempt=%d maxRetries=%d", jobs[0].Attempt, jobs[0].MaxRetries)
	}
	if string(jobs[0].Checkpoint) != `{"step":1}` {
		t.Errorf("job[0].Checkpoint = %q", jobs[0].Checkpoint)
	}
	if string(jobs[0].Tags) != `{"env":"prod"}` {
		t.Errorf("job[0].Tags = %q", jobs[0].Tags)
	}
	if string(jobs[0].Payload) != `{"action":"build"}` {
		t.Errorf("job[0].Payload = %q", jobs[0].Payload)
	}
	if jobs[0].LeaseToken != 111 {
		t.Errorf("job[0].LeaseToken = %d, want 111", jobs[0].LeaseToken)
	}

	// Job 2 checks.
	if jobs[1].JobID != "j2" || jobs[1].Queue != "q2" {
		t.Errorf("job[1] id=%q queue=%q", jobs[1].JobID, jobs[1].Queue)
	}
	if jobs[1].Attempt != 4 || jobs[1].MaxRetries != 10 {
		t.Errorf("job[1] attempt=%d maxRetries=%d", jobs[1].Attempt, jobs[1].MaxRetries)
	}
	if jobs[1].Checkpoint != nil {
		t.Errorf("job[1].Checkpoint should be nil, got %q", jobs[1].Checkpoint)
	}
	if jobs[1].Payload != nil {
		t.Errorf("job[1].Payload should be nil, got %q", jobs[1].Payload)
	}
	if jobs[1].LeaseToken != 222 {
		t.Errorf("job[1].LeaseToken = %d, want 222", jobs[1].LeaseToken)
	}
}
