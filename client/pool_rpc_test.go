package client

import (
	"encoding/binary"
	"io"
	"net"
	"testing"
)

// Wire message types used by the binary RPC mock below.
const (
	rpcHeaderSize    = 9
	rpcMsgFetchBatch = 0x02
	rpcMsgFetchResp  = 0x82
	rpcMsgError      = 0xFF
)

func readRPCFrame(t *testing.T, conn net.Conn) (uint8, uint32, []byte) {
	t.Helper()
	hdr := make([]byte, rpcHeaderSize)
	if _, err := io.ReadFull(conn, hdr); err != nil {
		t.Fatalf("read header: %v", err)
	}
	msgType := hdr[0]
	reqID := binary.LittleEndian.Uint32(hdr[1:5])
	payloadLen := binary.LittleEndian.Uint32(hdr[5:9])
	var payload []byte
	if payloadLen > 0 {
		payload = make([]byte, payloadLen)
		if _, err := io.ReadFull(conn, payload); err != nil {
			t.Fatalf("read payload: %v", err)
		}
	}
	return msgType, reqID, payload
}

func writeRPCFrame(t *testing.T, conn net.Conn, msgType uint8, reqID uint32, payload []byte) {
	t.Helper()
	hdr := make([]byte, rpcHeaderSize)
	hdr[0] = msgType
	binary.LittleEndian.PutUint32(hdr[1:5], reqID)
	binary.LittleEndian.PutUint32(hdr[5:9], uint32(len(payload)))
	if _, err := conn.Write(hdr); err != nil {
		t.Fatalf("write header: %v", err)
	}
	if len(payload) > 0 {
		if _, err := conn.Write(payload); err != nil {
			t.Fatalf("write payload: %v", err)
		}
	}
}

// TestFetchRPCReSubscribesAfterRejection proves the pooled FetchRPC backs off
// and re-subscribes when the server rejects the first subscribe with MSG_ERROR
// (server at connection capacity), then returns the jobs served on the retry.
func TestFetchRPCReSubscribesAfterRejection(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	addr := ln.Addr().(*net.TCPAddr)

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		// First subscribe: reject with MSG_ERROR (retryable).
		msgType, reqID, _ := readRPCFrame(t, conn)
		if msgType != rpcMsgFetchBatch {
			t.Errorf("frame 1: expected msgFetchBatch, got 0x%02x", msgType)
		}
		writeRPCFrame(t, conn, rpcMsgError, reqID, []byte("subscription rejected: server at connection capacity"))

		// Second subscribe (after backoff): serve one job.
		_, reqID2, _ := readRPCFrame(t, conn)
		resp := make([]byte, 128)
		roff := 0
		binary.LittleEndian.PutUint16(resp[roff:], 1) // count
		roff += 2
		resp[roff] = 2 // id length
		roff++
		copy(resp[roff:], "j1")
		roff += 2
		resp[roff] = 7 // queue length
		roff++
		copy(resp[roff:], "default")
		roff += 7
		binary.LittleEndian.PutUint16(resp[roff:], 1) // attempt
		roff += 2
		binary.LittleEndian.PutUint16(resp[roff:], 3) // max_retries
		roff += 2
		resp[roff] = 0 // checkpoint length
		roff++
		resp[roff] = 0 // tags length
		roff++
		binary.LittleEndian.PutUint32(resp[roff:], 0) // payload_len (u32)
		roff += 4
		binary.LittleEndian.PutUint64(resp[roff:], 42) // lease_token
		roff += 8
		writeRPCFrame(t, conn, rpcMsgFetchResp, reqID2, resp[:roff])
	}()

	p := NewPooledClient(PoolOptions{
		BaseURL: "http://127.0.0.1:1",
		Lanes:   1,
		RPCHost: addr.IP.String(),
		RPCPort: addr.Port,
	})
	defer p.Close()

	jobs, err := p.FetchRPC([]string{"default"}, "w1", 1)
	if err != nil {
		t.Fatalf("FetchRPC failed: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("expected 1 job, got %d", len(jobs))
	}
	if jobs[0].JobID != "j1" {
		t.Errorf("job.JobID = %q, want j1", jobs[0].JobID)
	}
	if jobs[0].LeaseToken != 42 {
		t.Errorf("job.LeaseToken = %d, want 42", jobs[0].LeaseToken)
	}
}
