package fluxmq

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
)

// conn is a pipelined TCP connection to a single FluxMQ broker.
// Multiple goroutines may call roundtrip() concurrently.
type conn struct {
	nc      net.Conn
	writeMu sync.Mutex
	corrID  atomic.Uint32
	pending sync.Map      // uint32 -> chan []byte
	closed  chan struct{}
	once    sync.Once
}

// dial opens a TCP connection to addr and starts the read loop.
func dial(addr string) (*conn, error) {
	nc, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	c := &conn{
		nc:     nc,
		closed: make(chan struct{}),
	}
	go c.readLoop()
	return c, nil
}

// roundtrip sends a request and waits for the matching response.
// It is safe to call from multiple goroutines concurrently.
func (c *conn) roundtrip(apiKey uint16, payload []byte) ([]byte, error) {
	// Check if already closed.
	select {
	case <-c.closed:
		return nil, fmt.Errorf("fluxmq: connection closed")
	default:
	}

	id := c.corrID.Add(1)
	// Buffered channel so readLoop never blocks even if the caller has timed out.
	ch := make(chan []byte, 1)
	c.pending.Store(id, ch)
	defer c.pending.Delete(id)

	if err := c.writeFrame(apiKey, id, payload); err != nil {
		return nil, err
	}

	select {
	case body, ok := <-ch:
		if !ok {
			return nil, fmt.Errorf("fluxmq: connection closed while waiting for response")
		}
		return body, nil
	case <-c.closed:
		return nil, fmt.Errorf("fluxmq: connection closed while waiting for response")
	}
}

// writeFrame encodes and sends a request frame on the wire.
// Request wire format: [4B total_len][2B api_key][2B api_version=0][4B corr_id][payload...]
// total_len = 8 + len(payload)  (does NOT include itself)
func (c *conn) writeFrame(apiKey uint16, corrID uint32, payload []byte) error {
	totalLen := uint32(8 + len(payload))
	frame := make([]byte, 4+8+len(payload))
	binary.BigEndian.PutUint32(frame[0:], totalLen)
	binary.BigEndian.PutUint16(frame[4:], apiKey)
	binary.BigEndian.PutUint16(frame[6:], 0) // api_version = 0
	binary.BigEndian.PutUint32(frame[8:], corrID)
	copy(frame[12:], payload)

	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	_, err := c.nc.Write(frame)
	return err
}

// readLoop reads response frames from the broker and routes them to pending channels.
// Response wire format: [4B total_len][4B corr_id][payload...]
// total_len = 4 + len(payload)
func (c *conn) readLoop() {
	defer c.close()

	hdr := make([]byte, 4)
	for {
		// Read the 4-byte length prefix.
		if _, err := io.ReadFull(c.nc, hdr); err != nil {
			return
		}
		totalLen := binary.BigEndian.Uint32(hdr)
		if totalLen < 4 {
			return
		}

		// Read totalLen bytes: [4B corr_id][payload...]
		body := make([]byte, totalLen)
		if _, err := io.ReadFull(c.nc, body); err != nil {
			return
		}

		corrID := binary.BigEndian.Uint32(body[:4])
		payload := body[4:]

		// Route to the waiting caller.
		if v, ok := c.pending.Load(corrID); ok {
			ch := v.(chan []byte)
			ch <- payload
		}
	}
}

// close shuts down the connection once.
func (c *conn) close() {
	c.once.Do(func() {
		c.nc.Close()
		close(c.closed)
		// Drain pending channels so blocked callers unblock.
		c.pending.Range(func(key, value any) bool {
			ch := value.(chan []byte)
			close(ch)
			return true
		})
	})
}
