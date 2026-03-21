package fluxmq

import (
	"bytes"
	"encoding/binary"
	"io"
)

// ─── encoder ──────────────────────────────────────────────────────────────────

// encoder accumulates big-endian encoded bytes into a buffer.
type encoder struct {
	buf []byte
}

func (e *encoder) u8(v uint8) {
	e.buf = append(e.buf, v)
}

func (e *encoder) u16(v uint16) {
	e.buf = append(e.buf, byte(v>>8), byte(v))
}

func (e *encoder) i16(v int16) {
	e.u16(uint16(v))
}

func (e *encoder) u32(v uint32) {
	e.buf = append(e.buf, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

func (e *encoder) i32(v int32) {
	e.u32(uint32(v))
}

func (e *encoder) u64(v uint64) {
	e.u32(uint32(v >> 32))
	e.u32(uint32(v))
}

// str writes a 2B big-endian length prefix followed by the string bytes.
func (e *encoder) str(s string) {
	e.u16(uint16(len(s)))
	e.buf = append(e.buf, s...)
}

// bytes16 writes a 2B big-endian length prefix followed by the byte slice.
func (e *encoder) bytes16(b []byte) {
	e.u16(uint16(len(b)))
	e.buf = append(e.buf, b...)
}

// bytes32 writes a 4B big-endian length prefix followed by the byte slice.
func (e *encoder) bytes32(b []byte) {
	e.u32(uint32(len(b)))
	e.buf = append(e.buf, b...)
}

// bytes returns the accumulated buffer.
func (e *encoder) bytes() []byte {
	return e.buf
}

// ─── decoder ──────────────────────────────────────────────────────────────────

// decoder reads big-endian encoded values from a bytes.Reader.
// Once an error occurs, all subsequent reads are no-ops and the error is preserved.
type decoder struct {
	r   *bytes.Reader
	err error
}

func newDecoder(b []byte) *decoder {
	return &decoder{r: bytes.NewReader(b)}
}

func (d *decoder) u16() uint16 {
	if d.err != nil {
		return 0
	}
	var v uint16
	if err := binary.Read(d.r, binary.BigEndian, &v); err != nil {
		d.err = err
		return 0
	}
	return v
}

func (d *decoder) i16() int16 {
	return int16(d.u16())
}

func (d *decoder) u32() uint32 {
	if d.err != nil {
		return 0
	}
	var v uint32
	if err := binary.Read(d.r, binary.BigEndian, &v); err != nil {
		d.err = err
		return 0
	}
	return v
}

func (d *decoder) i32() int32 {
	return int32(d.u32())
}

func (d *decoder) u64() uint64 {
	if d.err != nil {
		return 0
	}
	var v uint64
	if err := binary.Read(d.r, binary.BigEndian, &v); err != nil {
		d.err = err
		return 0
	}
	return v
}

// str reads a 2B length-prefixed string.
func (d *decoder) str() string {
	if d.err != nil {
		return ""
	}
	n := d.u16()
	if d.err != nil {
		return ""
	}
	b := make([]byte, n)
	if _, err := io.ReadFull(d.r, b); err != nil {
		d.err = err
		return ""
	}
	return string(b)
}

// bytes16 reads a 2B length-prefixed byte slice.
func (d *decoder) bytes16() []byte {
	if d.err != nil {
		return nil
	}
	n := d.u16()
	if d.err != nil {
		return nil
	}
	b := make([]byte, n)
	if _, err := io.ReadFull(d.r, b); err != nil {
		d.err = err
		return nil
	}
	return b
}

// bytes32 reads a 4B length-prefixed byte slice.
func (d *decoder) bytes32() []byte {
	if d.err != nil {
		return nil
	}
	n := d.u32()
	if d.err != nil {
		return nil
	}
	b := make([]byte, n)
	if _, err := io.ReadFull(d.r, b); err != nil {
		d.err = err
		return nil
	}
	return b
}
