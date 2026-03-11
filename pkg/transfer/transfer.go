// Package transfer provides helpers for splitting large byte payloads into
// small gRPC-safe chunks and re-assembling them on the other side.
//
// Why this exists
// ---------------
// gRPC has a default message-size limit of 4 MB and even with the 100 MB
// override in Rivage, a single MatMul tile for a 2000×2000 matrix is ~32 MB.
// More importantly, a single large proto message causes the entire payload to
// be buffered in RAM on both ends.  Chunked streaming lets us:
//   - Stay well below any message-size limit (default chunk: 512 KB)
//   - Pipe data directly to disk on the receiver side
//   - Back-pressure naturally through the gRPC flow-control window
package transfer

import (
	"fmt"
	"io"
)

const DefaultChunkSize = 512 * 1024 // 512 KB per chunk

// Chunker splits a []byte into successive chunks of at most chunkSize bytes.
// It implements io.Reader so it can be used wherever streaming is needed.
type Chunker struct {
	data      []byte
	pos       int
	chunkSize int
}

// NewChunker wraps data in a Chunker with the given chunk size.
// Pass 0 to use DefaultChunkSize.
func NewChunker(data []byte, chunkSize int) *Chunker {
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}
	return &Chunker{data: data, chunkSize: chunkSize}
}

// Next returns the next chunk and true, or nil and false when exhausted.
func (c *Chunker) Next() ([]byte, bool) {
	if c.pos >= len(c.data) {
		return nil, false
	}
	end := c.pos + c.chunkSize
	if end > len(c.data) {
		end = len(c.data)
	}
	chunk := c.data[c.pos:end]
	c.pos = end
	return chunk, true
}

// NumChunks returns how many chunks Next() will produce for data of this size.
func NumChunks(dataLen, chunkSize int) int {
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}
	if dataLen == 0 {
		return 0
	}
	return (dataLen + chunkSize - 1) / chunkSize
}

// Assembler collects incoming byte chunks into a single []byte or streams
// them to an io.Writer (e.g. an *os.File) to avoid RAM pressure.
type Assembler struct {
	w      io.Writer
	buf    []byte   // used only when w == nil
	total  int64
}

// NewAssembler creates an Assembler that writes to w.
// Pass nil to accumulate in memory (use only for small payloads).
func NewAssembler(w io.Writer) *Assembler {
	return &Assembler{w: w}
}

// Add appends a chunk to the assembler.
func (a *Assembler) Add(chunk []byte) error {
	if len(chunk) == 0 {
		return nil
	}
	a.total += int64(len(chunk))
	if a.w != nil {
		_, err := a.w.Write(chunk)
		return err
	}
	a.buf = append(a.buf, chunk...)
	return nil
}

// Bytes returns the assembled data (only valid when created with w == nil).
func (a *Assembler) Bytes() []byte { return a.buf }

// TotalBytes returns the number of bytes written so far.
func (a *Assembler) TotalBytes() int64 { return a.total }

// ChunkReader wraps an io.Reader and emits fixed-size chunks.
// Useful for streaming an already-open file through gRPC.
type ChunkReader struct {
	r         io.Reader
	chunkSize int
	buf       []byte
}

// NewChunkReader creates a ChunkReader that reads from r in chunkSize blocks.
func NewChunkReader(r io.Reader, chunkSize int) *ChunkReader {
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}
	return &ChunkReader{r: r, chunkSize: chunkSize, buf: make([]byte, chunkSize)}
}

// Next reads the next chunk.  Returns (chunk, nil) for data, (nil, io.EOF) at
// end, or (nil, err) on error.
func (cr *ChunkReader) Next() ([]byte, error) {
	n, err := io.ReadFull(cr.r, cr.buf)
	if n > 0 {
		out := make([]byte, n)
		copy(out, cr.buf[:n])
		return out, nil
	}
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return nil, io.EOF
	}
	return nil, fmt.Errorf("reading chunk: %w", err)
}

