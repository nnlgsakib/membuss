package chunk

import (
	"errors"
	"fmt"
	"io"

	"github.com/nnlgsakib/membuss/core/mid"

	membusspb "github.com/nnlgsakib/membuss/proto"
)

// Rabin configuration constants. The 64-bit Rabin fingerprint
// (mod 2^64) gives a roughly uniform distribution of cut points
// when the input is treated as a sliding window. We pick the
// mask so blocks land in the 64 KiB..512 KiB range for typical
// byte streams, which is the sweet spot for content-defined
// chunking on local storage.
const (
	// rabinMin is the minimum Rabin-cuttable block size. Any
	// input smaller than this is emitted as a single block.
	rabinMin = 64 * 1024
	// rabinMax is the maximum block size. If no Rabin boundary
	// is found before this many bytes, a forced cut is emitted.
	rabinMax = 512 * 1024
	// rabinWindow is the rolling-hash window size in bytes.
	rabinWindow = 64
)

// rabinPolynomial is an irreducible polynomial over GF(2) used to
// compute the fingerprint of a 64-bit sliding window.
const rabinPolynomial uint64 = 0x3DA3358B4DC173

// rabinMask keeps the low N bits of the rolling fingerprint. N is
// chosen so the expected cut distance is ~256 KiB:
//
//	expected = 2^N  =>  N = log2(256 KiB) = 18
const rabinMask = (uint64(1) << 18) - 1

// shiftTable lets the rolling hash shift its window in O(1) per
// byte. It is built once at init.
var shiftTable [256]uint64

func init() {
	for i := 0; i < 256; i++ {
		shiftTable[i] = uint64(i) * rabinPolynomial
	}
}

// rabinChunker is a content-defined chunker: it computes a
// rolling Rabin fingerprint over the input and emits a block
// whenever the low N bits of the fingerprint match a mask.
//
// Cut points are determined by content and not by absolute
// position, which gives strong deduplication properties when a
// file is appended to or shifted within a larger blob.
type rabinChunker struct {
	r       io.Reader
	buf     []byte // circular buffer of the most recent rabinWindow bytes
	start   int    // index of the oldest byte in buf
	filled  int    // how many bytes of buf are valid (0..rabinWindow)
	carry   uint64 // the value of the polynomial for the current window
	pending []byte // bytes accumulated since the last cut, not yet emitted
	eofSeen bool
}

// NewRabin returns a ChunkerFactory that splits the input using
// content-defined (Rabin) chunking. Blocks land in a size band
// roughly bounded by rabinMin and rabinMax, with a geometric mean
// near 256 KiB.
func NewRabin() ChunkerFactory {
	return func(r io.Reader) (Chunker, error) {
		if r == nil {
			return nil, errors.New("chunk: nil reader")
		}
		return &rabinChunker{
			r:   r,
			buf: make([]byte, rabinWindow),
		}, nil
	}
}

func (c *rabinChunker) Next() (Block, error) {
	if c.eofSeen {
		return Block{}, io.EOF
	}

	tmp := make([]byte, 1)
	for {
		n, err := c.r.Read(tmp)
		if n > 0 {
			b := tmp[0]
			c.rollByte(b)
			c.pending = append(c.pending, b)

			// Honour the absolute upper bound first, then the
			// lower bound, then look for a Rabin cut.
			if len(c.pending) >= rabinMax {
				return c.cut()
			}
			if len(c.pending) >= rabinMin && c.carry&rabinMask == 0 {
				return c.cut()
			}
			continue
		}
		if errors.Is(err, io.EOF) {
			c.eofSeen = true
			if len(c.pending) == 0 {
				return Block{}, io.EOF
			}
			return c.cut()
		}
		if err != nil {
			return Block{}, fmt.Errorf("chunk: rabin read: %w", err)
		}
	}
}

// rollByte updates the rolling fingerprint for an incoming byte.
// Polynomial arithmetic is performed modulo 2^64; with a 64-bit
// window, every shift is an implicit mod.
func (c *rabinChunker) rollByte(b byte) {
	outgoing := c.buf[c.start]
	c.buf[c.start] = b
	c.start = (c.start + 1) % rabinWindow
	if c.filled < rabinWindow {
		c.filled++
	}
	c.carry = (c.carry - shiftTable[outgoing]) << 1
	c.carry += shiftTable[b]
}

// cut returns the currently-pending bytes as a Block and resets
// the rolling window state.
func (c *rabinChunker) cut() (Block, error) {
	if len(c.pending) == 0 {
		return Block{}, io.ErrNoProgress
	}
	data := c.pending
	c.pending = nil
	c.filled = 0
	c.start = 0
	c.carry = 0
	c.buf = make([]byte, rabinWindow)
	return makeRabinBlock(data)
}

func makeRabinBlock(data []byte) (Block, error) {
	if len(data) == 0 {
		return Block{}, errors.New("chunk: refusing to emit empty block")
	}
	m := mid.FromBytes(data)
	return Block{Block: &membusspb.Block{
		Data: append([]byte(nil), data...),
		Mid:  m.String(),
		Size: uint64(len(data)),
	}}, nil
}
