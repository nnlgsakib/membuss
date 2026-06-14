// Package chunk splits an input stream into fixed-size or
// content-defined blocks and produces a typed Block for each.
//
// Every Block is addressed by its content via the core/mid package.
// The chunker is intentionally streaming: it never buffers the
// full input, so it is safe to use on inputs larger than memory.
package chunk

import (
	"errors"
	"fmt"
	"io"

	"github.com/nnlgsakib/membuss/core/mid"

	membusspb "github.com/nnlgsakib/membuss/proto"
)

// DefaultBlockSize is the recommended block size for the fixed
// chunker. 256 KiB matches the typical IPFS chunker default and
// keeps DAG fanout comfortable on commodity hardware.
const DefaultBlockSize = 256 * 1024

// MaxBlockSize is the upper bound accepted by NewFixed. Anything
// above this is almost always a misconfiguration and would
// explode the DAG.
const MaxBlockSize = 4 * 1024 * 1024

// MinBlockSize is the lower bound for any chunker. Sub-block-size
// inputs are emitted as a single block rather than being split.
const MinBlockSize = 1024

// Block is the user-facing view of a single chunk. The underlying
// protobuf message is kept side by side so callers can serialize it
// directly to a blockstore or to the wire.
type Block struct {
	*membusspb.Block
}

// Data returns a defensive copy of the block's bytes.
func (b Block) Data() []byte {
	if b.Block == nil {
		return nil
	}
	out := make([]byte, len(b.Block.Data))
	copy(out, b.Block.Data)
	return out
}

// MID returns the block's content identifier.
func (b Block) MID() mid.MID {
	if b.Block == nil || b.Block.Mid == "" {
		return mid.MID{}
	}
	return mid.MustParse(b.Block.Mid)
}

// Size returns the block's payload size in bytes.
func (b Block) Size() int {
	if b.Block == nil {
		return 0
	}
	return int(b.Block.Size)
}

// Chunker is the streaming interface implemented by every chunker
// strategy. Successive calls to Next return blocks in order. When
// the input is exhausted, Next returns io.EOF; any subsequent call
// also returns io.EOF.
type Chunker interface {
	Next() (Block, error)
}

// ChunkerFactory constructs a Chunker for a given input stream.
type ChunkerFactory func(io.Reader) (Chunker, error)

// NewFixed returns a ChunkerFactory that splits the input into
// fixed-size blocks of size bytes each. The final block may be
// shorter; an empty input yields zero blocks.
func NewFixed(size int) ChunkerFactory {
	return func(r io.Reader) (Chunker, error) {
		if r == nil {
			return nil, errors.New("chunk: nil reader")
		}
		if size < MinBlockSize {
			return nil, fmt.Errorf("chunk: fixed size %d below minimum %d", size, MinBlockSize)
		}
		if size > MaxBlockSize {
			return nil, fmt.Errorf("chunk: fixed size %d above maximum %d", size, MaxBlockSize)
		}
		return &fixedChunker{r: r, size: size}, nil
	}
}

type fixedChunker struct {
	r       io.Reader
	size    int
	buf     []byte
	eofSeen bool
}

func (f *fixedChunker) Next() (Block, error) {
	if f.eofSeen {
		return Block{}, io.EOF
	}
	if cap(f.buf) < f.size {
		f.buf = make([]byte, f.size)
	}
	f.buf = f.buf[:f.size]

	read := 0
	for read < f.size {
		n, err := f.r.Read(f.buf[read:])
		read += n
		if err != nil {
			if errors.Is(err, io.EOF) {
				f.eofSeen = true
				if read == 0 {
					return Block{}, io.EOF
				}
				return f.makeBlock(f.buf[:read]), nil
			}
			return Block{}, fmt.Errorf("chunk: read: %w", err)
		}
	}
	return f.makeBlock(f.buf), nil
}

func (f *fixedChunker) makeBlock(data []byte) Block {
	m := mid.FromBytes(data)
	return Block{Block: &membusspb.Block{
		Data: append([]byte(nil), data...),
		Mid:  m.String(),
		Size: uint64(len(data)),
	}}
}
