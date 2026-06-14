// Package erasure applies Reed-Solomon erasure coding to blocks
// before they are distributed across the network.
//
// A block is split into N data shards and augmented with M parity
// shards; the resulting N+M shards can reconstruct the original
// block as long as any N of them are present. Each shard is
// addressed independently by a content MID (shard-MID) and can
// be stored, transferred, and verified on its own.
//
// The package also produces an ErasureManifest that is stored
// under the original block's MID; the manifest is the
// authoritative description of how the block was sharded, and
// the decoder uses it to recover the original bytes from a
// (possibly incomplete) shard set.
package erasure

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/klauspost/reedsolomon"

	"github.com/nnlgsakib/membuss/core/mid"

	membusspb "github.com/nnlgsakib/membuss/proto"
)

// DefaultDataShards is the default number of data shards.
const DefaultDataShards = 10

// DefaultParityShards is the default number of parity shards.
const DefaultParityShards = 4

// MinShards is the smallest data-shard count accepted by
// NewEncoder.
const MinShards = 2

// MaxShards is the largest data+parity count accepted by
// NewEncoder.
const MaxShards = 256

// Config describes an erasure-coding configuration. A zero
// value is invalid; use NewConfig or DefaultConfig.
type Config struct {
	DataShards   int
	ParityShards int
}

// DefaultConfig returns a Config with the constitution-mandated
// defaults (10 data + 4 parity shards).
func DefaultConfig() Config {
	return Config{DataShards: DefaultDataShards, ParityShards: DefaultParityShards}
}

// NewConfig returns a validated Config or an error.
func NewConfig(data, parity int) (Config, error) {
	if data < MinShards {
		return Config{}, fmt.Errorf("erasure: data shards %d below minimum %d", data, MinShards)
	}
	if parity < 0 {
		return Config{}, fmt.Errorf("erasure: parity shards must be non-negative")
	}
	if data+parity > MaxShards {
		return Config{}, fmt.Errorf("erasure: total shards %d above maximum %d", data+parity, MaxShards)
	}
	return Config{DataShards: data, ParityShards: parity}, nil
}

// Shard is one erasure-coded shard of a block, paired with its
// shard-MID for content addressing.
type Shard struct {
	Index    int
	Data     []byte
	ShardMID mid.MID
	Original mid.MID
}

// Encoded is the result of encoding a block.
type Encoded struct {
	OriginalMID  mid.MID
	OriginalSize int
	Shards       []Shard
	Manifest     *membusspb.ErasureManifest
}

// Encoder applies a fixed Config to a stream of blocks.
type Encoder struct {
	cfg Config
	enc reedsolomon.Encoder
}

// NewEncoder returns an Encoder that uses the given config.
func NewEncoder(cfg Config) (*Encoder, error) {
	if _, err := NewConfig(cfg.DataShards, cfg.ParityShards); err != nil {
		return nil, err
	}
	enc, err := reedsolomon.New(cfg.DataShards, cfg.ParityShards)
	if err != nil {
		return nil, fmt.Errorf("erasure: build encoder: %w", err)
	}
	return &Encoder{cfg: cfg, enc: enc}, nil
}

// Encode splits data into shards and produces the parity shards.
// The returned Shard slice has length DataShards+ParityShards.
// Shards are zero-padded so each is exactly the same size; the
// original (unpadded) size is recorded in the manifest.
func (e *Encoder) Encode(data []byte) (*Encoded, error) {
	if len(data) == 0 {
		return nil, errors.New("erasure: cannot encode empty block")
	}
	original := mid.FromBytes(data)

	// Split returns a slice of length DataShards+ParityShards
	// with data shards populated and parity shards zeroed.
	shards, err := e.enc.Split(data)
	if err != nil {
		return nil, fmt.Errorf("erasure: split: %w", err)
	}
	if len(shards) != e.cfg.DataShards+e.cfg.ParityShards {
		return nil, fmt.Errorf("erasure: split produced %d shards, want %d", len(shards), e.cfg.DataShards+e.cfg.ParityShards)
	}
	if err := e.enc.Encode(shards); err != nil {
		return nil, fmt.Errorf("erasure: encode: %w", err)
	}

	total := e.cfg.DataShards + e.cfg.ParityShards
	out := &Encoded{
		OriginalMID:  original,
		OriginalSize: len(data),
		Shards:       make([]Shard, total),
		Manifest: &membusspb.ErasureManifest{
			OriginalMid:  original.String(),
			DataShards:   uint32(e.cfg.DataShards),
			ParityShards: uint32(e.cfg.ParityShards),
			OriginalSize: uint64(len(data)),
		},
	}
	for i, raw := range shards {
		s := Shard{
			Index:    i,
			Data:     raw,
			Original: original,
		}
		s.ShardMID = mid.FromBytes(raw)
		out.Shards[i] = s
		out.Manifest.ShardMids = append(out.Manifest.ShardMids, s.ShardMID.String())
	}
	return out, nil
}

// Decode reconstructs the original block from a (possibly
// incomplete) shard set. Missing shards MUST be nil entries.
// Returns the original (unpadded) bytes.
func (e *Encoder) Decode(shards [][]byte, manifest *membusspb.ErasureManifest) ([]byte, error) {
	if manifest == nil {
		return nil, errors.New("erasure: nil manifest")
	}
	if int(manifest.DataShards) != e.cfg.DataShards {
		return nil, fmt.Errorf("erasure: manifest data shards %d, encoder %d", manifest.DataShards, e.cfg.DataShards)
	}
	if int(manifest.ParityShards) != e.cfg.ParityShards {
		return nil, fmt.Errorf("erasure: manifest parity shards %d, encoder %d", manifest.ParityShards, e.cfg.ParityShards)
	}
	total := e.cfg.DataShards + e.cfg.ParityShards
	if len(shards) != total {
		return nil, fmt.Errorf("erasure: got %d shards, want %d", len(shards), total)
	}

	if err := e.enc.Reconstruct(shards); err != nil {
		return nil, fmt.Errorf("erasure: reconstruct: %w", err)
	}

	var buf bytes.Buffer
	if err := e.enc.Join(&buf, shards, len(shards[0])*e.cfg.DataShards); err != nil {
		return nil, fmt.Errorf("erasure: join: %w", err)
	}
	trimmed := buf.Bytes()[:manifest.OriginalSize]
	want, err := mid.Parse(manifest.OriginalMid)
	if err != nil {
		return nil, fmt.Errorf("erasure: parse manifest original MID: %w", err)
	}
	got := mid.FromBytes(trimmed)
	if !got.Equal(want) {
		return nil, errors.New("erasure: recovered bytes do not match manifest original MID")
	}
	return trimmed, nil
}

// Verify checks that a complete shard set is internally
// consistent (parity shards are correct for the data shards).
// It does NOT modify the shards.
func (e *Encoder) Verify(shards [][]byte) (bool, error) {
	return e.enc.Verify(shards)
}
