// Phase 14 smoke: confirm the new CIDv1 MID layout.
package main

import (
	"crypto/sha256"
	"fmt"
	"log"

	"github.com/nnlgsakib/membuss/core/mid"
)

func main() {
	if err := run(); err != nil {
		log.Fatalf("phase14 smoke: %v", err)
	}
}

func run() error {
	for _, data := range [][]byte{
		[]byte(""),
		[]byte("hello"),
		[]byte("membuss"),
	} {
		m := mid.FromBytes(data)
		s := m.String()
		fmt.Printf("input=%-12q -> %s (len=%d)\n", data, s, len(s))
		// Re-parse and round-trip
		back, err := mid.Parse(s)
		if err != nil {
			return fmt.Errorf("Parse(%q): %w", s, err)
		}
		if !m.Equal(back) {
			return fmt.Errorf("round-trip mismatch: %v vs %v", m, back)
		}
		// Verify the CID form reports v1
		c := m.CID()
		if c.Version() != 1 {
			return fmt.Errorf("CID version = %d, want 1", c.Version())
		}
		// Verify the digest matches SHA-256 of the input
		d, err := m.DigestBytes()
		if err != nil {
			return err
		}
		sum := sha256.Sum256(data)
		if fmt.Sprintf("%x", d) != fmt.Sprintf("%x", sum[:]) {
			return fmt.Errorf("digest mismatch for %q", data)
		}
	}
	return nil
}
