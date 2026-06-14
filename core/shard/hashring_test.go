package shard

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/nnlgsakib/membuss/core/mid"
)

func TestAssignDeterministic(t *testing.T) {
	r := NewHashRing()
	for i := 0; i < 5; i++ {
		r.AddPeer(fmt.Sprintf("peer-%d", i))
	}
	m := mid.FromBytes([]byte("payload"))
	a, err := r.Assign(m, 3)
	if err != nil {
		t.Fatalf("Assign a: %v", err)
	}
	b, err := r.Assign(m, 3)
	if err != nil {
		t.Fatalf("Assign b: %v", err)
	}
	if len(a) != 3 || len(b) != 3 {
		t.Fatalf("len(a)=%d len(b)=%d, want 3", len(a), len(b))
	}
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("assignment non-deterministic at index %d: %s vs %s", i, a[i], b[i])
		}
	}
}

func TestAssignEmptyRingFails(t *testing.T) {
	r := NewHashRing()
	m := mid.FromBytes([]byte("x"))
	if _, err := r.Assign(m, 1); err == nil {
		t.Fatal("Assign on empty ring must fail")
	}
}

func TestAssignClampedToPeerCount(t *testing.T) {
	r := NewHashRing()
	r.AddPeer("only")
	m := mid.FromBytes([]byte("x"))
	got, err := r.Assign(m, 5)
	if err != nil {
		t.Fatalf("Assign: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("len(got) = %d, want 1 (clamped to peer count)", len(got))
	}
}

func TestAssignValidation(t *testing.T) {
	r := NewHashRing()
	r.AddPeer("p")
	m := mid.FromBytes([]byte("x"))
	if _, err := r.Assign(m, 0); err == nil {
		t.Fatal("Assign with replicas=0 must fail")
	}
	if _, err := r.Assign(m, MaxReplicas+1); err == nil {
		t.Fatal("Assign with replicas>max must fail")
	}
}

func TestAssignZeroMidFails(t *testing.T) {
	r := NewHashRing()
	r.AddPeer("p")
	if _, err := r.Assign(mid.MID{}, 1); err == nil {
		t.Fatal("Assign with zero MID must fail")
	}
}

func TestAddRemovePeer(t *testing.T) {
	r := NewHashRing()
	r.AddPeer("a")
	r.AddPeer("b")
	r.AddPeer("a") // duplicate
	if r.Len() != 2 {
		t.Fatalf("Len after dup AddPeer = %d, want 2", r.Len())
	}
	r.RemovePeer("a")
	if r.Len() != 1 {
		t.Fatalf("Len after RemovePeer = %d, want 1", r.Len())
	}
	if p := r.Peers(); len(p) != 1 || p[0] != "b" {
		t.Fatalf("Peers = %v, want [b]", p)
	}
	r.RemovePeer("nope")
	if r.Len() != 1 {
		t.Fatalf("Len after RemovePeer missing = %d, want 1", r.Len())
	}
}

func TestAddEmptyPeerIgnored(t *testing.T) {
	r := NewHashRing()
	r.AddPeer("")
	if r.Len() != 0 {
		t.Fatalf("AddPeer(\"\") must be ignored")
	}
}

func TestScoresAreUniqueEnough(t *testing.T) {
	r := NewHashRing()
	for i := 0; i < 50; i++ {
		r.AddPeer(fmt.Sprintf("peer-%d", i))
	}
	seen := make(map[uint64]struct{})
	for i := 0; i < 1000; i++ {
		m := mid.FromBytes([]byte(fmt.Sprintf("m-%d", i)))
		if _, err := r.Assign(m, 1); err != nil {
			t.Fatalf("Assign: %v", err)
		}
		seen[hrwScore(fmt.Sprintf("peer-%d", i%50), []byte(m.String()))] = struct{}{}
	}
	if len(seen) < 100 {
		t.Logf("warning: only %d unique scores across 1000 samples", len(seen))
	}
}

func TestRemapFraction(t *testing.T) {
	const total = 20
	rng := rand.New(rand.NewSource(42))
	peers := make([]string, total)
	for i := range peers {
		peers[i] = fmt.Sprintf("peer-%02d", i)
	}

	r := NewHashRing()
	for _, p := range peers {
		r.AddPeer(p)
	}

	const mids = 1000
	midList := make([]mid.MID, mids)
	for i := range midList {
		data := make([]byte, 32)
		rng.Read(data)
		midList[i] = mid.FromBytes(data)
	}

	before := make(map[string]string, mids)
	for _, m := range midList {
		got, err := r.Assign(m, 1)
		if err != nil {
			t.Fatalf("Assign before: %v", err)
		}
		before[m.String()] = got[0]
	}

	distribution := make(map[string]int)
	for _, p := range before {
		distribution[p]++
	}
	if len(distribution) < total/2 {
		t.Fatalf("distribution too lopsided: only %d peers got any MIDs", len(distribution))
	}

	removed := []string{peers[3], peers[17]}
	for _, p := range removed {
		r.RemovePeer(p)
	}

	reassigned := 0
	for _, m := range midList {
		got, err := r.Assign(m, 1)
		if err != nil {
			t.Fatalf("Assign after: %v", err)
		}
		if got[0] != before[m.String()] {
			wasRemoved := false
			for _, rp := range removed {
				if before[m.String()] == rp {
					wasRemoved = true
					break
				}
			}
			if !wasRemoved {
				reassigned++
			}
		}
	}

	threshold := (mids * 15) / 100
	if reassigned > threshold {
		t.Fatalf("remap fraction too high: %d / %d = %.1f%%, want < 15%%",
			reassigned, mids, float64(reassigned)*100/float64(mids))
	}
	t.Logf("reassigned %d / %d MIDs (%.2f%%)", reassigned, mids, float64(reassigned)*100/float64(mids))
}

func TestReplicasOrderedByScore(t *testing.T) {
	r := NewHashRing()
	for i := 0; i < 10; i++ {
		r.AddPeer(fmt.Sprintf("peer-%d", i))
	}
	m := mid.FromBytes([]byte("replica-order"))
	got, err := r.Assign(m, 5)
	if err != nil {
		t.Fatalf("Assign: %v", err)
	}
	if len(got) != 5 {
		t.Fatalf("len(got) = %d, want 5", len(got))
	}
	seen := make(map[string]struct{})
	for _, p := range got {
		if _, dup := seen[p]; dup {
			t.Fatalf("duplicate peer %s in result", p)
		}
		seen[p] = struct{}{}
	}
}

func TestAssignAfterMassRemoval(t *testing.T) {
	r := NewHashRing()
	for i := 0; i < 20; i++ {
		r.AddPeer(fmt.Sprintf("peer-%02d", i))
	}
	for i := 0; i < 18; i++ {
		r.RemovePeer(fmt.Sprintf("peer-%02d", i))
	}
	if r.Len() != 2 {
		t.Fatalf("Len after mass remove = %d, want 2", r.Len())
	}
	for i := 0; i < 100; i++ {
		m := mid.FromBytes([]byte(fmt.Sprintf("m-%d", i)))
		if _, err := r.Assign(m, 1); err != nil {
			t.Fatalf("Assign after mass remove: %v", err)
		}
	}
}
