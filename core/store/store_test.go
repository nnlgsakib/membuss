package store

import (
	"errors"
	"testing"

	"github.com/nnlgsakib/membuss/core/mid"
)

func TestMemstorePutGet(t *testing.T) {
	s := NewMemstore()
	data := []byte("payload")
	m := mid.FromBytes(data)
	if err := s.Put(Block{MID: m, Data: data}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := s.Get(m)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !got.MID.Equal(m) {
		t.Fatalf("MID mismatch: %s vs %s", got.MID, m)
	}
	if string(got.Data) != string(data) {
		t.Fatalf("data mismatch: %q vs %q", got.Data, data)
	}
}

func TestMemstoreRejectsBadMID(t *testing.T) {
	s := NewMemstore()
	if err := s.Put(Block{MID: mid.FromBytes([]byte("a")), Data: []byte("b")}); err == nil {
		t.Fatal("Put with mismatched MID must fail")
	}
}

func TestMemstoreGetMissing(t *testing.T) {
	s := NewMemstore()
	_, err := s.Get(mid.FromBytes([]byte("nope")))
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get missing: err = %v, want ErrNotFound", err)
	}
}

func TestMemstoreDelete(t *testing.T) {
	s := NewMemstore()
	m := mid.FromBytes([]byte("x"))
	_ = s.Put(Block{MID: m, Data: []byte("x")})
	if !s.Has(m) {
		t.Fatal("Has: block should be present")
	}
	if err := s.Delete(m); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if s.Has(m) {
		t.Fatal("Has after delete: block should be absent")
	}
	if err := s.Delete(m); err != nil {
		t.Fatalf("Delete missing: %v", err)
	}
}

func TestMemstoreGetReturnsCopy(t *testing.T) {
	s := NewMemstore()
	m := mid.FromBytes([]byte("copy"))
	_ = s.Put(Block{MID: m, Data: []byte("copy")})
	got, _ := s.Get(m)
	got.Data[0] = 0xFF
	got2, _ := s.Get(m)
	if got2.Data[0] == 0xFF {
		t.Fatal("Get must return a defensive copy")
	}
}

func TestMemstoreConcurrent(t *testing.T) {
	s := NewMemstore()
	data := []byte("concurrent")
	m := mid.FromBytes(data)
	_ = s.Put(Block{MID: m, Data: data})
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func() {
			_, _ = s.Get(m)
			_ = s.Has(m)
			done <- true
		}()
	}
	for i := 0; i < 10; i++ {
		<-done
	}
}
