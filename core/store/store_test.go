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
	if err := s.Put(m, data); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := s.Get(m)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != string(data) {
		t.Fatalf("data mismatch: %q vs %q", got, data)
	}
}

func TestMemstoreRejectsBadMID(t *testing.T) {
	s := NewMemstore()
	if err := s.Put(mid.FromBytes([]byte("a")), []byte("b")); err == nil {
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
	_ = s.Put(m, []byte("x"))
	ok, _ := s.Has(m)
	if !ok {
		t.Fatal("Has: block should be present")
	}
	if err := s.Delete(m); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	ok, _ = s.Has(m)
	if ok {
		t.Fatal("Has after delete: block should be absent")
	}
	if err := s.Delete(m); err != nil {
		t.Fatalf("Delete missing: %v", err)
	}
}

func TestMemstoreGetReturnsCopy(t *testing.T) {
	s := NewMemstore()
	m := mid.FromBytes([]byte("copy"))
	_ = s.Put(m, []byte("copy"))
	got, _ := s.Get(m)
	got[0] = 0xFF
	got2, _ := s.Get(m)
	if got2[0] == 0xFF {
		t.Fatal("Get must return a defensive copy")
	}
}

func TestMemstoreConcurrent(t *testing.T) {
	s := NewMemstore()
	data := []byte("concurrent")
	m := mid.FromBytes(data)
	_ = s.Put(m, data)
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func() {
			_, _ = s.Get(m)
			_, _ = s.Has(m)
			done <- true
		}()
	}
	for i := 0; i < 10; i++ {
		<-done
	}
}
