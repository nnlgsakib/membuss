package memex

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/nnlgsakib/membuss/core/mid"
	membusspb "github.com/nnlgsakib/membuss/proto"
	"google.golang.org/protobuf/proto"
)

type mockStream struct {
	network.Stream
	buf bytes.Buffer
}

func (m *mockStream) SetWriteDeadline(t time.Time) error {
	return nil
}

func (m *mockStream) Write(p []byte) (n int, err error) {
	return m.buf.Write(p)
}

func decodeFrames(buf []byte) ([]*membusspb.MemexMessage, error) {
	var msgs []*membusspb.MemexMessage
	r := bytes.NewReader(buf)
	for r.Len() > 0 {
		var lenBuf [4]byte
		n, err := r.Read(lenBuf[:])
		if err != nil {
			return nil, err
		}
		if n < 4 {
			return nil, fmt.Errorf("short read for length prefix")
		}
		l := uint32(lenBuf[0])<<24 | uint32(lenBuf[1])<<16 | uint32(lenBuf[2])<<8 | uint32(lenBuf[3])
		msgBuf := make([]byte, l)
		n, err = r.Read(msgBuf)
		if err != nil {
			return nil, err
		}
		if n < int(l) {
			return nil, fmt.Errorf("short read for message: got %d want %d", n, l)
		}
		var msg membusspb.MemexMessage
		if err := proto.Unmarshal(msgBuf, &msg); err != nil {
			return nil, err
		}
		msgs = append(msgs, &msg)
	}
	return msgs, nil
}

func TestWriteLoopBatching(t *testing.T) {
	s := &Session{}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	stream := &mockStream{}
	eventChan := make(chan sessionEvent, 100)

	// Create some MIDs to request
	mids := make([]mid.MID, 5)
	for i := 0; i < 5; i++ {
		mids[i] = mid.FromBytes([]byte(fmt.Sprintf("dummy-mid-hash-%d", i)))
	}

	// Queue wants/cancels
	eventChan <- sessionEvent{isCancel: false, mid: mids[0]}
	eventChan <- sessionEvent{isCancel: false, mid: mids[1]}
	eventChan <- sessionEvent{isCancel: true, mid: mids[2]}
	eventChan <- sessionEvent{isCancel: false, mid: mids[3]}
	eventChan <- sessionEvent{isCancel: true, mid: mids[4]}

	// Close channel to signal writeLoop to exit after processing
	close(eventChan)

	err := s.writeLoop(ctx, stream, eventChan)
	if err != nil {
		t.Fatalf("writeLoop exited with error: %v", err)
	}

	// Decode frames sent
	msgs, err := decodeFrames(stream.buf.Bytes())
	if err != nil {
		t.Fatalf("failed to decode written frames: %v", err)
	}

	if len(msgs) != 1 {
		t.Fatalf("expected exactly 1 batched frame, got %d", len(msgs))
	}

	msg := msgs[0]
	if len(msg.Wants) != 3 {
		t.Errorf("expected 3 Wants, got %d", len(msg.Wants))
	}
	if len(msg.Cancel) != 2 {
		t.Errorf("expected 2 Cancel, got %d", len(msg.Cancel))
	}

	// Verify order/correctness of Wants
	expectedWants := []string{mids[0].String(), mids[1].String(), mids[3].String()}
	for i, w := range msg.Wants {
		if w.Mid != expectedWants[i] {
			t.Errorf("Wants[%d]: expected %s, got %s", i, expectedWants[i], w.Mid)
		}
	}

	// Verify order/correctness of Cancel
	expectedCancels := []string{mids[2].String(), mids[4].String()}
	for i, c := range msg.Cancel {
		if c != expectedCancels[i] {
			t.Errorf("Cancel[%d]: expected %s, got %s", i, expectedCancels[i], c)
		}
	}
}

func TestWriteLoopBatchTimeout(t *testing.T) {
	s := &Session{}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	stream := &mockStream{}
	eventChan := make(chan sessionEvent, 100)

	mids := make([]mid.MID, 2)
	for i := 0; i < 2; i++ {
		mids[i] = mid.FromBytes([]byte(fmt.Sprintf("timeout-mid-%d", i)))
	}

	// Start writeLoop in a goroutine
	errChan := make(chan error, 1)
	go func() {
		errChan <- s.writeLoop(ctx, stream, eventChan)
	}()

	// Send first event
	eventChan <- sessionEvent{isCancel: false, mid: mids[0]}

	// Wait long enough to trigger a flush (more than 5ms)
	time.Sleep(15 * time.Millisecond)

	// Send second event
	eventChan <- sessionEvent{isCancel: false, mid: mids[1]}

	// Wait another moment to flush second event
	time.Sleep(15 * time.Millisecond)

	// Close the channel and wait for writeLoop to exit
	close(eventChan)

	select {
	case err := <-errChan:
		if err != nil {
			t.Fatalf("writeLoop returned error: %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for writeLoop to finish")
	}

	// Decode frames
	msgs, err := decodeFrames(stream.buf.Bytes())
	if err != nil {
		t.Fatalf("failed to decode written frames: %v", err)
	}

	// We expect 2 separate messages (each containing 1 want) because of the flush delay.
	if len(msgs) != 2 {
		t.Fatalf("expected exactly 2 frames due to batch timeout flush, got %d", len(msgs))
	}

	if len(msgs[0].Wants) != 1 || msgs[0].Wants[0].Mid != mids[0].String() {
		t.Errorf("first frame invalid: %v", msgs[0])
	}
	if len(msgs[1].Wants) != 1 || msgs[1].Wants[0].Mid != mids[1].String() {
		t.Errorf("second frame invalid: %v", msgs[1])
	}
}

func TestWriteLoopMaxBatchSize(t *testing.T) {
	s := &Session{}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	stream := &mockStream{}
	eventChan := make(chan sessionEvent, 100)

	// Queue 33 events
	mids := make([]mid.MID, 33)
	for i := 0; i < 33; i++ {
		m := mid.FromBytes([]byte(fmt.Sprintf("batchsize-mid-%d", i)))
		mids[i] = m
		eventChan <- sessionEvent{isCancel: false, mid: m}
	}

	close(eventChan)

	err := s.writeLoop(ctx, stream, eventChan)
	if err != nil {
		t.Fatalf("writeLoop exited with error: %v", err)
	}

	// Decode frames
	msgs, err := decodeFrames(stream.buf.Bytes())
	if err != nil {
		t.Fatalf("failed to decode written frames: %v", err)
	}

	// We expect 2 frames because maxBatchSize is 32.
	if len(msgs) != 2 {
		t.Fatalf("expected exactly 2 frames, got %d", len(msgs))
	}

	if len(msgs[0].Wants) != 32 {
		t.Errorf("expected first frame to have 32 wants, got %d", len(msgs[0].Wants))
	}
	if len(msgs[1].Wants) != 1 {
		t.Errorf("expected second frame to have 1 want, got %d", len(msgs[1].Wants))
	}
}
