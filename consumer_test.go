package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

func startTestServer(t *testing.T) (*server.Server, string) {
	t.Helper()
	opts := &server.Options{
		Host:      "127.0.0.1",
		Port:      -1,
		JetStream: true,
		StoreDir:  t.TempDir(),
	}
	s, err := server.NewServer(opts)
	if err != nil {
		t.Fatalf("Failed to create NATS server: %v", err)
	}
	go s.Start()
	if !s.ReadyForConnections(10 * time.Second) {
		t.Fatalf("NATS server did not start in time")
	}
	return s, s.ClientURL()
}

func TestConsumer_AckRaceAndRedelivery(t *testing.T) {
	srv, url := startTestServer(t)
	defer srv.Shutdown()

	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("Failed to connect to NATS: %v", err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("Failed to get JetStream context: %v", err)
	}

	streamName := "TEST_STREAM"
	subject := "test.subject"
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     streamName,
		Subjects: []string{subject},
	})
	if err != nil {
		t.Fatalf("Failed to create stream: %v", err)
	}

	numMessages := 1000
	for i := 0; i < numMessages; i++ {
		_, err = js.Publish(subject, []byte(fmt.Sprintf("msg-%d", i)))
		if err != nil {
			t.Fatalf("Failed to publish message %d: %v", err)
		}
	}

	cfg := Config{
		Stream:            streamName,
		Durable:           "test-consumer",
		Subject:           subject,
		AckType:           AckSync,
		AckWait:           500 * time.Millisecond,
		MaxConcurrency:    50,
		HeartbeatInterval: 100 * time.Millisecond,
	}

	consumer := NewConsumer(nc, js, cfg)

	var processedCount int64
	var duplicateCount int64
	var mu sync.Mutex
	deliveredMap := make(map[uint64]uint64)

	handler := func(ctx context.Context, msg *nats.Msg) error {
		meta, err := msg.Metadata()
		if err != nil {
			t.Errorf("Failed to get metadata: %v", err)
			return err
		}

		mu.Lock()
		deliveredMap[meta.Sequence.Stream]++
		count := deliveredMap[meta.Sequence.Stream]
		mu.Unlock()

		if count > 1 {
			atomic.AddInt64(&duplicateCount, 1)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(150 * time.Millisecond):
		}

		atomic.AddInt64(&processedCount, 1)
		return nil
	}

	err = consumer.Start(handler)
	if err != nil {
		t.Fatalf("Failed to start consumer: %v", err)
	}

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt64(&processedCount) >= int64(numMessages) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	consumer.Stop()

	if atomic.LoadInt64(&processedCount) != int64(numMessages) {
		t.Errorf("Expected %d processed messages, got %d", numMessages, atomic.LoadInt64(&processedCount))
	}

	if atomic.LoadInt64(&duplicateCount) > 0 {
		t.Errorf("Expected 0 duplicate deliveries, got %d", atomic.LoadInt64(&duplicateCount))
	}
}

func TestConsumer_GracefulShutdown(t *testing.T) {
	srv, url := startTestServer(t)
	defer srv.Shutdown()

	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("Failed to connect to NATS: %v", err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("Failed to get JetStream context: %v", err)
	}

	streamName := "SHUTDOWN_STREAM"
	subject := "shutdown.subject"
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     streamName,
		Subjects: []string{subject},
	})
	if err != nil {
		t.Fatalf("Failed to create stream: %v", err)
	}

	numMessages := 10
	for i := 0; i < numMessages; i++ {
		_, err = js.Publish(subject, []byte(fmt.Sprintf("msg-%d", i)))
		if err != nil {
			t.Fatalf("Failed to publish message %d: %v", err)
		}
	}

	cfg := Config{
		Stream:         streamName,
		Durable:        "shutdown-consumer",
		Subject:        subject,
		AckType:        AckSync,
		AckWait:        2 * time.Second,
		MaxConcurrency: 5,
	}

	consumer := NewConsumer(nc, js, cfg)

	var processedCount int64
	var canceledCount int64

	handler := func(ctx context.Context, msg *nats.Msg) error {
		select {
		case <-ctx.Done():
			atomic.AddInt64(&canceledCount, 1)
			return ctx.Err()
		case <-time.After(1 * time.Second):
		}
		atomic.AddInt64(&processedCount, 1)
		return nil
	}

	err = consumer.Start(handler)
	if err != nil {
		t.Fatalf("Failed to start consumer: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	consumer.Stop()

	if atomic.LoadInt64(&canceledCount) == 0 {
		t.Errorf("Expected some messages to be canceled, got 0")
	}

	totalPulled := atomic.LoadInt64(&processedCount) + atomic.LoadInt64(&canceledCount)
	if totalPulled != 5 {
		t.Errorf("Expected 5 messages to be pulled, got %d", totalPulled)
	}
}
