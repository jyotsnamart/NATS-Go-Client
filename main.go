package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go"
)

func main() {
	fmt.Println("Starting NATS Go Client Consumer...")

	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		log.Fatalf("Failed to connect to NATS: %v", err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		log.Fatalf("Failed to get JetStream context: %v", err)
	}

	cfg := Config{
		Stream:            "MY_STREAM",
		Durable:           "my-consumer",
		Subject:           "my.subject",
		AckType:           AckSync,
		AckWait:           30 * time.Second,
		MaxConcurrency:    10,
		HeartbeatInterval: 5 * time.Second,
	}

	consumer := NewConsumer(nc, js, cfg)

	handler := func(ctx context.Context, msg *nats.Msg) error {
		fmt.Printf("Processing message: %s\n", string(msg.Data))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(1 * time.Second):
		}
		return nil
	}

	if err := consumer.Start(handler); err != nil {
		log.Fatalf("Failed to start consumer: %v", err)
	}

	time.Sleep(5 * time.Second)
	consumer.Stop()
	fmt.Println("Consumer stopped gracefully.")
}
