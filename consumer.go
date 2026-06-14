package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

// AckType defines the type of acknowledgment to send to the NATS server.
type AckType int

const (
	// AckAsync sends an asynchronous acknowledgment.
	AckAsync AckType = iota
	// AckSync sends a synchronous acknowledgment and waits for the server's response.
	AckSync
)

// Config holds the configuration for the JetStream consumer.
type Config struct {
	Stream            string
	Durable           string
	Subject           string
	AckType           AckType
	AckWait           time.Duration
	MaxDeliver        int
	MaxConcurrency    int
	HeartbeatInterval time.Duration
}

// Handler is the function signature for processing a message.
type Handler func(ctx context.Context, msg *nats.Msg) error

// Consumer represents a JetStream consumer.
type Consumer struct {
	nc     *nats.Conn
	js     nats.JetStreamContext
	sub    *nats.Subscription
	cfg    Config
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	mu     sync.Mutex
	active bool
}

// NewConsumer creates a new JetStream consumer.
func NewConsumer(nc *nats.Conn, js nats.JetStreamContext, cfg Config) *Consumer {
	ctx, cancel := context.WithCancel(context.Background())
	return &Consumer{
		nc:     nc,
		js:     js,
		cfg:    cfg,
		ctx:    ctx,
		cancel: cancel,
	}
}

// Start starts the consumer subscription and worker pool.
func (c *Consumer) Start(handler Handler) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.active {
		return errors.New("consumer is already active")
	}

	subOpts := []nats.SubOpt{
		nats.ManualAck(),
	}
	if c.cfg.Stream != "" {
		subOpts = append(subOpts, nats.BindStream(c.cfg.Stream))
	}
	if c.cfg.AckWait > 0 {
		subOpts = append(subOpts, nats.AckWait(c.cfg.AckWait))
	}
	if c.cfg.MaxDeliver > 0 {
		subOpts = append(subOpts, nats.MaxDeliver(c.cfg.MaxDeliver))
	}

	sub, err := c.js.PullSubscribe(c.cfg.Subject, c.cfg.Durable, subOpts...)
	if err != nil {
		return fmt.Errorf("failed to subscribe: %w", err)
	}
	c.sub = sub
	c.active = true

	maxConcurrency := c.cfg.MaxConcurrency
	if maxConcurrency <= 0 {
		maxConcurrency = 1
	}

	sem := make(chan struct{}, maxConcurrency)

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for {
			select {
			case <-c.ctx.Done():
				return
			default:
			}

			select {
			case <-c.ctx.Done():
				return
			case sem <- struct{}{}:
			}

			fetchCtx, fetchCancel := context.WithTimeout(c.ctx, 1*time.Second)
			msgs, err := sub.Fetch(1, nats.Context(fetchCtx))
			fetchCancel()
			if err != nil {
				<-sem
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, nats.ErrTimeout) {
					continue
				}
				time.Sleep(100 * time.Millisecond)
				continue
			}

			if len(msgs) == 0 {
				<-sem
				continue
			}

			msg := msgs[0]
			c.wg.Add(1)
			go func(m *nats.Msg) {
				defer func() {
					<-sem
					c.wg.Done()
				}()
				c.processMessage(m, handler)
			}(msg)
		}
	}()

	return nil
}

// processMessage handles a single message, including heartbeats and acknowledgments.
func (c *Consumer) processMessage(msg *nats.Msg, handler Handler) {
	msgCtx, cancel := context.WithCancel(c.ctx)
	defer cancel()

	if c.cfg.HeartbeatInterval > 0 {
		ticker := time.NewTicker(c.cfg.HeartbeatInterval)
		defer ticker.Stop()
		go func() {
			for {
				select {
				case <-msgCtx.Done():
					return
				case <-ticker.C:
					_ = msg.InProgress()
				}
			}
		}()
	}

	err := handler(msgCtx, msg)

	select {
	case <-c.ctx.Done():
		_ = msg.Nak()
		return
	default:
	}

	if err != nil {
		_ = msg.Nak()
		return
	}

	var ackErr error
	if c.cfg.AckType == AckSync {
		ackErr = msg.AckSync()
	} else {
		ackErr = msg.Ack()
	}

	if ackErr != nil {
		// Log or handle acknowledgment error if necessary
	}
}

// Stop gracefully shuts down the consumer, waiting for in-flight messages to finish.
func (c *Consumer) Stop() {
	c.mu.Lock()
	if !c.active {
		c.mu.Unlock()
		return
	}
	c.active = false
	c.mu.Unlock()

	c.cancel()
	c.wg.Wait()

	if c.nc != nil {
		_ = c.nc.Flush()
	}
}
