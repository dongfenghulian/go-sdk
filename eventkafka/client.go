package eventkafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

var ErrBusy = errors.New("eventkafka: send concurrency limit reached")

var ErrClosed = errors.New("eventkafka: client closed")

// BrokersKey is the existing etcd key; its spelling is intentional.
const BrokersKey = "/config/rw/kafka/borker"

type Config struct {
	Brokers      []string
	SourceSystem string
	JobName      string
	Environment  string
	Version      string
	Host         string
	SendTimeout  time.Duration
	MaxAttempts  int
	// MaxConcurrentSends bounds App and Sys sends together per Client. Zero defaults to 32.
	MaxConcurrentSends int
}

type messageWriter interface {
	WriteMessages(context.Context, ...kafka.Message) error
	Close() error
}

// Client is safe for concurrent sends. Do not mutate messages during Send.
type Client struct {
	mu     sync.Mutex
	writer messageWriter
	config Config
	closed bool
	// lifecycle serializes updates and cleanup, never sends.
	lifecycle sync.Mutex
	active    *sync.WaitGroup // protected by mu; replaced on each writer switch
	closeDone chan struct{}
	closeErr  error // published by closing closeDone
	slots     chan struct{}
}

func ParseBrokers(raw string) ([]string, error) {
	var brokers []string
	if err := json.Unmarshal([]byte(raw), &brokers); err != nil {
		return nil, fmt.Errorf("eventkafka: brokers JSON: %w", err)
	}
	return validateBrokers(brokers)
}

func validateBrokers(input []string) ([]string, error) {
	if len(input) == 0 {
		return nil, errors.New("eventkafka: brokers must not be empty")
	}
	brokers := make([]string, len(input))
	for i, b := range input {
		b = strings.TrimSpace(b)
		host, port, err := net.SplitHostPort(b)
		portNumber, portErr := strconv.Atoi(port)
		if err != nil || host == "" || strings.ContainsAny(host, "@/\\ \t\r\n") || portErr != nil || portNumber < 1 || portNumber > 65535 {
			return nil, fmt.Errorf("eventkafka: invalid broker at index %d", i)
		}
		brokers[i] = b
	}
	return brokers, nil
}

// New creates a reusable writer; connectivity is checked on first send.
func New(cfg Config) (*Client, error) {
	brokers, err := validateBrokers(cfg.Brokers)
	if err != nil {
		return nil, err
	}
	cfg.Brokers = brokers
	if cfg.SendTimeout < 0 || cfg.MaxAttempts < 0 || cfg.MaxConcurrentSends < 0 {
		return nil, errors.New("eventkafka: negative timeout, attempts or concurrency")
	}
	if cfg.SendTimeout == 0 {
		cfg.SendTimeout = 3 * time.Second
	}
	if cfg.MaxAttempts == 0 {
		cfg.MaxAttempts = 3
	}
	if cfg.MaxConcurrentSends == 0 {
		cfg.MaxConcurrentSends = 32
	}
	if cfg.Host == "" {
		cfg.Host, _ = os.Hostname()
	}
	return &Client{config: cfg, writer: newWriter(cfg), slots: make(chan struct{}, cfg.MaxConcurrentSends), active: &sync.WaitGroup{}, closeDone: make(chan struct{})}, nil
}

type ownedWriter struct {
	*kafka.Writer
	transport *kafka.Transport
}

func (w *ownedWriter) Close() error {
	err := w.Writer.Close()
	w.transport.CloseIdleConnections()
	return err
}

func newWriter(cfg Config) messageWriter {
	transport := &kafka.Transport{DialTimeout: cfg.SendTimeout, IdleTimeout: 30 * time.Second}
	return &ownedWriter{transport: transport, Writer: &kafka.Writer{
		Transport: transport,
		Addr:      kafka.TCP(cfg.Brokers...), Balancer: &kafka.Hash{},
		RequiredAcks: kafka.RequireAll, MaxAttempts: cfg.MaxAttempts,
		ReadTimeout: cfg.SendTimeout, WriteTimeout: cfg.SendTimeout,
		BatchTimeout: 10 * time.Millisecond,
	}}
}

// UpdateBrokers switches writers before draining and closing the old writer.
// Updates serialize with each other, but cleanup never blocks new sends.
// A cleanup error does not roll back the new configuration.
// Invalid input leaves the current writer unchanged.
func (c *Client) UpdateBrokers(brokers []string) error {
	valid, err := validateBrokers(brokers)
	if err != nil {
		return err
	}
	c.lifecycle.Lock()
	defer c.lifecycle.Unlock()
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return ErrClosed
	}
	if slices.Equal(valid, c.config.Brokers) {
		c.mu.Unlock()
		return nil
	}
	cfg := c.config
	cfg.Brokers = valid
	old, active := c.writer, c.active
	// newWriter only allocates local state; it performs no network I/O.
	c.writer = newWriter(cfg)
	c.active = &sync.WaitGroup{}
	c.config.Brokers = valid
	c.mu.Unlock()
	// No further Add calls can reach this generation after the switch.
	active.Wait()
	return old.Close()
}

func (c *Client) SendApp(ctx context.Context, m *AppMessage) error {
	ctx, cancel := context.WithTimeout(ctx, c.config.SendTimeout)
	defer cancel()
	if err := c.acquire(ctx); err != nil {
		return err
	}
	defer func() { <-c.slots }()
	if err := m.Validate(); err != nil {
		return err
	}
	return c.send(ctx, AppTopic, m.DeviceUUID, m.EventTime, m)
}

func (c *Client) SendSys(ctx context.Context, m *SysMessage) error {
	ctx, cancel := context.WithTimeout(ctx, c.config.SendTimeout)
	defer cancel()
	if err := c.acquire(ctx); err != nil {
		return err
	}
	defer func() { <-c.slots }()
	if m == nil {
		return errors.New("eventkafka: nil SysMessage")
	}
	copy := *m
	if copy.SourceSystem == "" {
		copy.SourceSystem = c.config.SourceSystem
	}
	if copy.JobName == "" {
		copy.JobName = c.config.JobName
	}
	if copy.Env == "" {
		copy.Env = c.config.Environment
	}
	if copy.Host == "" {
		copy.Host = c.config.Host
	}
	if copy.AppVersion == "" {
		copy.AppVersion = c.config.Version
	}
	if err := copy.Validate(); err != nil {
		return err
	}
	return c.send(ctx, SysTopic, copy.EventID, copy.EventTime, &copy)
}

func (c *Client) send(ctx context.Context, topic, key string, eventTime int64, m any) error {
	data, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("eventkafka: encode: %w", err)
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return ErrClosed
	}
	if err := ctx.Err(); err != nil {
		c.mu.Unlock()
		return err
	}
	writer, active := c.writer, c.active
	active.Add(1)
	c.mu.Unlock()
	defer active.Done()
	if err := writer.WriteMessages(ctx, kafka.Message{Topic: topic, Key: []byte(key), Value: data, Time: time.UnixMilli(eventTime)}); err != nil {
		return fmt.Errorf("eventkafka: send %s: %w", topic, err)
	}
	return nil
}

// Close waits for active sends and flushes the writer. It is idempotent.
func (c *Client) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		<-c.closeDone
		return c.closeErr
	}
	// Reject new sends immediately, even while an update is draining.
	c.closed = true
	c.mu.Unlock()

	c.lifecycle.Lock()
	defer c.lifecycle.Unlock()
	// Any update that switched writers has now finished retiring its writer.
	c.mu.Lock()
	writer, active := c.writer, c.active
	c.mu.Unlock()
	active.Wait()
	c.closeErr = writer.Close()
	close(c.closeDone)
	return c.closeErr
}

// acquire never queues work when the per-client concurrency budget is exhausted.
func (c *Client) acquire(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case c.slots <- struct{}{}:
		return nil
	default:
		return ErrBusy
	}
}
