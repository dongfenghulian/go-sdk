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
}

type messageWriter interface {
	WriteMessages(context.Context, ...kafka.Message) error
	Close() error
}

// Client is safe for concurrent sends. Do not mutate messages during Send.
type Client struct {
	mu     sync.RWMutex
	writer messageWriter
	config Config
	closed bool
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
	if cfg.SendTimeout < 0 || cfg.MaxAttempts < 0 {
		return nil, errors.New("eventkafka: negative timeout or attempts")
	}
	if cfg.SendTimeout == 0 {
		cfg.SendTimeout = 3 * time.Second
	}
	if cfg.MaxAttempts == 0 {
		cfg.MaxAttempts = 3
	}
	if cfg.Host == "" {
		cfg.Host, _ = os.Hostname()
	}
	return &Client{config: cfg, writer: newWriter(cfg)}, nil
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

// UpdateBrokers waits for active sends, then switches writers.
// Invalid input leaves the current writer unchanged.
func (c *Client) UpdateBrokers(brokers []string) error {
	valid, err := validateBrokers(brokers)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ErrClosed
	}
	if slices.Equal(valid, c.config.Brokers) {
		return nil
	}
	cfg := c.config
	cfg.Brokers = valid
	old := c.writer
	c.writer = newWriter(cfg)
	// Only Brokers is mutable; send defaults remain immutable.
	c.config.Brokers = valid
	return old.Close()
}

func (c *Client) SendApp(ctx context.Context, m *AppMessage) error {
	if err := m.Validate(); err != nil {
		return err
	}
	return c.send(ctx, AppTopic, m.DeviceUUID, m.EventTime, m)
}

func (c *Client) SendSys(ctx context.Context, m *SysMessage) error {
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
	ctx, cancel := context.WithTimeout(ctx, c.config.SendTimeout)
	defer cancel()
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed {
		return ErrClosed
	}
	if err := c.writer.WriteMessages(ctx, kafka.Message{Topic: topic, Key: []byte(key), Value: data, Time: time.UnixMilli(eventTime)}); err != nil {
		return fmt.Errorf("eventkafka: send %s: %w", topic, err)
	}
	return nil
}

// Close waits for active sends and flushes the writer. It is idempotent.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	return c.writer.Close()
}
