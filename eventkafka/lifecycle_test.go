package eventkafka

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

type drainingWriter struct {
	sendStarted  chan struct{}
	closeStarted chan struct{}
	releaseClose chan struct{}
	closeErr     error
	closes       atomic.Int32
	sends        atomic.Int32
}

func (w *drainingWriter) WriteMessages(ctx context.Context, _ ...kafka.Message) error {
	w.sends.Add(1)
	if w.sendStarted != nil {
		select {
		case w.sendStarted <- struct{}{}:
		default:
		}
	}
	<-ctx.Done()
	return ctx.Err()
}
func (w *drainingWriter) Close() error {
	w.closes.Add(1)
	if w.closeStarted != nil {
		close(w.closeStarted)
	}
	if w.releaseClose != nil {
		<-w.releaseClose
	}
	return w.closeErr
}
func lifecycleClient(t *testing.T, w messageWriter) *Client {
	t.Helper()
	c, err := New(Config{Brokers: []string{"old.example.invalid:9092"}, SourceSystem: "test", SendTimeout: 50 * time.Millisecond, MaxConcurrentSends: 128})
	if err != nil {
		t.Fatal(err)
	}
	c.writer.Close()
	c.writer = w
	return c
}
func awaitResult(t *testing.T, ch <-chan error) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(2 * time.Second):
		t.Fatal("operation blocked")
		return nil
	}
}
func awaitSignal(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("operation did not start")
	}
}

func TestUpdateCleanupDoesNotBlockSendDeadlines(t *testing.T) {
	cleanupErr := errors.New("old close failed")
	old := &drainingWriter{closeStarted: make(chan struct{}), releaseClose: make(chan struct{}), closeErr: cleanupErr}
	c := lifecycleClient(t, old)
	// Ensure cleanup is released even if the regression causes a test failure.
	defer c.Close()
	var release sync.Once
	defer release.Do(func() { close(old.releaseClose) })
	updated := make(chan error, 1)
	go func() { updated <- c.UpdateBrokers([]string{"new.example.invalid:9092"}) }()
	awaitSignal(t, old.closeStarted)
	// The production replacement has not been used. Replace it with a writer
	// that honors cancellation, keeping the old writer's Close blocked.
	current := &drainingWriter{}
	c.mu.Lock()
	unused := c.writer
	c.writer = current
	c.mu.Unlock()
	unused.Close()
	results := make(chan error, 64)
	for i := 0; i < 64; i++ {
		go func(i int) {
			ctx := context.Background()
			cancel := func() {}
			if i%2 == 0 {
				ctx, cancel = context.WithTimeout(ctx, 20*time.Millisecond)
			}
			defer cancel()
			if i%2 == 0 {
				m := NewAppMessage("test")
				m.DeviceUUID = "device"
				m.RequestID = "request"
				m.BID = "test"
				m.AppID = 1
				results <- c.SendApp(ctx, m)
			} else {
				results <- c.SendSys(ctx, NewSysMessage(LevelError, "TEST", "test"))
			}
		}(i)
	}
	for i := 0; i < 64; i++ {
		if err := awaitResult(t, results); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("send: %v", err)
		}
	}
	if current.sends.Load() != 64 {
		t.Fatalf("new writer received %d sends", current.sends.Load())
	}
	if len(c.slots) != 0 {
		t.Fatal("send slots leaked")
	}
	select {
	case err := <-updated:
		t.Fatalf("update returned before cleanup: %v", err)
	default:
	}
	release.Do(func() { close(old.releaseClose) })
	if err := awaitResult(t, updated); !errors.Is(err, cleanupErr) {
		t.Fatalf("cleanup error: %v", err)
	}
	c.mu.Lock()
	brokers := c.config.Brokers[0]
	c.mu.Unlock()
	if brokers != "new.example.invalid:9092" {
		t.Fatal("cleanup failure rolled back brokers")
	}
}

func TestUpdateDrainsOldGenerationAndCloseRejectsNewSends(t *testing.T) {
	old := &drainingWriter{sendStarted: make(chan struct{}, 1), closeStarted: make(chan struct{})}
	c := lifecycleClient(t, old)
	c.config.SendTimeout = 5 * time.Second
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sent := make(chan error, 1)
	go func() { sent <- c.SendSys(ctx, NewSysMessage(LevelError, "TEST", "test")) }()
	awaitSignal(t, old.sendStarted)
	updated := make(chan error, 1)
	go func() { updated <- c.UpdateBrokers([]string{"new.example.invalid:9092"}) }()
	deadline := time.After(2 * time.Second)
	for {
		c.mu.Lock()
		switched := c.writer != old
		c.mu.Unlock()
		if switched {
			break
		}
		select {
		case <-deadline:
			t.Fatal("update waited for old send before switching")
		case <-time.After(time.Millisecond):
		}
	}
	select {
	case <-old.closeStarted:
		t.Fatal("closed writer with active send")
	default:
	}
	closed := make(chan error, 2)
	go func() { closed <- c.Close() }()
	deadline = time.After(2 * time.Second)
	for {
		c.mu.Lock()
		stopped := c.closed
		c.mu.Unlock()
		if stopped {
			break
		}
		select {
		case <-deadline:
			t.Fatal("Close did not reject new work")
		case <-time.After(time.Millisecond):
		}
	}
	go func() { closed <- c.Close() }()
	if err := c.SendSys(context.Background(), NewSysMessage(LevelError, "TEST", "test")); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	select {
	case <-closed:
		t.Fatal("Close returned with active old send")
	default:
	}
	cancel()
	if err := awaitResult(t, sent); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := awaitResult(t, updated); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := awaitResult(t, closed); err != nil {
			t.Fatal(err)
		}
	}
	if old.closes.Load() != 1 {
		t.Fatal("old writer not closed exactly once")
	}
	if err := c.UpdateBrokers([]string{"later.example.invalid:9092"}); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}

func TestConcurrentUpdatesAndClose(t *testing.T) {
	c := lifecycleClient(t, &drainingWriter{})
	var wg sync.WaitGroup
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			err := c.UpdateBrokers([]string{"new.example.invalid:" + []string{"9092", "9093"}[i%2]})
			if err != nil && !errors.Is(err, ErrClosed) {
				t.Error(err)
			}
		}(i)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := c.Close(); err != nil {
			t.Error(err)
		}
	}()
	wg.Wait()
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
}
