package eventkafka

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

type recorder struct {
	messages []kafka.Message
	err      error
	closed   bool
}

func (r *recorder) WriteMessages(_ context.Context, m ...kafka.Message) error {
	r.messages = append(r.messages, m...)
	return r.err
}
func (r *recorder) Close() error { r.closed = true; return nil }

func TestSendContractsAndRetryIdentity(t *testing.T) {
	c, err := New(Config{Brokers: []string{"broker.example.invalid:9092"}, SourceSystem: "example-service"})
	if err != nil {
		t.Fatal(err)
	}
	c.writer.Close()
	r := &recorder{}
	c.writer = r
	defer c.Close()
	app := NewAppMessage("apply")
	app.RequestID = "request"
	app.DeviceUUID = "device"
	app.BID = "example-business"
	app.AppID = 101
	app.PayloadJSON = map[string]any{"amount": 123}
	for i := 0; i < 2; i++ {
		if err := c.SendApp(context.Background(), app); err != nil {
			t.Fatal(err)
		}
	}
	if string(r.messages[0].Value) != string(r.messages[1].Value) {
		t.Fatal("retry changed message")
	}
	sys := NewSysMessage(LevelWarn, "TIMEOUT", "timeout")
	sys.ContextJSON = []any{"context", 1}
	if err := c.SendSys(context.Background(), sys); err != nil {
		t.Fatal(err)
	}
	if sys.SourceSystem != "" {
		t.Fatal("send mutated caller")
	}
	for i, m := range r.messages {
		var body map[string]any
		if err := json.Unmarshal(m.Value, &body); err != nil {
			t.Fatal(err)
		}
		if i < 2 {
			if m.Topic != AppTopic || string(m.Key) != "device" {
				t.Fatal(m)
			}
			if _, ok := body["payload_json"].(map[string]any); !ok {
				t.Fatal(body)
			}
			if body["is_test"] != float64(0) {
				t.Fatal(body)
			}
		} else {
			if m.Topic != SysTopic || string(m.Key) != sys.EventID || body["source_system"] != "example-service" {
				t.Fatal(body)
			}
			if _, exists := body["fingerprint"]; exists {
				t.Fatal("fingerprint should be omitted")
			}
		}
	}
	r.err = errors.New("broker unavailable")
	if err := c.SendApp(context.Background(), app); !errors.Is(err, r.err) {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if err := c.SendApp(context.Background(), app); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}

func TestValidation(t *testing.T) {
	for _, raw := range []string{"null", "[]", `[""]`, `["kafka"]`, `"broker.example.invalid:9092"`} {
		if _, err := ParseBrokers(raw); err == nil {
			t.Fatalf("accepted %s", raw)
		}
	}
	if _, err := ParseBrokers(`["broker.example.invalid:9092"]`); err != nil {
		t.Fatal(err)
	}
	for _, value := range []any{"{}", 123, true, json.RawMessage("null")} {
		if err := jsonContainer("payload", value); err == nil {
			t.Fatalf("accepted %v", value)
		}
	}
	sys := NewSysMessage("INFO", "CODE", "text")
	sys.SourceSystem = "example-service"
	if sys.Validate() == nil {
		t.Fatal("accepted INFO")
	}
	sys.Level = LevelError
	sys.EventCode = ""
	if sys.Validate() == nil {
		t.Fatal("accepted missing code and type")
	}
	sys.EventType = "Timeout"
	if err := sys.Validate(); err != nil {
		t.Fatal(err)
	}
	app := NewAppMessage("event")
	if app.Validate() == nil {
		t.Fatal("accepted missing required fields")
	}
}

func TestInvalidUpdateRetainsWriter(t *testing.T) {
	c, err := New(Config{Brokers: []string{"broker.example.invalid:9092"}})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	old := c.writer
	if c.UpdateBrokers(nil) == nil {
		t.Fatal("accepted invalid brokers")
	}
	if c.writer != old {
		t.Fatal("replaced writer on invalid update")
	}
	if err := c.UpdateBrokers([]string{"other.example.invalid:9092"}); err != nil {
		t.Fatal(err)
	}
	if c.writer == old {
		t.Fatal("writer not replaced")
	}
}

func TestBrokerValidationRejectsCredentialsAndInvalidPorts(t *testing.T) {
	for _, broker := range []string{
		"broker.example.invalid:0", "broker.example.invalid:65536",
		"broker.example.invalid:http", "user:secret@broker.example.invalid:9092",
		"user@broker.example.invalid:9092", "bad host:9092",
	} {
		if _, err := validateBrokers([]string{broker}); err == nil {
			t.Fatalf("accepted invalid endpoint")
		}
	}
	for _, broker := range []string{"broker.example.invalid:9092", "[2001:db8::1]:9092"} {
		if _, err := validateBrokers([]string{broker}); err != nil {
			t.Fatal(err)
		}
	}
}

type waitingWriter struct {
	started chan struct{}
	closed  chan struct{}
}

func (w *waitingWriter) WriteMessages(ctx context.Context, _ ...kafka.Message) error {
	close(w.started)
	<-ctx.Done()
	return ctx.Err()
}
func (w *waitingWriter) Close() error { close(w.closed); return nil }

func TestConcurrentSendAndClose(t *testing.T) {
	c, err := New(Config{Brokers: []string{"broker.example.invalid:9092"}, SourceSystem: "example-service"})
	if err != nil {
		t.Fatal(err)
	}
	c.writer.Close()
	w := &waitingWriter{started: make(chan struct{}), closed: make(chan struct{})}
	c.writer = w
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sent := make(chan error, 1)
	go func() { sent <- c.SendSys(ctx, NewSysMessage(LevelError, "TEST", "test")) }()
	<-w.started
	closed := make(chan error, 1)
	go func() { closed <- c.Close() }()
	select {
	case <-w.closed:
		t.Fatal("closed writer during send")
	default:
	}
	cancel()
	if err := <-sent; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSendHonorsDeadline(t *testing.T) {
	c, err := New(Config{Brokers: []string{"broker.example.invalid:9092"}, SourceSystem: "example-service", SendTimeout: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	c.writer.Close()
	c.writer = &waitingWriter{started: make(chan struct{}), closed: make(chan struct{})}
	defer c.Close()
	err = c.SendSys(context.Background(), NewSysMessage(LevelError, "TEST", "test"))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
}

func TestAppMessageGroupUserIDJSON(t *testing.T) {
	for _, id := range []int64{0, 9223372036854775807} {
		data, err := json.Marshal(AppMessage{UserID: id, GroupUserID: id})
		if err != nil {
			t.Fatal(err)
		}
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(data, &fields); err != nil {
			t.Fatal(err)
		}
		for _, key := range []string{"user_id", "group_user_id"} {
			raw, ok := fields[key]
			if !ok {
				t.Fatalf("missing top-level %s", key)
			}
			var got int64
			if err := json.Unmarshal(raw, &got); err != nil {
				t.Fatal(err)
			}
			if got != id {
				t.Fatalf("%s=%d want %d", key, got, id)
			}
		}
	}
}

func TestAppMessageIDNumberJSON(t *testing.T) {
	data, err := json.Marshal(AppMessage{UserID: 1, IDNumber: "example-id-number"})
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		t.Fatal(err)
	}
	var got string
	if err := json.Unmarshal(fields["id_number"], &got); err != nil {
		t.Fatal(err)
	}
	if got != "example-id-number" {
		t.Fatalf("id_number=%q", got)
	}
	if _, ok := fields["user_id"]; !ok {
		t.Fatal("missing sibling user_id")
	}
	data, err = json.Marshal(AppMessage{})
	if err != nil {
		t.Fatal(err)
	}
	var empty map[string]json.RawMessage
	if err := json.Unmarshal(data, &empty); err != nil {
		t.Fatal(err)
	}
	if _, ok := empty["id_number"]; ok {
		t.Fatal("empty optional id_number should be omitted")
	}
}
