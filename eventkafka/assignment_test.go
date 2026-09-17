package eventkafka

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestExpAssignmentContract(t *testing.T) {
	c, err := New(Config{Brokers: []string{"broker.example.invalid:9092"}})
	if err != nil {
		t.Fatal(err)
	}
	c.writer.Close()
	r := &recorder{}
	c.writer = r
	defer c.Close()
	m := NewExpAssignmentMessage(" Experiment ", "9223372036854775807", "Treatment", 1690000000123)
	if m.EventID == "" {
		t.Fatal("missing audit ID")
	}
	for i := 0; i < 2; i++ {
		if err := c.SendExpAssignment(context.Background(), m); err != nil {
			t.Fatal(err)
		}
	}
	if string(r.messages[0].Value) != string(r.messages[1].Value) {
		t.Fatal("retry changed payload")
	}
	record := r.messages[0]
	if record.Topic != "dw.exp-assignment-v1" || record.Time.UnixMilli() != m.AssignedTimeMS {
		t.Fatal("wrong routing/time")
	}
	var key [2]string
	if err := json.Unmarshal(record.Key, &key); err != nil {
		t.Fatal(err)
	}
	if key != [2]string{m.ExperimentID, m.SubjectID} {
		t.Fatal("key changed identity")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(record.Value, &fields); err != nil {
		t.Fatal(err)
	}
	if len(fields) != 5 {
		t.Fatalf("unexpected fields: %s", record.Value)
	}
	if string(fields["assigned_time_ms"]) != "1690000000123" || string(fields["variant"]) != `"Treatment"` {
		t.Fatal("wrong encoding")
	}
	for _, active := range []int{0, 1} {
		m.IsActive = &active
		m.EventID = ""
		if err := c.SendExpAssignment(context.Background(), m); err != nil {
			t.Fatal(err)
		}
		var body map[string]any
		if err := json.Unmarshal(r.messages[len(r.messages)-1].Value, &body); err != nil {
			t.Fatal(err)
		}
		if body["is_active"] != float64(active) {
			t.Fatal("lost explicit status")
		}
		if _, ok := body["event_id"]; ok {
			t.Fatal("optional ID not omitted")
		}
	}
	// These pairs collide under naive colon concatenation.
	var keys []string
	for _, pair := range [][2]string{{"a:b", "c"}, {"a", "b:c"}} {
		m.ExperimentID, m.SubjectID = pair[0], pair[1]
		if err := c.SendExpAssignment(context.Background(), m); err != nil {
			t.Fatal(err)
		}
		keys = append(keys, string(r.messages[len(r.messages)-1].Key))
	}
	if keys[0] == keys[1] {
		t.Fatal("composite key collision")
	}
	r.err = errors.New("unavailable")
	if err := c.SendExpAssignment(context.Background(), m); !errors.Is(err, r.err) {
		t.Fatal(err)
	}
	c.Close()
	if err := c.SendExpAssignment(context.Background(), m); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}

func TestExpAssignmentValidation(t *testing.T) {
	invalid := 2
	cases := []func(*ExpAssignmentMessage){func(m *ExpAssignmentMessage) { m.ExperimentID = " " }, func(m *ExpAssignmentMessage) { m.SubjectID = "" }, func(m *ExpAssignmentMessage) { m.Variant = "" }, func(m *ExpAssignmentMessage) { m.AssignedTimeMS = 0 }, func(m *ExpAssignmentMessage) { m.AssignedTimeMS = -1 }, func(m *ExpAssignmentMessage) { m.IsActive = &invalid }, func(m *ExpAssignmentMessage) { m.ExperimentID = "a\nb" }, func(m *ExpAssignmentMessage) { m.SubjectID = "a\x00b" }, func(m *ExpAssignmentMessage) { m.Variant = "a\tb" }, func(m *ExpAssignmentMessage) { m.EventID = "a\x7fb" }, func(m *ExpAssignmentMessage) { m.SubjectID = string([]byte{255}) }}
	for i, change := range cases {
		m := NewExpAssignmentMessage("exp", "subject", "v1", 1)
		change(m)
		before := *m
		if err := m.Validate(); err == nil {
			t.Fatalf("case %d accepted", i)
		}
		if !reflect.DeepEqual(before, *m) {
			t.Fatal("validation mutated input")
		}
	}
	var m *ExpAssignmentMessage
	if m.Validate() == nil {
		t.Fatal("nil accepted")
	}
}

func TestExpAssignmentSharesLimitAndDeadline(t *testing.T) {
	c, err := New(Config{Brokers: []string{"broker.example.invalid:9092"}, SourceSystem: "test", MaxConcurrentSends: 1, SendTimeout: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	c.writer.Close()
	w := &waitingWriter{started: make(chan struct{}), closed: make(chan struct{})}
	c.writer = w
	defer c.Close()
	if err := c.SendExpAssignment(context.Background(), nil); err == nil || len(c.slots) != 0 {
		t.Fatal("invalid message retained slot")
	}
	done := make(chan error, 1)
	go func() {
		done <- c.SendExpAssignment(context.Background(), NewExpAssignmentMessage("exp", "s", "v1", 1))
	}()
	<-w.started
	if err := c.SendApp(context.Background(), nil); !errors.Is(err, ErrBusy) {
		t.Fatal(err)
	}
	if err := c.SendSys(context.Background(), nil); !errors.Is(err, ErrBusy) {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("deadline ignored")
	}
	if len(c.slots) != 0 {
		t.Fatal("deadline retained slot")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := c.SendExpAssignment(ctx, NewExpAssignmentMessage("exp", "s", "v1", 1)); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestExpAssignmentCallerDeadlineAndReassignment(t *testing.T) {
	c, err := New(Config{Brokers: []string{"broker.example.invalid:9092"}, SendTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	c.writer.Close()
	w := &waitingWriter{started: make(chan struct{}), closed: make(chan struct{})}
	c.writer = w
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- c.SendExpAssignment(ctx, NewExpAssignmentMessage("exp", "subject", "control", 1690000000000))
	}()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal(err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("caller deadline ignored")
	}
	w.Close()
	r := &recorder{}
	c.writer = r
	first := NewExpAssignmentMessage("exp", "subject", "control", 1690000000000)
	second := NewExpAssignmentMessage("exp", "subject", "treatment", 1690000000001)
	for _, m := range []*ExpAssignmentMessage{first, second} {
		if err := c.SendExpAssignment(context.Background(), m); err != nil {
			t.Fatal(err)
		}
	}
	if first.EventID == second.EventID {
		t.Fatal("new assignment reused audit ID")
	}
	if string(r.messages[0].Key) != string(r.messages[1].Key) {
		t.Fatal("reassignment changed business key")
	}
	if !r.messages[1].Time.After(r.messages[0].Time) {
		t.Fatal("reassignment lost version time")
	}
}
