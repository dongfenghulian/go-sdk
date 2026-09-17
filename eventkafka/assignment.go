package eventkafka

import (
	"context"
	"encoding/json"
	"fmt"
	"unicode"
	"unicode/utf8"

	"github.com/google/uuid"
)

const ExpAssignmentTopic = "dw.exp-assignment-v1"

// ExpAssignmentMessage describes one subject's assignment in one experiment.
// Nil IsActive omits the field (consumer default: 1); a pointer to 0 explicitly
// deactivates an assignment. EventID is optional and is not the deduplication key.
type ExpAssignmentMessage struct {
	ExperimentID   string `json:"experiment_id"`
	SubjectID      string `json:"subject_id"`
	Variant        string `json:"variant"`
	IsActive       *int   `json:"is_active,omitempty"`
	AssignedTimeMS int64  `json:"assigned_time_ms"`
	EventID        string `json:"event_id,omitempty"`
}

// NewExpAssignmentMessage preserves the actual assignment time supplied by the
// caller and generates an audit ID once. Reuse the message for retries.
func NewExpAssignmentMessage(experimentID, subjectID, variant string, assignedTimeMS int64) *ExpAssignmentMessage {
	return &ExpAssignmentMessage{ExperimentID: experimentID, SubjectID: subjectID, Variant: variant, AssignedTimeMS: assignedTimeMS, EventID: uuid.NewString()}
}

func (m *ExpAssignmentMessage) Validate() error {
	if m == nil {
		return fmt.Errorf("eventkafka: nil ExpAssignmentMessage")
	}
	if err := required("experiment_id", m.ExperimentID, "subject_id", m.SubjectID, "variant", m.Variant); err != nil {
		return err
	}
	if m.AssignedTimeMS <= 0 {
		return fmt.Errorf("eventkafka: assigned_time_ms must be positive")
	}
	if m.IsActive != nil && *m.IsActive != 0 && *m.IsActive != 1 {
		return fmt.Errorf("eventkafka: is_active must be 0 or 1")
	}
	for _, field := range []struct{ name, value string }{{"experiment_id", m.ExperimentID}, {"subject_id", m.SubjectID}, {"variant", m.Variant}, {"event_id", m.EventID}} {
		if !utf8.ValidString(field.value) {
			return fmt.Errorf("eventkafka: %s must be valid UTF-8", field.name)
		}
		for _, r := range field.value {
			if unicode.IsControl(r) {
				return fmt.Errorf("eventkafka: %s must not contain control characters", field.name)
			}
		}
	}
	return nil
}

// SendExpAssignment shares the client's send budget, timeout and writer lifecycle.
// The Kafka key is a JSON pair [experiment_id, subject_id], avoiding delimiter
// collisions and keeping reassignment of the same subject on the same partition.
func (c *Client) SendExpAssignment(ctx context.Context, m *ExpAssignmentMessage) error {
	ctx, cancel := context.WithTimeout(ctx, c.config.SendTimeout)
	defer cancel()
	if err := c.acquire(ctx); err != nil {
		return err
	}
	defer func() { <-c.slots }()
	if err := m.Validate(); err != nil {
		return err
	}
	key, err := json.Marshal([2]string{m.ExperimentID, m.SubjectID})
	if err != nil {
		return fmt.Errorf("eventkafka: encode assignment key: %w", err)
	}
	return c.send(ctx, ExpAssignmentTopic, string(key), m.AssignedTimeMS, m)
}
