// Package eventkafka sends application and system events using the v1 Kafka contracts.
package eventkafka

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	AppTopic   = "app.app-event-v1"
	SysTopic   = "sys.sys-event-v1"
	LevelWarn  = "WARN"
	LevelError = "ERROR"
	LevelFatal = "FATAL"
)

type AppMessage struct {
	EventID      string             `json:"event_id"`
	EventName    string             `json:"event_name"`
	EventTime    int64              `json:"event_time"`
	EventTimeApp int64              `json:"event_time_app,omitempty"`
	Sequence     int64              `json:"sequence,omitempty"`
	RequestID    string             `json:"request_id"`
	SessionID    string             `json:"session_id,omitempty"`
	IsTest       int                `json:"is_test"`
	DeviceUUID   string             `json:"device_uuid"`
	GaidIDFA     string             `json:"gaid_idfa,omitempty"`
	UserID       int64              `json:"user_id"`
	Mobile       string             `json:"mobile,omitempty"`
	BID          string             `json:"bid"`
	AppID        int                `json:"app_id"`
	AppVersion   string             `json:"app_version,omitempty"`
	IP           string             `json:"ip,omitempty"`
	FI           map[string]int64   `json:"fi,omitempty"`
	FF           map[string]float64 `json:"ff,omitempty"`
	FS           map[string]string  `json:"fs,omitempty"`
	PayloadJSON  any                `json:"payload_json,omitempty"`
}

type SysMessage struct {
	EventID      string `json:"event_id"`
	EventTime    int64  `json:"event_time"`
	SourceSystem string `json:"source_system"`
	JobName      string `json:"job_name,omitempty"`
	Component    string `json:"component,omitempty"`
	Env          string `json:"env,omitempty"`
	Level        string `json:"level"`
	EventCode    string `json:"event_code,omitempty"`
	EventType    string `json:"event_type,omitempty"`
	Message      string `json:"message"`
	StackTrace   string `json:"stack_trace,omitempty"`
	Fingerprint  string `json:"fingerprint,omitempty"`
	BID          string `json:"bid,omitempty"`
	AppID        int    `json:"app_id,omitempty"`
	RequestID    string `json:"request_id,omitempty"`
	EntityRef    string `json:"entity_ref,omitempty"`
	ContextJSON  any    `json:"context_json,omitempty"`
	Host         string `json:"host,omitempty"`
	AppVersion   string `json:"app_version,omitempty"`
}

// NewAppMessage assigns identity once. Reuse the message for retries.
func NewAppMessage(name string) *AppMessage {
	return &AppMessage{EventID: uuid.NewString(), EventTime: time.Now().UnixMilli(), EventName: name}
}

// NewSysMessage assigns identity once. SourceSystem can be supplied by Client defaults.
func NewSysMessage(level, code, message string) *SysMessage {
	return &SysMessage{EventID: uuid.NewString(), EventTime: time.Now().UnixMilli(), Level: level, EventCode: code, Message: message}
}

func required(fields ...string) error {
	for i := 0; i < len(fields); i += 2 {
		if strings.TrimSpace(fields[i+1]) == "" {
			return fmt.Errorf("eventkafka: %s is required", fields[i])
		}
	}
	return nil
}

func jsonContainer(name string, value any) error {
	if value == nil {
		return nil
	}
	b, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("eventkafka: %s: %w", name, err)
	}
	if len(b) == 0 || (b[0] != '{' && b[0] != '[') {
		return fmt.Errorf("eventkafka: %s must be a JSON object or array", name)
	}
	return nil
}

func (m *AppMessage) Validate() error {
	if m == nil {
		return fmt.Errorf("eventkafka: nil AppMessage")
	}
	if err := required("event_id", m.EventID, "event_name", m.EventName, "request_id", m.RequestID, "device_uuid", m.DeviceUUID, "bid", m.BID); err != nil {
		return err
	}
	if m.EventTime <= 0 || m.AppID <= 0 {
		return fmt.Errorf("eventkafka: event_time and app_id must be positive")
	}
	if m.IsTest != 0 && m.IsTest != 1 {
		return fmt.Errorf("eventkafka: is_test must be 0 or 1")
	}
	return jsonContainer("payload_json", m.PayloadJSON)
}

func (m *SysMessage) Validate() error {
	if m == nil {
		return fmt.Errorf("eventkafka: nil SysMessage")
	}
	if err := required("event_id", m.EventID, "source_system", m.SourceSystem, "message", m.Message); err != nil {
		return err
	}
	if m.EventTime <= 0 {
		return fmt.Errorf("eventkafka: event_time must be positive")
	}
	if m.Level != LevelWarn && m.Level != LevelError && m.Level != LevelFatal {
		return fmt.Errorf("eventkafka: invalid level %q", m.Level)
	}
	if strings.TrimSpace(m.EventCode) == "" && strings.TrimSpace(m.EventType) == "" {
		return fmt.Errorf("eventkafka: event_code or event_type is required")
	}
	return jsonContainer("context_json", m.ContextJSON)
}
