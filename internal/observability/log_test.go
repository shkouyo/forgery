package observability

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func TestNewLogger_DefaultLevel(t *testing.T) {
	var buf bytes.Buffer
	logger := newLogger(&buf, "", "json")

	logger.Debug("should not appear")
	logger.Info("should appear")

	output := buf.String()
	if strings.Contains(output, "should not appear") {
		t.Error("default level (info) should not log debug messages")
	}
	if !strings.Contains(output, "should appear") {
		t.Error("default level (info) should log info messages")
	}
}

func TestNewLogger_DebugLevel(t *testing.T) {
	var buf bytes.Buffer
	logger := newLogger(&buf, "debug", "json")

	logger.Debug("debug msg")

	output := buf.String()
	if !strings.Contains(output, "debug msg") {
		t.Error("debug level should log debug messages")
	}
}

func TestNewLogger_WarnLevel(t *testing.T) {
	var buf bytes.Buffer
	logger := newLogger(&buf, "warn", "json")

	logger.Debug("debug msg")
	logger.Info("info msg")
	logger.Warn("warn msg")

	output := buf.String()
	if strings.Contains(output, "debug msg") {
		t.Error("warn level should not log debug messages")
	}
	if strings.Contains(output, "info msg") {
		t.Error("warn level should not log info messages")
	}
	if !strings.Contains(output, "warn msg") {
		t.Error("warn level should log warn messages")
	}
}

func TestNewLogger_JSONFormat(t *testing.T) {
	var buf bytes.Buffer
	logger := newLogger(&buf, "info", "json")

	logger.Info("hello", "key", "value")

	output := strings.TrimSpace(buf.String())
	var decoded map[string]interface{}
	if err := json.Unmarshal([]byte(output), &decoded); err != nil {
		t.Fatalf("expected valid JSON, got error: %v; output: %s", err, output)
	}
	if decoded["msg"] != "hello" {
		t.Errorf("expected msg=hello, got %v", decoded["msg"])
	}
	if decoded["key"] != "value" {
		t.Errorf("expected key=value, got %v", decoded["key"])
	}
}

func TestNewLogger_TextFormat(t *testing.T) {
	var buf bytes.Buffer
	logger := newLogger(&buf, "info", "text")

	logger.Info("hello", "key", "value")

	output := buf.String()
	if !strings.Contains(output, "msg=hello") {
		t.Errorf("expected text format containing msg=hello, got: %s", output)
	}
	if !strings.Contains(output, "key=value") {
		t.Errorf("expected text format containing key=value, got: %s", output)
	}
}

func TestNewLogger_CaseInsensitive(t *testing.T) {
	tests := []struct {
		level       string
		expectDebug bool // whether debug messages should appear
	}{
		{"DEBUG", true},
		{"Debug", true},
		{"debug", true},
		{"WARN", false},
		{"Warn", false},
		{"warn", false},
		{"ERROR", false},
		{"Error", false},
		{"error", false},
	}

	for _, tt := range tests {
		t.Run(tt.level, func(t *testing.T) {
			var buf bytes.Buffer
			logger := newLogger(&buf, tt.level, "json")
			logger.Debug("test message")

			output := buf.String()
			hasDebug := strings.Contains(output, "test message")
			if hasDebug != tt.expectDebug {
				t.Errorf("level %q: expected debug to appear=%v, got=%v", tt.level, tt.expectDebug, hasDebug)
			}
		})
	}
}

func TestNewLogger_UnknownLevelDefaultsToInfo(t *testing.T) {
	var buf bytes.Buffer
	logger := newLogger(&buf, "invalid_level", "json")

	logger.Debug("debug msg")
	logger.Info("info msg")

	output := buf.String()
	if strings.Contains(output, "debug msg") {
		t.Error("unknown level should default to info, debug should NOT appear")
	}
	if !strings.Contains(output, "info msg") {
		t.Error("unknown level should default to info, info should appear")
	}
}

func TestNewLogger_NonNil(t *testing.T) {
	logger := NewLogger("info", "json")
	if logger == nil {
		t.Fatal("expected non-nil logger")
	}
}
