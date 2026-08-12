package slog

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strconv"
)

const (
	defaultLogMaxLineBytes = 4096
	minLogMaxLineBytes     = 512
	minimalValueBudget     = 16
)

// The order is also the priority used when building a minimal record.
var preservedLogKeys = []string{
	"service",
	"service.id",
	"service.name",
	"service.version",
	"module",
	"trace_id",
	"span_id",
	"trace_flags",
}

// boundedWriter relies on JSONHandler's guarantee that each record is sent in
// one serialized Write call.
type boundedWriter struct {
	out          io.Writer
	maxLineBytes int
}

func (w boundedWriter) Write(p []byte) (int, error) {
	line, newline := bytes.CutSuffix(p, []byte("\n"))
	if len(line) <= w.maxLineBytes {
		return w.out.Write(p)
	}

	line, err := truncateLine(line, w.maxLineBytes)
	if err != nil {
		return 0, err
	}
	return w.writeLine(p, line, newline)
}

func truncateLine(line []byte, maxLineBytes int) ([]byte, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(line, &fields); err != nil {
		return nil, fmt.Errorf("decode slog JSON: %w", err)
	}
	fields["log_truncated"] = json.RawMessage("true")
	fields["log_original_bytes"] = json.RawMessage(strconv.Itoa(len(line)))
	fields["log_max_bytes"] = json.RawMessage(strconv.Itoa(maxLineBytes))

	for budget := minLogMaxLineBytes; ; budget /= 2 {
		candidate := truncateFields(fields, budget)
		encoded, err := json.Marshal(candidate)
		if err != nil {
			return nil, fmt.Errorf("encode truncated slog JSON: %w", err)
		}
		if len(encoded) <= maxLineBytes {
			return encoded, nil
		}
		if budget == 0 {
			break
		}
	}

	encoded, err := minimalLine(fields, maxLineBytes)
	if err != nil {
		return nil, fmt.Errorf("encode minimal slog JSON: %w", err)
	}
	return encoded, nil
}

func (w boundedWriter) writeLine(original, line []byte, newline bool) (int, error) {
	if newline {
		line = append(line, '\n')
	}
	if n, err := w.out.Write(line); err != nil {
		return 0, err
	} else if n != len(line) {
		return 0, io.ErrShortWrite
	}
	return len(original), nil
}

func truncateFields(fields map[string]json.RawMessage, budget int) map[string]json.RawMessage {
	truncated := make(map[string]json.RawMessage, len(fields))
	for key, value := range fields {
		if slices.Contains(preservedLogKeys, key) {
			truncated[key] = value
			continue
		}
		truncated[key] = truncateValue(value, budget)
	}
	return truncated
}

func minimalLine(fields map[string]json.RawMessage, maxLineBytes int) ([]byte, error) {
	minimal := map[string]json.RawMessage{
		"log_truncated":      fields["log_truncated"],
		"log_original_bytes": fields["log_original_bytes"],
		"log_max_bytes":      fields["log_max_bytes"],
	}
	for _, key := range []string{"time", "level", "msg"} {
		if value, ok := fields[key]; ok {
			minimal[key] = truncateValue(value, minimalValueBudget)
		}
	}
	line, err := json.Marshal(minimal)
	if err != nil {
		return nil, err
	}
	if len(line) > maxLineBytes {
		return nil, fmt.Errorf("required fields exceed %d bytes", maxLineBytes)
	}

	for _, key := range preservedLogKeys {
		value, ok := fields[key]
		if !ok {
			continue
		}
		minimal[key] = truncateValue(value, minimalValueBudget)
		encoded, err := json.Marshal(minimal)
		if err != nil {
			return nil, err
		}
		if len(encoded) > maxLineBytes {
			delete(minimal, key)
			continue
		}
		line = encoded
	}
	return line, nil
}

func truncateValue(value json.RawMessage, budget int) json.RawMessage {
	if len(value) <= budget {
		return value
	}

	if len(value) > 0 && value[0] == '"' {
		var text string
		if err := json.Unmarshal(value, &text); err == nil {
			encoded, _ := json.Marshal(truncateString(text, budget))
			return encoded
		}
	}
	if len(value) <= minimalValueBudget {
		return value
	}
	encoded, _ := json.Marshal(fmt.Sprintf("<truncated bytes=%d>", len(value)))
	return encoded
}

func truncateString(value string, budget int) string {
	runes := []rune(value)
	length := len(runes)
	if length <= budget {
		return value
	}
	if budget <= 0 {
		return fmt.Sprintf("<truncated len=%d>", length)
	}

	suffix := fmt.Sprintf("...<truncated len=%d>", length)
	suffixRunes := []rune(suffix)
	if budget <= len(suffixRunes) {
		return string(suffixRunes[:budget])
	}
	return string(runes[:budget-len(suffixRunes)]) + suffix
}
