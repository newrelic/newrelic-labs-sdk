package model

import (
	"time"
)

// Log model variant.
type Log struct {
	Message string
	Timestamp int64
	Attributes map[string]interface{}
}

// Make a log.
func NewLog(message string, attributes map[string]interface{}, timestamp time.Time) Log {
	attrs := attributes
	if attrs == nil {
		attrs = make(map[string]any)
	}

	return Log{
		Message: message,
		Timestamp: timestamp.UnixMilli(),
		Attributes: attrs,
	}
}
