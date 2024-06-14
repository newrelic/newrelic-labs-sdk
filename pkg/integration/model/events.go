package model

import "time"

// Event model
type Event struct {
	Type string
	Timestamp int64
	Attributes map[string]any
}

// Make a event.
func NewEvent(eventType string, attributes map[string]any, timestamp time.Time) Event {
	attrs := attributes
	if attrs == nil {
		attrs = make(map[string]any)
	}

	return Event{
		Type: eventType,
		Timestamp:  timestamp.UnixMilli(),
		Attributes: attrs,
	}
}
