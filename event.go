package bus

import "time"

// Event is data structure for any logs
type Event struct {
	ID          string      `json:"id"`
	TxID        string      `json:"tx_id"`
	Topic       string      `json:"topic"`
	Source      string      `json:"source"`
	ScheduledAt time.Time   `json:"scheduled_at"`
	CreatedAt   time.Time   `json:"created_at"`
	Data        interface{} `json:"data"`
}

// EventOption is a function type to mutate event fields
type EventOption = func(Event) Event

// WithID returns an option to set event's id field
func WithID(id string) EventOption {
	return func(e Event) Event {
		e.ID = id
		return e
	}
}

// WithTxID returns an option to set event's txID field
func WithTxID(txID string) EventOption {
	return func(e Event) Event {
		e.TxID = txID
		return e
	}
}

// WithSource returns an option to set event's source field
func WithSource(source string) EventOption {
	return func(e Event) Event {
		e.Source = source
		return e
	}
}

// WithScheduledAt returns an option to set event's occurredAt field
func WithScheduledAt(time time.Time) EventOption {
	return func(e Event) Event {
		e.ScheduledAt = time
		return e
	}
}
