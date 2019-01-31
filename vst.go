package goswarm

import "time"

type CacheTimes struct {
	// UpdatedAt represents the most recent time the associated entry was
	// modified.
	UpdatedAt time.Time

	// StaleAt represents the time before which the item has not gone
	// stale. When the zero-value, the entry is never considered stale.
	StaleAt time.Time

	// ExpiresAt represents the time before which the item has not expired. When
	// the zero-value, the entry never expires.
	ExpiresAt time.Time
}

type VST uint32

const (
	ValueNone VST = iota
	ValueError
	ValueFresh
	ValueStale
)
