package domain

import "time"

// RequestLog is the analytics event emitted once per proxied request. It is the
// schema written to Kafka and consumed downstream for usage reports, top-consumer
// analysis, and error-rate monitoring.
//
// Keep this stable and additive: consumers (possibly written in other languages,
// possibly running against historical data) depend on the field names. Add new
// fields; don't rename or repurpose old ones.
type RequestLog struct {
	Time       time.Time `json:"time"`
	RequestID  string    `json:"request_id"`
	RouteID    string    `json:"route_id"`
	RouteName  string    `json:"route_name"`
	ConsumerID string    `json:"consumer_id"` // empty for anonymous
	Method     string    `json:"method"`
	Path       string    `json:"path"`
	Status     int       `json:"status"`
	Upstream   string    `json:"upstream"` // chosen upstream URL, empty if none/aborted
	DurationMs int64     `json:"duration_ms"`
	ClientIP   string    `json:"client_ip"`
	BytesOut   int64     `json:"bytes_out"`
}
