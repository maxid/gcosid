package gcosid

import "time"

// clockMilli reports the wall clock in milliseconds.
func clockMilli() int64 { return time.Now().UnixMilli() }
