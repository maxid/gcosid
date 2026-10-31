package gcosid

import "errors"

// errorIs is a small indirection so error matching stays in one place.
func errorIs(err, target error) bool { return errors.Is(err, target) }

// nowMillis is the default millisecond clock, isolated for tests.
var nowMillis = func() int64 { return clockMilli() }
