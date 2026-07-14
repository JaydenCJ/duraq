// Package dur parses human-friendly durations for the HTTP API and CLI.
//
// duraq accepts both Go duration strings ("30s", "1m30s", "250ms") and bare
// numbers, which are interpreted as seconds ("30" == "30s"). Bare numbers
// keep query strings curl-friendly: ?wait=20 reads naturally.
package dur

import (
	"fmt"
	"strconv"
	"time"
)

// Parse converts s into a non-negative duration. Bare integers and decimals
// are read as seconds; anything else must be a valid Go duration string.
func Parse(s string) (time.Duration, error) {
	if s == "" {
		return 0, fmt.Errorf("empty duration")
	}
	if n, err := strconv.ParseFloat(s, 64); err == nil {
		d := time.Duration(n * float64(time.Second))
		if d < 0 {
			return 0, fmt.Errorf("duration %q is negative", s)
		}
		return d, nil
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("invalid duration %q (use 30s, 1m, or a number of seconds)", s)
	}
	if d < 0 {
		return 0, fmt.Errorf("duration %q is negative", s)
	}
	return d, nil
}

// ParseDefault behaves like Parse but returns def when s is empty.
func ParseDefault(s string, def time.Duration) (time.Duration, error) {
	if s == "" {
		return def, nil
	}
	return Parse(s)
}
