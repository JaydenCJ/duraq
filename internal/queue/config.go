// Queue configuration and its JSON wire format.
//
// The same encoding is used in the HTTP API and in qcreate records of the
// write-ahead log, so a queue's config is auditable straight from the log.
package queue

import (
	"encoding/json"
	"fmt"
	"regexp"
	"time"

	"github.com/JaydenCJ/duraq/internal/dur"
)

// DefaultVisibility is how long a received message stays invisible when the
// queue and the request specify nothing. Matches the SQS default.
const DefaultVisibility = 30 * time.Second

// MaxVisibility caps lease lengths so a stuck consumer cannot park a message
// forever. 12 hours matches the SQS ceiling.
const MaxVisibility = 12 * time.Hour

// MaxDelay caps per-message delivery delays.
const MaxDelay = 15 * time.Minute

var nameRe = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,119}$`)

// ValidName reports whether s is an acceptable queue name: 1-120 chars of
// letters, digits, dot, underscore, hyphen, not starting with punctuation.
func ValidName(s string) bool { return nameRe.MatchString(s) }

// Config holds a queue's tunables.
type Config struct {
	// Visibility is the default lease length for received messages.
	Visibility time.Duration
	// MaxReceives dead-letters a message once it has been received this many
	// times without an ack. 0 disables the limit.
	MaxReceives int
	// DeadLetter names the queue that poisoned messages move to. Empty with
	// MaxReceives > 0 means poisoned messages are dropped (and logged).
	DeadLetter string
}

// DefaultConfig returns the config applied when a queue is created with an
// empty body.
func DefaultConfig() Config { return Config{Visibility: DefaultVisibility} }

type configWire struct {
	Visibility  string `json:"visibility_timeout"`
	MaxReceives int    `json:"max_receives"`
	DeadLetter  string `json:"dead_letter,omitempty"`
}

// MarshalJSON encodes durations as strings ("30s") for readability.
func (c Config) MarshalJSON() ([]byte, error) {
	return json.Marshal(configWire{
		Visibility:  c.Visibility.String(),
		MaxReceives: c.MaxReceives,
		DeadLetter:  c.DeadLetter,
	})
}

// UnmarshalJSON accepts "30s"-style strings or bare seconds for
// visibility_timeout; absent fields keep their zero values, which
// ParseConfig then fills with defaults.
func (c *Config) UnmarshalJSON(b []byte) error {
	var w struct {
		Visibility  json.RawMessage `json:"visibility_timeout"`
		MaxReceives int             `json:"max_receives"`
		DeadLetter  string          `json:"dead_letter"`
	}
	if err := json.Unmarshal(b, &w); err != nil {
		return err
	}
	c.MaxReceives = w.MaxReceives
	c.DeadLetter = w.DeadLetter
	c.Visibility = 0
	if len(w.Visibility) > 0 {
		var s string
		if err := json.Unmarshal(w.Visibility, &s); err != nil {
			// Not a JSON string: try a bare number of seconds.
			var n float64
			if err := json.Unmarshal(w.Visibility, &n); err != nil {
				return fmt.Errorf("visibility_timeout must be a duration string or seconds")
			}
			s = fmt.Sprintf("%vs", n)
		}
		d, err := dur.Parse(s)
		if err != nil {
			return err
		}
		c.Visibility = d
	}
	return nil
}

// ParseConfig decodes an API/WAL config payload, applies defaults, and
// validates ranges. An empty payload yields DefaultConfig.
func ParseConfig(b []byte) (Config, error) {
	c := DefaultConfig()
	if len(b) > 0 {
		c = Config{}
		if err := json.Unmarshal(b, &c); err != nil {
			return Config{}, err
		}
	}
	if c.Visibility == 0 {
		c.Visibility = DefaultVisibility
	}
	if c.Visibility < 0 || c.Visibility > MaxVisibility {
		return Config{}, fmt.Errorf("visibility_timeout must be between 0s and %s", MaxVisibility)
	}
	if c.MaxReceives < 0 {
		return Config{}, fmt.Errorf("max_receives must be >= 0")
	}
	if c.DeadLetter != "" && !ValidName(c.DeadLetter) {
		return Config{}, fmt.Errorf("dead_letter %q is not a valid queue name", c.DeadLetter)
	}
	return c, nil
}
