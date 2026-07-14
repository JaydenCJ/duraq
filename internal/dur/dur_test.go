// Tests for the human-friendly duration parser: bare numbers are seconds,
// Go duration strings pass through, negatives and garbage are rejected.
package dur

import (
	"testing"
	"time"
)

func TestParseGoDurationString(t *testing.T) {
	d, err := Parse("30s")
	if err != nil || d != 30*time.Second {
		t.Fatalf("Parse(30s) = %v, %v", d, err)
	}
}

func TestParseCompoundDuration(t *testing.T) {
	d, err := Parse("1m30s")
	if err != nil || d != 90*time.Second {
		t.Fatalf("Parse(1m30s) = %v, %v", d, err)
	}
}

func TestParseBareIntegerMeansSeconds(t *testing.T) {
	// Query strings like ?wait=20 must read as 20 seconds, not 20ns.
	d, err := Parse("20")
	if err != nil || d != 20*time.Second {
		t.Fatalf("Parse(20) = %v, %v", d, err)
	}
}

func TestParseBareDecimalMeansSeconds(t *testing.T) {
	d, err := Parse("0.5")
	if err != nil || d != 500*time.Millisecond {
		t.Fatalf("Parse(0.5) = %v, %v", d, err)
	}
}

func TestParseRejectsNegativeNumber(t *testing.T) {
	if _, err := Parse("-5"); err == nil {
		t.Fatal("Parse(-5) should fail")
	}
}

func TestParseRejectsNegativeDuration(t *testing.T) {
	if _, err := Parse("-5s"); err == nil {
		t.Fatal("Parse(-5s) should fail")
	}
}

func TestParseRejectsGarbage(t *testing.T) {
	if _, err := Parse("soon"); err == nil {
		t.Fatal("Parse(soon) should fail")
	}
}

func TestParseDefaultUsesDefaultOnEmpty(t *testing.T) {
	d, err := ParseDefault("", 7*time.Second)
	if err != nil || d != 7*time.Second {
		t.Fatalf("ParseDefault = %v, %v", d, err)
	}
}
