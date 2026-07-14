// Record types for the NDJSON write-ahead log.
//
// Every state transition in duraq is one JSON object on one line. The log is
// deliberately human-readable: `tail -f data/wal.ndjson` is a valid way to
// watch a queue, and `jq` is a valid way to audit it. Field names are short
// but unambiguous; message bodies are stored as plain strings whenever they
// are valid UTF-8, and base64 otherwise.
package wal

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"unicode/utf8"
)

// Op names every record type the log can contain.
const (
	OpQueueCreate = "qcreate" // create or reconfigure a queue
	OpQueueDelete = "qdelete" // delete a queue and all its messages
	OpSend        = "send"    // enqueue a message
	OpReceive     = "recv"    // lease a message to a consumer
	OpAck         = "ack"     // delete a message after successful processing
	OpNack        = "nack"    // return a leased message to the queue
	OpExtend      = "extend"  // push a lease's visibility deadline forward
	OpDead        = "dead"    // move a poisoned message to the dead-letter queue
	OpMove        = "move"    // redrive a message between queues
)

// Record is one line of the write-ahead log. Fields are omitted when empty
// so each op stays compact; see docs/wal-format.md for the per-op schema.
type Record struct {
	Op       string          `json:"op"`
	Queue    string          `json:"q,omitempty"`
	ID       string          `json:"id,omitempty"`
	Body     string          `json:"body,omitempty"`
	BodyB64  string          `json:"body_b64,omitempty"`
	TS       int64           `json:"ts,omitempty"`       // event time, unix milliseconds
	NBF      int64           `json:"nbf,omitempty"`      // not-before (delay), unix milliseconds
	Deadline int64           `json:"deadline,omitempty"` // visibility deadline, unix milliseconds
	Receipt  string          `json:"receipt,omitempty"`
	Count    int             `json:"count,omitempty"` // receive count
	To       string          `json:"to,omitempty"`    // target queue for dead / move
	Config   json.RawMessage `json:"cfg,omitempty"`   // queue config for qcreate
}

// SetBody stores b as a plain JSON string when it is valid UTF-8, otherwise
// as base64. Plain strings keep the log greppable; base64 keeps it lossless.
func (r *Record) SetBody(b []byte) {
	if utf8.Valid(b) {
		r.Body = string(b)
		r.BodyB64 = ""
		return
	}
	r.Body = ""
	r.BodyB64 = base64.StdEncoding.EncodeToString(b)
}

// GetBody reverses SetBody.
func (r *Record) GetBody() ([]byte, error) {
	if r.BodyB64 != "" {
		return base64.StdEncoding.DecodeString(r.BodyB64)
	}
	return []byte(r.Body), nil
}

// Validate rejects records that could not have been written by duraq. It is
// applied on replay so a corrupted or hand-edited log fails loudly instead
// of rebuilding nonsense state.
func (r *Record) Validate() error {
	switch r.Op {
	case OpQueueCreate, OpQueueDelete:
		if r.Queue == "" {
			return fmt.Errorf("%s record missing queue name", r.Op)
		}
	case OpSend, OpReceive, OpAck, OpNack, OpExtend, OpDead, OpMove:
		if r.Queue == "" {
			return fmt.Errorf("%s record missing queue name", r.Op)
		}
		if r.ID == "" {
			return fmt.Errorf("%s record missing message id", r.Op)
		}
	case "":
		return fmt.Errorf("record missing op")
	default:
		return fmt.Errorf("unknown op %q", r.Op)
	}
	if r.Op == OpReceive && r.Deadline == 0 {
		return fmt.Errorf("recv record missing deadline")
	}
	if r.Op == OpMove && r.To == "" {
		return fmt.Errorf("move record missing target queue")
	}
	return nil
}
