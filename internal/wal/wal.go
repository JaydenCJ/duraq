// Append-only NDJSON log with torn-tail recovery and atomic compaction.
//
// Durability model: one file, one JSON record per line, appended in commit
// order. A crash mid-write can only damage the final line ("torn tail"), so
// replay accepts and truncates an unparseable last line but treats damage
// anywhere else as corruption and refuses to guess. Compaction rewrites the
// live state into a temp file and renames it over the log, so a crash during
// compaction leaves either the old log or the new one — never a mix.
package wal

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// FileName is the log's name inside a duraq data directory.
const FileName = "wal.ndjson"

// Log is an open write-ahead log positioned for appending.
type Log struct {
	path     string
	f        *os.File
	w        *bufio.Writer
	syncEach bool
}

// ReadAll parses every record in the file at path. A missing file yields an
// empty slice. The returned validLen is the byte offset after the last good
// record; tornTail reports whether trailing bytes past validLen had to be
// discarded (crash recovery) — callers should log that, not fail on it.
func ReadAll(path string) (recs []Record, validLen int64, tornTail bool, err error) {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, 0, false, nil
	}
	if err != nil {
		return nil, 0, false, err
	}
	var offset int64
	for lineNo := 1; len(data) > 0; lineNo++ {
		line := data
		rest := []byte(nil)
		if i := bytes.IndexByte(data, '\n'); i >= 0 {
			line, rest = data[:i], data[i+1:]
		} else {
			// No trailing newline: the final append never completed.
			return recs, offset, true, nil
		}
		var r Record
		if uerr := json.Unmarshal(line, &r); uerr != nil {
			if len(rest) == 0 {
				// Damage confined to the last line: recoverable torn tail.
				return recs, offset, true, nil
			}
			return nil, 0, false, fmt.Errorf("%s:%d: corrupt record: %v", path, lineNo, uerr)
		}
		if verr := r.Validate(); verr != nil {
			return nil, 0, false, fmt.Errorf("%s:%d: %v", path, lineNo, verr)
		}
		recs = append(recs, r)
		offset += int64(len(line)) + 1
		data = rest
	}
	return recs, offset, false, nil
}

// Open reads the existing log (if any), truncates a torn tail, and returns
// the log opened for appending plus the replayable records. When syncEach is
// true every Append fsyncs before returning (crash-safe at the cost of
// latency); when false the OS decides when to flush.
func Open(dir string, syncEach bool) (*Log, []Record, bool, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, nil, false, err
	}
	path := filepath.Join(dir, FileName)
	recs, validLen, torn, err := ReadAll(path)
	if err != nil {
		return nil, nil, false, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, nil, false, err
	}
	if torn {
		if err := f.Truncate(validLen); err != nil {
			f.Close()
			return nil, nil, false, fmt.Errorf("truncate torn tail: %w", err)
		}
	}
	if _, err := f.Seek(validLen, 0); err != nil {
		f.Close()
		return nil, nil, false, err
	}
	return &Log{path: path, f: f, w: bufio.NewWriter(f), syncEach: syncEach}, recs, torn, nil
}

// Append writes one record and, in sync mode, fsyncs it to disk. A record is
// only considered committed once Append returns nil.
func (l *Log) Append(r Record) error {
	if err := r.Validate(); err != nil {
		return fmt.Errorf("refusing to append invalid record: %w", err)
	}
	b, err := json.Marshal(r)
	if err != nil {
		return err
	}
	if _, err := l.w.Write(b); err != nil {
		return err
	}
	if err := l.w.WriteByte('\n'); err != nil {
		return err
	}
	if err := l.w.Flush(); err != nil {
		return err
	}
	if l.syncEach {
		if err := l.f.Sync(); err != nil {
			return err
		}
	}
	return nil
}

// Compact atomically replaces the log with recs (a snapshot of live state).
// The old file handle is swapped for the new one on success.
func (l *Log) Compact(recs []Record) error {
	tmp, err := os.CreateTemp(filepath.Dir(l.path), FileName+".compact-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name()) // no-op after successful rename
	w := bufio.NewWriter(tmp)
	for _, r := range recs {
		b, err := json.Marshal(r)
		if err != nil {
			tmp.Close()
			return err
		}
		if _, err := w.Write(append(b, '\n')); err != nil {
			tmp.Close()
			return err
		}
	}
	if err := w.Flush(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp.Name(), l.path); err != nil {
		return err
	}
	f, err := os.OpenFile(l.path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	l.f.Close()
	l.f = f
	l.w = bufio.NewWriter(f)
	return nil
}

// Close flushes and closes the underlying file.
func (l *Log) Close() error {
	if err := l.w.Flush(); err != nil {
		l.f.Close()
		return err
	}
	return l.f.Close()
}
