// HTTP handlers for queue and message operations.
package server

import (
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/JaydenCJ/duraq/internal/dur"
	"github.com/JaydenCJ/duraq/internal/queue"
)

// --- queues ---------------------------------------------------------------

func (s *Server) handleListQueues(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"queues": s.engine.ListStats()})
}

func (s *Server) handleCreateQueue(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, MaxBodyBytes))
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", "could not read body: "+err.Error())
		return
	}
	cfg, err := queue.ParseConfig(body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_config", err.Error())
		return
	}
	created, err := s.engine.CreateQueue(name, cfg)
	if err != nil {
		writeEngineError(w, err)
		return
	}
	stats, err := s.engine.Stats(name)
	if err != nil {
		writeEngineError(w, err)
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	writeJSON(w, status, stats)
}

func (s *Server) handleQueueStats(w http.ResponseWriter, r *http.Request) {
	stats, err := s.engine.Stats(r.PathValue("name"))
	if err != nil {
		writeEngineError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, stats)
}

func (s *Server) handleDeleteQueue(w http.ResponseWriter, r *http.Request) {
	if err := s.engine.DeleteQueue(r.PathValue("name")); err != nil {
		writeEngineError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// --- messages ---------------------------------------------------------------

func (s *Server) handleSend(w http.ResponseWriter, r *http.Request) {
	delay, err := dur.ParseDefault(r.URL.Query().Get("delay"), 0)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_delay", err.Error())
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, MaxBodyBytes))
	if err != nil {
		writeError(w, http.StatusRequestEntityTooLarge, "body_too_large",
			"message bodies are capped at "+strconv.Itoa(MaxBodyBytes)+" bytes")
		return
	}
	id, err := s.engine.Send(r.PathValue("name"), body, delay)
	if err != nil {
		writeEngineError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]string{"id": id})
}

// deliveryWire is one received message on the wire. Body is a plain string
// when the payload is valid UTF-8, base64 in body_b64 otherwise — the same
// convention as the write-ahead log.
type deliveryWire struct {
	ID       string `json:"id"`
	Receipt  string `json:"receipt"`
	Body     string `json:"body,omitempty"`
	BodyB64  string `json:"body_b64,omitempty"`
	Receives int    `json:"receives"`
	SentAt   string `json:"sent_at"`
}

func toWire(d queue.Delivery) deliveryWire {
	w := deliveryWire{
		ID: d.ID, Receipt: d.Receipt, Receives: d.Receives,
		SentAt: d.SentAt.UTC().Format(time.RFC3339Nano),
	}
	if utf8.Valid(d.Body) {
		w.Body = string(d.Body)
	} else {
		w.BodyB64 = base64.StdEncoding.EncodeToString(d.Body)
	}
	return w
}

func (s *Server) handleReceive(w http.ResponseWriter, r *http.Request) {
	qs := r.URL.Query()
	wait, err := dur.ParseDefault(qs.Get("wait"), 0)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_wait", err.Error())
		return
	}
	if wait > MaxWait*time.Second {
		writeError(w, http.StatusBadRequest, "bad_wait",
			"wait is capped at "+strconv.Itoa(MaxWait)+"s")
		return
	}
	visibility, err := dur.ParseDefault(qs.Get("visibility"), 0)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_visibility", err.Error())
		return
	}
	max := 1
	if m := qs.Get("max"); m != "" {
		max, err = strconv.Atoi(m)
		if err != nil || max < 1 || max > 100 {
			writeError(w, http.StatusBadRequest, "bad_max", "max must be an integer between 1 and 100")
			return
		}
	}
	ds, err := s.engine.Receive(r.Context(), r.PathValue("name"), max, wait, visibility)
	if err != nil {
		if errors.Is(err, r.Context().Err()) && r.Context().Err() != nil {
			return // client went away mid-poll; nothing to write
		}
		writeEngineError(w, err)
		return
	}
	if len(ds) == 0 {
		w.WriteHeader(http.StatusNoContent) // long-poll elapsed empty-handed
		return
	}
	out := make([]deliveryWire, len(ds))
	for i, d := range ds {
		out[i] = toWire(d)
	}
	writeJSON(w, http.StatusOK, map[string]any{"messages": out})
}

// leaseParams pulls the queue/id/receipt triple every lease operation needs.
func leaseParams(w http.ResponseWriter, r *http.Request) (name, id, receipt string, ok bool) {
	name, id = r.PathValue("name"), r.PathValue("id")
	receipt = r.URL.Query().Get("receipt")
	if receipt == "" {
		writeError(w, http.StatusBadRequest, "missing_receipt",
			"pass the receipt from the receive response as ?receipt=")
		return "", "", "", false
	}
	return name, id, receipt, true
}

func (s *Server) handleAck(w http.ResponseWriter, r *http.Request) {
	name, id, receipt, ok := leaseParams(w, r)
	if !ok {
		return
	}
	if err := s.engine.Ack(name, id, receipt); err != nil {
		writeEngineError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleNack(w http.ResponseWriter, r *http.Request) {
	name, id, receipt, ok := leaseParams(w, r)
	if !ok {
		return
	}
	if err := s.engine.Nack(name, id, receipt); err != nil {
		writeEngineError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleExtend(w http.ResponseWriter, r *http.Request) {
	name, id, receipt, ok := leaseParams(w, r)
	if !ok {
		return
	}
	visibility, err := dur.ParseDefault(r.URL.Query().Get("visibility"), queue.DefaultVisibility)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_visibility", err.Error())
		return
	}
	if err := s.engine.Extend(name, id, receipt, visibility); err != nil {
		writeEngineError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleRedrive(w http.ResponseWriter, r *http.Request) {
	qs := r.URL.Query()
	to := qs.Get("to")
	if to == "" {
		writeError(w, http.StatusBadRequest, "missing_target",
			"pass the destination queue as ?to=")
		return
	}
	max := 0 // 0 = everything ready
	if m := qs.Get("max"); m != "" {
		var err error
		max, err = strconv.Atoi(m)
		if err != nil || max < 1 {
			writeError(w, http.StatusBadRequest, "bad_max", "max must be a positive integer")
			return
		}
	}
	moved, err := s.engine.Redrive(r.PathValue("name"), to, max)
	if err != nil {
		writeEngineError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]int{"moved": moved})
}
