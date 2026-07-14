// Package server exposes the queue engine as a plain HTTP API.
//
// The API is deliberately curl-shaped: message bodies are raw request/
// response payloads (no envelope required to produce), receipts travel in
// query strings, and long-polling is just a GET that takes its time. Any
// HTTP client in any language is a full producer and consumer.
package server

import (
	"net/http"

	"github.com/JaydenCJ/duraq/internal/queue"
	"github.com/JaydenCJ/duraq/internal/version"
)

// MaxWait caps long-poll duration so idle connections cannot pile up
// forever. Matches the SQS ceiling of 20s, tripled for fewer reconnects.
const MaxWait = 60 // seconds

// MaxBodyBytes caps a single message payload (1 MiB). Queues move
// references, not blobs.
const MaxBodyBytes = 1 << 20

// Server routes HTTP requests to a queue engine.
type Server struct {
	engine *queue.Engine
	mux    *http.ServeMux
}

// New wires up all routes against e.
func New(e *queue.Engine) *Server {
	s := &Server{engine: e, mux: http.NewServeMux()}
	s.mux.HandleFunc("GET /healthz", s.handleHealth)
	s.mux.HandleFunc("GET /version", s.handleVersion)
	s.mux.HandleFunc("GET /q", s.handleListQueues)
	s.mux.HandleFunc("PUT /q/{name}", s.handleCreateQueue)
	s.mux.HandleFunc("GET /q/{name}", s.handleQueueStats)
	s.mux.HandleFunc("DELETE /q/{name}", s.handleDeleteQueue)
	s.mux.HandleFunc("POST /q/{name}/messages", s.handleSend)
	s.mux.HandleFunc("GET /q/{name}/messages", s.handleReceive)
	s.mux.HandleFunc("DELETE /q/{name}/messages/{id}", s.handleAck)
	s.mux.HandleFunc("POST /q/{name}/messages/{id}/nack", s.handleNack)
	s.mux.HandleFunc("POST /q/{name}/messages/{id}/extend", s.handleExtend)
	s.mux.HandleFunc("POST /q/{name}/redrive", s.handleRedrive)
	return s
}

// ServeHTTP implements http.Handler.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "queues": len(s.engine.ListStats())})
}

func (s *Server) handleVersion(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"name": "duraq", "version": version.Version})
}
