// JSON response helpers and the error envelope.
//
// Every error the API returns is `{"error": {"code": ..., "message": ...}}`
// with an appropriate status, so `jq .error.message` always works.
package server

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/JaydenCJ/duraq/internal/queue"
)

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

type errorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func writeError(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, map[string]errorBody{"error": {Code: code, Message: msg}})
}

// writeEngineError maps the engine's typed errors onto HTTP semantics:
// unknown resources are 404, a lost lease is 409 (someone else may hold the
// message now), and bad input is 400.
func writeEngineError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, queue.ErrQueueNotFound):
		writeError(w, http.StatusNotFound, "queue_not_found", err.Error())
	case errors.Is(err, queue.ErrMessageNotFound):
		writeError(w, http.StatusNotFound, "message_not_found", err.Error())
	case errors.Is(err, queue.ErrNotLeased), errors.Is(err, queue.ErrWrongReceipt):
		writeError(w, http.StatusConflict, "lease_lost",
			err.Error()+" (the visibility timeout may have expired)")
	case errors.Is(err, queue.ErrBadName):
		writeError(w, http.StatusBadRequest, "bad_queue_name", err.Error())
	default:
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
	}
}
