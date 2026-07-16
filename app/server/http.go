package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/bjaus/flow/app/internal/core"
)

type Service interface {
	RegisteredWorkflows() []WorkflowInfo
	Trigger(context.Context, string, json.RawMessage) (string, error)
	ListRuns(context.Context, core.RunFilter) ([]*core.Run, error)
	GetRun(context.Context, string) (*core.Run, error)
	Decide(context.Context, string, core.Decision) error
	Cancel(context.Context, string) error
	Migrate(context.Context, string, string) error
	ConfigStatus() core.ConfigStatus
	ReloadConfig(context.Context) error
	EventSink() core.EventSink
}

type WorkflowInfo struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	InputType   string `json:"input_type"`
	OutputType  string `json:"output_type"`
	Fingerprint string `json:"fingerprint"`
}

type Handler struct{ service Service }

func New(s Service) http.Handler { return &Handler{service: s} }

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	path := strings.TrimSuffix(r.URL.Path, "/")
	switch {
	case r.Method == http.MethodGet && path == "/api/workflows":
		h.workflows(w)
	case r.Method == http.MethodPost && path == "/api/runs":
		h.trigger(w, r)
	case r.Method == http.MethodGet && path == "/api/runs":
		h.list(w, r)
	case r.Method == http.MethodGet && path == "/api/events":
		h.sse(w, r, "")
	case r.Method == http.MethodGet && path == "/api/config":
		writeJSON(w, http.StatusOK, h.service.ConfigStatus())
	case r.Method == http.MethodPost && path == "/api/config/reload":
		if err := h.service.ReloadConfig(r.Context()); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		writeJSON(w, http.StatusOK, h.service.ConfigStatus())
	case strings.HasPrefix(path, "/api/runs/"):
		h.runRoute(w, r, strings.TrimPrefix(path, "/api/runs/"))
	default:
		writeError(w, http.StatusNotFound, errors.New("not found"))
	}
}

func (h *Handler) workflows(w http.ResponseWriter) {
	writeJSON(w, http.StatusOK, h.service.RegisteredWorkflows())
}
func (h *Handler) trigger(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Workflow string          `json:"workflow"`
		Input    json.RawMessage `json:"input"`
	}
	if err := decode(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	id, err := h.service.Trigger(r.Context(), req.Workflow, req.Input)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"id": id})
}
func (h *Handler) list(w http.ResponseWriter, r *http.Request) {
	runs, err := h.service.ListRuns(r.Context(), core.RunFilter{Workflow: r.URL.Query().Get("workflow"), Status: core.Status(r.URL.Query().Get("status"))})
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, runs)
}
func (h *Handler) runRoute(w http.ResponseWriter, r *http.Request, tail string) {
	parts := strings.Split(tail, "/")
	id := parts[0]
	if id == "" {
		writeError(w, http.StatusNotFound, errors.New("not found"))
		return
	}
	if len(parts) == 1 && r.Method == http.MethodGet {
		run, err := h.service.GetRun(r.Context(), id)
		if err != nil {
			writeError(w, http.StatusNotFound, err)
			return
		}
		writeJSON(w, http.StatusOK, run)
		return
	}
	if len(parts) != 2 {
		writeError(w, http.StatusNotFound, errors.New("not found"))
		return
	}
	switch {
	case parts[1] == "events" && r.Method == http.MethodGet:
		h.sse(w, r, id)
	case parts[1] == "decision" && r.Method == http.MethodPost:
		var d core.Decision
		if err := decode(r, &d); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		if err := h.service.Decide(r.Context(), id, d); err != nil {
			writeError(w, http.StatusConflict, err)
			return
		}
		writeJSON(w, http.StatusAccepted, map[string]string{"status": "queued"})
	case parts[1] == "cancel" && r.Method == http.MethodPost:
		if err := h.service.Cancel(r.Context(), id); err != nil {
			writeError(w, http.StatusConflict, err)
			return
		}
		writeJSON(w, http.StatusAccepted, map[string]string{"status": "cancel_requested"})
	case parts[1] == "migration" && r.Method == http.MethodPost:
		var req struct {
			Action string `json:"action"`
		}
		if err := decode(r, &req); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		if err := h.service.Migrate(r.Context(), id, req.Action); err != nil {
			writeError(w, http.StatusConflict, err)
			return
		}
		writeJSON(w, http.StatusAccepted, map[string]string{"action": req.Action})
	default:
		writeError(w, http.StatusNotFound, errors.New("not found"))
	}
}

func (h *Handler) sse(w http.ResponseWriter, r *http.Request, runID string) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, errors.New("streaming unsupported"))
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	ch, cancel := h.service.EventSink().Subscribe(runID)
	defer cancel()
	for {
		select {
		case <-r.Context().Done():
			return
		case e, ok := <-ch:
			if !ok {
				return
			}
			data, _ := json.Marshal(e)
			_, _ = fmt.Fprintf(w, "id: %s:%d\nevent: %s\ndata: %s\n\n", e.RunID, e.Seq, e.Kind, data)
			flusher.Flush()
		}
	}
}

func decode(r *http.Request, dst any) error {
	defer func() { _ = r.Body.Close() }()
	dec := json.NewDecoder(http.MaxBytesReader(nil, r.Body, 4<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return fmt.Errorf("invalid JSON: %w", err)
	}
	return nil
}
func writeJSON(w http.ResponseWriter, status int, value any) {
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]string{"error": err.Error()})
}
