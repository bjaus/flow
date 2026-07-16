// Package web serves flow's embedded, server-rendered PWA.
package web

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"html/template"
	"io/fs"
	"net/http"
	"strings"

	"github.com/bjaus/flow/app/internal/core"
)

//go:embed static/* templates/*
var assets embed.FS

type Service interface {
	RegisteredWebWorkflows() []WorkflowInfo
	Trigger(context.Context, string, json.RawMessage) (string, error)
	ListRuns(context.Context, core.RunFilter) ([]*core.Run, error)
	GetRun(context.Context, string) (*core.Run, error)
	Decide(context.Context, string, core.Decision) error
	Migrate(context.Context, string, string) error
	ConfigStatus() core.ConfigStatus
	ReloadConfig(context.Context) error
	EventSink() core.EventSink
}

type WorkflowInfo struct {
	Name, Description, InputType, OutputType, Fingerprint string
}

type Handler struct {
	service Service
	tmpl    *template.Template
	static  http.Handler
}

func New(service Service) (http.Handler, error) {
	tmpl, err := template.ParseFS(assets, "templates/*.html")
	if err != nil {
		return nil, fmt.Errorf("parse web templates: %w", err)
	}
	staticFS, err := fs.Sub(assets, "static")
	if err != nil {
		return nil, err
	}
	return &Handler{service: service, tmpl: tmpl, static: http.StripPrefix("/static/", http.FileServer(http.FS(staticFS)))}, nil
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimSuffix(r.URL.Path, "/")
	switch {
	case strings.HasPrefix(r.URL.Path, "/static/"):
		h.static.ServeHTTP(w, r)
	case r.Method == http.MethodGet && (path == "" || path == "/index.html"):
		h.shell(w, r)
	case r.Method == http.MethodGet && path == "/manifest.webmanifest":
		h.staticFile(w, "manifest.webmanifest", "application/manifest+json")
	case r.Method == http.MethodGet && path == "/service-worker.js":
		h.staticFile(w, "service-worker.js", "text/javascript")
	case r.Method == http.MethodGet && path == "/ui/runs":
		h.runs(w, r)
	case r.Method == http.MethodPost && path == "/ui/runs":
		h.trigger(w, r)
	case r.Method == http.MethodGet && path == "/ui/events":
		h.events(w, r)
	case r.Method == http.MethodPost && path == "/ui/config/reload":
		if err := h.service.ReloadConfig(r.Context()); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		_, _ = fmt.Fprint(w, `<span class="notice">Configuration reloaded.</span>`)
	case strings.HasPrefix(path, "/ui/runs/"):
		h.run(w, r, strings.TrimPrefix(path, "/ui/runs/"))
	default:
		http.NotFound(w, r)
	}
}

func (h *Handler) shell(w http.ResponseWriter, r *http.Request) {
	runs, _ := h.service.ListRuns(r.Context(), core.RunFilter{})
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := h.tmpl.ExecuteTemplate(w, "shell", map[string]any{"Runs": runs, "Workflows": h.service.RegisteredWebWorkflows(), "Config": h.service.ConfigStatus()}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (h *Handler) runs(w http.ResponseWriter, r *http.Request) {
	runs, err := h.service.ListRuns(r.Context(), core.RunFilter{Workflow: r.URL.Query().Get("workflow"), Status: core.Status(r.URL.Query().Get("status"))})
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	_ = h.tmpl.ExecuteTemplate(w, "run-list", runs)
}

func (h *Handler) trigger(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	input := json.RawMessage(r.FormValue("input"))
	if len(input) == 0 {
		input = json.RawMessage("null")
	}
	id, err := h.service.Trigger(r.Context(), r.FormValue("workflow"), input)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	run, err := h.service.GetRun(r.Context(), id)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	w.WriteHeader(http.StatusCreated)
	_ = h.tmpl.ExecuteTemplate(w, "run-row", run)
}

func (h *Handler) run(w http.ResponseWriter, r *http.Request, tail string) {
	parts := strings.Split(strings.Trim(tail, "/"), "/")
	if len(parts) == 1 && r.Method == http.MethodGet {
		run, err := h.service.GetRun(r.Context(), parts[0])
		if err != nil {
			http.NotFound(w, r)
			return
		}
		_ = h.tmpl.ExecuteTemplate(w, "run-detail", run)
		return
	}
	if len(parts) == 2 && parts[1] == "decision" && r.Method == http.MethodPost {
		if err := r.ParseForm(); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		d := core.Decision{Outcome: r.FormValue("outcome"), Approved: r.FormValue("approved") == "true", Feedback: r.FormValue("feedback")}
		if d.Outcome != "" {
			d.Approved = d.Outcome == core.OutcomeApprove
		}
		if err := h.service.Decide(r.Context(), parts[0], d); err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		w.Header().Set("HX-Trigger", "flow-refresh")
		_, _ = fmt.Fprint(w, `<p class="notice">Decision submitted.</p>`)
		return
	}
	if len(parts) == 2 && parts[1] == "migration" && r.Method == http.MethodPost {
		if err := r.ParseForm(); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := h.service.Migrate(r.Context(), parts[0], r.FormValue("action")); err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		w.Header().Set("HX-Trigger", "flow-refresh")
		_, _ = fmt.Fprint(w, `<p class="notice">Migration action submitted.</p>`)
		return
	}
	http.NotFound(w, r)
}

func (h *Handler) events(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", 500)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	ch, cancel := h.service.EventSink().Subscribe("")
	defer cancel()
	for {
		select {
		case <-r.Context().Done():
			return
		case e, ok := <-ch:
			if !ok {
				return
			}
			var fragment strings.Builder
			_ = h.tmpl.ExecuteTemplate(&fragment, "event", e)
			switch e.Kind {
			case core.EventRunStarted, core.EventRunFinished, core.EventGateReached, core.EventDecisionApplied, core.EventRunParked, core.EventRunResumed:
				runs, err := h.service.ListRuns(r.Context(), core.RunFilter{})
				if err == nil {
					_ = h.tmpl.ExecuteTemplate(&fragment, "runs-oob", runs)
				}
			}
			data := strings.ReplaceAll(fragment.String(), "\n", "\ndata: ")
			_, _ = fmt.Fprintf(w, "id: %s:%d\nevent: %s\ndata: %s\n\n", e.RunID, e.Seq, e.Kind, data)
			flusher.Flush()
		}
	}
}

func (h *Handler) staticFile(w http.ResponseWriter, name, contentType string) {
	data, err := assets.ReadFile("static/" + name)
	if err != nil {
		http.NotFound(w, nil)
		return
	}
	w.Header().Set("Content-Type", contentType)
	_, _ = w.Write(data)
}
