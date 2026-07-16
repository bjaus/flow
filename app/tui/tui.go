// Package tui implements the Bubble Tea client for a flow daemon.
package tui

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/bjaus/flow/app/internal/core"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

type Client struct {
	Endpoint string
	HTTP     *http.Client
	ctx      context.Context
	stream   io.ReadCloser
	scanner  *bufio.Scanner
}
type eventMsg core.Event
type runsMsg []*core.Run
type errMsg error
type configMsg core.ConfigStatus

type Model struct {
	client      *Client
	runs        []*core.Run
	selected    int
	pipeline    []string
	transcript  string
	err         error
	width       int
	filter      core.Status
	configDirty bool
}

func New(endpoint string) Model {
	return Model{client: &Client{Endpoint: strings.TrimRight(endpoint, "/"), HTTP: &http.Client{}, ctx: context.Background()}}
}

func Run(ctx context.Context, endpoint string) error {
	m := New(endpoint)
	m.client.ctx = ctx
	p := tea.NewProgram(m, tea.WithContext(ctx), tea.WithAltScreen())
	_, err := p.Run()
	return err
}

func (m Model) Init() tea.Cmd { return tea.Batch(m.loadRuns(), m.loadConfig(), m.nextEvent()) }
func (m Model) loadConfig() tea.Cmd {
	return func() tea.Msg {
		req, _ := http.NewRequestWithContext(m.client.ctx, http.MethodGet, m.client.Endpoint+"/api/config", nil)
		resp, err := m.client.HTTP.Do(req)
		if err != nil {
			return errMsg(err)
		}
		defer func() { _ = resp.Body.Close() }()
		var status core.ConfigStatus
		if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
			return errMsg(err)
		}
		return configMsg(status)
	}
}
func (m Model) loadRuns() tea.Cmd {
	return func() tea.Msg {
		req, _ := http.NewRequestWithContext(m.client.ctx, http.MethodGet, m.client.Endpoint+"/api/runs", nil)
		resp, err := m.client.HTTP.Do(req)
		if err != nil {
			return errMsg(err)
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != http.StatusOK {
			return errMsg(fmt.Errorf("runs: %s", resp.Status))
		}
		var runs []*core.Run
		if err := json.NewDecoder(resp.Body).Decode(&runs); err != nil {
			return errMsg(err)
		}
		return runsMsg(runs)
	}
}
func (m Model) nextEvent() tea.Cmd {
	return func() tea.Msg {
		if m.client.scanner == nil {
			req, _ := http.NewRequestWithContext(m.client.ctx, http.MethodGet, m.client.Endpoint+"/api/events", nil)
			resp, err := m.client.HTTP.Do(req)
			if err != nil {
				return errMsg(err)
			}
			m.client.stream, m.client.scanner = resp.Body, bufio.NewScanner(resp.Body)
		}
		for m.client.scanner.Scan() {
			line := m.client.scanner.Text()
			if strings.HasPrefix(line, "data: ") {
				var e core.Event
				if json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &e) == nil {
					return eventMsg(e)
				}
			}
		}
		err := m.client.scanner.Err()
		_ = m.client.stream.Close()
		m.client.scanner, m.client.stream = nil, nil
		if err != nil {
			return errMsg(err)
		}
		return errMsg(io.EOF)
	}
}

func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch v := msg.(type) {
	case tea.KeyMsg:
		switch v.String() {
		case "q", "ctrl+c":
			if m.client.stream != nil {
				_ = m.client.stream.Close()
			}
			return m, tea.Quit
		case "up", "k":
			if m.selected > 0 {
				m.selected--
			}
		case "down", "j":
			if m.selected < len(m.runs)-1 {
				m.selected++
			}
		case "f":
			filters := []core.Status{"", core.StatusRunning, core.StatusAwaitingReview, core.StatusSucceeded, core.StatusFailed}
			for i, status := range filters {
				if status == m.filter {
					m.filter = filters[(i+1)%len(filters)]
					break
				}
			}
		case "a":
			return m, m.decision(true)
		case "r":
			return m, m.decision(false)
		case "1":
			return m, m.migration("restart")
		case "2":
			return m, m.migration("abandon")
		case "3":
			return m, m.migration("finish_on_previous")
		case "c":
			return m, m.reloadConfig()
		}
	case tea.WindowSizeMsg:
		m.width = v.Width
	case configMsg:
		m.configDirty = core.ConfigStatus(v).Dirty
	case runsMsg:
		m.runs = v
		sort.SliceStable(m.runs, func(i, j int) bool {
			if statusRank(m.runs[i].Status) != statusRank(m.runs[j].Status) {
				return statusRank(m.runs[i].Status) < statusRank(m.runs[j].Status)
			}
			return m.runs[i].UpdatedAt.After(m.runs[j].UpdatedAt)
		})
	case eventMsg:
		e := core.Event(v)
		switch e.Kind {
		case core.EventStepStarted, core.EventStepFinished:
			m.pipeline = append(m.pipeline, fmt.Sprintf("%s  %s", e.Kind, e.Data))
		case core.EventConfigChanged:
			m.configDirty = true
		case core.EventConfigReloaded:
			m.configDirty = false
		case core.EventAgentToken:
			var d struct {
				Delta string `json:"delta"`
			}
			_ = json.Unmarshal(e.Data, &d)
			m.transcript += d.Delta
		}
		return m, tea.Batch(m.loadRuns(), m.nextEvent())
	case errMsg:
		if v != io.EOF {
			m.err = v
		}
		return m, tea.Tick(time.Second, func(time.Time) tea.Msg { return runsMsg(m.runs) })
	}
	return m, nil
}
func (m Model) reloadConfig() tea.Cmd {
	return func() tea.Msg {
		req, _ := http.NewRequestWithContext(m.client.ctx, http.MethodPost, m.client.Endpoint+"/api/config/reload", nil)
		resp, err := m.client.HTTP.Do(req)
		if err != nil {
			return errMsg(err)
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode >= 300 {
			return errMsg(fmt.Errorf("config reload: %s", resp.Status))
		}
		var status core.ConfigStatus
		if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
			return errMsg(err)
		}
		return configMsg(status)
	}
}
func (m Model) migration(action string) tea.Cmd {
	return func() tea.Msg {
		if len(m.runs) == 0 || m.selected >= len(m.runs) || m.runs[m.selected].Status != core.StatusNeedsMigration {
			return nil
		}
		body, _ := json.Marshal(map[string]string{"action": action})
		req, _ := http.NewRequestWithContext(m.client.ctx, http.MethodPost, m.client.Endpoint+"/api/runs/"+m.runs[m.selected].ID+"/migration", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		resp, err := m.client.HTTP.Do(req)
		if err != nil {
			return errMsg(err)
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode >= 300 {
			return errMsg(fmt.Errorf("migration: %s", resp.Status))
		}
		return runsMsg(m.runs)
	}
}
func (m Model) decision(approved bool) tea.Cmd {
	return func() tea.Msg {
		if len(m.runs) == 0 || m.selected >= len(m.runs) || m.runs[m.selected].Status != core.StatusAwaitingReview {
			return nil
		}
		body, _ := json.Marshal(core.Decision{Approved: approved})
		req, _ := http.NewRequestWithContext(m.client.ctx, http.MethodPost, m.client.Endpoint+"/api/runs/"+m.runs[m.selected].ID+"/decision", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		resp, err := m.client.HTTP.Do(req)
		if err != nil {
			return errMsg(err)
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode >= 300 {
			return errMsg(fmt.Errorf("decision: %s", resp.Status))
		}
		return runsMsg(m.runs)
	}
}

var title = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("14"))
var muted = lipgloss.NewStyle().Foreground(lipgloss.Color("8"))
var selected = lipgloss.NewStyle().Background(lipgloss.Color("24")).Bold(true)

func (m Model) View() string {
	var b strings.Builder
	b.WriteString(title.Render("flow") + "  " + muted.Render("durable workflow console") + "\n")
	if m.configDirty {
		b.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("11")).Render("Configuration changed; press c to reload.") + "\n")
	}
	b.WriteString("\n")
	runsTitle := "Runs"
	if m.filter != "" {
		runsTitle += " · " + string(m.filter)
	}
	b.WriteString(title.Render(runsTitle) + "\n")
	visible, lastStatus := 0, core.Status("")
	for i, r := range m.runs {
		if m.filter != "" && r.Status != m.filter {
			continue
		}
		if r.Status != lastStatus {
			b.WriteString(muted.Render(strings.ToUpper(string(r.Status))) + "\n")
			lastStatus = r.Status
		}
		line := fmt.Sprintf("  %-18s %s", r.Workflow, short(r.ID))
		if i == m.selected {
			line = selected.Render(line)
		}
		b.WriteString(line + "\n")
		visible++
	}
	if visible == 0 {
		b.WriteString(muted.Render("No matching runs. Start one from the CLI or web app."))
	}
	b.WriteString("\n" + title.Render("Pipeline") + "\n")
	start := 0
	if len(m.pipeline) > 8 {
		start = len(m.pipeline) - 8
	}
	for _, line := range m.pipeline[start:] {
		b.WriteString(line + "\n")
	}
	b.WriteString("\n" + title.Render("Transcript") + "\n" + m.transcript + "\n")
	if len(m.runs) > 0 && m.selected < len(m.runs) && m.runs[m.selected].Status == core.StatusAwaitingReview {
		b.WriteString("\n" + title.Render("Review: ") + m.runs[m.selected].GatePrompt + "\n[a] approve  [r] return\n")
	}
	if len(m.runs) > 0 && m.selected < len(m.runs) && m.runs[m.selected].Status == core.StatusNeedsMigration {
		b.WriteString("\n" + title.Render("Migration required") + "\n[1] restart  [2] abandon  [3] finish on previous\n")
	}
	if m.err != nil {
		b.WriteString("\n" + lipgloss.NewStyle().Foreground(lipgloss.Color("9")).Render(m.err.Error()))
	}
	b.WriteString("\n" + muted.Render("↑/↓ select • f filter • c reload config • q quit"))
	return b.String()
}
func statusRank(status core.Status) int {
	switch status {
	case core.StatusRunning:
		return 0
	case core.StatusQueued:
		return 1
	case core.StatusAwaitingReview:
		return 2
	case core.StatusParked, core.StatusNeedsMigration:
		return 3
	case core.StatusFailed, core.StatusCanceled:
		return 4
	default:
		return 5
	}
}
func short(s string) string {
	if len(s) > 8 {
		return s[:8]
	}
	return s
}
