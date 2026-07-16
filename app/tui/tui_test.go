package tui

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/bjaus/flow/app/internal/core"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/stretchr/testify/require"
)

func TestModelRendersRunsPipelineTranscriptAndGate(t *testing.T) {
	m := New("http://example")
	runs := runsMsg{&core.Run{ID: "123456789", Workflow: "triage", Status: core.StatusAwaitingReview, GatePrompt: "Ship it?", UpdatedAt: time.Now()}}
	next, _ := m.Update(runs)
	m = next.(Model)
	step, _ := json.Marshal(map[string]string{"label": "draft"})
	next, _ = m.Update(eventMsg(core.Event{Kind: core.EventStepStarted, Data: step}))
	m = next.(Model)
	token, _ := json.Marshal(map[string]string{"delta": "hello"})
	next, _ = m.Update(eventMsg(core.Event{Kind: core.EventAgentToken, Data: token}))
	m = next.(Model)
	view := m.View()
	require.Contains(t, view, "triage")
	require.Contains(t, view, "step.started")
	require.Contains(t, view, "hello")
	require.Contains(t, view, "Ship it?")
	next, cmd := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'q'}})
	require.NotNil(t, next)
	require.NotNil(t, cmd)
}
func TestEmptyModelAndNavigationDoNotPanic(t *testing.T) {
	m := New("http://example")
	for _, key := range []string{"up", "down", "a", "r"} {
		next, _ := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune(key)})
		m = next.(Model)
	}
	require.Contains(t, m.View(), "No matching runs")
}
