package agent_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bjaus/flow/app/agent"
	"github.com/stretchr/testify/require"
)

func write(t *testing.T, path, data string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(t, os.WriteFile(path, []byte(data), 0o644))
}
func TestLoaderParsesPersonasAndSkills(t *testing.T) {
	root := t.TempDir()
	agents := filepath.Join(root, "agents")
	skills := filepath.Join(root, "skills")
	write(t, filepath.Join(skills, "review", "SKILL.md"), "---\nname: reviewer\n---\nCheck facts.")
	write(t, filepath.Join(agents, "writer.md"), "---\nname: writer\nmodel: local/model\ntools: [search]\nskills: [reviewer]\n---\nWrite clearly.")
	loader, err := agent.New(agents, skills)
	require.NoError(t, err)
	p, ok := loader.Persona("writer")
	require.True(t, ok)
	require.Equal(t, "local/model", p.Model)
	require.Equal(t, []string{"search"}, p.Tools)
	require.Contains(t, p.SystemInstruction, "Write clearly.")
	require.Contains(t, p.SystemInstruction, "Check facts.")
}
func TestLoaderRejectsMalformedDuplicateAndMissingSkill(t *testing.T) {
	tests := map[string]struct {
		files    map[string]string
		contains string
	}{"missing frontmatter": {map[string]string{"agents/a.md": "body"}, "frontmatter"}, "duplicate": {map[string]string{"agents/a.md": "---\nname: same\nmodel: m\n---\na", "agents/b.md": "---\nname: same\nmodel: m\n---\nb"}, "duplicate"}, "missing skill": {map[string]string{"agents/a.md": "---\nname: a\nmodel: m\nskills: [nope]\n---\na"}, "not found"}}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			require.NoError(t, os.MkdirAll(filepath.Join(root, "agents"), 0o755))
			require.NoError(t, os.MkdirAll(filepath.Join(root, "skills"), 0o755))
			for path, data := range tc.files {
				write(t, filepath.Join(root, path), data)
			}
			_, err := agent.New(filepath.Join(root, "agents"), filepath.Join(root, "skills"))
			require.ErrorContains(t, err, tc.contains)
		})
	}
}
func TestWatchDetectsChangesAndReloadsOnlyWhenRequested(t *testing.T) {
	root := t.TempDir()
	agents := filepath.Join(root, "agents")
	skills := filepath.Join(root, "skills")
	require.NoError(t, os.MkdirAll(skills, 0o755))
	path := filepath.Join(agents, "a.md")
	write(t, path, "---\nname: a\nmodel: one\n---\nFirst")
	loader, err := agent.New(agents, skills)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- loader.Watch(ctx) }()
	time.Sleep(30 * time.Millisecond)
	write(t, path, "---\nname: a\nmodel: two\n---\nSecond")
	require.Eventually(t, func() bool { return loader.Status().Dirty }, time.Second, 10*time.Millisecond)
	p, ok := loader.Persona("a")
	require.True(t, ok)
	require.Equal(t, "one", p.Model)
	require.NoError(t, loader.Reload())
	p, ok = loader.Persona("a")
	require.True(t, ok)
	require.Equal(t, "two", p.Model)
	require.Equal(t, "Second", p.SystemInstruction)
	cancel()
	require.NoError(t, <-done)
}
