package workspace_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/bjaus/flow/app/workspace"
	"github.com/stretchr/testify/require"
)

func git(t *testing.T, dir string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, string(out))
}
func TestManagerIdempotentLifecycle(t *testing.T) {
	repo := filepath.Join(t.TempDir(), "repo")
	require.NoError(t, os.MkdirAll(repo, 0o755))
	git(t, repo, "init")
	git(t, repo, "config", "user.email", "test@example.com")
	git(t, repo, "config", "user.name", "Test")
	require.NoError(t, os.WriteFile(filepath.Join(repo, "README"), []byte("x"), 0o644))
	git(t, repo, "add", "README")
	git(t, repo, "commit", "-m", "initial")
	m := workspace.Manager{Root: filepath.Join(t.TempDir(), "worktrees")}
	ctx := context.Background()
	first, err := m.Ensure(ctx, repo, "run-1", "")
	require.NoError(t, err)
	second, err := m.Ensure(ctx, repo, "run-1", "")
	require.NoError(t, err)
	require.Equal(t, first, second)
	require.FileExists(t, filepath.Join(first, "README"))
	require.Error(t, func() error { _, err := m.Ensure(ctx, repo, "../escape", ""); return err }())
	require.NoError(t, m.Cleanup(ctx, repo, "run-1"))
	require.NoDirExists(t, first)
	require.NoError(t, m.Cleanup(ctx, repo, "run-1"))
}
