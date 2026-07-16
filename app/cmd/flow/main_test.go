package main

import (
	"context"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestScaffoldCreatesBuildableProjectAndRefusesOverwrite(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, scaffold(dir))
	require.FileExists(t, filepath.Join(dir, "main.go"))
	require.FileExists(t, filepath.Join(dir, ".flow", "agents", "assistant.md"))
	require.FileExists(t, filepath.Join(dir, ".flow", "config.yml"))
	require.Error(t, scaffold(dir))
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	require.NoError(t, err)
	commands := [][]string{{"go", "mod", "edit", "-require=github.com/bjaus/flow@v0.0.0", "-require=github.com/bjaus/flow/app@v0.0.0"}, {"go", "mod", "edit", "-replace=github.com/bjaus/flow=" + root, "-replace=github.com/bjaus/flow/app=" + filepath.Join(root, "app")}, {"go", "mod", "tidy"}, {"go", "build", "."}}
	for _, args := range commands {
		cmd := exec.CommandContext(context.Background(), args[0], args[1:]...)
		cmd.Dir = dir
		out, err := cmd.CombinedOutput()
		require.NoError(t, err, string(out))
	}
}
