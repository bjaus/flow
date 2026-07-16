// Package workspace manages run-scoped git worktrees.
package workspace

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
)

var safeID = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)

type Manager struct{ Root string }

// Ensure creates or reuses the worktree keyed by runID.
func (m Manager) Ensure(ctx context.Context, repository, runID, ref string) (string, error) {
	if !safeID.MatchString(runID) {
		return "", errors.New("invalid run id")
	}
	if ref == "" {
		ref = "HEAD"
	}
	root := m.Root
	if root == "" {
		root = filepath.Join(repository, ".flow", "worktrees")
	}
	path := filepath.Join(root, runID)
	if info, err := os.Stat(path); err == nil && info.IsDir() {
		return path, nil
	} else if err != nil && !os.IsNotExist(err) {
		return "", err
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return "", err
	}
	cmd := exec.CommandContext(ctx, "git", "-C", repository, "worktree", "add", "--detach", path, ref)
	if out, err := cmd.CombinedOutput(); err != nil {
		return "", fmt.Errorf("create worktree: %w: %s", err, out)
	}
	return path, nil
}

// Cleanup removes a completed or abandoned run's worktree. It is idempotent.
func (m Manager) Cleanup(ctx context.Context, repository, runID string) error {
	if !safeID.MatchString(runID) {
		return errors.New("invalid run id")
	}
	root := m.Root
	if root == "" {
		root = filepath.Join(repository, ".flow", "worktrees")
	}
	path := filepath.Join(root, runID)
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil
	}
	cmd := exec.CommandContext(ctx, "git", "-C", repository, "worktree", "remove", "--force", path)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("remove worktree: %w: %s", err, out)
	}
	return nil
}
