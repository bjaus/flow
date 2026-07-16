package agent

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/bjaus/flow/app/internal/core"
	"github.com/fsnotify/fsnotify"
	"gopkg.in/yaml.v3"
)

type Loader struct {
	agents   string
	skills   string
	mu       sync.RWMutex
	personas map[string]core.Persona
}

type frontmatter struct {
	Name   string   `yaml:"name"`
	Model  string   `yaml:"model"`
	Tools  []string `yaml:"tools"`
	Skills []string `yaml:"skills"`
}

func New(agentsDir, skillsDir string) (*Loader, error) {
	if agentsDir == "" {
		agentsDir = "./agents"
	}
	if skillsDir == "" {
		skillsDir = "./skills"
	}
	l := &Loader{agents: agentsDir, skills: skillsDir}
	if err := l.Reload(); err != nil {
		return nil, err
	}
	return l, nil
}

func (l *Loader) Reload() error {
	skills, err := loadSkills(l.skills)
	if err != nil {
		return err
	}
	personas := map[string]core.Persona{}
	err = filepath.WalkDir(l.agents, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() || !strings.HasSuffix(strings.ToLower(d.Name()), ".md") {
			return nil
		}
		meta, body, err := parseFile(path)
		if err != nil {
			return err
		}
		if meta.Name == "" || meta.Model == "" {
			return fmt.Errorf("%s: name and model are required", path)
		}
		if _, exists := personas[meta.Name]; exists {
			return fmt.Errorf("duplicate persona %q", meta.Name)
		}
		instruction := strings.TrimSpace(body)
		for _, name := range meta.Skills {
			skill, ok := skills[name]
			if !ok {
				return fmt.Errorf("%s: skill %q not found", path, name)
			}
			instruction += "\n\n## Skill: " + name + "\n\n" + skill
		}
		personas[meta.Name] = core.Persona{Name: meta.Name, Model: meta.Model, Tools: append([]string(nil), meta.Tools...), Skills: append([]string(nil), meta.Skills...), SystemInstruction: instruction}
		return nil
	})
	if err != nil {
		return fmt.Errorf("load agents: %w", err)
	}
	l.mu.Lock()
	l.personas = personas
	l.mu.Unlock()
	return nil
}

// Watch reloads the registry whenever an agent or skill file changes. It blocks until ctx is canceled.
func (l *Loader) Watch(ctx context.Context) error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	defer func() { _ = watcher.Close() }()
	for _, root := range []string{l.agents, l.skills} {
		err = filepath.WalkDir(root, func(path string, d fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if d.IsDir() {
				return watcher.Add(path)
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-watcher.Errors:
			if err != nil {
				return err
			}
		case event, ok := <-watcher.Events:
			if !ok {
				return nil
			}
			if event.Op&(fsnotify.Write|fsnotify.Create|fsnotify.Remove|fsnotify.Rename) != 0 {
				if event.Op&fsnotify.Create != 0 {
					if info, statErr := os.Stat(event.Name); statErr == nil && info.IsDir() {
						_ = watcher.Add(event.Name)
					}
				}
				_ = l.Reload()
			}
		}
	}
}

func (l *Loader) Persona(name string) (core.Persona, bool) {
	// Rebuild per lookup: edits are visible on the next invocation even on filesystems where watch events are
	// coalesced. Reload failure preserves the last valid registry.
	_ = l.Reload()
	l.mu.RLock()
	defer l.mu.RUnlock()
	p, ok := l.personas[name]
	return p, ok
}

func loadSkills(root string) (map[string]string, error) {
	out := map[string]string{}
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() || !strings.EqualFold(d.Name(), "SKILL.md") {
			return nil
		}
		meta, body, err := parseFile(path)
		if err != nil {
			return err
		}
		name := meta.Name
		if name == "" {
			name = filepath.Base(filepath.Dir(path))
		}
		if _, exists := out[name]; exists {
			return fmt.Errorf("duplicate skill %q", name)
		}
		out[name] = strings.TrimSpace(body)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("load skills: %w", err)
	}
	return out, nil
}

func parseFile(path string) (frontmatter, string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return frontmatter{}, "", err
	}
	text := strings.ReplaceAll(string(data), "\r\n", "\n")
	if !strings.HasPrefix(text, "---\n") {
		return frontmatter{}, "", errors.New(path + ": missing YAML frontmatter")
	}
	rest := strings.TrimPrefix(text, "---\n")
	idx := strings.Index(rest, "\n---\n")
	if idx < 0 {
		return frontmatter{}, "", errors.New(path + ": unterminated YAML frontmatter")
	}
	var meta frontmatter
	if err := yaml.Unmarshal([]byte(rest[:idx]), &meta); err != nil {
		return meta, "", fmt.Errorf("%s: %w", path, err)
	}
	return meta, rest[idx+5:], nil
}
