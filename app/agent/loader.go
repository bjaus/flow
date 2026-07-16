package agent

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/bjaus/flow/app/internal/core"
	"github.com/fsnotify/fsnotify"
	"gopkg.in/yaml.v3"
)

type Loader struct {
	configFiles    []string
	explicitAgents []string
	explicitSkills []string
	mu             sync.RWMutex
	personas       map[string]core.Persona
	sources        Sources
	status         ConfigStatus
	onChange       func(ConfigStatus)
}

type frontmatter struct {
	Name    string   `yaml:"name"`
	Model   string   `yaml:"model"` // deprecated: use profile
	Profile string   `yaml:"profile"`
	Roles   []string `yaml:"roles"`
	Tools   []string `yaml:"tools"`
	Skills  []string `yaml:"skills"`
}

// New builds a registry from one agent and skill root.
func New(agents, skills string) (*Loader, error) { return NewPaths([]string{agents}, []string{skills}) }

// NewPaths builds a registry from explicit paths. Later roots take precedence.
func NewPaths(agents, skills []string) (*Loader, error) {
	l := &Loader{explicitAgents: append([]string(nil), agents...), explicitSkills: append([]string(nil), skills...)}
	if err := l.Reload(); err != nil {
		return nil, err
	}
	return l, nil
}

// NewConfigured reads ~/.flow/config.yml and .flow/config.yml (or FLOW_CONFIG) and uses their configured roots.
func NewConfigured(configFiles ...string) (*Loader, error) {
	l := &Loader{configFiles: append([]string(nil), configFiles...)}
	if err := l.Reload(); err != nil {
		return nil, err
	}
	return l, nil
}

func (l *Loader) Reload() error {
	sources, err := loadSources(l.configFiles)
	if err != nil {
		return l.reloadFailed(err)
	}
	if len(l.explicitAgents) > 0 {
		sources.Agents = append([]string(nil), l.explicitAgents...)
	}
	if len(l.explicitSkills) > 0 {
		sources.Skills = append([]string(nil), l.explicitSkills...)
	}
	skills, err := loadSkills(sources.Skills)
	if err != nil {
		return l.reloadFailed(err)
	}
	personas := map[string]core.Persona{}
	for _, root := range sources.Agents {
		if _, statErr := os.Stat(root); errors.Is(statErr, os.ErrNotExist) {
			continue
		}
		seenInRoot := map[string]bool{}
		err = filepath.WalkDir(root, func(path string, d fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if d.IsDir() || !strings.HasSuffix(strings.ToLower(d.Name()), ".md") {
				return nil
			}
			meta, body, parseErr := parseFile(path)
			if parseErr != nil {
				return parseErr
			}
			if meta.Name == "" {
				return fmt.Errorf("%s: name is required", path)
			}
			if seenInRoot[meta.Name] {
				return fmt.Errorf("duplicate persona %q", meta.Name)
			}
			seenInRoot[meta.Name] = true
			grants := append([]string(nil), meta.Tools...)
			skillNames := append([]string(nil), meta.Skills...)
			for _, roleName := range meta.Roles {
				role, ok := sources.Roles[roleName]
				if !ok {
					return fmt.Errorf("%s: role %q not found", path, roleName)
				}
				for _, grant := range role.Tools {
					expanded, expandErr := substitute(grant, sources.Vars)
					if expandErr != nil {
						return fmt.Errorf("%s: %w", path, expandErr)
					}
					grants = append(grants, expanded)
				}
				skillNames = append(skillNames, role.Skills...)
			}
			for i, grant := range grants {
				expanded, expandErr := substitute(grant, sources.Vars)
				if expandErr != nil {
					return fmt.Errorf("%s: %w", path, expandErr)
				}
				grants[i] = expanded
			}
			models := []string(nil)
			if meta.Profile != "" {
				profile, ok := sources.Profiles[meta.Profile]
				if !ok {
					return fmt.Errorf("%s: profile %q not found", path, meta.Profile)
				}
				models = append(models, profile.Models...)
			}
			if len(models) == 0 && meta.Model != "" {
				models = []string{meta.Model}
			}
			if len(models) == 0 {
				return fmt.Errorf("%s: profile must resolve to at least one model", path)
			}
			instruction := strings.TrimSpace(body)
			for _, name := range unique(skillNames) {
				skill, ok := skills[name]
				if !ok {
					return fmt.Errorf("%s: skill %q not found", path, name)
				}
				instruction += "\n\n## Skill: " + name + "\n\n" + skill
			}
			toolNames, parseErr := ToolNames(grants)
			if parseErr != nil {
				return fmt.Errorf("%s: %w", path, parseErr)
			}
			personas[meta.Name] = core.Persona{Name: meta.Name, Model: models[0], FallbackModels: append([]string(nil), models[1:]...), Tools: toolNames, ToolPermissions: unique(grants), Skills: unique(skillNames), Roles: unique(meta.Roles), Profile: meta.Profile, SystemInstruction: instruction}
			return nil
		})
		if err != nil {
			return l.reloadFailed(fmt.Errorf("load agents: %w", err))
		}
	}
	now := time.Now().UTC()
	l.mu.Lock()
	l.personas, l.sources = personas, sources
	l.status = ConfigStatus{LoadedAt: now, Files: append([]string(nil), sources.Files...)}
	l.mu.Unlock()
	return nil
}

func (l *Loader) reloadFailed(err error) error {
	l.mu.Lock()
	l.status.Error = err.Error()
	l.mu.Unlock()
	return err
}

func (l *Loader) SetOnChange(fn func(ConfigStatus)) { l.mu.Lock(); l.onChange = fn; l.mu.Unlock() }
func (l *Loader) Status() ConfigStatus {
	l.mu.RLock()
	defer l.mu.RUnlock()
	s := l.status
	s.Files = append([]string(nil), s.Files...)
	return s
}

// Watch marks configuration dirty. Changes are deliberately not activated until Reload is requested.
func (l *Loader) Watch(ctx context.Context) error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	defer func() { _ = watcher.Close() }()
	l.mu.RLock()
	sources := l.sources
	l.mu.RUnlock()
	roots := append(append([]string(nil), sources.Agents...), sources.Skills...)
	for _, file := range append(sources.Files, l.configFiles...) {
		roots = append(roots, filepath.Dir(expand(file)))
	}
	seen := map[string]bool{}
	for _, root := range roots {
		if seen[root] {
			continue
		}
		seen[root] = true
		if _, statErr := os.Stat(root); errors.Is(statErr, os.ErrNotExist) {
			continue
		}
		if err := filepath.WalkDir(root, func(path string, d fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if d.IsDir() {
				return watcher.Add(path)
			}
			return nil
		}); err != nil {
			return err
		}
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case watchErr := <-watcher.Errors:
			if watchErr != nil {
				return watchErr
			}
		case event, ok := <-watcher.Events:
			if !ok {
				return nil
			}
			if event.Op&(fsnotify.Write|fsnotify.Create|fsnotify.Remove|fsnotify.Rename) == 0 {
				continue
			}
			if event.Op&fsnotify.Create != 0 {
				if info, statErr := os.Stat(event.Name); statErr == nil && info.IsDir() {
					_ = watcher.Add(event.Name)
				}
			}
			now := time.Now().UTC()
			l.mu.Lock()
			l.status.Dirty, l.status.ChangedAt = true, &now
			status, fn := l.status, l.onChange
			l.mu.Unlock()
			if fn != nil {
				fn(status)
			}
		}
	}
}

func (l *Loader) Persona(name string) (core.Persona, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	p, ok := l.personas[name]
	return p, ok
}
func (l *Loader) Empty() bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return len(l.personas) == 0 && len(l.sources.Files) == 0
}

func loadSkills(roots []string) (map[string]string, error) {
	out := map[string]string{}
	for _, root := range roots {
		if _, statErr := os.Stat(root); errors.Is(statErr, os.ErrNotExist) {
			continue
		}
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
			out[name] = strings.TrimSpace(body)
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("load skills: %w", err)
		}
	}
	return out, nil
}

func unique(values []string) []string {
	seen := map[string]bool{}
	out := []string{}
	for _, v := range values {
		if v != "" && !seen[v] {
			seen[v] = true
			out = append(out, v)
		}
	}
	return out
}
func substitute(value string, vars map[string]string) (string, error) {
	for {
		start := strings.Index(value, "{{")
		if start < 0 {
			return value, nil
		}
		end := strings.Index(value[start+2:], "}}")
		if end < 0 {
			return "", fmt.Errorf("unterminated variable in %q", value)
		}
		end += start + 2
		name := strings.TrimSpace(value[start+2 : end])
		replacement, ok := vars[name]
		if !ok {
			return "", fmt.Errorf("variable %q not found", name)
		}
		value = value[:start] + replacement + value[end+2:]
	}
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
	sort.Strings(meta.Roles)
	return meta, rest[idx+5:], nil
}
