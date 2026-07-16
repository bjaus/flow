package agent

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/bjaus/flow/app/internal/core"
	"gopkg.in/yaml.v3"
)

// Profile names an ordered model ladder and its generation settings. The first model is preferred and the rest are fallbacks.
// Pointer settings preserve the provider default when omitted, while permitting explicit zero values.
type Profile struct {
	Models              []string
	Temperature         *float32 `yaml:"temperature"`
	TopP                *float32 `yaml:"topP"`
	MaxCompletionTokens *int     `yaml:"maxCompletionTokens"`
	Stop                []string `yaml:"stop"`
	PresencePenalty     *float32 `yaml:"presencePenalty"`
	FrequencyPenalty    *float32 `yaml:"frequencyPenalty"`
	Seed                *int     `yaml:"seed"`
}

func (p *Profile) UnmarshalYAML(node *yaml.Node) error {
	var list []string
	if node.Kind == yaml.SequenceNode {
		if err := node.Decode(&list); err != nil {
			return err
		}
		p.Models = list
		return nil
	}
	if node.Kind == yaml.ScalarNode {
		var one string
		if err := node.Decode(&one); err != nil {
			return err
		}
		p.Models = []string{one}
		return nil
	}
	var value struct {
		Models              []string `yaml:"models"`
		Model               string   `yaml:"model"`
		FallbackModels      []string `yaml:"fallbackModels"`
		Temperature         *float32 `yaml:"temperature"`
		TopP                *float32 `yaml:"topP"`
		MaxCompletionTokens *int     `yaml:"maxCompletionTokens"`
		Stop                []string `yaml:"stop"`
		PresencePenalty     *float32 `yaml:"presencePenalty"`
		FrequencyPenalty    *float32 `yaml:"frequencyPenalty"`
		Seed                *int     `yaml:"seed"`
	}
	if err := node.Decode(&value); err != nil {
		return err
	}
	p.Models = append([]string(nil), value.Models...)
	if value.Model != "" {
		p.Models = append([]string{value.Model}, value.FallbackModels...)
	}
	p.Temperature = value.Temperature
	p.TopP = value.TopP
	p.MaxCompletionTokens = value.MaxCompletionTokens
	p.Stop = append([]string(nil), value.Stop...)
	p.PresencePenalty = value.PresencePenalty
	p.FrequencyPenalty = value.FrequencyPenalty
	p.Seed = value.Seed
	return p.validate()
}

func (p Profile) validate() error {
	if p.Temperature != nil && (*p.Temperature < 0 || *p.Temperature > 2) {
		return fmt.Errorf("temperature must be between 0 and 2")
	}
	if p.TopP != nil && (*p.TopP < 0 || *p.TopP > 1) {
		return fmt.Errorf("topP must be between 0 and 1")
	}
	if p.MaxCompletionTokens != nil && *p.MaxCompletionTokens <= 0 {
		return fmt.Errorf("maxCompletionTokens must be greater than zero")
	}
	if p.PresencePenalty != nil && (*p.PresencePenalty < -2 || *p.PresencePenalty > 2) {
		return fmt.Errorf("presencePenalty must be between -2 and 2")
	}
	if p.FrequencyPenalty != nil && (*p.FrequencyPenalty < -2 || *p.FrequencyPenalty > 2) {
		return fmt.Errorf("frequencyPenalty must be between -2 and 2")
	}
	return nil
}

type Role struct {
	Tools  []string `yaml:"tools"`
	Allow  []string `yaml:"allow"`
	Skills []string `yaml:"skills"`
}

func (r *Role) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind == yaml.SequenceNode {
		return node.Decode(&r.Tools)
	}
	type plain Role
	if err := node.Decode((*plain)(r)); err != nil {
		return err
	}
	r.Tools = append(r.Tools, r.Allow...)
	return nil
}

type fileConfig struct {
	Agents   []string           `yaml:"agents"`
	Skills   []string           `yaml:"skills"`
	Profiles map[string]Profile `yaml:"profiles"`
	Roles    map[string]Role    `yaml:"roles"`
	Vars     map[string]string  `yaml:"vars"`
}

type Sources struct {
	Agents   []string
	Skills   []string
	Profiles map[string]Profile
	Roles    map[string]Role
	Vars     map[string]string
	Files    []string
}

type ConfigStatus = core.ConfigStatus

func defaultConfigFiles() ([]string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}
	project := filepath.Join(".flow", "config.yml")
	if configured := os.Getenv("FLOW_CONFIG"); configured != "" {
		project = configured
	}
	return []string{filepath.Join(home, ".flow", "config.yml"), project}, nil
}

func expand(path string) string {
	if path == "~" || strings.HasPrefix(path, "~/") {
		if home, err := os.UserHomeDir(); err == nil {
			return filepath.Join(home, strings.TrimPrefix(path, "~/"))
		}
	}
	return filepath.Clean(os.ExpandEnv(path))
}

func loadSources(files []string) (Sources, error) {
	if len(files) == 0 {
		var err error
		files, err = defaultConfigFiles()
		if err != nil {
			return Sources{}, err
		}
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return Sources{}, err
	}
	out := Sources{
		Agents:   []string{filepath.Join(home, ".flow", "agents"), filepath.Join(".flow", "agents")},
		Skills:   []string{filepath.Join(home, ".flow", "skills"), filepath.Join(".flow", "skills")},
		Profiles: map[string]Profile{}, Roles: map[string]Role{}, Vars: map[string]string{},
	}
	agentsConfigured, skillsConfigured := false, false
	for _, raw := range files {
		path := expand(raw)
		data, readErr := os.ReadFile(path)
		if errors.Is(readErr, os.ErrNotExist) {
			continue
		}
		if readErr != nil {
			return Sources{}, fmt.Errorf("read config %s: %w", path, readErr)
		}
		var cfg fileConfig
		if err := yaml.Unmarshal(data, &cfg); err != nil {
			return Sources{}, fmt.Errorf("parse config %s: %w", path, err)
		}
		out.Files = append(out.Files, path)
		if len(cfg.Agents) > 0 {
			if !agentsConfigured {
				out.Agents = nil
				agentsConfigured = true
			}
			for _, p := range cfg.Agents {
				out.Agents = append(out.Agents, expand(p))
			}
		}
		if len(cfg.Skills) > 0 {
			// Roots merge across user and project files; later definitions win by agent or skill name.
			if !skillsConfigured {
				out.Skills = nil
				skillsConfigured = true
			}
			for _, p := range cfg.Skills {
				out.Skills = append(out.Skills, expand(p))
			}
		}
		for name, p := range cfg.Profiles {
			out.Profiles[name] = p
		}
		for name, r := range cfg.Roles {
			out.Roles[name] = r
		}
		for name, v := range cfg.Vars {
			out.Vars[name] = v
		}
	}
	return out, nil
}
