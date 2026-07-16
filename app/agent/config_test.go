package agent_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/bjaus/flow/app/agent"
	"github.com/cloudwego/eino/components/tool"
	"github.com/cloudwego/eino/components/tool/utils"
	"github.com/stretchr/testify/require"
)

func TestConfiguredLoaderMergesUserAndProjectRolesProfilesAndRoots(t *testing.T) {
	root := t.TempDir()
	home := filepath.Join(root, "home")
	project := filepath.Join(root, "project")
	userAgents, projectAgents := filepath.Join(root, "user-agents"), filepath.Join(root, "project-agents")
	userSkills, projectSkills := filepath.Join(root, "user-skills"), filepath.Join(root, "project-skills")
	t.Setenv("HOME", home)
	t.Setenv("FLOW_CONFIG", filepath.Join(project, "config.yml"))
	write(t, filepath.Join(home, ".flow", "config.yml"), "agents: ["+userAgents+"]\nskills: ["+userSkills+"]\nvars: {scope: src}\nprofiles:\n  coding: [cheap, backup]\nroles:\n  reader: [\"read(**)\"]\n")
	write(t, filepath.Join(project, "config.yml"), "agents: ["+projectAgents+"]\nskills: ["+projectSkills+"]\nprofiles:\n  coding:\n    models: [strong, reserve]\n    temperature: 0\n    topP: 0.9\n    maxCompletionTokens: 1200\n    stop: [DONE]\n    presencePenalty: -0.5\n    frequencyPenalty: 0.25\n    seed: 42\nroles:\n  builder:\n    allow: [\"write({{scope}}/**)\"]\n    skills: [review]\n")
	write(t, filepath.Join(projectSkills, "review", "SKILL.md"), "---\nname: review\n---\nCheck the work.")
	write(t, filepath.Join(userAgents, "worker.md"), "---\nname: worker\nprofile: coding\nroles: [reader]\n---\nOld persona")
	write(t, filepath.Join(projectAgents, "worker.md"), "---\nname: worker\nprofile: coding\nroles: [reader, builder]\ntools: [\"bash(pnpm * check)\"]\n---\nBuild safely.")
	loader, err := agent.NewConfigured()
	require.NoError(t, err)
	persona, ok := loader.Persona("worker")
	require.True(t, ok)
	require.Equal(t, "strong", persona.Model)
	require.Equal(t, []string{"reserve"}, persona.FallbackModels)
	require.NotNil(t, persona.Temperature)
	require.Equal(t, float32(0), *persona.Temperature)
	require.NotNil(t, persona.TopP)
	require.Equal(t, float32(0.9), *persona.TopP)
	require.NotNil(t, persona.MaxCompletionTokens)
	require.Equal(t, 1200, *persona.MaxCompletionTokens)
	require.Equal(t, []string{"DONE"}, persona.Stop)
	require.NotNil(t, persona.PresencePenalty)
	require.Equal(t, float32(-0.5), *persona.PresencePenalty)
	require.NotNil(t, persona.FrequencyPenalty)
	require.Equal(t, float32(0.25), *persona.FrequencyPenalty)
	require.NotNil(t, persona.Seed)
	require.Equal(t, 42, *persona.Seed)
	require.ElementsMatch(t, []string{"read", "write", "bash"}, persona.Tools)
	require.Contains(t, persona.ToolPermissions, "write(src/**)")
	require.Contains(t, persona.SystemInstruction, "Check the work.")
	require.Contains(t, persona.SystemInstruction, "Build safely.")
}

func TestConfiguredLoaderRejectsInvalidProfileGenerationSetting(t *testing.T) {
	root := t.TempDir()
	config := filepath.Join(root, "config.yml")
	write(t, config, "profiles:\n  invalid:\n    models: [local]\n    temperature: 2.1\n")
	_, err := agent.NewConfigured(config)
	require.ErrorContains(t, err, "temperature must be between 0 and 2")
}

func TestGuardedCommandWildcardCannotMatchShellInjection(t *testing.T) {
	bash, err := utils.InferTool("bash", "run command", func(_ context.Context, in struct {
		Command string `json:"command"`
	}) (string, error) {
		return in.Command, nil
	})
	require.NoError(t, err)
	guarded, err := agent.GuardTool(bash, []string{"bash(pnpm * check)"})
	require.NoError(t, err)
	invokable := guarded.(tool.InvokableTool)
	out, err := invokable.InvokableRun(context.Background(), `{"command":"pnpm --filter app check"}`)
	require.NoError(t, err)
	require.Equal(t, "pnpm --filter app check", out)
	_, err = invokable.InvokableRun(context.Background(), `{"command":"pnpm -h ; rm -rf / ; echo check"}`)
	require.ErrorContains(t, err, "outside its allowed patterns")
	_, err = invokable.InvokableRun(context.Background(), `{"command":"pnpm $(rm -rf /) check"}`)
	require.ErrorContains(t, err, "outside its allowed patterns")
}

func TestGuardedFilePatternRejectsTraversal(t *testing.T) {
	writeTool, err := utils.InferTool("write", "write file", func(_ context.Context, in struct {
		Path string `json:"path"`
	}) (string, error) {
		return in.Path, nil
	})
	require.NoError(t, err)
	guarded, err := agent.GuardTool(writeTool, []string{"write(src/**)"})
	require.NoError(t, err)
	invokable := guarded.(tool.InvokableTool)
	_, err = invokable.InvokableRun(context.Background(), `{"path":"src/../../etc/passwd"}`)
	require.ErrorContains(t, err, "outside its allowed patterns")
}
