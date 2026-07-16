// Package tools contains executable eino tools — a shell runner and file
// utilities — that a persona can call through the engine's ReAct loop. Every
// tool is confined to a root directory: file paths are resolved relative to
// the root and traversal outside it is rejected, independently of the
// per-persona permission guard in package agent.
package tools

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/cloudwego/eino/components/tool"
	"github.com/cloudwego/eino/schema"
)

const (
	maxOutputBytes = 64 * 1024
	maxMatches     = 200
	defaultTimeout = 60 * time.Second
)

// Default returns every built-in tool keyed by its name, confined to root.
func Default(root string) map[string]tool.BaseTool {
	return map[string]tool.BaseTool{
		"bash":  Bash(root),
		"read":  Read(root),
		"write": Write(root),
		"edit":  Edit(root),
		"ls":    Ls(root),
		"grep":  Grep(root),
		"find":  Find(root),
	}
}

// Bash returns a tool that runs a shell command via `sh -c` inside root.
func Bash(root string) tool.InvokableTool { return bashTool{root: root} }

// Read returns a tool that reads a file under root.
func Read(root string) tool.InvokableTool { return readTool{root: root} }

// Write returns a tool that writes or creates a file under root.
func Write(root string) tool.InvokableTool { return writeTool{root: root} }

// Edit returns a tool that performs exact string replacement in a file under root.
func Edit(root string) tool.InvokableTool { return editTool{root: root} }

// Ls returns a tool that lists a directory under root.
func Ls(root string) tool.InvokableTool { return lsTool{root: root} }

// Grep returns a tool that searches file contents under root with a regexp.
func Grep(root string) tool.InvokableTool { return grepTool{root: root} }

// Find returns a tool that finds files under root matching a glob pattern.
func Find(root string) tool.InvokableTool { return findTool{root: root} }

// resolve joins a root-relative path onto root, rejecting absolute paths and
// any traversal that would escape the root.
func resolve(root, rel string) (string, error) {
	if rel == "" {
		rel = "."
	}
	rel = filepath.Clean(filepath.FromSlash(rel))
	if rel != "." && !filepath.IsLocal(rel) {
		return "", fmt.Errorf("path %q escapes the tool root", rel)
	}
	return filepath.Join(root, rel), nil
}

func truncate(s string) string {
	if len(s) <= maxOutputBytes {
		return s
	}
	return s[:maxOutputBytes] + fmt.Sprintf("\n... [output truncated: %d of %d bytes shown]", maxOutputBytes, len(s))
}

func decode[T any](arguments string) (T, error) {
	var in T
	if err := json.Unmarshal([]byte(arguments), &in); err != nil {
		return in, fmt.Errorf("invalid arguments: %w", err)
	}
	return in, nil
}

type bashTool struct{ root string }

type bashArgs struct {
	Command string  `json:"command"`
	Timeout float64 `json:"timeout"`
}

func (t bashTool) Info(context.Context) (*schema.ToolInfo, error) {
	return &schema.ToolInfo{
		Name: "bash",
		Desc: "Run a shell command via `sh -c` in the working directory and return its combined output and exit status.",
		ParamsOneOf: schema.NewParamsOneOfByParams(map[string]*schema.ParameterInfo{
			"command": {Type: schema.String, Desc: "The shell command to run.", Required: true},
			"timeout": {Type: schema.Number, Desc: "Timeout in seconds (default 60)."},
		}),
	}, nil
}

func (t bashTool) InvokableRun(ctx context.Context, arguments string, _ ...tool.Option) (string, error) {
	in, err := decode[bashArgs](arguments)
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(in.Command) == "" {
		return "", fmt.Errorf("command is required")
	}
	timeout := defaultTimeout
	if in.Timeout > 0 {
		timeout = time.Duration(in.Timeout * float64(time.Second))
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "sh", "-c", in.Command)
	cmd.Dir = t.root
	output, runErr := cmd.CombinedOutput()
	status := 0
	if runErr != nil {
		if exit, ok := runErr.(*exec.ExitError); ok {
			status = exit.ExitCode()
		} else {
			return "", fmt.Errorf("run command: %w", runErr)
		}
	}
	if ctx.Err() != nil {
		return "", fmt.Errorf("command timed out after %s", timeout)
	}
	return truncate(string(output)) + fmt.Sprintf("\nexit status: %d", status), nil
}

type readTool struct{ root string }

type readArgs struct {
	Path   string `json:"path"`
	Offset int    `json:"offset"`
	Limit  int    `json:"limit"`
}

func (t readTool) Info(context.Context) (*schema.ToolInfo, error) {
	return &schema.ToolInfo{
		Name: "read",
		Desc: "Read a file. Path is relative to the working directory. Optionally start at a 1-based line offset and read a limited number of lines.",
		ParamsOneOf: schema.NewParamsOneOfByParams(map[string]*schema.ParameterInfo{
			"path":   {Type: schema.String, Desc: "File path relative to the working directory.", Required: true},
			"offset": {Type: schema.Integer, Desc: "1-based line number to start reading from."},
			"limit":  {Type: schema.Integer, Desc: "Maximum number of lines to read."},
		}),
	}, nil
}

func (t readTool) InvokableRun(_ context.Context, arguments string, _ ...tool.Option) (string, error) {
	in, err := decode[readArgs](arguments)
	if err != nil {
		return "", err
	}
	path, err := resolve(t.root, in.Path)
	if err != nil {
		return "", err
	}
	content, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	if in.Offset > 0 || in.Limit > 0 {
		lines := strings.Split(string(content), "\n")
		start := max(in.Offset-1, 0)
		if start > len(lines) {
			start = len(lines)
		}
		end := len(lines)
		if in.Limit > 0 && start+in.Limit < end {
			end = start + in.Limit
		}
		return truncate(strings.Join(lines[start:end], "\n")), nil
	}
	return truncate(string(content)), nil
}

type writeTool struct{ root string }

type writeArgs struct {
	Path    string `json:"path"`
	Content string `json:"content"`
}

func (t writeTool) Info(context.Context) (*schema.ToolInfo, error) {
	return &schema.ToolInfo{
		Name: "write",
		Desc: "Write content to a file, creating it and any parent directories if needed. Path is relative to the working directory.",
		ParamsOneOf: schema.NewParamsOneOfByParams(map[string]*schema.ParameterInfo{
			"path":    {Type: schema.String, Desc: "File path relative to the working directory.", Required: true},
			"content": {Type: schema.String, Desc: "Full content to write.", Required: true},
		}),
	}, nil
}

func (t writeTool) InvokableRun(_ context.Context, arguments string, _ ...tool.Option) (string, error) {
	in, err := decode[writeArgs](arguments)
	if err != nil {
		return "", err
	}
	path, err := resolve(t.root, in.Path)
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return "", err
	}
	if err := os.WriteFile(path, []byte(in.Content), 0o644); err != nil {
		return "", err
	}
	return fmt.Sprintf("wrote %d bytes to %s", len(in.Content), in.Path), nil
}

type editTool struct{ root string }

type editArgs struct {
	Path       string `json:"path"`
	OldString  string `json:"old_string"`
	NewString  string `json:"new_string"`
	ReplaceAll bool   `json:"replace_all"`
}

func (t editTool) Info(context.Context) (*schema.ToolInfo, error) {
	return &schema.ToolInfo{
		Name: "edit",
		Desc: "Replace an exact string in a file. old_string must match exactly and be unique unless replace_all is true.",
		ParamsOneOf: schema.NewParamsOneOfByParams(map[string]*schema.ParameterInfo{
			"path":        {Type: schema.String, Desc: "File path relative to the working directory.", Required: true},
			"old_string":  {Type: schema.String, Desc: "Exact text to replace.", Required: true},
			"new_string":  {Type: schema.String, Desc: "Replacement text.", Required: true},
			"replace_all": {Type: schema.Boolean, Desc: "Replace every occurrence instead of requiring uniqueness."},
		}),
	}, nil
}

func (t editTool) InvokableRun(_ context.Context, arguments string, _ ...tool.Option) (string, error) {
	in, err := decode[editArgs](arguments)
	if err != nil {
		return "", err
	}
	if in.OldString == "" {
		return "", fmt.Errorf("old_string is required")
	}
	if in.OldString == in.NewString {
		return "", fmt.Errorf("old_string and new_string are identical")
	}
	path, err := resolve(t.root, in.Path)
	if err != nil {
		return "", err
	}
	content, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	text := string(content)
	count := strings.Count(text, in.OldString)
	switch {
	case count == 0:
		return "", fmt.Errorf("old_string not found in %s", in.Path)
	case count > 1 && !in.ReplaceAll:
		return "", fmt.Errorf("old_string appears %d times in %s; make it unique or set replace_all", count, in.Path)
	}
	replacements := 1
	if in.ReplaceAll {
		replacements = count
	}
	text = strings.Replace(text, in.OldString, in.NewString, replacements)
	if err := os.WriteFile(path, []byte(text), 0o644); err != nil {
		return "", err
	}
	return fmt.Sprintf("replaced %d occurrence(s) in %s", replacements, in.Path), nil
}

type lsTool struct{ root string }

type lsArgs struct {
	Path string `json:"path"`
}

func (t lsTool) Info(context.Context) (*schema.ToolInfo, error) {
	return &schema.ToolInfo{
		Name: "ls",
		Desc: "List the entries of a directory. Path is relative to the working directory and defaults to it.",
		ParamsOneOf: schema.NewParamsOneOfByParams(map[string]*schema.ParameterInfo{
			"path": {Type: schema.String, Desc: "Directory path relative to the working directory (default \".\")."},
		}),
	}, nil
}

func (t lsTool) InvokableRun(_ context.Context, arguments string, _ ...tool.Option) (string, error) {
	in, err := decode[lsArgs](arguments)
	if err != nil {
		return "", err
	}
	path, err := resolve(t.root, in.Path)
	if err != nil {
		return "", err
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		return "", err
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() {
			name += "/"
		}
		names = append(names, name)
	}
	return truncate(strings.Join(names, "\n")), nil
}

type grepTool struct{ root string }

type grepArgs struct {
	Pattern string `json:"pattern"`
	Path    string `json:"path"`
}

func (t grepTool) Info(context.Context) (*schema.ToolInfo, error) {
	return &schema.ToolInfo{
		Name: "grep",
		Desc: "Search file contents recursively with a Go regular expression. Returns matches as file:line:text, capped.",
		ParamsOneOf: schema.NewParamsOneOfByParams(map[string]*schema.ParameterInfo{
			"pattern": {Type: schema.String, Desc: "Go regular expression to search for.", Required: true},
			"path":    {Type: schema.String, Desc: "Directory or file to search, relative to the working directory (default \".\")."},
		}),
	}, nil
}

func (t grepTool) InvokableRun(_ context.Context, arguments string, _ ...tool.Option) (string, error) {
	in, err := decode[grepArgs](arguments)
	if err != nil {
		return "", err
	}
	re, err := regexp.Compile(in.Pattern)
	if err != nil {
		return "", fmt.Errorf("invalid pattern: %w", err)
	}
	start, err := resolve(t.root, in.Path)
	if err != nil {
		return "", err
	}
	matches := []string{}
	walkErr := filepath.WalkDir(start, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if entry.Name() == ".git" {
				return filepath.SkipDir
			}
			return nil
		}
		if len(matches) >= maxMatches {
			return filepath.SkipAll
		}
		rel, relErr := filepath.Rel(t.root, path)
		if relErr != nil {
			rel = path
		}
		return grepFile(path, filepath.ToSlash(rel), re, &matches)
	})
	if walkErr != nil {
		return "", walkErr
	}
	if len(matches) == 0 {
		return "no matches", nil
	}
	out := strings.Join(matches, "\n")
	if len(matches) >= maxMatches {
		out += fmt.Sprintf("\n... [match limit of %d reached]", maxMatches)
	}
	return truncate(out), nil
}

func grepFile(path, rel string, re *regexp.Regexp, matches *[]string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for line := 1; scanner.Scan(); line++ {
		text := scanner.Text()
		if strings.ContainsRune(text, 0) {
			return nil // binary file
		}
		if re.MatchString(text) {
			if len(text) > 500 {
				text = text[:500] + "..."
			}
			*matches = append(*matches, fmt.Sprintf("%s:%d:%s", rel, line, text))
			if len(*matches) >= maxMatches {
				return nil
			}
		}
	}
	if err := scanner.Err(); err != nil {
		if err == bufio.ErrTooLong {
			return nil // likely binary or minified; skip
		}
		return err
	}
	return nil
}

type findTool struct{ root string }

type findArgs struct {
	Pattern string `json:"pattern"`
	Path    string `json:"path"`
}

func (t findTool) Info(context.Context) (*schema.ToolInfo, error) {
	return &schema.ToolInfo{
		Name: "find",
		Desc: "Find files under a directory whose name or relative path matches a glob pattern (e.g. *.go or cmd/*/main.go).",
		ParamsOneOf: schema.NewParamsOneOfByParams(map[string]*schema.ParameterInfo{
			"pattern": {Type: schema.String, Desc: "Glob pattern matched against the file name and its relative path.", Required: true},
			"path":    {Type: schema.String, Desc: "Directory to search, relative to the working directory (default \".\")."},
		}),
	}, nil
}

func (t findTool) InvokableRun(_ context.Context, arguments string, _ ...tool.Option) (string, error) {
	in, err := decode[findArgs](arguments)
	if err != nil {
		return "", err
	}
	if in.Pattern == "" {
		return "", fmt.Errorf("pattern is required")
	}
	if _, err := filepath.Match(in.Pattern, ""); err != nil {
		return "", fmt.Errorf("invalid pattern: %w", err)
	}
	start, err := resolve(t.root, in.Path)
	if err != nil {
		return "", err
	}
	found := []string{}
	walkErr := filepath.WalkDir(start, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if entry.Name() == ".git" {
				return filepath.SkipDir
			}
			return nil
		}
		if len(found) >= maxMatches {
			return filepath.SkipAll
		}
		rel, relErr := filepath.Rel(t.root, path)
		if relErr != nil {
			rel = path
		}
		rel = filepath.ToSlash(rel)
		byName, _ := filepath.Match(in.Pattern, entry.Name())
		byPath, _ := filepath.Match(in.Pattern, rel)
		if byName || byPath {
			found = append(found, rel)
		}
		return nil
	})
	if walkErr != nil {
		return "", walkErr
	}
	if len(found) == 0 {
		return "no files found", nil
	}
	out := strings.Join(found, "\n")
	if len(found) >= maxMatches {
		out += fmt.Sprintf("\n... [result limit of %d reached]", maxMatches)
	}
	return truncate(out), nil
}
