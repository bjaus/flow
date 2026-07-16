package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/cloudwego/eino/components/tool"
	"github.com/cloudwego/eino/schema"
)

type Grant struct{ Tool, Pattern string }

func ParseGrant(value string) (Grant, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return Grant{}, fmt.Errorf("empty tool grant")
	}
	open := strings.IndexByte(value, '(')
	if open < 0 {
		if strings.ContainsAny(value, ") \t\r\n") {
			return Grant{}, fmt.Errorf("invalid tool grant %q", value)
		}
		return Grant{Tool: value}, nil
	}
	if !strings.HasSuffix(value, ")") || open == 0 {
		return Grant{}, fmt.Errorf("invalid tool grant %q", value)
	}
	name := strings.TrimSpace(value[:open])
	if strings.ContainsAny(name, "() \t\r\n") {
		return Grant{}, fmt.Errorf("invalid tool name %q", name)
	}
	return Grant{Tool: name, Pattern: value[open+1 : len(value)-1]}, nil
}

func ToolNames(values []string) ([]string, error) {
	set := map[string]bool{}
	for _, value := range values {
		grant, err := ParseGrant(value)
		if err != nil {
			return nil, err
		}
		set[grant.Tool] = true
	}
	out := make([]string, 0, len(set))
	for name := range set {
		out = append(out, name)
	}
	sort.Strings(out)
	return out, nil
}

func GuardTool(base tool.BaseTool, grants []string) (tool.BaseTool, error) {
	info, err := base.Info(context.Background())
	if err != nil {
		return nil, err
	}
	patterns := []string{}
	for _, raw := range grants {
		grant, parseErr := ParseGrant(raw)
		if parseErr != nil {
			return nil, parseErr
		}
		if grant.Tool == info.Name {
			if grant.Pattern == "" {
				return base, nil
			}
			patterns = append(patterns, grant.Pattern)
		}
	}
	if len(patterns) == 0 {
		return nil, fmt.Errorf("tool %q is not allowed", info.Name)
	}
	if streamable, ok := base.(tool.EnhancedStreamableTool); ok {
		return guardedEnhancedStreamable{BaseTool: base, inner: streamable, name: info.Name, patterns: patterns}, nil
	}
	if invokable, ok := base.(tool.EnhancedInvokableTool); ok {
		return guardedEnhancedInvokable{BaseTool: base, inner: invokable, name: info.Name, patterns: patterns}, nil
	}
	if streamable, ok := base.(tool.StreamableTool); ok {
		return guardedStreamable{BaseTool: base, inner: streamable, name: info.Name, patterns: patterns}, nil
	}
	if invokable, ok := base.(tool.InvokableTool); ok {
		return guardedInvokable{BaseTool: base, inner: invokable, name: info.Name, patterns: patterns}, nil
	}
	return nil, fmt.Errorf("tool %q cannot be guarded because it is not invokable", info.Name)
}

type guardedInvokable struct {
	tool.BaseTool
	inner    tool.InvokableTool
	name     string
	patterns []string
}

func (g guardedInvokable) InvokableRun(ctx context.Context, arguments string, opts ...tool.Option) (string, error) {
	if err := authorize(g.name, g.patterns, arguments); err != nil {
		return "", err
	}
	return g.inner.InvokableRun(ctx, arguments, opts...)
}

type guardedStreamable struct {
	tool.BaseTool
	inner    tool.StreamableTool
	name     string
	patterns []string
}

func (g guardedStreamable) StreamableRun(ctx context.Context, arguments string, opts ...tool.Option) (*schema.StreamReader[string], error) {
	if err := authorize(g.name, g.patterns, arguments); err != nil {
		return nil, err
	}
	return g.inner.StreamableRun(ctx, arguments, opts...)
}

type guardedEnhancedInvokable struct {
	tool.BaseTool
	inner    tool.EnhancedInvokableTool
	name     string
	patterns []string
}

func (g guardedEnhancedInvokable) InvokableRun(ctx context.Context, argument *schema.ToolArgument, opts ...tool.Option) (*schema.ToolResult, error) {
	if argument == nil {
		return nil, fmt.Errorf("tool %q call has no arguments", g.name)
	}
	if err := authorize(g.name, g.patterns, argument.Text); err != nil {
		return nil, err
	}
	return g.inner.InvokableRun(ctx, argument, opts...)
}

type guardedEnhancedStreamable struct {
	tool.BaseTool
	inner    tool.EnhancedStreamableTool
	name     string
	patterns []string
}

func (g guardedEnhancedStreamable) StreamableRun(ctx context.Context, argument *schema.ToolArgument, opts ...tool.Option) (*schema.StreamReader[*schema.ToolResult], error) {
	if argument == nil {
		return nil, fmt.Errorf("tool %q call has no arguments", g.name)
	}
	if err := authorize(g.name, g.patterns, argument.Text); err != nil {
		return nil, err
	}
	return g.inner.StreamableRun(ctx, argument, opts...)
}

func authorize(name string, patterns []string, arguments string) error {
	resources := scalarArguments(arguments)
	command := name == "bash" || name == "shell"
	fileTool := map[string]bool{"read": true, "write": true, "edit": true, "ls": true, "grep": true, "find": true}[name]
	for _, pattern := range patterns {
		for _, resource := range resources {
			if fileTool {
				pattern, resource = filepath.Clean(pattern), filepath.Clean(resource)
			}
			if match(pattern, resource, command) {
				return nil
			}
		}
	}
	return fmt.Errorf("tool %q call is outside its allowed patterns", name)
}

func scalarArguments(raw string) []string {
	var value any
	if json.Unmarshal([]byte(raw), &value) != nil {
		return []string{raw}
	}
	preferred := []string{"command", "path", "file", "query", "url", "name"}
	if object, ok := value.(map[string]any); ok {
		for _, key := range preferred {
			if text, yes := object[key].(string); yes {
				return []string{text}
			}
		}
	}
	out := []string{}
	var walk func(any)
	walk = func(v any) {
		switch x := v.(type) {
		case string:
			out = append(out, x)
		case []any:
			for _, item := range x {
				walk(item)
			}
		case map[string]any:
			for _, item := range x {
				walk(item)
			}
		}
	}
	walk(value)
	if len(out) == 0 {
		return []string{raw}
	}
	return out
}

func match(pattern, value string, command bool) bool {
	var b strings.Builder
	b.WriteByte('^')
	for i := 0; i < len(pattern); {
		if pattern[i] == '*' {
			double := i+1 < len(pattern) && pattern[i+1] == '*'
			if double {
				i++
			}
			if command {
				b.WriteString("[^;&|\\r\\n`$<>]*")
			} else if double {
				b.WriteString(".*")
			} else {
				b.WriteString("[^/]*")
			}
			i++
			continue
		}
		b.WriteString(regexp.QuoteMeta(pattern[i : i+1]))
		i++
	}
	b.WriteByte('$')
	ok, _ := regexp.MatchString(b.String(), value)
	return ok
}
