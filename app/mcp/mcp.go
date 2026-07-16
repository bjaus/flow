// Package mcp connects Flow to Model Context Protocol servers and exposes their
// discovered tools as Eino tools.
package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os/exec"
	"regexp"
	"sort"
	"strings"

	"github.com/cloudwego/eino/components/tool"
	"github.com/cloudwego/eino/schema"
	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

// Server configures one MCP server. Name namespaces its discovered tools as
// mcp__<name>__<tool>; it must contain only letters, digits, underscores, or
// hyphens. Set exactly one of Command or URL. URL uses Streamable HTTP unless
// SSE is true.
type Server struct {
	Name    string
	Command string
	Args    []string
	Env     []string
	URL     string
	SSE     bool
}

// Client owns MCP sessions and the discovered tool adapters.
type Client struct {
	sessions []*sdk.ClientSession
	tools    map[string]tool.BaseTool
}

var validName = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

// Connect connects to servers, discovers their tools, and adapts them for an
// Eino ReAct loop. Close the returned client when the application stops.
func Connect(ctx context.Context, servers ...Server) (*Client, error) {
	out := &Client{tools: map[string]tool.BaseTool{}}
	for _, server := range servers {
		if err := server.validate(); err != nil {
			_ = out.Close()
			return nil, err
		}
		session, err := connect(ctx, server)
		if err != nil {
			_ = out.Close()
			return nil, fmt.Errorf("connect MCP server %q: %w", server.Name, err)
		}
		out.sessions = append(out.sessions, session)
		listed, err := listTools(ctx, session)
		if err != nil {
			_ = out.Close()
			return nil, fmt.Errorf("list tools from MCP server %q: %w", server.Name, err)
		}
		for _, remote := range listed {
			if !validName.MatchString(remote.Name) {
				_ = out.Close()
				return nil, fmt.Errorf("MCP server %q exposes tool %q with unsupported name", server.Name, remote.Name)
			}
			name := toolName(server.Name, remote.Name)
			if _, exists := out.tools[name]; exists {
				_ = out.Close()
				return nil, fmt.Errorf("MCP server %q exposes duplicate tool %q", server.Name, remote.Name)
			}
			out.tools[name] = remoteTool{name: name, remote: remote, session: session}
		}
	}
	return out, nil
}

func (s Server) validate() error {
	if !validName.MatchString(s.Name) {
		return fmt.Errorf("MCP server name %q must contain only letters, digits, underscores, or hyphens", s.Name)
	}
	if (s.Command == "") == (s.URL == "") {
		return fmt.Errorf("MCP server %q must set exactly one of Command or URL", s.Name)
	}
	return nil
}

func connect(ctx context.Context, server Server) (*sdk.ClientSession, error) {
	client := sdk.NewClient(&sdk.Implementation{Name: "flow", Version: "v0"}, nil)
	if server.Command != "" {
		command := exec.Command(server.Command, server.Args...)
		if len(server.Env) > 0 {
			command.Env = append([]string(nil), server.Env...)
		}
		return client.Connect(ctx, &sdk.CommandTransport{Command: command}, nil)
	}
	httpClient := http.DefaultClient
	if server.SSE {
		return client.Connect(ctx, &sdk.SSEClientTransport{Endpoint: server.URL, HTTPClient: httpClient}, nil)
	}
	return client.Connect(ctx, &sdk.StreamableClientTransport{Endpoint: server.URL, HTTPClient: httpClient}, nil)
}

// Tools returns a copy of the discovered tools, keyed by their Flow tool name.
func (c *Client) Tools() map[string]tool.BaseTool {
	out := make(map[string]tool.BaseTool, len(c.tools))
	for name, tool := range c.tools {
		out[name] = tool
	}
	return out
}

// Close closes every MCP session. It is safe to call more than once.
func (c *Client) Close() error {
	if c == nil {
		return nil
	}
	var first error
	for _, session := range c.sessions {
		if err := session.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

func listTools(ctx context.Context, session *sdk.ClientSession) ([]*sdk.Tool, error) {
	var tools []*sdk.Tool
	cursor := ""
	for {
		page, err := session.ListTools(ctx, &sdk.ListToolsParams{Cursor: cursor})
		if err != nil {
			return nil, err
		}
		tools = append(tools, page.Tools...)
		if page.NextCursor == "" {
			return tools, nil
		}
		if page.NextCursor == cursor {
			return nil, fmt.Errorf("MCP server returned an unchanged tools cursor")
		}
		cursor = page.NextCursor
	}
}

func toolName(server, remote string) string { return "mcp__" + server + "__" + remote }

type remoteTool struct {
	name    string
	remote  *sdk.Tool
	session *sdk.ClientSession
}

func (t remoteTool) Info(context.Context) (*schema.ToolInfo, error) {
	params, err := paramsFromSchema(t.remote.InputSchema)
	if err != nil {
		return nil, fmt.Errorf("MCP tool %q schema: %w", t.name, err)
	}
	return &schema.ToolInfo{Name: t.name, Desc: t.remote.Description, ParamsOneOf: params}, nil
}

func (t remoteTool) InvokableRun(ctx context.Context, arguments string, _ ...tool.Option) (string, error) {
	args := map[string]any{}
	if strings.TrimSpace(arguments) != "" && strings.TrimSpace(arguments) != "null" {
		if err := json.Unmarshal([]byte(arguments), &args); err != nil {
			return "", fmt.Errorf("decode arguments for MCP tool %q: %w", t.name, err)
		}
	}
	result, err := t.session.CallTool(ctx, &sdk.CallToolParams{Name: t.remote.Name, Arguments: args})
	if err != nil {
		return "", fmt.Errorf("call MCP tool %q: %w", t.name, err)
	}
	text := renderResult(result)
	if result.IsError {
		if err := result.GetError(); err != nil {
			return text, err
		}
		return text, fmt.Errorf("MCP tool %q reported an error", t.name)
	}
	return text, nil
}

func renderResult(result *sdk.CallToolResult) string {
	parts := make([]string, 0, len(result.Content)+1)
	for _, content := range result.Content {
		if text, ok := content.(*sdk.TextContent); ok {
			parts = append(parts, text.Text)
			continue
		}
		encoded, err := json.Marshal(content)
		if err == nil {
			parts = append(parts, string(encoded))
		}
	}
	if result.StructuredContent != nil {
		encoded, err := json.Marshal(result.StructuredContent)
		if err == nil {
			parts = append(parts, string(encoded))
		}
	}
	return strings.Join(parts, "\n")
}

func paramsFromSchema(raw any) (*schema.ParamsOneOf, error) {
	if raw == nil {
		return nil, nil
	}
	data, err := json.Marshal(raw)
	if err != nil {
		return nil, err
	}
	var root struct {
		Properties map[string]json.RawMessage `json:"properties"`
		Required   []string                   `json:"required"`
	}
	if err := json.Unmarshal(data, &root); err != nil {
		return nil, err
	}
	if len(root.Properties) == 0 {
		return nil, nil
	}
	required := map[string]bool{}
	for _, name := range root.Required {
		required[name] = true
	}
	params := make(map[string]*schema.ParameterInfo, len(root.Properties))
	for name, raw := range root.Properties {
		info, err := parameterFromSchema(raw)
		if err != nil {
			return nil, fmt.Errorf("parameter %q: %w", name, err)
		}
		info.Required = required[name]
		params[name] = info
	}
	return schema.NewParamsOneOfByParams(params), nil
}

func parameterFromSchema(raw json.RawMessage) (*schema.ParameterInfo, error) {
	var value struct {
		Type        string                     `json:"type"`
		Description string                     `json:"description"`
		Enum        []string                   `json:"enum"`
		Properties  map[string]json.RawMessage `json:"properties"`
		Required    []string                   `json:"required"`
		Items       json.RawMessage            `json:"items"`
	}
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, err
	}
	info := &schema.ParameterInfo{Type: dataType(value.Type), Desc: value.Description, Enum: value.Enum}
	if value.Type == "array" && len(value.Items) > 0 {
		elem, err := parameterFromSchema(value.Items)
		if err != nil {
			return nil, err
		}
		info.ElemInfo = elem
	}
	if value.Type == "object" && len(value.Properties) > 0 {
		required := map[string]bool{}
		for _, name := range value.Required {
			required[name] = true
		}
		info.SubParams = map[string]*schema.ParameterInfo{}
		for name, child := range value.Properties {
			childInfo, err := parameterFromSchema(child)
			if err != nil {
				return nil, err
			}
			childInfo.Required = required[name]
			info.SubParams[name] = childInfo
		}
	}
	return info, nil
}

func dataType(value string) schema.DataType {
	switch value {
	case "boolean":
		return schema.Boolean
	case "integer":
		return schema.Integer
	case "number":
		return schema.Number
	case "array":
		return schema.Array
	case "object":
		return schema.Object
	default:
		return schema.String
	}
}

// Names returns discovered Flow tool names in deterministic order.
func (c *Client) Names() []string {
	names := make([]string, 0, len(c.tools))
	for name := range c.tools {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
