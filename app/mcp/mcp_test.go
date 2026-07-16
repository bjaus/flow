package mcp

import (
	"context"
	"testing"

	"github.com/cloudwego/eino/components/tool"
	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"
)

func TestRemoteToolAdaptsSchemaAndCall(t *testing.T) {
	serverTransport, clientTransport := sdk.NewInMemoryTransports()
	server := sdk.NewServer(&sdk.Implementation{Name: "test"}, nil)
	type input struct {
		Subject string `json:"subject" jsonschema:"required"`
	}
	sdk.AddTool(server, &sdk.Tool{Name: "greet", Description: "Greets a subject."}, func(_ context.Context, _ *sdk.CallToolRequest, in input) (*sdk.CallToolResult, any, error) {
		return &sdk.CallToolResult{Content: []sdk.Content{&sdk.TextContent{Text: "hello " + in.Subject}}}, nil, nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = server.Run(ctx, serverTransport) }()
	client := sdk.NewClient(&sdk.Implementation{Name: "test-client"}, nil)
	session, err := client.Connect(ctx, clientTransport, nil)
	require.NoError(t, err)
	defer func() { _ = session.Close() }()
	listed, err := session.ListTools(ctx, nil)
	require.NoError(t, err)
	require.Len(t, listed.Tools, 1)

	adapter := remoteTool{name: "mcp__test__greet", remote: listed.Tools[0], session: session}
	info, err := adapter.Info(ctx)
	require.NoError(t, err)
	require.Equal(t, "mcp__test__greet", info.Name)
	require.Equal(t, "Greets a subject.", info.Desc)
	require.NotNil(t, info.ParamsOneOf)
	out, err := adapter.InvokableRun(ctx, `{"subject":"Flow"}`)
	require.NoError(t, err)
	require.Equal(t, "hello Flow", out)
	_, err = adapter.InvokableRun(ctx, `{`)
	require.ErrorContains(t, err, "decode arguments")
}

func TestServerValidationAndToolNames(t *testing.T) {
	require.ErrorContains(t, (Server{Name: "bad name", Command: "x"}).validate(), "must contain")
	require.ErrorContains(t, (Server{Name: "test"}).validate(), "exactly one")
	require.ErrorContains(t, (Server{Name: "test", Command: "x", URL: "http://x"}).validate(), "exactly one")
	require.NoError(t, (Server{Name: "test", Command: "x"}).validate())
	require.Equal(t, "mcp__server__tool", toolName("server", "tool"))
	require.False(t, validName.MatchString("not/a-tool"))
}

var _ tool.InvokableTool = remoteTool{}
