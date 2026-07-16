package provider_test

import (
	"context"
	"io"
	"testing"

	"github.com/bjaus/flow/app"
	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/schema"
	"github.com/stretchr/testify/require"
)

func TestFakeProviderExactFallbackQueueAndCalls(t *testing.T) {
	p := app.FakeProvider(app.FakeScript{"writer": {"hello": {"first", "second"}, "*": {"fallback"}}})
	m, err := p.Model(context.Background(), app.Persona{Name: "writer"})
	require.NoError(t, err)
	msg, err := m.Generate(context.Background(), []*schema.Message{schema.UserMessage("hello")})
	require.NoError(t, err)
	require.Equal(t, "first", msg.Content)
	msg, err = m.Generate(context.Background(), []*schema.Message{schema.UserMessage("hello")})
	require.NoError(t, err)
	require.Equal(t, "second", msg.Content)
	msg, err = m.Generate(context.Background(), []*schema.Message{schema.UserMessage("hello")})
	require.NoError(t, err)
	require.Equal(t, "second", msg.Content)
	msg, err = m.Generate(context.Background(), []*schema.Message{schema.UserMessage("other")})
	require.NoError(t, err)
	require.Equal(t, "fallback", msg.Content)
	require.Len(t, p.Calls(), 4)
	require.Equal(t, "other", p.Calls()[3].Input)
}

func TestFakeProviderStreamsCompleteResponse(t *testing.T) {
	p := app.FakeProvider(app.FakeScript{"writer": {"*": {"one two three"}}})
	m, err := p.Model(context.Background(), app.Persona{Name: "writer"})
	require.NoError(t, err)
	r, err := m.Stream(context.Background(), []*schema.Message{schema.UserMessage("x")})
	require.NoError(t, err)
	defer r.Close()
	var got string
	for {
		msg, err := r.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		got += msg.Content
	}
	require.Equal(t, "one two three", got)
}

func TestFakeProviderRejectsMissingScript(t *testing.T) {
	_, err := app.FakeProvider(nil).Model(context.Background(), app.Persona{Name: "missing"})
	require.ErrorContains(t, err, "no script")
}

func TestFakeProviderSupportsToolBearingPersonas(t *testing.T) {
	m, err := app.FakeProvider(app.FakeScript{"worker": {"*": {"done"}}}).
		Model(context.Background(), app.Persona{Name: "worker"})
	require.NoError(t, err)
	toolModel, ok := m.(model.ToolCallingChatModel)
	require.True(t, ok, "fake model must support tool-bearing personas")
	configured, err := toolModel.WithTools(nil)
	require.NoError(t, err)
	msg, err := configured.Generate(context.Background(), []*schema.Message{schema.UserMessage("go")})
	require.NoError(t, err)
	require.Equal(t, "done", msg.Content)
}
