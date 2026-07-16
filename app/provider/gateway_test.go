package provider_test

import (
	"context"
	"os"
	"testing"

	"github.com/bjaus/flow/app"
	"github.com/stretchr/testify/require"
)

func TestGatewayValidatesConfiguration(t *testing.T) {
	t.Setenv("FLOW_GATEWAY_URL", "")
	_, err := app.Gateway("").Model(context.Background(), app.Persona{Name: "a", Model: "m"})
	require.ErrorContains(t, err, "FLOW_GATEWAY_URL")
	_, err = app.Gateway("http://localhost:1/v1").Model(context.Background(), app.Persona{Name: "a"})
	require.ErrorContains(t, err, "model")
}
func TestGatewayBuildsModelWhenConfigured(t *testing.T) {
	endpoint := os.Getenv("FLOW_GATEWAY_URL")
	if endpoint == "" {
		t.Skip("FLOW_GATEWAY_URL is unset")
	}
	model, err := app.Gateway(endpoint).Model(context.Background(), app.Persona{Name: "smoke", Model: "local"})
	require.NoError(t, err)
	require.NotNil(t, model)
}
