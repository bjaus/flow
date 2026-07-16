package app

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/bjaus/flow"
	"github.com/stretchr/testify/require"
)

func TestEmbeddedWebAppShellAssetsAndHTMXActions(t *testing.T) {
	a, s := testApp(t, FakeProvider(nil))
	require.NoError(t, a.Register(flow.Define("echo", "echo", flow.Do("echo", func(_ context.Context, in string) (string, error) { return in, nil }))))
	runWorker(t, a)
	ts := httptest.NewServer(a.Handler())
	defer ts.Close()
	for path, contentType := range map[string]string{"/": "text/html", "/manifest.webmanifest": "application/manifest+json", "/service-worker.js": "text/javascript", "/static/htmx.min.js": "text/javascript", "/static/style.css": "text/css"} {
		resp, err := ts.Client().Get(ts.URL + path)
		require.NoError(t, err)
		data, err := io.ReadAll(resp.Body)
		require.NoError(t, resp.Body.Close())
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		require.Contains(t, resp.Header.Get("Content-Type"), contentType)
		require.NotEmpty(t, data)
	}
	form := url.Values{"workflow": {"echo"}, "input": {`"web"`}}
	resp, err := ts.Client().Post(ts.URL+"/ui/runs", "application/x-www-form-urlencoded", strings.NewReader(form.Encode()))
	require.NoError(t, err)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, resp.Body.Close())
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	require.Contains(t, string(body), "echo")
	runs, err := s.Runs.List(context.Background(), RunFilter{Workflow: "echo"})
	require.NoError(t, err)
	require.Len(t, runs, 1)
	waitRun(t, s, runs[0].ID, StatusSucceeded)
	resp, err = ts.Client().Get(ts.URL + "/ui/runs")
	require.NoError(t, err)
	body, err = io.ReadAll(resp.Body)
	require.NoError(t, resp.Body.Close())
	require.NoError(t, err)
	require.Contains(t, string(body), "succeeded")
	require.True(t, json.Valid(runs[0].Input))
}
