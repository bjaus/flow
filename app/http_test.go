package app

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bjaus/flow"
	"github.com/stretchr/testify/require"
)

func requestJSON(t *testing.T, client *http.Client, method, url string, body any, want int, out any) {
	t.Helper()
	var data []byte
	if body != nil {
		var err error
		data, err = json.Marshal(body)
		require.NoError(t, err)
	}
	req, err := http.NewRequest(method, url, bytes.NewReader(data))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	require.NoError(t, err)
	defer func() { require.NoError(t, resp.Body.Close()) }()
	require.Equal(t, want, resp.StatusCode)
	if out != nil {
		require.NoError(t, json.NewDecoder(resp.Body).Decode(out))
	}
}
func runWorker(t *testing.T, a *App) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.work(ctx) }()
	t.Cleanup(func() { cancel(); require.NoError(t, <-done) })
}

func TestHTTPAPIAndCLIEndToEnd(t *testing.T) {
	a, s := testApp(t, FakeProvider(nil))
	wf := flow.Define("echo", "echo an integer", flow.Do("echo", func(_ context.Context, in int) (int, error) { return in, nil }))
	require.NoError(t, a.Register(wf))
	runWorker(t, a)
	ts := httptest.NewServer(a.Handler())
	defer ts.Close()
	var workflows []WorkflowInfo
	requestJSON(t, ts.Client(), "GET", ts.URL+"/api/workflows", nil, 200, &workflows)
	require.Equal(t, "echo", workflows[0].Name)
	var triggered map[string]string
	requestJSON(t, ts.Client(), "POST", ts.URL+"/api/runs", map[string]any{"workflow": "echo", "input": 7}, 202, &triggered)
	id := triggered["id"]
	require.NotEmpty(t, id)
	run := waitRun(t, s, id, StatusSucceeded)
	var got Run
	requestJSON(t, ts.Client(), "GET", ts.URL+"/api/runs/"+id, nil, 200, &got)
	require.JSONEq(t, "7", string(got.Result))
	var listed []*Run
	requestJSON(t, ts.Client(), "GET", ts.URL+"/api/runs?workflow=echo&status=succeeded", nil, 200, &listed)
	require.Len(t, listed, 1)
	resp, err := ts.Client().Get(ts.URL + "/api/runs/" + id + "/events")
	require.NoError(t, err)
	scan := bufio.NewScanner(resp.Body)
	var eventLines []string
	for scan.Scan() {
		if strings.HasPrefix(scan.Text(), "event: ") {
			eventLines = append(eventLines, scan.Text())
			if strings.Contains(scan.Text(), string(EventRunFinished)) {
				break
			}
		}
	}
	require.NoError(t, resp.Body.Close())
	require.Contains(t, eventLines, "event: run.started")
	require.Contains(t, eventLines, "event: run.finished")
	cmd := ClientCLI()
	cmd.SetArgs([]string{"--endpoint", ts.URL, "runs", "list", "--json"})
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	require.NoError(t, cmd.ExecuteContext(context.Background()))
	var cliRuns []*Run
	require.NoError(t, json.Unmarshal(out.Bytes(), &cliRuns))
	require.NotEmpty(t, cliRuns)
	requestJSON(t, ts.Client(), "POST", ts.URL+"/api/runs/missing/cancel", nil, 409, nil)
	_ = run
}

func TestCancelStopsAtNextStepBoundary(t *testing.T) {
	a, s := testApp(t, FakeProvider(nil))
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	first := flow.Do("first", func(_ context.Context, in int) (int, error) {
		calls.Add(1)
		close(started)
		<-release
		return in + 1, nil
	})
	second := flow.Do("second", func(_ context.Context, in int) (int, error) { calls.Add(1); return in + 1, nil })
	require.NoError(t, a.Register(flow.Define("cancel", "cancel", flow.Then(first, second))))
	runWorker(t, a)
	id, err := a.Trigger(context.Background(), "cancel", json.RawMessage(`1`))
	require.NoError(t, err)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("step did not start")
	}
	require.NoError(t, a.Cancel(context.Background(), id))
	close(release)
	waitRun(t, s, id, StatusCanceled)
	require.Equal(t, int32(1), calls.Load())
	require.Error(t, a.Cancel(context.Background(), id))
}
func TestGracefulShutdownParksAfterInFlightUnitAndFreshAppResumes(t *testing.T) {
	a, stores := testApp(t, FakeProvider(nil))
	started := make(chan struct{})
	release := make(chan struct{})
	var starts sync.Once
	var firstCalls atomic.Int32
	first := flow.Do("call", func(_ context.Context, in int) (int, error) {
		firstCalls.Add(1)
		starts.Do(func() { close(started) })
		<-release
		return in + 1, nil
	})
	second := flow.Do("finish", func(_ context.Context, in int) (int, error) { return in + 1, nil })
	wf := flow.Define("slow", "slow", flow.Then(first, second))
	require.NoError(t, a.Register(wf))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Serve(ctx) }()
	id, err := a.Trigger(context.Background(), "slow", json.RawMessage(`1`))
	require.NoError(t, err)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("step did not start")
	}
	cancel()
	select {
	case err := <-done:
		t.Fatalf("serve returned before in-flight call completed: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("drain timed out")
	}
	waitRun(t, stores, id, StatusParked)
	a2, err := New(Config{Store: stores, Provider: FakeProvider(nil), Listen: "127.0.0.1:0"})
	require.NoError(t, err)
	require.NoError(t, a2.Register(wf))
	serve(t, a2)
	run := waitRun(t, stores, id, StatusSucceeded)
	require.JSONEq(t, `3`, string(run.Result))
	require.LessOrEqual(t, firstCalls.Load(), int32(2))
}
func TestDrainOnlyRejectsNewRuns(t *testing.T) {
	a, _ := testApp(t, FakeProvider(nil))
	a.cfg.DrainOnly = true
	require.NoError(t, a.Register(flow.Define("w", "w", flow.Do("x", func(_ context.Context, v int) (int, error) { return v, nil }))))
	_, err := a.Trigger(context.Background(), "w", json.RawMessage(`1`))
	require.ErrorContains(t, err, "drain-only")
}

func TestMigrationActions(t *testing.T) {
	a, s := testApp(t, FakeProvider(nil))
	wf := flow.Define("w", "w", flow.Do("x", func(_ context.Context, v int) (int, error) { return v, nil }))
	require.NoError(t, a.Register(wf))
	id, err := a.Trigger(context.Background(), "w", json.RawMessage(`1`))
	require.NoError(t, err)
	r, err := s.Runs.Get(context.Background(), id)
	require.NoError(t, err)
	r.Status = StatusNeedsMigration
	require.NoError(t, s.Runs.Save(context.Background(), r))
	require.Error(t, a.Migrate(context.Background(), id, "bad"))
	require.NoError(t, a.Migrate(context.Background(), id, "restart"))
	r, err = s.Runs.Get(context.Background(), id)
	require.NoError(t, err)
	require.Equal(t, StatusQueued, r.Status)
	for action, expected := range map[string]Status{"abandon": StatusCanceled, "finish_on_previous": StatusParked} {
		id, err := a.Trigger(context.Background(), "w", json.RawMessage(`1`))
		require.NoError(t, err)
		run, err := s.Runs.Get(context.Background(), id)
		require.NoError(t, err)
		run.Status = StatusNeedsMigration
		require.NoError(t, s.Runs.Save(context.Background(), run))
		require.NoError(t, a.Migrate(context.Background(), id, action))
		run, err = s.Runs.Get(context.Background(), id)
		require.NoError(t, err)
		require.Equal(t, expected, run.Status)
	}
}
