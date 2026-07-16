package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/bjaus/flow"
	"github.com/bjaus/flow/app"
	"github.com/spf13/cobra"
)

type demoResult struct {
	Message  string `json:"message"`
	Approved bool   `json:"approved"`
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	provider := app.FakeProvider(app.FakeScript{"assistant": {"*": {"{\"message\":\"Demo completed\",\"approved\":false}"}}})
	a, err := app.New(app.Config{Provider: provider})
	if err != nil {
		fmt.Fprintln(os.Stderr, "init:", err)
		os.Exit(1)
	}
	root := a.CLI()
	var demo bool
	serve, _, _ := root.Find([]string{"serve"})
	serve.Flags().BoolVar(&demo, "demo", false, "register and seed deterministic demo workflows")
	serve.PreRunE = func(cmd *cobra.Command, _ []string) error {
		if !demo {
			return nil
		}
		stream := flow.Define("demo-stream", "A deterministic streaming agent demo", flow.Agent[string, demoResult]("assistant", func(s string) string { return s }))
		gate := flow.Define("demo-review", "A run awaiting operator review", flow.Human("review", func(v demoResult, d flow.Decision) demoResult { v.Approved = d.Approved; return v }, func(demoResult) string { return "Approve the demo result?" }))
		if err := a.Register(stream); err != nil {
			return err
		}
		if err := a.Register(gate); err != nil {
			return err
		}
		_, err := a.Trigger(cmd.Context(), "demo-stream", json.RawMessage(`"show streaming"`))
		if err != nil {
			return err
		}
		_, err = a.Trigger(cmd.Context(), "demo-review", json.RawMessage(`{"message":"Please review","approved":false}`))
		return err
	}
	if err := root.ExecuteContext(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
