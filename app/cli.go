package app

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/bjaus/flow/app/tui"
	"github.com/spf13/cobra"
)

type apiClient struct {
	endpoint string
	http     *http.Client
}

func (c apiClient) request(ctx context.Context, method, path string, body any, out any) error {
	var r io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return err
		}
		r = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(c.endpoint, "/")+path, r)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		var e struct {
			Error string `json:"error"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&e)
		if e.Error == "" {
			e.Error = resp.Status
		}
		return errors.New(e.Error)
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}

// CLI returns the full command tree bound to a daemon App.
func CLI(a *App) *cobra.Command { return commandTree(a) }

// CLI returns the full command tree bound to a.
func (a *App) CLI() *cobra.Command { return commandTree(a) }

// ClientCLI returns the workflow, run, and TUI commands that need only an endpoint.
func ClientCLI() *cobra.Command { return commandTree(nil) }

func commandTree(a *App) *cobra.Command {
	var endpoint string
	root := &cobra.Command{Use: "flow", SilenceUsage: true, SilenceErrors: true}
	root.PersistentFlags().StringVar(&endpoint, "endpoint", "http://localhost:7788", "flow daemon endpoint")
	if a != nil {
		var drainOnly bool
		serve := &cobra.Command{Use: "serve", Short: "run the daemon", RunE: func(cmd *cobra.Command, _ []string) error { a.cfg.DrainOnly = drainOnly; return a.Serve(cmd.Context()) }}
		serve.Flags().BoolVar(&drainOnly, "drain-only", false, "resume only version-pinned runs and reject new work")
		root.AddCommand(serve)
	}
	workflows := &cobra.Command{Use: "workflows"}
	workflows.AddCommand(workflowsList(&endpoint))
	root.AddCommand(workflows)
	runs := &cobra.Command{Use: "runs"}
	runs.AddCommand(runTrigger(&endpoint), runList(&endpoint), runGet(&endpoint), runWatch(&endpoint), runDecision(&endpoint, true), runDecision(&endpoint, false), runCancel(&endpoint), runMigrate(&endpoint))
	root.AddCommand(runs)
	config := &cobra.Command{Use: "config"}
	config.AddCommand(configStatus(&endpoint), configReload(&endpoint))
	root.AddCommand(config)
	root.AddCommand(&cobra.Command{Use: "tui", Short: "open the terminal client", RunE: func(cmd *cobra.Command, _ []string) error { return tui.Run(cmd.Context(), endpoint) }})
	return root
}
func client(endpoint *string) apiClient {
	return apiClient{endpoint: *endpoint, http: http.DefaultClient}
}
func printJSON(w io.Writer, v any) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}
func workflowsList(endpoint *string) *cobra.Command {
	var jsonOut bool
	c := &cobra.Command{Use: "list", RunE: func(cmd *cobra.Command, _ []string) error {
		var out []WorkflowInfo
		if err := client(endpoint).request(cmd.Context(), "GET", "/api/workflows", nil, &out); err != nil {
			return err
		}
		if jsonOut {
			return printJSON(cmd.OutOrStdout(), out)
		}
		tw := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 4, 2, ' ', 0)
		_, _ = fmt.Fprintln(tw, "NAME\tINPUT\tOUTPUT\tDESCRIPTION")
		for _, w := range out {
			_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", w.Name, w.InputType, w.OutputType, w.Description)
		}
		return tw.Flush()
	}}
	c.Flags().BoolVar(&jsonOut, "json", false, "print JSON")
	return c
}
func runTrigger(endpoint *string) *cobra.Command {
	var input string
	c := &cobra.Command{Use: "trigger <workflow>", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		raw, err := readInput(input)
		if err != nil {
			return err
		}
		var out map[string]string
		if err = client(endpoint).request(cmd.Context(), "POST", "/api/runs", map[string]any{"workflow": args[0], "input": json.RawMessage(raw)}, &out); err != nil {
			return err
		}
		_, err = fmt.Fprintln(cmd.OutOrStdout(), out["id"])
		return err
	}}
	c.Flags().StringVar(&input, "input", "null", "JSON or @file")
	return c
}
func readInput(s string) ([]byte, error) {
	if strings.HasPrefix(s, "@") {
		return os.ReadFile(strings.TrimPrefix(s, "@"))
	}
	if !json.Valid([]byte(s)) {
		return nil, errors.New("input is not valid JSON")
	}
	return []byte(s), nil
}
func runList(endpoint *string) *cobra.Command {
	var status, workflow, parent string
	var jsonOut bool
	c := &cobra.Command{Use: "list", RunE: func(cmd *cobra.Command, _ []string) error {
		path := "/api/runs?status=" + status + "&workflow=" + workflow + "&parent=" + parent
		var out []*Run
		if err := client(endpoint).request(cmd.Context(), "GET", path, nil, &out); err != nil {
			return err
		}
		if jsonOut {
			return printJSON(cmd.OutOrStdout(), out)
		}
		tw := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 4, 2, ' ', 0)
		_, _ = fmt.Fprintln(tw, "ID\tWORKFLOW\tSTATUS\tUPDATED")
		for _, r := range out {
			_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", r.ID, r.Workflow, r.Status, r.UpdatedAt.Format("2006-01-02 15:04:05"))
		}
		return tw.Flush()
	}}
	c.Flags().StringVar(&status, "status", "", "filter status")
	c.Flags().StringVar(&workflow, "workflow", "", "filter workflow")
	c.Flags().StringVar(&parent, "parent", "", "filter children of a parent run id")
	c.Flags().BoolVar(&jsonOut, "json", false, "print JSON")
	return c
}
func runGet(endpoint *string) *cobra.Command {
	var jsonOut bool
	c := &cobra.Command{Use: "get <id>", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		var r Run
		if err := client(endpoint).request(cmd.Context(), "GET", "/api/runs/"+args[0], nil, &r); err != nil {
			return err
		}
		if jsonOut {
			return printJSON(cmd.OutOrStdout(), r)
		}
		tw := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 4, 2, ' ', 0)
		_, _ = fmt.Fprintf(tw, "ID\t%s\nWORKFLOW\t%s\nSTATUS\t%s\nINPUT\t%s\nRESULT\t%s\nERROR\t%s\n", r.ID, r.Workflow, r.Status, r.Input, r.Result, r.Error)
		return tw.Flush()
	}}
	c.Flags().BoolVar(&jsonOut, "json", false, "print JSON")
	return c
}
func runWatch(endpoint *string) *cobra.Command {
	return &cobra.Command{Use: "watch <id>", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		req, err := http.NewRequestWithContext(cmd.Context(), "GET", strings.TrimRight(*endpoint, "/")+"/api/runs/"+args[0]+"/events", nil)
		if err != nil {
			return err
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode >= 300 {
			return fmt.Errorf("watch: %s", resp.Status)
		}
		scan := bufio.NewScanner(resp.Body)
		for scan.Scan() {
			line := scan.Text()
			if strings.HasPrefix(line, "data: ") {
				if _, err := fmt.Fprintln(cmd.OutOrStdout(), strings.TrimPrefix(line, "data: ")); err != nil {
					return err
				}
				var e Event
				if json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &e) == nil && e.Kind == EventRunFinished {
					return nil
				}
			}
		}
		return scan.Err()
	}}
}
func runDecision(endpoint *string, approved bool) *cobra.Command {
	var feedback string
	name := "approve"
	if !approved {
		name = "return"
	}
	c := &cobra.Command{Use: name + " <id>", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		return client(endpoint).request(cmd.Context(), "POST", "/api/runs/"+args[0]+"/decision", Decision{Approved: approved, Feedback: feedback}, nil)
	}}
	c.Flags().StringVar(&feedback, "feedback", "", "operator feedback")
	return c
}
func runCancel(endpoint *string) *cobra.Command {
	return &cobra.Command{Use: "cancel <id>", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		return client(endpoint).request(cmd.Context(), "POST", "/api/runs/"+args[0]+"/cancel", nil, nil)
	}}
}
func runMigrate(endpoint *string) *cobra.Command {
	return &cobra.Command{Use: "migrate <id> <restart|abandon|finish_on_previous>", Args: cobra.ExactArgs(2), RunE: func(cmd *cobra.Command, args []string) error {
		return client(endpoint).request(cmd.Context(), "POST", "/api/runs/"+args[0]+"/migration", map[string]string{"action": args[1]}, nil)
	}}
}
func configStatus(endpoint *string) *cobra.Command {
	return &cobra.Command{Use: "status", RunE: func(cmd *cobra.Command, _ []string) error {
		var status ConfigStatus
		if err := client(endpoint).request(cmd.Context(), "GET", "/api/config", nil, &status); err != nil {
			return err
		}
		return printJSON(cmd.OutOrStdout(), status)
	}}
}
func configReload(endpoint *string) *cobra.Command {
	return &cobra.Command{Use: "reload", RunE: func(cmd *cobra.Command, _ []string) error {
		var status ConfigStatus
		if err := client(endpoint).request(cmd.Context(), "POST", "/api/config/reload", nil, &status); err != nil {
			return err
		}
		return printJSON(cmd.OutOrStdout(), status)
	}}
}
