package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/bjaus/flow/app"
	"github.com/spf13/cobra"
)

func main() {
	root := app.ClientCLI()
	root.AddCommand(&cobra.Command{Use: "init", Short: "scaffold a flow project in the current directory", Args: cobra.NoArgs, RunE: func(*cobra.Command, []string) error { return scaffold(".") }})
	if err := root.ExecuteContext(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func scaffold(root string) error {
	files := map[string]string{
		"go.mod": "module workflows\n\ngo 1.24\n",
		"main.go": `package main
import("context";"log";"github.com/bjaus/flow";"github.com/bjaus/flow/app")
func main(){wf:=flow.Define("hello","A starter workflow",flow.Do("hello",func(_ context.Context,in string)(string,error){return "Hello, "+in,nil}));a,err:=app.New(app.Config{});if err!=nil{log.Fatal(err)};if err=a.Register(wf);err!=nil{log.Fatal(err)};if err=a.CLI().Execute();err!=nil{log.Fatal(err)}}
`,
		"agents/assistant.md":    "---\nname: assistant\nmodel: local\ntools: []\nskills: []\n---\nYou are a careful assistant.\n",
		"skills/review/SKILL.md": "---\nname: review\n---\nReview work for correctness and completeness.\n",
		"justfile":               "dev:\n    go run . serve\n\ncheck:\n    gofmt -w .\n    go test ./...\n",
	}
	for name, data := range files {
		path := filepath.Join(root, name)
		if _, err := os.Stat(path); err == nil {
			return fmt.Errorf("%s already exists", name)
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(path, []byte(data), 0o644); err != nil {
			return err
		}
	}
	return nil
}
