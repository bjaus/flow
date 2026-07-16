package tools_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bjaus/flow/app/tools"
	"github.com/cloudwego/eino/components/tool"
	"github.com/stretchr/testify/require"
)

func run(t *testing.T, tl tool.BaseTool, arguments string) (string, error) {
	t.Helper()
	invokable, ok := tl.(tool.InvokableTool)
	require.True(t, ok, "tool must be invokable")
	return invokable.InvokableRun(context.Background(), arguments)
}

func newRoot(t *testing.T, files map[string]string) string {
	t.Helper()
	root := t.TempDir()
	for name, content := range files {
		path := filepath.Join(root, filepath.FromSlash(name))
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
		require.NoError(t, os.WriteFile(path, []byte(content), 0o644))
	}
	return root
}

func TestDefaultRegistersEveryGuardedToolName(t *testing.T) {
	all := tools.Default(t.TempDir())
	for _, name := range []string{"bash", "read", "write", "edit", "ls", "grep", "find"} {
		tl, ok := all[name]
		require.True(t, ok, "missing tool %q", name)
		info, err := tl.Info(context.Background())
		require.NoError(t, err)
		require.Equal(t, name, info.Name)
		require.NotNil(t, info.ParamsOneOf)
	}
}

func TestBash(t *testing.T) {
	root := newRoot(t, map[string]string{"note.txt": "hello"})
	bash := tools.Bash(root)
	tests := []struct {
		name    string
		args    string
		want    string
		wantErr string
	}{
		{name: "runs in root", args: `{"command":"cat note.txt"}`, want: "hello\nexit status: 0"},
		{name: "reports exit status", args: `{"command":"exit 3"}`, want: "exit status: 3"},
		{name: "missing command", args: `{}`, wantErr: "command is required"},
		{name: "times out", args: `{"command":"sleep 5","timeout":0.05}`, wantErr: "timed out"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out, err := run(t, bash, tt.args)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Contains(t, out, tt.want)
		})
	}
}

func TestBashTruncatesLargeOutput(t *testing.T) {
	out, err := run(t, tools.Bash(t.TempDir()), `{"command":"yes x | head -c 100000"}`)
	require.NoError(t, err)
	require.Contains(t, out, "output truncated")
	require.Less(t, len(out), 70*1024)
}

func TestRead(t *testing.T) {
	root := newRoot(t, map[string]string{"dir/file.txt": "one\ntwo\nthree\nfour"})
	read := tools.Read(root)
	tests := []struct {
		name    string
		args    string
		want    string
		wantErr string
	}{
		{name: "whole file", args: `{"path":"dir/file.txt"}`, want: "one\ntwo\nthree\nfour"},
		{name: "offset and limit", args: `{"path":"dir/file.txt","offset":2,"limit":2}`, want: "two\nthree"},
		{name: "offset beyond end", args: `{"path":"dir/file.txt","offset":99}`, want: ""},
		{name: "missing file", args: `{"path":"nope.txt"}`, wantErr: "no such file"},
		{name: "escape rejected", args: `{"path":"../secret.txt"}`, wantErr: "escapes the tool root"},
		{name: "absolute rejected", args: `{"path":"/etc/passwd"}`, wantErr: "escapes the tool root"},
		{name: "sneaky traversal rejected", args: `{"path":"dir/../../secret.txt"}`, wantErr: "escapes the tool root"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out, err := run(t, read, tt.args)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, out)
		})
	}
}

func TestWrite(t *testing.T) {
	root := t.TempDir()
	write := tools.Write(root)
	tests := []struct {
		name    string
		args    string
		path    string
		want    string
		wantErr string
	}{
		{name: "creates file and parents", args: `{"path":"new/dir/out.txt","content":"payload"}`, path: "new/dir/out.txt", want: "payload"},
		{name: "overwrites", args: `{"path":"new/dir/out.txt","content":"second"}`, path: "new/dir/out.txt", want: "second"},
		{name: "escape rejected", args: `{"path":"../../evil.txt","content":"x"}`, wantErr: "escapes the tool root"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := run(t, write, tt.args)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			content, readErr := os.ReadFile(filepath.Join(root, filepath.FromSlash(tt.path)))
			require.NoError(t, readErr)
			require.Equal(t, tt.want, string(content))
		})
	}
}

func TestEdit(t *testing.T) {
	tests := []struct {
		name    string
		initial string
		args    string
		want    string
		wantErr string
	}{
		{
			name:    "unique replacement",
			initial: "alpha beta gamma",
			args:    `{"path":"f.txt","old_string":"beta","new_string":"delta"}`,
			want:    "alpha delta gamma",
		},
		{
			name:    "duplicate without replace_all fails",
			initial: "x y x",
			args:    `{"path":"f.txt","old_string":"x","new_string":"z"}`,
			wantErr: "appears 2 times",
		},
		{
			name:    "replace_all",
			initial: "x y x",
			args:    `{"path":"f.txt","old_string":"x","new_string":"z","replace_all":true}`,
			want:    "z y z",
		},
		{
			name:    "old string absent",
			initial: "abc",
			args:    `{"path":"f.txt","old_string":"zzz","new_string":"q"}`,
			wantErr: "not found",
		},
		{
			name:    "identical strings rejected",
			initial: "abc",
			args:    `{"path":"f.txt","old_string":"a","new_string":"a"}`,
			wantErr: "identical",
		},
		{
			name:    "escape rejected",
			initial: "abc",
			args:    `{"path":"../f.txt","old_string":"a","new_string":"b"}`,
			wantErr: "escapes the tool root",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := newRoot(t, map[string]string{"f.txt": tt.initial})
			_, err := run(t, tools.Edit(root), tt.args)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			content, readErr := os.ReadFile(filepath.Join(root, "f.txt"))
			require.NoError(t, readErr)
			require.Equal(t, tt.want, string(content))
		})
	}
}

func TestLs(t *testing.T) {
	root := newRoot(t, map[string]string{"a.txt": "", "sub/b.txt": ""})
	ls := tools.Ls(root)
	tests := []struct {
		name    string
		args    string
		want    []string
		wantErr string
	}{
		{name: "root listing", args: `{}`, want: []string{"a.txt", "sub/"}},
		{name: "subdirectory", args: `{"path":"sub"}`, want: []string{"b.txt"}},
		{name: "escape rejected", args: `{"path":".."}`, wantErr: "escapes the tool root"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out, err := run(t, ls, tt.args)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, strings.Split(out, "\n"))
		})
	}
}

func TestGrep(t *testing.T) {
	root := newRoot(t, map[string]string{
		"main.go":      "package main\nfunc main() {}\n",
		"sub/util.go":  "package sub\nfunc Helper() {}\n",
		".git/ignored": "func main() {}\n",
	})
	grep := tools.Grep(root)
	tests := []struct {
		name     string
		args     string
		contains []string
		absent   []string
		wantErr  string
	}{
		{
			name:     "matches across files with location",
			args:     `{"pattern":"func \\w+"}`,
			contains: []string{"main.go:2:func main() {}", "sub/util.go:2:func Helper() {}"},
			absent:   []string{".git"},
		},
		{name: "scoped to path", args: `{"pattern":"func","path":"sub"}`, contains: []string{"sub/util.go:2"}, absent: []string{"main.go"}},
		{name: "no matches", args: `{"pattern":"unicorn"}`, contains: []string{"no matches"}},
		{name: "invalid regexp", args: `{"pattern":"("}`, wantErr: "invalid pattern"},
		{name: "escape rejected", args: `{"pattern":"x","path":"../"}`, wantErr: "escapes the tool root"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out, err := run(t, grep, tt.args)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			for _, want := range tt.contains {
				require.Contains(t, out, want)
			}
			for _, notWant := range tt.absent {
				require.NotContains(t, out, notWant)
			}
		})
	}
}

func TestFind(t *testing.T) {
	root := newRoot(t, map[string]string{
		"main.go":     "",
		"sub/util.go": "",
		"sub/note.md": "",
		".git/config": "",
	})
	find := tools.Find(root)
	tests := []struct {
		name     string
		args     string
		want     []string
		wantErr  string
		wantText string
	}{
		{name: "by name", args: `{"pattern":"*.go"}`, want: []string{"main.go", "sub/util.go"}},
		{name: "by relative path", args: `{"pattern":"sub/*.md"}`, want: []string{"sub/note.md"}},
		{name: "scoped to path", args: `{"pattern":"*.go","path":"sub"}`, want: []string{"sub/util.go"}},
		{name: "no results", args: `{"pattern":"*.rs"}`, wantText: "no files found"},
		{name: "missing pattern", args: `{}`, wantErr: "pattern is required"},
		{name: "invalid pattern", args: `{"pattern":"[a-"}`, wantErr: "invalid pattern"},
		{name: "escape rejected", args: `{"pattern":"*","path":"../.."}`, wantErr: "escapes the tool root"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out, err := run(t, find, tt.args)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			if tt.wantText != "" {
				require.Equal(t, tt.wantText, out)
				return
			}
			require.ElementsMatch(t, tt.want, strings.Split(out, "\n"))
			require.NotContains(t, out, ".git")
		})
	}
}
