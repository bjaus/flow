set dotenv-load

# build + lint + test — the merge gate; must be clean before any commit.
# The app-module lines activate once app/ exists (Phase 1+); uncomment them then.
check: fmt
    go build ./...
    golangci-lint run
    go test ./...
    # cd app && go build ./... && golangci-lint run && go test ./...

fmt:
    gofmt -w .

# focused tests, e.g. `just test ./engine/...`
test path=".":
    go test {{path}}

# ---- recipes below target the app/ runtime you are building (SPEC.md §15) ----

# run the reference daemon with live reload; --demo self-seeds a sample run.
# NOTE: use watchexec, NOT air/wgo — a TUI-capable binary renders blank under those.
dev:
    watchexec -r -e go,html,css -- go run ./app/cmd/flowd serve --demo

# run the terminal UI against a running daemon
tui endpoint="http://localhost:7788":
    go run ./app/cmd/flowd tui --endpoint {{endpoint}}

# open the web app (macOS `open`; use xdg-open on Linux)
web endpoint="http://localhost:7788":
    open {{endpoint}}

# start the local OpenAI-compatible model gateway on :4000 (key from .env)
gateway:
    litellm --config ./litellm.yaml --port 4000

# render a scripted TUI scenario to a shareable gif (see the verify-and-iterate skill)
tape name:
    vhs tui/tapes/{{name}}.tape
