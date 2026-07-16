set dotenv-load

# Build, format, lint, and test both modules. This is the merge gate.
check: fmt
    go build ./...
    cd app && go build ./...
    golangci-lint run
    cd app && golangci-lint run
    go test ./...
    cd app && go test ./...

fmt:
    go fmt ./...
    cd app && go fmt ./...

test path=".":
    #!/usr/bin/env bash
    set -euo pipefail
    p="{{path}}"
    if [[ "$p" == ./app/* ]]; then
      cd app
      go test "./${p#./app/}"
    else
      go test "$p"
    fi

dev:
    watchexec -r -e go,html,css -- go run ./app/cmd/flowd serve --demo

tui endpoint="http://localhost:7788":
    go run ./app/cmd/flowd tui --endpoint {{endpoint}}

web endpoint="http://localhost:7788":
    open {{endpoint}}

gateway:
    litellm --config ./litellm.yaml --port 4000

tape name:
    vhs app/tui/tapes/{{name}}.tape

# Build a replacement binary. Set FLOW_PID to drain a running daemon before swapping.
deploy target="./flowd":
    #!/usr/bin/env bash
    set -euo pipefail
    next="{{target}}.next"
    go build -o "$next" ./app/cmd/flowd
    if [[ -n "${FLOW_PID:-}" ]]; then
      kill -TERM "$FLOW_PID"
      while kill -0 "$FLOW_PID" 2>/dev/null; do sleep .1; done
    fi
    mv "$next" "{{target}}"
