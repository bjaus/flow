// Package app turns typed flow workflows into a durable local daemon.
//
// New applies SQLite, gateway, filesystem-registry, and no-op tracing defaults.
// Register adds compiled-in workflow definitions. Serve owns the queue worker and
// HTTP/SSE server until its context is canceled. CLI, ClientCLI, and TUI expose
// clients of the same API, while the embedded PWA is served at the daemon root.
package app
