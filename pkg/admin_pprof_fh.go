package pkg

import (
	"github.com/oarkflow/fh"
	fhpprof "github.com/oarkflow/fh/mw/pprof"
)

// registerPprofRoutes exposes Go runtime profiles through fh's built-in
// pprof middleware, gated behind the same admin session required by the
// rest of /admin/api/*. This closes the observability gap of having no way
// to pull a live heap/goroutine/CPU profile from a running server.
func registerPprofRoutes(app *fh.App, server *MultiServer) {
	fhpprof.Enable(app, fhpprof.Config{
		Prefix: "/admin/debug/pprof",
		Auth: func(c fh.Ctx) bool {
			return server.auth.requireAdmin(c, false) == nil
		},
	})
}
