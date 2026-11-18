package pkg

import (
	"os"
	"strings"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/apikey"
	"github.com/oarkflow/fh/mw/bodylimit"
	"github.com/oarkflow/fh/mw/compress"
	"github.com/oarkflow/fh/mw/recover"
	"github.com/oarkflow/fh/mw/requestid"
	"github.com/oarkflow/fh/mw/security"
	fhstatic "github.com/oarkflow/fh/mw/static"
)

// NewFHApp builds LookupX's native fh application. HTTP requests stay in fh
// contexts throughout routing and handler execution; no net/http adaptation is
// performed on the server path.
func NewFHApp(handler any) *fh.App {
	maxBodyBytes := fhMaxBodyBytes(handler)
	app := fh.New(
		fh.WithStreamRequestBody(true),
		fh.WithMaxRequestBodySize(maxBodyBytes),
		fh.WithSecureByDefault(true),
		fh.WithStartupBannerDisabled(true),
	)
	app.Use(
		recover.New(recover.Config{EnableStackTrace: false}),
		requestid.New(requestid.Config{}),
		security.New(),
	)
	if auth := fhAuthMiddlewares(handler); len(auth) > 0 {
		app.Use(auth...)
	}
	if _, ok := handler.(*MultiServer); ok {
		app.Use(
			bodylimit.WithConfig(bodylimit.Config{Limit: 32 << 20, Next: isFileUploadRequest}),
			bodylimit.WithConfig(bodylimit.Config{Limit: int(maxIndexUploadBytes + (64 << 20)), Next: func(c fh.Ctx) bool { return !isFileUploadRequest(c) }}),
		)
	} else {
		app.Use(bodylimit.New(maxBodyBytes))
	}
	app.Use(
		compress.New(compress.Config{}),
	)
	switch h := handler.(type) {
	case *Server:
		registerServerRoutes(app, h)
	case *MultiServer:
		registerMultiServerRoutes(app, h)
		root := h.WebDir
		if root == "" {
			root = defaultWebDir()
		}
		app.Get("/*", fhstatic.New(root, fhstatic.Config{
			Root: root, Index: "index.html", CacheControl: "no-cache",
			ETag: true, LastModified: true,
		}))
	default:
		app.Get("/*path", func(c fh.Ctx) error { return c.Status(404).SendString("404 page not found\n") })
	}
	return app
}

func defaultWebDir() string {
	for _, root := range []string{"web", "../web"} {
		if info, err := os.Stat(root); err == nil && info.IsDir() {
			return root
		}
	}
	return "web"
}

func fhMaxBodyBytes(handler any) int {
	if server, ok := handler.(*Server); ok && server.Index != nil && server.Index.cfg.MaxRequestBytes > 0 {
		return int(server.Index.cfg.MaxRequestBytes)
	}
	if _, ok := handler.(*MultiServer); ok {
		return int(maxIndexUploadBytes + (64 << 20))
	}
	return 32 << 20
}

func fhAuthMiddlewares(handler any) []fh.HandlerFunc {
	var keys []string
	switch h := handler.(type) {
	case *MultiServer:
		if h == nil || h.Manager == nil {
			return nil
		}
		keys = h.APIKeys
	case *Server:
		if h == nil || h.Index == nil {
			return nil
		}
		gate := h.requestGateFH
		if len(h.Index.cfg.APIKeys) == 0 {
			return []fh.HandlerFunc{gate}
		}
		keys = h.Index.cfg.APIKeys
		return append([]fh.HandlerFunc{gate}, fhAPIKeyMiddlewares(keys)...)
	}
	return fhAPIKeyMiddlewares(keys)
}

func fhAPIKeyMiddlewares(keys []string) []fh.HandlerFunc {
	if len(keys) == 0 {
		return nil
	}
	unauthorized := func(c fh.Ctx) error { return fhError(c, 401, "unauthorized") }
	xAPIKey := apikey.New(apikey.Config{
		Header: "X-API-Key", Keys: keys, Error: unauthorized,
		Next: func(c fh.Ctx) bool {
			return c.Get("X-API-Key") == "" || strings.HasPrefix(c.Get("Authorization"), "Bearer ")
		},
	})
	bearer := apikey.New(apikey.Config{
		Header: "Authorization", Error: unauthorized,
		Lookup: func(_ fh.Ctx, value string) bool {
			if !strings.HasPrefix(value, "Bearer ") {
				return false
			}
			value = strings.TrimPrefix(value, "Bearer ")
			for _, allowed := range keys {
				if constantTimeStringEqual(allowed, value) {
					return true
				}
			}
			return false
		},
		Next: func(c fh.Ctx) bool {
			return !strings.HasPrefix(c.Get("Authorization"), "Bearer ") && c.Get("X-API-Key") != ""
		},
	})
	return []fh.HandlerFunc{xAPIKey, bearer}
}

func isFileUploadRequest(c fh.Ctx) bool {
	return c.Method() == "POST" && strings.HasSuffix(c.Path(), "/reload-file")
}
