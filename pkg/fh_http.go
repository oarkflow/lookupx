package pkg

import (
	"os"
	"strings"
	"time"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/apikey"
	"github.com/oarkflow/fh/mw/bodylimit"
	"github.com/oarkflow/fh/mw/compress"
	"github.com/oarkflow/fh/mw/ratelimiter"
	"github.com/oarkflow/fh/mw/realip"
	"github.com/oarkflow/fh/mw/recover"
	"github.com/oarkflow/fh/mw/requestid"
	"github.com/oarkflow/fh/mw/security"
	"github.com/oarkflow/fh/mw/session"
	fhstatic "github.com/oarkflow/fh/mw/static"
	fhconfig "github.com/oarkflow/fh/pkg/config"
)

// NewFHApp builds LookupX's native fh application. HTTP requests stay in fh
// contexts throughout routing and handler execution; no net/http adaptation is
// performed on the server path.
func NewFHApp(handler any) *fh.App {
	maxBodyBytes := fhMaxBodyBytes(handler)
	baseConfig := fhconfig.Config{Server: fhconfig.Server{
		SecureByDefault:          true,
		MaxRequestBodySize:       maxBodyBytes,
		MaxInFlightRequests:      5000,
		MaxGoroutines:            20_000,
		MaxHeapBytes:             "4GiB",
		MaxConnectionsPerIP:      100,
		MaxBufferedRequestBytes:  64 << 20,
		MaxBufferedResponseBytes: 64 << 20,
	}}
	loadedConfig, err := fhconfig.LoadEnvFile(".env", baseConfig, "FH")
	if err != nil {
		if !os.IsNotExist(err) {
			panic("load fh environment config: " + err.Error())
		}
		loadedConfig, err = fhconfig.ApplyEnv(baseConfig, "FH")
		if err != nil {
			panic("load fh environment variables: " + err.Error())
		}
	}
	appConfig, err := loadedConfig.AppConfig()
	if err != nil {
		panic("invalid fh environment config: " + err.Error())
	}
	appConfig.StreamRequestBody = true
	appConfig.StartupBanner.Disabled = true
	app := fh.NewWithConfig(appConfig)
	app.Use(
		recover.New(recover.Config{EnableStackTrace: false}),
		requestid.New(requestid.Config{}),
		security.New(),
	)
	if h, ok := handler.(*MultiServer); ok {
		h.auth = newAdminAuth(h)
		app.Use(func(c fh.Ctx) error {
			c.Locals(adminPeerIPLocalKey, c.IP())
			return c.Next()
		})
		if len(h.auth.trustedProxyNets) > 0 {
			app.Use(realip.New(realip.Config{TrustedProxies: h.auth.trustedProxyNets}))
		}
		if h.auth.manager != nil {
			sessionMiddleware := session.New(h.auth.manager)
			app.Use(func(c fh.Ctx) error {
				if !isProtectedAPIPath(c.Path()) && !strings.HasPrefix(c.Path(), "/auth/") && !strings.HasPrefix(c.Path(), "/admin/") {
					return c.Next()
				}
				return sessionMiddleware(c)
			})
		}
		app.Use(h.auth.middleware)
	} else if auth := fhAuthMiddlewares(handler); len(auth) > 0 {
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
		app.Post("/auth/login", ratelimiter.New(ratelimiter.Config{
			Max: 10, Window: time.Minute,
			KeyFunc: func(c fh.Ctx) string { return c.IP() },
			LimitReached: func(c fh.Ctx, _ ratelimiter.Result) error {
				return fhError(c, 429, "too many login attempts; try again later")
			},
		}), h.auth.login)
		app.Post("/auth/setup", ratelimiter.New(ratelimiter.Config{
			Max: 5, Window: time.Minute,
			KeyFunc: func(c fh.Ctx) string { return c.IP() },
			LimitReached: func(c fh.Ctx, _ ratelimiter.Result) error {
				return fhError(c, 429, "too many setup attempts; try again later")
			},
		}), h.auth.setup)
		app.Get("/auth/session", h.auth.status)
		app.Post("/auth/logout", h.auth.logout)
		registerMultiServerRoutes(app, h)
		registerAdminRoutes(app, h)
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
			return []fh.HandlerFunc{gate, func(c fh.Ctx) error { return fhError(c, 401, "unauthorized") }}
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
