package pkg

import (
	"context"
	"os"
	"path/filepath"
	"strings"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/sse"
)

type bclContentRequest struct {
	Content string `json:"content"`
}

func registerAdminRoutes(app *fh.App, server *MultiServer) {
	app.Get("/admin/api/logs/stream", server.streamLogsFH)
	app.Get("/admin/api/config", server.getBCLConfigFH)
	app.Post("/admin/api/config/validate", server.validateBCLConfigFH)
	app.Put("/admin/api/config", server.saveBCLConfigFH)
	app.Get("/admin/api/indexes", server.listAdminIndexes)
	app.Get("/admin/api/indexes/:id/editor", server.getAdminIndexEditor)
	app.Put("/admin/api/indexes/:id/editor", server.updateAdminIndexEditor)
	app.Get("/admin/api/sql-files", server.listOrReadAdminSQLFile)
	app.Put("/admin/api/sql-files", server.saveAdminSQLFile)
	app.Post("/admin/api/sql-folders", server.createAdminSQLFolder)
	app.Get("/admin/api/users", server.auth.listAPIUsers)
	app.Post("/admin/api/users", server.auth.createAPIUser)
	app.Delete("/admin/api/users/:id", server.auth.disableAPIUser)
	app.Get("/admin/api/keys", server.auth.listAPIKeys)
	app.Post("/admin/api/keys", server.auth.createAPIKey)
	app.Post("/admin/api/keys/:id/rotate", server.auth.rotateAPIKey)
	app.Delete("/admin/api/keys/:id", server.auth.revokeAPIKey)
}

func (s *MultiServer) streamLogsFH(c fh.Ctx) error {
	if err := s.auth.requireAdmin(c, false); err != nil {
		return err
	}
	return sse.Handler(EnableLogStreaming(), sse.HandlerOptions{})(c)
}

func (s *MultiServer) getBCLConfigFH(c fh.Ctx) error {
	if err := s.auth.requireAdmin(c, false); err != nil {
		return err
	}
	if strings.TrimSpace(s.ConfigPath) == "" {
		return fhError(c, 404, "BCL configuration editing is disabled")
	}
	s.configMu.Lock()
	content, err := os.ReadFile(s.ConfigPath)
	s.configMu.Unlock()
	if err != nil {
		return fhError(c, 500, "could not read BCL configuration")
	}
	c.Set("Cache-Control", "no-store")
	return c.JSON(map[string]any{"path": s.ConfigPath, "content": string(content)})
}

func (s *MultiServer) validateBCLConfigFH(c fh.Ctx) error {
	if err := s.auth.requireAdmin(c, true); err != nil {
		return err
	}
	if strings.TrimSpace(s.ConfigPath) == "" {
		return fhError(c, 404, "BCL configuration editing is disabled")
	}
	var req bclContentRequest
	if err := c.BodyParser(&req); err != nil {
		return fhError(c, 400, "invalid BCL request")
	}
	if err := validateBCLContentAt(req.Content, filepath.Dir(s.ConfigPath)); err != nil {
		return fhError(c, 400, err.Error())
	}
	return c.JSON(map[string]any{"valid": true})
}

func (s *MultiServer) saveBCLConfigFH(c fh.Ctx) error {
	if err := s.auth.requireAdmin(c, true); err != nil {
		return err
	}
	if strings.TrimSpace(s.ConfigPath) == "" {
		return fhError(c, 404, "BCL configuration editing is disabled")
	}
	var req bclContentRequest
	if err := c.BodyParser(&req); err != nil {
		return fhError(c, 400, "invalid BCL request")
	}
	s.configMu.Lock()
	defer s.configMu.Unlock()
	if err := validateBCLContentAt(req.Content, filepath.Dir(s.ConfigPath)); err != nil {
		return fhError(c, 400, err.Error())
	}
	if err := writePrivateAtomic(s.ConfigPath, []byte(req.Content)); err != nil {
		return fhError(c, 500, "could not save BCL configuration")
	}
	return c.JSON(map[string]any{"saved": true, "restart_required": true})
}

func validateBCLContent(content string) error {
	return validateBCLContentAt(content, "")
}

func validateBCLContentAt(content, baseDir string) error {
	if strings.TrimSpace(content) == "" || len(content) > 4<<20 {
		return os.ErrInvalid
	}
	cfg, err := ParseBCLBytes([]byte(content), nil)
	if err != nil {
		return err
	}
	if baseDir != "" {
		cfg.BaseDir, err = filepath.Abs(baseDir)
		if err != nil {
			return err
		}
	}
	mgr, err := BuildFromBCLConfig(context.Background(), cfg, nil)
	if err != nil {
		return err
	}
	for _, item := range mgr.List() {
		mgr.Remove(item.ID)
	}
	return nil
}

func writePrivateAtomic(path string, data []byte) error {
	path, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".lookupx-config-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := tmp.Chmod(0600); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		return err
	}
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}
