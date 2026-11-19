package pkg

import (
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/oarkflow/fh"
)

type adminSQLFileEntry struct {
	Name  string `json:"name"`
	Path  string `json:"path"`
	IsDir bool   `json:"is_dir"`
}

type adminSQLFileRequest struct {
	Path    string `json:"path"`
	Content string `json:"content,omitempty"`
}

// adminSQLPath confines SQL-file operations to the directory containing the
// active BCL file and rejects symlink traversal at every path component.
func (s *MultiServer) adminSQLPath(rel string, allowMissing bool) (string, error) {
	if strings.TrimSpace(s.ConfigPath) == "" {
		return "", os.ErrNotExist
	}
	root, err := filepath.Abs(filepath.Dir(s.ConfigPath))
	if err != nil {
		return "", err
	}
	rel = strings.TrimSpace(rel)
	if rel == "" || filepath.IsAbs(rel) || strings.Contains(rel, "\\") {
		return "", os.ErrPermission
	}
	clean := filepath.Clean(filepath.FromSlash(rel))
	if clean == "." || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", os.ErrPermission
	}
	if strings.ToLower(filepath.Ext(clean)) != ".sql" {
		return "", os.ErrPermission
	}
	full := filepath.Join(root, clean)
	if relative, err := filepath.Rel(root, full); err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return "", os.ErrPermission
	}
	current := root
	parts := strings.Split(clean, string(filepath.Separator))
	for i, part := range parts {
		current = filepath.Join(current, part)
		info, err := os.Lstat(current)
		if os.IsNotExist(err) && allowMissing {
			continue
		}
		if err != nil {
			return "", err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return "", os.ErrPermission
		}
		if i < len(parts)-1 && !info.IsDir() {
			return "", os.ErrNotExist
		}
	}
	return full, nil
}

func (s *MultiServer) listOrReadAdminSQLFile(c fh.Ctx) error {
	if err := s.auth.requireAdmin(c, false); err != nil {
		return err
	}
	rel := c.Query("path")
	if strings.TrimSpace(s.ConfigPath) == "" {
		return fhError(c, 404, "BCL configuration editing is disabled")
	}
	if strings.TrimSpace(rel) != "" {
		path, err := s.adminSQLPath(rel, false)
		if err != nil {
			return fhError(c, 400, "SQL file path is invalid or outside the BCL directory")
		}
		info, err := os.Stat(path)
		if err != nil || !info.Mode().IsRegular() {
			return fhError(c, 404, "SQL file not found")
		}
		if info.Size() > 1<<20 {
			return fhError(c, 413, "SQL file exceeds the 1 MiB editor limit")
		}
		content, err := os.ReadFile(path)
		if err != nil {
			return fhError(c, 500, "could not read SQL file")
		}
		c.Set("Cache-Control", "no-store")
		return c.JSON(map[string]any{"path": filepath.ToSlash(rel), "content": string(content)})
	}
	dirRel := strings.TrimSpace(c.Query("dir"))
	var dir string
	var err error
	if dirRel == "" {
		dir, err = filepath.Abs(filepath.Dir(s.ConfigPath))
	} else {
		dir, err = s.adminSQLPath(filepath.ToSlash(filepath.Join(dirRel, "placeholder.sql")), true)
		if err == nil {
			dir = filepath.Dir(dir)
		}
	}
	if err != nil {
		return fhError(c, 400, "SQL directory path is invalid")
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fhError(c, 500, "could not list SQL directory")
	}
	out := make([]adminSQLFileEntry, 0, len(entries))
	for _, entry := range entries {
		if entry.Type()&os.ModeSymlink != 0 {
			continue
		}
		if !entry.IsDir() && strings.ToLower(filepath.Ext(entry.Name())) != ".sql" {
			continue
		}
		path := entry.Name()
		if dirRel != "" {
			path = filepath.Join(dirRel, path)
		}
		out = append(out, adminSQLFileEntry{Name: entry.Name(), Path: filepath.ToSlash(path), IsDir: entry.IsDir()})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].IsDir != out[j].IsDir {
			return out[i].IsDir
		}
		return strings.ToLower(out[i].Name) < strings.ToLower(out[j].Name)
	})
	c.Set("Cache-Control", "no-store")
	return c.JSON(map[string]any{"dir": dirRel, "entries": out})
}

func (s *MultiServer) saveAdminSQLFile(c fh.Ctx) error {
	if err := s.auth.requireAdmin(c, true); err != nil {
		return err
	}
	var req adminSQLFileRequest
	if err := c.BodyParser(&req); err != nil || len(req.Content) > 1<<20 {
		return fhError(c, 400, "invalid SQL file request or content exceeds 1 MiB")
	}
	if err := validateSelectSQL(req.Content); err != nil {
		return fhError(c, 400, err.Error())
	}
	path, err := s.adminSQLPath(req.Path, true)
	if err != nil {
		return fhError(c, 400, "SQL file path is invalid or outside the BCL directory")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return fhError(c, 500, "could not create SQL file directory")
	}
	path, err = s.adminSQLPath(req.Path, true)
	if err != nil {
		return fhError(c, 400, "SQL file path is invalid or outside the BCL directory")
	}
	if err := writePrivateAtomic(path, []byte(req.Content)); err != nil {
		return fhError(c, 500, "could not save SQL file")
	}
	return c.JSON(map[string]any{"saved": true, "path": filepath.ToSlash(req.Path)})
}

func (s *MultiServer) createAdminSQLFolder(c fh.Ctx) error {
	if err := s.auth.requireAdmin(c, true); err != nil {
		return err
	}
	var req adminSQLFileRequest
	if err := c.BodyParser(&req); err != nil || strings.TrimSpace(req.Path) == "" {
		return fhError(c, 400, "folder path is required")
	}
	path, err := s.adminSQLPath(filepath.ToSlash(filepath.Join(req.Path, "placeholder.sql")), true)
	if err != nil {
		return fhError(c, 400, "folder path is invalid or outside the BCL directory")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return fhError(c, 500, "could not create SQL folder")
	}
	return c.JSON(map[string]any{"created": true, "path": filepath.ToSlash(req.Path)})
}
