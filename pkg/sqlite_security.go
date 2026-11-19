package pkg

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

// secureSQLiteDSN confines SQLite files to a private subdirectory of the
// configured data root. URI query options are rejected so callers cannot
// silently weaken file access or inject driver-specific pragmas.
func secureSQLiteDSN(driver, dsn, dataDir string) (string, error) {
	if !isSQLiteDriver(driver) {
		return dsn, nil
	}
	dsn = strings.TrimSpace(dsn)
	if dsn == ":memory:" {
		return dsn, nil
	}
	if dsn == "" || strings.ContainsAny(dsn, "?#") {
		return "", errors.New("SQLite DSN must be :memory: or a database path without URI options")
	}
	if strings.HasPrefix(strings.ToLower(dsn), "file:") {
		u, err := url.Parse(dsn)
		if err != nil || u.Scheme != "file" || u.Host != "" || u.RawQuery != "" || u.Fragment != "" {
			return "", errors.New("SQLite file URI options and remote hosts are not allowed")
		}
		dsn = u.Path
		if dsn == "" {
			return "", errors.New("SQLite file path is required")
		}
	}
	if dataDir == "" {
		dataDir = filepath.Join("data", "indexes")
	}
	root, err := filepath.Abs(filepath.Join(dataDir, "sqlite"))
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(root, 0700); err != nil {
		return "", fmt.Errorf("create SQLite directory: %w", err)
	}
	if err := os.Chmod(root, 0700); err != nil {
		return "", err
	}
	rootReal, err := filepath.EvalSymlinks(root)
	if err != nil {
		return "", err
	}
	path := dsn
	if !filepath.IsAbs(path) {
		path = filepath.Join(rootReal, path)
	}
	path, err = filepath.Abs(filepath.Clean(path))
	if err != nil {
		return "", err
	}
	rel, err := filepath.Rel(rootReal, path)
	if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", errors.New("SQLite database must be inside the configured data directory")
	}
	parent := filepath.Dir(path)
	if err := os.MkdirAll(parent, 0700); err != nil {
		return "", err
	}
	parentReal, err := filepath.EvalSymlinks(parent)
	if err != nil {
		return "", err
	}
	rel, err = filepath.Rel(rootReal, parentReal)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", errors.New("SQLite database directory escapes the configured data directory")
	}
	if info, err := os.Lstat(path); err == nil {
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			return "", errors.New("SQLite database must be a regular file, not a symlink")
		}
		if err := os.Chmod(path, 0600); err != nil {
			return "", err
		}
	} else if errors.Is(err, os.ErrNotExist) {
		f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
		if err != nil {
			return "", err
		}
		if err := f.Close(); err != nil {
			return "", err
		}
	} else {
		return "", err
	}
	return path, nil
}

func isSQLiteDriver(driver string) bool {
	driver = strings.ToLower(strings.TrimSpace(driver))
	return driver == "sqlite" || driver == "sqlite3"
}
