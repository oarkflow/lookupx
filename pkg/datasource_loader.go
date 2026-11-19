package pkg

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// ---------------------------------------------------------------------------
// BCL Configuration Loader
// ---------------------------------------------------------------------------
//
// Loads a BCL configuration file and produces a configured MultiIndexManager
// with all datasources wired up.
//
// Usage:
//
//   cfg, err := LoadBCLConfig("lookupx.bcl", nil)
//   if err != nil { ... }
//
//   mgr, err := BuildFromBCLConfig(context.Background(), cfg, nil)
//   if err != nil { ... }

// LoadBCLConfig parses a BCL file into a LookupXConfig. If registry is nil,
// the GlobalRegistry is used.
func LoadBCLConfig(path string, registry *DatasourceRegistry) (*LookupXConfig, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	cfg, err := ParseBCLBytes(b, registry)
	if err != nil {
		return nil, err
	}
	cfg.BaseDir, err = filepath.Abs(filepath.Dir(path))
	if err != nil {
		return nil, fmt.Errorf("resolve config directory: %w", err)
	}
	return cfg, nil
}

// ParseBCLBytes parses BCL bytes into a LookupXConfig using bcl.Unmarshal.
func ParseBCLBytes(src []byte, registry *DatasourceRegistry) (*LookupXConfig, error) {
	if registry == nil {
		registry = GlobalRegistry
	}
	var cfg LookupXConfig
	if err := unmarshalBCL(src, &cfg); err != nil {
		return nil, fmt.Errorf("bcl unmarshal: %w", err)
	}
	if err := applyBCLQueryObjects(src, &cfg); err != nil {
		return nil, err
	}
	cfg.BaseDir, _ = os.Getwd()
	// Validate that all referenced datasource types are registered.
	for _, ds := range cfg.Datasources {
		if !registry.Has(ds.Kind) {
			return nil, fmt.Errorf("datasource %q: unknown type %q (available: %v)", ds.ID, ds.Kind, registry.Types())
		}
	}
	return &cfg, nil
}

func resolveConfigSQLFile(baseDir, configuredPath string) (string, error) {
	configuredPath = strings.TrimSpace(configuredPath)
	if configuredPath == "" || filepath.IsAbs(configuredPath) || strings.Contains(configuredPath, "\\") || strings.ToLower(filepath.Ext(configuredPath)) != ".sql" {
		return "", fmt.Errorf("file must be a relative .sql path under the BCL directory")
	}
	rel := filepath.Clean(filepath.FromSlash(configuredPath))
	if rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("file path escapes the BCL directory")
	}
	root, err := filepath.Abs(baseDir)
	if err != nil {
		return "", err
	}
	full := filepath.Join(root, rel)
	if inside, err := filepath.Rel(root, full); err != nil || inside == ".." || strings.HasPrefix(inside, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("file path escapes the BCL directory")
	}
	current := root
	parts := strings.Split(rel, string(filepath.Separator))
	for i, part := range parts {
		current = filepath.Join(current, part)
		info, err := os.Lstat(current)
		if err != nil {
			return "", fmt.Errorf("open %q: %w", configuredPath, err)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return "", fmt.Errorf("symlinks are not allowed in SQL file paths")
		}
		if i < len(parts)-1 && !info.IsDir() {
			return "", fmt.Errorf("SQL path parent is not a directory")
		}
		if i == len(parts)-1 && !info.Mode().IsRegular() {
			return "", fmt.Errorf("SQL query file must be a regular file")
		}
		if i == len(parts)-1 && info.Size() > 1<<20 {
			return "", fmt.Errorf("SQL query file exceeds the 1 MiB limit")
		}
	}
	return full, nil
}

// BuildFromBCLConfig creates a MultiIndexManager from a parsed BCL config.
// Datasources are resolved from the registry and attached as SourceFactories.
func BuildFromBCLConfig(ctx context.Context, cfg *LookupXConfig, registry *DatasourceRegistry) (*MultiIndexManager, error) {
	if registry == nil {
		registry = GlobalRegistry
	}

	// Build a lookup map for datasources.
	dsMap := make(map[string]*DatasourceDef, len(cfg.Datasources))
	for i := range cfg.Datasources {
		dsMap[cfg.Datasources[i].ID] = &cfg.Datasources[i]
	}

	mgr := NewMultiIndexManager()
	for _, idx := range cfg.Indexes {
		ixCfg := idx.ToConfig()
		mi, err := mgr.Register(IndexDefinition{
			ID:          idx.ID,
			Config:      ixCfg,
			BulkOptions: idx.Bulk.ToBulkOptions(),
		})
		if err != nil {
			return nil, fmt.Errorf("register index %q: %w", idx.ID, err)
		}

		if idx.DatasourceID != "" {
			dsDef, ok := dsMap[idx.DatasourceID]
			if !ok {
				return nil, fmt.Errorf("index %q references unknown datasource %q", idx.ID, idx.DatasourceID)
			}
			dsConfig := dsDef.ToMap()
			if queryFile, ok := dsConfig["query_file"].(string); ok && strings.TrimSpace(queryFile) != "" {
				baseDir := cfg.BaseDir
				if baseDir == "" {
					baseDir, _ = os.Getwd()
				}
				resolved, err := resolveConfigSQLFile(baseDir, queryFile)
				if err != nil {
					return nil, fmt.Errorf("index %q datasource %q query file: %w", idx.ID, dsDef.ID, err)
				}
				dsConfig["query_file"] = resolved
			}
			if dsn, ok := dsConfig["dsn"].(string); ok {
				secureDSN, err := secureSQLiteDSN(dsDef.Driver, dsn, cfg.DataDir)
				if err != nil {
					return nil, fmt.Errorf("index %q secure datasource DSN: %w", idx.ID, err)
				}
				dsConfig["dsn"] = secureDSN
			}
			ds, err := registry.Create(dsDef.Kind, dsConfig, dsDef.Params)
			if err != nil {
				return nil, fmt.Errorf("index %q create datasource: %w", idx.ID, err)
			}
			mi.Source = func(d Datasource) SourceFactory {
				return func(ix *Index) (Source, error) {
					// Resolve field IDs using the index's compiled schema.
					resolveDSFieldIDs(d, ix)
					return d, nil
				}
			}(ds)
		}
	}
	return mgr, nil
}

// resolveDSFieldIDs updates the Field values in SQLColumn/CSVBinding/JSONBinding
// slices using the index's field resolver. This is called after the index schema
// is compiled so FieldID values are available.
func resolveDSFieldIDs(ds Datasource, ix *Index) {
	resolver := func(name string) FieldID {
		return ix.FieldID(name)
	}
	switch d := ds.(type) {
	case *SQLTableDatasource:
		d.src.ColumnBindings = ResolveSQLColumns(d.src.ColumnBindings, resolver)
	case *SQLViewDatasource:
		d.src.Columns = ResolveSQLColumns(d.src.Columns, resolver)
	case *SQLQueryDatasource:
		d.src.Columns = ResolveSQLColumns(d.src.Columns, resolver)
	case *SQLFileDatasource:
		d.columns = ResolveSQLColumns(d.columns, resolver)
	}
}

// unmarshalBCL calls bcl.Unmarshal. This is the single import point for the
// BCL library.
func unmarshalBCL(src []byte, v any) error {
	return unmarshalBCLRaw(src, v)
}

// ReloadBCL re-reads a BCL config file and performs an atomic swap of all
// index datasources in the manager. Indexes not present in the new config
// are left untouched.
func ReloadBCL(ctx context.Context, mgr *MultiIndexManager, path string, registry *DatasourceRegistry) error {
	cfg, err := LoadBCLConfig(path, registry)
	if err != nil {
		return err
	}
	_, err = BuildFromBCLConfig(ctx, cfg, registry)
	return err
}

// ---------------------------------------------------------------------------
// Direct programmatic config builder (no BCL dependency needed).
// ---------------------------------------------------------------------------

// ConfigBuilder provides a fluent API for building LookupXConfig without BCL.
type ConfigBuilder struct {
	cfg *LookupXConfig
}

// NewConfigBuilder creates a new ConfigBuilder.
func NewConfigBuilder() *ConfigBuilder {
	return &ConfigBuilder{cfg: &LookupXConfig{}}
}

// Server sets server-level configuration.
func (b *ConfigBuilder) Server(addr, dataDir string, apiKeys []string) *ConfigBuilder {
	b.cfg.Addr = addr
	b.cfg.DataDir = dataDir
	b.cfg.APIKeys = apiKeys
	return b
}

// AddDatasource adds a datasource configuration.
func (b *ConfigBuilder) AddDatasource(id string, dsKind string, config map[string]any) *ConfigBuilder {
	ds := DatasourceDef{ID: id, Kind: dsKind}
	if v, ok := config["file"].(string); ok {
		ds.File = v
	}
	if v, ok := config["url"].(string); ok {
		ds.URL = v
	}
	if v, ok := config["driver"].(string); ok {
		ds.Driver = v
	}
	if v, ok := config["dsn"].(string); ok {
		ds.DSN = v
	}
	if v, ok := config["table"].(string); ok {
		ds.Table = v
	}
	if v, ok := config["view"].(string); ok {
		ds.View = v
	}
	if v, ok := config["query"].(string); ok {
		ds.Query = v
	}
	if v, ok := config["id_column"].(string); ok {
		ds.IDColumn = v
	}
	if v, ok := config["id_field"].(string); ok {
		ds.IDField = v
	}
	b.cfg.Datasources = append(b.cfg.Datasources, ds)
	return b
}

// AddIndex adds an index configuration.
func (b *ConfigBuilder) AddIndex(id, schema, datasourceID string, opts ...IndexOption) *ConfigBuilder {
	ic := IndexDef{
		ID:           id,
		Schema:       schema,
		DatasourceID: datasourceID,
	}
	for _, o := range opts {
		o(&ic)
	}
	b.cfg.Indexes = append(b.cfg.Indexes, ic)
	return b
}

// Build returns the final LookupXConfig.
func (b *ConfigBuilder) Build() *LookupXConfig {
	return b.cfg
}

// IndexOption configures an IndexDef.
type IndexOption func(*IndexDef)

// WithInitialCapacity sets the initial capacity.
func WithInitialCapacity(n int) IndexOption {
	return func(ic *IndexDef) { ic.InitialCapacity = n }
}

// WithBulkOptions sets bulk ingestion options.
func WithBulkOptions(opt BulkOptions) IndexOption {
	return func(ic *IndexDef) {
		ic.Bulk = BulkDef{
			BatchSize:       opt.BatchSize,
			CheckpointEvery: opt.CheckpointEvery,
			Resume:          opt.Resume,
			SkipBadRecords:  opt.SkipBadRecords,
		}
		if fc, ok := opt.Checkpoint.(FileCheckpoint); ok {
			ic.Bulk.CheckpointPath = fc.Path
		}
	}
}

// WithPersistent enables persistent storage.
func WithPersistent() IndexOption {
	return func(ic *IndexDef) { ic.Persistent = true }
}
