package pkg

import (
	"errors"
	"fmt"
	"sort"
	"sync"
)

// Datasource extends Source with metadata and validation. Each datasource has
// a unique ID, a type identifier for registry lookup, and supports parameterized
// configuration for dynamic behavior (e.g., time-range filters, tenant scoping).
type Datasource interface {
	Source
	ID() string
	Type() string
	Validate() error
}

// DatasourceFactory constructs a Datasource from parsed configuration. The config
// map contains the BCL block body values and params provides runtime parameters
// that can override configuration values (e.g., batch_size, checkpoint_every).
type DatasourceFactory func(config map[string]any, params map[string]any) (Datasource, error)

// DatasourceRegistry holds registered datasource factories keyed by type name.
// It is safe for concurrent use after initial registration.
type DatasourceRegistry struct {
	mu        sync.RWMutex
	factories map[string]DatasourceFactory
}

// GlobalRegistry is the default datasource registry. Call RegisterDatasource to
// add new datasource types before loading configuration.
var GlobalRegistry = &DatasourceRegistry{factories: make(map[string]DatasourceFactory)}

// RegisterDatasource registers a datasource factory under the given type name.
// Type names are case-insensitive. Returns an error if the type is already registered.
func RegisterDatasource(typeName string, factory DatasourceFactory) error {
	return GlobalRegistry.Register(typeName, factory)
}

// Register adds a datasource factory to this registry.
func (r *DatasourceRegistry) Register(typeName string, factory DatasourceFactory) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := normalizeDSType(typeName)
	if _, exists := r.factories[key]; exists {
		return fmt.Errorf("datasource type %q already registered", typeName)
	}
	r.factories[key] = factory
	return nil
}

// MustRegister is like Register but panics on error.
func (r *DatasourceRegistry) MustRegister(typeName string, factory DatasourceFactory) {
	if err := r.Register(typeName, factory); err != nil {
		panic(err)
	}
}

// Get returns the factory for the given type name.
func (r *DatasourceRegistry) Get(typeName string) (DatasourceFactory, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	f, ok := r.factories[normalizeDSType(typeName)]
	return f, ok
}

// Has returns true if a factory is registered for the given type.
func (r *DatasourceRegistry) Has(typeName string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.factories[normalizeDSType(typeName)]
	return ok
}

// Types returns a sorted list of registered type names.
func (r *DatasourceRegistry) Types() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.factories))
	for k := range r.factories {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// Create constructs a Datasource using the factory registered for the given type.
func (r *DatasourceRegistry) Create(typeName string, config map[string]any, params map[string]any) (Datasource, error) {
	f, ok := r.Get(typeName)
	if !ok {
		available := r.Types()
		return nil, fmt.Errorf("unknown datasource type %q (available: %v)", typeName, available)
	}
	return f(config, params)
}

func normalizeDSType(s string) string {
	out := make([]byte, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		out[i] = c
	}
	return string(out)
}

// ---------------------------------------------------------------------------
// Config helpers for extracting typed values from BCL-parsed maps.
// ---------------------------------------------------------------------------

// ConfigString extracts a string value from a config map.
func ConfigString(m map[string]any, key string) (string, error) {
	v, ok := m[key]
	if !ok || v == nil {
		return "", fmt.Errorf("missing required config key %q", key)
	}
	s, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("config key %q must be a string, got %T", key, v)
	}
	return s, nil
}

// ConfigStringOr returns a string value or the default.
func ConfigStringOr(m map[string]any, key, def string) string {
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return def
}

// ConfigInt extracts an int value from a config map.
func ConfigInt(m map[string]any, key string) (int, error) {
	v, ok := m[key]
	if !ok || v == nil {
		return 0, fmt.Errorf("missing required config key %q", key)
	}
	switch x := v.(type) {
	case int:
		return x, nil
	case int64:
		return int(x), nil
	case float64:
		return int(x), nil
	default:
		return 0, fmt.Errorf("config key %q must be numeric, got %T", key, v)
	}
}

// ConfigIntOr returns an int value or the default.
func ConfigIntOr(m map[string]any, key string, def int) int {
	if v, ok := m[key]; ok {
		switch x := v.(type) {
		case int:
			return x
		case int64:
			return int(x)
		case float64:
			return int(x)
		}
	}
	return def
}

// ConfigBool extracts a bool value from a config map.
func ConfigBool(m map[string]any, key string) (bool, error) {
	v, ok := m[key]
	if !ok || v == nil {
		return false, nil
	}
	b, ok := v.(bool)
	if !ok {
		return false, fmt.Errorf("config key %q must be bool, got %T", key, v)
	}
	return b, nil
}

// ConfigBoolOr returns a bool value or the default.
func ConfigBoolOr(m map[string]any, key string, def bool) bool {
	if v, ok := m[key]; ok {
		if b, ok := v.(bool); ok {
			return b
		}
	}
	return def
}

// ConfigMap extracts a nested map from a config map.
func ConfigMap(m map[string]any, key string) (map[string]any, error) {
	v, ok := m[key]
	if !ok || v == nil {
		return nil, nil
	}
	sub, ok := v.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("config key %q must be an object, got %T", key, v)
	}
	return sub, nil
}

// ConfigMapOr returns a nested map or the default.
func ConfigMapOr(m map[string]any, key string, def map[string]any) map[string]any {
	if sub, err := ConfigMap(m, key); err == nil && sub != nil {
		return sub
	}
	return def
}

// ConfigList extracts a list value from a config map.
func ConfigList(m map[string]any, key string) ([]any, error) {
	v, ok := m[key]
	if !ok || v == nil {
		return nil, nil
	}
	list, ok := v.([]any)
	if !ok {
		return nil, fmt.Errorf("config key %q must be a list, got %T", key, v)
	}
	return list, nil
}

// ConfigStringList extracts a list of strings from a config map.
func ConfigStringList(m map[string]any, key string) ([]string, error) {
	list, err := ConfigList(m, key)
	if err != nil {
		return nil, err
	}
	if list == nil {
		return nil, nil
	}
	out := make([]string, 0, len(list))
	for i, item := range list {
		s, ok := item.(string)
		if !ok {
			return nil, fmt.Errorf("config key %q[%d] must be a string, got %T", key, i, item)
		}
		out = append(out, s)
	}
	return out, nil
}

// ApplyParams overlays parameter values on top of config values. Params take
// precedence when present. This allows runtime overrides from CLI flags or
// environment variables.
func ApplyParams(config, params map[string]any) map[string]any {
	if len(params) == 0 {
		return config
	}
	out := make(map[string]any, len(config)+len(params))
	for k, v := range config {
		out[k] = v
	}
	for k, v := range params {
		out[k] = v
	}
	return out
}

// ResolveDSID resolves a datasource ID, validating it is non-empty.
func ResolveDSID(config map[string]any) (string, error) {
	return ConfigString(config, "id")
}

// ErrUnknownDSType is returned when a datasource type is not found in the registry.
var ErrUnknownDSType = errors.New("unknown datasource type")
