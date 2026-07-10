package pkg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// ---------------------------------------------------------------------------
// http — REST/HTTP API integration datasource.
// ---------------------------------------------------------------------------

// HTTPDatasource fetches records from an HTTP/JSON API endpoint. It supports
// paginated APIs, custom headers, bearer token auth, and field mapping from
// JSON response objects.
type HTTPDatasource struct {
	id         string
	url        string
	method     string
	headers    map[string]any
	auth       string
	idField    string
	bindings   []HTTPBinding
	pageParam  string
	sizeParam  int
	limitParam int
	offsetBase int
	client     *http.Client
	params     map[string]any
}

// HTTPBinding maps a JSON field from the HTTP response to an index field.
type HTTPBinding struct {
	JSONField  string
	Field      FieldID
	Kind       ValueKind
	Normalized bool
}

func init() {
	GlobalRegistry.MustRegister("http", newHTTPDatasource)
}

func newHTTPDatasource(config map[string]any, params map[string]any) (Datasource, error) {
	config = ApplyParams(config, params)
	id, err := ConfigString(config, "id")
	if err != nil {
		return nil, err
	}
	url, err := ConfigString(config, "url")
	if err != nil {
		return nil, fmt.Errorf("http %s: %w", id, err)
	}
	method := ConfigStringOr(config, "method", "GET")
	method = strings.ToUpper(method)
	idField := ConfigStringOr(config, "id_field", "id")
	auth := ConfigStringOr(config, "auth", "")
	timeoutSec := ConfigIntOr(config, "timeout_seconds", 30)

	bindings, err := parseHTTPBindings(config)
	if err != nil {
		return nil, fmt.Errorf("http %s: %w", id, err)
	}

	headers, _ := ConfigMap(config, "headers")
	pageParam := ConfigStringOr(config, "page_param", "")
	sizeParam := ConfigIntOr(config, "size_param", 0)
	limitParam := ConfigIntOr(config, "limit_param", 0)
	offsetBase := ConfigIntOr(config, "offset_base", 0)

	return &HTTPDatasource{
		id:         id,
		url:        url,
		method:     method,
		headers:    headers,
		auth:       auth,
		idField:    idField,
		bindings:   bindings,
		pageParam:  pageParam,
		sizeParam:  sizeParam,
		limitParam: limitParam,
		offsetBase: offsetBase,
		client:     &http.Client{Timeout: time.Duration(timeoutSec) * time.Second},
		params:     params,
	}, nil
}

func (d *HTTPDatasource) ID() string   { return d.id }
func (d *HTTPDatasource) Type() string { return "http" }

func (d *HTTPDatasource) Validate() error {
	if d.url == "" {
		return errors.New("http: url required")
	}
	if d.idField == "" {
		return errors.New("http: id_field required")
	}
	return nil
}

func (d *HTTPDatasource) Open(ctx context.Context) (Cursor, error) {
	return &httpCursor{
		datasource: d,
		ctx:        ctx,
		offset:     d.offsetBase,
		seq:        0,
		done:       false,
	}, nil
}

// httpCursor iterates over paginated HTTP responses, decoding JSON objects.
type httpCursor struct {
	datasource *HTTPDatasource
	ctx        context.Context
	offset     int
	page       int
	current    []map[string]any
	idx        int
	seq        uint64
	err        error
	done       bool
}

func (c *httpCursor) Next(ctx context.Context, dst *SourceRecord) bool {
	for {
		if c.idx < len(c.current) {
			obj := c.current[c.idx]
			c.idx++
			c.seq++
			dst.Reset()
			dst.Seq = c.seq
			if v, ok := obj[c.datasource.idField]; ok {
				dst.ID = fmt.Sprint(v)
			}
			for _, b := range c.datasource.bindings {
				v, ok := obj[b.JSONField]
				if !ok || v == nil {
					continue
				}
				switch b.Kind {
				case ValueKeyword:
					dst.AddKeyword(b.Field, fmt.Sprint(v), b.Normalized)
				case ValueText:
					dst.AddText(b.Field, fmt.Sprint(v), b.Normalized)
				case ValueNumber:
					switch x := v.(type) {
					case float64:
						dst.AddNumber(b.Field, x)
					case json.Number:
						if f, err := x.Float64(); err == nil {
							dst.AddNumber(b.Field, f)
						}
					case int:
						dst.AddNumber(b.Field, float64(x))
					case string:
						// Attempt parse
					}
				}
			}
			return true
		}

		if c.done {
			return false
		}

		records, err := c.fetchPage()
		if err != nil {
			c.err = err
			return false
		}
		if len(records) == 0 {
			c.done = true
			return false
		}
		c.current = records
		c.idx = 0
		c.page++
	}
}

func (c *httpCursor) fetchPage() ([]map[string]any, error) {
	url := c.datasource.url
	if c.datasource.pageParam != "" {
		sep := "?"
		if strings.Contains(url, "?") {
			sep = "&"
		}
		url += fmt.Sprintf("%s%s=%d", sep, c.datasource.pageParam, c.page)
	}
	if c.datasource.limitParam > 0 {
		sep := "?"
		if strings.Contains(url, "?") {
			sep = "&"
		}
		url += fmt.Sprintf("%s%s=%d", sep, "limit", c.datasource.limitParam)
	}

	req, err := http.NewRequestWithContext(c.ctx, c.datasource.method, url, nil)
	if err != nil {
		return nil, fmt.Errorf("http %s: build request: %w", c.datasource.id, err)
	}
	for k, v := range c.datasource.headers {
		req.Header.Set(k, fmt.Sprint(v))
	}
	if c.datasource.auth != "" {
		if !strings.HasPrefix(strings.ToLower(c.datasource.auth), "bearer ") {
			req.Header.Set("Authorization", "Bearer "+c.datasource.auth)
		} else {
			req.Header.Set("Authorization", c.datasource.auth)
		}
	}

	resp, err := c.datasource.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http %s: request: %w", c.datasource.id, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return nil, fmt.Errorf("http %s: status %d: %s", c.datasource.id, resp.StatusCode, string(body))
	}

	var raw any
	dec := json.NewDecoder(resp.Body)
	dec.UseNumber()
	if err := dec.Decode(&raw); err != nil {
		return nil, fmt.Errorf("http %s: decode: %w", c.datasource.id, err)
	}

	// Support both array responses and object-wrapped arrays.
	switch v := raw.(type) {
	case []any:
		return coerceMapList(v)
	case map[string]any:
		// Try common wrapper keys: "data", "results", "items", "records"
		for _, key := range []string{"data", "results", "items", "records"} {
			if inner, ok := v[key]; ok {
				if list, ok := inner.([]any); ok {
					return coerceMapList(list)
				}
			}
		}
		return nil, fmt.Errorf("http %s: response is not an array or wrapped array", c.datasource.id)
	default:
		return nil, fmt.Errorf("http %s: unexpected response type %T", c.datasource.id, raw)
	}
}

func (c *httpCursor) Err() error { return c.err }
func (c *httpCursor) Close() error { return nil }

func coerceMapList(list []any) ([]map[string]any, error) {
	out := make([]map[string]any, 0, len(list))
	for i, item := range list {
		m, ok := item.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("response[%d] is not an object, got %T", i, item)
		}
		out = append(out, m)
	}
	return out, nil
}

// parseHTTPBindings parses the "columns" list from BCL config into HTTPBinding values.
func parseHTTPBindings(config map[string]any) ([]HTTPBinding, error) {
	list, err := ConfigList(config, "columns")
	if err != nil {
		return nil, err
	}
	if list == nil {
		return nil, nil
	}
	out := make([]HTTPBinding, 0, len(list))
	for i, item := range list {
		m, ok := item.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("columns[%d] must be an object, got %T", i, item)
		}
		jsonField, err := ConfigString(m, "field")
		if err != nil {
			jsonField, err = ConfigString(m, "column")
			if err != nil {
				return nil, fmt.Errorf("columns[%d]: %w", i, err)
			}
		}
		kind := ConfigStringOr(m, "kind", "keyword")
		normalized, _ := ConfigBool(m, "normalized")
		out = append(out, HTTPBinding{
			JSONField:  jsonField,
			Field:      FieldID(0), // resolved later
			Kind:       parseValueKind(kind),
			Normalized: normalized,
		})
	}
	return out, nil
}
