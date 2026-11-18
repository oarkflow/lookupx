package pkg

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFHNativeMultiIndexRoutes(t *testing.T) {
	mgr := NewMultiIndexManager()
	if err := RegisterDemoIndexes(mgr, 0); err != nil {
		t.Fatal(err)
	}
	app := NewFHApp(&MultiServer{Manager: mgr})
	resp, err := app.Test(fhTestRequest(t, http.MethodGet, "/v1/indexes", ""))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), "dataset_a") {
		t.Fatalf("native fh indexes route status=%d body=%s", resp.StatusCode, body)
	}

	resp, err = NewFHApp(&MultiServer{Manager: mgr}).Test(fhTestRequest(t, http.MethodGet, "/health", ""))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err = io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), `"ok":true`) {
		t.Fatalf("native fh health route status=%d body=%s", resp.StatusCode, body)
	}
}

// serveFHTest preserves the old recorder-oriented assertions while executing
// requests through the native fh app and its router/middleware stack.
func serveFHTest(t *testing.T, recorder *httptest.ResponseRecorder, handler any, req *http.Request) {
	t.Helper()
	resp, err := NewFHApp(handler).Test(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	for name, values := range resp.Header {
		for _, value := range values {
			recorder.Header().Add(name, value)
		}
	}
	recorder.Code = resp.StatusCode
	if _, err := io.Copy(recorder.Body, resp.Body); err != nil {
		t.Fatal(err)
	}
}

func TestFHNativeSingleIndexJSONHandlers(t *testing.T) {
	ix, err := New(Config{Schema: Schema{Fields: map[string]FieldOptions{
		"name": {Kind: FieldKeyword, Indexed: true, Stored: true, Lowercase: true},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	server := &Server{Index: ix}
	req := fhTestRequest(t, http.MethodPut, "/docs/doc-1", `{"name":"Harbor Health"}`)
	resp, err := NewFHApp(server).Test(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("native fh upsert status=%d", resp.StatusCode)
	}

	req = fhTestRequest(t, http.MethodPost, "/search", `{"type":"term","field":"name","value":"Harbor Health","with_docs":true}`)
	resp, err = NewFHApp(server).Test(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK || !bytes.Contains(body, []byte("doc-1")) {
		t.Fatalf("native fh search status=%d body=%s", resp.StatusCode, body)
	}
}

func TestFHStaticFSAndRequestBodyLimit(t *testing.T) {
	webDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(webDir, "index.html"), []byte("custom console"), 0600); err != nil {
		t.Fatal(err)
	}
	app := NewFHApp(&MultiServer{Manager: NewMultiIndexManager(), WebDir: webDir})
	resp, err := app.Test(fhTestRequest(t, http.MethodGet, "/", ""))
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK || !bytes.Contains(body, []byte("custom console")) || resp.Header.Get("Cache-Control") != "no-cache" {
		t.Fatalf("custom static response status=%d cache=%q body=%q", resp.StatusCode, resp.Header.Get("Cache-Control"), body)
	}

	ix, err := New(Config{MaxRequestBytes: 1024, Schema: Schema{Fields: map[string]FieldOptions{"name": {Kind: FieldKeyword, Indexed: true}}}})
	if err != nil {
		t.Fatal(err)
	}
	large := `{"name":"` + strings.Repeat("x", 2048) + `"}`
	resp, err = NewFHApp(&Server{Index: ix}).Test(fhTestRequest(t, http.MethodPut, "/docs/large", large))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized fh request status=%d, want %d", resp.StatusCode, http.StatusRequestEntityTooLarge)
	}
}

func fhTestRequest(t *testing.T, method, path, body string) *http.Request {
	t.Helper()
	req, err := http.NewRequest(method, "http://lookupx.test"+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	return req
}
