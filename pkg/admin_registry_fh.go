package pkg

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/apikey"
	"github.com/oarkflow/fh/mw/session"
	"github.com/oarkflow/fh/pkg/storage/kv"
)

const (
	apiUserRegistryKey = "lookupx_api_users"
	readScope          = "lookup:read"
	writeScope         = "lookup:write"
)

type APIUser struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Enabled   bool      `json:"enabled"`
	Scopes    []string  `json:"scopes"`
	CreatedAt time.Time `json:"created_at"`
}

type createAPIUserRequest struct {
	ID     string   `json:"id"`
	Name   string   `json:"name"`
	Scopes []string `json:"scopes"`
}

type createAPIKeyRequest struct {
	UserID     string   `json:"user_id"`
	Name       string   `json:"name"`
	ExpiresAt  string   `json:"expires_at"`
	AllowedIPs []string `json:"allowed_ips"`
}

var safeRegistryID = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}$`)

func (a *adminAuth) initRegistry(server *MultiServer) error {
	root := server.DataDir
	if strings.TrimSpace(root) == "" {
		root = filepath.Join("data", "indexes")
	}
	// Keep credentials and API key metadata inside the configured data root.
	// Removing LOOKUPX_DATA_DIR then fully resets the local auth state too.
	store, err := kv.NewFileStore(filepath.Join(root, "auth"))
	if err != nil {
		return fmt.Errorf("open private auth store: %w", err)
	}
	a.registry = store
	a.keys = make(map[string]apikey.KeyRecord)
	a.users = make(map[string]APIUser)

	if raw, ok, err := store.Get("registry"); err != nil {
		return err
	} else if ok {
		var records []apikey.KeyRecord
		if err := json.Unmarshal(raw, &records); err != nil {
			return fmt.Errorf("read API key registry: %w", err)
		}
		for _, rec := range records {
			a.keys[rec.ID] = rec
		}
	}
	// Import configured bootstrap keys additively. This lets an operator add a
	// key in .env after first boot; durable registry entries remain manageable
	// through the admin page and are never silently removed on restart.
	keysChanged := false
	adminUser := server.AdminUser
	if adminUser == "" {
		adminUser = "admin"
	}
	for _, key := range server.APIKeys {
		if key == "" {
			continue
		}
		hash := apikey.HashKey(key)
		id := "bootstrap_" + hash[:16]
		if _, exists := a.keys[id]; exists {
			continue
		}
		a.keys[id] = apikey.KeyRecord{ID: id, Name: "Configured bootstrap key", Hash: hash, SubjectID: adminUser, Scopes: []string{readScope, writeScope}}
		keysChanged = true
	}
	if keysChanged || a.keys == nil {
		if err := a.saveKeysLocked(); err != nil {
			return err
		}
	}
	if raw, ok, err := store.Get(apiUserRegistryKey); err != nil {
		return err
	} else if ok {
		var users []APIUser
		if err := json.Unmarshal(raw, &users); err != nil {
			return fmt.Errorf("read API user registry: %w", err)
		}
		for _, user := range users {
			a.users[user.ID] = user
		}
	} else {
		id := server.AdminUser
		if id == "" {
			id = "admin"
		}
		a.users[id] = APIUser{ID: id, Name: "Admin", Enabled: true, Scopes: []string{readScope, writeScope}, CreatedAt: time.Now().UTC()}
		if err := a.saveUsersLocked(); err != nil {
			return err
		}
	}
	return nil
}

func (a *adminAuth) saveKeysLocked() error {
	records := make([]apikey.KeyRecord, 0, len(a.keys))
	for _, rec := range a.keys {
		records = append(records, rec)
	}
	return apikey.SaveRegistry(a.registry, records)
}

func (a *adminAuth) saveUsersLocked() error {
	users := make([]APIUser, 0, len(a.users))
	for _, user := range a.users {
		users = append(users, user)
	}
	b, err := json.Marshal(users)
	if err != nil {
		return err
	}
	return a.registry.Set(apiUserRegistryKey, b, 0)
}

func (a *adminAuth) validateAPIKey(c fh.Ctx) bool {
	key := strings.TrimSpace(c.Get("X-API-Key"))
	if key == "" {
		authorization := strings.TrimSpace(c.Get("Authorization"))
		if len(authorization) >= 7 && strings.EqualFold(authorization[:7], "Bearer ") {
			key = strings.TrimSpace(authorization[7:])
		}
	}
	if key == "" {
		return false
	}
	id, _ := apikey.SplitKey(key)
	a.mu.RLock()
	rec, exists := a.keys[id]
	if !exists || !apikey.VerifyRecord(c, key, rec) {
		for _, candidate := range a.keys {
			if apikey.VerifyRecord(c, key, candidate) {
				rec, exists = candidate, true
				break
			}
		}
	}
	user := a.users[rec.SubjectID]
	a.mu.RUnlock()
	if !exists || !user.Enabled || !apikey.VerifyRecord(c, key, rec) {
		return false
	}
	need := readScope
	path := c.Path()
	readOnlyPost := strings.HasSuffix(path, "/search") || strings.HasSuffix(path, "/count") || strings.HasSuffix(path, "/validate") || path == "/v1/infer-columns"
	if !readOnlyPost && c.Method() != "GET" && c.Method() != "HEAD" && c.Method() != "OPTIONS" {
		need = writeScope
	}
	return hasRequiredScopes(rec.Scopes, []string{need})
}

func hasRequiredScopes(have, need []string) bool {
	set := make(map[string]struct{}, len(have))
	for _, value := range have {
		set[value] = struct{}{}
	}
	for _, value := range need {
		if _, ok := set[value]; !ok {
			return false
		}
	}
	return true
}

func (a *adminAuth) requireAdmin(c fh.Ctx, write bool) error {
	if a == nil || a.manager == nil {
		return fhError(c, 401, "admin authentication is not configured")
	}
	s := session.Get(c)
	user, _ := s.Get(adminSessionUserKey).(string)
	a.credentialMu.RLock()
	adminUsername := a.username
	a.credentialMu.RUnlock()
	if user == "" || user != adminUsername {
		return fhError(c, 401, "admin session required")
	}
	if !a.managerIsSafeForRequest(c) {
		return fhError(c, 403, "admin session requires HTTPS or loopback")
	}
	if write && !a.validCSRF(c, s) {
		return fhError(c, 403, "invalid CSRF token")
	}
	return nil
}

func (a *adminAuth) listAPIUsers(c fh.Ctx) error {
	if err := a.requireAdmin(c, false); err != nil {
		return err
	}
	a.mu.RLock()
	users := make([]APIUser, 0, len(a.users))
	for _, user := range a.users {
		users = append(users, user)
	}
	a.mu.RUnlock()
	return c.JSON(users)
}

func (a *adminAuth) createAPIUser(c fh.Ctx) error {
	if err := a.requireAdmin(c, true); err != nil {
		return err
	}
	var req createAPIUserRequest
	if err := c.BodyParser(&req); err != nil {
		return fhError(c, 400, "invalid user request")
	}
	req.ID, req.Name = strings.TrimSpace(req.ID), strings.TrimSpace(req.Name)
	if !safeRegistryID.MatchString(req.ID) || req.Name == "" || len(req.Name) > 120 {
		return fhError(c, 400, "user id or name is invalid")
	}
	scopes, err := normalizeScopes(req.Scopes)
	if err != nil {
		return fhError(c, 400, err.Error())
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, exists := a.users[req.ID]; exists {
		return fhError(c, 409, "user already exists")
	}
	user := APIUser{ID: req.ID, Name: req.Name, Enabled: true, Scopes: scopes, CreatedAt: time.Now().UTC()}
	a.users[user.ID] = user
	if err := a.saveUsersLocked(); err != nil {
		delete(a.users, user.ID)
		return fhError(c, 500, "could not save API user")
	}
	return c.Status(201).JSON(user)
}

func (a *adminAuth) disableAPIUser(c fh.Ctx) error {
	if err := a.requireAdmin(c, true); err != nil {
		return err
	}
	id := strings.TrimSpace(c.Param("id"))
	a.mu.Lock()
	defer a.mu.Unlock()
	user, ok := a.users[id]
	if !ok {
		return fhError(c, 404, "user not found")
	}
	user.Enabled = false
	a.users[id] = user
	for keyID, rec := range a.keys {
		if rec.SubjectID == id {
			rec.Revoked = true
			a.keys[keyID] = rec
		}
	}
	if err := a.saveUsersLocked(); err != nil {
		return fhError(c, 500, "could not save API user")
	}
	if err := a.saveKeysLocked(); err != nil {
		return fhError(c, 500, "could not revoke user keys")
	}
	return c.Status(204).SendString("")
}

func (a *adminAuth) listAPIKeys(c fh.Ctx) error {
	if err := a.requireAdmin(c, false); err != nil {
		return err
	}
	a.mu.RLock()
	records := make([]apikey.KeyRecord, 0, len(a.keys))
	for _, rec := range a.keys {
		rec.Hash = ""
		records = append(records, rec)
	}
	a.mu.RUnlock()
	return c.JSON(records)
}

func (a *adminAuth) createAPIKey(c fh.Ctx) error {
	if err := a.requireAdmin(c, true); err != nil {
		return err
	}
	var req createAPIKeyRequest
	if err := c.BodyParser(&req); err != nil {
		return fhError(c, 400, "invalid API key request")
	}
	req.UserID, req.Name = strings.TrimSpace(req.UserID), strings.TrimSpace(req.Name)
	if req.Name == "" || len(req.Name) > 120 {
		return fhError(c, 400, "key name is required")
	}
	expires := time.Time{}
	if req.ExpiresAt != "" {
		var err error
		expires, err = time.Parse(time.RFC3339, req.ExpiresAt)
		if err != nil || !expires.After(time.Now()) {
			return fhError(c, 400, "expires_at must be a future RFC3339 timestamp")
		}
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	user, ok := a.users[req.UserID]
	if !ok || !user.Enabled {
		return fhError(c, 400, "enabled user required")
	}
	key, id, hash, err := generateManagedAPIKey(a.keys)
	if err != nil {
		return fhError(c, 500, "could not generate API key")
	}
	allowedIPs := make([]string, 0, len(req.AllowedIPs))
	for _, raw := range req.AllowedIPs {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		if net.ParseIP(raw) == nil {
			if _, _, err := net.ParseCIDR(raw); err != nil {
				return fhError(c, 400, "allowed IP entries must be IP addresses or CIDRs")
			}
		}
		allowedIPs = append(allowedIPs, raw)
	}
	rec := apikey.KeyRecord{ID: id, Name: req.Name, Hash: hash, Prefix: id, SubjectID: user.ID, Scopes: append([]string(nil), user.Scopes...), ExpiresAt: expires, AllowedIPs: allowedIPs}
	a.keys[id] = rec
	if err := a.saveKeysLocked(); err != nil {
		delete(a.keys, id)
		return fhError(c, 500, "could not save API key")
	}
	return c.Status(201).JSON(map[string]any{"id": id, "name": req.Name, "user_id": user.ID, "key": key, "expires_at": expires})
}

func (a *adminAuth) rotateAPIKey(c fh.Ctx) error {
	if err := a.requireAdmin(c, true); err != nil {
		return err
	}
	id := strings.TrimSpace(c.Param("id"))
	a.mu.Lock()
	defer a.mu.Unlock()
	old, ok := a.keys[id]
	if !ok || old.Revoked {
		return fhError(c, 404, "active API key not found")
	}
	if !old.ExpiresAt.IsZero() && !old.ExpiresAt.After(time.Now()) {
		return fhError(c, 409, "expired API key cannot be rotated")
	}
	user, ok := a.users[old.SubjectID]
	if !ok || !user.Enabled {
		return fhError(c, 409, "API key owner is disabled")
	}
	key, newID, hash, err := generateManagedAPIKey(a.keys)
	if err != nil {
		return fhError(c, 500, "could not generate API key")
	}
	replacement := apikey.KeyRecord{
		ID: newID, Name: old.Name, Hash: hash, Prefix: newID,
		SubjectID: old.SubjectID, Scopes: append([]string(nil), old.Scopes...),
		ExpiresAt: old.ExpiresAt, AllowedIPs: append([]string(nil), old.AllowedIPs...),
	}
	old.Revoked = true
	a.keys[id] = old
	a.keys[newID] = replacement
	if err := a.saveKeysLocked(); err != nil {
		delete(a.keys, newID)
		old.Revoked = false
		a.keys[id] = old
		return fhError(c, 500, "could not save rotated API key")
	}
	return c.Status(201).JSON(map[string]any{
		"id": newID, "name": replacement.Name, "user_id": replacement.SubjectID,
		"key": key, "expires_at": replacement.ExpiresAt, "rotated_from": id,
	})
}

func (a *adminAuth) revokeAPIKey(c fh.Ctx) error {
	if err := a.requireAdmin(c, true); err != nil {
		return err
	}
	id := strings.TrimSpace(c.Param("id"))
	a.mu.Lock()
	defer a.mu.Unlock()
	rec, ok := a.keys[id]
	if !ok {
		return fhError(c, 404, "API key not found")
	}
	rec.Revoked = true
	a.keys[id] = rec
	if err := a.saveKeysLocked(); err != nil {
		return fhError(c, 500, "could not revoke API key")
	}
	return c.Status(204).SendString("")
}

func randomRegistryID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func generateManagedAPIKey(existing map[string]apikey.KeyRecord) (key, id, hash string, err error) {
	for attempt := 0; attempt < 3; attempt++ {
		randomID, randomErr := randomRegistryID()
		if randomErr != nil {
			return "", "", "", randomErr
		}
		key, hash, err = apikey.Generate("lx_"+randomID, 32)
		if err != nil {
			return "", "", "", err
		}
		id, _ = apikey.SplitKey(key)
		if _, collision := existing[id]; !collision {
			return key, id, hash, nil
		}
	}
	return "", "", "", fmt.Errorf("could not allocate unique key ID")
}

func normalizeScopes(scopes []string) ([]string, error) {
	if len(scopes) == 0 {
		scopes = []string{readScope, writeScope}
	}
	allowed := map[string]bool{readScope: true, writeScope: true}
	result := make([]string, 0, len(scopes))
	seen := make(map[string]bool)
	for _, scope := range scopes {
		scope = strings.TrimSpace(scope)
		if !allowed[scope] {
			return nil, errors.New("scopes may only include lookup:read and lookup:write")
		}
		if !seen[scope] {
			seen[scope] = true
			result = append(result, scope)
		}
	}
	return result, nil
}
