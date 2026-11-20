package pkg

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/apikey"
	"github.com/oarkflow/fh/mw/session"
	"github.com/oarkflow/fh/pkg/storage/kv"
	"golang.org/x/crypto/argon2"
)

const adminSessionUserKey = "lookupx_admin_user"
const adminSessionCSRFKey = "lookupx_admin_csrf"
const adminPeerIPLocalKey = "lookupx_socket_peer_ip"
const adminCredentialRegistryKey = "lookupx_admin_credentials"

type adminCredential struct {
	Username string `json:"username"`
	Hash     string `json:"hash"`
}

type adminAuth struct {
	username         string
	passwordHash     string
	manager          *session.SessionManager
	secure           bool
	trustedProxyNets []*net.IPNet
	registry         kv.Store
	credentialMu     sync.RWMutex
	passwordVerifyMu sync.Mutex
	keys             map[string]apikey.KeyRecord
	users            map[string]APIUser
	mu               sync.RWMutex
	// disabled marks an intentionally unauthenticated deployment: no session
	// secret, admin password, or API keys were configured. The public API
	// (and /health, /metrics) must stay open in that state rather than the
	// gate denying every request because no credential was ever supplied.
	disabled bool
}

func newAdminAuth(server *MultiServer) *adminAuth {
	if server == nil {
		return &adminAuth{disabled: true}
	}
	a := &adminAuth{}
	for _, raw := range server.SessionTrustedProxies {
		raw = strings.TrimSpace(raw)
		if _, subnet, err := net.ParseCIDR(raw); err == nil {
			a.trustedProxyNets = append(a.trustedProxyNets, subnet)
			continue
		}
		ip := net.ParseIP(raw)
		if ip == nil {
			panic(fmt.Sprintf("invalid LOOKUPX_SESSION_TRUSTED_PROXIES entry %q: expected an IP address or CIDR", raw))
		}
		bits := 128
		if ip.To4() != nil {
			ip, bits = ip.To4(), 32
		}
		a.trustedProxyNets = append(a.trustedProxyNets, &net.IPNet{IP: ip, Mask: net.CIDRMask(bits, bits)})
	}
	if strings.TrimSpace(server.AdminUser) != "" && strings.TrimSpace(server.AdminPassword) != "" && (!safeRegistryID.MatchString(server.AdminUser) || len(server.AdminPassword) < 12 || len(server.AdminPassword) > 1024) {
		panic("bootstrap admin username must be 1–64 letters, numbers, underscores, or hyphens and password must be 12–1024 characters")
	}
	if len(server.SessionSecret) < 32 {
		if server.AdminPassword != "" || len(server.APIKeys) > 0 {
			panic("LOOKUPX_SESSION_SECRET must contain at least 32 characters when admin auth or API keys are enabled")
		}
		a.disabled = true
		return a
	}
	name, secure := "lookupx_admin_session", server.SessionSecure
	if secure {
		name = "__Host-lookupx_admin_session"
	}
	manager := session.NewSessionManager(kv.NewMemoryStore(),
		session.SessionCookieName(name), session.SessionSecret([]byte(server.SessionSecret)),
		session.SessionMaxAge(8*time.Hour), session.SessionHTTPOnly(true),
		session.SessionSecure(secure), session.SessionSameSite(fh.SameSiteStrict), session.SessionPath("/"),
	)
	a.manager, a.secure = manager, secure
	if err := a.initRegistry(server); err != nil {
		panic("initialize API key registry: " + err.Error())
	}
	if err := a.loadOrBootstrapAdmin(server); err != nil {
		panic("initialize admin credentials: " + err.Error())
	}
	return a
}

func (a *adminAuth) loadOrBootstrapAdmin(server *MultiServer) error {
	if found, err := a.refreshAdminCredential(); err != nil {
		return err
	} else if found {
		return nil
	}
	if strings.TrimSpace(server.AdminUser) == "" || strings.TrimSpace(server.AdminPassword) == "" {
		return nil // The browser can claim the one-time setup flow.
	}
	hash, err := hashAdminPassword(server.AdminPassword)
	if err != nil {
		return err
	}
	return a.storeAdminCredential(adminCredential{Username: server.AdminUser, Hash: hash})
}

// refreshAdminCredential makes the durable credential authoritative for each
// server instance. This also handles setup completed by another process that
// shares the configured auth store.
func (a *adminAuth) refreshAdminCredential() (bool, error) {
	if a == nil || a.registry == nil {
		return false, nil
	}
	raw, ok, err := a.registry.Get(adminCredentialRegistryKey)
	if err != nil {
		return false, fmt.Errorf("read stored admin credentials: %w", err)
	}
	if !ok {
		return false, nil
	}
	var credential adminCredential
	if err := json.Unmarshal(raw, &credential); err != nil {
		return false, fmt.Errorf("decode stored admin credentials: %w", err)
	}
	if credential.Username == "" || credential.Hash == "" || !strings.HasPrefix(credential.Hash, "$argon2id$") {
		return false, fmt.Errorf("stored admin credentials are invalid")
	}
	a.credentialMu.Lock()
	a.username, a.passwordHash = credential.Username, credential.Hash
	a.credentialMu.Unlock()
	return true, nil
}

func (a *adminAuth) storeAdminCredential(credential adminCredential) error {
	raw, err := json.Marshal(credential)
	if err != nil {
		return err
	}
	if err := a.registry.Set(adminCredentialRegistryKey, raw, 0); err != nil {
		return err
	}
	a.username, a.passwordHash = credential.Username, credential.Hash
	return nil
}

func hashAdminPassword(password string) (string, error) {
	salt := make([]byte, 16)
	if _, err := rand.Read(salt); err != nil {
		return "", err
	}
	const memory, iterations, parallelism, keyLength = 64 * 1024, 3, 2, 32
	hash := argon2.IDKey([]byte(password), salt, iterations, memory, parallelism, keyLength)
	return fmt.Sprintf("$argon2id$v=19$m=%d,t=%d,p=%d$%s$%s", memory, iterations, parallelism,
		base64.RawStdEncoding.EncodeToString(salt), base64.RawStdEncoding.EncodeToString(hash)), nil
}

func verifyAdminPassword(password, encoded string) bool {
	parts := strings.Split(encoded, "$")
	if len(parts) != 6 || parts[1] != "argon2id" || parts[2] != "v=19" {
		return false
	}
	var memory, iterations, parallelism uint32
	if _, err := fmt.Sscanf(parts[3], "m=%d,t=%d,p=%d", &memory, &iterations, &parallelism); err != nil || memory < 8*1024 || memory > 128*1024 || iterations == 0 || iterations > 10 || parallelism == 0 || parallelism > 8 {
		return false
	}
	salt, err := base64.RawStdEncoding.DecodeString(parts[4])
	if err != nil || len(salt) < 16 || len(salt) > 64 {
		return false
	}
	want, err := base64.RawStdEncoding.DecodeString(parts[5])
	if err != nil || len(want) < 16 || len(want) > 64 {
		return false
	}
	got := argon2.IDKey([]byte(password), salt, iterations, memory, uint8(parallelism), uint32(len(want)))
	return subtle.ConstantTimeCompare(got, want) == 1
}

func (a *adminAuth) setup(c fh.Ctx) error {
	if a == nil || a.manager == nil || a.registry == nil {
		return fhError(c, 503, "admin setup is unavailable")
	}
	if !a.managerIsSafeForRequest(c) || !sameOriginOrMissing(c) {
		return fhError(c, 403, "admin setup requires a trusted HTTPS or loopback request")
	}
	if _, err := a.refreshAdminCredential(); err != nil {
		return fhError(c, 500, "could not read stored admin setup state")
	}
	type setupRequest struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	var req setupRequest
	if err := c.BodyParser(&req); err != nil {
		return fhError(c, 400, "invalid setup request")
	}
	req.Username = strings.TrimSpace(req.Username)
	if !safeRegistryID.MatchString(req.Username) || len(req.Password) < 12 || len(req.Password) > 1024 {
		return fhError(c, 400, "username must be 1–64 letters, numbers, underscores, or hyphens; password must be 12–1024 characters")
	}
	a.credentialMu.Lock()
	defer a.credentialMu.Unlock()
	if a.username != "" {
		return fhError(c, 409, "admin setup is already complete")
	}
	if _, exists, err := a.registry.Get(adminCredentialRegistryKey); err != nil {
		return fhError(c, 500, "could not verify admin setup state")
	} else if exists {
		return fhError(c, 409, "admin setup is already complete")
	}
	a.passwordVerifyMu.Lock()
	hash, err := hashAdminPassword(req.Password)
	a.passwordVerifyMu.Unlock()
	if err != nil {
		return fhError(c, 500, "could not create admin credentials")
	}
	if err := a.storeAdminCredential(adminCredential{Username: req.Username, Hash: hash}); err != nil {
		return fhError(c, 500, "could not save admin credentials")
	}
	return a.createSession(c, req.Username)
}

func (a *adminAuth) createSession(c fh.Ctx, username string) error {
	s := session.Get(c)
	if err := a.manager.Regenerate(c, s); err != nil {
		return fhError(c, 500, "could not create session")
	}
	csrf, err := randomAuthToken(32)
	if err != nil {
		return fhError(c, 500, "could not create session")
	}
	s.Set(adminSessionUserKey, username)
	s.Set(adminSessionCSRFKey, csrf)
	return c.JSON(map[string]any{"authenticated": true, "csrf_token": csrf, "user": username})
}

func (a *adminAuth) login(c fh.Ctx) error {
	if a == nil || a.manager == nil {
		return fhError(c, 404, "admin authentication is not configured")
	}
	if !a.managerIsSafeForRequest(c) {
		return fhError(c, 403, "admin login requires HTTPS or loopback")
	}
	if !sameOriginOrMissing(c) {
		return fhError(c, 403, "cross-origin login rejected")
	}
	if _, err := a.refreshAdminCredential(); err != nil {
		return fhError(c, 500, "could not read stored admin credentials")
	}
	user, pass, ok := parseBasicAuthorization(c.Get("Authorization"))
	a.credentialMu.RLock()
	wantUser, encodedHash := a.username, a.passwordHash
	a.credentialMu.RUnlock()
	a.passwordVerifyMu.Lock()
	validPassword := ok && encodedHash != "" && verifyAdminPassword(pass, encodedHash)
	a.passwordVerifyMu.Unlock()
	if !ok || !constantTimeStringEqual(user, wantUser) || !validPassword {
		return fhError(c, 401, "invalid credentials")
	}
	return a.createSession(c, wantUser)
}

func (a *adminAuth) logout(c fh.Ctx) error {
	if a == nil || a.manager == nil {
		return fhError(c, 404, "admin authentication is not configured")
	}
	s := session.Get(c)
	if s.Get(adminSessionUserKey) == nil {
		return c.Status(204).SendString("")
	}
	if !a.validCSRF(c, s) {
		return fhError(c, 403, "invalid CSRF token")
	}
	if err := a.manager.Destroy(c, s); err != nil {
		return fhError(c, 500, "could not end session")
	}
	return c.Status(204).SendString("")
}

func (a *adminAuth) status(c fh.Ctx) error {
	if a == nil || a.manager == nil {
		return c.JSON(map[string]any{"authenticated": false, "setup_required": false})
	}
	if _, err := a.refreshAdminCredential(); err != nil {
		return fhError(c, 500, "could not read admin setup state")
	}
	a.credentialMu.RLock()
	setupRequired := a.username == ""
	a.credentialMu.RUnlock()
	s := session.Get(c)
	user, _ := s.Get(adminSessionUserKey).(string)
	csrf, _ := s.Get(adminSessionCSRFKey).(string)
	if user == "" {
		return c.JSON(map[string]any{"authenticated": false, "setup_required": setupRequired})
	}
	return c.JSON(map[string]any{"authenticated": true, "setup_required": setupRequired, "csrf_token": csrf, "user": user})
}

func (a *adminAuth) middleware(c fh.Ctx) error {
	if a != nil && a.disabled {
		return c.Next()
	}
	path := c.Path()
	if strings.HasPrefix(path, "/admin/api/") || strings.HasPrefix(path, "/auth/") {
		c.Set("Cache-Control", "no-store")
		c.Set("Pragma", "no-cache")
	}
	if !isProtectedAPIPath(path) {
		return c.Next()
	}
	if a == nil {
		return fhError(c, 401, "unauthorized")
	}
	if a.manager != nil {
		s := session.Get(c)
		a.credentialMu.RLock()
		adminUsername := a.username
		a.credentialMu.RUnlock()
		if user, _ := s.Get(adminSessionUserKey).(string); user != "" && user == adminUsername {
			if !a.requestTransportIsSafe(c) {
				return fhError(c, 403, "admin session requires HTTPS or loopback")
			}
			if c.Method() != "GET" && c.Method() != "HEAD" && c.Method() != "OPTIONS" && !a.validCSRF(c, s) {
				return fhError(c, 403, "invalid CSRF token")
			}
			return c.Next()
		}
	}
	if a.validateAPIKey(c) {
		if !a.requestTransportIsSafe(c) {
			return fhError(c, 403, "API keys require HTTPS or loopback")
		}
		return c.Next()
	}
	return fhError(c, 401, "unauthorized")
}

func isProtectedAPIPath(path string) bool {
	return path == "/health" || path == "/metrics" || path == "/v1" || strings.HasPrefix(path, "/v1/")
}

func (a *adminAuth) validCSRF(c fh.Ctx, s *session.Session) bool {
	expected, _ := s.Get(adminSessionCSRFKey).(string)
	return expected != "" && constantTimeStringEqual(expected, c.Get("X-CSRF-Token")) && sameOrigin(c)
}

func (a *adminAuth) managerIsSafeForRequest(c fh.Ctx) bool {
	return a.manager != nil && a.requestTransportIsSafe(c)
}

func (a *adminAuth) requestTransportIsSafe(c fh.Ctx) bool {
	peerIP, _ := c.Locals(adminPeerIPLocalKey).(string)
	if peerIP == "" {
		peerIP = c.IP()
	}
	ip := net.ParseIP(strings.Trim(peerIP, "[]"))
	if ip != nil && ip.IsLoopback() {
		return true
	}
	if ip == nil || !a.secure || !strings.EqualFold(strings.TrimSpace(c.Get("X-Forwarded-Proto")), "https") {
		return false
	}
	for _, subnet := range a.trustedProxyNets {
		if subnet.Contains(ip) {
			return true
		}
	}
	return false
}

func randomAuthToken(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func parseBasicAuthorization(value string) (string, string, bool) {
	prefix, payload, ok := strings.Cut(value, " ")
	if !ok || !strings.EqualFold(prefix, "Basic") {
		return "", "", false
	}
	raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(payload))
	if err != nil {
		return "", "", false
	}
	user, pass, ok := strings.Cut(string(raw), ":")
	return user, pass, ok && user != "" && len(raw) <= 4096
}

func sameOrigin(c fh.Ctx) bool {
	origin := strings.TrimSpace(c.Get("Origin"))
	if origin == "" {
		return false
	}
	u, err := url.Parse(origin)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return false
	}
	host := c.Get("Host")
	return host != "" && strings.EqualFold(u.Host, host)
}

func sameOriginOrMissing(c fh.Ctx) bool {
	if strings.TrimSpace(c.Get("Origin")) == "" {
		return true
	}
	return sameOrigin(c)
}
