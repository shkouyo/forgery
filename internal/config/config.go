// Package config loads forgery configuration from environment variables,
// with optional .env file support via godotenv.
//
// Loading chain: .env file → OS environment → defaults → validation.
// Environment variables take precedence over .env values.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

// Config holds all configuration for the forgery daemon.
// See DETAIL-DESIGN §5.2 and §6.1 for the full specification.
type Config struct {
	// ── Forgejo connection ──
	ForgejoURL          string   // FORGEJO_URL (required)
	ForgejoRunnerToken  string   // FORGEJO_RUNNER_TOKEN (required)
	ForgejoRunnerName   string   // FORGEJO_RUNNER_NAME (required)
	ForgejoRunnerLabels []string // FORGEJO_RUNNER_LABELS (required, comma-sep)

	// ── GitHub Actions connection ──
	GitHubToken     string // GITHUB_TOKEN (required)
	GitHubRepo      string // GITHUB_REPO "owner/repo" (required)
	GitHubWorkflowID string // GITHUB_WORKFLOW_ID (required)
	GitHubRef       string // GITHUB_REF (default: "main")
	GHApiURL        string // GITHUB_API_URL (default: "https://api.github.com")

	// ── Forgery server ──
	ListenAddr string // LISTEN_ADDR (default: ":8443")
	PublicURL  string // PUBLIC_URL (default: "https://<hostname>:8443")

	// ── Container / labels ──
	DefaultContainerImage string // DEFAULT_CONTAINER_IMAGE

	// ── Concurrency & timeouts ──
	MaxParallelTasks  int           // MAX_PARALLEL_TASKS (default: 5)
	PollInterval      time.Duration // POLL_INTERVAL (default: 3s)
	RegTokenTTL       time.Duration // REG_TOKEN_TTL (default: 15m)
	GAStartupTimeout  time.Duration // GA_STARTUP_TIMEOUT (default: 15m)
	HeartbeatInterval time.Duration // HEARTBEAT_INTERVAL (default: 30s)
	PingKeepalive     bool          // PING_KEEPALIVE (default: true)

	// ── Observability ──
	LogLevel    string // LOG_LEVEL (default: "info")
	LogFormat   string // LOG_FORMAT (default: "json")
	MetricsAddr string // METRICS_ADDR (default: ":9090")
	HealthAddr  string // HEALTH_ADDR (default: "")

	// ── TLS ──
	TLSInsecureSkipVerify bool // TLS_INSECURE_SKIP_VERIFY (default: false)
}

// Load reads configuration following the chain:
//  1. Load .env file via godotenv (silently skipped if missing)
//  2. Read OS environment variables
//  3. Apply defaults for zero-value fields
//  4. Validate required fields
//
// Returns the parsed Config or an error listing all missing required fields.
func Load() (*Config, error) {
	// Step 1: Load .env file — ignore error if file doesn't exist.
	_ = godotenv.Load()

	cfg := &Config{}

	// Step 2: Read from environment variables.
	cfg.ForgejoURL = os.Getenv("FORGEJO_URL")
	cfg.ForgejoRunnerToken = os.Getenv("FORGEJO_RUNNER_TOKEN")
	cfg.ForgejoRunnerName = os.Getenv("FORGEJO_RUNNER_NAME")
	cfg.ForgejoRunnerLabels = parseLabels(os.Getenv("FORGEJO_RUNNER_LABELS"))

	cfg.GitHubToken = os.Getenv("GITHUB_TOKEN")
	cfg.GitHubRepo = os.Getenv("GITHUB_REPO")
	cfg.GitHubWorkflowID = os.Getenv("GITHUB_WORKFLOW_ID")
	cfg.GitHubRef = os.Getenv("GITHUB_REF")
	cfg.GHApiURL = os.Getenv("GITHUB_API_URL")

	cfg.ListenAddr = os.Getenv("LISTEN_ADDR")
	cfg.PublicURL = os.Getenv("PUBLIC_URL")

	cfg.DefaultContainerImage = os.Getenv("DEFAULT_CONTAINER_IMAGE")

	cfg.MaxParallelTasks = parseInt(os.Getenv("MAX_PARALLEL_TASKS"))
	cfg.PollInterval = parseDuration(os.Getenv("POLL_INTERVAL"))
	cfg.RegTokenTTL = parseDuration(os.Getenv("REG_TOKEN_TTL"))
	cfg.GAStartupTimeout = parseDuration(os.Getenv("GA_STARTUP_TIMEOUT"))
	cfg.HeartbeatInterval = parseDuration(os.Getenv("HEARTBEAT_INTERVAL"))
	cfg.PingKeepalive = parseBool(os.Getenv("PING_KEEPALIVE"))

	cfg.LogLevel = os.Getenv("LOG_LEVEL")
	cfg.LogFormat = os.Getenv("LOG_FORMAT")
	cfg.MetricsAddr = os.Getenv("METRICS_ADDR")
	cfg.HealthAddr = os.Getenv("HEALTH_ADDR")

	cfg.TLSInsecureSkipVerify = parseBool(os.Getenv("TLS_INSECURE_SKIP_VERIFY"))

	// Step 3: Apply defaults for zero-value fields.
	applyDefaults(cfg)

	// Step 4: Validate required fields.
	if err := validateRequired(cfg); err != nil {
		return nil, err
	}

	return cfg, nil
}

// MustLoad calls Load and logs the error via slog, then exits.
// Useful for main() where there is no recovery from missing configuration.
func MustLoad(log *slog.Logger) *Config {
	cfg, err := Load()
	if err != nil {
		log.Error("config load failed", "err", err.Error())
		os.Exit(1)
	}
	return cfg
}

// ── helpers ──

// parseLabels splits a comma-separated string, trimming whitespace from each element.
// An empty input returns nil.
func parseLabels(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	result := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			result = append(result, p)
		}
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

// parseInt returns the parsed integer or 0 if parsing fails or the string is empty.
func parseInt(s string) int {
	if s == "" {
		return 0
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0
	}
	return n
}

// parseDuration returns the parsed duration or 0 if parsing fails or the string is empty.
func parseDuration(s string) time.Duration {
	if s == "" {
		return 0
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0
	}
	return d
}

// parseBool parses a boolean string using strconv.ParseBool.
// Returns false for empty strings or parse errors.
func parseBool(s string) bool {
	if s == "" {
		return false
	}
	b, err := strconv.ParseBool(s)
	if err != nil {
		return false
	}
	return b
}

// applyDefaults fills in default values for zero-value fields.
func applyDefaults(cfg *Config) {
	if cfg.GitHubRef == "" {
		cfg.GitHubRef = "main"
	}
	if cfg.GHApiURL == "" {
		cfg.GHApiURL = "https://api.github.com"
	}
	if cfg.ListenAddr == "" {
		cfg.ListenAddr = ":8443"
	}
	if cfg.PublicURL == "" {
		hostname, err := os.Hostname()
		if err != nil {
			hostname = "localhost"
		}
		cfg.PublicURL = "https://" + hostname + ":8443"
	}
	if cfg.MaxParallelTasks <= 0 {
		cfg.MaxParallelTasks = 5
	}
	if cfg.PollInterval == 0 {
		cfg.PollInterval = 3 * time.Second
	}
	if cfg.RegTokenTTL == 0 {
		cfg.RegTokenTTL = 15 * time.Minute
	}
	if cfg.GAStartupTimeout == 0 {
		cfg.GAStartupTimeout = 15 * time.Minute
	}
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = 30 * time.Second
	}
	if !cfg.PingKeepalive {
		// PingKeepalive defaults to true. Since we can't distinguish
		// "not set" from "set to false" with a single bool, we use
		// the raw env to decide. If the env var was empty, default to true.
		if os.Getenv("PING_KEEPALIVE") == "" {
			cfg.PingKeepalive = true
		}
	}
	if cfg.LogLevel == "" {
		cfg.LogLevel = "info"
	}
	if cfg.LogFormat == "" {
		cfg.LogFormat = "json"
	}
	if cfg.MetricsAddr == "" {
		cfg.MetricsAddr = ":9090"
	}
	// HealthAddr default is already "" (zero value).
	// TLSInsecureSkipVerify default is already false (zero value).
}

// validateRequired checks that all required fields are non-empty.
// Returns an aggregated error listing every missing field.
func validateRequired(cfg *Config) error {
	var missing []string

	if cfg.ForgejoURL == "" {
		missing = append(missing, "FORGEJO_URL")
	}
	if cfg.ForgejoRunnerToken == "" {
		missing = append(missing, "FORGEJO_RUNNER_TOKEN")
	}
	if cfg.ForgejoRunnerName == "" {
		missing = append(missing, "FORGEJO_RUNNER_NAME")
	}
	if len(cfg.ForgejoRunnerLabels) == 0 {
		missing = append(missing, "FORGEJO_RUNNER_LABELS")
	}
	if cfg.GitHubToken == "" {
		missing = append(missing, "GITHUB_TOKEN")
	}
	if cfg.GitHubRepo == "" {
		missing = append(missing, "GITHUB_REPO")
	}
	if cfg.GitHubWorkflowID == "" {
		missing = append(missing, "GITHUB_WORKFLOW_ID")
	}

	if len(missing) == 0 {
		return nil
	}

	return &errMissingFields{Fields: missing}
}

// errMissingFields is a sentinel type for testing — allows callers to check
// whether an error is a missing-fields error via errors.As.
type errMissingFields struct {
	Fields []string
}

func (e *errMissingFields) Error() string {
	return fmt.Sprintf("missing required configuration: %s", strings.Join(e.Fields, ", "))
}

// MissingFields extracts the list of missing field names from an error,
// if the error is a missing-fields validation error.
func MissingFields(err error) []string {
	var mf *errMissingFields
	if errors.As(err, &mf) {
		return mf.Fields
	}
	return nil
}
