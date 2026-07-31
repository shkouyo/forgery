// Package config loads forgery configuration from a TOML file.
//
// The configuration file has top-level global keys plus a [[instances]]
// array of Forgejo connections:
//
//	log_level = "info"
//
//	[[instances]]
//	forgejo_url = "https://forgejo.example.com"
//
// Loading chain: TOML file → defaults → validation. Unknown keys are
// rejected so typos fail loudly, and invalid values (durations, integers,
// booleans) return an error instead of silently falling back to defaults.
// See forgery.toml.example at the repository root for a fully commented
// example.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
)

// Config holds all configuration for the forgery daemon.
type Config struct {
	// Global holds daemon-wide settings. It is embedded so its TOML keys
	// live at the top level of the file, on par with [[instances]].
	Global

	// Instances holds one entry per Forgejo connection. Instance names are
	// the routing key for task → client resolution and must be unique.
	Instances []Instance
}

// Global holds daemon-wide settings. TOML keys are snake_case and live at
// the top level of the configuration file.
type Global struct {
	// ── Observability ──
	LogLevel   string `toml:"log_level"`   // log_level (default: "info")
	LogFormat  string `toml:"log_format"`  // log_format (default: "json")
	HealthAddr string `toml:"health_addr"` // health_addr (default: "")

	// ── Forgery server ──
	ListenAddr string `toml:"listen_addr"` // listen_addr (default: ":8443")
	PublicURL  string `toml:"public_url"`  // public_url (default: "https://<hostname>:<listen port>")
	StateFile  string `toml:"state_file"`  // state_file (default: <config dir>/forgery-state.json)

	// ── Concurrency ──
	MaxParallelTasks int `toml:"max_parallel_tasks"` // max_parallel_tasks (default: 5)

	// ── GitHub Actions connection ──
	GitHubToken      string `toml:"github_token"`       // github_token (required)
	GitHubRepo       string `toml:"github_repo"`        // github_repo "owner/repo" (required)
	GitHubWorkflowID string `toml:"github_workflow_id"` // github_workflow_id (required)
	GitHubRef        string `toml:"github_ref"`         // github_ref (default: "main")
	GitHubAPIURL     string `toml:"github_api_url"`     // github_api_url (default: "https://api.github.com")
}

// Instance describes one Forgejo connection. TOML keys are snake_case
// inside a [[instances]] array element.
type Instance struct {
	// ── Forgejo connection ──
	Name                string `toml:"name"`                  // name (default: host of forgejo_url)
	ForgejoURL          string `toml:"forgejo_url"`           // forgejo_url (required)
	ForgejoRunnerToken  string `toml:"forgejo_runner_token"`  // forgejo_runner_token (required)
	ForgejoRunnerName   string `toml:"forgejo_runner_name"`   // forgejo_runner_name (default: "forgery")
	ForgejoRunnerLabels Labels `toml:"forgejo_runner_labels"` // forgejo_runner_labels (required, comma-sep)

	// ── Container ──
	DefaultContainerImage string `toml:"default_container_image"` // default_container_image (default: "")

	// ── Concurrency & timeouts ──
	PollInterval      time.Duration `toml:"poll_interval"`      // poll_interval (default: 3s)
	RegTokenTTL       time.Duration `toml:"reg_token_ttl"`      // reg_token_ttl (default: 15m)
	GAStartupTimeout  time.Duration `toml:"ga_startup_timeout"` // ga_startup_timeout (default: 15m)
	HeartbeatInterval time.Duration `toml:"heartbeat_interval"` // heartbeat_interval (default: 30s)

	// ── TLS ──
	TLSInsecureSkipVerify bool `toml:"tls_insecure_skip_verify"` // tls_insecure_skip_verify (default: false)
}

// Labels is a runner label list stored in the TOML file as a single
// comma-separated string (e.g. "ubuntu-latest:docker://node:20,docker:…")
// and decoded into a string slice.
type Labels []string

// UnmarshalText splits a comma-separated label string, trimming whitespace
// from each element. Empty elements are dropped; an all-empty input
// yields nil. It implements encoding.TextUnmarshaler for BurntSushi/toml.
func (l *Labels) UnmarshalText(text []byte) error {
	*l = parseLabels(string(text))
	return nil
}

// Load reads, parses, and validates the TOML configuration file at path.
//
// It returns an error if the file is missing or unreadable, if it contains
// unknown keys or invalid values, or if required fields are missing. All
// missing and invalid entries are reported together in a single error.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: read %s: %w", path, err)
	}

	cfg := &Config{}
	md, err := toml.Decode(string(data), cfg)
	if err != nil {
		return nil, fmt.Errorf("config: parse %s: %w", path, err)
	}

	// Reject unknown keys so typos in the file are not silently ignored.
	if undecoded := md.Undecoded(); len(undecoded) > 0 {
		keys := make([]string, 0, len(undecoded))
		for _, k := range undecoded {
			keys = append(keys, k.String())
		}
		return nil, fmt.Errorf("config: %s: unknown keys: %s", path, strings.Join(keys, ", "))
	}

	applyDefaults(cfg, path)

	if err := validate(cfg); err != nil {
		return nil, err
	}

	return cfg, nil
}

// MustLoad calls Load and logs the error via the slog default logger, then
// exits. Useful for main() where there is no recovery from missing
// configuration. It intentionally takes no logger: it runs before the
// configured logger exists, so the error is reported through slog.Default().
func MustLoad(path string) *Config {
	cfg, err := Load(path)
	if err != nil {
		slog.Error("config load failed", "path", path, "err", err.Error())
		os.Exit(1)
	}
	return cfg
}

// ── defaults ──

// applyDefaults fills in default values for unset fields.
func applyDefaults(cfg *Config, path string) {
	g := &cfg.Global
	if g.LogLevel == "" {
		g.LogLevel = "info"
	}
	if g.LogFormat == "" {
		g.LogFormat = "json"
	}
	// HealthAddr default is "" (zero value).
	if g.ListenAddr == "" {
		g.ListenAddr = ":8443"
	}
	if g.PublicURL == "" {
		g.PublicURL = defaultPublicURL(g.ListenAddr)
	}
	if g.StateFile == "" {
		g.StateFile = filepath.Join(filepath.Dir(path), "forgery-state.json")
	}
	if g.MaxParallelTasks == 0 {
		g.MaxParallelTasks = 5
	}
	if g.GitHubRef == "" {
		g.GitHubRef = "main"
	}
	if g.GitHubAPIURL == "" {
		g.GitHubAPIURL = "https://api.github.com"
	}

	for i := range cfg.Instances {
		inst := &cfg.Instances[i]
		if inst.Name == "" {
			inst.Name = defaultInstanceName(inst.ForgejoURL)
		}
		if inst.ForgejoRunnerName == "" {
			inst.ForgejoRunnerName = "forgery"
		}
		// DefaultContainerImage default is "" (zero value).
		if inst.PollInterval == 0 {
			inst.PollInterval = 3 * time.Second
		}
		if inst.RegTokenTTL == 0 {
			inst.RegTokenTTL = 15 * time.Minute
		}
		if inst.GAStartupTimeout == 0 {
			inst.GAStartupTimeout = 15 * time.Minute
		}
		if inst.HeartbeatInterval == 0 {
			inst.HeartbeatInterval = 30 * time.Second
		}
		// TLSInsecureSkipVerify default is false (zero value).
	}
}

// defaultPublicURL derives the public URL from the hostname and the port
// of the listen address: "https://<hostname>:<port>".
func defaultPublicURL(listenAddr string) string {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "localhost"
	}
	port := "8443"
	if _, p, err := net.SplitHostPort(listenAddr); err == nil && p != "" {
		port = p
	}
	return "https://" + hostname + ":" + port
}

// defaultInstanceName derives the instance name from the host part of its
// Forgejo URL. Returns "" when the URL cannot be parsed (the field stays
// empty and can be overridden by an explicit name).
func defaultInstanceName(forgejoURL string) string {
	u, err := url.Parse(forgejoURL)
	if err != nil || u.Host == "" {
		return ""
	}
	return u.Host
}

// ── validation ──

// validate checks that all required fields are present and that no field
// holds a semantically invalid value. Errors are aggregated so a single
// run reports every missing and invalid entry.
func validate(cfg *Config) error {
	v := &errValidation{}

	if cfg.GitHubToken == "" {
		v.missing = append(v.missing, "github_token")
	}
	if cfg.GitHubRepo == "" {
		v.missing = append(v.missing, "github_repo")
	}
	if cfg.GitHubWorkflowID == "" {
		v.missing = append(v.missing, "github_workflow_id")
	}
	if cfg.MaxParallelTasks < 0 {
		v.invalid = append(v.invalid, "max_parallel_tasks (must be positive)")
	}

	if len(cfg.Instances) == 0 {
		v.missing = append(v.missing, "instances")
	}

	// Validate instance fields. When no instance is declared, report the
	// fields a single implicit instance would still need.
	check := cfg.Instances
	if len(check) == 0 {
		check = []Instance{{}}
	}
	// Instance names are the routing key for task → client resolution, so
	// they must be unique across the whole configuration.
	seenNames := make(map[string]int, len(check))
	for i := range check {
		inst := &check[i]
		prefix := fmt.Sprintf("instances[%d]", i)
		if prev, dup := seenNames[inst.Name]; dup {
			v.invalid = append(v.invalid, fmt.Sprintf(
				"%s.name (duplicate instance name %q, already used by instances[%d])",
				prefix, inst.Name, prev))
		}
		seenNames[inst.Name] = i
		if inst.ForgejoURL == "" {
			v.missing = append(v.missing, prefix+".forgejo_url")
		}
		if inst.ForgejoRunnerToken == "" {
			v.missing = append(v.missing, prefix+".forgejo_runner_token")
		}
		if len(inst.ForgejoRunnerLabels) == 0 {
			v.missing = append(v.missing, prefix+".forgejo_runner_labels")
		}
		if inst.PollInterval < 0 {
			v.invalid = append(v.invalid, prefix+".poll_interval (must be positive)")
		}
		if inst.RegTokenTTL < 0 {
			v.invalid = append(v.invalid, prefix+".reg_token_ttl (must be positive)")
		}
		if inst.GAStartupTimeout < 0 {
			v.invalid = append(v.invalid, prefix+".ga_startup_timeout (must be positive)")
		}
		if inst.HeartbeatInterval < 0 {
			v.invalid = append(v.invalid, prefix+".heartbeat_interval (must be positive)")
		}
	}

	if len(v.missing) == 0 && len(v.invalid) == 0 {
		return nil
	}
	return v
}

// errValidation aggregates all configuration problems found in one run:
// missing required fields and invalid values.
type errValidation struct {
	missing []string
	invalid []string
}

func (e *errValidation) Error() string {
	var parts []string
	if len(e.missing) > 0 {
		parts = append(parts, "missing required configuration: "+strings.Join(e.missing, ", "))
	}
	if len(e.invalid) > 0 {
		parts = append(parts, "invalid configuration values: "+strings.Join(e.invalid, ", "))
	}
	return strings.Join(parts, "; ")
}

// MissingFields extracts the list of missing required field names from an
// error, if the error is a validation error.
func MissingFields(err error) []string {
	var ve *errValidation
	if errors.As(err, &ve) {
		return ve.missing
	}
	return nil
}

// InvalidFields extracts the list of invalid value descriptions from an
// error, if the error is a validation error.
func InvalidFields(err error) []string {
	var ve *errValidation
	if errors.As(err, &ve) {
		return ve.invalid
	}
	return nil
}

// ── helpers ──

// parseLabels splits a comma-separated string, trimming whitespace from
// each element. Empty elements are dropped; an all-empty input returns nil.
func parseLabels(s string) []string {
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
