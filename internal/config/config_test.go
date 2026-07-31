package config

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// validConfig returns TOML content with every required field set.
func validConfig() string {
	return `
log_level = "debug"
log_format = "text"
health_addr = ":8080"
listen_addr = ":9999"
public_url = "https://forgery.example.com:8443"
max_parallel_tasks = 10
github_token = "ghp_test123"
github_repo = "owner/repo"
github_workflow_id = "forgery-runner.yml"
github_ref = "develop"
github_api_url = "https://github.example.com/api"

[[instances]]
name = "test-runner"
forgejo_url = "https://forgejo.example.com"
forgejo_runner_token = "test-runner-token"
forgejo_runner_name = "test-runner"
forgejo_runner_labels = "ubuntu-latest:docker://node:20,docker:docker://catthehacker/ubuntu:act-latest"
poll_interval = "5s"
reg_token_ttl = "10m"
ga_startup_timeout = "20m"
heartbeat_interval = "45s"
tls_insecure_skip_verify = true
`
}

// writeConfig writes content to a fresh forgery.toml in a temp dir and
// returns the file path.
func writeConfig(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "forgery.toml")
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	return path
}

// loadValid writes a fully-populated config file and loads it.
func loadValid(t *testing.T) *Config {
	t.Helper()
	cfg, err := Load(writeConfig(t, validConfig()))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	return cfg
}

// ── Load: full parse ─────────────────────────────────────────────────────────

func TestLoadAllFields(t *testing.T) {
	cfg := loadValid(t)

	// Global — observability
	if cfg.Global.LogLevel != "debug" {
		t.Errorf("Global.LogLevel = %q, want debug", cfg.Global.LogLevel)
	}
	if cfg.Global.LogFormat != "text" {
		t.Errorf("Global.LogFormat = %q, want text", cfg.Global.LogFormat)
	}
	if cfg.Global.HealthAddr != ":8080" {
		t.Errorf("Global.HealthAddr = %q, want :8080", cfg.Global.HealthAddr)
	}

	// Global — forgery server
	if cfg.Global.ListenAddr != ":9999" {
		t.Errorf("Global.ListenAddr = %q, want :9999", cfg.Global.ListenAddr)
	}
	if cfg.Global.PublicURL != "https://forgery.example.com:8443" {
		t.Errorf("Global.PublicURL = %q", cfg.Global.PublicURL)
	}

	// Global — concurrency
	if cfg.Global.MaxParallelTasks != 10 {
		t.Errorf("Global.MaxParallelTasks = %d, want 10", cfg.Global.MaxParallelTasks)
	}

	// Global — GitHub Actions connection
	if cfg.Global.GitHubToken != "ghp_test123" {
		t.Errorf("Global.GitHubToken = %q", cfg.Global.GitHubToken)
	}
	if cfg.Global.GitHubRepo != "owner/repo" {
		t.Errorf("Global.GitHubRepo = %q", cfg.Global.GitHubRepo)
	}
	if cfg.Global.GitHubWorkflowID != "forgery-runner.yml" {
		t.Errorf("Global.GitHubWorkflowID = %q", cfg.Global.GitHubWorkflowID)
	}
	if cfg.Global.GitHubRef != "develop" {
		t.Errorf("Global.GitHubRef = %q, want develop", cfg.Global.GitHubRef)
	}
	if cfg.Global.GitHubAPIURL != "https://github.example.com/api" {
		t.Errorf("Global.GitHubAPIURL = %q", cfg.Global.GitHubAPIURL)
	}

	if len(cfg.Instances) != 1 {
		t.Fatalf("len(Instances) = %d, want 1", len(cfg.Instances))
	}
	inst := cfg.Instances[0]

	// Instance — Forgejo connection
	if inst.Name != "test-runner" {
		t.Errorf("Instances[0].Name = %q", inst.Name)
	}
	if inst.ForgejoURL != "https://forgejo.example.com" {
		t.Errorf("Instances[0].ForgejoURL = %q", inst.ForgejoURL)
	}
	if inst.ForgejoRunnerToken != "test-runner-token" {
		t.Errorf("Instances[0].ForgejoRunnerToken = %q", inst.ForgejoRunnerToken)
	}
	if inst.ForgejoRunnerName != "test-runner" {
		t.Errorf("Instances[0].ForgejoRunnerName = %q", inst.ForgejoRunnerName)
	}
	expectedLabels := []string{
		"ubuntu-latest:docker://node:20",
		"docker:docker://catthehacker/ubuntu:act-latest",
	}
	if !stringSlicesEqual(inst.ForgejoRunnerLabels, expectedLabels) {
		t.Errorf("Instances[0].ForgejoRunnerLabels = %v, want %v", inst.ForgejoRunnerLabels, expectedLabels)
	}

	// Instance — concurrency & timeouts
	if inst.PollInterval != 5*time.Second {
		t.Errorf("Instances[0].PollInterval = %v, want 5s", inst.PollInterval)
	}
	if inst.RegTokenTTL != 10*time.Minute {
		t.Errorf("Instances[0].RegTokenTTL = %v, want 10m", inst.RegTokenTTL)
	}
	if inst.GAStartupTimeout != 20*time.Minute {
		t.Errorf("Instances[0].GAStartupTimeout = %v, want 20m", inst.GAStartupTimeout)
	}
	if inst.HeartbeatInterval != 45*time.Second {
		t.Errorf("Instances[0].HeartbeatInterval = %v, want 45s", inst.HeartbeatInterval)
	}

	// Instance — TLS
	if inst.TLSInsecureSkipVerify != true {
		t.Errorf("Instances[0].TLSInsecureSkipVerify = %v, want true", inst.TLSInsecureSkipVerify)
	}
}

// ── Defaults ─────────────────────────────────────────────────────────────────

func TestDefaults(t *testing.T) {
	cfg := loadValid(t)

	// String defaults are not applied when values are set — spot check a
	// few that the file overrides.
	if cfg.Global.GitHubRef != "develop" {
		t.Errorf("GitHubRef = %q, want develop (explicit value)", cfg.Global.GitHubRef)
	}
	if cfg.Global.GitHubAPIURL != "https://github.example.com/api" {
		t.Errorf("GitHubAPIURL = %q, want explicit value", cfg.Global.GitHubAPIURL)
	}
}

func TestDefaultsUnset(t *testing.T) {
	minimal := `
github_token = "ghp_test123"
github_repo = "owner/repo"
github_workflow_id = "forgery-runner.yml"

[[instances]]
forgejo_url = "https://forgejo.example.com"
forgejo_runner_token = "test-runner-token"
forgejo_runner_labels = "ubuntu-latest:docker://node:20"
`
	cfg, err := Load(writeConfig(t, minimal))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	// Global string defaults
	if cfg.Global.LogLevel != "info" {
		t.Errorf("Global.LogLevel = %q, want info", cfg.Global.LogLevel)
	}
	if cfg.Global.LogFormat != "json" {
		t.Errorf("Global.LogFormat = %q, want json", cfg.Global.LogFormat)
	}
	if cfg.Global.HealthAddr != "" {
		t.Errorf("Global.HealthAddr = %q, want empty", cfg.Global.HealthAddr)
	}
	if cfg.Global.ListenAddr != ":8443" {
		t.Errorf("Global.ListenAddr = %q, want :8443", cfg.Global.ListenAddr)
	}
	if cfg.Global.MaxParallelTasks != 5 {
		t.Errorf("Global.MaxParallelTasks = %d, want 5", cfg.Global.MaxParallelTasks)
	}
	if cfg.Global.GitHubRef != "main" {
		t.Errorf("Global.GitHubRef = %q, want main", cfg.Global.GitHubRef)
	}
	if cfg.Global.GitHubAPIURL != "https://api.github.com" {
		t.Errorf("Global.GitHubAPIURL = %q, want https://api.github.com", cfg.Global.GitHubAPIURL)
	}

	// Instance defaults
	inst := cfg.Instances[0]
	if inst.Name != "forgejo.example.com" {
		t.Errorf("Instances[0].Name = %q, want host of forgejo_url", inst.Name)
	}
	if inst.ForgejoRunnerName != "forgery" {
		t.Errorf("Instances[0].ForgejoRunnerName = %q, want forgery", inst.ForgejoRunnerName)
	}
	if inst.PollInterval != 3*time.Second {
		t.Errorf("Instances[0].PollInterval = %v, want 3s", inst.PollInterval)
	}
	if inst.RegTokenTTL != 15*time.Minute {
		t.Errorf("Instances[0].RegTokenTTL = %v, want 15m", inst.RegTokenTTL)
	}
	if inst.GAStartupTimeout != 15*time.Minute {
		t.Errorf("Instances[0].GAStartupTimeout = %v, want 15m", inst.GAStartupTimeout)
	}
	if inst.HeartbeatInterval != 30*time.Second {
		t.Errorf("Instances[0].HeartbeatInterval = %v, want 30s", inst.HeartbeatInterval)
	}
	if inst.TLSInsecureSkipVerify != false {
		t.Errorf("Instances[0].TLSInsecureSkipVerify = %v, want false", inst.TLSInsecureSkipVerify)
	}
}

func TestInstanceStateDefaults(t *testing.T) {
	// Explicit name must win; unset name derives from forgejo_url host.
	doc := `
github_token = "ghp_test123"
github_repo = "owner/repo"
github_workflow_id = "forgery-runner.yml"

[[instances]]
name = "explicit-name"
forgejo_url = "https://forgejo.example.com:3000"
forgejo_runner_token = "token-a"
forgejo_runner_labels = "a"

[[instances]]
forgejo_url = "https://second.example.com"
forgejo_runner_token = "token-b"
forgejo_runner_labels = "b"
`
	cfg, err := Load(writeConfig(t, doc))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.Instances[0].Name != "explicit-name" {
		t.Errorf("Instances[0].Name = %q, want explicit-name", cfg.Instances[0].Name)
	}
	// Host of a URL with a port includes the port.
	if cfg.Instances[1].Name != "second.example.com" {
		t.Errorf("Instances[1].Name = %q, want second.example.com", cfg.Instances[1].Name)
	}
	if cfg.Instances[1].ForgejoRunnerName != "forgery" {
		t.Errorf("Instances[1].ForgejoRunnerName = %q, want forgery", cfg.Instances[1].ForgejoRunnerName)
	}
}

func TestPublicURLDefault(t *testing.T) {
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		hostname = "localhost"
	}

	minimal := `
github_token = "ghp_test123"
github_repo = "owner/repo"
github_workflow_id = "forgery-runner.yml"

[[instances]]
forgejo_url = "https://forgejo.example.com"
forgejo_runner_token = "test-runner-token"
forgejo_runner_labels = "ubuntu-latest:docker://node:20"
`
	// Default listen addr → default port 8443.
	cfg, err := Load(writeConfig(t, minimal))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	expected := "https://" + hostname + ":8443"
	if cfg.Global.PublicURL != expected {
		t.Errorf("Global.PublicURL = %q, want %q", cfg.Global.PublicURL, expected)
	}

	// Custom listen addr → port derived from it.
	withListen := `
github_token = "ghp_test123"
github_repo = "owner/repo"
github_workflow_id = "forgery-runner.yml"
listen_addr = ":9999"

[[instances]]
forgejo_url = "https://forgejo.example.com"
forgejo_runner_token = "test-runner-token"
forgejo_runner_labels = "ubuntu-latest:docker://node:20"
`
	cfg, err = Load(writeConfig(t, withListen))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	expected = "https://" + hostname + ":9999"
	if cfg.Global.PublicURL != expected {
		t.Errorf("Global.PublicURL = %q, want %q (derived from listen_addr)", cfg.Global.PublicURL, expected)
	}
}

func TestStateFileDefaultPath(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sub", "forgery.toml")
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	content := `
github_token = "ghp_test123"
github_repo = "owner/repo"
github_workflow_id = "forgery-runner.yml"

[[instances]]
forgejo_url = "https://forgejo.example.com"
forgejo_runner_token = "test-runner-token"
forgejo_runner_labels = "ubuntu-latest:docker://node:20"
`
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	expected := filepath.Join(dir, "sub", "forgery-state.json")
	if cfg.Global.StateFile != expected {
		t.Errorf("Global.StateFile = %q, want %q (relative to config dir)", cfg.Global.StateFile, expected)
	}

	// Explicit state_file wins.
	explicit := `
github_token = "ghp_test123"
github_repo = "owner/repo"
github_workflow_id = "forgery-runner.yml"
state_file = "/var/lib/forgery/state.json"

[[instances]]
forgejo_url = "https://forgejo.example.com"
forgejo_runner_token = "test-runner-token"
forgejo_runner_labels = "ubuntu-latest:docker://node:20"
`
	cfg, err = Load(writeConfig(t, explicit))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Global.StateFile != "/var/lib/forgery/state.json" {
		t.Errorf("Global.StateFile = %q, want explicit value", cfg.Global.StateFile)
	}
}

// ── Validation ───────────────────────────────────────────────────────────────

func TestRequiredFieldValidation(t *testing.T) {
	requiredFields := []string{
		"github_token",
		"github_repo",
		"github_workflow_id",
		"instances[0].forgejo_url",
		"instances[0].forgejo_runner_token",
		"instances[0].forgejo_runner_labels",
	}

	t.Run("all missing", func(t *testing.T) {
		_, err := Load(writeConfig(t, ""))
		if err == nil {
			t.Fatal("expected error for all missing fields")
		}

		missing := missingFields(err)
		if len(missing) != len(requiredFields)+1 {
			t.Errorf("got %d missing fields, want %d: %v", len(missing), len(requiredFields)+1, missing)
		}
		for _, f := range requiredFields {
			if !contains(missing, f) {
				t.Errorf("missing field %q not in error", f)
			}
		}
		// An empty file also lacks any [[instances]] entry.
		if !contains(missing, "instances") {
			t.Errorf("missing field %q not in error", "instances")
		}
	})

	t.Run("each individual field", func(t *testing.T) {
		for _, field := range requiredFields {
			t.Run(field, func(t *testing.T) {
				doc := validConfig()
				switch field {
				case "github_token":
					doc = strings.Replace(doc, "github_token = \"ghp_test123\"\n", "", 1)
				case "github_repo":
					doc = strings.Replace(doc, "github_repo = \"owner/repo\"\n", "", 1)
				case "github_workflow_id":
					doc = strings.Replace(doc, "github_workflow_id = \"forgery-runner.yml\"\n", "", 1)
				case "instances[0].forgejo_url":
					doc = strings.Replace(doc, "forgejo_url = \"https://forgejo.example.com\"\n", "", 1)
				case "instances[0].forgejo_runner_token":
					doc = strings.Replace(doc, "forgejo_runner_token = \"test-runner-token\"\n", "", 1)
				case "instances[0].forgejo_runner_labels":
					doc = strings.Replace(doc, "forgejo_runner_labels = \"ubuntu-latest:docker://node:20,docker:docker://catthehacker/ubuntu:act-latest\"\n", "", 1)
				}

				_, err := Load(writeConfig(t, doc))
				if err == nil {
					t.Fatalf("expected error for missing %s", field)
				}
				missing := missingFields(err)
				if len(missing) != 1 {
					t.Errorf("expected exactly 1 missing field, got %d: %v", len(missing), missing)
				}
				if len(missing) >= 1 && missing[0] != field {
					t.Errorf("missing field = %q, want %q", missing[0], field)
				}
			})
		}
	})

	t.Run("no instances array", func(t *testing.T) {
		doc := `
github_token = "ghp_test123"
github_repo = "owner/repo"
github_workflow_id = "forgery-runner.yml"
`
		_, err := Load(writeConfig(t, doc))
		if err == nil {
			t.Fatal("expected error when no [[instances]] is present")
		}
		missing := missingFields(err)
		if !contains(missing, "instances") {
			t.Errorf("missing fields = %v, want them to include instances", missing)
		}
	})

	t.Run("multiple fields missing", func(t *testing.T) {
		doc := `
github_token = "ghp_test123"

[[instances]]
forgejo_url = "https://forgejo.example.com"
`
		_, err := Load(writeConfig(t, doc))
		if err == nil {
			t.Fatal("expected error for multiple missing fields")
		}
		missing := missingFields(err)
		// github_repo, github_workflow_id, forgejo_runner_token,
		// forgejo_runner_labels.
		if len(missing) != 4 {
			t.Errorf("got %d missing fields, want 4: %v", len(missing), missing)
		}
	})
}

func TestDuplicateInstanceNameValidation(t *testing.T) {
	doc := validConfig() + `
[[instances]]
name = "test-runner"
forgejo_url = "https://second.example.com"
forgejo_runner_token = "second-token"
forgejo_runner_labels = "linux"
`
	_, err := Load(writeConfig(t, doc))
	if err == nil {
		t.Fatal("expected error for duplicate instance names")
	}
	invalid := invalidFields(err)
	if len(invalid) != 1 {
		t.Fatalf("got %d invalid fields, want 1: %v", len(invalid), invalid)
	}
	if !strings.Contains(invalid[0], "duplicate instance name") {
		t.Errorf("invalid field = %q, want duplicate instance name mention", invalid[0])
	}
}

// TestDefaultInstanceNameUniqueness verifies that two instances whose names
// default to the same host are rejected: uniqueness applies to defaults too.
func TestDefaultInstanceNameUniqueness(t *testing.T) {
	doc := `
github_token = "ghp_test123"
github_repo = "owner/repo"
github_workflow_id = "forgery-runner.yml"

[[instances]]
forgejo_url = "https://forgejo.example.com"
forgejo_runner_token = "token-a"
forgejo_runner_labels = "ubuntu-latest:docker://node:20"

[[instances]]
forgejo_url = "http://forgejo.example.com"
forgejo_runner_token = "token-b"
forgejo_runner_labels = "ubuntu-latest:docker://node:20"
`
	// Both instances omit `name`, so both default to the host of their URL.
	// Different schemes share the host part, so the names collide.
	_, err := Load(writeConfig(t, doc))
	if err == nil {
		t.Fatal("expected error when defaulted instance names collide")
	}
	invalid := invalidFields(err)
	found := false
	for _, f := range invalid {
		if strings.Contains(f, "duplicate instance name") {
			found = true
		}
	}
	if !found {
		t.Errorf("invalid fields = %v, want a duplicate instance name entry", invalid)
	}
}

func TestGitHubRequiredMissing(t *testing.T) {
	doc := `
[[instances]]
forgejo_url = "https://forgejo.example.com"
forgejo_runner_token = "test-runner-token"
forgejo_runner_labels = "ubuntu-latest:docker://node:20"
`
	_, err := Load(writeConfig(t, doc))
	if err == nil {
		t.Fatal("expected error when GitHub fields are missing")
	}
	missing := missingFields(err)
	for _, f := range []string{"github_token", "github_repo", "github_workflow_id"} {
		if !contains(missing, f) {
			t.Errorf("missing field %q not in error: %v", f, missing)
		}
	}
	if len(missing) != 3 {
		t.Errorf("got %d missing fields, want exactly 3: %v", len(missing), missing)
	}
}

// ── Strict parsing ───────────────────────────────────────────────────────────

func TestUnknownKeysError(t *testing.T) {
	tests := []struct {
		name string
		doc  string
		key  string
	}{
		{
			name: "top-level typo",
			doc: `
log_leve = "debug"
github_token = "ghp_test123"
github_repo = "owner/repo"
github_workflow_id = "forgery-runner.yml"

[[instances]]
forgejo_url = "https://forgejo.example.com"
forgejo_runner_token = "test-runner-token"
forgejo_runner_labels = "ubuntu-latest:docker://node:20"
`,
			key: "log_leve",
		},
		{
			name: "instance-level typo",
			doc: `
github_token = "ghp_test123"
github_repo = "owner/repo"
github_workflow_id = "forgery-runner.yml"

[[instances]]
forgejo_url = "https://forgejo.example.com"
forgejo_runner_token = "test-runner-token"
forgejo_runner_lables = "ubuntu-latest:docker://node:20"
`,
			key: "instances.forgejo_runner_lables",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Load(writeConfig(t, tt.doc))
			if err == nil {
				t.Fatal("expected error for unknown key")
			}
			if !strings.Contains(err.Error(), tt.key) {
				t.Errorf("error %q does not mention unknown key %q", err, tt.key)
			}
		})
	}
}

// TestRemovedDefaultContainerImageKey pins the breaking removal of the
// default_container_image instance key: it was dropped end-to-end (config →
// workflow_dispatch payload → workflow template) because nothing downstream
// consumed it. A config that still sets it must fail loudly as an unknown
// key instead of being silently ignored.
func TestRemovedDefaultContainerImageKey(t *testing.T) {
	doc := validConfig() + `
default_container_image = "docker://ghcr.io/catthehacker/ubuntu:act-latest"
`
	_, err := Load(writeConfig(t, doc))
	if err == nil {
		t.Fatal("expected error for removed key default_container_image")
	}
	if !strings.Contains(err.Error(), "unknown keys") {
		t.Errorf("error %q does not mention unknown keys", err)
	}
	if !strings.Contains(err.Error(), "default_container_image") {
		t.Errorf("error %q does not mention removed key default_container_image", err)
	}
}

func TestInvalidDurationError(t *testing.T) {
	// Unparseable duration string → TOML decode error.
	doc := `
github_token = "ghp_test123"
github_repo = "owner/repo"
github_workflow_id = "forgery-runner.yml"

[[instances]]
forgejo_url = "https://forgejo.example.com"
forgejo_runner_token = "test-runner-token"
forgejo_runner_labels = "ubuntu-latest:docker://node:20"
poll_interval = "not-a-duration"
`
	_, err := Load(writeConfig(t, doc))
	if err == nil {
		t.Fatal("expected error for invalid duration")
	}

	// Negative duration via integer → validation error listing the field.
	doc = strings.Replace(doc, "poll_interval = \"not-a-duration\"", "poll_interval = -3", 1)
	_, err = Load(writeConfig(t, doc))
	if err == nil {
		t.Fatal("expected error for negative duration")
	}
	invalid := invalidFields(err)
	if !contains(invalid, "instances[0].poll_interval (must be positive)") {
		t.Errorf("invalid fields = %v, want poll_interval listed", invalid)
	}
}

func TestInvalidIntError(t *testing.T) {
	// String into int field → TOML decode error.
	doc := `
max_parallel_tasks = "abc"
github_token = "ghp_test123"
github_repo = "owner/repo"
github_workflow_id = "forgery-runner.yml"

[[instances]]
forgejo_url = "https://forgejo.example.com"
forgejo_runner_token = "test-runner-token"
forgejo_runner_labels = "ubuntu-latest:docker://node:20"
`
	if _, err := Load(writeConfig(t, doc)); err == nil {
		t.Fatal("expected error for non-integer max_parallel_tasks")
	}

	// Negative int → validation error.
	doc = strings.Replace(doc, "max_parallel_tasks = \"abc\"", "max_parallel_tasks = -1", 1)
	_, err := Load(writeConfig(t, doc))
	if err == nil {
		t.Fatal("expected error for negative max_parallel_tasks")
	}
	invalid := invalidFields(err)
	if !contains(invalid, "max_parallel_tasks (must be positive)") {
		t.Errorf("invalid fields = %v, want max_parallel_tasks listed", invalid)
	}
}

func TestInvalidBoolError(t *testing.T) {
	doc := `
github_token = "ghp_test123"
github_repo = "owner/repo"
github_workflow_id = "forgery-runner.yml"

[[instances]]
forgejo_url = "https://forgejo.example.com"
forgejo_runner_token = "test-runner-token"
forgejo_runner_labels = "ubuntu-latest:docker://node:20"
tls_insecure_skip_verify = "yes"
`
	if _, err := Load(writeConfig(t, doc)); err == nil {
		t.Fatal("expected error for non-boolean tls_insecure_skip_verify")
	}
}

func TestMissingFileError(t *testing.T) {
	_, err := Load(filepath.Join(t.TempDir(), "does-not-exist.toml"))
	if err == nil {
		t.Fatal("expected error for missing config file")
	}
	if !strings.Contains(err.Error(), "does-not-exist.toml") {
		t.Errorf("error %q does not mention the file path", err)
	}
	if fields := missingFields(err); fields != nil {
		t.Errorf("MissingFields on read error should be nil, got %v", fields)
	}
}

// ── Labels ───────────────────────────────────────────────────────────────────

func TestLabelsParsing(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "simple comma-separated",
			input:    "a:docker://x,b",
			expected: []string{"a:docker://x", "b"},
		},
		{
			name:     "with whitespace",
			input:    "a:docker://x , b",
			expected: []string{"a:docker://x", "b"},
		},
		{
			name:     "with tabs and newlines",
			input:    "ubuntu-latest:docker://node:20,\tdocker:docker://catthehacker/ubuntu:act-latest",
			expected: []string{"ubuntu-latest:docker://node:20", "docker:docker://catthehacker/ubuntu:act-latest"},
		},
		{
			name:     "empty string",
			input:    "",
			expected: nil,
		},
		{
			name:     "only commas",
			input:    ",,,",
			expected: nil,
		},
		{
			name:     "single label",
			input:    "ubuntu-latest:docker://node:20",
			expected: []string{"ubuntu-latest:docker://node:20"},
		},
		{
			name:     "trailing comma",
			input:    "a,b,",
			expected: []string{"a", "b"},
		},
		{
			name:     "leading comma",
			input:    ",a,b",
			expected: []string{"a", "b"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parseLabels(tt.input)
			if !stringSlicesEqual(result, tt.expected) {
				t.Errorf("parseLabels(%q) = %v, want %v", tt.input, result, tt.expected)
			}
		})
	}
}

func TestLabelsDecodedFromTOML(t *testing.T) {
	doc := `
github_token = "ghp_test123"
github_repo = "owner/repo"
github_workflow_id = "forgery-runner.yml"

[[instances]]
forgejo_url = "https://forgejo.example.com"
forgejo_runner_token = "test-runner-token"
forgejo_runner_labels = "ubuntu-latest:docker://node:20 , docker:docker://catthehacker/ubuntu:act-latest"
`
	cfg, err := Load(writeConfig(t, doc))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	expected := []string{"ubuntu-latest:docker://node:20", "docker:docker://catthehacker/ubuntu:act-latest"}
	if !stringSlicesEqual(cfg.Instances[0].ForgejoRunnerLabels, expected) {
		t.Errorf("labels = %v, want %v", cfg.Instances[0].ForgejoRunnerLabels, expected)
	}
}

// ── Multi-instance ───────────────────────────────────────────────────────────

func TestMultiInstanceParsing(t *testing.T) {
	doc := `
github_token = "ghp_test123"
github_repo = "owner/repo"
github_workflow_id = "forgery-runner.yml"

[[instances]]
name = "primary"
forgejo_url = "https://forgejo-a.example.com"
forgejo_runner_token = "token-a"
forgejo_runner_labels = "ubuntu-latest:docker://node:20"
poll_interval = "1s"

[[instances]]
forgejo_url = "https://forgejo-b.example.com"
forgejo_runner_token = "token-b"
forgejo_runner_labels = "linux"
heartbeat_interval = "1m"
`
	cfg, err := Load(writeConfig(t, doc))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if len(cfg.Instances) != 2 {
		t.Fatalf("len(Instances) = %d, want 2", len(cfg.Instances))
	}

	a := cfg.Instances[0]
	if a.Name != "primary" {
		t.Errorf("Instances[0].Name = %q, want primary", a.Name)
	}
	if a.PollInterval != 1*time.Second {
		t.Errorf("Instances[0].PollInterval = %v, want 1s", a.PollInterval)
	}
	// Unset fields still get defaults.
	if a.HeartbeatInterval != 30*time.Second {
		t.Errorf("Instances[0].HeartbeatInterval = %v, want 30s", a.HeartbeatInterval)
	}

	b := cfg.Instances[1]
	if b.Name != "forgejo-b.example.com" {
		t.Errorf("Instances[1].Name = %q, want host default", b.Name)
	}
	if b.HeartbeatInterval != 1*time.Minute {
		t.Errorf("Instances[1].HeartbeatInterval = %v, want 1m", b.HeartbeatInterval)
	}
	if b.PollInterval != 3*time.Second {
		t.Errorf("Instances[1].PollInterval = %v, want 3s default", b.PollInterval)
	}
}

// ── API surface ──────────────────────────────────────────────────────────────

func TestMustLoad(t *testing.T) {
	// MustLoad should not exit when the config is valid.
	cfg := MustLoad(writeConfig(t, validConfig()))
	if cfg == nil {
		t.Fatal("MustLoad returned nil")
	}
	if len(cfg.Instances) != 1 {
		t.Fatalf("MustLoad Instances = %d, want 1", len(cfg.Instances))
	}
}

// Note: MustLoad with missing config logs via the slog default logger and
// calls os.Exit, which we can't easily test without replacing log/slog.
// That's tested implicitly via Load's error path.

func TestMissingFieldsHelper(t *testing.T) {
	// missingFields returns nil for non-validation errors.
	if fields := missingFields(errors.New("some other error")); fields != nil {
		t.Errorf("missingFields on plain error should return nil, got %v", fields)
	}

	// invalidFields returns nil for non-validation errors.
	if fields := invalidFields(errors.New("some other error")); fields != nil {
		t.Errorf("invalidFields on plain error should return nil, got %v", fields)
	}

	// Both work on validation errors.
	_, err := Load(writeConfig(t, "")) // empty file → validation error
	if err == nil {
		t.Fatal("expected error")
	}
	missing := missingFields(err)
	if len(missing) == 0 {
		t.Error("missingFields on validation error should return fields")
	}
	// All 7 required items: 3 global + instances + 3 instance fields.
	if len(missing) != 7 {
		t.Errorf("got %d missing fields, want 7: %v", len(missing), missing)
	}
}

func TestErrValidationError(t *testing.T) {
	e := &errValidation{missing: []string{"A", "B"}}
	if e.Error() != "missing required configuration: A, B" {
		t.Errorf("Error() = %q", e.Error())
	}

	e = &errValidation{invalid: []string{"C (must be positive)"}}
	if e.Error() != "invalid configuration values: C (must be positive)" {
		t.Errorf("Error() = %q", e.Error())
	}

	e = &errValidation{missing: []string{"A"}, invalid: []string{"B"}}
	want := "missing required configuration: A; invalid configuration values: B"
	if e.Error() != want {
		t.Errorf("Error() = %q, want %q", e.Error(), want)
	}
}

// TestExampleFileParses verifies that the checked-in example file at the
// repository root is valid TOML and passes validation.
func TestExampleFileParses(t *testing.T) {
	path := filepath.Join("..", "..", "forgery.toml.example")
	if _, err := os.Stat(path); err != nil {
		t.Skipf("example file not found: %v", err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("forgery.toml.example failed to load: %v", err)
	}
	if len(cfg.Instances) == 0 {
		t.Error("example file has no instances")
	}
}

// ── helpers ──

// missingFields extracts the list of missing required field names from an
// error, if the error is a validation error. Test-only counterpart of the
// errValidation accessors that used to live in the production package.
func missingFields(err error) []string {
	var ve *errValidation
	if errors.As(err, &ve) {
		return ve.missing
	}
	return nil
}

// invalidFields extracts the list of invalid value descriptions from an
// error, if the error is a validation error. Test-only counterpart of the
// errValidation accessors that used to live in the production package.
func invalidFields(err error) []string {
	var ve *errValidation
	if errors.As(err, &ve) {
		return ve.invalid
	}
	return nil
}

func stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func contains(slice []string, s string) bool {
	for _, item := range slice {
		if item == s {
			return true
		}
	}
	return false
}
