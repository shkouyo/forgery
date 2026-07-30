package config

import (
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// setAllRequired sets all required env vars to valid values.
func setAllRequired(t *testing.T) {
	t.Helper()
	t.Setenv("FORGEJO_URL", "https://forgejo.example.com")
	t.Setenv("FORGEJO_RUNNER_TOKEN", "test-runner-token")
	t.Setenv("FORGEJO_RUNNER_NAME", "test-runner")
	t.Setenv("FORGEJO_RUNNER_LABELS", "ubuntu-latest:docker://node:20,docker:docker://catthehacker/ubuntu:act-latest")
	t.Setenv("GITHUB_TOKEN", "ghp_test123")
	t.Setenv("GITHUB_REPO", "owner/repo")
	t.Setenv("GITHUB_WORKFLOW_ID", "forgery-runner.yml")
}

func TestLoadAllEnvVars(t *testing.T) {
	setAllRequired(t)
	t.Setenv("GITHUB_REF", "develop")
	t.Setenv("GITHUB_API_URL", "https://github.example.com/api")
	t.Setenv("LISTEN_ADDR", ":9999")
	t.Setenv("PUBLIC_URL", "https://forgery.example.com:8443")
	t.Setenv("DEFAULT_CONTAINER_IMAGE", "docker://ghcr.io/catthehacker/ubuntu:act-latest")
	t.Setenv("MAX_PARALLEL_TASKS", "10")
	t.Setenv("POLL_INTERVAL", "5s")
	t.Setenv("REG_TOKEN_TTL", "10m")
	t.Setenv("GA_STARTUP_TIMEOUT", "20m")
	t.Setenv("HEARTBEAT_INTERVAL", "45s")
	t.Setenv("PING_KEEPALIVE", "false")
	t.Setenv("LOG_LEVEL", "debug")
	t.Setenv("LOG_FORMAT", "text")
	t.Setenv("HEALTH_ADDR", ":8080")
	t.Setenv("TLS_INSECURE_SKIP_VERIFY", "true")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	// Forgejo connection
	if cfg.ForgejoURL != "https://forgejo.example.com" {
		t.Errorf("ForgejoURL = %q, want %q", cfg.ForgejoURL, "https://forgejo.example.com")
	}
	if cfg.ForgejoRunnerToken != "test-runner-token" {
		t.Errorf("ForgejoRunnerToken = %q", cfg.ForgejoRunnerToken)
	}
	if cfg.ForgejoRunnerName != "test-runner" {
		t.Errorf("ForgejoRunnerName = %q", cfg.ForgejoRunnerName)
	}
	expectedLabels := []string{
		"ubuntu-latest:docker://node:20",
		"docker:docker://catthehacker/ubuntu:act-latest",
	}
	if !stringSlicesEqual(cfg.ForgejoRunnerLabels, expectedLabels) {
		t.Errorf("ForgejoRunnerLabels = %v, want %v", cfg.ForgejoRunnerLabels, expectedLabels)
	}

	// GitHub Actions connection
	if cfg.GitHubToken != "ghp_test123" {
		t.Errorf("GitHubToken = %q", cfg.GitHubToken)
	}
	if cfg.GitHubRepo != "owner/repo" {
		t.Errorf("GitHubRepo = %q", cfg.GitHubRepo)
	}
	if cfg.GitHubWorkflowID != "forgery-runner.yml" {
		t.Errorf("GitHubWorkflowID = %q", cfg.GitHubWorkflowID)
	}
	if cfg.GitHubRef != "develop" {
		t.Errorf("GitHubRef = %q", cfg.GitHubRef)
	}
	if cfg.GHApiURL != "https://github.example.com/api" {
		t.Errorf("GHApiURL = %q", cfg.GHApiURL)
	}

	// Forgery server
	if cfg.ListenAddr != ":9999" {
		t.Errorf("ListenAddr = %q", cfg.ListenAddr)
	}
	if cfg.PublicURL != "https://forgery.example.com:8443" {
		t.Errorf("PublicURL = %q", cfg.PublicURL)
	}

	// Container
	if cfg.DefaultContainerImage != "docker://ghcr.io/catthehacker/ubuntu:act-latest" {
		t.Errorf("DefaultContainerImage = %q", cfg.DefaultContainerImage)
	}

	// Concurrency & timeouts
	if cfg.MaxParallelTasks != 10 {
		t.Errorf("MaxParallelTasks = %d, want 10", cfg.MaxParallelTasks)
	}
	if cfg.PollInterval != 5*time.Second {
		t.Errorf("PollInterval = %v, want 5s", cfg.PollInterval)
	}
	if cfg.RegTokenTTL != 10*time.Minute {
		t.Errorf("RegTokenTTL = %v, want 10m", cfg.RegTokenTTL)
	}
	if cfg.GAStartupTimeout != 20*time.Minute {
		t.Errorf("GAStartupTimeout = %v, want 20m", cfg.GAStartupTimeout)
	}
	if cfg.HeartbeatInterval != 45*time.Second {
		t.Errorf("HeartbeatInterval = %v, want 45s", cfg.HeartbeatInterval)
	}
	if cfg.PingKeepalive != false {
		t.Errorf("PingKeepalive = %v, want false", cfg.PingKeepalive)
	}

	// Observability
	if cfg.LogLevel != "debug" {
		t.Errorf("LogLevel = %q", cfg.LogLevel)
	}
	if cfg.LogFormat != "text" {
		t.Errorf("LogFormat = %q", cfg.LogFormat)
	}
	if cfg.HealthAddr != ":8080" {
		t.Errorf("HealthAddr = %q", cfg.HealthAddr)
	}

	// TLS
	if cfg.TLSInsecureSkipVerify != true {
		t.Errorf("TLSInsecureSkipVerify = %v, want true", cfg.TLSInsecureSkipVerify)
	}
}

func TestDefaults(t *testing.T) {
	setAllRequired(t)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	// String defaults
	if cfg.GitHubRef != "main" {
		t.Errorf("GitHubRef = %q, want %q", cfg.GitHubRef, "main")
	}
	if cfg.GHApiURL != "https://api.github.com" {
		t.Errorf("GHApiURL = %q", cfg.GHApiURL)
	}
	if cfg.ListenAddr != ":8443" {
		t.Errorf("ListenAddr = %q", cfg.ListenAddr)
	}
	// PublicURL should be set from hostname
	if cfg.PublicURL == "" {
		t.Error("PublicURL should not be empty (default from hostname)")
	}
	if !strings.HasPrefix(cfg.PublicURL, "https://") {
		t.Errorf("PublicURL = %q, should start with https://", cfg.PublicURL)
	}

	// Numeric defaults
	if cfg.MaxParallelTasks != 5 {
		t.Errorf("MaxParallelTasks = %d, want 5", cfg.MaxParallelTasks)
	}
	if cfg.PollInterval != 3*time.Second {
		t.Errorf("PollInterval = %v, want 3s", cfg.PollInterval)
	}
	if cfg.RegTokenTTL != 15*time.Minute {
		t.Errorf("RegTokenTTL = %v, want 15m", cfg.RegTokenTTL)
	}
	if cfg.GAStartupTimeout != 15*time.Minute {
		t.Errorf("GAStartupTimeout = %v, want 15m", cfg.GAStartupTimeout)
	}
	if cfg.HeartbeatInterval != 30*time.Second {
		t.Errorf("HeartbeatInterval = %v, want 30s", cfg.HeartbeatInterval)
	}

	// Bool defaults
	if cfg.PingKeepalive != true {
		t.Errorf("PingKeepalive = %v, want true", cfg.PingKeepalive)
	}
	if cfg.TLSInsecureSkipVerify != false {
		t.Errorf("TLSInsecureSkipVerify = %v, want false", cfg.TLSInsecureSkipVerify)
	}

	// Observability defaults
	if cfg.LogLevel != "info" {
		t.Errorf("LogLevel = %q, want %q", cfg.LogLevel, "info")
	}
	if cfg.LogFormat != "json" {
		t.Errorf("LogFormat = %q, want %q", cfg.LogFormat, "json")
	}
	if cfg.HealthAddr != "" {
		t.Errorf("HealthAddr = %q, want %q", cfg.HealthAddr, "")
	}

	// Optional fields should remain empty
	if cfg.DefaultContainerImage != "" {
		t.Errorf("DefaultContainerImage = %q, want empty", cfg.DefaultContainerImage)
	}
}

func TestDotenvFallback(t *testing.T) {
	// Save env vars that the .env file will set, so we can restore them after.
	cleanup := saveEnv(
		"FORGEJO_URL", "FORGEJO_RUNNER_TOKEN", "FORGEJO_RUNNER_NAME",
		"FORGEJO_RUNNER_LABELS", "GITHUB_TOKEN", "GITHUB_REPO",
		"GITHUB_WORKFLOW_ID", "GITHUB_REF", "LISTEN_ADDR",
	)
	defer cleanup()

	// Create a temporary directory with a .env file.
	dir := t.TempDir()
	envContent := "FORGEJO_URL=https://forgejo.from.env\n" +
		"FORGEJO_RUNNER_TOKEN=env-token\n" +
		"FORGEJO_RUNNER_NAME=env-runner\n" +
		"FORGEJO_RUNNER_LABELS=env:docker://env\n" +
		"GITHUB_TOKEN=env-gh-token\n" +
		"GITHUB_REPO=env/repo\n" +
		"GITHUB_WORKFLOW_ID=env-workflow.yml\n" +
		"GITHUB_REF=env-branch\n" +
		"LISTEN_ADDR=:7777\n"

	if err := os.WriteFile(filepath.Join(dir, ".env"), []byte(envContent), 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	// Change to the temp directory so godotenv.Load() finds .env.
	origDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	defer func() { _ = os.Chdir(origDir) }()
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("Chdir: %v", err)
	}

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.ForgejoURL != "https://forgejo.from.env" {
		t.Errorf("ForgejoURL = %q, want from .env", cfg.ForgejoURL)
	}
	if cfg.ListenAddr != ":7777" {
		t.Errorf("ListenAddr = %q, want from .env", cfg.ListenAddr)
	}
	if cfg.GitHubRef != "env-branch" {
		t.Errorf("GitHubRef = %q, want from .env", cfg.GitHubRef)
	}
}

func TestEnvOverridesDotenv(t *testing.T) {
	// Save env vars that the .env file will set.
	cleanup := saveEnv("LISTEN_ADDR", "LOG_LEVEL")
	defer cleanup()

	dir := t.TempDir()
	envContent := "LISTEN_ADDR=:7777\n" +
		"LOG_LEVEL=debug\n"

	if err := os.WriteFile(filepath.Join(dir, ".env"), []byte(envContent), 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	// Set environment variables that should override .env.
	// Note: we need ALL required vars set via env since we're testing overrides.
	setAllRequired(t)
	t.Setenv("LISTEN_ADDR", ":8888") // overrides .env
	// LOG_LEVEL is NOT set via t.Setenv, so it should come from .env

	origDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	defer func() { _ = os.Chdir(origDir) }()
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("Chdir: %v", err)
	}

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	// Environment variable should take precedence over .env.
	if cfg.ListenAddr != ":8888" {
		t.Errorf("ListenAddr = %q, want :8888 (env should override .env)", cfg.ListenAddr)
	}

	// LOG_LEVEL was in .env but not set via env — godotenv should have loaded it.
	if cfg.LogLevel != "debug" {
		t.Errorf("LogLevel = %q, want debug from .env", cfg.LogLevel)
	}
}

func TestRequiredFieldValidation(t *testing.T) {
	requiredFields := []string{
		"FORGEJO_URL",
		"FORGEJO_RUNNER_TOKEN",
		"FORGEJO_RUNNER_NAME",
		"FORGEJO_RUNNER_LABELS",
		"GITHUB_TOKEN",
		"GITHUB_REPO",
		"GITHUB_WORKFLOW_ID",
	}

	t.Run("all missing", func(t *testing.T) {
		_, err := Load()
		if err == nil {
			t.Fatal("expected error for all missing fields")
		}

		missing := MissingFields(err)
		if len(missing) != len(requiredFields) {
			t.Errorf("got %d missing fields, want %d: %v", len(missing), len(requiredFields), missing)
		}
		for _, f := range requiredFields {
			if !contains(missing, f) {
				t.Errorf("missing field %q not in error", f)
			}
		}
	})

	t.Run("each individual field", func(t *testing.T) {
		for _, field := range requiredFields {
			t.Run(field, func(t *testing.T) {
				// Set all required fields except the one being tested.
				for _, f := range requiredFields {
					if f == field {
						continue
					}
					switch f {
					case "FORGEJO_RUNNER_LABELS":
						t.Setenv(f, "test:docker://test")
					default:
						t.Setenv(f, "test-value")
					}
				}
				// Intentionally leave the target field unset.

				_, err := Load()
				if err == nil {
					t.Fatalf("expected error for missing %s", field)
				}

				missing := MissingFields(err)
				if len(missing) != 1 {
					t.Errorf("expected exactly 1 missing field, got %d: %v", len(missing), missing)
				}
				if len(missing) >= 1 && missing[0] != field {
					t.Errorf("missing field = %q, want %q", missing[0], field)
				}
			})
		}
	})

	t.Run("multiple fields missing", func(t *testing.T) {
		// Set only FORGEJO_URL and GITHUB_TOKEN — 5 fields missing.
		t.Setenv("FORGEJO_URL", "https://f.example.com")
		t.Setenv("GITHUB_TOKEN", "ghp_token")

		_, err := Load()
		if err == nil {
			t.Fatal("expected error for multiple missing fields")
		}

		missing := MissingFields(err)
		if len(missing) != 5 {
			t.Errorf("got %d missing fields, want 5: %v", len(missing), missing)
		}
	})
}

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
			input:    "ubuntu-latest:docker://node:20,	docker:docker://catthehacker/ubuntu:act-latest",
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

func TestDurationParsing(t *testing.T) {
	tests := []struct {
		name     string
		envValue string
		setEnv   bool
		field    string
		expected time.Duration
	}{
		{"5s", "5s", true, "POLL_INTERVAL", 5 * time.Second},
		{"1m", "1m", true, "POLL_INTERVAL", 1 * time.Minute},
		{"500ms", "500ms", true, "POLL_INTERVAL", 500 * time.Millisecond},
		{"2h", "2h", true, "REG_TOKEN_TTL", 2 * time.Hour},
		{"zero/empty", "", false, "POLL_INTERVAL", 3 * time.Second}, // default
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setAllRequired(t)
			if tt.setEnv {
				t.Setenv(tt.field, tt.envValue)
			}

			cfg, err := Load()
			if err != nil {
				t.Fatalf("Load() error = %v", err)
			}

			var got time.Duration
			switch tt.field {
			case "POLL_INTERVAL":
				got = cfg.PollInterval
			case "REG_TOKEN_TTL":
				got = cfg.RegTokenTTL
			case "GA_STARTUP_TIMEOUT":
				got = cfg.GAStartupTimeout
			case "HEARTBEAT_INTERVAL":
				got = cfg.HeartbeatInterval
			}

			if got != tt.expected {
				t.Errorf("%s = %v, want %v", tt.field, got, tt.expected)
			}
		})
	}
}

func TestInvalidDurationFallsBackToDefault(t *testing.T) {
	setAllRequired(t)
	t.Setenv("POLL_INTERVAL", "not-a-duration")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.PollInterval != 3*time.Second {
		t.Errorf("PollInterval = %v, want 3s (default after invalid parse)", cfg.PollInterval)
	}
}

func TestBoolParsing(t *testing.T) {
	tests := []struct {
		name     string
		envValue string
		expected bool
	}{
		{"true", "true", true},
		{"1", "1", true},
		{"false", "false", false},
		{"0", "0", false},
		{"empty string (default)", "", true}, // PingKeepalive default is true
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setAllRequired(t)
			if tt.envValue != "" {
				t.Setenv("PING_KEEPALIVE", tt.envValue)
			}

			cfg, err := Load()
			if err != nil {
				t.Fatalf("Load() error = %v", err)
			}

			if cfg.PingKeepalive != tt.expected {
				t.Errorf("PingKeepalive = %v, want %v (env=%q)", cfg.PingKeepalive, tt.expected, tt.envValue)
			}
		})
	}
}

func TestBoolParsingExplicitFalse(t *testing.T) {
	setAllRequired(t)
	t.Setenv("PING_KEEPALIVE", "false")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.PingKeepalive != false {
		t.Errorf("PingKeepalive = %v, want false (explicitly set to false)", cfg.PingKeepalive)
	}
}

func TestBoolParsingTLS(t *testing.T) {
	setAllRequired(t)

	// Default should be false
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.TLSInsecureSkipVerify != false {
		t.Error("TLSInsecureSkipVerify should default to false")
	}

	// Explicitly true
	t.Setenv("TLS_INSECURE_SKIP_VERIFY", "true")
	cfg, err = Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.TLSInsecureSkipVerify != true {
		t.Error("TLSInsecureSkipVerify should be true when set")
	}
}

func TestIntParsing(t *testing.T) {
	tests := []struct {
		name     string
		envValue string
		expected int
	}{
		{"valid", "10", 10},
		{"zero", "0", 5},     // falls back to default because 0 is zero-value
		{"negative", "-1", 5}, // falls back to default
		{"empty", "", 5},
		{"invalid", "abc", 5},
		{"large", "100", 100},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setAllRequired(t)
			if tt.envValue != "" {
				t.Setenv("MAX_PARALLEL_TASKS", tt.envValue)
			}

			cfg, err := Load()
			if err != nil {
				t.Fatalf("Load() error = %v", err)
			}

			if cfg.MaxParallelTasks != tt.expected {
				t.Errorf("MaxParallelTasks = %d, want %d (env=%q)", cfg.MaxParallelTasks, tt.expected, tt.envValue)
			}
		})
	}
}

func TestPublicURLDefault(t *testing.T) {
	setAllRequired(t)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = "localhost"
	}
	expected := "https://" + hostname + ":8443"

	if cfg.PublicURL != expected {
		t.Errorf("PublicURL = %q, want %q", cfg.PublicURL, expected)
	}
}

func TestMustLoad(t *testing.T) {
	// MustLoad should not panic when all required vars are set.
	setAllRequired(t)
	cfg := MustLoad(slog.Default())
	if cfg == nil {
		t.Fatal("MustLoad returned nil")
	}
}

// Note: MustLoad with missing config uses the provided logger + os.Exit,
// which we can't easily test without replacing log/slog. That's tested
// implicitly via Load's error path.

func TestMissingFieldsHelper(t *testing.T) {
	// Test that MissingFields returns nil for non-validation errors.
	if fields := MissingFields(errors.New("some other error")); fields != nil {
		t.Errorf("MissingFields on plain error should return nil, got %v", fields)
	}

	// Test that MissingFields works with validation errors.
	_, err := Load() // no env set → validation error
	if err == nil {
		t.Fatal("expected error")
	}
	fields := MissingFields(err)
	if len(fields) == 0 {
		t.Error("MissingFields on validation error should return fields")
	}
	// Should contain all 7 required fields
	if len(fields) != 7 {
		t.Errorf("got %d missing fields, want 7: %v", len(fields), fields)
	}
}

func TestParseLabelsEdgeCases(t *testing.T) {
	// Whitespace-only elements should be stripped.
	result := parseLabels("  a  ,  b  ,  ")
	if !stringSlicesEqual(result, []string{"a", "b"}) {
		t.Errorf("got %v, want [a b]", result)
	}

	// Mixed with newlines.
	result = parseLabels("a\n,b\t,c")
	if !stringSlicesEqual(result, []string{"a", "b", "c"}) {
		t.Errorf("got %v, want [a b c]", result)
	}
}

func TestEnvVarPrecedence(t *testing.T) {
	// Test that explicitly-set env vars take effect.
	setAllRequired(t)
	t.Setenv("LOG_LEVEL", "warn")
	t.Setenv("MAX_PARALLEL_TASKS", "7")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.LogLevel != "warn" {
		t.Errorf("LogLevel = %q, want warn", cfg.LogLevel)
	}
	if cfg.MaxParallelTasks != 7 {
		t.Errorf("MaxParallelTasks = %d, want 7", cfg.MaxParallelTasks)
	}
}

func TestGHApiURLDefault(t *testing.T) {
	setAllRequired(t)
	// Don't set GITHUB_API_URL.

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.GHApiURL != "https://api.github.com" {
		t.Errorf("GHApiURL = %q, want https://api.github.com", cfg.GHApiURL)
	}
}

func TestAllFieldsPopulatedRoundTrip(t *testing.T) {
	setAllRequired(t)
	t.Setenv("GITHUB_REF", "feature/xyz")
	t.Setenv("GITHUB_API_URL", "https://ghe.example.com/api/v3")
	t.Setenv("LISTEN_ADDR", ":4433")
	t.Setenv("PUBLIC_URL", "https://forgery.custom.com")
	t.Setenv("DEFAULT_CONTAINER_IMAGE", "docker://ubuntu:22.04")
	t.Setenv("MAX_PARALLEL_TASKS", "8")
	t.Setenv("POLL_INTERVAL", "10s")
	t.Setenv("REG_TOKEN_TTL", "30m")
	t.Setenv("GA_STARTUP_TIMEOUT", "25m")
	t.Setenv("HEARTBEAT_INTERVAL", "60s")
	t.Setenv("PING_KEEPALIVE", "false")
	t.Setenv("LOG_LEVEL", "error")
	t.Setenv("LOG_FORMAT", "text")
	t.Setenv("HEALTH_ADDR", ":8086")
	t.Setenv("TLS_INSECURE_SKIP_VERIFY", "true")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	// Spot-check a few representative fields.
	checks := []struct {
		field string
		got   any
		want  any
	}{
		{"ForgejoURL", cfg.ForgejoURL, "https://forgejo.example.com"},
		{"GitHubRef", cfg.GitHubRef, "feature/xyz"},
		{"GHApiURL", cfg.GHApiURL, "https://ghe.example.com/api/v3"},
		{"ListenAddr", cfg.ListenAddr, ":4433"},
		{"PublicURL", cfg.PublicURL, "https://forgery.custom.com"},
		{"DefaultContainerImage", cfg.DefaultContainerImage, "docker://ubuntu:22.04"},
		{"MaxParallelTasks", cfg.MaxParallelTasks, 8},
		{"PollInterval", cfg.PollInterval, 10 * time.Second},
		{"RegTokenTTL", cfg.RegTokenTTL, 30 * time.Minute},
		{"GAStartupTimeout", cfg.GAStartupTimeout, 25 * time.Minute},
		{"HeartbeatInterval", cfg.HeartbeatInterval, 60 * time.Second},
		{"PingKeepalive", cfg.PingKeepalive, false},
		{"LogLevel", cfg.LogLevel, "error"},
		{"LogFormat", cfg.LogFormat, "text"},
		{"HealthAddr", cfg.HealthAddr, ":8086"},
		{"TLSInsecureSkipVerify", cfg.TLSInsecureSkipVerify, true},
	}

	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %v, want %v", c.field, c.got, c.want)
		}
	}
}

func TestParseInt(t *testing.T) {
	tests := []struct {
		input    string
		expected int
	}{
		{"", 0},
		{"0", 0},
		{"5", 5},
		{"-1", -1},
		{"abc", 0},
		{"100", 100},
	}

	for _, tt := range tests {
		result := parseInt(tt.input)
		if result != tt.expected {
			t.Errorf("parseInt(%q) = %d, want %d", tt.input, result, tt.expected)
		}
	}
}

func TestParseDuration(t *testing.T) {
	tests := []struct {
		input    string
		expected time.Duration
	}{
		{"", 0},
		{"1s", time.Second},
		{"5m", 5 * time.Minute},
		{"3h", 3 * time.Hour},
		{"500ms", 500 * time.Millisecond},
		{"invalid", 0},
	}

	for _, tt := range tests {
		result := parseDuration(tt.input)
		if result != tt.expected {
			t.Errorf("parseDuration(%q) = %v, want %v", tt.input, result, tt.expected)
		}
	}
}

func TestParseBool(t *testing.T) {
	tests := []struct {
		input    string
		expected bool
	}{
		{"", false},
		{"true", true},
		{"false", false},
		{"1", true},
		{"0", false},
		{"TRUE", true},
		{"FALSE", false},
		{"t", true},
		{"f", false},
		{"invalid", false},
	}

	for _, tt := range tests {
		result := parseBool(tt.input)
		if result != tt.expected {
			t.Errorf("parseBool(%q) = %v, want %v", tt.input, result, tt.expected)
		}
	}
}

func TestErrMissingFieldsError(t *testing.T) {
	e := &errMissingFields{Fields: []string{"A", "B"}}
	if e.Error() != "missing required configuration: A, B" {
		t.Errorf("Error() = %q", e.Error())
	}
}

// ── helpers ──

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

// saveEnv saves the current values of the given environment variables
// and returns a function that restores them. Used to prevent dotenv tests
// from leaking env vars across tests.
func saveEnv(keys ...string) func() {
	saved := make(map[string]string, len(keys))
	for _, k := range keys {
		saved[k] = os.Getenv(k)
	}
	return func() {
		for k, v := range saved {
			if v == "" {
				_ = os.Unsetenv(k)
			} else {
				_ = os.Setenv(k, v)
			}
		}
	}
}
// Ensure strconv import is used (it's used in tests via the strconv package
// but we import it just in case we add future tests). The blank identifier
// ensures the import stays.
var _ = strconv.Quote
