**English** | [简体中文](README.zh.md)

# forgery — A Forgejo Actions runner proxy that executes jobs on GitHub Actions

forgery is a Go daemon that acts as a **bidirectional gRPC proxy** between
Forgejo and GitHub Actions. It looks like a normal Forgejo Actions runner
to your Forgejo instance — it registers, polls for tasks, and reports
results — but it doesn't actually execute jobs locally. Instead, it
delegates execution to a GitHub Actions Workflow, where a real
`forgejo-runner` spins up, connects back to forgery, and runs the job.
Logs and status updates flow back through forgery to Forgejo.

```
                        gRPC (Connect)                            HTTPS
  ┌──────────┐  Register/Declare/FetchTask  ┌──────────┐  workflow_dispatch  ┌──────────────┐
  │ Forgejo  │ ←──────────────────────────→ │ forgery  │ ──────────────────→ │  GitHub      │
  │ Instance │  UpdateTask/UpdateLog        │  (Go)    │                     │  Actions     │
  └──────────┘                              └────┬─────┘                     │  Workflow    │
                                                 │                           │              │
                                                 │  gRPC (Connect)           │ ┌──────────┐ │
                                                 │  Register/Declare/        │ │forgejo-   │ │
                                                 │  FetchTask                │ │runner     │ │
                                                 └───────────────────────────→│(one-job)   │ │
                                                    UpdateTask/UpdateLog      │ │(ephemeral) │ │
                                                                              │ └──────────┘ │
                                                                              └──────────────┘
```

## How It Works

1. **Forgejo dispatches a job** — forgery, registered as a runner, receives it
   via `FetchTask`.
2. **forgery triggers a GitHub Actions Workflow** — it calls the
   `workflow_dispatch` API, passing task metadata (proxy URL, one-time
   registration token, labels, container image).
3. **The GA Workflow starts a real `forgejo-runner`** — it downloads the
   official runner binary and launches it in `one-job` ephemeral mode,
   connecting back to forgery's gRPC endpoint.
4. **forgery hands the real Forgejo task to the internal runner** — the
   runner fetches the task, executes it inside a container, and reports
   results.
5. **Logs and task status flow back** — `UpdateTask` and `UpdateLog` RPCs
   are transparently relayed through forgery to Forgejo.

## Quick Start

### 1. Clone and Build

```sh
git clone https://git.0x0f.dev/forgery
cd forgery
go build ./cmd/forgery
```

Prerequisites: Go 1.22+.

### 2. Copy the Workflow Template

Copy `templates/forgery-runner.yml` to your GitHub repository's
`.github/workflows/` directory:

```sh
cp templates/forgery-runner.yml /path/to/your/repo/.github/workflows/
```

### 3. Set Up Environment Variables

Create a `.env` file in the forgery working directory (or export them in
your shell). At minimum you need:

```env
# ── Forgejo connection ──
FORGEJO_URL=https://forgejo.example.com
FORGEJO_RUNNER_TOKEN=your-runner-registration-token
FORGEJO_RUNNER_NAME=forgery-proxy
FORGEJO_RUNNER_LABELS=ubuntu-latest:docker://node:20-bookworm,docker:docker://ghcr.io/catthehacker/ubuntu:act-latest

# ── GitHub Actions connection ──
GITHUB_TOKEN=github_pat_...
GITHUB_REPO=your-org/your-repo
GITHUB_WORKFLOW_ID=forgery-runner.yml

# ── Optional but recommended ──
PUBLIC_URL=https://forgery.example.com
DEFAULT_CONTAINER_IMAGE=docker://ghcr.io/catthehacker/ubuntu:act-latest
```

See [Configuration](#configuration) for the complete reference.

### 4. Run forgery

```sh
./forgery
```

forgery loads configuration from `.env` (if present), then from the
environment. Environment variables take precedence over `.env` values.

### 5. Configure a Reverse Proxy

forgery listens on `:8443` for plain HTTP by default. You should place a
TLS-terminating reverse proxy in front of it. See [Reverse Proxy
Setup](#reverse-proxy-setup) for examples.

## Configuration

forgery is configured entirely through environment variables (with an
optional `.env` file as a fallback). The loading order is:

1. `.env` file (silently skipped if missing)
2. OS environment variables (override `.env`)
3. Default values for unset fields
4. Validation of required fields

### Complete Environment Variable Reference

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `FORGEJO_URL` | Yes | — | Forgejo instance base URL (e.g. `https://forgejo.example.com`) |
| `FORGEJO_RUNNER_TOKEN` | Yes | — | Forgejo runner registration token |
| `FORGEJO_RUNNER_NAME` | Yes | — | Display name shown in Forgejo's runner list |
| `FORGEJO_RUNNER_LABELS` | Yes | — | Comma-separated labels with optional container image mappings (e.g. `ubuntu-latest:docker://node:20-bookworm,docker:docker://ghcr.io/catthehacker/ubuntu:act-latest`) |
| `GITHUB_TOKEN` | Yes | — | GitHub personal access token with `workflow` scope (fine-grained, repo-scoped recommended) |
| `GITHUB_REPO` | Yes | — | Repository holding the workflow, in `owner/repo` format |
| `GITHUB_WORKFLOW_ID` | Yes | — | Workflow filename (e.g. `forgery-runner.yml`) or workflow ID |
| `GITHUB_REF` | No | `main` | Branch ref to trigger the workflow on |
| `GITHUB_API_URL` | No | `https://api.github.com` | GitHub API base URL (set for GitHub Enterprise) |
| `LISTEN_ADDR` | No | `:8443` | Southbound gRPC listen address |
| `PUBLIC_URL` | No | `https://<hostname>:8443` | Publicly reachable URL for forgery (used by internal runners to connect back) |
| `DEFAULT_CONTAINER_IMAGE` | No | — | Default container image for labels without an explicit `:docker://...` mapping |
| `MAX_PARALLEL_TASKS` | No | `5` | Maximum number of concurrently executing tasks |
| `POLL_INTERVAL` | No | `3s` | Interval between northbound `FetchTask` polls |
| `REG_TOKEN_TTL` | No | `15m` | Lifetime of a one-time registration token |
| `GA_STARTUP_TIMEOUT` | No | `15m` | Maximum time to wait for a GA Workflow to start and the internal runner to register |
| `HEARTBEAT_INTERVAL` | No | `30s` | Interval for sending `UpdateTask(state=running)` heartbeats while waiting for the internal runner |
| `PING_KEEPALIVE` | No | `true` | Send Ping RPCs during backpressure to keep the Forgejo connection alive |
| `LOG_LEVEL` | No | `info` | Log level: `debug`, `info`, `warn`, or `error` |
| `LOG_FORMAT` | No | `json` | Log output format: `json` or `text` |
| `METRICS_ADDR` | No | `:9090` | Prometheus `/metrics` endpoint listen address |
| `HEALTH_ADDR` | No | — | Health check endpoint listen address (if set, exposes `/healthz` and `/readyz`) |
| `TLS_INSECURE_SKIP_VERIFY` | No | `false` | Skip TLS certificate verification for northbound connections (development only) |

## Reverse Proxy Setup

forgery's southbound gRPC endpoint listens for plain HTTP on
`LISTEN_ADDR` (default `:8443`). In production, you **must** place a
TLS-terminating reverse proxy in front of it.

### Recommended Topology

```
Internet (HTTPS)
    │
    ▼
┌──────────────────┐
│  Caddy / nginx   │  ← TLS termination (Let's Encrypt / custom cert)
│  :443            │
└────────┬─────────┘
         │  HTTP (plain)
         ▼
┌──────────────────┐
│  forgery         │
│  :8443           │  ← listen only on localhost or internal network
└──────────────────┘
```

### Caddy

```
forgery.example.com {
    reverse_proxy localhost:8443
}
```

### nginx

```nginx
server {
    listen 443 ssl;
    server_name forgery.example.com;

    ssl_certificate     /etc/ssl/certs/forgery.example.com.pem;
    ssl_certificate_key /etc/ssl/private/forgery.example.com.key;

    location / {
        proxy_pass http://localhost:8443;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host $host;
    }
}
```

Make sure `PUBLIC_URL` matches the HTTPS URL of your reverse proxy.

## Security Considerations

### Northbound (forgery → Forgejo)

- forgery connects to Forgejo over **HTTPS**. Ensure `FORGEJO_URL` uses
  `https://`.
- For development with self-signed certificates, set
  `TLS_INSECURE_SKIP_VERIFY=true`.

### Southbound (internal runner → forgery)

- TLS is **terminated at the reverse proxy** (Caddy or nginx). The proxy
  handles certificate management (e.g. Let's Encrypt).
- Internal `forgejo-runner` instances connect to `PUBLIC_URL` (which
  should be `https://`), trusting standard public CAs.

### One-Time Registration Tokens

- Each task dispatch generates a unique registration token via
  `crypto/rand` (32 bytes, 256 bits of entropy).
- Tokens are **single-use** — the first successful `Register` call
  consumes the token. Subsequent attempts are rejected.
- Tokens expire after `REG_TOKEN_TTL` (default 15 minutes).
- **WARNING:** `reg_token` is passed as a `workflow_dispatch` input and
  is **visible in GitHub Actions workflow logs**. The token is useless
  after first use, but restrict access to Actions logs on the Forgejo
  side as an additional precaution.

### Secrets Handling

- Forgejo repository secrets from incoming tasks are held **only in
  memory** and forwarded over TLS connections. They are never written to
  disk or logged.

### GitHub PAT

- `GITHUB_TOKEN` should be a **fine-grained personal access token**
  scoped to a single repository.
- Minimum required permissions: `contents: read` and `actions: write`.
- The built-in `GITHUB_TOKEN` in Actions workflows is used by the runner
  workflow itself and requires no extra configuration.

## Labels and Container Images

forgery uses a label-to-container-image mapping to route Forgejo job
requests to appropriate execution environments.

### Format

```
label1:docker://image1,label2:docker://image2
```

- The part before `:` is the **runs-on label** that Forgejo workflow
  jobs match against.
- The part after `:` is the **container image URL** prefixed with
  `docker://`.

### Example

```env
FORGEJO_RUNNER_LABELS=ubuntu-latest:docker://node:20-bookworm,docker:docker://ghcr.io/catthehacker/ubuntu:act-latest
DEFAULT_CONTAINER_IMAGE=docker://ghcr.io/catthehacker/ubuntu:act-latest
```

- When a Forgejo workflow specifies `runs-on: ubuntu-latest`, forgery
  matches it and the internal runner executes in `node:20-bookworm`.
- When `runs-on: docker`, it executes in
  `ghcr.io/catthehacker/ubuntu:act-latest`.
- If a `runs-on` label has no explicit `:docker://...` suffix in
  `FORGEJO_RUNNER_LABELS`, the `DEFAULT_CONTAINER_IMAGE` is used.

The same label string is used for both northbound registration (so
Forgejo knows which tasks to route to forgery) and the internal runner
configuration (so the GA workflow knows which container to use).

## GitHub Actions Setup

### 1. Add the Workflow

Copy `templates/forgery-runner.yml` to `.github/workflows/` in the
repository specified by `GITHUB_REPO`:

```sh
cp templates/forgery-runner.yml path/to/your-repo/.github/workflows/
```

### 2. Set the Runner Version

The workflow uses `$FORGEJO_RUNNER_VERSION` to determine which
`forgejo-runner` binary to download. Set this as a **GitHub Actions
variable** in your repository (Settings → Secrets and variables →
Actions → Variables):

| Variable | Value |
|----------|-------|
| `FORGEJO_RUNNER_VERSION` | `v12.8.0` (default if not set) |

Keep this in sync with the Forgejo runner version your Forgejo instance
expects.

### 3. Built-in `GITHUB_TOKEN`

The `GITHUB_TOKEN` used inside the workflow is the standard
[automatic token](https://docs.github.com/en/actions/security-guides/automatic-token-authentication)
provided by GitHub Actions. No additional configuration is needed for
the workflow itself.

### 4. Security Note

> **Warning:** The `reg_token` input is visible in GitHub Actions
> workflow logs. It is a one-time token that is invalidated on first
> use, but you should still restrict access to Actions logs in your
> repository settings.

## Building and Development

### Prerequisites

- Go 1.22 or later
- Access to a Forgejo instance with Actions enabled
- A GitHub repository for the runner workflow

### Build

```sh
go build ./cmd/forgery
```

### Test

```sh
go test ./... -race
```

All tests run with the `-race` flag by convention. The `store` and
`session` packages include concurrent access tests.

### Project Structure

```
forgery/
├── cmd/
│   └── forgery/          # main entrypoint, wires all modules
├── internal/
│   ├── config/           # env var / dotenv / defaults
│   ├── token/            # cryptographic random token generation
│   ├── store/            # in-memory task/session storage
│   ├── north/            # northbound client (forgery → Forgejo)
│   ├── south/            # southbound server (internal runner → forgery)
│   ├── dispatch/         # GitHub Actions workflow_dispatch calls
│   ├── session/          # session lifecycle management
│   ├── run/              # per-task orchestration
│   └── observability/    # logging, metrics, health checks
├── templates/
│   └── forgery-runner.yml  # GA Workflow template for users

├── go.mod
├── go.sum
├── COPYING                 # GPLv3
└── README.md
```

Module path: `git.0x0f.dev/forgery`.

## License

forgery is free software: you can redistribute it and/or modify it under
the terms of the **GNU General Public License** as published by the Free
Software Foundation, either version 3 of the License, or (at your
option) any later version.

See [COPYING](COPYING) for the full license text.
