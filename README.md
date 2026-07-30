# Forgery

**Forgery** is a Go daemon that acts as a bidirectional proxy between Forgejo and GitHub Actions. It registers with Forgejo as a normal runner, but instead of executing jobs locally, it delegates them to a `forgejo-runner` instance running inside a GitHub Actions workflow.

## Overview

Forgery connects to your Forgejo instance as an Actions runner, polling for tasks via the standard gRPC protocol. When a task arrives, Forgery triggers a `workflow_dispatch` event on GitHub Actions. The workflow launches the official `forgejo-runner` binary in ephemeral `one-job` mode, which connects back to Forgery's southbound gRPC endpoint, fetches the actual task from Forgejo through Forgery, executes it in a container, and streams logs and results back through Forgery to Forgejo.

## Quick Start

**1. Clone and build**

```sh
git clone https://git.0x0f.dev/forgery
cd forgery
go build ./cmd/forgery
```

Prerequisites: Go 1.25.0.

**2. Copy the workflow template**

Copy `templates/forgery-runner.yml` to `.github/workflows/` in the repository specified by `GITHUB_REPO`:

```sh
cp templates/forgery-runner.yml /path/to/your/repo/.github/workflows/
```

**3. Set environment variables**

Create a `.env` file or export these environment variables:

```env
# Forgejo connection
FORGEJO_URL=https://forgejo.example.com
FORGEJO_RUNNER_TOKEN=your-runner-registration-token
FORGEJO_RUNNER_NAME=forgery-proxy
FORGEJO_RUNNER_LABELS=ubuntu-latest:docker://node:20-bookworm,docker:docker://ghcr.io/catthehacker/ubuntu:act-latest

# GitHub Actions connection
GITHUB_TOKEN=github_pat_...
GITHUB_REPO=your-org/your-repo
GITHUB_WORKFLOW_ID=forgery-runner.yml
```

**4. Run Forgery**

```sh
./forgery
```

**5. Configure a reverse proxy**

Forgery listens on `:8443` for plain HTTP by default. Place a TLS-terminating reverse proxy (Caddy, nginx) in front of it for production use.

## Configuration

Forgery is configured entirely through environment variables, with an optional `.env` file as a fallback. The loading order is:

1. `.env` file (silently skipped if missing)
2. OS environment variables (override `.env`)
3. Default values for unset fields
4. Validation of required fields

### Environment Variable Reference

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `FORGEJO_URL` | Yes | — | Forgejo instance base URL (e.g. `https://forgejo.example.com`) |
| `FORGEJO_RUNNER_TOKEN` | Yes | — | Forgejo runner registration token |
| `FORGEJO_RUNNER_NAME` | Yes | — | Display name shown in Forgejo's runner list |
| `FORGEJO_RUNNER_LABELS` | Yes | — | Comma-separated labels with optional container image mappings (see [Labels](#labels)) |
| `GITHUB_TOKEN` | Yes | — | GitHub personal access token with `workflow` scope (fine-grained, repo-scoped recommended) |
| `GITHUB_REPO` | Yes | — | Repository holding the workflow, in `owner/repo` format |
| `GITHUB_WORKFLOW_ID` | Yes | — | Workflow filename (e.g. `forgery-runner.yml`) or workflow ID |
| `GITHUB_REF` | No | `main` | Branch ref to trigger the workflow on |
| `GITHUB_API_URL` | No | `https://api.github.com` | GitHub API base URL (set for GitHub Enterprise) |
| `LISTEN_ADDR` | No | `:8443` | Southbound gRPC listen address |
| `PUBLIC_URL` | No | `https://<hostname>:8443` | Publicly reachable URL for Forgery (used by internal runners to connect back) |
| `DEFAULT_CONTAINER_IMAGE` | No | — | Default container image for labels without an explicit `:docker://...` mapping |
| `MAX_PARALLEL_TASKS` | No | `5` | Maximum number of concurrently executing tasks |
| `POLL_INTERVAL` | No | `3s` | Interval between northbound `FetchTask` polls |
| `REG_TOKEN_TTL` | No | `15m` | Lifetime of a one-time registration token |
| `GA_STARTUP_TIMEOUT` | No | `15m` | Maximum time to wait for a GA workflow to start and the internal runner to register |
| `HEARTBEAT_INTERVAL` | No | `30s` | Interval for sending `UpdateTask(state=running)` heartbeats while waiting for the internal runner |
| `PING_KEEPALIVE` | No | `true` | Send Ping RPCs during backpressure to keep the Forgejo connection alive |
| `LOG_LEVEL` | No | `info` | Log level: `debug`, `info`, `warn`, or `error` |
| `LOG_FORMAT` | No | `json` | Log output format: `json` or `text` |
| `HEALTH_ADDR` | No | — | Health check endpoint listen address (if set, exposes `/healthz` and `/readyz`) |
| `TLS_INSECURE_SKIP_VERIFY` | No | `false` | Skip TLS certificate verification for northbound connections (development only) |

### Labels

`FORGEJO_RUNNER_LABELS` maps Forgejo `runs-on` labels to container images using the format:

```
label1:docker://image1,label2:docker://image2
```

- The part before `:` is the **runs-on label** that Forgejo workflow jobs match against.
- The part after `:` is the **container image URL** prefixed with `docker://`.

Example:

```env
FORGEJO_RUNNER_LABELS=ubuntu-latest:docker://node:20-bookworm,docker:docker://ghcr.io/catthehacker/ubuntu:act-latest
DEFAULT_CONTAINER_IMAGE=docker://ghcr.io/catthehacker/ubuntu:act-latest
```

- When a Forgejo workflow specifies `runs-on: ubuntu-latest`, Forgery matches it and the runner executes in `node:20-bookworm`.
- When `runs-on: docker`, it executes in `ghcr.io/catthehacker/ubuntu:act-latest`.
- If a `runs-on` label has no explicit `:docker://...` suffix, `DEFAULT_CONTAINER_IMAGE` is used.

The same label string is used for both northbound registration (so Forgejo knows which tasks to route to Forgery) and the internal runner configuration (so the GA workflow knows which container to use).

## How It Works

1. **Forgejo dispatches a job** — Forgery, registered as a runner, receives it via `FetchTask`.
2. **Forgery triggers a GitHub Actions workflow** — it calls the `workflow_dispatch` API, passing task metadata (proxy URL, one-time registration token, labels, container image).
3. **The GA workflow starts a real `forgejo-runner`** — it downloads the official runner binary and launches it in `one-job` ephemeral mode, connecting back to Forgery's gRPC endpoint.
4. **Forgery hands the Forgejo task to the internal runner** — the runner fetches the task, executes it inside a container, and reports results.
5. **Logs and task status flow back** — `UpdateTask` and `UpdateLog` RPCs are transparently relayed through Forgery to Forgejo.

## Build & Test

### Prerequisites

- Go 1.25.0
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

All tests run with the `-race` flag by convention. The `store` and `session` packages include concurrent access tests.

### Project Structure

```
forgery/
├── cmd/forgery/       # main entry
├── internal/
│   ├── config/        # env var loading and defaults
│   ├── dispatch/      # GitHub Actions workflow_dispatch
│   ├── north/         # northbound client (Forgery → Forgejo)
│   ├── observability/ # logging and health checks
│   ├── run/           # per-task orchestration
│   ├── session/       # session lifecycle
│   ├── south/         # southbound server (forgejo-runner → Forgery)
│   ├── store/         # in-memory task/session storage
│   ├── token/         # cryptographic token generation
│   └── version/       # version constant
├── templates/
│   └── forgery-runner.yml
├── go.mod / go.sum
├── COPYING            # GPLv3
└── README.md
```

Module path: `git.0x0f.dev/forgery`.

## Security

### Northbound (Forgery → Forgejo)

Forgery connects to Forgejo over HTTPS. Ensure `FORGEJO_URL` uses `https://`. For development with self-signed certificates, set `TLS_INSECURE_SKIP_VERIFY=true`.

### Southbound (internal runner → Forgery)

TLS is terminated at the reverse proxy (Caddy or nginx). The proxy handles certificate management (e.g. Let's Encrypt). Internal `forgejo-runner` instances connect to `PUBLIC_URL` (which should be `https://`), trusting standard public CAs.

### One-Time Registration Tokens

- Each task dispatch generates a unique registration token via `crypto/rand` (32 bytes, 256 bits of entropy).
- Tokens are **single-use** — the first successful `Register` call consumes the token; subsequent attempts are rejected.
- Tokens expire after `REG_TOKEN_TTL` (default 15 minutes).
- **Warning:** `reg_token` is passed as a `workflow_dispatch` input and is **visible in GitHub Actions workflow logs**. Restrict access to Actions logs in your repository settings as a precaution.

### Secrets Handling

Forgejo repository secrets from incoming tasks are held **only in memory** and forwarded over TLS connections. They are never written to disk or logged.

### GitHub PAT

- `GITHUB_TOKEN` should be a **fine-grained personal access token** scoped to a single repository.
- Minimum required permissions: `contents: read` and `actions: write`.
- The built-in `GITHUB_TOKEN` in Actions workflows is used by the runner workflow itself and requires no extra configuration.

## License

Copyright (C) 2026 ShinKouyo <i@0x0f.dev>

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

See [COPYING](COPYING) for the full license text.
