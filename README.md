**English** | [简体中文](README.zh.md)

# Forgery

**Forgery** is a Go daemon that acts as a bidirectional proxy between Forgejo and GitHub Actions. It registers with Forgejo as a normal runner, but instead of executing jobs locally, it delegates them to a `forgejo-runner` instance running inside a GitHub Actions workflow.

## Overview

Forgery connects to your Forgejo instance as an Actions runner, polling for tasks via the standard gRPC protocol. When a task arrives, Forgery triggers a `workflow_dispatch` event on GitHub Actions. The workflow launches the official `forgejo-runner` binary in ephemeral `one-job` mode, which connects back to Forgery's southbound gRPC endpoint, fetches the actual task from Forgejo through Forgery, executes it in a container, and streams logs and results back through Forgery to Forgejo.

A single Forgery process can serve multiple Forgejo instances at once: each instance gets its own runner identity and task poller, all sharing one southbound endpoint.

## Quick Start

**1. Clone and build**

```sh
git clone https://git.0x0f.dev/shkouyo/forgery
cd forgery
go build ./cmd/forgery
```

Prerequisites: Go 1.25.0.

**2. Copy the workflow template**

Copy `templates/forgery-runner.yml` to `.github/workflows/` in the repository specified by `github_repo`:

```sh
cp templates/forgery-runner.yml /path/to/your/repo/.github/workflows/
```

**3. Create the configuration file**

Copy the commented example and fill in your values:

```sh
cp forgery.toml.example forgery.toml
```

At minimum, set the GitHub connection keys and the first instance's Forgejo settings:

```toml
# GitHub Actions connection
github_token = "github_pat_..."
github_repo = "your-org/your-repo"
github_workflow_id = "forgery-runner.yml"

[[instances]]
forgejo_url = "https://forgejo.example.com"
forgejo_runner_token = "your-runner-registration-token"
forgejo_runner_name = "forgery-proxy"
forgejo_runner_labels = "ubuntu-latest:docker://node:20-bookworm,docker:docker://ghcr.io/catthehacker/ubuntu:act-latest"
```

**4. Run Forgery**

```sh
./forgery
```

Forgery loads `forgery.toml` from the current directory by default; pass `--config` to use another file (e.g. `./forgery --config /etc/forgery/forgery.toml`).

**5. Configure a reverse proxy**

Forgery listens on `:8443` for plain HTTP by default. Place a TLS-terminating reverse proxy (Caddy, nginx) in front of it for production use, and make sure `public_url` points at the public `https://` URL (when unset it is derived from the hostname and the `listen_addr` port).

## Configuration

Forgery is configured through a single TOML file, passed with `--config` (default: `forgery.toml` in the current directory). See `forgery.toml.example` at the repository root for a fully commented example. The loading chain is:

1. TOML file (a missing or unreadable file is an error)
2. Default values for unset fields
3. Validation of required fields and values

Parsing is strict: unknown keys, invalid values (durations, integers, booleans), and missing required fields all abort startup — every problem found in a single pass is reported together. Instance names must be unique: they are the routing key that maps each task back to the Forgejo instance that owns it.

### Global Keys

| Key | Required | Default | Description |
|----------|----------|---------|-------------|
| `log_level` | No | `info` | Log level: `debug`, `info`, `warn`, or `error` |
| `log_format` | No | `json` | Log output format: `json` or `text` |
| `health_addr` | No | — | Health probe server listen address; empty disables health checks (see [Health Checks](#health-checks)) |
| `listen_addr` | No | `:8443` | Southbound gRPC listen address |
| `public_url` | No | `https://<hostname>:<listen port>` | Publicly reachable URL for Forgery (used by internal runners to connect back) |
| `state_file` | No | `<config dir>/forgery-state.json` | State file persisting runner identities across restarts |
| `max_parallel_tasks` | No | `5` | Maximum number of concurrently executing tasks (one global budget across all instances) |
| `github_token` | Yes | — | GitHub personal access token with `workflow` scope (fine-grained, repo-scoped recommended) |
| `github_repo` | Yes | — | Repository holding the workflow, in `owner/repo` format |
| `github_workflow_id` | Yes | — | Workflow filename (e.g. `forgery-runner.yml`) or workflow ID |
| `github_ref` | No | `main` | Branch ref to trigger the workflow on |
| `github_api_url` | No | `https://api.github.com` | GitHub API base URL (set for GitHub Enterprise) |

### Instance Keys

One `[[instances]]` entry per Forgejo connection (at least one is required):

| Key | Required | Default | Description |
|----------|----------|---------|-------------|
| `name` | No | host of `forgejo_url` | Unique instance name; the routing key for task → instance resolution |
| `forgejo_url` | Yes | — | Forgejo instance base URL (e.g. `https://forgejo.example.com`) |
| `forgejo_runner_token` | Yes | — | Forgejo runner registration token |
| `forgejo_runner_name` | No | `forgery` | Display name shown in Forgejo's runner list |
| `forgejo_runner_labels` | Yes | — | Comma-separated labels with optional container image mappings (see [Labels](#labels)) |
| `poll_interval` | No | `3s` | Interval between northbound `FetchTask` polls |
| `reg_token_ttl` | No | `15m` | Lifetime of a one-time registration token |
| `ga_startup_timeout` | No | `15m` | Maximum time to wait for a GA workflow to start and the internal runner to register |
| `heartbeat_interval` | No | `30s` | Interval for sending `UpdateTask(state=running)` heartbeats, from dispatch until the task reaches a terminal state (the critical window is while waiting for the internal runner) |
| `tls_insecure_skip_verify` | No | `false` | Skip TLS certificate verification for northbound connections (development only) |

`state_file` defaults to `forgery-state.json` next to the configuration file. Forgery persists the runner identity (UUID + permanent token) of every Forgejo instance in it, keyed by `forgejo_url`, so a restart reuses the same runner instead of registering an orphan. The file is written atomically with `0600` permissions; a corrupt or wrongly versioned file is a hard error at startup — identity data is never silently discarded.

### Labels

`forgejo_runner_labels` maps Forgejo `runs-on` labels to container images using the format:

```
label1:docker://image1,label2:docker://image2
```

- The part before `:` is the **runs-on label** that Forgejo workflow jobs match against.
- The part after `:` is the **container image URL** prefixed with `docker://`.

Example:

```toml
forgejo_runner_labels = "ubuntu-latest:docker://node:20-bookworm,docker:docker://ghcr.io/catthehacker/ubuntu:act-latest"
```

- When a Forgejo workflow specifies `runs-on: ubuntu-latest`, Forgery matches it and the runner executes in `node:20-bookworm`.
- When `runs-on: docker`, it executes in `ghcr.io/catthehacker/ubuntu:act-latest`.

The same label string drives both sides: northbound `Register`/`Declare` send bare label names (the `:docker://…` image mapping is stripped — Forgejo expects bare labels), while the `labels` input of the `workflow_dispatch` carries the full `label:docker://image` mapping, so the GA workflow knows which container to use.

### Health Checks

When `health_addr` is set, Forgery runs a health probe HTTP server on that address: only `/healthz` and `/readyz` answer `200` with `{"status":"ok"}`, and any other path — including `/` — returns `404`. When `health_addr` is empty (the default), no health server is started. The health server is shut down gracefully as part of the daemon's shutdown sequence.

## How It Works

1. **Forgejo dispatches a job** — Forgery, registered as a runner, receives it via `FetchTask`.
2. **Forgery triggers a GitHub Actions workflow** — it calls the `workflow_dispatch` API, passing task metadata (proxy URL, one-time registration token, labels).
3. **The GA workflow starts a real `forgejo-runner`** — it downloads the official runner binary and launches it in `one-job` ephemeral mode, connecting back to Forgery's gRPC endpoint.
4. **Forgery hands the Forgejo task to the internal runner** — the runner fetches the task, executes it inside a container, and reports results.
5. **Logs and task status flow back** — `UpdateTask` and `UpdateLog` RPCs are transparently relayed through Forgery to Forgejo.

**Multiple Forgejo instances** — each `[[instances]]` entry gets its own runner identity (registered against its own Forgejo) and its own task poller. Every task is tagged with its instance name; registration-token validation, heartbeats, and `UpdateTask`/`UpdateLog` forwarding are all routed through the owning instance's northbound client. The southbound side stays a single shared endpoint (`listen_addr`/`public_url` are global): the one-time registration token embedded in each `workflow_dispatch` is what maps a connecting internal runner back to its task, and therefore to its instance. `max_parallel_tasks` is one global concurrency budget shared by all instances' pollers.

**Checkout and artifacts via Forgery** — the internal runner's `GITHUB_SERVER_URL`, `GITHUB_API_URL`, and artifact endpoints all point at Forgery's `public_url` (forgejo-runner derives them from `--url`). Forgery reverse-proxies the git smart-HTTP paths (`/{owner}/{repo}` + git endpoints), the `/api/v1/repos/...` default-branch query, and the `/api/actions_pipeline/...` artifact endpoints to the Forgejo instance that owns the task, routing by the task token in the request's `Authorization` header — a token Forgejo itself issued, so the upstream instance authenticates it directly. Unknown tokens get `401`; with exactly one configured instance, unregistered tokens fall back to it.

**Persistent runner identity** — after the first `Register`, Forgery saves the runner UUID and permanent token to the state file. On restart it reuses the same identity instead of registering a fresh orphan runner; if Forgejo rejects the permanent token (revoked or rotated), Forgery re-registers automatically and retries. Re-registration creates a **new** runner identity — the old one is gone and a new entry appears in Forgejo's runner list. Tasks that were in flight on the old identity cannot be recovered: Forgejo marks them failed automatically within roughly 10–15 minutes (its zombie-reaping cron). If the registration token itself is invalid (reset in the Forgejo UI or wrong in the config), Forgery logs an error pointing you at `forgejo_runner_token` or the Forgejo UI and jumps straight to the 5min backoff ceiling — no retry cadence helps until the token is fixed. Transient failures (network errors, etc.), by contrast, back off exponentially between attempts, starting at 30s and doubling up to the 5min ceiling, logged at warning level.

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

All tests run with the `-race` flag by convention. The `store`, `session`, `slots`, and `state` packages include concurrent access tests.

### Project Structure

```
forgery/
├── cmd/forgery/       # main entry (flags, config load)
├── internal/
│   ├── app/           # daemon assembly and lifecycle (multi-instance wiring)
│   ├── config/        # TOML loading, defaults, and validation
│   ├── dispatch/      # GitHub Actions workflow_dispatch
│   ├── north/         # northbound client (Forgery → Forgejo)
│   ├── observability/ # logging and health checks
│   ├── run/           # per-task orchestration
│   ├── session/       # session lifecycle
│   ├── slots/         # global backpressure pool (max_parallel_tasks)
│   ├── south/         # southbound server (forgejo-runner → Forgery)
│   ├── state/         # runner identity persistence (state file)
│   ├── store/         # in-memory task/session storage
│   ├── token/         # cryptographic token generation
│   └── version/       # version constant
├── templates/
│   └── forgery-runner.yml
├── forgery.toml.example
├── go.mod / go.sum
├── COPYING            # GPL-3.0
└── README.md
```

Module path: `git.0x0f.dev/forgery`.

## Security

### Northbound (Forgery → Forgejo)

Forgery connects to Forgejo over HTTPS. Ensure `forgejo_url` uses `https://`. For development with self-signed certificates, set `tls_insecure_skip_verify = true` in the corresponding `[[instances]]` entry.

### Configuration File Protection

`forgery.toml` contains the GitHub token and the Forgejo registration tokens. Restrict its permissions (e.g. `chmod 600 forgery.toml`) and never commit it to version control.

### Southbound (internal runner → Forgery)

TLS is terminated at the reverse proxy (Caddy or nginx). The proxy handles certificate management (e.g. Let's Encrypt). Internal `forgejo-runner` instances connect to `public_url` (which should be `https://`), trusting standard public CAs.

### One-Time Registration Tokens

- Each task dispatch generates a unique registration token via `crypto/rand` (32 bytes, 256 bits of entropy).
- Tokens are **single-use** — the first successful `Register` call consumes the token; subsequent attempts are rejected.
- Tokens expire after `reg_token_ttl` (default 15 minutes).
- **Warning:** `reg_token` is passed as a `workflow_dispatch` input and is **visible in GitHub Actions workflow logs**. Restrict access to Actions logs in your repository settings as a precaution.

### Secrets Handling

Forgejo repository secrets from incoming tasks are held **only in memory** and forwarded over TLS connections. They are never written to disk or logged.

### State File

The state file (default `forgery-state.json` next to the config file) contains runner credentials — the UUID and permanent token of every Forgejo instance — and is written with `0600` permissions. Protect it like a secret; deleting it makes Forgery register fresh runner identities on the next start.

### GitHub PAT

- `github_token` should be a **fine-grained personal access token** scoped to a single repository.
- Minimum required permissions: `contents: read` and `actions: write`.
- The built-in `GITHUB_TOKEN` in Actions workflows is used by the runner workflow itself and requires no extra configuration.

## License

Copyright (C) 2026 ShinKouyo &lt;i@0x0f.dev&gt;

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

See [COPYING](COPYING) for the full license text.
