[English](README.md) | **简体中文**

# Forgery

**Forgery** is a Go daemon that acts as a bidirectional proxy between Forgejo and GitHub Actions. It registers with Forgejo as a normal runner, but instead of executing jobs locally, it delegates them to a `forgejo-runner` instance running inside a GitHub Actions workflow.

## Overview

**Forgery** 连接到你的 Forgejo 实例作为 Actions runner, 通过标准 gRPC 协议轮询任务. 当任务到达时, **Forgery** 在 GitHub Actions 上触发一个 `workflow_dispatch` 事件. 该 workflow 以临时 `one-job` 模式启动官方 `forgejo-runner` 二进制文件, 连接回 **Forgery** 的南向 gRPC 端点, 通过 **Forgery** 从 Forgejo 获取实际任务, 在容器中执行, 并将日志和结果通过 **Forgery** 流式传回 Forgejo.

## Quick Start

**1. Clone and build**

```sh
git clone https://git.0x0f.dev/forgery
cd forgery
go build ./cmd/forgery
```

前置要求: Go 1.25.0.

**2. Copy the workflow template**

将 `templates/forgery-runner.yml` 复制到 `GITHUB_REPO` 指定仓库的 `.github/workflows/` 目录:

```sh
cp templates/forgery-runner.yml /path/to/your/repo/.github/workflows/
```

**3. Set environment variables**

创建 `.env` 文件或导出以下环境变量:

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

**Forgery** 默认在 `:8443` 上监听纯 HTTP. 生产环境前应放置一个 TLS 终止反向代理 (Caddy, nginx).

## Configuration

**Forgery** 完全通过环境变量配置, 可选 `.env` 文件作为后备. 加载顺序为:

1. `.env` 文件 (不存在则静默跳过)
2. OS 环境变量 (覆盖 `.env`)
3. 未设置字段的默认值
4. 必填字段的验证

### Environment Variable Reference

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `FORGEJO_URL` | Yes | -- | Forgejo 实例基础 URL (例如 `https://forgejo.example.com`) |
| `FORGEJO_RUNNER_TOKEN` | Yes | -- | Forgejo runner 注册令牌 |
| `FORGEJO_RUNNER_NAME` | Yes | -- | 在 Forgejo runner 列表中显示的显示名称 |
| `FORGEJO_RUNNER_LABELS` | Yes | -- | 逗号分隔的标签及可选容器镜像映射 (见 [Labels](#labels)) |
| `GITHUB_TOKEN` | Yes | -- | 具有 `workflow` 权限的 GitHub 个人访问令牌 (建议使用细粒度仓库作用域令牌) |
| `GITHUB_REPO` | Yes | -- | 存放 workflow 的仓库, 格式为 `owner/repo` |
| `GITHUB_WORKFLOW_ID` | Yes | -- | Workflow 文件名 (例如 `forgery-runner.yml`) 或 workflow ID |
| `GITHUB_REF` | No | `main` | 触发 workflow 的分支引用 |
| `GITHUB_API_URL` | No | `https://api.github.com` | GitHub API 基础 URL (使用 GitHub Enterprise 时设置) |
| `LISTEN_ADDR` | No | `:8443` | 南向 gRPC 监听地址 |
| `PUBLIC_URL` | No | `https://<hostname>:8443` | Forgery 的公网可达 URL (供内部 runner 连接回来) |
| `DEFAULT_CONTAINER_IMAGE` | No | -- | 无显式 `:docker://...` 映射的标签使用的默认容器镜像 |
| `MAX_PARALLEL_TASKS` | No | `5` | 最大并发执行任务数 |
| `POLL_INTERVAL` | No | `3s` | 北向 `FetchTask` 轮询间隔 |
| `REG_TOKEN_TTL` | No | `15m` | 一次性注册令牌的有效期 |
| `GA_STARTUP_TIMEOUT` | No | `15m` | 等待 GA workflow 启动及内部 runner 注册的最长时间 |
| `HEARTBEAT_INTERVAL` | No | `30s` | 等待内部 runner 时发送 `UpdateTask(state=running)` 心跳的间隔 |
| `PING_KEEPALIVE` | No | `true` | 在背压期间发送 Ping RPC 以保持 Forgejo 连接存活 |
| `LOG_LEVEL` | No | `info` | 日志级别: `debug`, `info`, `warn`, 或 `error` |
| `LOG_FORMAT` | No | `json` | 日志输出格式: `json` 或 `text` |
| `HEALTH_ADDR` | No | -- | 健康检查端点监听地址 (设置后暴露 `/healthz` 和 `/readyz`) |
| `TLS_INSECURE_SKIP_VERIFY` | No | `false` | 跳过北向连接 TLS 证书验证 (仅开发环境) |

### Labels

`FORGEJO_RUNNER_LABELS` 使用以下格式将 Forgejo `runs-on` 标签映射到容器镜像:

```
label1:docker://image1,label2:docker://image2
```

- `:` 之前的部分是 Forgejo workflow 作业匹配的 **runs-on 标签**.
- `:` 之后的部分是以 `docker://` 为前缀的**容器镜像 URL**.

示例:

```env
FORGEJO_RUNNER_LABELS=ubuntu-latest:docker://node:20-bookworm,docker:docker://ghcr.io/catthehacker/ubuntu:act-latest
DEFAULT_CONTAINER_IMAGE=docker://ghcr.io/catthehacker/ubuntu:act-latest
```

- 当 Forgejo workflow 指定 `runs-on: ubuntu-latest` 时, Forgery 匹配该标签, runner 在 `node:20-bookworm` 中执行.
- 当 `runs-on: docker` 时, 在 `ghcr.io/catthehacker/ubuntu:act-latest` 中执行.
- 如果 `runs-on` 标签没有显式的 `:docker://...` 后缀, 则使用 `DEFAULT_CONTAINER_IMAGE`.

相同的标签字符串同时用于北向注册 (让 Forgejo 知道路由哪些任务到 Forgery) 和内部 runner 配置 (让 GA workflow 知道使用哪个容器).

## How It Works

1. **Forgejo dispatches a job** -- 已注册为 runner 的 Forgery 通过 `FetchTask` 接收作业.
2. **Forgery triggers a GitHub Actions workflow** -- 调用 `workflow_dispatch` API, 传递任务元数据 (代理 URL, 一次性注册令牌, 标签, 容器镜像).
3. **The GA workflow starts a real `forgejo-runner`** -- 下载官方 runner 二进制文件, 以 `one-job` 临时模式启动, 连接回 Forgery 的 gRPC 端点.
4. **Forgery hands the Forgejo task to the internal runner** -- runner 获取任务, 在容器内执行, 并报告结果.
5. **Logs and task status flow back** -- `UpdateTask` 和 `UpdateLog` RPC 通过 Forgery 透明中继回 Forgejo.

## Build & Test

### Prerequisites

- Go 1.25.0
- 访问已启用 Actions 的 Forgejo 实例
- 一个用于 runner workflow 的 GitHub 仓库

### Build

```sh
go build ./cmd/forgery
```

### Test

```sh
go test ./... -race
```

所有测试按惯例使用 `-race` 标志运行. `store` 和 `session` 包包含并发访问测试.

### Project Structure

```
forgery/
├── cmd/forgery/       # main entry
├── internal/
│   ├── config/        # env var loading and defaults
│   ├── dispatch/      # GitHub Actions workflow_dispatch
│   ├── north/         # northbound client (Forgery -> Forgejo)
│   ├── observability/ # logging and health checks
│   ├── run/           # per-task orchestration
│   ├── session/       # session lifecycle
│   ├── south/         # southbound server (forgejo-runner -> Forgery)
│   ├── store/         # in-memory task/session storage
│   ├── token/         # cryptographic token generation
│   └── version/       # version constant
├── templates/
│   └── forgery-runner.yml
├── go.mod / go.sum
├── COPYING            # GPLv3
└── README.md
```

模块路径: `git.0x0f.dev/forgery`.

## Security

### Northbound (Forgery -> Forgejo)

**Forgery** 通过 HTTPS 连接 Forgejo. 确保 `FORGEJO_URL` 使用 `https://`. 使用自签名证书进行开发时, 设置 `TLS_INSECURE_SKIP_VERIFY=true`.

### Southbound (internal runner -> Forgery)

TLS 在反向代理 (Caddy 或 nginx) 处终止. 代理负责证书管理 (例如 Let's Encrypt). 内部 `forgejo-runner` 实例连接到 `PUBLIC_URL` (应为 `https://`), 信任标准公共 CA.

### One-Time Registration Tokens

- 每次任务分发通过 `crypto/rand` 生成唯一注册令牌 (32 字节, 256 位熵).
- 令牌**一次性使用** -- 首次成功的 `Register` 调用消耗令牌, 后续尝试被拒绝.
- 令牌在 `REG_TOKEN_TTL` (默认 15 分钟) 后过期.
- **警告:** `reg_token` 作为 `workflow_dispatch` 输入传递, **在 GitHub Actions workflow 日志中可见**. 作为预防措施, 应在仓库设置中限制对 Actions 日志的访问.

### Secrets Handling

来自 Forgejo 仓库密钥的传入任务**仅在内存中**保存, 并通过 TLS 连接转发. 绝不会写入磁盘或记录到日志中.

### GitHub PAT

- `GITHUB_TOKEN` 应为**细粒度个人访问令牌**, 限定到单个仓库.
- 最低所需权限: `contents: read` 和 `actions: write`.
- Actions workflow 中的内置 `GITHUB_TOKEN` 由 runner workflow 自身使用, 无需额外配置.

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

完整许可证文本见 [COPYING](COPYING).
