[English](README.md) | **简体中文**

# forgery — 在 GitHub Actions 上执行 Forgejo Actions 作业的代理

forgery 是一个 Go 守护进程，作为 Forgejo 和 GitHub Actions 之间的**双向 gRPC 代理**。
它对 Forgejo 实例来说就像一个普通的 Forgejo Actions runner —— 注册、轮询任务、报告
结果 —— 但实际上并不在本地执行作业。相反，它将执行委托给 GitHub Actions Workflow，
真正的 `forgejo-runner` 在那里启动，连接回 forgery，并运行作业。
日志和状态更新通过 forgery 传回 Forgejo。

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

## 工作原理

1. **Forgejo 分发作业** —— 已注册为 runner 的 forgery 通过 `FetchTask` 接收作业。
2. **forgery 触发 GitHub Actions Workflow** —— 调用 `workflow_dispatch` API，
   传递任务元数据（代理 URL、一次性注册令牌、标签、容器镜像）。
3. **GA Workflow 启动真实的 `forgejo-runner`** —— 下载官方 runner 二进制文件，
   以 `one-job` 临时模式启动，连接回 forgery 的 gRPC 端点。
4. **forgery 将真实 Forgejo 任务交给内部 runner** —— runner 获取任务，
   在容器内执行，并报告结果。
5. **日志和任务状态回流** —— `UpdateTask` 和 `UpdateLog` RPC
   通过 forgery 透明中继回 Forgejo。

## 快速开始

### 1. 克隆并构建

```sh
git clone https://git.0x0f.dev/forgery
cd forgery
go build ./cmd/forgery
```

前置要求：Go 1.22+。

### 2. 复制 Workflow 模板

将 `templates/forgery-runner.yml` 复制到 GitHub 仓库的
`.github/workflows/` 目录：

```sh
cp templates/forgery-runner.yml /path/to/your/repo/.github/workflows/
```

### 3. 设置环境变量

在 forgery 工作目录创建 `.env` 文件（或在 shell 中导出环境变量）。
至少需要以下内容：

```env
# ── Forgejo 连接 ──
FORGEJO_URL=https://forgejo.example.com
FORGEJO_RUNNER_TOKEN=your-runner-registration-token
FORGEJO_RUNNER_NAME=forgery-proxy
FORGEJO_RUNNER_LABELS=ubuntu-latest:docker://node:20-bookworm,docker:docker://ghcr.io/catthehacker/ubuntu:act-latest

# ── GitHub Actions 连接 ──
GITHUB_TOKEN=github_pat_...
GITHUB_REPO=your-org/your-repo
GITHUB_WORKFLOW_ID=forgery-runner.yml

# ── 可选但推荐 ──
PUBLIC_URL=https://forgery.example.com
DEFAULT_CONTAINER_IMAGE=docker://ghcr.io/catthehacker/ubuntu:act-latest
```

完整配置参考见[配置](#配置)。

### 4. 运行 forgery

```sh
./forgery
```

forgery 从 `.env` 文件（如果存在）加载配置，然后从环境变量加载。
环境变量的优先级高于 `.env` 中的值。

### 5. 配置反向代理

forgery 默认在 `:8443` 上监听纯 HTTP。应在前面放置
一个 TLS 终止反向代理。示例见[反向代理设置](#反向代理设置)。

## 配置

forgery 完全通过环境变量配置（可选 `.env` 文件作为后备）。加载顺序为：

1. `.env` 文件（不存在则静默跳过）
2. 操作系统环境变量（覆盖 `.env`）
3. 未设置字段的默认值
4. 必填字段的验证

### 完整环境变量参考

| 变量 | 必填 | 默认值 | 描述 |
|----------|----------|---------|-------------|
| `FORGEJO_URL` | 是 | — | Forgejo 实例基础 URL（如 `https://forgejo.example.com`） |
| `FORGEJO_RUNNER_TOKEN` | 是 | — | Forgejo runner 注册令牌 |
| `FORGEJO_RUNNER_NAME` | 是 | — | 在 Forgejo runner 列表中显示的显示名称 |
| `FORGEJO_RUNNER_LABELS` | 是 | — | 逗号分隔的标签及可选的容器镜像映射（如 `ubuntu-latest:docker://node:20-bookworm,docker:docker://ghcr.io/catthehacker/ubuntu:act-latest`） |
| `GITHUB_TOKEN` | 是 | — | 具有 `workflow` 权限的 GitHub 个人访问令牌（建议使用细粒度、仓库作用域的令牌） |
| `GITHUB_REPO` | 是 | — | 存放 workflow 的仓库，格式为 `owner/repo` |
| `GITHUB_WORKFLOW_ID` | 是 | — | Workflow 文件名（如 `forgery-runner.yml`）或 workflow ID |
| `GITHUB_REF` | 否 | `main` | 触发 workflow 的分支 ref |
| `GITHUB_API_URL` | 否 | `https://api.github.com` | GitHub API 基础 URL（使用 GitHub Enterprise 时设置） |
| `LISTEN_ADDR` | 否 | `:8443` | 南向 gRPC 监听地址 |
| `PUBLIC_URL` | 否 | `https://<hostname>:8443` | forgery 的公网可达 URL（供内部 runner 连接回 forgery） |
| `DEFAULT_CONTAINER_IMAGE` | 否 | — | 没有显式 `:docker://...` 映射的标签使用的默认容器镜像 |
| `MAX_PARALLEL_TASKS` | 否 | `5` | 最大并发执行任务数 |
| `POLL_INTERVAL` | 否 | `3s` | 北向 `FetchTask` 轮询间隔 |
| `REG_TOKEN_TTL` | 否 | `15m` | 一次性注册令牌的有效期 |
| `GA_STARTUP_TIMEOUT` | 否 | `15m` | 等待 GA Workflow 启动及内部 runner 注册的最长时间 |
| `HEARTBEAT_INTERVAL` | 否 | `30s` | 等待内部 runner 时发送 `UpdateTask(state=running)` 心跳的间隔 |
| `PING_KEEPALIVE` | 否 | `true` | 在背压期间发送 Ping RPC 以保持 Forgejo 连接 |
| `LOG_LEVEL` | 否 | `info` | 日志级别：`debug`、`info`、`warn` 或 `error` |
| `LOG_FORMAT` | 否 | `json` | 日志输出格式：`json` 或 `text` |
| `METRICS_ADDR` | 否 | `:9090` | Prometheus `/metrics` 端点监听地址 |
| `HEALTH_ADDR` | 否 | — | 健康检查端点监听地址（设置后暴露 `/healthz` 和 `/readyz`） |
| `TLS_INSECURE_SKIP_VERIFY` | 否 | `false` | 跳过北向连接的 TLS 证书验证（仅用于开发环境） |

## 反向代理设置

forgery 的南向 gRPC 端点在 `LISTEN_ADDR`（默认 `:8443`）上监听纯 HTTP。
在生产环境中，**必须**在前面放置一个 TLS 终止反向代理。

### 推荐拓扑

```
Internet (HTTPS)
    │
    ▼
┌──────────────────┐
│  Caddy / nginx   │  ← TLS 终止（Let's Encrypt / 自定义证书）
│  :443            │
└────────┬─────────┘
         │  HTTP (plain)
         ▼
┌──────────────────┐
│  forgery         │
│  :8443           │  ← 仅在 localhost 或内部网络监听
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

确保 `PUBLIC_URL` 与反向代理的 HTTPS URL 匹配。

## 安全考量

### 北向（forgery → Forgejo）

- forgery 通过 **HTTPS** 连接 Forgejo。确保 `FORGEJO_URL` 使用 `https://`。
- 使用自签名证书开发时，设置 `TLS_INSECURE_SKIP_VERIFY=true`。

### 南向（内部 runner → forgery）

- TLS 在**反向代理处终止**（Caddy 或 nginx）。代理负责证书管理（如 Let's Encrypt）。
- 内部 `forgejo-runner` 实例连接到 `PUBLIC_URL`（应为 `https://`），信任标准公共 CA。

### 一次性注册令牌

- 每次任务分发通过 `crypto/rand` 生成唯一的注册令牌（32 字节，256 位熵）。
- 令牌**一次性使用** —— 首次成功的 `Register` 调用即消耗令牌，后续尝试被拒绝。
- 令牌在 `REG_TOKEN_TTL`（默认 15 分钟）后过期。
- **警告：** `reg_token` 作为 `workflow_dispatch` 输入传递，**在 GitHub Actions workflow 日志中可见**。
  令牌在首次使用后即失效，但作为额外预防措施，应限制对 Actions 日志的访问。

### 密钥处理

- 来自 Forgejo 仓库密钥的传入任务**仅在内存中**保存，并通过 TLS 连接转发。绝不会写入磁盘或记录到日志中。

### GitHub PAT

- `GITHUB_TOKEN` 应为**细粒度个人访问令牌**，限定到单个仓库。
- 最低所需权限：`contents: read` 和 `actions: write`。
- Actions workflow 中使用的内置 `GITHUB_TOKEN` 由 runner workflow 自身使用，无需额外配置。

## 标签与容器镜像

forgery 使用标签到容器镜像的映射，将 Forgejo 作业请求路由到适当的执行环境。

### 格式

```
label1:docker://image1,label2:docker://image2
```

- `:` 之前的部分是 Forgejo workflow 作业匹配的 **runs-on 标签**。
- `:` 之后的部分是以 `docker://` 为前缀的**容器镜像 URL**。

### 示例

```env
FORGEJO_RUNNER_LABELS=ubuntu-latest:docker://node:20-bookworm,docker:docker://ghcr.io/catthehacker/ubuntu:act-latest
DEFAULT_CONTAINER_IMAGE=docker://ghcr.io/catthehacker/ubuntu:act-latest
```

- 当 Forgejo workflow 指定 `runs-on: ubuntu-latest` 时，forgery 匹配该标签，内部 runner 在 `node:20-bookworm` 中执行。
- 当 `runs-on: docker` 时，在 `ghcr.io/catthehacker/ubuntu:act-latest` 中执行。
- 如果 `runs-on` 标签在 `FORGEJO_RUNNER_LABELS` 中没有显式的 `:docker://...` 后缀，则使用 `DEFAULT_CONTAINER_IMAGE`。

相同的标签字符串同时用于北向注册（让 Forgejo 知道将哪些任务路由到 forgery）
和内部 runner 配置（让 GA workflow 知道使用哪个容器）。

## GitHub Actions 设置

### 1. 添加 Workflow

将 `templates/forgery-runner.yml` 复制到 `GITHUB_REPO` 指定仓库的 `.github/workflows/` 目录：

```sh
cp templates/forgery-runner.yml path/to/your-repo/.github/workflows/
```

### 2. 设置 Runner 版本

Workflow 使用 `$FORGEJO_RUNNER_VERSION` 确定要下载哪个 `forgejo-runner` 二进制文件。
将其设置为仓库中的 **GitHub Actions 变量**（Settings → Secrets and variables →
Actions → Variables）：

| 变量 | 值 |
|----------|-------|
| `FORGEJO_RUNNER_VERSION` | `v12.8.0`（未设置时的默认值） |

请与你的 Forgejo 实例期望的 Forgejo runner 版本保持同步。

### 3. 内置 `GITHUB_TOKEN`

Workflow 内部使用的 `GITHUB_TOKEN` 是 GitHub Actions 提供的标准
[自动令牌](https://docs.github.com/en/actions/security-guides/automatic-token-authentication)。
Workflow 本身无需额外配置。

### 4. 安全说明

> **警告：** `reg_token` 输入在 GitHub Actions workflow 日志中可见。
> 它是一次性令牌，首次使用后即失效，但仍应在仓库设置中限制对 Actions 日志的访问。

## 构建与开发

### 前置要求

- Go 1.22 或更高版本
- 具有已启用 Actions 功能的 Forgejo 实例的访问权限
- 用于 runner workflow 的 GitHub 仓库

### 构建

```sh
go build ./cmd/forgery
```

### 测试

```sh
go test ./... -race
```

所有测试按惯例使用 `-race` 标志运行。`store` 和 `session` 包包含并发访问测试。

### 项目结构

```
forgery/
├── cmd/
│   └── forgery/          # 主入口，组装所有模块
├── internal/
│   ├── config/           # 环境变量 / dotenv / 默认值
│   ├── token/            # 加密随机令牌生成
│   ├── store/            # 内存任务/会话存储
│   ├── north/            # 北向客户端（forgery → Forgejo）
│   ├── south/            # 南向服务器（内部 runner → forgery）
│   ├── dispatch/         # GitHub Actions workflow_dispatch 调用
│   ├── session/          # 会话生命周期管理
│   ├── run/              # 逐任务编排
│   └── observability/    # 日志、指标、健康检查
├── templates/
│   └── forgery-runner.yml  # 面向用户的 GA Workflow 模板

├── go.mod
├── go.sum
├── COPYING                 # GPLv3
└── README.md
```

模块路径：`git.0x0f.dev/forgery`。

## 许可证

forgery 是自由软件：你可以依据**GNU 通用公共许可证**（由自由软件基金会发布，
版本 3 或（按你的选择）任何更新版本）的条款重新分发和/或修改它。

完整许可证文本见 [COPYING](COPYING)。
