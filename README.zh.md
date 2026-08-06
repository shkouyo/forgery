[English](README.md) | **简体中文**

# Forgery

**Forgery** 是一个 Go 守护进程, 充当 Forgejo 与 GitHub Actions 之间的双向代理. 它像普通 runner 一样向 Forgejo 注册, 但不在本地执行作业, 而是将任务委托给在 GitHub Actions workflow 中运行的 `forgejo-runner` 实例.

## 概述

Forgery 连接到你的 Forgejo 实例作为 Actions runner, 通过标准 gRPC 协议轮询任务. 当任务到达时, Forgery 在 GitHub Actions 上触发一个 `workflow_dispatch` 事件. 该 workflow 以临时 `one-job` 模式启动官方 `forgejo-runner` 二进制文件, 连接回 Forgery 的南向 gRPC 端点, 通过 Forgery 从 Forgejo 获取实际任务, 在容器中执行, 并将日志和结果通过 Forgery 流式传回 Forgejo.

单个 Forgery 进程可同时服务多个 Forgejo 实例: 每个实例拥有独立的 Runner 身份和任务 poller, 共享同一个南向端点.

## 快速开始

**1. 克隆并构建**

```sh
git clone https://git.0x0f.dev/shkouyo/forgery
cd forgery
go build ./cmd/forgery
```

前置要求: Go 1.25.0.

**2. 复制 Workflow 模板**

将 `templates/forgery-runner.yml` 复制到 `github_repo` 指定仓库的 `.github/workflows/` 目录:

```sh
cp templates/forgery-runner.yml /path/to/your/repo/.github/workflows/
```

**3. 创建配置文件**

复制带注释的示例并填入你的值:

```sh
cp forgery.toml.example forgery.toml
```

至少需要设置 GitHub 连接键和第一个实例的 Forgejo 设置:

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

**4. 运行 Forgery**

```sh
./forgery
```

默认从当前目录加载 `forgery.toml`; 使用 `--config` 指定其他文件 (例如 `./forgery --config /etc/forgery/forgery.toml`).

**5. 配置反向代理**

Forgery 默认在 `:8443` 上监听纯 HTTP. 生产环境前应放置一个 TLS 终止反向代理 (Caddy, nginx), 并确保 `public_url` 指向公开的 `https://` URL (未设置时由主机名和 `listen_addr` 端口推导).

## 配置

Forgery 通过单个 TOML 文件配置, 使用 `--config` 指定 (默认: 当前目录下的 `forgery.toml`). 仓库根目录的 `forgery.toml.example` 是带完整注释的示例. 加载链为:

1. TOML 文件 (缺失或不可读即报错)
2. 未设置字段的默认值
3. 必填字段与取值的验证

解析是严格的: 未知键, 非法值 (时长, 整数, 布尔), 以及缺失的必填字段都会使启动报错退出 -- 一次解析中发现的全部问题会一起报告. 实例 name 必须唯一: 它是将每个任务映射回所属 Forgejo 实例的路由键.

### 全局键

| 键 | 必填 | 默认值 | 描述 |
|----------|----------|---------|-------------|
| `log_level` | No | `info` | 日志级别: `debug`, `info`, `warn`, 或 `error` |
| `log_format` | No | `json` | 日志输出格式: `json` 或 `text` |
| `health_addr` | No | -- | 健康检查服务监听地址; 为空则不启用 (见 [健康检查](#健康检查)) |
| `listen_addr` | No | `:8443` | 南向 gRPC 监听地址 |
| `public_url` | No | `https://<hostname>:<listen port>` | Forgery 的公网可达 URL (供内部 runner 连接回来) |
| `state_file` | No | `<config dir>/forgery-state.json` | 跨重启持久化 Runner 身份的 state 文件 |
| `max_parallel_tasks` | No | `5` | 最大并发执行任务数 (所有实例共享一个全局预算) |
| `github_token` | Yes | -- | 具有 `workflow` 权限的 GitHub 个人访问令牌 (建议使用细粒度仓库作用域令牌) |
| `github_repo` | Yes | -- | 存放 workflow 的仓库, 格式为 `owner/repo` |
| `github_workflow_id` | Yes | -- | Workflow 文件名 (例如 `forgery-runner.yml`) 或 workflow ID |
| `github_ref` | No | `main` | 触发 workflow 的分支引用 |
| `github_api_url` | No | `https://api.github.com` | GitHub API 基础 URL (使用 GitHub Enterprise 时设置) |

### 实例键

每个 Forgejo 连接对应一个 `[[instances]]` 条目 (至少需要一个):

| 键 | 必填 | 默认值 | 描述 |
|----------|----------|---------|-------------|
| `name` | No | `forgejo_url` 的主机名 | 唯一的实例名; 任务到实例解析的路由键 |
| `forgejo_url` | Yes | -- | Forgejo 实例基础 URL (例如 `https://forgejo.example.com`) |
| `forgejo_runner_token` | Yes | -- | Forgejo runner 注册令牌 |
| `forgejo_runner_name` | No | `forgery` | 在 Forgejo runner 列表中显示的显示名称 |
| `forgejo_runner_labels` | Yes | -- | 逗号分隔的标签及可选容器镜像映射 (见 [标签](#标签)) |
| `poll_interval` | No | `3s` | 北向 `FetchTask` 轮询间隔 |
| `reg_token_ttl` | No | `15m` | 一次性注册令牌的有效期 |
| `ga_startup_timeout` | No | `15m` | 等待 GA workflow 启动及内部 runner 注册的最长时间 |
| `heartbeat_interval` | No | `30s` | 发送 `UpdateTask(state=running)` 心跳的间隔, 从 dispatch 成功后持续到任务进入终态 (关键窗口是等待内部 runner 期间) |
| `tls_insecure_skip_verify` | No | `false` | 跳过北向连接 TLS 证书验证 (仅开发环境) |

`state_file` 默认位于配置文件同目录的 `forgery-state.json`. Forgery 将每个 Forgejo 实例的 Runner 身份 (UUID + 永久令牌) 按 `forgejo_url` 键控保存在其中, 因此重启后复用同一 Runner, 而不会注册孤儿 Runner. 文件以原子方式写入, 权限为 `0600`; 文件损坏或版本不匹配时启动直接报错 -- 身份数据绝不会被静默丢弃.

### 标签

`forgejo_runner_labels` 使用以下格式将 Forgejo `runs-on` 标签映射到容器镜像:

```
label1:docker://image1,label2:docker://image2
```

- `:` 之前的部分是 Forgejo workflow 作业匹配的 **runs-on 标签**.
- `:` 之后的部分是以 `docker://` 为前缀的**容器镜像 URL**.

示例:

```toml
forgejo_runner_labels = "ubuntu-latest:docker://node:20-bookworm,docker:docker://ghcr.io/catthehacker/ubuntu:act-latest"
```

- 当 Forgejo workflow 指定 `runs-on: ubuntu-latest` 时, Forgery 匹配该标签, runner 在 `node:20-bookworm` 中执行.
- 当 `runs-on: docker` 时, 在 `ghcr.io/catthehacker/ubuntu:act-latest` 中执行.

同一个标签字符串驱动两侧: 北向 `Register`/`Declare` 发送裸标签名 (`:docker://…` 镜像映射后缀被剥离 -- Forgejo 只接受裸标签名), 而 `workflow_dispatch` 的 `labels` input 携带完整的 `label:docker://image` 映射, 供 GA workflow 决定使用哪个容器.

### 健康检查

设置 `health_addr` 后, Forgery 在该地址运行健康检查 HTTP 服务: 仅 `/healthz` 和 `/readyz` 返回 `200` 和 `{"status":"ok"}`, 其他任何路径 (包括 `/`) 均返回 `404`. `health_addr` 为空 (默认) 时不启动健康检查服务. 健康检查服务作为守护进程优雅关闭流程的一部分被关闭.

## 工作原理

1. **Forgejo 分发作业** -- 已注册为 runner 的 Forgery 通过 `FetchTask` 接收作业.
2. **Forgery 触发 GitHub Actions Workflow** -- 调用 `workflow_dispatch` API, 传递任务元数据 (代理 URL, 一次性注册令牌, 标签).
3. **GA Workflow 启动 forgejo-runner** -- 下载官方 runner 二进制文件, 以 `one-job` 临时模式启动, 连接回 Forgery 的 gRPC 端点.
4. **Forgery 将任务交给内部 runner** -- runner 获取任务, 在容器内执行, 并报告结果.
5. **日志和任务状态回流** -- `UpdateTask` 和 `UpdateLog` RPC 通过 Forgery 透明中继回 Forgejo.

**多个 Forgejo 实例** -- 每个 `[[instances]]` 条目都有独立的 Runner 身份 (分别向其自己的 Forgejo 注册) 和独立的任务 poller. 每个任务都带有所属实例 name; 注册令牌校验, 心跳, 以及 `UpdateTask`/`UpdateLog` 转发都通过所属实例的北向客户端路由. 南向保持为单一共享端点 (`listen_addr`/`public_url` 是全局配置): 每个 `workflow_dispatch` 携带的一次性注册令牌将连入的内部 runner 映射回其任务, 从而映射回所属实例. `max_parallel_tasks` 是所有实例 poller 共享的全局并发预算.

**通过 Forgery 拉取代码与上传产物** -- 内部 runner 的 `GITHUB_SERVER_URL`, `GITHUB_API_URL` 和产物端点都指向 Forgery 的 `public_url` (forgejo-runner 从 `--url` 推导). 为让 git 流量绕过代理路径, Forgery 在交给 runner 的任务载荷中重写: 每个 `actions/checkout` 步骤注入 `github-server-url`, 并把服务器 URL 表达式 -- `${{ github.server_url }}`, `${{ github.api_url }}`, `${{ github.repository_url }}`, `${{ env.GITHUB_SERVER_URL }}`, `${{ env.GITHUB_API_URL }}`, `${{ env.FORGEJO_SERVER_URL }}`, `${{ env.FORGEJO_API_URL }}` -- 归一化为所属实例的 `forgejo_url` 字面量, 克隆, API 调用与容器仓库推送 (`docker/login-action` 的 `registry:` 输入) 均直连 Forgejo; 残留的 `/api/v1/repos/...` 默认分支查询和 `/api/actions_pipeline/...` 产物端点仍按请求 `Authorization` 头中的任务令牌反向代理 -- 该令牌由 Forgejo 签发, 上游实例直接校验. 未知令牌返回 `401`; 当仅配置一个实例时, 未注册令牌回退到该实例. 无显式 ref 的跨仓库 checkout 需显式 ref: Forgejo 的 API v3 无法为其他仓库提供 checkout 的默认分支查询. 已知边界: `run:` 步骤内通过 shell 环境变量读取的 `$GITHUB_SERVER_URL` 无法改写 -- runner 从 `--url` 硬编码.

**持久的 Runner 身份** -- 首次 `Register` 成功后, Forgery 将 runner UUID 和永久令牌保存到 state 文件. 重启后复用同一身份, 而不会注册一个全新的孤儿 runner; 若 Forgejo 拒绝永久令牌 (被吊销或轮换), Forgery 会自动重新注册并重试. 重新注册会创建**新的** runner 身份 -- 旧身份已失效, Forgejo runner 列表中会出现新条目. 旧身份的在途任务无法恢复: Forgejo 会在大约 10-15 分钟内自动将其标记为失败 (其 zombie 清理定时任务). 若注册令牌本身无效 (在 Forgejo UI 中被重置或配置错误), Forgery 会记录指向 `forgejo_runner_token` 或 Forgejo UI 的错误日志, 并直接跳到 5min 退避上限 -- 令牌修复前, 重试节奏并无帮助. 瞬时性失败 (如网络错误) 则相反: 在尝试之间指数退避, 从 30s 起每次翻倍, 上限 5min, 记录为 Warn 级日志.

## 构建与测试

### 前置要求

- Go 1.25.0
- 访问已启用 Actions 的 Forgejo 实例
- 一个用于 runner workflow 的 GitHub 仓库

### 构建

```sh
go build ./cmd/forgery
```

### 测试

```sh
go test ./... -race
```

所有测试按惯例使用 `-race` 标志运行. `store`, `session`, `slots` 和 `state` 包包含并发访问测试.

### 项目结构

```
forgery/
├── cmd/forgery/       # 主入口 (命令行旗标, 配置加载)
├── internal/
│   ├── app/           # 守护进程装配与生命周期 (多实例接线)
│   ├── config/        # TOML 加载, 默认值与校验
│   ├── dispatch/      # GitHub Actions workflow_dispatch 调用
│   ├── north/         # 北向客户端 (Forgery -> Forgejo)
│   ├── observability/ # 日志和健康检查
│   ├── run/           # 单任务编排
│   ├── session/       # 会话生命周期
│   ├── slots/         # 全局背压池 (max_parallel_tasks)
│   ├── south/         # 南向服务端 (forgejo-runner -> Forgery)
│   ├── state/         # Runner 身份持久化 (state 文件)
│   ├── store/         # 内存任务/会话存储
│   ├── token/         # 加密令牌生成
│   └── version/       # 版本常量
├── templates/
│   └── forgery-runner.yml
├── forgery.toml.example
├── go.mod / go.sum
├── COPYING            # GPL-3.0
└── README.md
```

模块路径: `git.0x0f.dev/forgery`.

## 安全

### 北向

Forgery 通过 HTTPS 连接 Forgejo. 确保 `forgejo_url` 使用 `https://`. 使用自签名证书进行开发时, 在对应的 `[[instances]]` 条目中设置 `tls_insecure_skip_verify = true`.

### 配置文件保护

`forgery.toml` 包含 GitHub 令牌和 Forgejo 注册令牌. 请限制其权限 (例如 `chmod 600 forgery.toml`), 切勿提交到版本库.

### 南向

TLS 在反向代理 (Caddy 或 nginx) 处终止. 代理负责证书管理 (例如 Let's Encrypt). 内部 `forgejo-runner` 实例连接到 `public_url` (应为 `https://`), 信任标准公共 CA.

### 一次性注册令牌

- 每次任务分发通过 `crypto/rand` 生成唯一注册令牌 (32 字节, 256 位熵).
- 令牌**一次性使用** -- 首次成功的 `Register` 调用消耗令牌, 后续尝试被拒绝.
- 令牌在 `reg_token_ttl` (默认 15 分钟) 后过期.
- **警告:** `reg_token` 作为 `workflow_dispatch` 输入传递, **在 GitHub Actions workflow 日志中可见**. 作为预防措施, 应在仓库设置中限制对 Actions 日志的访问.

### 密钥处理

来自 Forgejo 仓库密钥的传入任务**仅在内存中**保存, 并通过 TLS 连接转发. 绝不会写入磁盘或记录到日志中.

### State 文件

State 文件 (默认与配置文件同目录的 `forgery-state.json`) 包含 Runner 凭据 -- 每个 Forgejo 实例的 UUID 和永久令牌, 以 `0600` 权限写入. 请像对待密钥一样保护它; 删除它会使 Forgery 在下次启动时注册全新的 Runner 身份.

### GitHub 个人访问令牌

- `github_token` 应为**细粒度个人访问令牌**, 限定到单个仓库.
- 最低所需权限: `contents: read` 和 `actions: write`.
- Actions workflow 中的内置 `GITHUB_TOKEN` 由 runner workflow 自身使用, 无需额外配置.

## 许可证

Copyright (C) 2026 ShinKouyo &lt;i@0x0f.dev&gt;

本程序为自由软件, 在 Free Software Foundation 发布的 GNU General Public License
的约束下, 你可以对其进行再发布及修改. 协议版本为第三版或 (按你的选择) 任何更新
的版本.

本程序分发时希望它有用, 但**不提供任何担保**; 甚至不提供适销性或特定用途适用性
的默示担保. 详情参见 GNU General Public License.

完整许可证文本见 [COPYING](COPYING).
