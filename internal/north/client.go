// Package north implements the northbound client — forgery acting as a
// Forgejo Actions runner, connecting to a real Forgejo instance via the
// Connect (gRPC-over-HTTP) protocol. It handles runner registration,
// task polling, and transparent forwarding of task status/log updates.
package north

import (
	"context"
	"crypto/tls"
	"log/slog"
	"net/http"
	"time"

	v1 "code.gitea.io/actions-proto-go/runner/v1"
	"code.gitea.io/actions-proto-go/runner/v1/runnerv1connect"
	"connectrpc.com/connect"

	"git.0x0f.dev/forgery/internal/config"
	"git.0x0f.dev/forgery/internal/store"
)

// Client is the northbound client that connects to the real Forgejo instance.
// It registers as an Actions runner, polls for tasks, and forwards
// status/log updates from the internal runner back to Forgejo.
type Client struct {
	client runnerv1connect.RunnerServiceClient
	cfg    *config.Config
	store  store.TaskStore
	sem    chan struct{} // backpressure semaphore
	log    *slog.Logger
}

// New creates a new northbound Client.
//
// It constructs a net/http.Client with TLS configuration from cfg
// and wraps it with the Connect-generated RunnerServiceClient.
// maxParallel controls the backpressure semaphore capacity.
func New(cfg *config.Config, s store.TaskStore, maxParallel int, log *slog.Logger) *Client {
	httpClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: cfg.TLSInsecureSkipVerify, // #nosec G402
			},
		},
	}

	return &Client{
		client: runnerv1connect.NewRunnerServiceClient(httpClient, cfg.ForgejoURL),
		cfg:    cfg,
		store:  s,
		sem:    make(chan struct{}, maxParallel),
		log:    log,
	}
}

// Register calls the Forgejo Register RPC with the configured runner
// token, name, labels, and version. It must be called once at startup
// before Declare or FetchTask.
func (c *Client) Register(ctx context.Context) error {
	req := connect.NewRequest(&v1.RegisterRequest{
		Token:   c.cfg.ForgejoRunnerToken,
		Name:    c.cfg.ForgejoRunnerName,
		Labels:  c.cfg.ForgejoRunnerLabels,
		Version: "1.0.0",
	})
	_, err := c.client.Register(ctx, req)
	return err
}

// Declare announces the runner's labels and version to Forgejo.
// It must be called after a successful Register and before polling
// for tasks.
func (c *Client) Declare(ctx context.Context) error {
	req := connect.NewRequest(&v1.DeclareRequest{
		Labels:  c.cfg.ForgejoRunnerLabels,
		Version: "1.0.0",
	})
	_, err := c.client.Declare(ctx, req)
	return err
}

// ForwardUpdateTask transparently relays an UpdateTask request from the
// internal runner (southbound) to the real Forgejo instance (northbound).
// The request payload is passed through unchanged because task IDs are
// the real Forgejo task IDs.
func (c *Client) ForwardUpdateTask(ctx context.Context, req *v1.UpdateTaskRequest) (*v1.UpdateTaskResponse, error) {
	resp, err := c.client.UpdateTask(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

// ForwardUpdateLog transparently relays an UpdateLog request from the
// internal runner to the real Forgejo instance. As with ForwardUpdateTask,
// the payload is passed through unchanged.
func (c *Client) ForwardUpdateLog(ctx context.Context, req *v1.UpdateLogRequest) (*v1.UpdateLogResponse, error) {
	resp, err := c.client.UpdateLog(ctx, connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

// ReleaseSlot releases one backpressure slot. It must be called by the
// run module when a task reaches a terminal state (success, failure, or
// cancelled). The slot was acquired in PollLoop when the task was fetched.
func (c *Client) ReleaseSlot() {
	select {
	case <-c.sem:
	default:
	}
}

// StartHeartbeat sends periodic UpdateTask(state=running) calls to Forgejo
// for the given task until ctx is cancelled. This prevents Forgejo from
// marking the task as stalled during the window between a successful
// workflow_dispatch and the internal runner connecting to pick up the task.
//
// The heartbeat uses cfg.HeartbeatInterval as the tick period. Errors are
// logged but do not stop the heartbeat — Forgejo tolerates missed heartbeats
// within a grace period.
//
// Callers (the run module) must cancel ctx when the internal runner
// connects and sends its own UpdateTask, or when the GA startup timeout
// expires.
func (c *Client) StartHeartbeat(ctx context.Context, taskCtx *store.TaskCtx) {
	interval := c.cfg.HeartbeatInterval
	if interval <= 0 {
		interval = 30 * time.Second
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Build the UpdateTask request once — it never changes across ticks.
	req := &v1.UpdateTaskRequest{
		State: &v1.TaskState{
			Id:     taskCtx.ID,
			Result: v1.Result_RESULT_UNSPECIFIED, // non-terminal = running
		},
	}

	c.log.Debug("heartbeat started", "task_id", taskCtx.ID, "interval", interval)

	for {
		select {
		case <-ctx.Done():
			c.log.Debug("heartbeat stopped", "task_id", taskCtx.ID, "reason", ctx.Err())
			return
		case <-ticker.C:
			if _, err := c.client.UpdateTask(ctx, connect.NewRequest(req)); err != nil {
				c.log.Warn("heartbeat UpdateTask failed", "task_id", taskCtx.ID, "err", err)
			}
		}
	}
}
