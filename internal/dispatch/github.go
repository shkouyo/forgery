// Package dispatch handles GitHub Actions workflow_dispatch API calls.
// When forgery receives a task from Forgejo, it triggers a GitHub Actions
// Workflow that starts an internal forgejo-runner to execute the task.
package dispatch

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strings"
	"time"

	"log/slog"

	"git.0x0f.dev/forgery/internal/config"
	"git.0x0f.dev/forgery/internal/store"
)

// Dispatcher sends workflow_dispatch requests to the GitHub Actions API
// to trigger execution of a forgejo-runner inside a GitHub Actions Workflow.
type Dispatcher struct {
	client     *http.Client // Timeout: 30s
	cfg        *config.Config
	log        *slog.Logger
	maxRetries int // default 3
}

// NewDispatcher creates a Dispatcher with a 30-second HTTP client timeout
// and the forgery/1.0.0 User-Agent header.
func NewDispatcher(cfg *config.Config, log *slog.Logger) *Dispatcher {
	return &Dispatcher{
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
		cfg:        cfg,
		log:        log,
		maxRetries: 3,
	}
}

// DispatchError wraps a non-204 response from the GitHub Actions API.
type DispatchError struct {
	StatusCode int
	Body       string
}

// Error implements the error interface.
func (e *DispatchError) Error() string {
	return fmt.Sprintf("dispatch: GitHub API returned %d: %s", e.StatusCode, e.Body)
}

// dispatchInputs is the JSON body sent to the workflow_dispatch endpoint.
type dispatchInputs struct {
	Ref    string        `json:"ref"`
	Inputs dispatchInput `json:"inputs"`
}

type dispatchInput struct {
	ProxyURL        string `json:"proxy_url"`
	RegToken        string `json:"reg_token"`
	Labels          string `json:"labels"`
	ContainerImage  string `json:"container_image"`
	TaskID          string `json:"task_id"`
}

// Trigger sends a workflow_dispatch request to the GitHub API to start
// a forgejo-runner for the given task. It retries on server errors (5xx)
// and network errors with exponential backoff. Client errors (4xx) are
// not retried. Context cancellation is respected.
func (d *Dispatcher) Trigger(ctx context.Context, taskCtx *store.TaskCtx) error {
	return d.triggerWithRetry(ctx, taskCtx, d.maxRetries)
}

// triggerWithRetry calls dispatch with exponential backoff retry.
// It retries up to maxRetries times on server errors (5xx) and network errors.
// Client errors (4xx) are NOT retried.
func (d *Dispatcher) triggerWithRetry(ctx context.Context, taskCtx *store.TaskCtx, maxRetries int) error {
	var lastErr error
	backoff := 1 * time.Second

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			d.log.Warn("retrying dispatch",
				"attempt", attempt,
				"task_id", taskCtx.ID,
				"err", lastErr,
			)
			// Exponential backoff with ±25% jitter.
			jitter := time.Duration(float64(backoff) * (0.75 + 0.5*rand.Float64()))
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(jitter):
			}
			backoff *= 2
		}

		err := d.dispatch(ctx, taskCtx)
		if err == nil {
			return nil
		}
		lastErr = err

		// Don't retry client errors (4xx).
		var dispErr *DispatchError
		if errors.As(err, &dispErr) && dispErr.StatusCode >= 400 && dispErr.StatusCode < 500 {
			return err
		}
	}

	return fmt.Errorf("dispatch failed after %d retries: %w", maxRetries, lastErr)
}

// dispatch sends a single workflow_dispatch request to the GitHub API.
func (d *Dispatcher) dispatch(ctx context.Context, taskCtx *store.TaskCtx) error {
	url := fmt.Sprintf("%s/repos/%s/actions/workflows/%s/dispatches",
		d.cfg.GHApiURL, d.cfg.GitHubRepo, d.cfg.GitHubWorkflowID)

	body := dispatchInputs{
		Ref: d.cfg.GitHubRef,
		Inputs: dispatchInput{
			ProxyURL:       d.cfg.PublicURL,
			RegToken:       taskCtx.RegToken,
			Labels:         strings.Join(d.cfg.ForgejoRunnerLabels, ","),
			ContainerImage: d.cfg.DefaultContainerImage,
			TaskID:         fmt.Sprintf("%d", taskCtx.ID),
		},
	}

	payload, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("dispatch: marshal body: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("dispatch: create request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+d.cfg.GitHubToken)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "forgery/1.0.0")

	d.log.Debug("triggering workflow_dispatch",
		"url", url,
		"task_id", taskCtx.ID,
	)

	resp, err := d.client.Do(req)
	if err != nil {
		// Context cancellation surfaces as an error from the HTTP client.
		return fmt.Errorf("dispatch: http request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNoContent {
		d.log.Info("workflow_dispatch triggered",
			"task_id", taskCtx.ID,
			"status", resp.StatusCode,
		)
		return nil
	}

	// Read up to 1KB of the response body for error diagnostics.
	limitedReader := io.LimitReader(resp.Body, 1024)
	respBody, _ := io.ReadAll(limitedReader)

	d.log.Error("workflow_dispatch failed",
		"task_id", taskCtx.ID,
		"status", resp.StatusCode,
		"body", string(respBody),
	)

	return &DispatchError{
		StatusCode: resp.StatusCode,
		Body:       string(respBody),
	}
}
