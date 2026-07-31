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
	"git.0x0f.dev/forgery/internal/version"
)

// Tunables for workflow_dispatch HTTP calls and retry handling.
const (
	// httpClientTimeout bounds every HTTP request to the GitHub Actions API.
	httpClientTimeout = 30 * time.Second

	// errorBodyLimit caps how many bytes of a non-204 response body are
	// read for error diagnostics (1 KiB is plenty for an API error).
	errorBodyLimit = 1024

	// defaultMaxRetries is the retry count for server (5xx) and network
	// errors applied by NewDispatcher; client errors (4xx) never retry.
	defaultMaxRetries = 3

	// initialBackoff is the first retry delay; each retry doubles it,
	// with ±25% jitter applied.
	initialBackoff = 1 * time.Second
)

// GitHub holds the daemon-wide GitHub Actions connection settings used for
// workflow_dispatch. It is a decoupled subset of config.Global: dispatch
// never sees the full configuration, only these fields.
type GitHub struct {
	Token      string // github_token (required)
	Repo       string // github_repo "owner/repo" (required)
	WorkflowID string // github_workflow_id (required)
	Ref        string // github_ref (default: "main")
	APIURL     string // github_api_url (default: "https://api.github.com")
	PublicURL  string // public_url, sent to the workflow as proxy_url
}

// Dispatcher sends workflow_dispatch requests to the GitHub Actions API
// to trigger execution of a forgejo-runner inside a GitHub Actions Workflow.
type Dispatcher struct {
	client     *http.Client // Timeout: httpClientTimeout
	gh         GitHub
	log        *slog.Logger
	maxRetries int // 5xx/network-error retries; NewDispatcher applies defaultMaxRetries
}

// NewDispatcher creates a Dispatcher with the httpClientTimeout HTTP client
// timeout and the forgery/<version> User-Agent header. gh carries the global
// GitHub Actions settings; per-instance values (labels) arrive at Trigger
// time.
func NewDispatcher(gh GitHub, log *slog.Logger) *Dispatcher {
	return &Dispatcher{
		client: &http.Client{
			Timeout: httpClientTimeout,
		},
		gh:         gh,
		log:        log,
		maxRetries: defaultMaxRetries,
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
	ProxyURL string `json:"proxy_url"`
	RegToken string `json:"reg_token"`
	Labels   string `json:"labels"`
}

// Trigger sends a workflow_dispatch request to the GitHub API to start
// a forgejo-runner for the given task on the given Forgejo instance. It
// retries on server errors (5xx) and network errors with exponential
// backoff. Client errors (4xx) are not retried. Context cancellation is
// respected. inst supplies the per-instance dispatch inputs (labels);
// all GitHub connection settings come from the global GitHub struct
// captured at construction.
func (d *Dispatcher) Trigger(ctx context.Context, taskCtx *store.TaskCtx, inst config.Instance) error {
	return d.triggerWithRetry(ctx, taskCtx, inst, d.maxRetries)
}

// triggerWithRetry calls dispatch with exponential backoff retry.
// It retries up to maxRetries times on server errors (5xx) and network errors.
// Client errors (4xx) are NOT retried.
func (d *Dispatcher) triggerWithRetry(ctx context.Context, taskCtx *store.TaskCtx, inst config.Instance, maxRetries int) error {
	var lastErr error
	backoff := initialBackoff

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

		err := d.dispatch(ctx, taskCtx, inst)
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
// Global connection settings come from d.gh; per-instance inputs (labels)
// come from inst.
func (d *Dispatcher) dispatch(ctx context.Context, taskCtx *store.TaskCtx, inst config.Instance) error {
	url := fmt.Sprintf("%s/repos/%s/actions/workflows/%s/dispatches",
		d.gh.APIURL, d.gh.Repo, d.gh.WorkflowID)

	body := dispatchInputs{
		Ref: d.gh.Ref,
		Inputs: dispatchInput{
			ProxyURL: d.gh.PublicURL,
			RegToken: taskCtx.RegToken,
			Labels:   strings.Join(inst.ForgejoRunnerLabels, ","),
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

	req.Header.Set("Authorization", "Bearer "+d.gh.Token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "forgery/"+version.Version)

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
			"http_status", resp.StatusCode,
		)
		return nil
	}

	// Read up to errorBodyLimit (1 KiB) of the response body for diagnostics.
	limitedReader := io.LimitReader(resp.Body, errorBodyLimit)
	respBody, _ := io.ReadAll(limitedReader)

	d.log.Error("workflow_dispatch failed",
		"task_id", taskCtx.ID,
		"http_status", resp.StatusCode,
		"body", string(respBody),
	)

	return &DispatchError{
		StatusCode: resp.StatusCode,
		Body:       string(respBody),
	}
}
