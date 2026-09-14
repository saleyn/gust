# GustK8s - Kubernetes Pod Execution Adapter

This adapter enables Gust DAGs to execute tasks as Kubernetes pods, providing native K8s integration without requiring external Python scripts.

## Features

- **Native K8s Pod Execution**: Run DAG tasks as short-lived Kubernetes pods
- **Flexible Configuration**: Support for custom namespaces, node selectors, restart policies, and more
- **In-Cluster & Local Support**: Automatically detects in-cluster authentication or falls back to kubeconfig
- **Pod Lifecycle Management**: Automatic pod creation, monitoring, log collection, and cleanup
- **YAML-Based Configuration**: Define K8s tasks directly in shell YAML DAGs using `handler: k8s`

## Installation

Enable K8s support when building Gust:

```bash
GUST_WITH_K8S=true mix release
```

Or for development:

```bash
GUST_WITH_K8S=true mix test
```

## Usage

### Define K8s Tasks in YAML DAGs

```yaml
tasks:
  - name: run_batch_job
    handler: k8s
    image: python:3.11-slim
    command: ["python"]
    args: ["/app/process.py"]
    namespace: batch
    pod_name: job-{{.task.id}}-{{.task.attempt}}
    restart_policy: Never
    node_selector:
      workload-type: batch
    downstream: [validate_results]

  - name: validate_results
    handler: k8s
    image: busybox:latest
    command: ["/bin/sh", "-c"]
    args: ["echo 'Validation passed'"]
```

### Task Configuration Options

- **image** (required): Docker image URI
- **command** (optional): Container command (array of strings)
- **args** (optional): Container arguments (array of strings)
- **namespace** (optional): K8s namespace, defaults to `default`
- **pod_name** (optional): Pod name, supports templating with `{{.task.id}}` and `{{.task.attempt}}`; auto-generated if omitted
- **restart_policy** (optional): K8s restart policy, defaults to `Never`
- **node_selector** (optional): K8s node selector labels (map)
- **downstream** (optional): List of downstream task names
- **store_result** or **save** (optional): Whether to store task output

### Template Variables

Pod names can use templates for dynamic naming:

- `{{.task.id}}` - Unique task ID
- `{{.task.attempt}}` - Current attempt number
- `{{.task.name}}` - Task name

Example: `pod_name: gust-{{.task.name}}-{{.task.id}}-{{.task.attempt}}`

## Configuration

### Environment Variables

- `GUST_WITH_K8S` - Enable K8s adapter (`true`)
- `KUBECONFIG` - Path to kubeconfig file (optional, defaults to `~/.kube/config`)

### Application Config

Customize K8s behavior in `config/config.exs`:

```elixir
config :gust_k8s,
  k8s_api_poll_interval: 2_000,           # Poll interval in milliseconds
  k8s_task_timeout: 30 * 60 * 1_000,      # Maximum task duration
  k8s_pod_name_prefix: "gust-",           # Pod name prefix
  k8s_default_namespace: "default",       # Default namespace
  k8s_default_restart_policy: "Never"     # Default restart policy
```

## Authentication

The adapter supports multiple authentication methods:

### 1. In-Cluster (Default)

When running inside a Kubernetes cluster, the adapter automatically uses the pod's service account:

- Token: `/var/run/secrets/kubernetes.io/serviceaccount/token`
- CA Certificate: `/var/run/secrets/kubernetes.io/serviceaccount/ca.crt`

### 2. Kubeconfig File

Falls back to kubeconfig authentication when not running in-cluster:

- Reads from `$KUBECONFIG` environment variable
- Or `~/.kube/config`
- Supports bearer token authentication

## Architecture

### Components

- **K8sClient** - Direct Kubernetes API REST client using Req library
- **PodWatcher** - Polls pod status until completion (Pending → Running → Succeeded/Failed)
- **ConfigLoader** - Handles authentication and API server discovery
- **Template** - Renders dynamic pod names with task context
- **Parser.Adapter** - Validates YAML task configuration for K8s handler
- **TaskWorker.Adapter** - Manages pod lifecycle during task execution
- **Runtime.Adapter** - Minimal setup/teardown (no special lifecycle needed)

### Adapter Pattern

K8s tasks integrate with Gust's pluggable adapter system:

- K8s tasks are invoked via the shell parser's `handler: k8s` option
- Shell task worker delegates K8s tasks to `GustK8s.TaskWorker.Adapter`
- Adapter registration happens in `config/config.exs`

## Testing

Run K8s adapter tests:

```bash
cd apps/gust_k8s && mix test
```

Tests use Mox for mocking K8s API calls. Configure mocks in test setup:

```elixir
setup do
  Application.put_env(:gust_k8s, :k8s_client, GustK8s.K8sClientMock)
  Application.put_env(:gust_k8s, :pod_watcher, GustK8s.PodWatcherMock)
end
```

## Pod Lifecycle

1. **Pod Creation**: Pod spec is created and sent to Kubernetes API
2. **Polling**: Status is polled at regular intervals until pod reaches terminal phase
3. **Log Collection**: Pod logs are collected once pod completes
4. **Pod Cleanup**: Pod is deleted automatically
5. **Result Return**: Task result (success/failure + logs) is returned to DAG runner

### Phases Monitored

- **Pending** → Continue polling
- **Running** → Continue polling
- **Succeeded** → Success (exit code 0)
- **Failed** → Error
- **Unknown** → Continue polling (edge case)

## Error Handling

Tasks fail with clear error messages for:

- Pod creation failures (image not found, etc.)
- Pod execution failures (exit code non-zero)
- Pod watch timeouts (configurable, default 30 minutes)
- API authentication failures (invalid credentials)
- Missing configuration (kubeconfig, in-cluster auth unavailable)

## Limitations & Future Work

- Live log streaming not implemented (logs collected on completion)
- No pod resource limits configuration (can be added)
- No init containers support (can be added)
- No volume mounts (can be added if needed)
- Single container per pod (can extend to support multiple)

## Troubleshooting

### Pod Not Found After Creation

This typically indicates the pod was quickly cleaned up. Check:
- Pod logs before deletion
- K8s namespace is correct
- Service account has permissions

### TLS/Certificate Errors

When using kubeconfig outside cluster:
- Verify `~/.kube/config` has valid CA certificate
- Check `$KUBECONFIG` is not pointing to expired config
- Ensure certificate is trusted by system

### Task Timeout

If tasks are timing out:
- Increase `k8s_task_timeout` configuration
- Check pod resource requests/limits
- Verify container image exists and can be pulled

## License

See parent project for license information.
