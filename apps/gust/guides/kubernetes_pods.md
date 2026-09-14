# Executing Tasks as Kubernetes Pods

Gust supports executing DAG tasks as Kubernetes pods through the `gust_k8s` adapter. This guide shows how to define and run tasks in Kubernetes clusters, whether running Gust itself inside Kubernetes or accessing remote clusters via kubeconfig.

## When to Use Kubernetes Pods

Use K8s pod tasks when you need to:

- Run computationally intensive workloads on cluster nodes
- Isolate tasks in separate containers with specific resource limits
- Leverage Kubernetes scheduling (node selectors, affinity, tolerations)
- Use images already deployed in your container registry
- Execute tasks in a cluster environment separate from Gust itself

For simple shell commands or Elixir code execution, consider the shell adapter instead.

## Installation

Enable K8s pod support when building Gust:

```bash
GUST_WITH_K8S=true mix release
```

For development:

```bash
GUST_WITH_K8S=true mix test
```

## Basic Configuration

### YAML DAG Format

Define K8s tasks in shell YAML DAGs by specifying `handler: k8s`:

```yaml
tasks:
  - name: process_data
    handler: k8s
    image: python:3.11-slim
    command: ["python"]
    args: ["/app/process.py"]
    downstream: [validate_results]

  - name: validate_results
    handler: k8s
    image: busybox:latest
    command: ["/bin/sh", "-c"]
    args: ["echo 'Validation complete'"]
```

Save as `dags/pipeline.yml` and load via your DAG source (Git, S3, Folder, Database).

### Task Options

All K8s task options:

- **image** *(required)* — Container image URI (e.g., `python:3.11-slim`)
- **command** *(optional)* — Container entrypoint (array of strings)
- **args** *(optional)* — Container arguments (array of strings)
- **namespace** *(optional)* — Kubernetes namespace; defaults to `default`
- **pod_name** *(optional)* — Pod name; auto-generated if omitted
- **restart_policy** *(optional)* — Kubernetes restart policy; defaults to `Never`
- **node_selector** *(optional)* — Node selector labels (map of key-value pairs)
- **downstream** *(optional)* — Downstream task names (list)
- **store_result** or **save** *(optional)* — Store task output for downstream access

## Advanced Examples

### Dynamic Pod Naming

Use task context variables in pod names:

```yaml
tasks:
  - name: batch_job
    handler: k8s
    image: batch-processor:latest
    pod_name: batch-{{.task.id}}-attempt-{{.task.attempt}}
    downstream: []
```

Available variables:
- `{{.task.id}}` — Unique task ID
- `{{.task.attempt}}` — Attempt number (1, 2, 3, ...)
- `{{.task.name}}` — Task name

### Node Selection

Schedule tasks on specific nodes using selectors:

```yaml
tasks:
  - name: gpu_training
    handler: k8s
    image: tensorflow:latest-gpu
    command: ["python", "/ml/train.py"]
    node_selector:
      accelerator: gpu
      workload-type: ml
    downstream: [evaluate_model]

  - name: evaluate_model
    handler: k8s
    image: sklearn:latest
    node_selector:
      workload-type: ml
    downstream: []
```

### Restart Policies

Configure how Kubernetes handles task failures:

```yaml
tasks:
  - name: transient_job
    handler: k8s
    image: worker:latest
    restart_policy: OnFailure
    downstream: []

  - name: one_shot_job
    handler: k8s
    image: unique-task:latest
    restart_policy: Never
    downstream: []
```

Policies: `Never`, `OnFailure`, `Always`

### Chaining Tasks

Create dependencies between pod tasks:

```yaml
tasks:
  - name: extract
    handler: k8s
    image: etl-tool:latest
    command: ["extract-data"]
    args: ["--source", "s3://bucket/data"]
    downstream: [transform, load]
    save: true

  - name: transform
    handler: k8s
    image: etl-tool:latest
    command: ["transform-data"]
    downstream: [validate]

  - name: load
    handler: k8s
    image: etl-tool:latest
    command: ["load-data"]
    args: ["--dest", "postgres://db"]
    downstream: [validate]

  - name: validate
    handler: k8s
    image: etl-tool:latest
    command: ["validate-load"]
```

## Authentication

The K8s adapter automatically detects and uses the appropriate authentication method:

### In-Cluster (Default)

When Gust runs inside a Kubernetes cluster, the adapter uses the pod's service account:

- **Token**: `/var/run/secrets/kubernetes.io/serviceaccount/token`
- **CA Certificate**: `/var/run/secrets/kubernetes.io/serviceaccount/ca.crt`
- **API Server**: `https://kubernetes.default.svc`

No configuration needed; just run Gust as a pod with appropriate RBAC permissions.

### Kubeconfig (Local/Remote)

When running outside a cluster, the adapter reads kubeconfig:

1. Check `$KUBECONFIG` environment variable
2. Fall back to `~/.kube/config`
3. Use bearer token authentication from kubeconfig

Example setup:

```bash
export KUBECONFIG=/path/to/kubeconfig
GUST_WITH_K8S=true mix ecto.setup
mix phx.server
```

## Configuration

Customize K8s behavior in `config/config.exs`:

```elixir
config :gust_k8s,
  # Poll pod status every N milliseconds
  k8s_api_poll_interval: 2_000,

  # Maximum task duration (milliseconds)
  k8s_task_timeout: 30 * 60 * 1_000,

  # Pod name prefix for auto-generated names
  k8s_pod_name_prefix: "gust-",

  # Default Kubernetes namespace
  k8s_default_namespace: "default",

  # Default restart policy
  k8s_default_restart_policy: "Never"
```

## Pod Lifecycle

Understanding pod execution flow helps with debugging:

1. **Pod Creation** — Pod spec is sent to Kubernetes API
2. **Polling** — Status checked every `k8s_api_poll_interval` ms
3. **Phase Transitions**:
   - `Pending` → Pod is being scheduled/pulled
   - `Running` → Container is executing
   - `Succeeded` → Completed successfully (exit code 0)
   - `Failed` → Container exited with non-zero code
4. **Log Collection** — Container logs retrieved after completion
5. **Cleanup** — Pod automatically deleted

Total timeout: `k8s_task_timeout` (default 30 minutes)

## Real-World Example

Complete ETL pipeline with K8s pods:

```yaml
tasks:
  # Extract from data source
  - name: extract_raw
    handler: k8s
    image: etl-scripts:latest
    command: ["python", "/scripts/extract.py"]
    args:
      - "--source"
      - "s3://raw-data/2024-10"
      - "--output"
      - "/tmp/extracted.parquet"
    node_selector:
      workload: etl
    save: true
    downstream: [validate_extract, transform]

  # Validate extracted data
  - name: validate_extract
    handler: k8s
    image: data-quality:latest
    command: ["python", "/scripts/validate.py"]
    args:
      - "--input"
      - "/tmp/extracted.parquet"
      - "--rules"
      - "extraction_schema.yaml"
    downstream: []

  # Transform data
  - name: transform
    handler: k8s
    image: etl-scripts:latest
    command: ["python", "/scripts/transform.py"]
    args:
      - "--input"
      - "/tmp/extracted.parquet"
      - "--output"
      - "/tmp/transformed.parquet"
    node_selector:
      workload: etl
    save: true
    downstream: [load_warehouse, archive]

  # Load into data warehouse
  - name: load_warehouse
    handler: k8s
    image: warehouse-client:latest
    command: ["warehouse", "load"]
    args:
      - "--table"
      - "analytics.events"
      - "--input"
      - "/tmp/transformed.parquet"
    node_selector:
      workload: etl
    downstream: [notify_success]

  # Archive for compliance
  - name: archive
    handler: k8s
    image: storage-tools:latest
    command: ["archive", "store"]
    args:
      - "--source"
      - "/tmp/transformed.parquet"
      - "--dest"
      - "s3://archive/processed/2024-10"
    downstream: [notify_success]

  # Notify completion
  - name: notify_success
    handler: k8s
    image: notification-service:latest
    command: ["notify"]
    args:
      - "--channel"
      - "slack"
      - "--message"
      - "ETL pipeline completed successfully"
    downstream: []
```

## Troubleshooting

### Pod Creation Fails

**Error**: `Pod creation failed: image not found`

**Solutions**:
- Verify image URI is correct and accessible from cluster
- Check image pull secrets if using private registry
- Ensure node has internet access to pull from registry

### Pod Timeout

**Error**: `Pod polling timed out after 30 minutes`

**Solutions**:
- Increase `k8s_task_timeout` configuration
- Check pod resources; may need more CPU/memory
- Review pod logs: `kubectl logs pod-name -n namespace`

### Authentication Failures

**Error**: `Could not determine Kubernetes API server`

**Solutions**:
- If in-cluster: verify pod has service account mounted
- If local: set `KUBECONFIG` or ensure `~/.kube/config` exists
- Check RBAC permissions: pod service account needs `create`, `get`, `delete`, `list` on pods

### Logs Not Available

**Error**: Pod completed but logs are empty

**Solutions**:
- Container may not have written to stdout/stderr
- Check container logs manually: `kubectl logs pod-name`
- Verify container command produces output

## Performance Tips

- Use smaller images for faster pull times
- Set node selectors to avoid scheduling on unrelated nodes
- Batch related tasks to run on same nodes
- Monitor `k8s_api_poll_interval` — smaller = more responsive, but higher API load
- Use `restart_policy: Never` to avoid pod retries (Gust handles retries)

## Next Steps

- See [DAG Sources](dag_sources.md) for loading YAML DAGs from Git, S3, or databases
- See [Writing DAGs](writing_dags.md) for combining K8s pods with Elixir tasks
- Review `apps/gust_k8s/README.md` for technical architecture details
