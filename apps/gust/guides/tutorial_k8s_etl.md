# Tutorial: Building an ETL Pipeline with Kubernetes Pods

In this tutorial, you'll build a complete ETL (Extract, Transform, Load) pipeline using Gust and Kubernetes pods. This is a practical example that demonstrates real-world usage patterns.

## Prerequisites

- Gust running with K8s support: `GUST_WITH_K8S=true`
- Access to a Kubernetes cluster (local or remote)
- Kubeconfig configured if running outside cluster
- Basic familiarity with YAML and ETL concepts

## Scenario

You have CSV data in S3 that needs to be:
1. Extracted (download from S3)
2. Validated (check data quality)
3. Transformed (clean and normalize)
4. Loaded (insert into PostgreSQL)
5. Archived (backup to S3)
6. Notified (send status to Slack)

## Step 1: Create Docker Images

First, create the container images your tasks will use. We'll need three images:

### Image 1: Data Processor

Create `Dockerfile.processor`:

```dockerfile
FROM python:3.11-slim

RUN pip install boto3 pandas pyarrow

WORKDIR /app
COPY scripts/process.py /app/

ENTRYPOINT ["python", "process.py"]
```

### Image 2: Data Validator

Create `Dockerfile.validator`:

```dockerfile
FROM python:3.11-slim

RUN pip install pandas pyyaml

WORKDIR /app
COPY scripts/validate.py /app/

ENTRYPOINT ["python", "validate.py"]
```

### Image 3: Notification Service

Create `Dockerfile.notifier`:

```dockerfile
FROM python:3.11-slim

RUN pip install slack-sdk

WORKDIR /app
COPY scripts/notify.py /app/

ENTRYPOINT ["python", "notify.py"]
```

Build and push to your container registry:

```bash
docker build -f Dockerfile.processor -t myregistry.io/etl-processor:v1 .
docker build -f Dockerfile.validator -t myregistry.io/etl-validator:v1 .
docker build -f Dockerfile.notifier -t myregistry.io/etl-notifier:v1 .

docker push myregistry.io/etl-processor:v1
docker push myregistry.io/etl-validator:v1
docker push myregistry.io/etl-notifier:v1
```

## Step 2: Create the YAML DAG

Create `dags/etl_pipeline.yml`:

```yaml
# ETL Pipeline: Extract, Transform, Load data from S3 to PostgreSQL
tasks:
  # Step 1: Extract raw data from S3
  - name: extract_data
    handler: k8s
    image: myregistry.io/etl-processor:v1
    command: ["python", "/app/process.py"]
    args:
      - "extract"
      - "--source"
      - "s3://data-bucket/raw/2024-10-01/events.csv"
      - "--output"
      - "/tmp/extracted.parquet"
    namespace: etl
    pod_name: etl-extract-{{.task.id}}-{{.task.attempt}}
    restart_policy: Never
    node_selector:
      workload: batch
      tier: worker
    save: true
    downstream: [validate_schema, transform_data]

  # Step 2a: Validate data schema and completeness
  - name: validate_schema
    handler: k8s
    image: myregistry.io/etl-validator:v1
    command: ["python", "/app/validate.py"]
    args:
      - "--stage"
      - "extraction"
      - "--input"
      - "/tmp/extracted.parquet"
    namespace: etl
    pod_name: etl-validate-{{.task.id}}-{{.task.attempt}}
    restart_policy: Never
    node_selector:
      workload: batch
    save: true
    downstream: [check_validation]

  # Step 2b: Wait for validation before proceeding
  - name: check_validation
    handler: k8s
    image: busybox:latest
    command: ["/bin/sh", "-c"]
    args: ["echo 'Data validated successfully'"]
    namespace: etl
    downstream: []

  # Step 3: Transform and clean the data
  - name: transform_data
    handler: k8s
    image: myregistry.io/etl-processor:v1
    command: ["python", "/app/process.py"]
    args:
      - "transform"
      - "--input"
      - "/tmp/extracted.parquet"
      - "--output"
      - "/tmp/transformed.parquet"
      - "--mapping"
      - "column_mappings.json"
    namespace: etl
    pod_name: etl-transform-{{.task.id}}-{{.task.attempt}}
    restart_policy: Never
    node_selector:
      workload: batch
      tier: worker
    save: true
    downstream: [load_postgres, archive_raw]

  # Step 4: Load into PostgreSQL
  - name: load_postgres
    handler: k8s
    image: myregistry.io/etl-processor:v1
    command: ["python", "/app/process.py"]
    args:
      - "load"
      - "--input"
      - "/tmp/transformed.parquet"
      - "--database"
      - "postgresql://etl-user@postgres-svc:5432/analytics"
      - "--table"
      - "events"
    namespace: etl
    pod_name: etl-load-{{.task.id}}-{{.task.attempt}}
    restart_policy: Never
    node_selector:
      workload: batch
    save: true
    downstream: [notify_completion]

  # Step 5: Archive the raw data
  - name: archive_raw
    handler: k8s
    image: myregistry.io/etl-processor:v1
    command: ["python", "/app/process.py"]
    args:
      - "archive"
      - "--source"
      - "/tmp/extracted.parquet"
      - "--destination"
      - "s3://archive/raw/2024-10-01/events.parquet"
    namespace: etl
    pod_name: etl-archive-{{.task.id}}-{{.task.attempt}}
    restart_policy: Never
    node_selector:
      workload: batch
    downstream: [notify_completion]

  # Step 6: Notify Slack
  - name: notify_completion
    handler: k8s
    image: myregistry.io/etl-notifier:v1
    command: ["python", "/app/notify.py"]
    args:
      - "--webhook"
      - "$SLACK_WEBHOOK_URL"
      - "--message"
      - "ETL pipeline completed successfully"
    namespace: etl
    pod_name: etl-notify-{{.task.id}}
    restart_policy: Never
    downstream: []
```

## Step 3: Configure Gust

### Update `config/config.exs`

```elixir
config :gust_k8s,
  # Check pod status every 2 seconds
  k8s_api_poll_interval: 2_000,

  # Allow up to 1 hour for ETL tasks
  k8s_task_timeout: 60 * 60 * 1_000,

  # Prefix for auto-generated pod names
  k8s_pod_name_prefix: "gust-",

  # Use ETL namespace by default
  k8s_default_namespace: "etl",

  # Never restart pods; Gust handles retries
  k8s_default_restart_policy: "Never"
```

### Create Kubernetes Resources

Create RBAC and namespace:

```bash
kubectl create namespace etl

# Create service account
kubectl create serviceaccount gust-etl -n etl

# Create role for pod operations
kubectl create role gust-pod-runner \
  --verb=create,get,delete,list \
  --resource=pods \
  -n etl

# Bind role to service account
kubectl create rolebinding gust-pod-runner \
  --role=gust-pod-runner \
  --serviceaccount=etl:gust-etl \
  -n etl

# Create ConfigMap for pipeline config
kubectl create configmap etl-config \
  --from-file=column_mappings.json \
  -n etl

# Create Secret for credentials
kubectl create secret generic etl-credentials \
  --from-literal=slack-webhook-url=$SLACK_WEBHOOK_URL \
  --from-literal=postgres-password=$PG_PASSWORD \
  -n etl
```

### Deploy Gust with K8s Service Account

Create `kubernetes/gust-deployment.yaml`:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: gust
  namespace: etl
spec:
  replicas: 1
  selector:
    matchLabels:
      app: gust
  template:
    metadata:
      labels:
        app: gust
    spec:
      serviceAccountName: gust-etl
      containers:
      - name: gust
        image: myregistry.io/gust:latest
        env:
        - name: GUST_WITH_K8S
          value: "true"
        - name: DATABASE_URL
          value: "postgresql://gust:password@postgres-svc:5432/gust"
        ports:
        - containerPort: 4000
          name: web
        volumeMounts:
        - name: dags
          mountPath: /app/dags
      volumes:
      - name: dags
        configMap:
          name: etl-dags
```

Deploy:

```bash
kubectl apply -f kubernetes/gust-deployment.yaml
```

## Step 4: Load the DAG

### Option A: Git Source (Recommended)

Push your `dags/etl_pipeline.yml` to a Git repo and configure:

```elixir
config :gust, :dag_source, :git,
  url: "https://github.com/myorg/dags.git",
  branch: "main",
  path: "etl/"
```

### Option B: Folder Source

Copy the DAG file locally:

```bash
mkdir -p priv/dags
cp dags/etl_pipeline.yml priv/dags/
```

Configure:

```elixir
config :gust, :dag_source, :folder,
  path: "priv/dags"
```

### Option C: S3 Source

Upload to S3:

```bash
aws s3 cp dags/etl_pipeline.yml s3://my-dags-bucket/etl/
```

Configure:

```elixir
config :gust, :dag_source, :s3,
  bucket: "my-dags-bucket",
  prefix: "etl/"
```

## Step 5: Run the Pipeline

### Manually Trigger

Via Gust Web UI:
1. Navigate to `/dags`
2. Click `etl_pipeline`
3. Click "New Run"
4. Optionally set parameters
5. Click "Submit"

Via HTTP API:

```bash
curl -X POST http://localhost:4000/api/dags/etl_pipeline/runs \
  -H "Content-Type: application/json" \
  -d '{"params": {"date": "2024-10-01"}}'
```

### View Results

Monitor in the Web UI:
- Task status (running, succeeded, failed)
- Pod names and namespaces
- Log output from each pod
- Task duration and timing

### Debug Failed Tasks

Check pod logs:

```bash
kubectl logs etl-extract-123-1 -n etl

# Get pod details
kubectl describe pod etl-extract-123-1 -n etl

# List all pods for the pipeline
kubectl get pods -n etl -l app=gust
```

## Step 6: Schedule Recurring Runs

Add scheduling to trigger daily at 2 AM UTC:

```yaml
tasks:
  - name: extract_data
    handler: k8s
    # ... rest of config ...
```

At the top of the YAML file, add:

```yaml
# Run daily at 2 AM UTC
schedule: "0 2 * * *"
```

Or configure in `config/config.exs`:

```elixir
config :gust, :dag_scheduler, Gust.DAG.Scheduler.Quantum

config :quantum, cron: [
  etl_pipeline: [
    schedule: "0 2 * * *",
    task: {:gust, :execute_dag, ["etl_pipeline"]}
  ]
]
```

## Step 7: Monitor and Optimize

### Set Up Alerting

Configure on-failure notifications:

```elixir
# In your Gust app
defmodule MyApp.DAGCallbacks do
  def on_etl_pipeline_failed(run) do
    # Send alert
    notify_slack("ETL pipeline failed: #{run.id}")
  end
end
```

### Performance Optimization

Profile your pipeline:

1. Check average task duration in Web UI
2. Identify slow tasks
3. Optimize docker images or scale resources
4. Adjust `k8s_api_poll_interval` based on API load

Example optimization:
- If `extract_data` takes 30+ minutes: increase pod CPU/memory
- If `transform_data` is slow: cache intermediate results
- If polling is too frequent: increase `k8s_api_poll_interval` to 5000

### Cost Optimization

```yaml
tasks:
  - name: extract_data
    handler: k8s
    # ... config ...
    node_selector:
      # Use cheaper spot instances for batch work
      karpenter.sh/capacity-type: spot
      karpenter.sh/performance-class: moderate
```

## Next Steps

Now that you have a working ETL pipeline, you can:

1. **Add Error Handling** — Configure retry policies and dead-letter queues
2. **Combine with Elixir Tasks** — Use Elixir tasks for pre/post processing
3. **Implement Data Quality** — Add validation gates between stages
4. **Scale Out** — Run parallel extraction, transformation per data partition
5. **Add Dependencies** — Chain multiple pipelines (input from previous pipeline)

See [Kubernetes Pods Guide](kubernetes_pods.md) for more advanced patterns.

## Troubleshooting

### Pods stuck in Pending

```bash
# Check events
kubectl describe pod POD_NAME -n etl

# Common issues:
# - Image pull backoff: check image URI and registry access
# - Node affinity: adjust node_selector in DAG
# - Resources: increase cluster capacity or reduce requests
```

### Authentication Failures

```bash
# Verify service account
kubectl get sa gust-etl -n etl

# Check RBAC
kubectl auth can-i create pods --as=system:serviceaccount:etl:gust-etl -n etl

# Should return: yes
```

### Network Issues

```bash
# Test pod connectivity
kubectl run debug -it --image=busybox -n etl -- sh
# Inside pod:
wget -O- postgresql://postgres-svc:5432  # Should work
```

## Summary

You've learned how to:
- Create Docker images for ETL tasks
- Write Kubernetes pod tasks in YAML DAGs
- Configure Gust with K8s support
- Set up RBAC and cluster resources
- Load and run the pipeline
- Monitor and debug pod execution
- Schedule recurring runs
- Optimize performance

Your ETL pipeline is now running reliably on Kubernetes with Gust managing orchestration, scheduling, and monitoring.
