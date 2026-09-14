# Writing Shell DAGs

Shell DAGs allow you to define workflows in YAML, making them accessible to teams without Elixir expertise. Shell DAGs support executing shell commands, and—with the K8s adapter—Kubernetes pods.

## Overview

Shell DAGs are YAML files that define tasks as a list. Each task specifies:
- A name (unique within the DAG)
- A command to execute (handler)
- Downstream tasks (execution dependencies)
- Optional execution options

### Supported File Extensions

- `.yml`
- `.yaml`

## Basic Structure

```yaml
# Optional: Schedule this DAG to run automatically
schedule: "0 9 * * *"  # 9 AM daily

# Optional: Callback function name (runs when DAG completes)
on_finished_callback: notify_completion

# Required: List of tasks
tasks:
  - name: task_name
    run: "shell command"
    downstream: [next_task]

  - name: next_task
    run: "another command"
    downstream: []
```

## Task Configuration

### Shell Task (Default)

Execute shell commands:

```yaml
tasks:
  - name:       greet
    run:        "echo 'Hello, World!'"
    downstream: [process]

  - name:       process
    handler:    shell  # Optional explicit indicator that this is a shell task
    run:        "cat data.txt | sort | uniq"
    downstream: []
```

### Kubernetes Pod Task

Execute in a Kubernetes pod (requires `GUST_WITH_K8S=true`):

```yaml
tasks:
  - name:       batch_job
    handler:    k8s
    image:      python:3.11-slim
    command:    ["python"]
    args:       ["/app/process.py"]
    downstream: []
```

See [Kubernetes Pods Guide](kubernetes_pods.md) for complete K8s task reference.

## Task Options

### Common Options (All Tasks)

- **name** *(required)* — Unique task identifier
- **downstream** *(optional)* — List of downstream task names
- **save** or **store_result** *(optional)* — Persist task output for downstream access

### Shell Task Options

#### Command Execution

- **run** *(required)* — Shell command to execute

#### Working Directory

```yaml
- name: list_files
  run:  "ls -la"
  cd:   "/app/data"  # or: cwd, working_dir
```

#### Environment Variables

```yaml
- name: process
  run:  "python process.py"
  env:
    DATABASE_URL: "postgresql://localhost/mydb"
    LOG_LEVEL:    "debug"
    TIMEOUT:      300
```

#### Input/Output Redirection

```yaml
- name:   read_input
  run:    "cat input.txt"
  stdin:  "/dev/null"     # null, close, or file path
  stdout: output.log      # null, close, or file path
  stderr: errors.log      # null, close, or file path
```

#### Process Configuration

```yaml
- name:  background_job
  run:   "long_running_command"
  user:  "appuser"
  group: 1000
  nice:  10               # Priority (-20 to 19)
  kill_timeout:      5000 # Milliseconds before force-kill
  success_exit_code: 0    # What exit codes count as success
```

#### PTY and TTY

```yaml
- name:     interactive
  run:      "read -p 'Enter name: ' name && echo $name"
  pty:      true          # Allocate pseudo-terminal
  pty_echo: true          # Echo input
```

#### Resource Limits (via cgroup)

```yaml
- name:   limited_job
  run:    "intensive_task"
  cgroup: "/myapp/batch"  # cgroup path
```

## Complete Examples

### Simple Sequential Pipeline

```yaml
tasks:
  - name: extract
    run: "wget https://example.com/data.csv -O data.csv"
    downstream: [transform]

  - name: transform
    run: "python transform.py data.csv > transformed.csv"
    downstream: [load]

  - name: load
    run: "psql -h db.example.com -U user -d mydb -c 'COPY events FROM stdin' < transformed.csv"
    downstream: []
```

### Parallel Execution

```yaml
tasks:
  - name: fetch_api_1
    run:  "curl https://api1.example.com/data > api1.json"
    downstream: [merge]

  - name: fetch_api_2
    run:  "curl https://api2.example.com/data > api2.json"
    downstream: [merge]

  - name: fetch_api_3
    run:  "curl https://api3.example.com/data > api3.json"
    downstream: [merge]

  - name: merge
    run:  "jq -s 'add' api*.json > merged.json"
    downstream: [validate]

  - name: validate
    run:  "python validate.py merged.json"
    downstream: []
```

### With Environment Variables and Error Handling

```yaml
tasks:
  - name: connect_database
    run:  "psql -h $DB_HOST -U $DB_USER -d $DB_NAME -c 'SELECT 1'"
    env:
      DB_HOST: "localhost"
      DB_USER: "postgres"
      DB_NAME: "testdb"
    downstream: [run_migration]

  - name: run_migration
    run:  "psql -h $DB_HOST -U $DB_USER -d $DB_NAME -f migrations/001.sql"
    env:
      DB_HOST: "localhost"
      DB_USER: "postgres"
      DB_NAME: "testdb"
    success_exit_code: 0
    downstream: [verify_schema]

  - name: verify_schema
    run:  "psql -h $DB_HOST -U $DB_USER -d $DB_NAME -c '\\d' | wc -l"
    env:
      DB_HOST: "localhost"
      DB_USER: "postgres"
      DB_NAME: "testdb"
    downstream: []
```

### Saving Results for Downstream Tasks

```yaml
tasks:
  - name: list_files
    run:  "find . -name '*.txt' | head -10"
    save: true
    downstream: [process_files]

  - name: process_files
    run:  "wc -l file.txt"
    downstream: []
```

Note: Saved results are available in the Web UI and can be queried via the Gust API.

### Scheduled Pipeline

```yaml
# Run every day at 2 AM
schedule: "0 2 * * *"

tasks:
  - name: backup_database
    run:  "mysqldump -u root mydb > backup-$(date +%Y%m%d).sql"
    cd:   "/backups"
    downstream: [upload_backup]

  - name: upload_backup
    run:  "aws s3 cp backup-$(date +%Y%m%d).sql s3://my-backups/"
    cd:   "/backups"
    downstream: [cleanup]

  - name: cleanup
    run:  "find /backups -name 'backup-*.sql' -mtime +30 -delete"
    downstream: []

on_finished_callback: notify_backup_complete
```

### Mixed Shell and Kubernetes Tasks

Combine shell tasks with Kubernetes pods:

```yaml
tasks:
  - name:       prepare_data
    run:        "python prepare.py"
    downstream: [process_in_k8s]

  - name:       process_in_k8s
    handler:    k8s
    image:      processing-image:latest
    command:    ["python"]
    args:       ["/app/process.py"]
    downstream: [analyze_results]

  - name:       analyze_results
    run:        "python analyze.py"
    downstream: []
```

## Templating

Gust supports basic templating in shell commands using Jinja-like syntax.

### Available Template Variables

Currently limited in shell DAGs, but can access:
- Environment variables: `$VARIABLE_NAME`
- Task-specific context through environment

### Example

```yaml
tasks:
  - name: process
    run:  "echo 'Running task with ID: $TASK_ID'"
    env:
      TASK_ID: "{{task_id}}"
    downstream: []
```

(Note: Template variable support depends on DAG source and configuration)

## Best Practices

### 1. Clear Task Names

Use descriptive names that indicate purpose:

```yaml
# Good
- name: extract_customer_data
- name: validate_data_schema
- name: load_to_warehouse

# Avoid
- name: task1
- name: process
- name: final
```

### 2. Fail Fast

Use `set -e` to stop on first error:

```yaml
- name: pipeline
  run: |
    set -e
    step1
    step2
    step3
  downstream: []
```

### 3. Handle Large Output

Redirect logs for long-running tasks:

```yaml
- name: long_job
  run: "python long_script.py 2>&1"
  stdout: logs/output.log
  stderr: logs/errors.log
  downstream: []
```

### 4. Isolate Concerns

Each task should have a single responsibility:

```yaml
# Good
- name: fetch_data
  run:  "curl ... > data.json"
  downstream: [validate]

- name: validate
  run:  "python validate.py"
  downstream: [process]

# Less ideal: multiple concerns in one task
- name: fetch_and_validate_and_process
  run:  "curl ... > data.json && python validate.py && python process.py"
```

### 5. Use Comments

Document complex pipelines:

```yaml
# Data pipeline: Extract raw data, validate, transform, load

tasks:
  # Step 1: Download from external API
  - name: extract
    run:  "curl ... > raw_data.json"
    downstream: [validate]

  # Step 2: Ensure data meets schema
  - name: validate
    run:  "python validate.py"
    downstream: [transform]

  # Step 3: Clean and normalize
  - name: transform
    run:  "python transform.py"
    downstream: [load]

  # Step 4: Persist to database
  - name: load
    run:  "psql ... < data.sql"
    downstream: []
```

### 6. Avoid Hard-coded Values

Use environment variables:

```yaml
# Good
- name: deploy
  run: "docker push $REGISTRY/$IMAGE:$VERSION"
  env:
    REGISTRY: "myregistry.io"
    IMAGE:    "myapp"
    VERSION:  "1.0.0"
  downstream: []

# Avoid: hard-coded values
- name: deploy
  run:  "docker push myregistry.io/myapp:1.0.0"
```

## Loading Shell DAGs

### From Git

```elixir
config :gust, :dag_source, :git,
  url:    "https://github.com/myorg/dags.git",
  branch: "main"
```

Push your `.yml` files to this repo, and Gust loads them.

### From Folder

```elixir
config :gust, :dag_source, :folder,
  path: "priv/dags"
```

Place `.yml` files in `priv/dags/` directory.

### From S3

```elixir
config :gust, :dag_source, :s3,
  bucket: "my-dags",
  prefix: "shell/"
```

Upload `.yml` files to S3 under `shell/` prefix.

### From Database

```elixir
config :gust, :dag_source, :database
```

Store DAG YAML content in database (schema provided by Gust migrations).

## Error Handling

When a task fails:

1. **Immediate Failure** — Task returns non-zero exit code
2. **Downstream Skip** — All downstream tasks are skipped
3. **Retry** — Can be configured in run parameters
4. **Callback** — `on_finished_callback` invoked with `:error` status

Configure success exit codes:

```yaml
- name: optional_task
  run:  "curl --fail https://example.com || true"  # Always succeeds
  downstream: []

- name: specific_success
  run:  "command"
  success_exit_code: 42  # Only exit code 42 is success
  downstream: []
```

## Debugging

### View Task Output

In Web UI:
1. Navigate to run
2. Click on task
3. View stdout/stderr logs

### Manual Execution

Test locally:

```bash
# Test individual command
bash -c "echo 'Hello'"

# Test with environment
DB_HOST=localhost bash -c "psql -h $DB_HOST"
```

### Enable Verbose Logging

Set Elixir logger level:

```elixir
config :logger, level: :debug
```

## Combining with Elixir DAGs

For complex workflows, combine approaches:

```elixir
# my_dag.ex - Elixir DAG
defmodule MyDAG do
  use Gust.DSL

  task :elixir_task do
    Logger.info("Elixir task executed")
  end

  task :shell_task_wrapper, downstream: [] do
    # Could invoke shell DAG result or orchestrate
  end
end
```

Then load shell DAGs separately via configured source.

## Next Steps

- See [DAG Sources](dag_sources.md) for loading strategies
- See [Writing DAGs](writing_dags.md) for Elixir DAGs
- See [Kubernetes Pods Guide](kubernetes_pods.md) for K8s pod tasks
- Review [Configuration](configuration.md) for system setup
