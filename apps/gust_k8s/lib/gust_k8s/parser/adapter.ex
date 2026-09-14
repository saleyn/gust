defmodule GustK8s.Parser.Adapter do
  @moduledoc """
  Parser adapter for Kubernetes pod tasks within shell YAML DAGs.

  Parses tasks with `handler: k8s` and validates K8s-specific configuration.
  This adapter is invoked by the shell parser when a task specifies the K8s handler.
  """

  @task_keys ~w(
    name
    handler
    image
    command
    args
    namespace
    pod_name
    restart_policy
    node_selector
    downstream
    save
    store_result
  )

  @doc """
  Parse a Kubernetes task from YAML task configuration.

  Expected task format:
  ```yaml
  - name:           my_k8s_task
    handler:        k8s
    image:          busybox:latest
    command:        ["/bin/sh", "-c"]
    args:           ["echo 'hello'"]
    namespace:      default               # optional
    pod_name:       my-pod-{{.task.id}}   # optional
    restart_policy: Never                 # optional
    node_selector:                        # optional
      workload:     batch
    downstream:     [next_task]           # optional
    store_result:   false                 # optional
  ```
  """
  def parse_task(task) when is_map(task) do
    with :ok <- validate_keys(task),
         :ok <- validate_required_fields(task),
         :ok <- validate_field_types(task),
         k8s_opts <- extract_k8s_opts(task) do
      name = Map.get(task, "name")
      downstream = Map.get(task, "downstream", [])
      store_result = Map.get(task, "store_result", Map.get(task, "save", false))

      {:ok,
       {name,
        %{
          downstream: downstream,
          store_result: store_result,
          opts: k8s_opts
        }}}
    end
  end

  def parse_task!(task) do
    case parse_task(task) do
      {:ok, result} ->
        result

      {:error, reason} ->
        name = Map.get(task, "name")
        raise ArgumentError, message: "task #{name}: #{reason}"
    end
  end

  # Private implementation

  defp validate_keys(task) do
    case Map.keys(task) -- @task_keys do
      [] -> :ok
      keys -> {:error, "unknown keys #{inspect(keys)}"}
    end
  end

  defp validate_required_fields(task) do
    required = ["name", "handler", "image"]
    missing = Enum.reject(required, &Map.has_key?(task, &1))

    case missing do
      [] -> :ok
      fields -> {:error, "missing required fields: #{inspect(fields)}"}
    end
  end

  defp validate_field_types(task) do
    with \
      :ok <- validate_string(task, "name", "name must be a non-empty string"),
      :ok <- validate_string(task, "handler", "handler must be 'k8s'"),
      :ok <- validate_handler(task),
      :ok <- validate_string(task, "image", "image must be a non-empty string"),
      :ok <- validate_optional_list(task, "command", "command must be a list of strings"),
      :ok <- validate_optional_list(task, "args", "args must be a list of strings"),
      :ok <- validate_optional_string(task, "namespace", "namespace must be a string"),
      :ok <- validate_optional_string(task, "pod_name", "pod_name must be a string"),
      :ok <- validate_optional_string(task, "restart_policy", "restart_policy must be a string"),
      :ok <- validate_optional_map(task, "node_selector", "node_selector must be a map"),
      :ok <- validate_optional_list(task, "downstream", "downstream must be a list of task names"),
      :ok <- validate_optional_bool(task, "save", "save must be boolean"),
      do:    validate_optional_bool(task, "store_result", "store_result must be boolean")
  end

  defp validate_string(task, key, error_msg) do
    case Map.get(task, key) do
      value when is_binary(value) and value != "" -> :ok
      _ -> {:error, error_msg}
    end
  end

  defp validate_handler(task) do
    case Map.get(task, "handler") do
      "k8s" -> :ok
      other -> {:error, "handler must be 'k8s', got #{inspect(other)}"}
    end
  end

  defp validate_optional_string(task, key, error_msg) do
    case Map.get(task, key) do
      nil -> :ok
      value when is_binary(value) -> :ok
      _ -> {:error, error_msg}
    end
  end

  defp validate_optional_bool(task, key, error_msg) do
    case Map.get(task, key) do
      nil -> :ok
      value when is_boolean(value) -> :ok
      _ -> {:error, error_msg}
    end
  end

  defp validate_optional_map(task, key, error_msg) do
    case Map.get(task, key) do
      nil -> :ok
      value when is_map(value) -> :ok
      _ -> {:error, error_msg}
    end
  end

  defp validate_optional_list(task, key, error_msg) do
    case Map.get(task, key) do
      nil ->
        :ok

      value when is_list(value) ->
        if Enum.all?(value, &is_binary/1) do
          :ok
        else
          {:error, error_msg}
        end

      _ ->
        {:error, error_msg}
    end
  end

  defp extract_k8s_opts(task) do
    %{
      image: Map.get(task, "image"),
      command: Map.get(task, "command"),
      args: Map.get(task, "args", []),
      namespace: Map.get(task, "namespace", "default"),
      pod_name: Map.get(task, "pod_name"),
      restart_policy: Map.get(task, "restart_policy", "Never"),
      node_selector: Map.get(task, "node_selector", %{})
    }
  end
end
