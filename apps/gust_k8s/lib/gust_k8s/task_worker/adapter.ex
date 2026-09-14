defmodule GustK8s.TaskWorker.Adapter do
  @moduledoc """
  Task worker adapter for Kubernetes pod execution.

  Non-blocking GenServer implementation for pod lifecycle management:
  1. `:run` message creates pod and schedules first poll (non-blocking)
  2. `{:poll, pod_name}` messages check status at intervals
  3. `:timeout` message handles overall task timeout
  4. Pod completion triggers log collection and cleanup
  5. Results reported back to owner

  Uses `Process.send_after/3` for polling instead of blocking `Process.sleep()`.
  This keeps the GenServer responsive to other messages.

  Pod phases monitored: Pending → Running → Succeeded/Failed
  """

  use Gust.DAG.TaskWorker

  require Logger

  alias Gust.DAG.Logger, as: DagLogger
  alias GustK8s.K8sClient
  alias GustK8s.Template

  # Override the default :run handler from the base macro
  def handle_info(:run, %{task: task, opts: opts} = state) do
    DagLogger.set_task(task.id, task.attempt)

    with {:ok, namespace} <- get_namespace(opts),
         {:ok, pod_name} <- render_pod_name(opts, task),
         {:ok, pod_spec} <- build_pod_spec(opts, pod_name),
         {:ok, ^pod_name} <- create_pod(namespace, pod_spec) do
      # Non-blocking: schedule first poll, don't wait
      poll_interval = Application.get_env(:gust_k8s, :k8s_api_poll_interval, 2_000)
      timeout = Application.get_env(:gust_k8s, :k8s_task_timeout, 30 * 60 * 1_000)

      # Schedule first poll
      poll_timer_ref = Process.send_after(self(), {:poll, pod_name}, poll_interval)

      # Schedule global timeout
      timeout_timer_ref = Process.send_after(self(), :timeout, timeout)

      {:noreply,
       state
       |> Map.put(:pod_name, pod_name)
       |> Map.put(:namespace, namespace)
       |> Map.put(:poll_interval, poll_interval)
       |> Map.put(:poll_timer_ref, poll_timer_ref)
       |> Map.put(:timeout_timer_ref, timeout_timer_ref)
       |> Map.put(:start_time, System.monotonic_time(:millisecond))}
    else
      {:error, error} ->
        send_task_error(state, error)
    end
  end

  def handle_info({:poll, pod_name}, %{pod_name: pod_name, namespace: namespace} = state) do
    k8s_client = Application.get_env(:gust_k8s, :k8s_client, K8sClient)

    case k8s_client.get_pod(namespace, pod_name) do
      {:ok, pod} ->
        phase = get_in(pod, ["status", "phase"])
        handle_pod_phase(phase, pod, pod_name, state)

      {:error, reason} ->
        Logger.error("Error polling pod #{pod_name}: #{inspect(reason)}")
        send_task_error(state, reason)
    end
  end

  def handle_info({:poll, _pod_name}, state) do
    # Stale poll message, ignore
    {:noreply, state}
  end

  def handle_info(:timeout, %{pod_name: pod_name, namespace: namespace} = state) do
    Logger.warning("Pod #{pod_name} execution timed out")

    # Delete the pod
    cleanup_pod(namespace, pod_name)

    # Report timeout error
    send_task_error(state, "Pod execution timeout")
  end

  def handle_info(:timeout, state) do
    # Timeout before pod was created
    send_task_error(state, "Task initialization timeout")
  end

  def handle_cast(:kill, %{pod_name: pod_name, namespace: namespace} = state) when not is_nil(pod_name) do
    cleanup_pod(namespace, pod_name)
    {:stop, :normal, state}
  end

  def handle_cast(:kill, state) do
    {:stop, :normal, state}
  end

  # Private helpers

  defp handle_pod_phase("Succeeded", _pod, pod_name, state) do
    # Pod completed successfully, collect logs and cleanup
    handle_pod_success(pod_name, state)
  end

  defp handle_pod_phase("Failed", pod, pod_name, state) do
    # Pod failed, cleanup and report error
    reason = get_failure_reason(pod)
    Logger.warning("Pod #{pod_name} failed: #{reason}")

    cleanup_pod(state.namespace, pod_name)
    send_task_error(state, "Pod failed: #{reason}")
  end

  defp handle_pod_phase(phase, _pod, pod_name, %{poll_interval: poll_interval} = state) do
    # Pod still running (Pending, Running, Unknown, or any other phase), schedule next poll
    Logger.debug("Pod #{pod_name} in phase: #{phase}")

    poll_timer_ref = Process.send_after(self(), {:poll, pod_name}, poll_interval)
    {:noreply, Map.put(state, :poll_timer_ref, poll_timer_ref)}
  end

  defp handle_pod_success(pod_name, %{namespace: namespace, task: task, owner_pid: owner_pid, timeout_timer_ref: timeout_ref} = state) do
    k8s_client = Application.get_env(:gust_k8s, :k8s_client, K8sClient)

    # Cancel timeout timer
    Process.cancel_timer(timeout_ref)

    case k8s_client.get_pod_logs(namespace, pod_name) do
      {:ok, logs} ->
        # Cleanup pod
        cleanup_pod(namespace, pod_name)

        result = %{
          status: :success,
          stdout: logs,
          stderr: "",
          exit_code: 0
        }

        DagLogger.unset()
        send(owner_pid, {:task_result, result, task.id, :ok})
        {:stop, :normal, state}

      {:error, reason} ->
        Logger.warning("Failed to get pod logs: #{inspect(reason)}")
        cleanup_pod(namespace, pod_name)

        result = %{
          status: :success,
          stdout: "",
          stderr: "Failed to collect logs: #{reason}",
          exit_code: 0
        }

        DagLogger.unset()
        send(owner_pid, {:task_result, result, task.id, :ok})
        {:stop, :normal, state}
    end
  end

  defp create_pod(namespace, pod_spec) do
    k8s_client = Application.get_env(:gust_k8s, :k8s_client, K8sClient)
    k8s_client.create_pod(namespace, pod_spec)
  end

  defp cleanup_pod(namespace, pod_name) do
    k8s_client = Application.get_env(:gust_k8s, :k8s_client, K8sClient)

    case k8s_client.delete_pod(namespace, pod_name) do
      :ok ->
        Logger.info("Deleted pod #{pod_name} in namespace #{namespace}")

      {:error, reason} ->
        Logger.warning("Failed to delete pod #{pod_name}: #{inspect(reason)}")
    end
  end

  defp get_failure_reason(pod) do
    pod_status = Map.get(pod, "status", %{})
    container_statuses = Map.get(pod_status, "containerStatuses", [])

    Enum.find_value(container_statuses, "Unknown reason", fn container ->
      state = Map.get(container, "state", %{})

      case state do
        %{"waiting" => %{"message" => message}} -> message
        %{"terminated" => %{"message" => message}} when message != "" -> message
        %{"terminated" => %{"reason" => reason}} -> reason
        _ -> nil
      end
    end)
  end

  defp build_pod_spec(opts, pod_name) do
    namespace = Map.get(opts, :namespace, "default")
    image = Map.get(opts, :image)
    command = Map.get(opts, :command)
    args = Map.get(opts, :args, [])
    restart_policy = Map.get(opts, :restart_policy, "Never")
    node_selector = Map.get(opts, :node_selector, %{})

    unless is_binary(image) and image != "" do
      raise ArgumentError, "image must be specified"
    end

    container_spec = %{
      "name" => "gust-container",
      "image" => image
    }

    container_spec =
      if command do
        Map.put(container_spec, "command", command)
      else
        container_spec
      end

    container_spec =
      if args && args != [] do
        Map.put(container_spec, "args", args)
      else
        container_spec
      end

    pod_spec = %{
      "apiVersion" => "v1",
      "kind" => "Pod",
      "metadata" => %{
        "name" => pod_name,
        "namespace" => namespace
      },
      "spec" => %{
        "containers" => [container_spec],
        "restartPolicy" => restart_policy
      }
    }

    pod_spec =
      if is_map(node_selector) && map_size(node_selector) > 0 do
        put_in(pod_spec, ["spec", "nodeSelector"], node_selector)
      else
        pod_spec
      end

    {:ok, pod_spec}
  rescue
    e -> {:error, "Failed to build pod spec: #{inspect(e)}"}
  end

  defp get_namespace(opts) do
    case Map.get(opts, :namespace, "default") do
      ns when is_binary(ns) and ns != "" -> {:ok, ns}
      _ -> {:error, "namespace must be a non-empty string"}
    end
  end

  defp render_pod_name(opts, task) do
    case Map.get(opts, :pod_name) do
      nil ->
        # Auto-generate pod name when not provided
        {:ok, "gust-task-#{task.id}-#{task.attempt}"}

      template when is_binary(template) and template != "" ->
        # Render user-provided template
        case Template.render(template, task) do
          pod_name when is_binary(pod_name) -> {:ok, pod_name}
          _ -> {:error, "Failed to render pod_name template"}
        end

      _ ->
        {:error, "pod_name must be nil or a non-empty string"}
    end
  rescue
    e -> {:error, "Failed to render pod_name: #{inspect(e)}"}
  end

  defp send_task_error(%{task: task, owner_pid: owner_pid} = state, error) do
    DagLogger.unset()

    error_struct =
      if is_exception(error) do
        error
      else
        RuntimeError.exception(to_string(error))
      end

    send(owner_pid, {:task_result, error_struct, task.id, :error})
    {:stop, error_struct, state}
  end
end
