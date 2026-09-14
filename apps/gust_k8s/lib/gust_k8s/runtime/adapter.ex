defmodule GustK8s.Runtime.Adapter do
  @moduledoc """
  Runtime adapter for Kubernetes pod execution.

  Minimal lifecycle management following the shell adapter pattern.
  """

  @doc """
  Setup DAG runtime.

  No special setup needed for K8s pod execution.
  """
  def setup(dag_def, _runtime_id), do: dag_def

  @doc """
  Teardown DAG runtime.

  No cleanup needed at runtime level; pods are cleaned up at task completion.
  """
  def teardown(_dag_def, _runtime_id), do: :ok

  @doc """
  Handle on_finished callback.

  No special handling needed for K8s adapter.
  """
  def on_finished_callback(_definition, _fn_name, _run, _status), do: :ok

  @doc """
  Kill a K8s pod task.

  Sends a kill message to the task worker GenServer.
  """
  def kill(task_pid) when is_pid(task_pid) do
    :ok == GenServer.cast(task_pid, :kill)
  end
end
