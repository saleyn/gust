defmodule Gust.DAG.Source.Polling do
  @moduledoc """
  Generic polling monitor for sources that can't use OS file watching.

  Used by Git and Database sources to periodically reload and detect changes.
  """

  @doc """
  Start a polling monitor that periodically calls the given load function
  and broadcasts changes to the loader_pid.

  The load function should return `%{dag_name => {:ok | :error, result}}`.
  """
  def start_link(loader_pid, load_fn, poll_interval_ms) do
    {:ok,
     spawn_link(__MODULE__, :loop, [
       loader_pid,
       load_fn,
       poll_interval_ms,
       %{},
       System.monotonic_time()
     ])}
  end

  @doc false
  def loop(loader_pid, load_fn, poll_interval_ms, last_state, start_time) do
    Process.sleep(poll_interval_ms)

    current_state = load_fn.()

    # Detect changes between last_state and current_state
    detect_and_broadcast_changes(loader_pid, last_state, current_state)

    # Track state by computing a checksum of each DAG's result
    checksummed_state =
      current_state
      |> Enum.map(fn {name, result} ->
        {name, {result_checksum(result), result}}
      end)
      |> Map.new()

    loop(loader_pid, load_fn, poll_interval_ms, checksummed_state, start_time)
  end

  defp detect_and_broadcast_changes(loader_pid, last_state, current_state) do
    # Find new and updated DAGs
    Enum.each(current_state, fn {name, result} ->
      case Map.get(last_state, name) do
        nil ->
          # New DAG
          send(loader_pid, {name, result, "reload"})

        {old_checksum, _} ->
          new_checksum = result_checksum(result)

          old_checksum != new_checksum && send(loader_pid, {name, result, "reload"})
      end
    end)

    # Find removed DAGs
    Enum.each(last_state, fn {name, _} ->
      unless Map.has_key?(current_state, name) do
        send(loader_pid, {name, {:error, "DAG removed"}, "removed"})
      end
    end)
  end

  defp result_checksum({:ok, definition}) do
    definition |> inspect() |> sha256_hash()
  end

  defp result_checksum({:error, reason}) do
    reason |> inspect() |> sha256_hash()
  end

  defp sha256_hash(data) do
    :crypto.hash(:sha256, data) |> Base.encode16()
  end
end
