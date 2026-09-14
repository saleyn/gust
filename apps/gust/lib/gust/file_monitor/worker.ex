defmodule Gust.FileMonitor.Worker do
  @moduledoc false

  use GenServer
  require Logger

  def start_link(args) do
    GenServer.start_link(__MODULE__, args)
  end

  @impl true
  def init(%{loader: loader} = args) do
    source = get_source()

    case source.monitor(loader, args) do
      {:ok, monitor_pid} ->
        Logger.info("Started #{source.name()} monitor for DAG changes")
        {:ok, %{monitor_pid: monitor_pid, source: source, loader: loader}}

      {:error, reason} ->
        {:stop, "Failed to start DAG source monitor: #{inspect(reason)}"}
    end
  end

  @impl true
  def handle_info(msg, state) do
    if state.monitor_pid && is_pid(state.monitor_pid) do
      send(state.monitor_pid, msg)
    end

    {:noreply, state}
  end

  defp get_source do
    Application.get_env(:gust, :dag_source, Gust.DAG.Source.Folder)
  end
end
