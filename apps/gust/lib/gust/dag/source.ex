defmodule Gust.DAG.Source do
  @moduledoc """
  Behavior for pluggable DAG source implementations.

  A DAG source defines where DAGs come from (filesystem, git repo, database, etc.)
  and how to monitor them for changes.
  """

  @doc """
  Load all DAG definitions from the source.

  Takes an optional effective_time parameter (defaults to DateTime.utc_now())
  for DAG versioning and time-based filtering.

  Returns a map with separate lists of successful and failed DAG loads.
  On source-level errors (can't connect to DB, can't clone repo, etc), returns {:error, reason}.
  """
  @callback load() ::
              %{success: [{String.t(), any()}], error: [{String.t(), any()}]} | {:error, any()}
  @callback load(effective_time :: nil | DateTime.t()) ::
              %{success: [{String.t(), any()}], error: [{String.t(), any()}]} | {:error, any()}

  @doc """
  Start monitoring the source for changes.

  When the source changes (files modified, repo updated, DB records changed),
  the monitor should send messages to `loader_pid` in the format:
  `{dag_name, parse_result, action}`
  where `action` is "reload" or "removed".

  Returns `{:ok, monitor_pid}` or `{:error, reason}`.
  """
  @callback monitor(loader_pid :: pid(), config :: map()) :: {:ok, pid()} | {:error, term()}

  @doc """
  Get the name of the source implementation for logging/debugging.
  """
  @callback name() :: String.t()

  @optional_callbacks load: 1
end
