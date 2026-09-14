defmodule Gust.DAG.Parser.Adapter do
  @moduledoc """
  Behavior for DAG file parsers.

  Each parser adapter handles parsing DAG definitions from files with specific extensions
  and may support multiple task handler types within those files.
  """

  @callback parse_file(String.t()) :: {:ok, Gust.DAG.Definition.t()} | {:error, term()}

  @doc """
  List of file extensions for DAGs supported by this parser (e.g., [".yml", ".yaml"]).
  """
  @callback extensions() :: [String.t()]

  @doc """
  Optional list of task handler types supported by this parser (e.g., ["shell", "k8s"]).

  Task handlers are specified in DAG tasks via the 'handler' field.
  A parser may support multiple handler types within the same file format.
  """
  @callback handlers() :: [String.t()]

  @optional_callbacks handlers: 0
end
