defmodule GustK8s.Template do
  @moduledoc """
  Template rendering for K8s pod names and configurations.

  Supports Elixir expression templates like `{{.task.id}}` for dynamic pod naming.
  """

  @doc """
  Render a template string with task context.

  Supports:
  - `{{.task.id}}` - Task ID
  - `{{.task.attempt}}` - Task attempt number
  - `{{.task.name}}` - Task name

  Returns the rendered string or raises on error.
  """
  def render(template, %{id: task_id, attempt: attempt, name: name}) when is_binary(template) do
    template
    |> String.replace("{{.task.id}}", to_string(task_id))
    |> String.replace("{{.task.attempt}}", to_string(attempt))
    |> String.replace("{{.task.name}}", name)
  end

  @doc """
  Validate template string syntax.

  Raises ArgumentError if template contains invalid syntax.
  """
  def validate!(template) when is_binary(template) do
    # Check for unbalanced braces
    open_count = String.graphemes(template) |> Enum.count(&(&1 == "{"))
    close_count = String.graphemes(template) |> Enum.count(&(&1 == "}"))

    unless open_count == close_count do
      raise ArgumentError, "template has unbalanced braces"
    end

    :ok
  end
end
