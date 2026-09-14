defmodule GustK8s.Parser.AdapterTest do
  use ExUnit.Case

  alias GustK8s.Parser.Adapter

  describe "parse_task/1" do
    test "parses valid K8s task with required fields" do
      task = %{
        "name" => "my_task",
        "handler" => "k8s",
        "image" => "busybox:latest",
        "downstream" => []
      }

      {:ok, {name, parsed}} = Adapter.parse_task(task)

      assert name == "my_task"
      assert parsed.downstream == []
      assert parsed.store_result == false
      assert parsed.opts.image == "busybox:latest"
      assert parsed.opts.namespace == "default"
      assert parsed.opts.restart_policy == "Never"
    end

    test "parses K8s task with all optional fields" do
      task = %{
        "name" => "complex_task",
        "handler" => "k8s",
        "image" => "python:3.11-slim",
        "command" => ["python", "-u"],
        "args" => ["/app/script.py", "--verbose"],
        "namespace" => "batch",
        "pod_name" => "task-{{.task.id}}",
        "restart_policy" => "OnFailure",
        "node_selector" => %{"gpu" => "true"},
        "downstream" => ["next_task"],
        "store_result" => true
      }

      {:ok, {name, parsed}} = Adapter.parse_task(task)

      assert name == "complex_task"
      assert parsed.downstream == ["next_task"]
      assert parsed.store_result == true
      assert parsed.opts.image == "python:3.11-slim"
      assert parsed.opts.command == ["python", "-u"]
      assert parsed.opts.args == ["/app/script.py", "--verbose"]
      assert parsed.opts.namespace == "batch"
      assert parsed.opts.pod_name == "task-{{.task.id}}"
      assert parsed.opts.restart_policy == "OnFailure"
      assert parsed.opts.node_selector == %{"gpu" => "true"}
    end

    test "defaults namespace to 'default' when omitted" do
      task = %{
        "name" => "task",
        "handler" => "k8s",
        "image" => "busybox"
      }

      {:ok, {_, parsed}} = Adapter.parse_task(task)

      assert parsed.opts.namespace == "default"
    end

    test "defaults restart_policy to 'Never' when omitted" do
      task = %{
        "name" => "task",
        "handler" => "k8s",
        "image" => "busybox"
      }

      {:ok, {_, parsed}} = Adapter.parse_task(task)

      assert parsed.opts.restart_policy == "Never"
    end

    test "defaults args to empty list when omitted" do
      task = %{
        "name" => "task",
        "handler" => "k8s",
        "image" => "busybox"
      }

      {:ok, {_, parsed}} = Adapter.parse_task(task)

      assert parsed.opts.args == []
    end

    test "defaults node_selector to empty map when omitted" do
      task = %{
        "name" => "task",
        "handler" => "k8s",
        "image" => "busybox"
      }

      {:ok, {_, parsed}} = Adapter.parse_task(task)

      assert parsed.opts.node_selector == %{}
    end

    test "supports 'save' as alias for 'store_result'" do
      task = %{
        "name" => "task",
        "handler" => "k8s",
        "image" => "busybox",
        "save" => true
      }

      {:ok, {_, parsed}} = Adapter.parse_task(task)

      assert parsed.store_result == true
    end

    test "rejects task without 'name'" do
      task = %{
        "handler" => "k8s",
        "image" => "busybox"
      }

      {:error, reason} = Adapter.parse_task(task)

      assert reason =~ "missing required fields"
      assert reason =~ "name"
    end

    test "rejects task without 'handler'" do
      task = %{
        "name" => "task",
        "image" => "busybox"
      }

      {:error, reason} = Adapter.parse_task(task)

      assert reason =~ "missing required fields"
      assert reason =~ "handler"
    end

    test "rejects task without 'image'" do
      task = %{
        "name" => "task",
        "handler" => "k8s"
      }

      {:error, reason} = Adapter.parse_task(task)

      assert reason =~ "missing required fields"
      assert reason =~ "image"
    end

    test "rejects task with wrong handler type" do
      task = %{
        "name" => "task",
        "handler" => "shell",
        "image" => "busybox"
      }

      {:error, reason} = Adapter.parse_task(task)

      assert reason =~ "handler must be 'k8s'"
    end

    test "rejects task with empty name" do
      task = %{
        "name" => "",
        "handler" => "k8s",
        "image" => "busybox"
      }

      {:error, reason} = Adapter.parse_task(task)

      assert reason =~ "name must be a non-empty string"
    end

    test "rejects task with empty image" do
      task = %{
        "name" => "task",
        "handler" => "k8s",
        "image" => ""
      }

      {:error, reason} = Adapter.parse_task(task)

      assert reason =~ "image must be a non-empty string"
    end

    test "rejects task with non-list command" do
      task = %{
        "name" => "task",
        "handler" => "k8s",
        "image" => "busybox",
        "command" => "not a list"
      }

      {:error, reason} = Adapter.parse_task(task)

      assert reason =~ "command must be a list"
    end

    test "rejects task with non-list args" do
      task = %{
        "name" => "task",
        "handler" => "k8s",
        "image" => "busybox",
        "args" => "not a list"
      }

      {:error, reason} = Adapter.parse_task(task)

      assert reason =~ "args must be a list"
    end

    test "rejects task with non-map node_selector" do
      task = %{
        "name" => "task",
        "handler" => "k8s",
        "image" => "busybox",
        "node_selector" => ["not", "a", "map"]
      }

      {:error, reason} = Adapter.parse_task(task)

      assert reason =~ "node_selector must be a map"
    end

    test "rejects task with unknown keys" do
      task = %{
        "name" => "task",
        "handler" => "k8s",
        "image" => "busybox",
        "unknown_key" => "value"
      }

      {:error, reason} = Adapter.parse_task(task)

      assert reason =~ "unknown keys"
      assert reason =~ "unknown_key"
    end
  end

  describe "parse_task!/1" do
    test "returns parsed task on success" do
      task = %{
        "name" => "task",
        "handler" => "k8s",
        "image" => "busybox"
      }

      {name, parsed} = Adapter.parse_task!(task)

      assert name == "task"
      assert parsed.opts.image == "busybox"
    end

    test "raises ArgumentError on validation failure" do
      task = %{
        "name" => "task",
        "handler" => "k8s"
      }

      assert_raise ArgumentError, fn ->
        Adapter.parse_task!(task)
      end
    end

    test "includes task name in error message" do
      task = %{
        "name" => "my_failing_task",
        "handler" => "k8s"
      }

      assert_raise ArgumentError, ~r/my_failing_task/, fn ->
        Adapter.parse_task!(task)
      end
    end
  end
end
