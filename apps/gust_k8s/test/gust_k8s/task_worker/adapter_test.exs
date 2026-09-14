defmodule GustK8s.TaskWorker.AdapterTest do
  use ExUnit.Case
  import Mox

  import GustK8s.Test.Fixtures

  alias GustK8s.TaskWorker.Adapter

  Mox.defmock(GustK8s.K8sClientMock, for: GustK8s.K8sAPI)
  Mox.defmock(DagLoggerMock, for: Gust.DAG.Logger)

  setup :verify_on_exit!
  setup :set_mox_from_context

  setup do
    # Configure mocks
    Application.put_env(:gust_k8s, :k8s_client, GustK8s.K8sClientMock)
    Application.put_env(:gust_k8s, :k8s_api_poll_interval, 10)  # Fast polling for tests
    Application.put_env(:gust_k8s, :k8s_task_timeout, 5000)

    GustK8s.K8sClientMock
    |> Mox.stub(:delete_pod, fn _ns, _name -> :ok end)

    # Stub DagLogger for all tests
    DagLoggerMock
    |> Mox.stub(:set_task, fn _id, _attempt -> :ok end)
    |> Mox.stub(:unset, fn -> :ok end)

    # Make the adapter use the mock
    old_logger = Application.get_env(:gust, :dag_logger)
    Application.put_env(:gust, :dag_logger, DagLoggerMock)

    on_exit(fn ->
      Application.delete_env(:gust_k8s, :k8s_client)
      Application.delete_env(:gust_k8s, :k8s_api_poll_interval)
      Application.delete_env(:gust_k8s, :k8s_task_timeout)
      if old_logger, do: Application.put_env(:gust, :dag_logger, old_logger)
    end)

    :ok
  end

  describe "handle_info(:run) - non-blocking pod creation" do
    test "creates pod and schedules polling (returns noreply)" do
      task = mock_task(%{id: 123, attempt: 1, name: "k8s_task"})
      pod_name = "gust-task-123-1"

      opts = %{
        image: "busybox:latest",
        command: ["/bin/sh", "-c"],
        args: ["echo 'hello world'"],
        namespace: "default",
        pod_name: nil,
        restart_policy: "Never",
        node_selector: %{}
      }

      state = %{
        task: task,
        opts: opts,
        owner_pid: self(),
        dag_def: %{name: "test_dag", adapter: :k8s}
      }

      GustK8s.K8sClientMock
      |> Mox.expect(:create_pod, fn namespace, pod_spec ->
        assert namespace == "default"
        assert pod_spec["metadata"]["name"] == pod_name
        assert pod_spec["spec"]["containers"] |> List.first() |> Map.get("image") == "busybox:latest"
        {:ok, pod_name}
      end)

      # Non-blocking: should return noreply with scheduled timers
      {:noreply, new_state} = Adapter.handle_info(:run, state)

      assert new_state.pod_name == pod_name
      assert new_state.namespace == "default"
      assert is_reference(new_state.poll_timer_ref)
      assert is_reference(new_state.timeout_timer_ref)
    end

    test "handles pod creation failure" do
      task = mock_task()

      opts = %{
        image: "nonexistent:image",
        namespace: "default",
        pod_name: nil,
        restart_policy: "Never",
        node_selector: %{}
      }

      state = %{
        task: task,
        opts: opts,
        owner_pid: self(),
        dag_def: %{name: "test_dag", adapter: :k8s}
      }

      GustK8s.K8sClientMock
      |> Mox.expect(:create_pod, fn _namespace, _pod_spec ->
        {:error, "Image not found"}
      end)

      {:stop, error, _} = Adapter.handle_info(:run, state)

      assert is_exception(error)
      assert error.message =~ "Image not found"
      assert_receive {:task_result, ^error, 1, :error}
    end
  end

  describe "handle_info({:poll, pod_name}) - asynchronous polling" do
    test "transitions from Pending to Running and continues polling" do
      task = mock_task()
      pod_name = "test-pod"

      state = %{
        task: task,
        pod_name: pod_name,
        namespace: "default",
        owner_pid: self(),
        poll_interval: 10,
        poll_timer_ref: nil,
        timeout_timer_ref: Process.send_after(self(), {:timeout}, 5000),
        start_time: System.monotonic_time(:millisecond)
      }

      pending_pod = %{
        "status" => %{"phase" => "Pending"}
      }

      GustK8s.K8sClientMock
      |> Mox.expect(:get_pod, fn namespace, name ->
        assert namespace == "default"
        assert name == pod_name
        {:ok, pending_pod}
      end)

      # Should schedule another poll, not stop
      {:noreply, new_state} = Adapter.handle_info({:poll, pod_name}, state)

      assert is_reference(new_state.poll_timer_ref)
      # No task result sent yet
      refute_received {:task_result, _, _, _}
    end

    test "handles pod success and sends task result" do
      task = mock_task(%{id: 456})
      pod_name = "test-pod"

      state = %{
        task: task,
        pod_name: pod_name,
        namespace: "default",
        owner_pid: self(),
        poll_interval: 10,
        poll_timer_ref: nil,
        timeout_timer_ref: Process.send_after(self(), {:timeout}, 5000),
        start_time: System.monotonic_time(:millisecond)
      }

      succeeded_pod = %{
        "status" => %{"phase" => "Succeeded"}
      }

      GustK8s.K8sClientMock
      |> Mox.expect(:get_pod, fn _namespace, _name ->
        {:ok, succeeded_pod}
      end)
      |> Mox.expect(:get_pod_logs, fn _namespace, _name ->
        {:ok, "Success output\n"}
      end)
      |> Mox.expect(:delete_pod, fn _namespace, _name ->
        :ok
      end)

      {:stop, :normal, _} = Adapter.handle_info({:poll, pod_name}, state)

      assert_receive {:task_result, result, 456, :ok}
      assert result.status == :success
      assert result.stdout == "Success output\n"
      assert result.exit_code == 0
    end

    test "handles pod failure and sends error" do
      task = mock_task(%{id: 789})
      pod_name = "test-pod"

      state = %{
        task: task,
        pod_name: pod_name,
        namespace: "default",
        owner_pid: self(),
        poll_interval: 10,
        poll_timer_ref: nil,
        timeout_timer_ref: Process.send_after(self(), {:timeout}, 5000),
        start_time: System.monotonic_time(:millisecond)
      }

      failed_pod = %{
        "status" => %{
          "phase" => "Failed",
          "containerStatuses" => [
            %{
              "state" => %{
                "terminated" => %{
                  "reason" => "Error",
                  "message" => "Process exited"
                }
              }
            }
          ]
        }
      }

      GustK8s.K8sClientMock
      |> Mox.expect(:get_pod, fn _namespace, _name ->
        {:ok, failed_pod}
      end)
      |> Mox.expect(:delete_pod, fn _namespace, _name ->
        :ok
      end)

      {:stop, error, _} = Adapter.handle_info({:poll, pod_name}, state)

      assert is_exception(error)
      assert_receive {:task_result, ^error, 789, :error}
    end

    test "handles API errors during polling" do
      task = mock_task()
      pod_name = "test-pod"

      state = %{
        task: task,
        pod_name: pod_name,
        namespace: "default",
        owner_pid: self(),
        poll_interval: 10,
        poll_timer_ref: nil,
        timeout_timer_ref: Process.send_after(self(), {:timeout}, 5000),
        start_time: System.monotonic_time(:millisecond)
      }

      GustK8s.K8sClientMock
      |> Mox.expect(:get_pod, fn _namespace, _name ->
        {:error, "API connection failed"}
      end)

      {:stop, error, _} = Adapter.handle_info({:poll, pod_name}, state)

      assert is_exception(error)
      assert_receive {:task_result, ^error, _, :error}
    end

    test "ignores stale poll messages for different pod" do
      task = mock_task()
      state = %{
        task: task,
        pod_name: "pod-1",
        namespace: "default",
        owner_pid: self(),
        poll_interval: 10,
        poll_timer_ref: nil,
        timeout_timer_ref: Process.send_after(self(), {:timeout}, 5000)
      }

      # Poll message for different pod (should be ignored)
      {:noreply, _} = Adapter.handle_info({:poll, "pod-2"}, state)

      refute_received {:task_result, _, _, _}
    end
  end

  describe "handle_info(:timeout) - task timeout" do
    test "cleans up pod and sends timeout error" do
      task = mock_task(%{id: 999})
      pod_name = "test-pod"

      state = %{
        task: task,
        pod_name: pod_name,
        namespace: "default",
        owner_pid: self(),
        timeout_timer_ref: nil,
        poll_timer_ref: Process.send_after(self(), {:poll, pod_name}, 100)
      }

      GustK8s.K8sClientMock
      |> Mox.expect(:delete_pod, fn namespace, name ->
        assert namespace == "default"
        assert name == pod_name
        :ok
      end)

      {:stop, error, _} = Adapter.handle_info(:timeout, state)

      assert is_exception(error)
      assert error.message =~ "timeout"
      assert_receive {:task_result, ^error, 999, :error}
    end

    test "handles timeout before pod was created" do
      task = mock_task(%{id: 888})

      state = %{
        task: task,
        owner_pid: self(),
        timeout_timer_ref: nil
      }

      {:stop, error, _} = Adapter.handle_info(:timeout, state)

      assert is_exception(error)
      assert error.message =~ "initialization timeout"
      assert_receive {:task_result, ^error, 888, :error}
    end
  end

  describe "handle_cast(:kill)" do
    test "deletes pod when pod_name is set" do
      task = mock_task()

      state = %{
        task: task,
        opts: %{image: "busybox:latest"},
        pod_name: "my-pod",
        namespace: "default",
        owner_pid: self()
      }

      GustK8s.K8sClientMock
      |> Mox.expect(:delete_pod, fn namespace, pod_name ->
        assert namespace == "default"
        assert pod_name == "my-pod"
        :ok
      end)

      {:stop, :normal, _} = Adapter.handle_cast(:kill, state)
    end

    test "stops without error when pod_name is nil" do
      task = mock_task()

      state = %{
        task: task,
        opts: %{image: "busybox:latest"},
        pod_name: nil,
        namespace: "default",
        owner_pid: self()
      }

      {:stop, :normal, _} = Adapter.handle_cast(:kill, state)
    end

    test "handles delete errors gracefully" do
      task = mock_task()

      state = %{
        task: task,
        opts: %{image: "busybox:latest"},
        pod_name: "my-pod",
        namespace: "default",
        owner_pid: self()
      }

      GustK8s.K8sClientMock
      |> Mox.expect(:delete_pod, fn _ns, _name ->
        {:error, "Pod not found"}
      end)

      {:stop, :normal, _} = Adapter.handle_cast(:kill, state)
    end
  end

  describe "pod_name rendering" do
    test "auto-generates pod name when template is nil" do
      task = mock_task(%{id: 789, attempt: 3})

      opts = %{
        image: "busybox:latest",
        namespace: "default",
        pod_name: nil,
        restart_policy: "Never",
        node_selector: %{}
      }

      state = %{
        task: task,
        opts: opts,
        owner_pid: self(),
        dag_def: %{name: "test_dag", adapter: :k8s}
      }

      GustK8s.K8sClientMock
      |> Mox.expect(:create_pod, fn _ns, pod_spec ->
        # Should have auto-generated name matching pattern
        name = pod_spec["metadata"]["name"]
        assert name == "gust-task-789-3"
        {:ok, name}
      end)

      {:noreply, new_state} = Adapter.handle_info(:run, state)

      assert new_state.pod_name == "gust-task-789-3"
    end

    test "uses custom pod name template if provided" do
      task = mock_task(%{id: 456, attempt: 2})

      opts = %{
        image: "busybox:latest",
        namespace: "default",
        pod_name: "my-task-{{.task.id}}-{{.task.attempt}}",
        restart_policy: "Never",
        node_selector: %{}
      }

      state = %{
        task: task,
        opts: opts,
        owner_pid: self(),
        dag_def: %{name: "test_dag", adapter: :k8s}
      }

      expected_pod_name = "my-task-456-2"

      GustK8s.K8sClientMock
      |> Mox.expect(:create_pod, fn _ns, pod_spec ->
        actual_name = pod_spec["metadata"]["name"]
        assert actual_name == expected_pod_name
        {:ok, actual_name}
      end)

      {:noreply, new_state} = Adapter.handle_info(:run, state)

      assert new_state.pod_name == expected_pod_name
    end
  end
end
