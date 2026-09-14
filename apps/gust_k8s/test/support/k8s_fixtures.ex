defmodule GustK8s.Test.Fixtures do
  @moduledoc """
  Test fixtures and helpers for K8s adapter tests.
  """

  @doc """
  Create a mock task for testing.
  """
  def mock_task(overrides \\ %{}) do
    defaults = %{
      id: 1,
      run_id: 100,
      attempt: 1,
      name: "test_task",
      params: %{}
    }

    Map.merge(defaults, overrides)
  end

  @doc """
  Create a mock K8s pod spec for testing.
  """
  def mock_pod_spec(pod_name \\ "test-pod", namespace \\ "default") do
    %{
      "apiVersion" => "v1",
      "kind" => "Pod",
      "metadata" => %{
        "name" => pod_name,
        "namespace" => namespace
      },
      "spec" => %{
        "containers" => [
          %{
            "name" => "gust-container",
            "image" => "busybox:latest",
            "command" => ["/bin/sh", "-c"],
            "args" => ["echo 'test'"]
          }
        ],
        "restartPolicy" => "Never"
      }
    }
  end

  @doc """
  Create a mock pod status response.
  """
  def mock_pod_status(pod_name \\ "test-pod", phase \\ "Running") do
    %{
      "apiVersion" => "v1",
      "kind" => "Pod",
      "metadata" => %{
        "name" => pod_name,
        "namespace" => "default"
      },
      "status" => %{
        "phase" => phase,
        "containerStatuses" => [
          %{
            "name" => "gust-container",
            "ready" => phase == "Running",
            "state" => %{
              "running" => %{}
            }
          }
        ]
      }
    }
  end

  @doc """
  Create a failed pod status.
  """
  def mock_failed_pod_status(pod_name \\ "test-pod", reason \\ "CrashLoopBackOff") do
    %{
      "apiVersion" => "v1",
      "kind" => "Pod",
      "metadata" => %{
        "name" => pod_name,
        "namespace" => "default"
      },
      "status" => %{
        "phase" => "Failed",
        "containerStatuses" => [
          %{
            "name" => "gust-container",
            "ready" => false,
            "state" => %{
              "terminated" => %{
                "exitCode" => 1,
                "reason" => reason,
                "message" => "Container failed with reason: #{reason}"
              }
            }
          }
        ]
      }
    }
  end

  @doc """
  Create a succeeded pod status.
  """
  def mock_succeeded_pod_status(pod_name \\ "test-pod") do
    %{
      "apiVersion" => "v1",
      "kind" => "Pod",
      "metadata" => %{
        "name" => pod_name,
        "namespace" => "default"
      },
      "status" => %{
        "phase" => "Succeeded",
        "containerStatuses" => [
          %{
            "name" => "gust-container",
            "ready" => false,
            "state" => %{
              "terminated" => %{
                "exitCode" => 0,
                "reason" => "Completed"
              }
            }
          }
        ]
      }
    }
  end
end
