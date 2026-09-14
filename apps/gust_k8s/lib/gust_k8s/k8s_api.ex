defmodule GustK8s.K8sAPI do
  @moduledoc """
  Behavior for Kubernetes API operations.
  """

  @callback create_pod(namespace :: binary, pod_spec :: map) ::
              {:ok, pod_name :: binary} | {:error, reason :: any}
  @callback get_pod(namespace :: binary, pod_name :: binary) ::
              {:ok, pod_data :: map} | {:error, reason :: any}
  @callback get_pod_logs(namespace :: binary, pod_name :: binary) ::
              {:ok, logs :: binary} | {:error, reason :: any}
  @callback delete_pod(namespace :: binary, pod_name :: binary) ::
              :ok | {:error, reason :: any}
  @callback list_pods(namespace :: binary, selector :: binary | nil) ::
              {:ok, pods :: list} | {:error, reason :: any}
end
