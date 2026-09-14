defmodule GustK8s.K8sClient do
  @moduledoc """
  Kubernetes API client using Req for HTTP requests.

  Handles pod creation, status monitoring, log collection, and deletion
  via the Kubernetes REST API.
  """

  @behaviour GustK8s.K8sAPI

  alias GustK8s.ConfigLoader

  @doc """
  Create a Kubernetes pod.

  Args:
    - `namespace` - Kubernetes namespace
    - `pod_spec` - Pod specification map with metadata and spec keys

  Returns:
    - `{:ok, pod_name}` - Successfully created pod
    - `{:error, reason}` - Creation failed
  """
  def create_pod(namespace, pod_spec) when is_binary(namespace) and is_map(pod_spec) do
    with {:ok, url} <- build_pods_url(namespace),
         {:ok, req} <- build_request(url),
         {:ok, response} <- Req.post(req, json: pod_spec) do
      check_create_response(response, pod_spec)
    else
      {:error, reason} -> {:error, reason}
    end
  rescue
    e ->
      {:error, "Exception creating pod: #{inspect(e)}"}
  end

  @doc """
  Get pod status.

  Args:
    - `namespace` - Kubernetes namespace
    - `pod_name` - Name of the pod

  Returns:
    - `{:ok, pod_data}` - Pod data with status
    - `{:error, reason}` - Failed to get pod
  """
  def get_pod(namespace, pod_name) when is_binary(namespace) and is_binary(pod_name) do
    with {:ok, url} <- build_pod_url(namespace, pod_name),
         {:ok, req} <- build_request(url),
         {:ok, response} <- Req.get(req) do
      check_get_response(response)
    else
      {:error, reason} -> {:error, reason}
    end
  rescue
    e ->
      {:error, "Exception getting pod: #{inspect(e)}"}
  end

  @doc """
  Get pod logs.

  Args:
    - `namespace` - Kubernetes namespace
    - `pod_name` - Name of the pod
    - `container` - Container name (optional, defaults to first container)

  Returns:
    - `{:ok, logs}` - Pod logs as string
    - `{:error, reason}` - Failed to get logs
  """
  def get_pod_logs(namespace, pod_name, container \\ nil) do
    container_param = if container, do: "?container=#{container}", else: ""

    case ConfigLoader.get_api_server() do
      {:ok, server} ->
        url = "#{server}/api/v1/namespaces/#{namespace}/pods/#{pod_name}/log#{container_param}"
        with {:ok, req} <- build_request(url),
             {:ok, response} <- Req.get(req) do
          check_logs_response(response)
        else
          {:error, reason} -> {:error, reason}
        end

      {:error, _} ->
        {:error, "Could not determine API server"}
    end
  rescue
    e ->
      {:error, "Exception getting pod logs: #{inspect(e)}"}
  end

  @doc """
  Delete a pod.

  Args:
    - `namespace` - Kubernetes namespace
    - `pod_name` - Name of the pod

  Returns:
    - `:ok` - Pod deleted
    - `{:error, reason}` - Failed to delete pod
  """
  def delete_pod(namespace, pod_name) when is_binary(namespace) and is_binary(pod_name) do
    with {:ok, url} <- build_pod_url(namespace, pod_name),
         {:ok, req} <- build_request(url),
         {:ok, response} <- Req.delete(req) do
      check_delete_response(response, pod_name)
    else
      {:error, reason} -> {:error, reason}
    end
  rescue
    e ->
      {:error, "Exception deleting pod: #{inspect(e)}"}
  end

  @doc """
  List pods in a namespace with optional label selector.

  Args:
    - `namespace` - Kubernetes namespace
    - `selector` - Optional label selector string (e.g., "app=myapp")

  Returns:
    - `{:ok, [pods]}` - List of pod data
    - `{:error, reason}` - Failed to list pods
  """
  def list_pods(namespace, selector \\ nil) do
    query_param = if selector, do: "?labelSelector=#{URI.encode_www_form(selector)}", else: ""

    case ConfigLoader.get_api_server() do
      {:ok, server} ->
        url = "#{server}/api/v1/namespaces/#{namespace}/pods#{query_param}"
        with {:ok, req} <- build_request(url),
             {:ok, response} <- Req.get(req) do
          check_list_response(response)
        else
          {:error, reason} -> {:error, reason}
        end

      {:error, _} ->
        {:error, "Could not determine API server"}
    end
  rescue
    e ->
      {:error, "Exception listing pods: #{inspect(e)}"}
  end

  # Private helpers

  defp check_create_response(%{status: 201} = resp, _pod_spec), do: {:ok, resp.body["metadata"]["name"]}
  defp check_create_response(%{status: 409}, pod_spec), do: {:ok, pod_spec["metadata"]["name"]}
  defp check_create_response(%{status: code} = resp, _pod_spec) when code >= 400 do
    {:error, "Failed to create pod: #{resp.body["message"] || "Unknown error"}"}
  end

  defp check_create_response(%{status: code}, _pod_spec), do: {:error, "Unexpected response status: #{code}"}

  defp check_get_response(%{status: 200} = resp), do: {:ok, resp.body}
  defp check_get_response(%{status: 404}), do: {:error, "Pod not found"}
  defp check_get_response(%{status: code} = resp) when code >= 400 do
    {:error, resp.body["message"] || "Failed to get pod"}
  end

  defp check_get_response(%{status: code}), do: {:error, "Unexpected response status: #{code}"}

  defp check_logs_response(%{status: 200} = resp), do: {:ok, resp.body}
  defp check_logs_response(%{status: 404}), do: {:ok, ""}
  defp check_logs_response(%{status: code} = resp) when code >= 400 do
    {:error, resp.body["message"] || "Failed to get logs"}
  end

  defp check_logs_response(%{status: code}), do: {:error, "Unexpected response status: #{code}"}

  defp check_delete_response(%{status: code}, _pod_name) when code in [200, 204, 404], do: :ok
  defp check_delete_response(%{status: code} = resp, pod_name) when code >= 400 do
    {:error, "Failed to delete pod #{pod_name}: #{resp.body["message"] || "Failed to delete pod"}"}
  end

  defp check_delete_response(%{status: code}, _pod_name), do: {:error, "Unexpected response status: #{code}"}

  defp check_list_response(%{status: 200} = resp), do: {:ok, resp.body["items"] || []}
  defp check_list_response(%{status: code} = resp) when code >= 400 do
    {:error, resp.body["message"] || "Failed to list pods"}
  end
  defp check_list_response(%{status: code}), do: {:error, "Unexpected response status: #{code}"}

  defp build_pods_url(namespace) do
    case ConfigLoader.get_api_server() do
      {:ok, server} -> {:ok, "#{server}/api/v1/namespaces/#{namespace}/pods"}
      {:error, reason} -> {:error, reason}
    end
  end

  defp build_pod_url(namespace, pod_name) do
    case ConfigLoader.get_api_server() do
      {:ok, server} -> {:ok, "#{server}/api/v1/namespaces/#{namespace}/pods/#{pod_name}"}
      {:error, reason} -> {:error, reason}
    end
  end

  defp build_request(url) do
    case ConfigLoader.get_auth_header() do
      {:ok, auth_header} ->
        headers = [{"authorization", auth_header}, {"content-type", "application/json"}]

        req_opts =
          case ConfigLoader.get_ca_cert() do
            {:ok, ca_cert} ->
              [headers: headers, transport_opts: [cacertfile: ca_cert]]

            :none ->
              # In-cluster or trusting system certs
              [headers: headers]
          end

        {:ok, Req.new(url: url) |> Req.Request.merge_options(req_opts)}
      {:error, reason} ->
        {:error, reason}
    end
  rescue
    e ->
      {:error, "Failed to build request: #{inspect(e)}"}
  end
end
