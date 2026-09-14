defmodule GustK8s.ConfigLoader do
  @moduledoc """
  Loads Kubernetes authentication configuration from in-cluster or kubeconfig.

  Supports:
  - In-cluster authentication (service account token and CA cert from standard K8s volumes)
  - Kubeconfig file (from $KUBECONFIG or ~/.kube/config)
  """

  @in_cluster_host "https://kubernetes.default.svc"
  @in_cluster_token_path "/var/run/secrets/kubernetes.io/serviceaccount/token"
  @in_cluster_ca_path "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"

  @doc """
  Get the Kubernetes API server URL.

  Returns:
  - `{:ok, url}` — the API server URL
  - `{:error, reason}` — if auth configuration cannot be determined
  """
  def get_api_server do
    cond do
      in_cluster?() ->
        {:ok, @in_cluster_host}

      kubeconfig = System.get_env("KUBECONFIG") ->
        {:ok, load_server_from_kubeconfig(kubeconfig)}

      File.exists?(home_kubeconfig()) ->
        {:ok, load_server_from_kubeconfig(home_kubeconfig())}

      true ->
        {:error, "Could not determine Kubernetes API server. Not running in-cluster and no kubeconfig found."}
    end
  rescue
    e ->
      {:error, "Failed to get API server: #{inspect(e)}"}
  end

  @doc """
  Get the Authorization header value (Bearer token).

  Returns:
  - `{:ok, "Bearer token"}` — the authorization header
  - `{:error, reason}` — if token cannot be determined
  """
  def get_auth_header do
    cond do
      in_cluster?() && File.exists?(@in_cluster_token_path) ->
        case File.read(@in_cluster_token_path) do
          {:ok, token} ->
            {:ok, "Bearer #{String.trim(token)}"}

          {:error, reason} ->
            {:error, "Failed to read in-cluster token: #{inspect(reason)}"}
        end

      kubeconfig = System.get_env("KUBECONFIG") ->
        load_token_from_kubeconfig(kubeconfig)

      File.exists?(home_kubeconfig()) ->
        load_token_from_kubeconfig(home_kubeconfig())

      true ->
        {:error, "Could not determine Kubernetes authentication token"}
    end
  rescue
    e ->
      {:error, "Failed to get auth header: #{inspect(e)}"}
  end

  @doc """
  Get the CA certificate for TLS validation.

  Returns:
  - `{:ok, pem_cert}` — the CA certificate in PEM format
  - `:none` — if running in-cluster (CA will be system-managed)
  """
  def get_ca_cert do
    if in_cluster?() && File.exists?(@in_cluster_ca_path) do
      case File.read(@in_cluster_ca_path) do
        {:ok, cert} -> {:ok, cert}
        {:error, _reason} -> :none
      end
    else
      :none
    end
  end

  # Private helpers

  defp in_cluster? do
    File.exists?(@in_cluster_token_path) && File.exists?(@in_cluster_ca_path)
  end

  defp home_kubeconfig do
    Path.expand("~/.kube/config")
  end

  defp load_server_from_kubeconfig(path) do
    case :glazer_yaml.read_file(path) do
      config when is_map(config) ->
        clusters = Map.get(config, "clusters", [])

        case clusters do
          [%{"cluster" => %{"server" => server}} | _] ->
            server

          _ ->
            raise "No valid cluster configuration found in kubeconfig"
        end

      _ ->
        raise "Could not parse kubeconfig"
    end
  end

  defp load_token_from_kubeconfig(path) do
    config = Glazer.YAML.read_file!(path)
    users = Map.get(config, "users", [])

    token =
      users
      |> Enum.find_value(fn user ->
        user_name = user["name"]
        user_data = user["user"] || %{}

        if user_name && Map.has_key?(user_data, "token") do
          user_data["token"]
        end
      end)

    case token do
      nil -> {:error, "No token found in kubeconfig"}
      token -> {:ok, "Bearer #{token}"}
    end
  rescue
    e ->
      {:error, "Failed to load kubeconfig: #{inspect(e)}"}
  end
end
