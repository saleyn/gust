defmodule GustK8s.MixProject do
  use Mix.Project

  def project do
    [
      app: :gust_k8s,
      version: "0.1.0",
      build_path: "../../_build",
      config_path: "../../config/config.exs",
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.14",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      elixirc_paths: elixirc_paths(Mix.env()),
      aliases: aliases()
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      {:gust, in_umbrella: true},
      {:req, "~> 0.5"},
      {:mox, "~> 1.1", only: :test}
    ]
  end

  defp aliases do
    [
      test: ["test"]
    ]
  end
end
