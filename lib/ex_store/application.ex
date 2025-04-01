defmodule ExStore.Application do
  @moduledoc """
  The ExStore Application.

  Starts all core components of the distributed key-value store.
  """
  use Application
  require Logger

  @impl true
  def start(_type, _args) do
    consistency_model = Application.get_env(:ex_store, :consistency_model, :eventual)

    topologies = Application.get_env(:libcluster, :topologies, [])

    children = [
      # PubSub system
      {Phoenix.PubSub, name: ExStore.PubSub},
      {Cluster.Supervisor, [topologies, [name: ExStore.ClusterSupervisor]]},
      {ExStore.ClusterManager, [consistency_model: consistency_model]},
      {ExStore.Storage.Engine, []},
      consistency_controller(consistency_model),
      {ExStore.RequestRouter, []}
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    opts = [strategy: :one_for_one, name: ExStore.Supervisor]

    Logger.info("Starting ExStore with consistency model: #{consistency_model}")
    Logger.info("Node name: #{Node.self()}")
    Logger.info("Cluster topologies: #{inspect(topologies)}")

    Supervisor.start_link(children, opts)
  end

  defp consistency_controller(:eventual) do
    {ExStore.Consistency.EventualController, []}
  end

  defp consistency_controller(unknown) do
    raise "Unknown consistency model: #{inspect(unknown)}"
  end
end
