import Config

config :ex_store,
  consistency_model: :eventual

config :libcluster,
  topologies: [
    example: [
      # The selected clustering strategy. Required.
      strategy: Cluster.Strategy.Epmd,

      # Configuration for the provided strategy. Optional.
      # This example uses EPMD with explicit host list
      config: [hosts: [:"node1@127.0.0.1", :"node2@127.0.0.1"]],

      # Custom connect/disconnect functions that will notify our ClusterManager
      connect: {ExStore.ClusterManager, :connect_node, []},
      disconnect: {ExStore.ClusterManager, :disconnect_node, []},

      # Default functions for listing nodes
      list_nodes: {:erlang, :nodes, [:connected]}
    ]
  ]

config :logger, level: :info
