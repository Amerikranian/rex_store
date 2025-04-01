defmodule ExStore.ClusterManager do
  @moduledoc """
  Manages cluster membership, node discovery, and the consistent hash ring.

  Responsibilities:
  - Cluster bootstrap with the selected consistency model
  - Handling node join/leave events
  - Failure detection via heartbeats
  - Maintaining the consistent hash ring
  """
  use GenServer
  require Logger

  @doc """
  Starts the Cluster Manager.
  """
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Gets the current node list in the cluster.
  """
  def get_nodes do
    GenServer.call(__MODULE__, :get_nodes)
  end

  @doc """
  Gets the node responsible for a given key.
  """
  def get_node_for_key(key) do
    GenServer.call(__MODULE__, {:get_node_for_key, key})
  end

  @doc """
  Gets nodes that should contain replicas for a given key.
  """
  def get_replica_nodes(key, count \\ 3) do
    GenServer.call(__MODULE__, {:get_replica_nodes, key, count})
  end

  @doc """
  Callback for libcluster to connect nodes.
  Used in topology configuration.
  """
  def connect_node(node) do
    GenServer.cast(__MODULE__, {:node_connected, node})
    :net_kernel.connect_node(node)
  end

  @doc """
  Callback for libcluster to disconnect nodes.
  Used in topology configuration.
  """
  def disconnect_node(node) do
    GenServer.cast(__MODULE__, {:node_disconnected, node})
    :erlang.disconnect_node(node)
  end

  @impl true
  def init(opts) do
    consistency_model = Keyword.get(opts, :consistency_model, :eventual)
    Logger.info("Starting Cluster Manager with consistency model: #{consistency_model}")

    {:ok, hash_ring} = ExHashRing.Ring.start_link()

    node_str = Atom.to_string(node_name())
    {:ok, _} = ExHashRing.Ring.add_node(hash_ring, node_str)

    :ok = :net_kernel.monitor_nodes(true, [:nodedown_reason])

    schedule_heartbeat()

    {:ok,
     %{
       consistency_model: consistency_model,
       ring: hash_ring,
       nodes: MapSet.new([node_name()]),
       node_states: %{node_name() => :alive}
     }}
  end

  @impl true
  def handle_call(:get_nodes, _from, %{nodes: nodes} = state) do
    {:reply, MapSet.to_list(nodes), state}
  end

  @impl true
  def handle_call({:get_node_for_key, key}, _from, %{ring: ring} = state) do
    {:ok, node_str} = ExHashRing.Ring.find_node(ring, key)
    node_atom = String.to_atom(node_str)
    {:reply, node_atom, state}
  end

  @impl true
  def handle_call({:get_replica_nodes, key, count}, _from, %{ring: ring, nodes: nodes} = state) do
    {:ok, primary_str} = ExHashRing.Ring.find_node(ring, key)
    primary = String.to_atom(primary_str)

    all_nodes = MapSet.to_list(nodes)

    if length(all_nodes) <= count do
      {:reply, all_nodes, state}
    else
      # In a more sophisticated implementation, we would get nodes based on their
      # position in the ring to ensure proper distribution
      # For now, we'll just get random nodes excluding the primary
      replicas =
        all_nodes
        |> Enum.reject(fn node -> node == primary end)
        |> Enum.take_random(count - 1)

      {:reply, [primary | replicas], state}
    end
  end

  @impl true
  def handle_cast({:node_connected, node}, state) do
    Logger.info("Node connected via libcluster: #{inspect(node)}")
    {:noreply, handle_node_join(node, state)}
  end

  @impl true
  def handle_cast({:node_disconnected, node}, state) do
    Logger.info("Node disconnected via libcluster: #{inspect(node)}")
    {:noreply, handle_node_leave(node, state)}
  end

  @impl true
  def handle_info(:heartbeat, state) do
    new_state = update_cluster_view(state)

    schedule_heartbeat()

    {:noreply, new_state}
  end

  @impl true
  def handle_info({:nodeup, node, _info}, state) do
    Logger.info("Node up event received: #{inspect(node)}")
    new_state = handle_node_join(node, state)
    {:noreply, new_state}
  end

  @impl true
  def handle_info({:nodedown, node, _info}, state) do
    Logger.info("Node down event received: #{inspect(node)}")
    new_state = handle_node_leave(node, state)
    {:noreply, new_state}
  end

  @impl true
  def handle_info({:nodeup, node}, state) do
    Logger.info("Node up event received (simple): #{inspect(node)}")
    new_state = handle_node_join(node, state)
    {:noreply, new_state}
  end

  @impl true
  def handle_info({:nodedown, node}, state) do
    Logger.info("Node down event received (simple): #{inspect(node)}")
    new_state = handle_node_leave(node, state)
    {:noreply, new_state}
  end

  # Catch-all for unexpected messages
  @impl true
  def handle_info(msg, state) do
    Logger.debug("Unhandled message in ClusterManager: #{inspect(msg)}")
    {:noreply, state}
  end

  defp update_cluster_view(state) do
    current_nodes = [node_name() | Node.list()]
    known_nodes = MapSet.to_list(state.nodes)

    joined_nodes = current_nodes -- known_nodes

    left_nodes = known_nodes -- current_nodes

    new_state =
      Enum.reduce(joined_nodes, state, fn node, acc ->
        handle_node_join(node, acc)
      end)

    new_state =
      Enum.reduce(left_nodes, new_state, fn node, acc ->
        handle_node_leave(node, acc)
      end)

    new_state
  end

  defp handle_node_join(node, %{ring: ring, nodes: nodes} = state) do
    # Skip if we already know about this node
    if MapSet.member?(nodes, node) do
      state
    else
      Logger.info("Node joined: #{inspect(node)}")

      node_str = Atom.to_string(node)
      {:ok, _} = ExHashRing.Ring.add_node(ring, node_str)

      new_state = %{
        state
        | ring: ring,
          nodes: MapSet.put(nodes, node),
          node_states: Map.put(state.node_states, node, :alive)
      }

      publish_ring_update(ring)

      new_state
    end
  end

  defp handle_node_leave(node, %{ring: ring, nodes: nodes} = state) do
    # Skip if we don't know about this node or it's our own node
    if node == node_name() or not MapSet.member?(nodes, node) do
      state
    else
      Logger.warning("Node left or failed: #{inspect(node)}")

      node_str = Atom.to_string(node)
      {:ok, _} = ExHashRing.Ring.remove_node(ring, node_str)

      new_state = %{
        state
        | ring: ring,
          nodes: MapSet.delete(nodes, node),
          node_states: Map.put(state.node_states, node, :down)
      }

      publish_ring_update(ring)

      new_state
    end
  end

  defp node_name do
    Node.self()
  end

  defp schedule_heartbeat do
    Process.send_after(self(), :heartbeat, 5000)
  end

  defp publish_ring_update(ring) do
    # Notify the consistency controller and request router about the updated ring
    controller =
      case Process.whereis(ExStore.Consistency.Controller) do
        nil -> nil
        _ -> ExStore.Consistency.Controller
      end

    for component <- [ExStore.RequestRouter, controller] do
      if component != nil && Process.whereis(component) do
        send(component, {:ring_updated, ring})
      end
    end
  end
end
