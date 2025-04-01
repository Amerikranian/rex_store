defmodule ExStore.Consistency.EventualController do
  @moduledoc """
  Implementation of eventual consistency model.

  Features (eventually):
  - Asynchronous replication to N successor nodes
  - Last-write-wins conflict resolution using timestamps
  - Background gossip protocol for anti-entropy
  """
  use GenServer
  require Logger
  @behaviour ExStore.Consistency.Behaviour

  # Number of replicas for each key
  @default_replica_count 3
  # Interval for anti-entropy gossip in milliseconds
  @gossip_interval 30_000

  @doc """
  Starts the EventualController.
  """
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl GenServer
  def init(opts) do
    Logger.info("Starting Eventual Consistency Controller")
    replica_count = Keyword.get(opts, :replica_count, @default_replica_count)

    schedule_gossip()

    state = %{
      replica_count: replica_count,
      vector_clock: %{},
      pending_writes: %{},
      ring: nil
    }

    {:ok, state}
  end

  @impl GenServer
  def handle_info(:gossip, state) do
    Logger.debug("Running anti-entropy gossip")
    new_state = perform_anti_entropy_gossip(state)

    schedule_gossip()

    {:noreply, new_state}
  end

  @impl GenServer
  def handle_info({:ring_updated, ring}, state) do
    Logger.info("Ring topology updated in EventualController")
    {:noreply, %{state | ring: ring}}
  end

  @impl GenServer
  def handle_call({:process_write, key, value, metadata}, _from, state) do
    metadata = Map.put_new(metadata, :timestamp, System.system_time(:millisecond))

    case ExStore.Storage.Engine.put(key, value, metadata) do
      :ok ->
        Task.start(fn -> replicate_write(key, value, metadata, state.replica_count) end)
        {:reply, {:ok, metadata}, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl GenServer
  def handle_call({:process_read, key}, _from, state) do
    case ExStore.Storage.Engine.get(key) do
      {:ok, value, metadata} ->
        {:reply, {:ok, value, metadata}, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl GenServer
  def handle_call({:handle_replication, key, value, metadata}, _from, state) do
    case ExStore.Storage.Engine.get(key) do
      {:ok, _existing_value, existing_metadata} ->
        if Map.get(metadata, :timestamp, 0) > Map.get(existing_metadata, :timestamp, 0) do
          :ok = ExStore.Storage.Engine.put(key, value, metadata)
          {:reply, {:ok, state}, state}
        else
          {:reply, {:ok, state}, state}
        end

      {:error, :not_found} ->
        :ok = ExStore.Storage.Engine.put(key, value, metadata)
        {:reply, {:ok, state}, state}

      {:error, reason} ->
        {:reply, {:error, reason, state}, state}
    end
  end

  @impl GenServer
  def handle_call({:resolve_conflicts, _key, versions}, _from, state) do
    {winning_value, winning_metadata} =
      Enum.reduce(versions, {nil, %{timestamp: 0}}, fn {value, metadata},
                                                       {best_value, best_metadata} ->
        current_ts = Map.get(metadata, :timestamp, 0)
        best_ts = Map.get(best_metadata, :timestamp, 0)

        if current_ts > best_ts do
          {value, metadata}
        else
          {best_value, best_metadata}
        end
      end)

    {:reply, {:ok, winning_value, winning_metadata, state}, state}
  end

  @impl GenServer
  def handle_call({:handle_ring_update, ring}, _from, state) do
    {:reply, {:ok, %{state | ring: ring}}, %{state | ring: ring}}
  end

  @impl ExStore.Consistency.Behaviour
  def initialize(opts) do
    {:ok,
     %{
       replica_count: Keyword.get(opts, :replica_count, @default_replica_count),
       vector_clock: %{},
       pending_writes: %{},
       ring: nil
     }}
  end

  @impl ExStore.Consistency.Behaviour
  def process_write(key, value, metadata, state) do
    metadata = Map.put_new(metadata, :timestamp, System.system_time(:millisecond))

    case ExStore.Storage.Engine.put(key, value, metadata) do
      :ok ->
        Task.start(fn -> replicate_write(key, value, metadata, state.replica_count) end)
        {:ok, metadata, state}

      {:error, reason} ->
        {:error, reason, state}
    end
  end

  @impl ExStore.Consistency.Behaviour
  def process_read(key, state) do
    case ExStore.Storage.Engine.get(key) do
      {:ok, value, metadata} ->
        {:ok, value, metadata, state}

      {:error, reason} ->
        {:error, reason, state}
    end
  end

  @impl ExStore.Consistency.Behaviour
  def handle_replication(key, value, metadata, state) do
    case ExStore.Storage.Engine.get(key) do
      {:ok, _existing_value, existing_metadata} ->
        if Map.get(metadata, :timestamp, 0) > Map.get(existing_metadata, :timestamp, 0) do
          :ok = ExStore.Storage.Engine.put(key, value, metadata)
          {:ok, state}
        else
          {:ok, state}
        end

      {:error, :not_found} ->
        :ok = ExStore.Storage.Engine.put(key, value, metadata)
        {:ok, state}

      {:error, reason} ->
        {:error, reason, state}
    end
  end

  @impl ExStore.Consistency.Behaviour
  def resolve_conflicts(_key, versions, state) do
    {winning_value, winning_metadata} =
      Enum.reduce(versions, {nil, %{timestamp: 0}}, fn {value, metadata},
                                                       {best_value, best_metadata} ->
        current_ts = Map.get(metadata, :timestamp, 0)
        best_ts = Map.get(best_metadata, :timestamp, 0)

        if current_ts > best_ts do
          {value, metadata}
        else
          {best_value, best_metadata}
        end
      end)

    {:ok, winning_value, winning_metadata, state}
  end

  @impl ExStore.Consistency.Behaviour
  def handle_ring_update(ring, state) do
    {:ok, %{state | ring: ring}}
  end

  defp schedule_gossip do
    Process.send_after(self(), :gossip, @gossip_interval)
  end

  defp perform_anti_entropy_gossip(state) do
    # Implementation of anti-entropy gossip
    # 1. Select a random node from the cluster
    # 2. Exchange metadata about stored keys
    # 3. Synchronize any differences
    nodes =
      case ExStore.ClusterManager.get_nodes() do
        [] -> []
        # Just us, no gossip needed
        [_] -> []
        node_list -> node_list -- [Atom.to_string(Node.self())]
      end

    if !Enum.empty?(nodes) do
      # Pick a random node to gossip with
      target_node = Enum.random(nodes)
      Logger.debug("Gossiping with node: #{target_node}")

      # In a real implementation, we'd send a subset of our keys to compare
      # For simplicity, we'll skip the actual implementation here
    end

    state
  end

  defp replicate_write(key, value, metadata, replica_count) do
    case ExStore.ClusterManager.get_replica_nodes(key, replica_count) do
      [] ->
        # No other nodes to replicate to
        :ok

      [_self_node] ->
        # Only ourselves, no need to replicate
        :ok

      replica_nodes ->
        # Filter out our own node
        other_nodes = replica_nodes -- [Atom.to_string(Node.self())]

        for node <- other_nodes do
          try do
            target_node = String.to_atom(node)

            GenServer.cast(
              {__MODULE__, target_node},
              {:replicate, key, value, metadata}
            )
          catch
            kind, reason ->
              Logger.warning(
                "Failed to replicate to node #{node}: #{inspect(kind)}, #{inspect(reason)}"
              )
          end
        end
    end
  end

  @impl GenServer
  def handle_cast({:replicate, key, value, metadata}, state) do
    case handle_replication(key, value, metadata, state) do
      {:ok, new_state} ->
        {:noreply, new_state}

      {:error, reason, state} ->
        Logger.warning("Replication error for key #{key}: #{inspect(reason)}")
        {:noreply, state}
    end
  end
end
