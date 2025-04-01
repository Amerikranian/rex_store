defmodule ExStore.RequestRouter do
  @moduledoc """
  Routes client requests to the appropriate node based on the key.

  Responsibilities:
  - Maps keys to responsible nodes using consistent hashing
  - Implements routing strategy appropriate for the consistency model
  - Forwards requests between nodes when necessary
  """
  use GenServer
  require Logger

  @doc """
  Starts the Request Router.
  """
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Put a key-value pair into the store.
  Returns :ok on success, or {:error, reason} on failure.
  """
  def put(key, value, opts \\ []) do
    GenServer.call(__MODULE__, {:put, key, value, opts})
  end

  @doc """
  Get the value for a key from the store.
  Returns {:ok, value} if the key exists, or {:error, :not_found} if it doesn't.
  """
  def get(key, opts \\ []) do
    GenServer.call(__MODULE__, {:get, key, opts})
  end

  @doc """
  Delete a key from the store.
  Returns :ok on success, or {:error, reason} on failure.
  """
  def delete(key, opts \\ []) do
    GenServer.call(__MODULE__, {:delete, key, opts})
  end

  @impl true
  def init(_opts) do
    Logger.info("Starting Request Router")
    {:ok, %{ring: nil}}
  end

  @impl true
  def handle_call({:put, key, value, opts}, _from, state) do
    case route_request(key, :write, state) do
      {:local, _node} ->
        metadata = Map.new(opts)
        consistency_controller = ExStore.Consistency.get_controller()

        case GenServer.call(consistency_controller, {:process_write, key, value, metadata}) do
          {:ok, _updated_metadata} ->
            {:reply, :ok, state}

          {:error, reason} ->
            {:reply, {:error, reason}, state}
        end

      {:remote, node} ->
        try do
          result = :rpc.call(String.to_atom(node), __MODULE__, :put, [key, value, opts])
          {:reply, result, state}
        catch
          :exit, reason ->
            {:reply, {:error, {:node_unreachable, node, reason}}, state}
        end
    end
  end

  @impl true
  def handle_call({:get, key, opts}, _from, state) do
    case route_request(key, :read, state) do
      {:local, _node} ->
        consistency_controller = ExStore.Consistency.get_controller()

        case GenServer.call(consistency_controller, {:process_read, key}) do
          {:ok, value, _metadata} ->
            {:reply, {:ok, value}, state}

          {:error, reason} ->
            {:reply, {:error, reason}, state}
        end

      {:remote, node} ->
        try do
          result = :rpc.call(String.to_atom(node), __MODULE__, :get, [key, opts])
          {:reply, result, state}
        catch
          :exit, reason ->
            {:reply, {:error, {:node_unreachable, node, reason}}, state}
        end
    end
  end

  @impl true
  def handle_call({:delete, _key, _opts}, _from, state) do
    # TODO
    {:reply, {:error, :not_implemented}, state}
  end

  @impl true
  def handle_info({:ring_updated, ring}, state) do
    {:noreply, %{state | ring: ring}}
  end

  defp route_request(_key, _operation_type, %{ring: nil} = _state) do
    # This is a fallback for initial bootstrapping
    {:local, Atom.to_string(Node.self())}
  end

  defp route_request(key, _operation_type, _state) do
    target_node =
      case ExStore.ClusterManager.get_node_for_key(key) do
        {:error, _} -> Atom.to_string(Node.self())
        node -> node
      end

    current_node = Atom.to_string(Node.self())

    if target_node == current_node do
      {:local, current_node}
    else
      {:remote, target_node}
    end
  end
end
