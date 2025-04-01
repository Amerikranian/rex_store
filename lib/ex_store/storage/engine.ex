defmodule ExStore.Storage.Engine do
  @moduledoc """
  Storage engine for the key-value store.

  Responsibilities:
  - Provides local persistence for key-value pairs
  - Maintains versioning metadata appropriate for the consistency model
  - Handles data transfer during rebalancing
  """
  use GenServer
  require Logger

  @doc """
  Starts the Storage Engine.
  """
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Put a key-value pair with associated metadata.
  """
  def put(key, value, metadata \\ %{}) do
    GenServer.call(__MODULE__, {:put, key, value, metadata})
  end

  @doc """
  Get the value and metadata for a key.
  """
  def get(key) do
    GenServer.call(__MODULE__, {:get, key})
  end

  @doc """
  Delete a key.
  """
  def delete(key) do
    GenServer.call(__MODULE__, {:delete, key})
  end

  @doc """
  Get all keys in a range.
  """
  def get_keys_in_range(start_key, end_key, limit \\ 1000) do
    GenServer.call(__MODULE__, {:get_keys_in_range, start_key, end_key, limit})
  end

  @impl true
  def init(_opts) do
    Logger.info("Starting Storage Engine")

    {:ok, %{store: %{}}}
  end

  @impl true
  def handle_call({:put, key, value, metadata}, _from, %{store: store} = state) do
    metadata = Map.put_new(metadata, :timestamp, System.system_time(:millisecond))

    new_store = Map.put(store, key, {value, metadata})
    {:reply, :ok, %{state | store: new_store}}
  end

  @impl true
  def handle_call({:get, key}, _from, %{store: store} = state) do
    case Map.fetch(store, key) do
      {:ok, {value, metadata}} ->
        {:reply, {:ok, value, metadata}, state}

      :error ->
        {:reply, {:error, :not_found}, state}
    end
  end

  @impl true
  def handle_call({:delete, key}, _from, %{store: store} = state) do
    new_store = Map.delete(store, key)
    {:reply, :ok, %{state | store: new_store}}
  end

  @impl true
  def handle_call({:get_keys_in_range, start_key, end_key, limit}, _from, %{store: store} = state) do
    keys =
      store
      |> Map.keys()
      |> Enum.filter(fn k -> k >= start_key && k <= end_key end)
      |> Enum.take(limit)

    {:reply, {:ok, keys}, state}
  end
end
