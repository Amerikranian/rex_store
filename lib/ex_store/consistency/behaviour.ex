defmodule ExStore.Consistency.Behaviour do
  @moduledoc """
  Behaviour that all consistency model implementations must follow.

  This defines the common interface for different consistency models.
  """

  @doc """
  Initialize the consistency controller with appropriate configuration.
  """
  @callback initialize(opts :: Keyword.t()) :: {:ok, state :: term()}

  @doc """
  Process a write operation for a key-value pair with associated metadata.
  Returns {:ok, metadata} on success, or {:error, reason} on failure.
  """
  @callback process_write(key :: String.t(), value :: term(), metadata :: map(), state :: term()) ::
              {:ok, metadata :: map(), new_state :: term()}
              | {:error, reason :: term(), state :: term()}

  @doc """
  Process a read operation for a key.
  Returns {:ok, value, metadata} if the key exists, or {:error, :not_found} if it doesn't.
  """
  @callback process_read(key :: String.t(), state :: term()) ::
              {:ok, value :: term(), metadata :: map(), new_state :: term()}
              | {:error, reason :: term(), state :: term()}

  @doc """
  Handle replication of a key-value pair to this node.
  """
  @callback handle_replication(
              key :: String.t(),
              value :: term(),
              metadata :: map(),
              state :: term()
            ) ::
              {:ok, new_state :: term()} | {:error, reason :: term(), state :: term()}

  @doc """
  Resolve conflicts between multiple versions of the same key.
  Returns the winning version and updated metadata.
  """
  @callback resolve_conflicts(key :: String.t(), versions :: list(), state :: term()) ::
              {:ok, value :: term(), metadata :: map(), new_state :: term()}

  @doc """
  Handle a notification about a ring topology change.
  """
  @callback handle_ring_update(ring :: term(), state :: term()) ::
              {:ok, new_state :: term()}
end

defmodule ExStore.Consistency do
  @moduledoc """
  Helper functions for working with consistency controllers.
  """

  @doc """
  Gets the appropriate controller module based on the configured consistency model.
  """
  def get_controller do
    case Application.get_env(:ex_store, :consistency_model, :eventual) do
      :eventual ->
        ExStore.Consistency.EventualController
    end
  end
end
