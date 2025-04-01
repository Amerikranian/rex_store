defmodule ExStore.Storage.EngineTest do
  use ExUnit.Case
  alias ExStore.Storage.Engine

  setup do
    # The Engine is already started by the application
    # Clear its state between tests instead of trying to restart it
    :sys.replace_state(Engine, fn _state -> %{store: %{}} end)
    :ok
  end

  describe "put/3 and get/1" do
    test "stores and retrieves a value" do
      key = "test_key"
      value = "test_value"

      assert :ok = Engine.put(key, value)
      assert {:ok, ^value, metadata} = Engine.get(key)
      assert is_map(metadata)
      assert Map.has_key?(metadata, :timestamp)
    end

    test "stores and retrieves a value with custom metadata" do
      key = "test_key"
      value = "test_value"
      metadata = %{version: 1, priority: :high}

      assert :ok = Engine.put(key, value, metadata)
      assert {:ok, ^value, stored_metadata} = Engine.get(key)
      assert stored_metadata.version == 1
      assert stored_metadata.priority == :high
      assert Map.has_key?(stored_metadata, :timestamp)
    end

    test "returns error when getting a non-existent key" do
      assert {:error, :not_found} = Engine.get("non_existent_key")
    end
  end

  describe "delete/1" do
    test "deletes an existing key" do
      key = "test_key"
      Engine.put(key, "test_value")

      assert :ok = Engine.delete(key)
      assert {:error, :not_found} = Engine.get(key)
    end

    test "deleting a non-existent key is idempotent" do
      assert :ok = Engine.delete("non_existent_key")
    end
  end

  describe "get_keys_in_range/3" do
    test "returns keys in the specified range" do
      Engine.put("a", "value_a")
      Engine.put("b", "value_b")
      Engine.put("c", "value_c")
      Engine.put("d", "value_d")

      assert {:ok, keys} = Engine.get_keys_in_range("a", "c", 10)
      assert length(keys) == 3
      assert "a" in keys
      assert "b" in keys
      assert "c" in keys
      refute "d" in keys
    end

    test "respects the limit parameter" do
      for i <- 1..10 do
        # Pad numbers with zeros to ensure proper string ordering
        key = "test_key_#{String.pad_leading("#{i}", 2, "0")}"
        Engine.put(key, "value_#{i}")
      end

      assert {:ok, keys} = Engine.get_keys_in_range("test_key_01", "test_key_10", 5)
      assert length(keys) == 5
    end

    test "returns empty list when no keys in range" do
      Engine.put("a", "value_a")
      Engine.put("z", "value_z")

      assert {:ok, keys} = Engine.get_keys_in_range("b", "y", 10)
      assert keys == []
    end
  end
end
