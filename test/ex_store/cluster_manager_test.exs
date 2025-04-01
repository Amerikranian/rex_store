defmodule ExStore.ClusterManagerTest do
  use ExUnit.Case
  doctest ExStore.ClusterManager

  alias ExStore.ClusterManager

  describe "single node environment" do
    test "local ring maps keys to the current node" do
      current_node = Node.self()
      nodes = ClusterManager.get_nodes()

      assert current_node in nodes

      test_keys = ["key1", "key2", "user:123", "product:456"]

      for key <- test_keys do
        node_for_key = ClusterManager.get_node_for_key(key)

        assert node_for_key == current_node,
               "Expected key '#{key}' to map to #{current_node}, but got #{node_for_key}"
      end
    end
  end

  describe "hash ring functionality" do
    setup do
      {:ok, ring} = ExHashRing.Ring.start_link()
      %{ring: ring}
    end

    test "key distribution is even across nodes", %{ring: ring} do
      # Create unique node names to avoid conflicts
      test_nodes = Enum.map(1..6, fn i -> :"test_node#{i}@localhost" end)

      # Add nodes to the ring
      for node <- test_nodes do
        # Convert node to string for ExHashRing
        node_str = Atom.to_string(node)
        {:ok, _} = ExHashRing.Ring.add_node(ring, node_str)
      end

      # Generate test keys
      test_keys = Enum.map(1..1000, fn i -> "test_key_#{i}" end)

      # Map keys to nodes and count
      node_counts =
        Enum.reduce(test_keys, %{}, fn key, acc ->
          {:ok, node} = ExHashRing.Ring.find_node(ring, key)
          Map.update(acc, node, 1, &(&1 + 1))
        end)

      # Check distribution
      total_keys = length(test_keys)
      expected_per_node = total_keys / length(test_nodes)
      # 20%
      max_acceptable_deviation = 20.0

      for {node, count} <- node_counts do
        deviation = abs(count - expected_per_node) / expected_per_node * 100

        assert deviation <= max_acceptable_deviation,
               "Node #{node} has deviation of #{deviation}% which exceeds the maximum acceptable deviation of #{max_acceptable_deviation}%"
      end
    end
  end
end
