defmodule ExStoreTest do
  use ExUnit.Case
  doctest ExStore

  test "greets the world" do
    assert ExStore.hello() == :world
  end
end
