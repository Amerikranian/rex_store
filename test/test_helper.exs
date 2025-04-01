ExUnit.start(exclude: [:multi_node])

Code.require_file("support/test_cluster.ex", __DIR__)

if System.get_env("INCLUDE_MULTI_NODE") do
  ExUnit.configure(exclude: [])
  IO.puts("Including multi-node tests")
end

Application.ensure_all_started(:ex_store)
