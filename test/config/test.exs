import Config

# Configure consistency model for testing
config :ex_store, :consistency_model, :eventual

# Configure replica count for testing
config :ex_store, :replica_count, 3

# Configure shorter timeouts for testing
# 1 second for tests (instead of normal 30 seconds)
config :ex_store, :gossip_interval, 1000

# Default empty topology for libcluster
# This will be replaced at runtime during tests
config :libcluster, :topologies, []
