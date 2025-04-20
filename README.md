# Rex Store

## What is this?

An eventually consistent key value store using gossip protocols.

Said protocol is simple: peers maintain a list of active participants in the system and every now and then exchange their full state with a random peer selected from said list.

As of now, dynamic peer discovery is extremely limited and barely could be considered functional.

## How to run

You will need 2 terminals: run the following in one of them.

```bash
cargo run
```

In another:

```bash
cargo run --bin client
```

You may run help on either binary to get more information:

```bash
cargo run -- --help
```

Or

```bash
cargo run --bin client -- --help
```

If you want to see tests, the standard command should work:

```bash
cargo test
```
