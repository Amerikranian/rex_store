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


## Dependencies
We use [hwloc2](https://crates.io/crates/hwloc2). Run something like
```bash
sudo apt install libhwloc-dev
```
if you're on Linux. If you're on Mac, try and do it from brew:
```bash
brew install hwloc
```
If you're on Windows, please use WSL, as building the library is far from trivial. Note that the system will still work if it could not find hwloc, but the core pinning will not necessarily be accurate, I.e, one logical node per one physical core.