# Distributed Systems — Spring 2025 (University of Tehran)

This repository contains my solutions and working code for distributed systems labs, adapted from the MIT 6.5840 (2025) lab framework. The repo is organized into three project folders that mirror the 6.5840 lab structure (MapReduce, Raft, Key/Value service, and Sharded KV).

The code is Go modules–based and targets Go 1.22. Test suites are provided for each component.

## Prerequisites

- Go 1.22 or newer
- Git
- For MapReduce plugin-based tests: a Unix-like environment (Linux or macOS). Go plugins are not supported on Windows; use WSL (recommended) or run on Linux/macOS for those tests.
- Optional but useful: Make (for creating submission tarballs via the provided Makefiles)

Windows note:
- You can build and run most Go tests natively with PowerShell. However, MapReduce tests (`test-mr.sh`) and plugin builds (`-buildmode=plugin`) require WSL/Linux/macOS.
- Where this README shows bash commands/scripts, run them inside WSL. PowerShell commands are indicated as such.

## Repository layout

- `Project1/`
	- `Makefile`, `LICENSE`, docs
	- `src/` (module root: `6.5840`)
		- `mr/`, `mrapps/`, `main/`: MapReduce framework, apps, and test harness
		- `raft1/`: Raft consensus
		- `kvsrv1/`: Simple KV server (single node)
		- `kvraft1/`: Fault-tolerant KV on Raft
		- `shardkv1/`: Sharded KV and shard controller
		- support libs: `labgob/`, `labrpc/`, `tester1/`, `models1/`, etc.

- `Project2/6.5840/` and `Project3/6.5840/`
	- Similar structure to `Project1/src/` with their own `go.mod` under `.../src/`

Each `src/` directory is a Go module with path `6.5840`, so run `go` commands from inside the corresponding `src/` folder.

## What each project builds (big picture)

Here’s a high-level tour of the tasks and what you’ll implement/verify:

- MapReduce framework (MR)
	- Build a simplified MapReduce system with a coordinator and workers.
	- Implement task assignment, worker crashes/restarts, and parallel map/reduce execution.
	- Validate parallelism and fault tolerance using the provided plugins (e.g., word count, indexer, crash behavior).

- kvsrv1 (single-node KV)
	- A baseline key/value server with a client library and JSON-based operations in tests.
	- Focus on correct semantics (Get/Put with versions), concurrency handling, and basic reliability under an unreliable RPC layer.

- Raft (3A–3C)
	- 3A: Leader election and heartbeats. Ensure a single leader emerges and remains stable without failures.
	- 3B: Log replication and commitment. Clients submit commands; followers replicate; the system commits in order.
	- 3C: Persist state and handle crashes/restarts; ensure safety and liveness across failures and partitions.

- kvraft1 (4A–4C): KV service backed by Raft
	- Build a linearizable KV store on top of Raft with at-most-once semantics via client request IDs.
	- 4A: Integrate with Raft for basic operations under reliability.
	- 4B: Cope with unreliable networks, partitions, and leader changes; maintain linearizability.
	- 4C: Implement snapshots to keep Raft state bounded (triggered when log grows), and restore on restart.

- shardkv1 (5A–5C): Sharded KV and reconfiguration
	- 5A: Shard controller: maintain configurations that map shards to groups; clients consult the controller.
	- 5B: Shard migration: when configurations change, move shard data between Raft groups safely and correctly.
	- 5C: Advanced reconfiguration and fault tolerance: ensure progress during concurrent joins/leaves, crashes, and partitions.

## Quick start

PowerShell (Windows, native) example for running a unit test package:

```powershell
# Example: run KV server tests in Project1
Set-Location Project1/src
go test ./kvsrv1 -count=1
```

WSL/Linux/macOS bash example for MapReduce end-to-end tests:

```bash
# Example: run MapReduce test harness in Project1 (requires plugins)
cd Project1/src/main
chmod +x test-mr.sh
./test-mr.sh
```

## How to run tests (Project1)

All commands below assume your shell is positioned at `Project1/src` unless noted.

General tips:
- Add `-race` to enable the race detector on Unix/macOS. On some platforms, plugin + race can be flaky; the test script already disables it for certain Go/Mac combos.
- Use `-run <regex>` to select subsets of tests.

### MapReduce (MR)

MapReduce tests are driven by bash scripts and require Go plugins:

WSL/Linux/macOS:

```bash
cd Project1/src/main
chmod +x test-mr.sh
./test-mr.sh              # full MR test suite
# or run multiple trials
chmod +x test-mr-many.sh
./test-mr-many.sh 3
```

Notes:
- The script builds plugins from `mrapps/*.go` using `-buildmode=plugin` and builds `mrcoordinator`, `mrworker`, and `mrsequential` from `main/`.
- On Windows, use WSL to run these scripts; Go plugins are not available on native Windows builds.

### Raft (raft1)

Run the Raft tests. You can target parts (3A, 3B, 3C) using `-run`:

```powershell
Set-Location Project1/src
# all raft tests
go test ./raft1 -count=1
# just 3A tests (examples: TestInitialElection3A, TestReElection3A, ...)
go test ./raft1 -run 3A -count=1
# 3B, 3C similarly
go test ./raft1 -run 3B -count=1
go test ./raft1 -run 3C -count=1
```

### Single-node KV service (kvsrv1)

```powershell
Set-Location Project1/src
go test ./kvsrv1 -count=1
```

Examples in the suite cover reliable/unreliable RPC, concurrency, and memory usage.

### KV on Raft (kvraft1)

These tests stress the KV layer built on Raft. They also use `raft1/rsm` utilities internally.

```powershell
Set-Location Project1/src
# run all kvraft tests
go test ./kvraft1 -count=1
# or filter (e.g., snapshot-related)
go test ./kvraft1 -run Snapshot -count=1
```

### Sharded KV (shardkv1)

Shard controller and sharded KV group tests. You can filter by parts (5A, 5B, 5C) similarly.

```powershell
Set-Location Project1/src
# all sharded KV tests
go test ./shardkv1 -count=1
# part-specific
go test ./shardkv1 -run 5A -count=1
go test ./shardkv1 -run 5B -count=1
go test ./shardkv1 -run 5C -count=1
```

## How to run tests (Project2 and Project3)

`Project2/6.5840/src` and `Project3/6.5840/src` are separate Go modules mirroring the same package layout. Run commands from within each module’s `src` directory, e.g.:

```powershell
# Project2 examples
Set-Location Project2/6.5840/src
go test ./raft1 -count=1
go test ./kvsrv1 -count=1

# Project3 examples
Set-Location Project3/6.5840/src
go test ./kvraft1 -count=1
go test ./shardkv1 -count=1
```

For MapReduce in Project2/Project3, use WSL/Linux/macOS and run the `test-mr.sh` inside `.../src/main/` if present.

## Building individual binaries (optional)

Inside `Project1/src/main/` you can build the MR binaries manually:

```bash
# WSL/Linux/macOS
cd Project1/src/main
go build mrcoordinator.go
go build mrworker.go
go build mrsequential.go
```

To run a simple MR job manually (WSL/Linux/macOS):

```bash
# Build an MR app plugin (e.g., word count)
cd Project1/src/mrapps
go build -buildmode=plugin wc.go

# In another terminal
cd Project1/src/main
./mrcoordinator ../main/pg-*.txt
# Then start one or more workers
./mrworker ../mrapps/wc.so
```

## Submission tarballs (optional)

Each project includes a `Makefile` with targets such as `lab1`, `lab2`, `lab3a`, ..., `lab5c`. From the project root (same folder as the `Makefile`):

```bash
# Linux/macOS/WSL
cd Project1
make lab1
```

This will create a `lab1-handin.tar.gz` containing the `src/` subtree (excluding binaries and large files) for upload to a grader. The `check-` target uses the provided `.check-build` to validate a clean build against the MIT reference.

On Windows without Make, either use WSL or create the tarball manually.

## Troubleshooting

- Module path: run `go test` from the `src/` directory of the project so that imports like `6.5840/raft1` resolve correctly.
- Plugins on Windows: use WSL for MapReduce tests that require `-buildmode=plugin`.
- Race detector: you can add `-race` to many `go test` runs. In MR, the script may disable `-race` for certain OS/Go versions.
- Long-running tests: some Raft and Sharded KV tests are time-based and may take minutes. Use `-run` to target a subset while iterating.

## Acknowledgements

This coursework builds on the MIT 6.5840 labs (2025 edition). Files, structure, and many tests originate from that framework. All rights remain with their respective authors.

## License

See `Project1/LICENSE` for the original lab license. Any additional code by me is provided under the same license unless stated otherwise.
