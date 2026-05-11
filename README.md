# Message Broker System

A Kafka-inspired message broker written in Go. This repository implements a durable, file-backed messaging system with a TCP protocol, segment-based log storage, consumer group offset tracking, and startup recovery.

This project is intentionally smaller than Apache Kafka, but it follows several of the same core ideas:

- messages are appended to an immutable log
- data is stored on disk rather than only in memory
- offsets identify a consumer's position in the stream
- consumer groups advance independently
- the broker can recover its state after a restart
- logs rotate into segments so the system can grow without a single infinite file

If you are familiar with Kafka, the easiest way to understand this project is to think of it as a simplified single-broker version of Kafka's storage and consumption model. It does not implement topics, partitions, replication, leader election, rebalancing, or compression yet, but it does implement the foundations that make a log-based broker useful.

## What Kafka Is

Apache Kafka is a distributed event streaming platform. At a high level, Kafka is used to move data from producers to consumers reliably and at scale.

Kafka's most important ideas are:

- **Producers** write records into Kafka.
- **Consumers** read records from Kafka.
- **Topics** are named streams of records.
- **Partitions** split a topic into ordered shards for parallelism and throughput.
- **Offsets** identify a consumer's position inside a partition.
- **Consumer groups** coordinate multiple consumers so work can be shared.
- **Replication** keeps multiple copies of data for fault tolerance.
- **Segments** break a long log into manageable files.
- **Retention** removes older data according to time or size rules.

Kafka is not just a queue. It is a distributed log with replayable history. That design gives it durability, high throughput, and the ability for multiple consumers to read the same data independently.

## What This Project Implements

This repository is a compact implementation of the same log-centric model.

It currently provides:

- a TCP broker listening on `:8080`
- a producer client that appends messages
- a consumer client that reads messages using broker-managed offsets
- on-disk storage with `.log` and `.idx` files
- segment rotation when a log grows too large
- startup discovery and recovery of all segments
- consumer group offset persistence
- a versioned binary protocol with request and response headers
- concurrent access using `sync.RWMutex`

It intentionally does not yet provide:

- topics
- partitions
- replication
- leader/follower behavior
- rebalancing
- batching
- compression
- retention policies
- durable cluster membership

The implementation is therefore best understood as a single-broker, file-backed message log with Kafka-like semantics.

## Repository Layout

```text
message-broker-system/
├── cmd/
│   ├── consumer/
│   │   └── main.go
│   ├── producer/
│   │   └── main.go
│   └── server/
│       └── main.go
├── internal/
│   ├── protocol/
│   │   └── protocol.go
│   ├── server/
│   │   └── handler.go
│   └── store/
│       └── store.go
├── data/
│   └── log/
├── go.mod
├── .gitignore
└── README.md
```

## High-Level Architecture

The system is organized into three primary layers.

### 1. Command Layer

The `cmd/` directory contains the executable entry points:

- `cmd/server/main.go` starts the broker
- `cmd/producer/main.go` sends messages to the broker
- `cmd/consumer/main.go` fetches and commits messages as a consumer group

### 2. Protocol and Request Handling Layer

The `internal/protocol/` and `internal/server/` packages handle network framing and command dispatch.

- `internal/protocol/protocol.go` defines the binary request and response headers
- `internal/server/handler.go` reads requests, decodes commands, and invokes the store

### 3. Storage Layer

The `internal/store/` package owns persistence, recovery, rotation, and offset lookup.

- `internal/store/store.go` manages all file I/O and segment metadata

## File-by-File Explanation

### `cmd/server/main.go`

This file starts the broker process.

What it does:

1. Creates the storage engine by calling `store.New("./data/log", "messages")`.
2. Opens a TCP listener on port `8080`.
3. Accepts incoming client connections in a loop.
4. Spawns a goroutine for each connection and hands it to `server.HandleConn`.

Why it matters:

- This is the broker entry point.
- It wires together storage and network handling.
- The same store instance is shared across all connections.

The server is intentionally simple: it focuses on being a long-running broker process rather than a web service or HTTP API.

### `cmd/producer/main.go`

This file is the producer client.

What it does:

1. Dials the broker at `localhost:8080`.
2. Creates a list of example messages.
3. For each message, builds a request header and payload.
4. Sends a `CommandProduce` request.
5. Reads the broker response header.
6. Reads the returned message offset.
7. Prints the acknowledged offset.

Important behavior:

- Each request gets a correlation ID.
- The client sends a structured protocol message rather than a raw byte stream.
- The broker returns the assigned offset after the append succeeds.

In Kafka terms, this is the producer side of the system. It is responsible for writing records into the broker's log.

### `cmd/consumer/main.go`

This file is the consumer client.

What it does:

1. Dials the broker at `localhost:8080`.
2. Uses a fixed consumer group ID.
3. Builds a `CommandFetch` request for the broker.
4. Receives the next available message for that group.
5. Prints the record and its offset.
6. Commits the offset back to the broker using `CommandCommit`.

Important behavior:

- The consumer no longer manually increments offsets on its own.
- It asks the broker for the next offset using the group ID.
- This means the broker owns the persisted consumer progress.
- A restart of the consumer does not lose its last committed position.

In Kafka terms, this is a consumer group client reading from a log and committing progress.

### `internal/protocol/protocol.go`

This file defines the binary wire protocol.

It contains:

- the protocol version constant
- command constants
- request and response header types
- header serialization and deserialization helpers

Why it exists:

A broker protocol should not be an ad hoc byte stream. A structured protocol makes the system easier to extend, debug, and evolve.

Current protocol features:

- protocol versioning
- correlation IDs
- client IDs
- command codes
- explicit payload lengths
- response status codes

The protocol is intentionally small, but it establishes the foundation for later additions such as batching, metadata requests, and partition-aware routing.

### `internal/server/handler.go`

This file handles all network requests after a connection is accepted.

Main responsibilities:

- read request headers
- read request payloads
- dispatch commands to the store
- write structured responses back to the client

Supported commands:

- `CommandProduce`
- `CommandConsume`
- `CommandCommit`
- `CommandFetch`

Each handler is responsible for validating its payload, invoking the store, and returning a well-formed response.

This is the request/response control plane of the broker. It is where network traffic becomes application behavior.

### `internal/store/store.go`

This is the core of the system.

It contains:

- the `Store` type
- the `Segment` type
- segment discovery logic
- rotation logic
- append logic
- read logic
- consumer offset storage
- recovery logic

This file is the heart of the broker because it defines how data is stored, found, recovered, and rotated.

## Core Data Structures

### `Store`

The `Store` type keeps all broker state in one place.

Relevant fields:

- `dirPath`: base directory for log files
- `baseName`: file prefix, currently `messages`
- `segments`: all known segments on disk
- `active`: the segment currently receiving appends
- `nextID`: the next message offset to assign
- `maxBytes`: maximum size of one segment before rotation
- `currentSize`: current size of the active segment
- `mu`: `sync.RWMutex` for concurrency control

### `Segment`

A segment represents one pair of log and index files.

Fields:

- `BaseOffset`: the first message ID contained in the segment
- `LogFile`: the `.log` file handle
- `IdxFile`: the `.idx` file handle

This is the key abstraction that fixes the segment-rotation bug from the earlier version. Instead of assuming all reads come from the active file, the store now knows how to locate the correct segment for any offset.

## Storage Format

### Log File

The log file stores the actual record payloads.

Each record is written as:

```text
[length:4 bytes][payload bytes]
```

Example:

```text
00 00 00 05 68 65 6c 6c 6f
```

That means:

- length = 5
- payload = `hello`

This format is simple, compact, and allows variable-length messages.

### Index File

The index file maps message offsets to positions inside the corresponding log.

Each index entry is:

```text
[messageID:8 bytes][logPosition:8 bytes]
```

Example:

```text
[ID=12][pos=0]
[ID=13][pos=9]
[ID=14][pos=21]
```

This index allows the broker to find a record without scanning the entire log file.

### Offset Files

Consumer group offsets are stored in files named after the group.

Format:

```text
[offset:8 bytes]
```

Example:

- `worker-group-1.offset`
- `analytics.offset`

This lets consumer groups resume from the correct position after a restart.

## Message Flow

### Produce Flow

When a producer sends a message:

1. The client creates a request header with version, correlation ID, client ID, command, and payload length.
2. The broker reads the header and payload.
3. The store checks whether the active segment is full.
4. If necessary, the store rotates to a new segment.
5. The payload is appended to the active log.
6. The log position is stored in the active index.
7. Both files are synced to disk.
8. The broker returns the assigned message ID.

This flow makes message writes durable and recoverable.

### Consume Flow

When a consumer requests a message by offset:

1. The broker receives a consume request.
2. The store locates the correct segment using the offset.
3. The segment-local index offset is computed.
4. The log position is read from the index.
5. The payload is read from the log.
6. The broker returns the payload.

This is the direct read path by ID.

### Fetch Flow

When a consumer group fetches the next message:

1. The broker reads the consumer group ID.
2. The store loads the last committed offset for that group.
3. The next unread offset is computed.
4. The broker attempts to read that message.
5. If a record exists, it is returned with its offset.
6. If no record exists yet, the broker returns the next offset to wait for.

This is the broker-managed offset flow and is the closer equivalent to Kafka consumer behavior.

### Commit Flow

When a consumer commits progress:

1. The broker reads the group ID and offset.
2. The store writes the offset to `{groupID}.offset`.
3. The offset is durable on disk.
4. Future fetches resume from the next position.

This is how consumer progress survives restarts.

## Segment Rotation

The system rotates segments when the active log file grows beyond `maxBytes`.

Current threshold:

- `maxBytes = 1MB`

Rotation process:

1. Close the current active segment files.
2. Rename the active `.log` and `.idx` files to include the starting offset.
3. Create a fresh `messages.log` and `messages.idx` pair.
4. Register the new segment as the active one.

Why this matters:

- log files stay bounded in size
- old data remains readable
- startup recovery becomes manageable
- the broker can locate records by segment

This is one of the most important differences between an append-only toy implementation and a real log-backed broker.

## Startup Recovery

On startup, the store does not assume a clean shutdown.

It performs three important tasks:

1. **Discover segments**
   - Scan `data/log`
   - Find all matching log and index files
   - Parse their base offsets
   - Open them in sorted order

2. **Recover the next offset**
   - Inspect the index files
   - Find the highest committed message ID
   - Set `nextID` to the next available value

3. **Validate the WAL**
   - Check index/log consistency in the active segment
   - Truncate incomplete trailing writes if necessary

This recovery path is what prevents the broker from losing logical continuity after a restart.

## Concurrency Model

The store uses `sync.RWMutex`.

Why this matters:

- multiple readers can access the store concurrently
- writes still remain exclusive
- reads do not block each other unnecessarily
- the broker is better suited for concurrent consumer traffic

Lock usage:

- `Append()` uses an exclusive lock
- `ReadByID()` uses a read lock
- segment rotation happens under the write path

This is a better fit for a log system than a plain `sync.Mutex` because read traffic is typically much higher than write traffic.

## Current Network Behavior

The broker listens on TCP port `8080`.

Request handling is connection-based rather than HTTP-based.

That means:

- the protocol is binary, not JSON
- the broker can keep a connection open for multiple requests
- every request includes a command and payload length
- responses include status and payload length

This is closer to how a low-level broker protocol works in practice.

## How the Project Maps to Kafka Concepts

This project mirrors Kafka in a simplified form.

### Kafka Concept to Project Mapping

- **Broker** → `cmd/server/main.go` plus `internal/server/handler.go`
- **Producer** → `cmd/producer/main.go`
- **Consumer** → `cmd/consumer/main.go`
- **Log segment** → `Segment` in `internal/store/store.go`
- **Offset** → assigned message ID
- **Consumer group offset** → `{groupID}.offset`
- **Append-only log** → `.log` file
- **Index** → `.idx` file
- **Recovery** → `discoverSegments()` and `recoverState()`

### What Kafka Has That This Project Does Not Yet Have

- multiple topics
- partitions per topic
- replication
- leader election
- follower synchronization
- consumer group rebalance
- heartbeat/session timeouts
- offset commit APIs compatible with Kafka clients
- retention policies
- compression
- transactional semantics
- zero-copy network optimizations

Those are all reasonable future additions, but the current codebase is focused on getting the storage and offset model correct first.

## How to Run

### Prerequisites

- Go 1.26.2 or compatible Go toolchain
- TCP port `8080` available

### Build

From the project root:

```bash
go build ./cmd/server
go build ./cmd/producer
go build ./cmd/consumer
```

### Run the Broker

```bash
./server
```

The broker will start on `:8080` and create its storage under `./data/log`.

### Run the Producer

In a second terminal:

```bash
./producer
```

This sends a small set of sample messages to the broker.

### Run the Consumer

In a third terminal:

```bash
./consumer
```

This fetches messages for the configured group and commits offsets back to the broker.

## Data Directory

Runtime data lives under `data/log`.

Expected files include:

- `messages.log`
- `messages.idx`
- rotated files such as `messages-0000000005.log`
- consumer offset files such as `worker-group-1.offset`

These files are generated at runtime and should not be committed to source control.

## Development Notes

- The broker intentionally uses a minimal file format to keep the storage model easy to reason about.
- The code favors explicit logic over abstraction-heavy design.
- The protocol is binary so the system can evolve without changing to text-based framing later.
- The consumer client demonstrates broker-managed fetch and commit behavior rather than manual offset tracking.

## Known Limitations

These are not bugs in the current design, but they are important constraints:

- only one broker process exists
- there is no replication
- there are no partitions
- there is no retention policy
- batching is not implemented
- compression is not implemented
- protocol framing is simple and does not yet include all Kafka-style metadata

