[![progress-banner](https://backend.codecrafters.io/progress/redis/5524b540-1740-470e-a270-03e30a7b6e93)](https://app.codecrafters.io/users/vedant-kokate?r=2qF)

# Redis Server in Python

A Redis-compatible server implemented from scratch in Python as part of the [CodeCrafters "Build Your Own Redis" challenge](https://codecrafters.io/challenges/redis).

The goal was to go beyond implementing individual commands and understand the systems behind Redis: the RESP protocol, TCP connections, concurrency, transactions, persistence, replication, blocking operations, and data structures.

## What is implemented

### Core server

* TCP server with concurrent client connections
* RESP protocol parsing and response encoding
* Command dispatch and request handling
* Multiple clients handled concurrently using Python threads
* Key expiration with `PX`

### Strings

* `PING`
* `ECHO`
* `SET`
* `GET`
* `INCR`
* `TYPE`
* `KEYS`

### Lists

* `LPUSH`
* `RPUSH`
* `LPOP`
* `BLPOP`
* `LLEN`
* `LRANGE`

### Streams

* `XADD`
* `XRANGE`
* `XREAD`

### Transactions

* `MULTI`
* `EXEC`
* `DISCARD`
* `WATCH`
* `UNWATCH`

The implementation tracks key versions to detect modifications between `WATCH` and `EXEC`.

### Replication

* Primary/replica server roles
* `PSYNC`
* Replication offsets
* Replica acknowledgements
* `WAIT`
* Command propagation to replicas

### Pub/Sub

* `SUBSCRIBE`
* `UNSUBSCRIBE`
* `PUBLISH`

### Sorted sets

* `ZADD`
* `ZRANK`
* `ZRANGE`
* `ZCARD`
* `ZSCORE`
* `ZREM`

### Geospatial operations

* `GEOADD`
* `GEOPOS`
* `GEODIST`
* `GEOSEARCH`

Geospatial coordinates are encoded using an integer grid and bit interleaving, similar to the approach used by Redis for geospatial indexing.

### Authentication and ACL

* `AUTH`
* `ACL`
* User/password management
* Per-connection authentication state

## Architecture

The server is intentionally implemented with a small number of primitives rather than relying on Redis-specific libraries.

At a high level:

```text
Client
  │
  │ TCP
  ▼
RESP parser
  │
  ▼
Command dispatcher
  │
  ├── Strings
  ├── Lists
  ├── Streams
  ├── Transactions
  ├── Pub/Sub
  ├── Sorted Sets
  ├── Geospatial
  ├── Replication
  └── ACL / Authentication
  │
  ▼
In-memory data store
```

Concurrency is handled with Python threads, locks, conditions, and per-connection state. Conditions are also used for blocking operations such as `BLPOP`.

## Running locally

The server entry point is:

```bash
app/main.py
```

Run it with:

```bash
./your_program.sh
```

The default Redis port is `6379`.

You can then connect with the Redis CLI:

```bash
redis-cli -p 6379
```

For example:

```text
SET name Vedant
GET name
```

## CodeCrafters

This project was built against the CodeCrafters Redis challenge and completed all available stages.

The original challenge is available at:

https://codecrafters.io/challenges/redis

## Why I built this

I wanted to understand Redis as a system rather than only use it as a dependency.

Building the server from scratch made concepts such as wire protocols, concurrent connections, transactions, replication, blocking operations, and data-structure implementations much more concrete.
