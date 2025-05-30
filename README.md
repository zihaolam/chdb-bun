# chdb-bun

Native chDB bindings for [Bun](https://bun.sh).

[chDB](https://clickhouse.com/chdb) is a fast, reliable, and scalable in-process OLAP SQL database built on top of Clickhouse.

## Motivation

The [official chDB bindings for Bun](https://github.com/chdb-io/chdb-bun) are out-of-date. It:

1. requires [libchdb](https://github.com/chdb-io/chdb) to be installed on the system,
2. requires `gcc` or `clang` to be available on the system for building C code,
3. does not support the latest version of chdb, which includes several performance improvements and bug fixes and features such as:
   - [a connection-based API](https://github.com/chdb-io/chdb?tab=readme-ov-file#%EF%B8%8F-connection-based-api-recommended),
   - [streaming queries](https://github.com/chdb-io/chdb?tab=readme-ov-file#%EF%B8%8F-streaming-query), and
   - query runtime statistics, and
4. does not automatically release/destroy resources when garbage-collected.

## Features

- Built using [bun:ffi](https://bun.sh/docs/api/ffi).
- Supports the latest version of [libchdb](https://github.com/chdb-io/chdb).
- Streaming query support.
- Connection-based API support.
- Connections are closed and cleaned up if garbage-collected via. [FinalizationRegistry](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/FinalizationRegistry).
- Streams are closed and cleaned up if garbage-collected via. [FinalizationRegistry](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/FinalizationRegistry).

## Installation

```bash
bun install @lithdew/chdb-bun && bun pm trust @lithdew/chdb-bun
```

## Usage

```typescript
import { query, Connection } from "@lithdew/chdb-bun";
```

### Stateless Query

For single, self-contained queries:

```typescript
try {
  const result = query(
    "SELECT number, toString(number) FROM system.numbers LIMIT 3",
    "Pretty"
  );
  console.log("Query Result:\n", result.data);
  console.log("Statistics:", result.stats);
  // Output:
  // Query Result:
  // ┌─number─┬─toString(number)─┐
  // │      0 │ 0                │
  // │      1 │ 1                │
  // │      2 │ 2                │
  // └────────┴──────────────────┘
  // Statistics: { elapsed: 0.001234, rowsRead: 3n, bytesRead: 24n }
} catch (e) {
  console.error("Stateless query execution failed:", e);
}
```

### Stateful Connection

For scenarios involving persistent storage, multiple queries within a session, or UDFs.

#### In-Memory Database

```typescript
try {
  const conn = new Connection(":memory:"); // Establishes an in-memory database session

  conn.query(
    "CREATE TABLE IF NOT EXISTS users (id UInt32, name String) ENGINE = Memory;"
  );
  conn.query("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob');");

  const result = conn.query("SELECT * FROM users ORDER BY id", "JSONEachRow");
  console.log("Users:\n", result.data);
  // Output:
  // Users:
  // {"id":1,"name":"Alice"}
  // {"id":2,"name":"Bob"}
} catch (e) {
  console.error("In-memory connection operation failed:", e);
}
```

#### File-Based Database

Data is persisted to the specified file path.

```typescript
try {
  const conn = new Connection("my_database.db"); // Relative path
  // Alternatively, an absolute path: new Connection("/path/to/my_database.db")
  // Or using the file: prefix: new Connection("file:my_database.db")

  conn.query(
    "CREATE TABLE IF NOT EXISTS events (timestamp DateTime, event_name String) ENGINE = MergeTree ORDER BY timestamp;"
  );
  conn.query(
    "INSERT INTO events VALUES (now(), 'AppStart'), (now() + 1, 'UserLogin');"
  );

  const events = conn.query("SELECT * FROM events", "CSVWithNames");
  console.log("Events:\n", events.data);
} catch (e) {
  console.error("File-based connection operation failed:", e);
}
```

### Streaming Query

Process large result sets efficiently by iterating over data chunks.

```typescript
try {
  const conn = new Connection(":memory:");

  conn.query(
    "CREATE TABLE IF NOT EXISTS large_data (value Int64) ENGINE = Memory;"
  );
  conn.query(
    "INSERT INTO large_data SELECT number FROM system.numbers LIMIT 200000;"
  ); // Populates with 200,000 rows

  const stream = conn.stream("SELECT * FROM large_data", "CSV");

  let totalRowsStreamed = 0n;
  let chunkCount = 0;

  for (const chunk of stream) {
    chunkCount++;
    // chunk.data contains a segment of the result set
    console.log(
      `Chunk ${chunkCount} statistics: ${chunk.stats.rowsRead} rows, ${chunk.stats.bytesRead} bytes, duration ${chunk.stats.elapsed}s`
    );
    totalRowsStreamed += chunk.stats.rowsRead;
  }

  console.log(
    `\nTotal rows processed via stream: ${totalRowsStreamed} across ${chunkCount} chunks.`
  );
} catch (e) {
  console.error("Streaming query execution failed:", e);
}
```

### Error Handling

Errors originating from chDB operations will throw a `CHDBError`.