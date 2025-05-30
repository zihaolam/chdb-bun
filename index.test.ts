import { test, expect } from "bun:test";
import { Connection, query } from ".";

test("query() works", () => {
  const result = query("select 123");
  expect(result.data).toMatchInlineSnapshot(`
    "123
    "
  `);
  expect(result.stats.rowsRead).toMatchInlineSnapshot(`1n`);
});

test("Connection.query() works", () => {
  const conn = new Connection(":memory:");
  const result = conn.query("select 123");
  expect(result.data).toMatchInlineSnapshot(`
    "123
    "
  `);
  expect(result.stats.rowsRead).toMatchInlineSnapshot(`1n`);
});

test("Connection.stream() works", () => {
  const conn = new Connection(":memory:");

  let rowsRead = 0n;
  for (const result of conn.stream("select * from numbers(200_000)")) {
    rowsRead += result.stats.rowsRead;
  }
  expect(rowsRead).toMatchInlineSnapshot(`200000n`);
});

test("Connection.query() in-memory example", () => {
  const conn = new Connection(":memory:");

  conn.query(
    "CREATE TABLE IF NOT EXISTS users (id UInt32, name String) ENGINE = Memory;"
  );
  conn.query("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob');");

  const result = conn.query("SELECT * FROM users ORDER BY id", "JSONEachRow");
  expect(result.data).toMatchInlineSnapshot(`
    "{"id":1,"name":"Alice"}
    {"id":2,"name":"Bob"}
    "
  `);
});
