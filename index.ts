import { CString, dlopen, FFIType, ptr, read, type Pointer } from "bun:ffi";
import path from "node:path";

export interface QueryStats {
  elapsed: number;
  rowsRead: bigint; // uint64_t
  bytesRead: bigint; // uint64_t
}

export interface QueryResultWithStats {
  data: string;
  stats: QueryStats;
}

export class CHDBError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "CHDBError";
  }
}

const chdb = dlopen(import.meta.resolve("./libchdb.so"), {
  query_stable_v2: {
    args: [FFIType.i32, FFIType.ptr], // argc, char** argv
    returns: FFIType.ptr, // local_result_v2*
  },
  free_result_v2: {
    args: [FFIType.ptr], // local_result_v2*
    returns: FFIType.void,
  },
  connect_chdb: {
    args: [FFIType.i32, FFIType.ptr], // argc, char** argv
    returns: FFIType.ptr, // chdb_conn**
  },
  close_conn: {
    args: [FFIType.ptr], // chdb_conn**
    returns: FFIType.void,
  },
  query_conn: {
    args: [FFIType.ptr, FFIType.cstring, FFIType.cstring], // chdb_conn*, const char* query, const char* format
    returns: FFIType.ptr, // local_result_v2*
  },
  // Streaming related functions
  query_conn_streaming: {
    args: [FFIType.ptr, FFIType.cstring, FFIType.cstring], // chdb_conn*, const char* query, const char* format
    returns: FFIType.ptr, // chdb_streaming_result*
  },
  chdb_streaming_result_error: {
    args: [FFIType.ptr], // chdb_streaming_result*
    returns: FFIType.ptr, // const char* (error message)
  },
  chdb_streaming_fetch_result: {
    args: [FFIType.ptr, FFIType.ptr], // chdb_conn*, chdb_streaming_result*
    returns: FFIType.ptr, // local_result_v2*
  },
  chdb_streaming_cancel_query: {
    args: [FFIType.ptr, FFIType.ptr], // chdb_conn*, chdb_streaming_result*
    returns: FFIType.void,
  },
  chdb_destroy_result: {
    args: [FFIType.ptr], // chdb_streaming_result*
    returns: FFIType.void,
  },
});

const connectionRegistry = new FinalizationRegistry<Pointer>((connPtr) => {
  chdb.symbols.close_conn(connPtr);
});

const streamRegistry = new FinalizationRegistry<
  [connPtr: Pointer, streamPtr: Pointer]
>(([connPtr, streamPtr]) => {
  chdb.symbols.chdb_streaming_cancel_query(connPtr, streamPtr);
  chdb.symbols.chdb_destroy_result(streamPtr);
});

function buildArgv(args: string[]): {
  argc: number;
  argvPtr: Pointer;
} {
  const pointerArray = new BigUint64Array(args.length + 1);

  for (let i = 0; i < args.length; i++) {
    const argPtrVal = ptr(Buffer.from(args[i] + "\0"));
    pointerArray[i] = BigInt(argPtrVal);
  }
  pointerArray[args.length] = 0n; // Null-terminate the array of pointers

  return {
    argc: args.length,
    argvPtr: ptr(pointerArray),
  };
}

function processLocalResultV2(resultPtr: Pointer): QueryResultWithStats {
  // Offsets for local_result_v2 (assuming 64-bit pointers and size_t)
  // struct local_result_v2 {
  //     char * buf;            // offset 0
  //     size_t len;            // offset 8
  //     void * _vec;           // offset 16
  //     double elapsed;        // offset 24
  //     uint64_t rows_read;    // offset 32
  //     uint64_t bytes_read;   // offset 40
  //     char * error_message;  // offset 48
  // };

  const bufOffset = 0;
  const lenOffset = 8;
  const elapsedOffset = 24;
  const rowsReadOffset = 32;
  const bytesReadOffset = 40;
  const errorMessageOffset = 48;

  const errorMessagePtr = read.ptr(resultPtr, errorMessageOffset) as Pointer;

  try {
    if (errorMessagePtr) {
      const errorMessage = new CString(errorMessagePtr).toString();
      throw new CHDBError(`chDB Error: ${errorMessage}`);
    }
    const bufPtr = read.ptr(resultPtr, bufOffset) as Pointer;
    const len = read.u64(resultPtr, lenOffset);
    const data = new CString(bufPtr, 0, Number(len)).toString();

    const stats: QueryStats = {
      elapsed: read.f64(resultPtr, elapsedOffset),
      rowsRead: read.u64(resultPtr, rowsReadOffset),
      bytesRead: read.u64(resultPtr, bytesReadOffset),
    };
    return { data, stats };
  } finally {
    chdb.symbols.free_result_v2(resultPtr);
  }
}

function parseConnectionString(
  str: string
): [path: string, params: Record<string, string>] {
  const params: Record<string, string> = {};

  if (!str || str === ":memory:") {
    return [":memory:", params];
  }

  if (str.startsWith("file:")) {
    str = str.slice(5);
    if (str.startsWith("///")) {
      str = str.slice(2);
    }
  }

  const queryPos = str.indexOf("?");
  if (queryPos !== -1) {
    const search = new URLSearchParams(str.slice(queryPos + 1));
    for (const [key, value] of search.entries()) {
      params[key] = value;
    }

    const udfPath = search.get("udf_path");
    if (udfPath) {
      params["--"] = "";
      params["user_scripts_path"] = udfPath;
      params["user_defined_executable_functions_config"] = `${udfPath}/*.xml`;
      delete params["udf_path"];
    }

    str = str.slice(0, queryPos);
  }

  if (str && str[0] !== "/" && str !== ":memory:") {
    str = path.resolve(str);
  }

  return [str, params];
}

export function query(
  queryString: string,
  format: string = "CSV"
): QueryResultWithStats {
  const queryArgs = [
    "clickhouse",
    "--multiquery",
    `--output-format=${format}`,
    `--query=${queryString}`,
  ];

  const { argc, argvPtr } = buildArgv(queryArgs);
  const resultPtr = chdb.symbols.query_stable_v2(argc, argvPtr);
  if (!resultPtr) {
    throw new CHDBError(
      "chDB call failed to return a result structure (null pointer)."
    );
  }
  return processLocalResultV2(resultPtr);
}

export class Connection {
  public readonly path: string;
  public readonly params: Record<string, string>;

  private ptr: Pointer;

  constructor(connectionString: string) {
    const [path, params] = parseConnectionString(connectionString);

    this.path = path;
    this.params = params;

    const args = ["clickhouse"];
    if (path !== ":memory:") {
      args.push(`--path=${path}`);
    }
    for (const [key, value] of Object.entries(params)) {
      if (key === "mode") {
        if (value === "ro") {
          args.push(`--readonly=1`);
        }
      } else if (key === "--") {
        args.push(`--`);
      } else if (!value) {
        args.push(`--${key}`);
      } else {
        args.push(`--${key}=${value}`);
      }
    }

    const { argc, argvPtr } = buildArgv(args);

    const ptr = chdb.symbols.connect_chdb(argc, argvPtr);
    if (!ptr) {
      throw new CHDBError(
        `Failed to connect to chDB connection at path: ${this.path}`
      );
    }

    this.ptr = ptr;

    connectionRegistry.register(this, this.ptr);
  }

  private getConnPtr(): Pointer {
    const ptr = read.ptr(this.ptr, 0) as Pointer;
    if (!ptr) {
      throw new CHDBError(
        "Invalid connection pointer. The connection might be corrupted or closed."
      );
    }
    return ptr;
  }

  query(queryString: string, format: string = "CSV"): QueryResultWithStats {
    const connPtr = this.getConnPtr();
    const queryBuffer = Buffer.from(queryString + "\0");
    const formatBuffer = Buffer.from(format + "\0");

    const resultPtr = chdb.symbols.query_conn(
      connPtr,
      queryBuffer,
      formatBuffer
    );

    if (!resultPtr) {
      throw new CHDBError(
        "chDB call failed to return a result structure (null pointer)."
      );
    }

    return processLocalResultV2(resultPtr);
  }

  stream(
    queryString: string,
    format: string = "CSV"
  ): Generator<QueryResultWithStats, void, undefined> {
    type StreamContext = { connPtr: Pointer; streamPtr: Pointer };

    function* iterate(ctx: StreamContext) {
      try {
        const initialErrorPtr = chdb.symbols.chdb_streaming_result_error(
          ctx.streamPtr
        );
        if (initialErrorPtr) {
          const errorMessage = new CString(initialErrorPtr).toString();
          throw new CHDBError(
            `chDB Streaming Initialization Error: ${errorMessage}`
          );
        }

        while (true) {
          const resultChunkPtr = chdb.symbols.chdb_streaming_fetch_result(
            ctx.connPtr,
            ctx.streamPtr
          );
          if (!resultChunkPtr) {
            const streamErrorPtr = chdb.symbols.chdb_streaming_result_error(
              ctx.streamPtr
            );
            if (streamErrorPtr) {
              const errorMessage = new CString(streamErrorPtr).toString();
              throw new CHDBError(
                `chDB Streaming Fetch Error: ${errorMessage}`
              );
            }
            break;
          }

          const bufPtrInChunk = read.ptr(resultChunkPtr, 0);
          if (!bufPtrInChunk) {
            chdb.symbols.free_result_v2(resultChunkPtr);
            break;
          }

          yield processLocalResultV2(resultChunkPtr);
        }
      } finally {
        streamRegistry.unregister(ctx);
        chdb.symbols.chdb_destroy_result(ctx.streamPtr);
      }
    }

    const connPtr = this.getConnPtr();

    const streamPtr = chdb.symbols.query_conn_streaming(
      connPtr,
      Buffer.from(queryString + "\0"),
      Buffer.from(format + "\0")
    );
    if (!streamPtr) {
      throw new CHDBError("Failed to initiate chDB streaming query.");
    }

    const ctx: StreamContext = { connPtr, streamPtr };

    streamRegistry.register(ctx, [connPtr, streamPtr], ctx);

    return iterate(ctx);
  }
}
