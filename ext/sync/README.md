# Litestream Synchronous Replication Extension

A SQLite loadable extension that enables synchronous replication through the litestream sidecar. When enabled, SQLite transactions block until data is uploaded to S3 (or other replica storage), and roll back if replication fails.

## Build

```bash
make
```

Produces `litestream_sync.so`. Requires only the SQLite headers (included in `../../src/`).

## Usage

1. Start litestream with the control socket enabled.

2. Load the extension and configure the socket path:

```sql
.load ./litestream_sync
PRAGMA litestream_socket = '/var/run/litestream.sock';
```

The socket path defaults to the `LITESTREAM_SOCKET` environment variable, falling back to `/var/run/litestream.sock`.

3. Ensure `synchronous=FULL` is set (required for WAL mode — the default `NORMAL` only fsyncs during checkpoints, so xSync would never fire on commits):

```sql
PRAGMA synchronous=FULL;
```

4. Enable synchronous replication per-transaction:

```sql
PRAGMA litestream_sync = 1;
BEGIN;
INSERT INTO payments (amount) VALUES (100);
COMMIT;  -- blocks until S3 upload completes; rolls back on failure
```

The `litestream_sync` flag auto-resets to 0 after each commit/rollback, so only explicitly opted-in transactions use synchronous replication.

## How It Works

The extension registers a VFS wrapper ("litestream") as the **default VFS**. It is a transparent passthrough for all operations — zero overhead when `litestream_sync` is not set. When `litestream_sync = 1`, the intercepted WAL `xSync`:

1. POSTs to the litestream sidecar's `/sync-replicate` endpoint over the Unix socket
2. Litestream reads new WAL frames and uploads LTX files to S3
3. On success, the real `xSync` (fsync) runs and the commit completes
4. On failure, `SQLITE_IOERR` is returned and SQLite rolls back the transaction
