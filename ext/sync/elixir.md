# Synchronous Replication with Elixir + ecto_sqlite3

Ensure critical transactions are replicated to S3 before `Repo.transaction` returns. If replication fails, the transaction rolls back — your app sees an error, and nothing is committed locally or remotely.

## 1. Build the extension

```bash
cd ext/sync
make
```

Copy `litestream_sync.so` somewhere your app can find it at runtime, e.g. `priv/native/litestream_sync`.

## 2. Bootstrap the VFS before the Repo starts

The extension registers a VFS shim as the default SQLite VFS. It must be loaded **once** before any Ecto connections open. The simplest way is a one-shot call in `application.ex`:

```elixir
# lib/my_app/application.ex

defmodule MyApp.Application do
  use Application

  @impl true
  def start(_type, _args) do
    # Register the litestream VFS before Repo opens connections.
    load_litestream_sync!()

    children = [
      MyApp.Repo,
      # ...
    ]

    opts = [strategy: :one_for_one, name: MyApp.Supervisor]
    Supervisor.start_link(children, opts)
  end

  defp load_litestream_sync! do
    ext_path = Path.join(:code.priv_dir(:my_app), "native/litestream_sync")
    {:ok, conn} = Exqlite.Sqlite3.open(":memory:")
    :ok = Exqlite.Sqlite3.enable_load_extension(conn)
    :ok = Exqlite.Sqlite3.execute(conn, "SELECT load_extension('#{ext_path}')")
    :ok = Exqlite.Sqlite3.close(conn)
  end
end
```

After this call, the "litestream" VFS is the process-wide default. All subsequent `Exqlite` / `ecto_sqlite3` connections automatically use it. The shim is a pure passthrough (zero overhead) unless you explicitly set `PRAGMA litestream_sync = 1`.

## 3. Configure the socket path

Set the `LITESTREAM_SOCKET` environment variable to match your litestream config:

```bash
export LITESTREAM_SOCKET="/var/run/litestream.sock"
```

This is read once when the extension loads. If unset, it defaults to `/var/run/litestream.sock`.

You can also override per-database at runtime (rarely needed):

```elixir
Repo.query!("PRAGMA litestream_socket = '/tmp/my-other.sock'")
```

## 4. Use synchronous replication

Wrap critical writes with `PRAGMA litestream_sync = 1` inside a transaction:

```elixir
Repo.transaction(fn ->
  Repo.query!("PRAGMA litestream_sync = 1")
  Repo.insert!(%Payment{amount: 100, user_id: user.id})
end)
# If we reach here: committed locally AND uploaded to S3.
# If MatchError/rollback: nothing committed anywhere.
```

The flag **auto-resets to 0** after the commit (or rollback), so normal transactions that don't set it are completely unaffected.

### Helper for cleaner call sites

```elixir
defmodule MyApp.Repo do
  use Ecto.Repo, otp_app: :my_app, adapter: Ecto.Adapters.SQLite3

  @doc """
  Like `transaction/2`, but ensures the transaction is replicated to S3
  before returning. Rolls back if replication fails.
  """
  def synced_transaction(fun_or_multi, opts \\ []) do
    transaction(fn ->
      query!("PRAGMA litestream_sync = 1")

      case fun_or_multi do
        %Ecto.Multi{} -> raise "use a function, not Multi"
        fun when is_function(fun, 0) -> fun.()
      end
    end, opts)
  end
end
```

Then:

```elixir
MyApp.Repo.synced_transaction(fn ->
  Repo.insert!(%Payment{amount: 100})
end)
```

## How it works

```
Repo.transaction →
  PRAGMA litestream_sync = 1  (sets flag in VFS shim)
  INSERT ...                  (writes to WAL as normal)
  COMMIT →
    SQLite calls xSync on WAL file →
      VFS shim sees syncOnCommit=1 →
        POST /sync-replicate to litestream Unix socket →
          litestream: DB.Sync() → Replica.Sync() → S3 upload
        success → real fsync → COMMIT completes
        failure → SQLITE_IOERR → SQLite rolls back
```

## Requirements

- litestream running with `socket.enabled: true` in its config
- The `litestream_sync.so` extension built for your platform
- `LITESTREAM_SOCKET` env var pointing to the litestream control socket
